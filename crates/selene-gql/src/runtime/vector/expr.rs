//! Batch expression evaluator for columnar DataChunk execution.
//!
//! `eval_vec` evaluates an expression tree once across all active rows in a
//! DataChunk, producing a `Column` result. This replaces the Phase 1 pattern
//! of `for row in chunk { eval_expr_ctx(expr, &row.to_binding(), ctx) }` with
//! a single column-level dispatch per expression node.
//!
//! Supported expressions: Literal, Var, Property, Compare, Logic (AND/OR/NOT),
//! Arithmetic, IsNull, InList, Negate, Parameter.
//!
//! Unsupported expressions (subqueries, aggregates, CASE, temporal property,
//! function calls) return `Err(Unsupported)`, signaling the caller to fall
//! back to per-row RowView evaluation.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder,
    StringBuilder, UInt64Array,
};
use selene_core::IStr;

use crate::ast::expr::{ArithOp, CompareOp, Expr, LogicOp};
use crate::runtime::eval::EvalContext;
use crate::runtime::vector::gather::PropertyGatherer;
use crate::types::chunk::{Column, DataChunk};
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

/// Evaluate an expression across all physical rows of a DataChunk, producing
/// a Column result.
///
/// The selection vector is passed to PropertyGatherer to avoid reading
/// properties for filtered-out rows, but the output column always has
/// `chunk.len()` physical rows (matching the chunk's physical layout).
///
/// Returns `Err(GqlError::Unsupported)` for expression nodes not yet handled
/// by the batch evaluator. The caller should catch this and fall back to
/// per-row evaluation.
pub fn eval_vec(
    expr: &Expr,
    chunk: &DataChunk,
    gatherer: &dyn PropertyGatherer,
    ctx: &EvalContext<'_>,
) -> Result<Column, GqlError> {
    let len = chunk.len();
    let sel = chunk.selection().indices();

    match expr {
        // ── Leaves ──────────────────────────────────────────────
        Expr::Literal(val) => Ok(broadcast_literal(val, len)),

        Expr::Var(name) => eval_var(*name, chunk),

        Expr::Parameter(name) => {
            let val = ctx
                .parameters
                .and_then(|p| p.get(name))
                .cloned()
                .unwrap_or(GqlValue::Null);
            Ok(broadcast_literal(&val, len))
        }

        Expr::Property(target, key) => eval_property(target, *key, chunk, gatherer, ctx),

        // ── Comparisons ─────────────────────────────────────────
        Expr::Compare(left, op, right) => {
            let lc = eval_vec(left, chunk, gatherer, ctx)?;
            let rc = eval_vec(right, chunk, gatherer, ctx)?;
            eval_compare_columns(&lc, *op, &rc, len, ctx)
        }

        // ── Logic ───────────────────────────────────────────────
        Expr::Logic(left, op, right) => {
            let lc = eval_vec(left, chunk, gatherer, ctx)?;
            let rc = eval_vec(right, chunk, gatherer, ctx)?;
            Ok(eval_logic_columns(&lc, *op, &rc, len))
        }

        Expr::Not(inner) => {
            let col = eval_vec(inner, chunk, gatherer, ctx)?;
            Ok(eval_not_column(&col, len))
        }

        // ── Arithmetic ──────────────────────────────────────────
        Expr::Arithmetic(left, op, right) => {
            let lc = eval_vec(left, chunk, gatherer, ctx)?;
            let rc = eval_vec(right, chunk, gatherer, ctx)?;
            eval_arithmetic_columns(&lc, *op, &rc, len, ctx)
        }

        Expr::Negate(inner) => {
            let col = eval_vec(inner, chunk, gatherer, ctx)?;
            eval_negate_column(&col, len)
        }

        // ── Predicates ──────────────────────────────────────────
        Expr::IsNull { expr, negated } => {
            let col = eval_vec(expr, chunk, gatherer, ctx)?;
            let mut builder = BooleanBuilder::with_capacity(len);
            for i in 0..len {
                let is_null = col.is_null(i);
                builder.append_value(if *negated { !is_null } else { is_null });
            }
            Ok(Column::Bool(Arc::new(builder.finish())))
        }

        Expr::InList {
            expr,
            list,
            negated,
        } => eval_in_list(expr, list, *negated, chunk, gatherer, ctx, sel),

        // ── Unsupported: fall back to per-row ───────────────────
        _ => Err(GqlError::Unsupported {
            feature: "eval_vec: unsupported expression node".into(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Leaf evaluators
// ---------------------------------------------------------------------------

/// Broadcast a scalar literal to a constant-valued column.
fn broadcast_literal(val: &GqlValue, len: usize) -> Column {
    match val {
        GqlValue::Null => Column::Null(len),
        GqlValue::Int(v) => Column::Int64(Arc::new(Int64Array::from(vec![*v; len]))),
        GqlValue::UInt(v) => Column::UInt64(Arc::new(UInt64Array::from(vec![*v; len]))),
        GqlValue::Float(v) => Column::Float64(Arc::new(Float64Array::from(vec![*v; len]))),
        GqlValue::Bool(v) => Column::Bool(Arc::new(BooleanArray::from(vec![*v; len]))),
        GqlValue::String(s) => {
            let mut builder = StringBuilder::new();
            for _ in 0..len {
                builder.append_value(s.as_str());
            }
            Column::Utf8(Arc::new(builder.finish()))
        }
        other => Column::Values(Arc::from(vec![other.clone(); len])),
    }
}

/// Resolve a variable reference to its column in the chunk.
fn eval_var(name: IStr, chunk: &DataChunk) -> Result<Column, GqlError> {
    let slot = chunk
        .schema()
        .slot_of(&name)
        .ok_or_else(|| GqlError::internal(format!("eval_vec: unbound variable '{name}'")))?;
    Ok(chunk.column(slot).clone())
}

/// Evaluate property access: gather the property column from the graph.
fn eval_property(
    target: &Expr,
    key: IStr,
    chunk: &DataChunk,
    gatherer: &dyn PropertyGatherer,
    _ctx: &EvalContext<'_>,
) -> Result<Column, GqlError> {
    // The common case: Expr::Property(Expr::Var(name), key)
    // Resolve the variable to find its column, then gather the property.
    match target {
        Expr::Var(var_name) => {
            let slot = chunk.schema().slot_of(var_name).ok_or_else(|| {
                GqlError::internal(format!("eval_vec: unbound variable '{var_name}'"))
            })?;
            let col = chunk.column(slot);
            let sel = chunk.selection().indices();

            match col {
                Column::NodeIds(ids) => Ok(gatherer.gather_node_property(ids, sel, key)),
                Column::EdgeIds(ids) => Ok(gatherer.gather_edge_property(ids, sel, key)),
                // For Values columns containing Node/Edge refs, fall back
                _ => Err(GqlError::Unsupported {
                    feature: "eval_vec: property access on non-entity column".into(),
                }),
            }
        }
        // Nested property (e.g., a.b.c) or computed target: unsupported
        _ => Err(GqlError::Unsupported {
            feature: "eval_vec: computed property target".into(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Column-level comparison
// ---------------------------------------------------------------------------

fn eval_compare_columns(
    left: &Column,
    op: CompareOp,
    right: &Column,
    len: usize,
    ctx: &EvalContext<'_>,
) -> Result<Column, GqlError> {
    // Fast path: both columns are the same typed Arrow array.
    // Try typed comparison first, fall back to per-element GqlValue comparison.
    match (left, right) {
        (Column::Int64(la), Column::Int64(ra)) => {
            return Ok(Column::Bool(Arc::new(compare_int64(la, op, ra))));
        }
        (Column::Float64(la), Column::Float64(ra)) => {
            return Ok(Column::Bool(Arc::new(compare_float64(la, op, ra))));
        }
        (Column::UInt64(la), Column::UInt64(ra)) => {
            return Ok(Column::Bool(Arc::new(compare_uint64(la, op, ra))));
        }
        (Column::Utf8(la), Column::Utf8(ra)) => {
            return Ok(Column::Bool(Arc::new(compare_utf8(la, op, ra))));
        }
        (Column::Bool(la), Column::Bool(ra)) if matches!(op, CompareOp::Eq | CompareOp::Neq) => {
            return Ok(Column::Bool(Arc::new(compare_bool(la, op, ra))));
        }
        // Numeric promotion: Int64 vs Float64 (cross-type, requires strict check)
        (Column::Int64(_), Column::Float64(_)) | (Column::Float64(_), Column::Int64(_)) => {
            if ctx.options.strict_coercion {
                let (lt, rt) = match (left, right) {
                    (Column::Int64(_), Column::Float64(_)) => ("INT64", "FLOAT64"),
                    _ => ("FLOAT64", "INT64"),
                };
                return Err(crate::types::coercion::strict_type_error(lt, rt, "compare"));
            }
            match (left, right) {
                (Column::Int64(la), Column::Float64(ra)) => {
                    let promoted = promote_int64_to_float64(la);
                    return Ok(Column::Bool(Arc::new(compare_float64(&promoted, op, ra))));
                }
                (Column::Float64(la), Column::Int64(ra)) => {
                    let promoted = promote_int64_to_float64(ra);
                    return Ok(Column::Bool(Arc::new(compare_float64(la, op, &promoted))));
                }
                _ => unreachable!(),
            }
        }
        _ => {}
    }

    // Fallback: per-element comparison via GqlValue
    compare_via_gql_value(left, right, op, len, ctx)
}

fn compare_int64(left: &Int64Array, op: CompareOp, right: &Int64Array) -> BooleanArray {
    let len = left.len();
    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let (l, r) = (left.value(i), right.value(i));
            builder.append_value(match op {
                CompareOp::Eq => l == r,
                CompareOp::Neq => l != r,
                CompareOp::Lt => l < r,
                CompareOp::Gt => l > r,
                CompareOp::Lte => l <= r,
                CompareOp::Gte => l >= r,
            });
        }
    }
    builder.finish()
}

fn compare_float64(left: &Float64Array, op: CompareOp, right: &Float64Array) -> BooleanArray {
    let len = left.len();
    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let (l, r) = (left.value(i), right.value(i));
            builder.append_value(match op {
                CompareOp::Eq => l == r,
                CompareOp::Neq => l != r,
                CompareOp::Lt => l < r,
                CompareOp::Gt => l > r,
                CompareOp::Lte => l <= r,
                CompareOp::Gte => l >= r,
            });
        }
    }
    builder.finish()
}

fn compare_uint64(left: &UInt64Array, op: CompareOp, right: &UInt64Array) -> BooleanArray {
    let len = left.len();
    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let (l, r) = (left.value(i), right.value(i));
            builder.append_value(match op {
                CompareOp::Eq => l == r,
                CompareOp::Neq => l != r,
                CompareOp::Lt => l < r,
                CompareOp::Gt => l > r,
                CompareOp::Lte => l <= r,
                CompareOp::Gte => l >= r,
            });
        }
    }
    builder.finish()
}

fn compare_utf8(
    left: &arrow::array::StringArray,
    op: CompareOp,
    right: &arrow::array::StringArray,
) -> BooleanArray {
    let len = left.len();
    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let (l, r) = (left.value(i), right.value(i));
            builder.append_value(match op {
                CompareOp::Eq => l == r,
                CompareOp::Neq => l != r,
                CompareOp::Lt => l < r,
                CompareOp::Gt => l > r,
                CompareOp::Lte => l <= r,
                CompareOp::Gte => l >= r,
            });
        }
    }
    builder.finish()
}

fn compare_bool(left: &BooleanArray, op: CompareOp, right: &BooleanArray) -> BooleanArray {
    let len = left.len();
    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let (l, r) = (left.value(i), right.value(i));
            builder.append_value(match op {
                CompareOp::Eq => l == r,
                CompareOp::Neq => l != r,
                _ => false, // Bool ordering not meaningful
            });
        }
    }
    builder.finish()
}

fn promote_int64_to_float64(arr: &Int64Array) -> Float64Array {
    let mut builder = Float64Builder::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(arr.value(i) as f64);
        }
    }
    builder.finish()
}

/// Fallback comparison via GqlValue for mixed-type or Values columns.
fn compare_via_gql_value(
    left: &Column,
    right: &Column,
    op: CompareOp,
    len: usize,
    ctx: &EvalContext<'_>,
) -> Result<Column, GqlError> {
    use crate::types::chunk::column_to_gql_value_pub;
    use crate::types::coercion::coerce_for_comparison;
    use crate::types::trilean::Trilean;

    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        let lv = column_to_gql_value_pub(left, i);
        let rv = column_to_gql_value_pub(right, i);

        if lv.is_null() || rv.is_null() {
            builder.append_null();
            continue;
        }

        if ctx.options.strict_coercion {
            let needs_coercion = matches!(
                (&lv, &rv),
                (
                    GqlValue::String(_),
                    GqlValue::Int(_) | GqlValue::Float(_) | GqlValue::UInt(_)
                ) | (
                    GqlValue::Int(_) | GqlValue::Float(_) | GqlValue::UInt(_),
                    GqlValue::String(_)
                ) | (GqlValue::Bool(_), GqlValue::Int(_))
                    | (GqlValue::Int(_), GqlValue::Bool(_))
            );
            if needs_coercion {
                return Err(crate::types::coercion::strict_type_error(
                    &lv.gql_type().to_string(),
                    &rv.gql_type().to_string(),
                    "compare",
                ));
            }
        }

        let (cl, cr) = coerce_for_comparison(&lv, &rv);
        let result = match op {
            CompareOp::Eq => cl.gql_eq(&cr) == Trilean::True,
            CompareOp::Neq => cl.gql_eq(&cr) != Trilean::True,
            CompareOp::Lt => cl.gql_order(&cr).map(|o| o == std::cmp::Ordering::Less)?,
            CompareOp::Gt => cl
                .gql_order(&cr)
                .map(|o| o == std::cmp::Ordering::Greater)?,
            CompareOp::Lte => cl
                .gql_order(&cr)
                .map(|o| o != std::cmp::Ordering::Greater)?,
            CompareOp::Gte => cl.gql_order(&cr).map(|o| o != std::cmp::Ordering::Less)?,
        };
        builder.append_value(result);
    }
    Ok(Column::Bool(Arc::new(builder.finish())))
}

// ---------------------------------------------------------------------------
// Column-level logic (AND, OR, NOT)
// ---------------------------------------------------------------------------

fn eval_logic_columns(left: &Column, op: LogicOp, right: &Column, len: usize) -> Column {
    // Fast path: both sides are Bool columns
    if let (Column::Bool(la), Column::Bool(ra)) = (left, right) {
        let mut builder = BooleanBuilder::with_capacity(len);
        for i in 0..len {
            let l_null = la.is_null(i);
            let r_null = ra.is_null(i);

            match op {
                LogicOp::And => {
                    if !l_null && !la.value(i) {
                        // FALSE AND anything = FALSE
                        builder.append_value(false);
                    } else if !r_null && !ra.value(i) {
                        // anything AND FALSE = FALSE
                        builder.append_value(false);
                    } else if l_null || r_null {
                        builder.append_null(); // UNKNOWN
                    } else {
                        builder.append_value(la.value(i) && ra.value(i));
                    }
                }
                LogicOp::Or => {
                    if !l_null && la.value(i) {
                        // TRUE OR anything = TRUE
                        builder.append_value(true);
                    } else if !r_null && ra.value(i) {
                        // anything OR TRUE = TRUE
                        builder.append_value(true);
                    } else if l_null || r_null {
                        builder.append_null(); // UNKNOWN
                    } else {
                        builder.append_value(la.value(i) || ra.value(i));
                    }
                }
                LogicOp::Xor => {
                    if l_null || r_null {
                        builder.append_null();
                    } else {
                        builder.append_value(la.value(i) ^ ra.value(i));
                    }
                }
            }
        }
        return Column::Bool(Arc::new(builder.finish()));
    }

    // Fallback: mixed types, use per-element GqlValue logic
    eval_logic_via_gql_value(left, op, right, len)
}

fn eval_logic_via_gql_value(left: &Column, op: LogicOp, right: &Column, len: usize) -> Column {
    use crate::types::chunk::column_to_gql_value_pub;
    use crate::types::trilean::Trilean;

    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        let lv = value_to_trilean(&column_to_gql_value_pub(left, i));
        let rv = value_to_trilean(&column_to_gql_value_pub(right, i));

        let result = match op {
            LogicOp::And => lv.and(rv),
            LogicOp::Or => lv.or(rv),
            LogicOp::Xor => match (lv, rv) {
                (Trilean::True, Trilean::False) | (Trilean::False, Trilean::True) => Trilean::True,
                (Trilean::True, Trilean::True) | (Trilean::False, Trilean::False) => Trilean::False,
                _ => Trilean::Unknown,
            },
        };

        match result {
            Trilean::True => builder.append_value(true),
            Trilean::False => builder.append_value(false),
            Trilean::Unknown => builder.append_null(),
        }
    }
    Column::Bool(Arc::new(builder.finish()))
}

fn value_to_trilean(val: &GqlValue) -> crate::types::trilean::Trilean {
    use crate::types::trilean::Trilean;
    match val {
        GqlValue::Null => Trilean::Unknown,
        GqlValue::Bool(b) => Trilean::from(*b),
        _ => Trilean::Unknown,
    }
}

fn eval_not_column(col: &Column, len: usize) -> Column {
    if let Column::Bool(arr) = col {
        let mut builder = BooleanBuilder::with_capacity(len);
        for i in 0..len {
            if arr.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(!arr.value(i));
            }
        }
        return Column::Bool(Arc::new(builder.finish()));
    }

    // Fallback for non-Bool columns
    use crate::types::chunk::column_to_gql_value_pub;
    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        let v = value_to_trilean(&column_to_gql_value_pub(col, i));
        match v.not() {
            crate::types::trilean::Trilean::True => builder.append_value(true),
            crate::types::trilean::Trilean::False => builder.append_value(false),
            crate::types::trilean::Trilean::Unknown => builder.append_null(),
        }
    }
    Column::Bool(Arc::new(builder.finish()))
}

// ---------------------------------------------------------------------------
// Column-level arithmetic
// ---------------------------------------------------------------------------

fn eval_arithmetic_columns(
    left: &Column,
    op: ArithOp,
    right: &Column,
    len: usize,
    ctx: &EvalContext<'_>,
) -> Result<Column, GqlError> {
    // Fast path: both Int64
    if let (Column::Int64(la), Column::Int64(ra)) = (left, right) {
        return Ok(Column::Int64(Arc::new(arith_int64(la, op, ra)?)));
    }
    // Fast path: both Float64
    if let (Column::Float64(la), Column::Float64(ra)) = (left, right) {
        return Ok(Column::Float64(Arc::new(arith_float64(la, op, ra)?)));
    }
    // Promotion: Int64 vs Float64
    if let (Column::Int64(la), Column::Float64(ra)) = (left, right) {
        let promoted = promote_int64_to_float64(la);
        return Ok(Column::Float64(Arc::new(arith_float64(&promoted, op, ra)?)));
    }
    if let (Column::Float64(la), Column::Int64(ra)) = (left, right) {
        let promoted = promote_int64_to_float64(ra);
        return Ok(Column::Float64(Arc::new(arith_float64(la, op, &promoted)?)));
    }

    // Fallback: per-element via GqlValue
    arith_via_gql_value(left, op, right, len, ctx)
}

fn arith_int64(left: &Int64Array, op: ArithOp, right: &Int64Array) -> Result<Int64Array, GqlError> {
    let len = left.len();
    let mut builder = Int64Builder::with_capacity(len);
    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let (l, r) = (left.value(i), right.value(i));
            let result = match op {
                ArithOp::Add => l.checked_add(r),
                ArithOp::Sub => l.checked_sub(r),
                ArithOp::Mul => l.checked_mul(r),
                ArithOp::Div => {
                    if r == 0 {
                        return Err(GqlError::type_error("division by zero"));
                    }
                    l.checked_div(r)
                }
                ArithOp::Mod => {
                    if r == 0 {
                        return Err(GqlError::type_error("division by zero"));
                    }
                    l.checked_rem(r)
                }
            };
            match result {
                Some(v) => builder.append_value(v),
                None => {
                    return Err(GqlError::type_error(format!("overflow in {l} {op:?} {r}")));
                }
            }
        }
    }
    Ok(builder.finish())
}

fn arith_float64(
    left: &Float64Array,
    op: ArithOp,
    right: &Float64Array,
) -> Result<Float64Array, GqlError> {
    let len = left.len();
    let mut builder = Float64Builder::with_capacity(len);
    for i in 0..len {
        if left.is_null(i) || right.is_null(i) {
            builder.append_null();
        } else {
            let (l, r) = (left.value(i), right.value(i));
            let result = match op {
                ArithOp::Add => l + r,
                ArithOp::Sub => l - r,
                ArithOp::Mul => l * r,
                ArithOp::Div => {
                    if r == 0.0 {
                        return Err(GqlError::type_error("division by zero"));
                    }
                    l / r
                }
                ArithOp::Mod => {
                    if r == 0.0 {
                        return Err(GqlError::type_error("division by zero"));
                    }
                    l % r
                }
            };
            builder.append_value(result);
        }
    }
    Ok(builder.finish())
}

fn arith_via_gql_value(
    left: &Column,
    op: ArithOp,
    right: &Column,
    len: usize,
    ctx: &EvalContext<'_>,
) -> Result<Column, GqlError> {
    use crate::runtime::eval_arithmetic::eval_arithmetic;
    use crate::types::chunk::column_to_gql_value_pub;

    let mut values = Vec::with_capacity(len);
    for i in 0..len {
        let lv = column_to_gql_value_pub(left, i);
        let rv = column_to_gql_value_pub(right, i);
        if lv.is_null() || rv.is_null() {
            values.push(GqlValue::Null);
        } else {
            values.push(eval_arithmetic(&lv, op, &rv, ctx)?);
        }
    }
    Ok(Column::Values(Arc::from(values)))
}

fn eval_negate_column(col: &Column, len: usize) -> Result<Column, GqlError> {
    match col {
        Column::Int64(arr) => {
            let mut builder = Int64Builder::with_capacity(len);
            for i in 0..len {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    match arr.value(i).checked_neg() {
                        Some(v) => builder.append_value(v),
                        None => {
                            return Err(GqlError::type_error(format!(
                                "overflow negating {}",
                                arr.value(i)
                            )));
                        }
                    }
                }
            }
            Ok(Column::Int64(Arc::new(builder.finish())))
        }
        Column::Float64(arr) => {
            let mut builder = Float64Builder::with_capacity(len);
            for i in 0..len {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(-arr.value(i));
                }
            }
            Ok(Column::Float64(Arc::new(builder.finish())))
        }
        Column::Null(n) => Ok(Column::Null(*n)),
        _ => Err(GqlError::Unsupported {
            feature: "eval_vec: negate on non-numeric column".into(),
        }),
    }
}

// ---------------------------------------------------------------------------
// IN list
// ---------------------------------------------------------------------------

fn eval_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    chunk: &DataChunk,
    gatherer: &dyn PropertyGatherer,
    ctx: &EvalContext<'_>,
    _sel: Option<&[u32]>,
) -> Result<Column, GqlError> {
    let len = chunk.len();
    let val_col = eval_vec(expr, chunk, gatherer, ctx)?;

    // Evaluate list elements (they may be constants or expressions)
    let list_cols: Vec<Column> = list
        .iter()
        .map(|e| eval_vec(e, chunk, gatherer, ctx))
        .collect::<Result<_, _>>()?;

    use crate::types::chunk::column_to_gql_value_pub;
    use crate::types::trilean::Trilean;

    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        let v = column_to_gql_value_pub(&val_col, i);
        if v.is_null() {
            builder.append_null();
            continue;
        }
        let mut found = false;
        let mut has_unknown = false;
        for lc in &list_cols {
            let item = column_to_gql_value_pub(lc, i);
            match v.gql_eq(&item) {
                Trilean::True => {
                    found = true;
                    break;
                }
                Trilean::Unknown => has_unknown = true,
                Trilean::False => {}
            }
        }
        if found {
            builder.append_value(!negated);
        } else if has_unknown {
            builder.append_null();
        } else {
            builder.append_value(negated);
        }
    }
    Ok(Column::Bool(Arc::new(builder.finish())))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::functions::FunctionRegistry;
    use crate::types::chunk::{ChunkSchema, ColumnBuilder, ColumnKind, DataChunk};
    use selene_core::{IStr as CoreIStr, LabelSet, NodeId, PropertyMap, Value};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;

    fn test_graph() -> SeleneGraph {
        let mut graph = SeleneGraph::new();
        let mut m = graph.mutate();

        m.create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![
                (CoreIStr::new("name"), Value::String(SmolStr::new("alice"))),
                (CoreIStr::new("score"), Value::Int(100)),
            ]),
        )
        .unwrap();

        m.create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![
                (CoreIStr::new("name"), Value::String(SmolStr::new("bob"))),
                (CoreIStr::new("score"), Value::Int(200)),
            ]),
        )
        .unwrap();

        m.create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![
                (CoreIStr::new("name"), Value::String(SmolStr::new("carol"))),
                (CoreIStr::new("score"), Value::Int(300)),
            ]),
        )
        .unwrap();

        m.commit(0).unwrap();
        graph
    }

    fn make_node_chunk() -> DataChunk {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("n"), ColumnKind::NodeId);

        let mut b = ColumnBuilder::new_node_ids(3);
        b.append_node_id(NodeId(1));
        b.append_node_id(NodeId(2));
        b.append_node_id(NodeId(3));

        DataChunk::from_builders(vec![b], schema, 3)
    }

    fn make_int_chunk() -> DataChunk {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("x"), ColumnKind::Int64);

        let mut b = ColumnBuilder::new_int64(4);
        b.append_gql_value(&GqlValue::Int(10));
        b.append_gql_value(&GqlValue::Int(20));
        b.append_gql_value(&GqlValue::Int(30));
        b.append_gql_value(&GqlValue::Int(40));

        DataChunk::from_builders(vec![b], schema, 4)
    }

    #[test]
    fn eval_vec_literal() {
        let chunk = make_int_chunk();
        let graph = SeleneGraph::new();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        let col = eval_vec(&Expr::Literal(GqlValue::Int(42)), &chunk, &gatherer, &ctx).unwrap();

        match &col {
            Column::Int64(arr) => {
                assert_eq!(arr.len(), 4);
                assert!(arr.iter().all(|v| v == Some(42)));
            }
            other => panic!("expected Int64, got {:?}", other.kind()),
        }
    }

    #[test]
    fn eval_vec_var() {
        let chunk = make_int_chunk();
        let graph = SeleneGraph::new();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        let col = eval_vec(&Expr::Var(IStr::new("x")), &chunk, &gatherer, &ctx).unwrap();

        match &col {
            Column::Int64(arr) => {
                assert_eq!(arr.value(0), 10);
                assert_eq!(arr.value(3), 40);
            }
            other => panic!("expected Int64, got {:?}", other.kind()),
        }
    }

    #[test]
    fn eval_vec_property_access() {
        let graph = test_graph();
        let chunk = make_node_chunk();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        // n.score
        let expr = Expr::Property(Box::new(Expr::Var(IStr::new("n"))), IStr::new("score"));
        let col = eval_vec(&expr, &chunk, &gatherer, &ctx).unwrap();

        match &col {
            Column::Int64(arr) => {
                assert_eq!(arr.value(0), 100);
                assert_eq!(arr.value(1), 200);
                assert_eq!(arr.value(2), 300);
            }
            other => panic!("expected Int64, got {:?}", other.kind()),
        }
    }

    #[test]
    fn eval_vec_compare_int_gt() {
        let graph = test_graph();
        let chunk = make_node_chunk();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        // n.score > 150
        let expr = Expr::Compare(
            Box::new(Expr::Property(
                Box::new(Expr::Var(IStr::new("n"))),
                IStr::new("score"),
            )),
            CompareOp::Gt,
            Box::new(Expr::Literal(GqlValue::Int(150))),
        );
        let col = eval_vec(&expr, &chunk, &gatherer, &ctx).unwrap();

        match &col {
            Column::Bool(arr) => {
                assert!(!arr.value(0)); // 100 > 150 = false
                assert!(arr.value(1)); // 200 > 150 = true
                assert!(arr.value(2)); // 300 > 150 = true
            }
            other => panic!("expected Bool, got {:?}", other.kind()),
        }
    }

    #[test]
    fn eval_vec_logic_and() {
        let chunk = make_int_chunk(); // x = [10, 20, 30, 40]
        let graph = SeleneGraph::new();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        // x > 15 AND x < 35
        let expr = Expr::Logic(
            Box::new(Expr::Compare(
                Box::new(Expr::Var(IStr::new("x"))),
                CompareOp::Gt,
                Box::new(Expr::Literal(GqlValue::Int(15))),
            )),
            LogicOp::And,
            Box::new(Expr::Compare(
                Box::new(Expr::Var(IStr::new("x"))),
                CompareOp::Lt,
                Box::new(Expr::Literal(GqlValue::Int(35))),
            )),
        );
        let col = eval_vec(&expr, &chunk, &gatherer, &ctx).unwrap();

        match &col {
            Column::Bool(arr) => {
                assert!(!arr.value(0)); // 10: false AND true = false
                assert!(arr.value(1)); // 20: true AND true = true
                assert!(arr.value(2)); // 30: true AND true = true
                assert!(!arr.value(3)); // 40: true AND false = false
            }
            other => panic!("expected Bool, got {:?}", other.kind()),
        }
    }

    #[test]
    fn eval_vec_arithmetic() {
        let chunk = make_int_chunk(); // x = [10, 20, 30, 40]
        let graph = SeleneGraph::new();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        // x + 5
        let expr = Expr::Arithmetic(
            Box::new(Expr::Var(IStr::new("x"))),
            ArithOp::Add,
            Box::new(Expr::Literal(GqlValue::Int(5))),
        );
        let col = eval_vec(&expr, &chunk, &gatherer, &ctx).unwrap();

        match &col {
            Column::Int64(arr) => {
                assert_eq!(arr.value(0), 15);
                assert_eq!(arr.value(1), 25);
                assert_eq!(arr.value(2), 35);
                assert_eq!(arr.value(3), 45);
            }
            other => panic!("expected Int64, got {:?}", other.kind()),
        }
    }

    #[test]
    fn eval_vec_is_null() {
        let mut schema = ChunkSchema::new();
        schema.extend(IStr::new("v"), ColumnKind::Values);

        let col = Column::Values(Arc::from(vec![
            GqlValue::Int(1),
            GqlValue::Null,
            GqlValue::Int(3),
        ]));
        let chunk = DataChunk::from_columns(smallvec::smallvec![col], schema, 3);
        let graph = SeleneGraph::new();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        let expr = Expr::IsNull {
            expr: Box::new(Expr::Var(IStr::new("v"))),
            negated: false,
        };
        let result = eval_vec(&expr, &chunk, &gatherer, &ctx).unwrap();

        match &result {
            Column::Bool(arr) => {
                assert!(!arr.value(0));
                assert!(arr.value(1));
                assert!(!arr.value(2));
            }
            other => panic!("expected Bool, got {:?}", other.kind()),
        }
    }

    #[test]
    fn eval_vec_in_list() {
        let chunk = make_int_chunk(); // x = [10, 20, 30, 40]
        let graph = SeleneGraph::new();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        // x IN [20, 40]
        let expr = Expr::InList {
            expr: Box::new(Expr::Var(IStr::new("x"))),
            list: vec![
                Expr::Literal(GqlValue::Int(20)),
                Expr::Literal(GqlValue::Int(40)),
            ],
            negated: false,
        };
        let col = eval_vec(&expr, &chunk, &gatherer, &ctx).unwrap();

        match &col {
            Column::Bool(arr) => {
                assert!(!arr.value(0)); // 10 not in list
                assert!(arr.value(1)); // 20 in list
                assert!(!arr.value(2)); // 30 not in list
                assert!(arr.value(3)); // 40 in list
            }
            other => panic!("expected Bool, got {:?}", other.kind()),
        }
    }

    #[test]
    fn eval_vec_unsupported_returns_error() {
        let chunk = make_int_chunk();
        let graph = SeleneGraph::new();
        let gatherer = super::super::gather::GraphPropertyGatherer::new(&graph);
        let functions = FunctionRegistry::new();
        let ctx = EvalContext::new(&graph, &functions);

        // Function calls are unsupported in eval_vec
        let expr = Expr::Function(crate::ast::expr::FunctionCall {
            name: IStr::new("upper"),
            args: vec![Expr::Literal(GqlValue::String("test".into()))],
            count_star: false,
        });
        let result = eval_vec(&expr, &chunk, &gatherer, &ctx);
        assert!(matches!(result, Err(GqlError::Unsupported { .. })));
    }
}
