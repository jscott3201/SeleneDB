//! Expression evaluator: evaluates `Expr` AST nodes against bindings and the graph.
//!
//! All evaluation uses three-valued logic (Trilean) for comparisons and
//! null propagation. Property access on Node/Edge bindings resolves lazily
//! from the pinned graph snapshot.

use std::sync::Arc;

use selene_core::IStr;
use selene_graph::SeleneGraph;
use smol_str::SmolStr;

use crate::ast::expr::*;
use crate::runtime::functions::FunctionRegistry;
use crate::types::binding::{Binding, BoundValue};
use crate::types::error::GqlError;
use crate::types::trilean::Trilean;
use crate::types::value::{GqlList, GqlType, GqlValue, ZonedDateTime};

/// Trait for resolving temporal property values (decouples eval from VersionStore).
pub trait TemporalResolver: Send + Sync {
    /// Get the property value at a specific point in time.
    fn value_at(
        &self,
        node_id: selene_core::NodeId,
        key: &str,
        timestamp_nanos: i64,
        graph: &SeleneGraph,
    ) -> Option<selene_core::Value>;
}

/// Maximum expression evaluation depth before returning an error.
const MAX_EVAL_DEPTH: u16 = 256;

/// Evaluation context bundling graph snapshot, function registry, and query parameters.
/// Passed through all eval functions instead of individual parameters.
pub struct EvalContext<'a> {
    pub graph: &'a SeleneGraph,
    pub functions: &'a FunctionRegistry,
    pub parameters: Option<&'a std::collections::HashMap<IStr, crate::types::value::GqlValue>>,
    /// Optional temporal resolver for AT TIME property access.
    pub temporal: Option<&'a dyn TemporalResolver>,
    /// Execution options (strict coercion mode, etc.).
    pub options: crate::GqlOptions,
    /// Auth scope bitmap restricting visible nodes/edges. Propagated into subqueries.
    pub scope: Option<&'a roaring::RoaringBitmap>,
    /// Current expression evaluation depth (guards against stack overflow from deep nesting).
    pub depth: u16,
}

impl<'a> EvalContext<'a> {
    pub fn new(graph: &'a SeleneGraph, functions: &'a FunctionRegistry) -> Self {
        Self {
            graph,
            functions,
            parameters: None,
            temporal: None,
            options: crate::GqlOptions::default(),
            scope: None,
            depth: 0,
        }
    }

    /// Builder: set query parameters for `$param` resolution.
    pub fn with_parameters(
        mut self,
        parameters: &'a std::collections::HashMap<IStr, crate::types::value::GqlValue>,
    ) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Builder: set execution options (strict coercion, etc.).
    pub fn with_options(mut self, options: crate::GqlOptions) -> Self {
        self.options = options;
        self
    }

    /// Builder: set auth scope for scope-aware evaluation.
    pub fn with_scope(mut self, scope: Option<&'a roaring::RoaringBitmap>) -> Self {
        self.scope = scope;
        self
    }
}

/// Evaluate an expression against a binding and graph, producing a GqlValue.
/// Uses the static builtin function registry (OnceLock, zero per-call cost).
/// Prefer eval_expr_ctx with an explicit EvalContext for hot paths.
pub(crate) fn eval_expr(
    expr: &Expr,
    binding: &Binding,
    graph: &SeleneGraph,
) -> Result<GqlValue, GqlError> {
    let ctx = EvalContext::new(graph, FunctionRegistry::builtins());
    eval_expr_ctx(expr, binding, &ctx)
}

/// Evaluate an expression for a single row of a `DataChunk`.
///
/// Phase 1 adapter: materializes a `Binding` from the `RowView`, then
/// delegates to `eval_expr_ctx`. Phase 2 replaces this with batch `eval_vec`.
pub(crate) fn eval_expr_row(
    expr: &Expr,
    row: &crate::types::chunk::RowView<'_>,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let binding = row.to_binding();
    eval_expr_ctx(expr, &binding, ctx)
}

/// Evaluate a predicate for a single row of a `DataChunk`, returning Trilean.
pub(crate) fn eval_predicate_row(
    expr: &Expr,
    row: &crate::types::chunk::RowView<'_>,
    ctx: &EvalContext<'_>,
) -> Result<Trilean, GqlError> {
    let binding = row.to_binding();
    eval_predicate(expr, &binding, ctx)
}

/// Evaluate an expression with an explicit EvalContext.
pub(crate) fn eval_expr_ctx(
    expr: &Expr,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    // Leaf expressions: no recursion possible, skip depth tracking
    match expr {
        Expr::Literal(_) | Expr::Var(_) | Expr::Parameter(_) => {
            return eval_expr_inner(expr, binding, ctx);
        }
        _ => {}
    }
    // Recursive expressions: check and increment depth
    if ctx.depth >= MAX_EVAL_DEPTH {
        return Err(GqlError::ResourcesExhausted {
            message: "expression evaluation depth exceeds 256".into(),
        });
    }
    let child_ctx = EvalContext {
        graph: ctx.graph,
        functions: ctx.functions,
        parameters: ctx.parameters,
        temporal: ctx.temporal,
        options: ctx.options.clone(),
        scope: ctx.scope,
        depth: ctx.depth + 1,
    };
    eval_expr_inner(expr, binding, &child_ctx)
}

/// Inner expression evaluator, called by `eval_expr_ctx` after depth check.
fn eval_expr_inner(
    expr: &Expr,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    match expr {
        Expr::Literal(val) => Ok(val.clone()),

        Expr::Var(name) => resolve_var(*name, binding),

        Expr::ListConstruct(elements) => eval_list_construct(elements, binding, ctx),

        Expr::Property(target, key) => {
            let target_val = eval_expr_ctx(target, binding, ctx)?;
            resolve_property(&target_val, *key, ctx.graph)
        }

        Expr::TemporalProperty(target, key, timestamp_str) => {
            eval_temporal_property(target, *key, timestamp_str, binding, ctx)
        }

        Expr::ListAccess(list_expr, index_expr) => {
            let list = eval_expr_ctx(list_expr, binding, ctx)?;
            let index = eval_expr_ctx(index_expr, binding, ctx)?;
            eval_list_access(&list, &index)
        }

        Expr::Compare(left, op, right) => {
            let lv = eval_expr_ctx(left, binding, ctx)?;
            let rv = eval_expr_ctx(right, binding, ctx)?;
            eval_compare(&lv, *op, &rv, ctx)
        }

        Expr::Arithmetic(left, op, right) => {
            let lv = eval_expr_ctx(left, binding, ctx)?;
            let rv = eval_expr_ctx(right, binding, ctx)?;
            eval_arithmetic(&lv, *op, &rv, ctx)
        }

        Expr::Logic(left, op, right) => eval_logic(left, *op, right, binding, ctx),

        Expr::Not(inner) => {
            let v = eval_predicate(inner, binding, ctx)?;
            Ok(trilean_to_value(v.not()))
        }

        Expr::Negate(inner) => {
            let v = eval_expr_ctx(inner, binding, ctx)?;
            eval_negate(&v)
        }

        Expr::Concat(left, right) => {
            let lv = eval_expr_ctx(left, binding, ctx)?;
            let rv = eval_expr_ctx(right, binding, ctx)?;
            eval_concat(&lv, &rv, ctx)
        }

        Expr::StringMatch(haystack, op, needle) => {
            let hv = eval_expr_ctx(haystack, binding, ctx)?;
            let nv = eval_expr_ctx(needle, binding, ctx)?;
            eval_string_match(&hv, *op, &nv)
        }

        Expr::IsNull { expr, negated } => {
            let v = eval_expr_ctx(expr, binding, ctx)?;
            let is_null = v.is_null();
            Ok(GqlValue::Bool(if *negated { !is_null } else { is_null }))
        }

        Expr::InList {
            expr,
            list,
            negated,
        } => eval_in_list(expr, list, *negated, binding, ctx),

        Expr::Function(call) => eval_function(call, binding, ctx),

        Expr::Cast(inner, target_type) => {
            let v = eval_expr_ctx(inner, binding, ctx)?;
            eval_cast(&v, target_type)
        }

        Expr::Aggregate(agg) => eval_aggregate_expr(agg, binding, ctx),

        Expr::Labels(inner) => {
            let v = eval_expr_ctx(inner, binding, ctx)?;
            eval_labels(&v, ctx.graph)
        }

        Expr::AllDifferent(exprs) => eval_all_different(exprs, binding, ctx),

        Expr::Same(exprs) => eval_same(exprs, binding, ctx),

        Expr::PropertyExists(expr, key) => eval_property_exists(expr, *key, binding, ctx),

        Expr::Exists { pattern, negated } => eval_exists(pattern, *negated, ctx),

        Expr::IsLabeled {
            expr,
            label,
            negated,
        } => eval_is_labeled(expr, label, *negated, binding, ctx),

        Expr::Trim {
            source,
            character,
            spec,
        } => eval_trim(source, character.as_deref(), *spec, binding, ctx),

        Expr::RecordConstruct(fields) => eval_record_construct(fields, binding, ctx),

        Expr::IsTyped {
            expr,
            type_name,
            negated,
        } => eval_is_typed(expr, type_name, *negated, binding, ctx),

        Expr::IsNormalized {
            expr,
            form,
            negated,
        } => eval_is_normalized(expr, *form, *negated, binding, ctx),

        Expr::IsDirected { expr, negated } => {
            let val = eval_expr_ctx(expr, binding, ctx)?;
            let result = matches!(val, GqlValue::Edge(_)); // all Selene edges are directed
            Ok(GqlValue::Bool(if *negated { !result } else { result }))
        }

        Expr::IsTruthValue {
            expr,
            value,
            negated,
        } => eval_is_truth_value(expr, *value, *negated, binding, ctx),

        Expr::IsSourceOf {
            node,
            edge,
            negated,
        } => eval_edge_endpoint(node, edge, *negated, true, binding, ctx),

        Expr::IsDestinationOf {
            node,
            edge,
            negated,
        } => eval_edge_endpoint(node, edge, *negated, false, binding, ctx),

        Expr::Parameter(name) => eval_parameter(*name, ctx),

        Expr::Case {
            branches,
            else_expr,
        } => eval_case(branches, else_expr.as_deref(), binding, ctx),

        // Note: ProcedureCall is handled by PipelineStatement::Call,
        // not as an expression. It does not appear as an Expr variant.
        Expr::ValueSubquery(pipeline) => eval_value_subquery(pipeline, ctx),

        Expr::CollectSubquery(pipeline) => eval_collect_subquery(pipeline, ctx),

        Expr::Like {
            expr,
            pattern,
            negated,
        } => eval_like(expr, pattern, *negated, binding, ctx),

        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => eval_between(expr, low, high, *negated, binding, ctx),

        Expr::CountSubquery(pattern) => eval_count_subquery(pattern, binding, ctx),
    }
}

// ── Extracted eval helpers (moved to eval_helpers.rs) ──────────────────────
use super::eval_helpers::{
    eval_aggregate_expr, eval_all_different, eval_between, eval_case, eval_collect_subquery,
    eval_count_subquery, eval_edge_endpoint, eval_exists, eval_in_list, eval_is_labeled,
    eval_is_normalized, eval_is_truth_value, eval_is_typed, eval_like, eval_list_construct,
    eval_logic, eval_parameter, eval_property_exists, eval_record_construct, eval_same,
    eval_temporal_property, eval_trim, eval_value_subquery,
};

/// Evaluate an expression as a Trilean predicate (for FILTER, WHERE).
/// Returns Unknown for NULL values, True/False for booleans.
pub(crate) fn eval_predicate(
    expr: &Expr,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<Trilean, GqlError> {
    let val = eval_expr_ctx(expr, binding, ctx)?;
    Ok(value_to_trilean(&val))
}

/// Convert a GqlValue to Trilean for predicate context.
fn value_to_trilean(val: &GqlValue) -> Trilean {
    match val {
        GqlValue::Null => Trilean::Unknown,
        GqlValue::Bool(b) => Trilean::from(*b),
        // Non-boolean values → UNKNOWN in predicate context.
        // FILTER only passes TRUE, so non-boolean expressions are filtered out.
        // This is GQL-spec compliant: predicates must be boolean.
        _ => Trilean::Unknown,
    }
}

/// Convert Trilean back to GqlValue.
pub(crate) fn trilean_to_value(t: Trilean) -> GqlValue {
    match t {
        Trilean::True => GqlValue::Bool(true),
        Trilean::False => GqlValue::Bool(false),
        Trilean::Unknown => GqlValue::Null,
    }
}

// ── Variable resolution ────────────────────────────────────────────

/// Resolve a variable to a GqlValue. Used by GROUP BY key extraction.
/// Resolves Node/Edge properties to their values, scalars directly.
#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn resolve_var_as_value(
    name: &IStr,
    binding: &Binding,
    _ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    resolve_var(*name, binding)
}

/// Resolve a variable name from a binding.
fn resolve_var(name: IStr, binding: &Binding) -> Result<GqlValue, GqlError> {
    match binding.get(&name) {
        Some(BoundValue::Node(id)) => Ok(GqlValue::Node(*id)),
        Some(BoundValue::Edge(id)) => Ok(GqlValue::Edge(*id)),
        Some(BoundValue::Scalar(val)) => Ok(val.clone()),
        Some(BoundValue::Path(path)) => Ok(GqlValue::Path(path.clone())),
        Some(BoundValue::Group(edges)) => {
            let elements: Arc<[GqlValue]> = edges.iter().map(|e| GqlValue::Edge(*e)).collect();
            Ok(GqlValue::List(GqlList {
                element_type: GqlType::Edge,
                elements,
            }))
        }
        None => Err(GqlError::internal(format!("unbound variable '{name}'"))),
    }
}

// ── Property resolution (lazy from graph) ──────────────────────────

/// Resolve a property on a GqlValue.
/// For Node/Edge, looks up the property from the graph snapshot.
pub(crate) fn resolve_property(
    target: &GqlValue,
    key: IStr,
    graph: &SeleneGraph,
) -> Result<GqlValue, GqlError> {
    match target {
        GqlValue::Node(node_id) => {
            // Special properties
            if key.as_str() == "id" {
                return Ok(GqlValue::UInt(node_id.0));
            }
            // Look up from graph
            match graph.get_node(*node_id) {
                Some(node) => match node.properties.get(key) {
                    Some(val) => Ok(GqlValue::from(val)),
                    None => {
                        // Fall back to schema default for lazy migration support.
                        // Avoids double node lookup by using the node we already have.
                        match graph.schema().property_default(node.labels, key) {
                            Some(val) => Ok(GqlValue::from(&val)),
                            None => Ok(GqlValue::Null),
                        }
                    }
                },
                None => Ok(GqlValue::Null), // Deleted node -> NULL
            }
        }
        GqlValue::Edge(edge_id) => {
            // Special properties
            if key.as_str() == "id" {
                return Ok(GqlValue::UInt(edge_id.0));
            }
            match graph.get_edge(*edge_id) {
                Some(edge) => match edge.properties.get(key) {
                    Some(val) => Ok(GqlValue::from(val)),
                    None => Ok(GqlValue::Null),
                },
                None => Ok(GqlValue::Null),
            }
        }
        GqlValue::Record(record) => {
            // Record field access: record.field_name
            Ok(record.get(key.as_str()).cloned().unwrap_or(GqlValue::Null))
        }
        GqlValue::List(list) => {
            // Property access on List<Edge> -> List<prop values> (horizontal aggregation)
            let values: Vec<GqlValue> = list
                .elements
                .iter()
                .map(|elem| resolve_property(elem, key, graph).unwrap_or(GqlValue::Null))
                .collect();
            let element_type = crate::types::value::infer_list_element_type(&values);
            Ok(GqlValue::List(GqlList {
                element_type,
                elements: Arc::from(values),
            }))
        }
        GqlValue::Null => Ok(GqlValue::Null), // NULL.prop -> NULL
        _ => Err(GqlError::type_error(format!(
            "cannot access property '{}' on {}",
            key,
            target.gql_type()
        ))),
    }
}

// ── Comparison ─────────────────────────────────────────────────────

fn eval_compare(
    left: &GqlValue,
    op: CompareOp,
    right: &GqlValue,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    // Three-valued: any NULL -> return NULL (UNKNOWN)
    if left.is_null() || right.is_null() {
        return Ok(GqlValue::Null);
    }

    // Implicit type coercion for cross-type comparisons
    // (e.g., String "72" vs Int 72, Bool true vs Int 1)
    if ctx.options.strict_coercion {
        // In strict mode, reject cross-type comparison without explicit CAST
        let lt = left.gql_type().to_string();
        let rt = right.gql_type().to_string();
        let needs_coercion = matches!(
            (left, right),
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
                &lt, &rt, "compare",
            ));
        }
    }
    let (left, right) = crate::types::coercion::coerce_for_comparison(left, right);

    match op {
        CompareOp::Eq => Ok(trilean_to_value(left.gql_eq(&right))),
        CompareOp::Neq => Ok(trilean_to_value(left.gql_eq(&right).not())),
        CompareOp::Lt | CompareOp::Gt | CompareOp::Lte | CompareOp::Gte => {
            let ord = left.gql_order(&right)?;
            let result = match op {
                CompareOp::Lt => ord == std::cmp::Ordering::Less,
                CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                CompareOp::Lte => ord != std::cmp::Ordering::Greater,
                CompareOp::Gte => ord != std::cmp::Ordering::Less,
                _ => unreachable!(),
            };
            Ok(GqlValue::Bool(result))
        }
    }
}

// ── Arithmetic (delegated to eval_arithmetic module) ───────────────

use super::eval_arithmetic::{eval_arithmetic, eval_cast, eval_negate};

// ── String operations ──────────────────────────────────────────────

fn eval_concat(
    left: &GqlValue,
    right: &GqlValue,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    if left.is_null() || right.is_null() {
        return Ok(GqlValue::Null);
    }
    // In strict mode, both sides must be strings
    if ctx.options.strict_coercion {
        let l_is_str = matches!(left, GqlValue::String(_));
        let r_is_str = matches!(right, GqlValue::String(_));
        if !l_is_str || !r_is_str {
            return Err(crate::types::coercion::strict_type_error(
                &left.gql_type().to_string(),
                &right.gql_type().to_string(),
                "concatenate",
            ));
        }
    }
    // Coerce both to string representation
    let l = value_to_string(left);
    let r = value_to_string(right);
    Ok(GqlValue::String(SmolStr::new(format!("{l}{r}").as_str())))
}

fn eval_string_match(
    haystack: &GqlValue,
    op: StringMatchOp,
    needle: &GqlValue,
) -> Result<GqlValue, GqlError> {
    if haystack.is_null() || needle.is_null() {
        return Ok(GqlValue::Null);
    }
    let h = haystack.as_str()?;
    let n = needle.as_str()?;
    let result = match op {
        StringMatchOp::Contains => h.contains(n),
        StringMatchOp::StartsWith => h.starts_with(n),
        StringMatchOp::EndsWith => h.ends_with(n),
    };
    Ok(GqlValue::Bool(result))
}

/// Convert a value to its string representation for concatenation.
pub(crate) fn value_to_string(val: &GqlValue) -> String {
    match val {
        GqlValue::String(s) => s.to_string(),
        GqlValue::Int(i) => i.to_string(),
        GqlValue::UInt(u) => u.to_string(),
        GqlValue::Float(f) => f.to_string(),
        GqlValue::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
        other => format!("{other}"),
    }
}

// ── List access ────────────────────────────────────────────────────

fn eval_list_access(list: &GqlValue, index: &GqlValue) -> Result<GqlValue, GqlError> {
    if list.is_null() || index.is_null() {
        return Ok(GqlValue::Null);
    }
    let idx_i64 = index.as_int()?;
    if idx_i64 < 0 {
        return Ok(GqlValue::Null); // Negative index → NULL (out of bounds)
    }
    let idx = idx_i64 as usize;
    match list {
        GqlValue::List(l) => {
            if idx < l.elements.len() {
                Ok(l.elements[idx].clone())
            } else {
                Ok(GqlValue::Null) // Out of bounds → NULL
            }
        }
        _ => Err(GqlError::type_error(format!(
            "cannot index into {}",
            list.gql_type()
        ))),
    }
}

// ── Function evaluation ────────────────────────────────────────────

fn eval_function(
    call: &FunctionCall,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    if call.count_star {
        return Ok(GqlValue::Int(1));
    }

    let args: Vec<GqlValue> = call
        .args
        .iter()
        .map(|a| eval_expr_ctx(a, binding, ctx))
        .collect::<Result<_, _>>()?;

    // Try the function registry
    if let Some(func) = ctx.functions.get(&call.name) {
        return func.invoke(&args, ctx);
    }

    // Fallback for functions not yet in the registry
    let name = call.name.as_str();
    match name {
        "coalesce" => {
            for arg in &args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(GqlValue::Null)
        }
        "char_length" => {
            if args.is_empty() {
                return Err(GqlError::InvalidArgument {
                    message: "char_length requires 1 argument".into(),
                });
            }
            match &args[0] {
                GqlValue::Null => Ok(GqlValue::Null),
                GqlValue::String(s) => Ok(GqlValue::Int(s.chars().count() as i64)),
                _ => Err(GqlError::type_error("char_length requires a string")),
            }
        }
        "upper" => match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => {
                Ok(GqlValue::String(SmolStr::new(s.to_uppercase().as_str())))
            }
            _ => Err(GqlError::type_error("upper requires a string")),
        },
        "lower" => match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => {
                Ok(GqlValue::String(SmolStr::new(s.to_lowercase().as_str())))
            }
            _ => Err(GqlError::type_error("lower requires a string")),
        },
        "trim" => match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => Ok(GqlValue::String(SmolStr::new(s.trim()))),
            _ => Err(GqlError::type_error("trim requires a string")),
        },
        "size" => match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::List(l)) => Ok(GqlValue::Int(l.len() as i64)),
            Some(GqlValue::Path(p)) => Ok(GqlValue::Int(p.edge_count() as i64)),
            _ => Err(GqlError::type_error("size requires a list or path")),
        },
        "duration" => {
            // Parse duration string to GqlDuration (was incorrectly returning ZonedDateTime)
            match args.first() {
                Some(GqlValue::String(s)) => {
                    let nanos = parse_duration(s)?;
                    Ok(GqlValue::Duration(
                        crate::types::value::GqlDuration::day_time(nanos),
                    ))
                }
                Some(GqlValue::Null) | None => Ok(GqlValue::Null),
                _ => Err(GqlError::type_error("duration requires a string")),
            }
        }
        "zoned_datetime" => match args.first() {
            Some(GqlValue::String(s)) => super::functions::parse_iso8601(s),
            None | Some(_) => Ok(GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                selene_core::now_nanos(),
            ))),
        },
        _ => Err(GqlError::UnknownProcedure {
            name: name.to_string(),
        }),
    }
}

// ── LABELS ─────────────────────────────────────────────────────────

fn eval_labels(val: &GqlValue, graph: &SeleneGraph) -> Result<GqlValue, GqlError> {
    match val {
        GqlValue::Node(node_id) => match graph.get_node(*node_id) {
            Some(node) => {
                let labels: Arc<[GqlValue]> = node
                    .labels
                    .iter()
                    .map(|l| GqlValue::String(SmolStr::new(l.as_str())))
                    .collect();
                Ok(GqlValue::List(GqlList {
                    element_type: GqlType::String,
                    elements: labels,
                }))
            }
            None => Ok(GqlValue::Null),
        },
        GqlValue::Edge(edge_id) => match graph.get_edge(*edge_id) {
            Some(edge) => Ok(GqlValue::List(GqlList {
                element_type: GqlType::String,
                elements: Arc::from(vec![GqlValue::String(SmolStr::new(edge.label.as_str()))]),
            })),
            None => Ok(GqlValue::Null),
        },
        GqlValue::Null => Ok(GqlValue::Null),
        _ => Err(GqlError::type_error(format!(
            "LABELS requires a node or edge, got {}",
            val.gql_type()
        ))),
    }
}

// Re-export items from eval_aggregate used by other modules in this crate.
pub(crate) use super::eval_aggregate::eval_aggregate;

// Re-export items from eval_arithmetic used by other modules in this crate.
pub(crate) use super::eval_arithmetic::{parse_duration, ymd_to_epoch_days};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::functions::FunctionRegistry;
    use selene_core::{LabelSet, NodeId, PropertyMap, Value};
    use selene_graph::SeleneGraph;
    use std::sync::Arc;

    fn make_ctx(graph: &SeleneGraph) -> EvalContext<'_> {
        EvalContext::new(graph, FunctionRegistry::builtins())
    }

    fn test_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        let props = PropertyMap::from_pairs(vec![
            (IStr::new("name"), Value::String(SmolStr::new("TempSensor"))),
            (IStr::new("temp"), Value::Float(72.5)),
            (IStr::new("active"), Value::Bool(true)),
        ]);
        m.create_node(LabelSet::from_strs(&["sensor"]), props)
            .unwrap();
        m.commit(0).unwrap();
        g
    }

    fn binding_with_node(var: &str, id: u64) -> Binding {
        Binding::single(IStr::new(var), BoundValue::Node(NodeId(id)))
    }

    // ── Literal evaluation ──

    #[test]
    fn eval_literal_int() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Literal(GqlValue::Int(42));
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Int(42));
    }

    #[test]
    fn eval_literal_null() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Literal(GqlValue::Null);
        assert!(eval_expr(&expr, &b, &g).unwrap().is_null());
    }

    // ── Variable resolution ──

    #[test]
    fn eval_var_node() {
        let g = test_graph();
        let b = binding_with_node("s", 1);
        let expr = Expr::Var(IStr::new("s"));
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Node(NodeId(1)));
    }

    #[test]
    fn eval_var_scalar() {
        let g = SeleneGraph::new();
        let mut b = Binding::empty();
        b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Int(42)));
        let expr = Expr::Var(IStr::new("x"));
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Int(42));
    }

    // ── Property access ──

    #[test]
    fn eval_property_name() {
        let g = test_graph();
        let b = binding_with_node("s", 1);
        let expr = Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("name"));
        match eval_expr(&expr, &b, &g).unwrap() {
            GqlValue::String(s) => assert_eq!(&*s, "TempSensor"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn eval_property_float() {
        let g = test_graph();
        let b = binding_with_node("s", 1);
        let expr = Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("temp"));
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Float(72.5));
    }

    #[test]
    fn eval_property_missing_is_null() {
        let g = test_graph();
        let b = binding_with_node("s", 1);
        let expr = Expr::Property(
            Box::new(Expr::Var(IStr::new("s"))),
            IStr::new("nonexistent"),
        );
        assert!(eval_expr(&expr, &b, &g).unwrap().is_null());
    }

    #[test]
    fn eval_property_id() {
        let g = test_graph();
        let b = binding_with_node("s", 1);
        let expr = Expr::Property(Box::new(Expr::Var(IStr::new("s"))), IStr::new("id"));
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::UInt(1));
    }

    #[test]
    fn eval_null_property_is_null() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Property(Box::new(Expr::Literal(GqlValue::Null)), IStr::new("name"));
        assert!(eval_expr(&expr, &b, &g).unwrap().is_null());
    }

    // ── Arithmetic ──

    #[test]
    fn eval_add_ints() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Arithmetic(
            Box::new(Expr::Literal(GqlValue::Int(3))),
            ArithOp::Add,
            Box::new(Expr::Literal(GqlValue::Int(4))),
        );
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Int(7));
    }

    #[test]
    fn eval_add_float_promotion() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Arithmetic(
            Box::new(Expr::Literal(GqlValue::Int(3))),
            ArithOp::Mul,
            Box::new(Expr::Literal(GqlValue::Float(2.5))),
        );
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Float(7.5));
    }

    #[test]
    fn eval_null_arithmetic() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Arithmetic(
            Box::new(Expr::Literal(GqlValue::Int(5))),
            ArithOp::Add,
            Box::new(Expr::Literal(GqlValue::Null)),
        );
        assert!(eval_expr(&expr, &b, &g).unwrap().is_null());
    }

    #[test]
    fn eval_division_by_zero() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Arithmetic(
            Box::new(Expr::Literal(GqlValue::Int(10))),
            ArithOp::Div,
            Box::new(Expr::Literal(GqlValue::Int(0))),
        );
        assert!(eval_expr(&expr, &b, &g).is_err());
    }

    // ── Comparison ──

    #[test]
    fn eval_eq_true() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Compare(
            Box::new(Expr::Literal(GqlValue::Int(5))),
            CompareOp::Eq,
            Box::new(Expr::Literal(GqlValue::Int(5))),
        );
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(true));
    }

    #[test]
    fn eval_eq_null_is_null() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Compare(
            Box::new(Expr::Literal(GqlValue::Int(5))),
            CompareOp::Eq,
            Box::new(Expr::Literal(GqlValue::Null)),
        );
        assert!(eval_expr(&expr, &b, &g).unwrap().is_null());
    }

    #[test]
    fn eval_gt() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Compare(
            Box::new(Expr::Literal(GqlValue::Float(72.5))),
            CompareOp::Gt,
            Box::new(Expr::Literal(GqlValue::Float(72.0))),
        );
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(true));
    }

    // ── Boolean logic ──

    #[test]
    fn eval_and_true() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Logic(
            Box::new(Expr::Literal(GqlValue::Bool(true))),
            LogicOp::And,
            Box::new(Expr::Literal(GqlValue::Bool(true))),
        );
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(true));
    }

    #[test]
    fn eval_and_null() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Logic(
            Box::new(Expr::Literal(GqlValue::Bool(true))),
            LogicOp::And,
            Box::new(Expr::Literal(GqlValue::Null)),
        );
        assert!(eval_expr(&expr, &b, &g).unwrap().is_null());
    }

    #[test]
    fn eval_not_true() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Not(Box::new(Expr::Literal(GqlValue::Bool(true))));
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(false));
    }

    // ── String operations ──

    #[test]
    fn eval_concat() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Concat(
            Box::new(Expr::Literal(GqlValue::String(SmolStr::new("hello")))),
            Box::new(Expr::Literal(GqlValue::String(SmolStr::new(" world")))),
        );
        match eval_expr(&expr, &b, &g).unwrap() {
            GqlValue::String(s) => assert_eq!(&*s, "hello world"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn eval_contains() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::StringMatch(
            Box::new(Expr::Literal(GqlValue::String(SmolStr::new("hello world")))),
            StringMatchOp::Contains,
            Box::new(Expr::Literal(GqlValue::String(SmolStr::new("world")))),
        );
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(true));
    }

    // ── IS NULL ──

    #[test]
    fn eval_is_null_true() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::IsNull {
            expr: Box::new(Expr::Literal(GqlValue::Null)),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(true));
    }

    #[test]
    fn eval_is_not_null() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::IsNull {
            expr: Box::new(Expr::Literal(GqlValue::Int(5))),
            negated: true,
        };
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(true));
    }

    // ── IN list ──

    #[test]
    fn eval_in_list() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::InList {
            expr: Box::new(Expr::Literal(GqlValue::Int(2))),
            list: vec![
                Expr::Literal(GqlValue::Int(1)),
                Expr::Literal(GqlValue::Int(2)),
                Expr::Literal(GqlValue::Int(3)),
            ],
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(true));
    }

    // ── Functions ──

    #[test]
    fn eval_coalesce() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Function(FunctionCall {
            name: IStr::new("coalesce"),
            args: vec![
                Expr::Literal(GqlValue::Null),
                Expr::Literal(GqlValue::Int(42)),
            ],
            count_star: false,
        });
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Int(42));
    }

    #[test]
    fn eval_size_list() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Function(FunctionCall {
            name: IStr::new("size"),
            args: vec![Expr::Literal(GqlValue::List(GqlList {
                element_type: GqlType::Int,
                elements: Arc::from(vec![GqlValue::Int(1), GqlValue::Int(2)]),
            }))],
            count_star: false,
        });
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Int(2));
    }

    // ── CAST ──

    #[test]
    fn eval_cast_int_to_string() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Cast(Box::new(Expr::Literal(GqlValue::Int(42))), GqlType::String);
        match eval_expr(&expr, &b, &g).unwrap() {
            GqlValue::String(s) => assert_eq!(&*s, "42"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn eval_cast_string_to_int() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Cast(
            Box::new(Expr::Literal(GqlValue::String(SmolStr::new("42")))),
            GqlType::Int,
        );
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Int(42));
    }

    // ── LABELS ──

    #[test]
    fn eval_labels_node() {
        let g = test_graph();
        let b = binding_with_node("s", 1);
        let expr = Expr::Labels(Box::new(Expr::Var(IStr::new("s"))));
        match eval_expr(&expr, &b, &g).unwrap() {
            GqlValue::List(l) => {
                assert_eq!(l.len(), 1);
                match &l.elements[0] {
                    GqlValue::String(s) => assert_eq!(&**s, "sensor"),
                    _ => panic!("expected string"),
                }
            }
            other => panic!("expected List, got {other:?}"),
        }
    }

    // ── Duration parsing ──

    #[test]
    fn parse_duration_hours() {
        assert_eq!(parse_duration("1h").unwrap(), 3_600_000_000_000);
    }

    #[test]
    fn parse_duration_compound() {
        assert_eq!(
            parse_duration("1h30m").unwrap(),
            3_600_000_000_000 + 30 * 60_000_000_000
        );
    }

    #[test]
    fn parse_duration_days() {
        assert_eq!(parse_duration("7d").unwrap(), 7 * 86_400_000_000_000);
    }

    // ── Aggregation ──

    #[test]
    fn eval_count_star() {
        let g = test_graph();
        let bindings = vec![
            binding_with_node("s", 1),
            binding_with_node("s", 1),
            binding_with_node("s", 1),
        ];
        let agg = AggregateExpr {
            op: AggregateOp::Count,
            expr: None,
            distinct: false,
        };
        assert_eq!(
            eval_aggregate(&agg, &bindings, &make_ctx(&g)).unwrap(),
            GqlValue::Int(3)
        );
    }

    #[test]
    fn eval_avg_floats() {
        let g = SeleneGraph::new();
        let bindings: Vec<Binding> = vec![10.0, 20.0, 30.0]
            .into_iter()
            .map(|v| {
                let mut b = Binding::empty();
                b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Float(v)));
                b
            })
            .collect();

        let agg = AggregateExpr {
            op: AggregateOp::Avg,
            expr: Some(Box::new(Expr::Var(IStr::new("x")))),
            distinct: false,
        };
        assert_eq!(
            eval_aggregate(&agg, &bindings, &make_ctx(&g)).unwrap(),
            GqlValue::Float(20.0)
        );
    }

    #[test]
    fn eval_sum_ints() {
        let g = SeleneGraph::new();
        let bindings: Vec<Binding> = vec![1, 2, 3]
            .into_iter()
            .map(|v| {
                let mut b = Binding::empty();
                b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Int(v)));
                b
            })
            .collect();

        let agg = AggregateExpr {
            op: AggregateOp::Sum,
            expr: Some(Box::new(Expr::Var(IStr::new("x")))),
            distinct: false,
        };
        assert_eq!(
            eval_aggregate(&agg, &bindings, &make_ctx(&g)).unwrap(),
            GqlValue::Int(6)
        );
    }

    #[test]
    fn eval_min_max() {
        let g = SeleneGraph::new();
        let bindings: Vec<Binding> = vec![5, 2, 8, 1, 9]
            .into_iter()
            .map(|v| {
                let mut b = Binding::empty();
                b.bind(IStr::new("x"), BoundValue::Scalar(GqlValue::Int(v)));
                b
            })
            .collect();

        let min_agg = AggregateExpr {
            op: AggregateOp::Min,
            expr: Some(Box::new(Expr::Var(IStr::new("x")))),
            distinct: false,
        };
        let max_agg = AggregateExpr {
            op: AggregateOp::Max,
            expr: Some(Box::new(Expr::Var(IStr::new("x")))),
            distinct: false,
        };
        assert_eq!(
            eval_aggregate(&min_agg, &bindings, &make_ctx(&g)).unwrap(),
            GqlValue::Int(1)
        );
        assert_eq!(
            eval_aggregate(&max_agg, &bindings, &make_ctx(&g)).unwrap(),
            GqlValue::Int(9)
        );
    }

    #[test]
    fn eval_aggregate_skips_nulls() {
        let g = SeleneGraph::new();
        let bindings: Vec<Binding> = vec![GqlValue::Int(10), GqlValue::Null, GqlValue::Int(20)]
            .into_iter()
            .map(|v| {
                let mut b = Binding::empty();
                b.bind(IStr::new("x"), BoundValue::Scalar(v));
                b
            })
            .collect();

        let agg = AggregateExpr {
            op: AggregateOp::Avg,
            expr: Some(Box::new(Expr::Var(IStr::new("x")))),
            distinct: false,
        };
        assert_eq!(
            eval_aggregate(&agg, &bindings, &make_ctx(&g)).unwrap(),
            GqlValue::Float(15.0) // avg of 10 and 20, NULL skipped
        );
    }

    // ── Predicate evaluation ──

    #[test]
    fn predicate_true() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Literal(GqlValue::Bool(true));
        assert_eq!(
            eval_predicate(&expr, &b, &make_ctx(&g)).unwrap(),
            Trilean::True
        );
    }

    #[test]
    fn predicate_null_is_unknown() {
        let g = SeleneGraph::new();
        let b = Binding::empty();
        let expr = Expr::Literal(GqlValue::Null);
        assert_eq!(
            eval_predicate(&expr, &b, &make_ctx(&g)).unwrap(),
            Trilean::Unknown
        );
    }

    // ── Integration: property filter from graph ──

    #[test]
    fn eval_property_comparison_from_graph() {
        let g = test_graph();
        let b = binding_with_node("s", 1);
        // s.temp > 72
        let expr = Expr::Compare(
            Box::new(Expr::Property(
                Box::new(Expr::Var(IStr::new("s"))),
                IStr::new("temp"),
            )),
            CompareOp::Gt,
            Box::new(Expr::Literal(GqlValue::Float(72.0))),
        );
        assert_eq!(eval_expr(&expr, &b, &g).unwrap(), GqlValue::Bool(true));
    }
}
