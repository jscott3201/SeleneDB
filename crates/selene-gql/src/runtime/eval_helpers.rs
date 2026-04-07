//! Extracted eval helper functions: individual expression evaluators called
//! from `eval_expr_inner` in the parent `eval` module.

use std::sync::Arc;

use selene_core::IStr;
use smol_str::SmolStr;

use crate::ast::expr::*;
use crate::types::binding::{Binding, BoundValue};
use crate::types::error::GqlError;
use crate::types::trilean::Trilean;
use crate::types::value::{GqlList, GqlType, GqlValue};

use super::eval::{EvalContext, eval_expr_ctx, eval_predicate, resolve_property, trilean_to_value};
use super::eval_aggregate::eval_horizontal_aggregate;

// ── Extracted eval helpers ──────────────────────────────────────────────────

pub(super) fn eval_list_construct(
    elements: &[Expr],
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let values: Vec<GqlValue> = elements
        .iter()
        .map(|e| eval_expr_ctx(e, binding, ctx))
        .collect::<Result<_, _>>()?;
    let element_type = crate::types::value::infer_list_element_type(&values);
    Ok(GqlValue::List(GqlList {
        element_type,
        elements: Arc::from(values),
    }))
}

pub(super) fn eval_temporal_property(
    target: &Expr,
    key: IStr,
    timestamp_str: &str,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let target_val = eval_expr_ctx(target, binding, ctx)?;
    let node_id = match &target_val {
        GqlValue::UInt(id) => selene_core::NodeId(*id),
        GqlValue::Int(id) => selene_core::NodeId(*id as u64),
        _ => {
            if let Expr::Var(name) = target {
                match binding.get(name) {
                    Some(BoundValue::Node(nid)) => *nid,
                    _ => {
                        return Err(GqlError::type_error("AT TIME requires a node reference"));
                    }
                }
            } else {
                return Err(GqlError::type_error("AT TIME requires a node reference"));
            }
        }
    };
    let timestamp = if let Ok(GqlValue::ZonedDateTime(dt)) =
        super::functions::parse_iso8601(timestamp_str)
    {
        dt.nanos
    } else if let Ok(n) = timestamp_str.parse::<i64>() {
        n
    } else {
        return Err(GqlError::InvalidArgument {
            message: format!("invalid AT TIME timestamp: '{timestamp_str}' (expected ISO 8601)"),
        });
    };
    if let Some(resolver) = ctx.temporal {
        match resolver.value_at(node_id, key.as_str(), timestamp, ctx.graph) {
            Some(val) => Ok(GqlValue::from(&val)),
            None => Ok(GqlValue::Null),
        }
    } else {
        resolve_property(&target_val, key, ctx.graph)
    }
}

pub(super) fn eval_logic(
    left: &Expr,
    op: LogicOp,
    right: &Expr,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let lv = eval_predicate(left, binding, ctx)?;
    match op {
        LogicOp::And if lv == Trilean::False => return Ok(GqlValue::Bool(false)),
        LogicOp::Or if lv == Trilean::True => return Ok(GqlValue::Bool(true)),
        _ => {}
    }
    let rv = eval_predicate(right, binding, ctx)?;
    let result = match op {
        LogicOp::And => lv.and(rv),
        LogicOp::Or => lv.or(rv),
        LogicOp::Xor => match (lv, rv) {
            (Trilean::True, Trilean::False) | (Trilean::False, Trilean::True) => Trilean::True,
            (Trilean::True, Trilean::True) | (Trilean::False, Trilean::False) => Trilean::False,
            _ => Trilean::Unknown,
        },
    };
    Ok(trilean_to_value(result))
}

pub(super) fn eval_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let v = eval_expr_ctx(expr, binding, ctx)?;
    if v.is_null() {
        return Ok(GqlValue::Null);
    }
    let mut found = false;
    let mut has_unknown = false;
    for item_expr in list {
        let item = eval_expr_ctx(item_expr, binding, ctx)?;
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
        Ok(GqlValue::Bool(!negated))
    } else if has_unknown {
        Ok(GqlValue::Null)
    } else {
        Ok(GqlValue::Bool(negated))
    }
}

pub(super) fn eval_aggregate_expr(
    agg: &crate::ast::expr::AggregateExpr,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    if let Some(inner_expr) = &agg.expr {
        let val = eval_expr_ctx(inner_expr, binding, ctx)?;
        if let GqlValue::List(ref list) = val {
            return eval_horizontal_aggregate(agg.op, &list.elements);
        }
    }
    Err(GqlError::internal(
        "aggregate expressions must be evaluated by the RETURN stage",
    ))
}

/// Extract element IDs from expressions, used by AllDifferent and Same.
fn extract_element_ids(
    exprs: &[Expr],
    label: &str,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<Vec<u64>, GqlError> {
    let mut ids = Vec::new();
    for expr in exprs {
        match eval_expr_ctx(expr, binding, ctx)? {
            GqlValue::Node(id) => ids.push(id.0),
            GqlValue::Edge(id) => ids.push(id.0),
            GqlValue::Null => {
                return Err(GqlError::type_error(format!(
                    "{label}: null value not allowed (ISO \u{00a7}19.11)"
                )));
            }
            _ => {
                return Err(GqlError::type_error(format!(
                    "{label} requires node or edge arguments"
                )));
            }
        }
    }
    Ok(ids)
}

pub(super) fn eval_all_different(
    exprs: &[Expr],
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let ids = extract_element_ids(exprs, "ALL_DIFFERENT", binding, ctx)?;
    let unique: std::collections::HashSet<u64> = ids.iter().copied().collect();
    Ok(GqlValue::Bool(unique.len() == ids.len()))
}

pub(super) fn eval_same(
    exprs: &[Expr],
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let ids = extract_element_ids(exprs, "SAME", binding, ctx)?;
    let all_same = ids.windows(2).all(|w| w[0] == w[1]);
    Ok(GqlValue::Bool(all_same))
}

pub(super) fn eval_property_exists(
    expr: &Expr,
    key: IStr,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let val = eval_expr_ctx(expr, binding, ctx)?;
    match val {
        GqlValue::Node(id) => {
            let exists = ctx
                .graph
                .get_node(id)
                .is_some_and(|n| n.property(key.as_str()).is_some());
            Ok(GqlValue::Bool(exists))
        }
        GqlValue::Edge(id) => {
            let exists = ctx
                .graph
                .get_edge(id)
                .is_some_and(|e| e.properties.get_by_str(key.as_str()).is_some());
            Ok(GqlValue::Bool(exists))
        }
        GqlValue::Null => Ok(GqlValue::Null),
        _ => Err(GqlError::type_error(
            "PROPERTY_EXISTS requires a node or edge",
        )),
    }
}

pub(super) fn eval_exists(
    pattern: &crate::ast::pattern::MatchClause,
    negated: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let inner_ops = plan_subquery_cached(pattern, ctx.graph)?;
    let results = crate::runtime::execute::execute_pattern_ops_with_max_and_ctx(
        &inner_ops,
        ctx.graph,
        ctx.scope,
        None,
        Some(binding),
        None,
        None,
        ctx,
    )?;
    // Apply the inner MATCH's WHERE clause if present (plan_subquery_cached
    // only returns pattern ops and does not convert WHERE to a filter).
    let results = filter_subquery_results(results, pattern, ctx)?;
    let exists = !results.is_empty();
    Ok(GqlValue::Bool(if negated { !exists } else { exists }))
}

pub(super) fn eval_is_labeled(
    expr: &Expr,
    label: &crate::ast::pattern::LabelExpr,
    negated: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let val = eval_expr_ctx(expr, binding, ctx)?;
    let result = match val {
        GqlValue::Node(id) => {
            if let Some(node) = ctx.graph.get_node(id) {
                match label {
                    crate::ast::pattern::LabelExpr::Name(name) => node.has_label(name.as_str()),
                    crate::ast::pattern::LabelExpr::Wildcard => !node.labels.is_empty(),
                    crate::ast::pattern::LabelExpr::And(exprs) => exprs.iter().all(|e| match e {
                        crate::ast::pattern::LabelExpr::Name(n) => node.has_label(n.as_str()),
                        _ => true,
                    }),
                    crate::ast::pattern::LabelExpr::Or(exprs) => exprs.iter().any(|e| match e {
                        crate::ast::pattern::LabelExpr::Name(n) => node.has_label(n.as_str()),
                        _ => false,
                    }),
                    crate::ast::pattern::LabelExpr::Not(inner) => {
                        if let crate::ast::pattern::LabelExpr::Name(n) = inner.as_ref() {
                            !node.has_label(n.as_str())
                        } else {
                            false
                        }
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    };
    Ok(GqlValue::Bool(if negated { !result } else { result }))
}

pub(super) fn eval_trim(
    source: &Expr,
    character: Option<&Expr>,
    spec: TrimSpec,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let src = eval_expr_ctx(source, binding, ctx)?;
    let s = match &src {
        GqlValue::String(s) => s.as_str(),
        GqlValue::Null => return Ok(GqlValue::Null),
        _ => return Err(GqlError::type_error("TRIM: source must be a string")),
    };
    let trim_char = if let Some(ch_expr) = character {
        let ch_val = eval_expr_ctx(ch_expr, binding, ctx)?;
        match &ch_val {
            GqlValue::String(c) => c.chars().next().unwrap_or(' '),
            _ => ' ',
        }
    } else {
        ' '
    };
    let result = match spec {
        TrimSpec::Leading => s.trim_start_matches(trim_char),
        TrimSpec::Trailing => s.trim_end_matches(trim_char),
        TrimSpec::Both => s.trim_matches(trim_char),
    };
    Ok(GqlValue::String(SmolStr::new(result)))
}

pub(super) fn eval_record_construct(
    fields: &[(IStr, Box<Expr>)],
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let mut record_fields = Vec::with_capacity(fields.len());
    for (key, val_expr) in fields {
        let val = eval_expr_ctx(val_expr, binding, ctx)?;
        record_fields.push((*key, val));
    }
    Ok(GqlValue::Record(crate::types::value::GqlRecord {
        fields: record_fields,
    }))
}

pub(super) fn eval_is_typed(
    expr: &Expr,
    type_name: &GqlType,
    negated: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let val = eval_expr_ctx(expr, binding, ctx)?;
    let matches = match (&val, type_name) {
        (GqlValue::Null, _) => false,
        (GqlValue::Bool(_), GqlType::Bool) => true,
        (GqlValue::Int(_), GqlType::Int) => true,
        (GqlValue::UInt(_), GqlType::UInt) => true,
        (GqlValue::Float(_), GqlType::Float) => true,
        (GqlValue::String(_), GqlType::String) => true,
        (GqlValue::ZonedDateTime(_), GqlType::ZonedDateTime) => true,
        (GqlValue::Date(_), GqlType::Date) => true,
        (GqlValue::LocalDateTime(_), GqlType::LocalDateTime) => true,
        (GqlValue::ZonedTime(_), GqlType::ZonedTime) => true,
        (GqlValue::LocalTime(_), GqlType::LocalTime) => true,
        (GqlValue::Duration(_), GqlType::Duration) => true,
        (GqlValue::Bytes(_), GqlType::Bytes) => true,
        (GqlValue::List(_), GqlType::List(_)) => true,
        (GqlValue::Node(_), GqlType::Node) => true,
        (GqlValue::Edge(_), GqlType::Edge) => true,
        (GqlValue::Path(_), GqlType::Path) => true,
        (GqlValue::Record(_), GqlType::Record) => true,
        _ => false,
    };
    Ok(GqlValue::Bool(if negated { !matches } else { matches }))
}

pub(super) fn eval_is_normalized(
    expr: &Expr,
    form: NormalForm,
    negated: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let val = eval_expr_ctx(expr, binding, ctx)?;
    let result = match val {
        GqlValue::String(ref s) => match form {
            NormalForm::NFC => unicode_normalization::is_nfc(s.as_str()),
            NormalForm::NFD => unicode_normalization::is_nfd(s.as_str()),
            NormalForm::NFKC => unicode_normalization::is_nfkc(s.as_str()),
            NormalForm::NFKD => unicode_normalization::is_nfkd(s.as_str()),
        },
        GqlValue::Null => return Ok(GqlValue::Null),
        _ => return Err(GqlError::type_error("IS NORMALIZED requires a string")),
    };
    Ok(GqlValue::Bool(if negated { !result } else { result }))
}

pub(super) fn eval_is_truth_value(
    expr: &Expr,
    value: TruthValue,
    negated: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let trilean = eval_predicate(expr, binding, ctx)?;
    let matches = match value {
        TruthValue::True => trilean == crate::types::trilean::Trilean::True,
        TruthValue::False => trilean == crate::types::trilean::Trilean::False,
        TruthValue::Unknown => trilean == crate::types::trilean::Trilean::Unknown,
    };
    Ok(GqlValue::Bool(if negated { !matches } else { matches }))
}

/// Shared evaluator for IS SOURCE OF / IS DESTINATION OF.
pub(super) fn eval_edge_endpoint(
    node: &Expr,
    edge: &Expr,
    negated: bool,
    is_source: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let node_val = eval_expr_ctx(node, binding, ctx)?;
    let edge_val = eval_expr_ctx(edge, binding, ctx)?;
    let result = match (&node_val, &edge_val) {
        (GqlValue::Node(nid), GqlValue::Edge(eid)) => ctx.graph.get_edge(*eid).is_some_and(|e| {
            if is_source {
                e.source == *nid
            } else {
                e.target == *nid
            }
        }),
        _ => false,
    };
    Ok(GqlValue::Bool(if negated { !result } else { result }))
}

pub(super) fn eval_parameter(name: IStr, ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
    match ctx.parameters {
        Some(params) => match params.get(&name) {
            Some(val) => Ok(val.clone()),
            None => Err(GqlError::InvalidArgument {
                message: format!("missing query parameter: ${}", name.as_str()),
            }),
        },
        None => Err(GqlError::InvalidArgument {
            message: format!(
                "query parameter ${} used but no parameters provided",
                name.as_str()
            ),
        }),
    }
}

pub(super) fn eval_case(
    branches: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    for (when_expr, then_expr) in branches {
        let cond = eval_expr_ctx(when_expr, binding, ctx)?;
        if let GqlValue::Bool(true) = cond {
            return eval_expr_ctx(then_expr, binding, ctx);
        }
    }
    match else_expr {
        Some(e) => eval_expr_ctx(e, binding, ctx),
        None => Ok(GqlValue::Null),
    }
}

/// Execute subquery plan and return bindings (shared by Value/Collect subqueries).
fn execute_subquery_plan(
    pipeline: &crate::ast::statement::QueryPipeline,
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    let plan = crate::planner::plan_query(pipeline, ctx.graph)?;
    let mut bindings = crate::runtime::execute::execute_pattern_ops_with_eval_ctx(
        &plan.pattern_ops,
        ctx.graph,
        ctx.scope,
        ctx,
    )?;
    for op in &plan.pipeline {
        bindings = crate::pipeline::stages::execute_pipeline_op(op, bindings, ctx)?;
    }
    Ok(bindings)
}

/// Extract a scalar value from the first variable of a binding.
fn binding_to_scalar(b: &Binding) -> GqlValue {
    match b.iter().next() {
        Some((_, BoundValue::Scalar(v))) => v.clone(),
        Some((_, BoundValue::Node(id))) => GqlValue::Node(*id),
        Some((_, BoundValue::Edge(id))) => GqlValue::Edge(*id),
        _ => GqlValue::Null,
    }
}

pub(super) fn eval_value_subquery(
    pipeline: &crate::ast::statement::QueryPipeline,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let bindings = execute_subquery_plan(pipeline, ctx)?;
    if bindings.is_empty() {
        return Ok(GqlValue::Null);
    }
    if bindings.len() > 1 {
        return Err(GqlError::internal(format!(
            "VALUE subquery returned {} rows, expected at most 1",
            bindings.len()
        )));
    }
    Ok(binding_to_scalar(&bindings[0]))
}

pub(super) fn eval_collect_subquery(
    pipeline: &crate::ast::statement::QueryPipeline,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let bindings = execute_subquery_plan(pipeline, ctx)?;
    let values: Vec<GqlValue> = bindings.iter().map(binding_to_scalar).collect();
    let element_type = values
        .first()
        .map_or(crate::types::value::GqlType::Nothing, |v| v.gql_type());
    Ok(GqlValue::List(crate::types::value::GqlList {
        element_type,
        elements: std::sync::Arc::from(values),
    }))
}

pub(super) fn eval_like(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let val = eval_expr_ctx(expr, binding, ctx)?;
    let pat = eval_expr_ctx(pattern, binding, ctx)?;
    if val.is_null() || pat.is_null() {
        return Ok(GqlValue::Null);
    }
    let s = val.as_str()?;
    let p = pat.as_str()?;

    if let Some(matched) = like_fast_path(s, p) {
        return Ok(GqlValue::Bool(if negated { !matched } else { matched }));
    }

    thread_local! {
        static LIKE_CACHE: std::cell::RefCell<Option<(String, regex::Regex)>> =
            const { std::cell::RefCell::new(None) };
    }

    let matched = LIKE_CACHE.with(|cache| -> Result<bool, GqlError> {
        let mut cache = cache.borrow_mut();
        if let Some((ref cached_pat, ref re)) = *cache
            && cached_pat == p
        {
            return Ok(re.is_match(s));
        }
        let regex_str = like_pattern_to_regex(p);
        let re = regex::Regex::new(&regex_str)
            .map_err(|e| GqlError::internal(format!("invalid LIKE pattern: {e}")))?;
        let result = re.is_match(s);
        *cache = Some((p.to_string(), re));
        Ok(result)
    })?;

    Ok(GqlValue::Bool(if negated { !matched } else { matched }))
}

pub(super) fn eval_between(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let val = eval_expr_ctx(expr, binding, ctx)?;
    let lo = eval_expr_ctx(low, binding, ctx)?;
    let hi = eval_expr_ctx(high, binding, ctx)?;
    if val.is_null() || lo.is_null() || hi.is_null() {
        return Ok(GqlValue::Null);
    }
    let ge_low = val.gql_order(&lo)? != std::cmp::Ordering::Less;
    let le_high = val.gql_order(&hi)? != std::cmp::Ordering::Greater;
    let result = ge_low && le_high;
    Ok(GqlValue::Bool(if negated { !result } else { result }))
}

pub(super) fn eval_count_subquery(
    pattern: &crate::ast::pattern::MatchClause,
    binding: &Binding,
    ctx: &EvalContext<'_>,
) -> Result<GqlValue, GqlError> {
    let inner_ops = plan_subquery_cached(pattern, ctx.graph)?;
    let results = crate::runtime::execute::execute_pattern_ops_correlated_with_ctx(
        &inner_ops, ctx.graph, ctx.scope, binding, ctx,
    )?;
    // Apply the inner MATCH's WHERE clause if present.
    let results = filter_subquery_results(results, pattern, ctx)?;
    Ok(GqlValue::Int(results.len() as i64))
}

/// Apply the WHERE clause from an inner MATCH to subquery results.
///
/// `plan_subquery_cached` only returns pattern ops and does not convert the
/// MatchClause's `where_clause` into a pipeline filter. This helper applies
/// the WHERE predicate as a post-filter on the returned bindings.
fn filter_subquery_results(
    results: Vec<Binding>,
    pattern: &crate::ast::pattern::MatchClause,
    ctx: &EvalContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    let Some(ref predicate) = pattern.where_clause else {
        return Ok(results);
    };
    results
        .into_iter()
        .filter_map(|b| match eval_expr_ctx(predicate, &b, ctx) {
            Ok(GqlValue::Bool(true)) => Some(Ok(b)),
            Ok(GqlValue::Bool(false) | GqlValue::Null) => None,
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
        .collect()
}

/// Cache subquery plans by MatchClause pointer identity + graph generation.
/// The AST is borrowed immutably during evaluation, so pointer stability is guaranteed.
/// This avoids re-planning correlated subqueries on every binding row (O(N) to O(1)).
/// Returns an Arc to share the plan across evaluations without cloning PatternOp.
///
/// Graph generation is included in the cache key so that cardinality-based ordering
/// is invalidated when the graph changes between queries.
///
/// **Stale pointer key risk:** The cache key includes the raw pointer of the
/// `MatchClause` AST node. Once the AST is dropped (after query execution), a
/// subsequent query could allocate a new AST at the same address, producing a
/// false cache hit. In practice this is safe because: (1) the graph generation
/// component of the key invalidates entries when the graph changes, and (2) the
/// thread-local HashMap is small and entries are inexpensive. If this ever
/// becomes a concern, clear the cache at query entry points or switch to a
/// content-based hash key.
#[allow(clippy::type_complexity)]
fn plan_subquery_cached(
    pattern: &crate::ast::pattern::MatchClause,
    graph: &selene_graph::SeleneGraph,
) -> Result<std::sync::Arc<Vec<crate::planner::plan::PatternOp>>, crate::types::error::GqlError> {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::Arc;

    thread_local! {
        #[allow(clippy::type_complexity)]
        static PLAN_CACHE: RefCell<HashMap<(usize, u64), Arc<Vec<crate::planner::plan::PatternOp>>>> =
            RefCell::new(HashMap::new());
    }

    let key = (std::ptr::from_ref(pattern) as usize, graph.generation());

    PLAN_CACHE.with(|cache| {
        if let Some(ops) = cache.borrow().get(&key) {
            return Ok(Arc::clone(ops));
        }

        let mut ops = Vec::new();
        crate::planner::plan_match_public(pattern, &mut ops, graph)?;

        let arc = Arc::new(ops);
        cache.borrow_mut().insert(key, Arc::clone(&arc));
        Ok(arc)
    })
}

/// Fast path for common LIKE patterns. Bypasses regex engine entirely.
/// Returns `Some(matched)` for prefix%, %suffix, %contains%, exact patterns.
/// Returns `None` for patterns with `_`, `\` (escapes), or complex multi-% positions.
fn like_fast_path(s: &str, pattern: &str) -> Option<bool> {
    // Patterns with `_` (single-char wildcard) or escapes always need regex
    if pattern.contains('_') || pattern.contains('\\') {
        return None;
    }

    let percent_count = pattern.chars().filter(|&c| c == '%').count();

    match percent_count {
        0 => {
            // Exact match: 'hello' LIKE 'hello'
            Some(s == pattern)
        }
        1 if pattern.ends_with('%') => {
            // Prefix: 'hello%'
            let prefix = &pattern[..pattern.len() - 1];
            Some(s.starts_with(prefix))
        }
        1 if pattern.starts_with('%') => {
            // Suffix: '%world'
            let suffix = &pattern[1..];
            Some(s.ends_with(suffix))
        }
        2 if pattern.starts_with('%') && pattern.ends_with('%') => {
            // Contains: '%substring%'
            let substring = &pattern[1..pattern.len() - 1];
            if substring.contains('%') {
                None // complex pattern like '%a%b%'
            } else {
                Some(s.contains(substring))
            }
        }
        _ => None, // Complex pattern, needs regex
    }
}

/// Convert a SQL LIKE pattern to a regex string.
/// `%` matches any sequence, `_` matches any single character.
fn like_pattern_to_regex(pattern: &str) -> String {
    let mut result = String::from("(?s)^");
    for ch in pattern.chars() {
        match ch {
            '%' => result.push_str(".*"),
            '_' => result.push('.'),
            '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '\\' | '^' | '$' | '|' => {
                result.push('\\');
                result.push(ch);
            }
            c => result.push(c),
        }
    }
    result.push('$');
    result
}
