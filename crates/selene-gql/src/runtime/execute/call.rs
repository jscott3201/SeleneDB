//! CALL procedure and subquery execution.

use roaring::RoaringBitmap;
use selene_core::IStr;
use selene_graph::SeleneGraph;

use crate::ast::expr::Expr;
use crate::ast::expr::ProcedureCall;
use crate::pipeline::stages;
use crate::planner::plan::*;
use crate::runtime::eval::EvalContext;
use crate::runtime::functions::FunctionRegistry;
use crate::runtime::procedures::ProcedureRegistry;
use crate::types::binding::{Binding, BoundValue};
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

use super::pattern::execute_pattern_ops_with_eval_ctx;

/// Normalize a column name for flexible matching: uppercase and strip underscores.
/// This lets `node_count` match `nodeCount` (both become `NODECOUNT`).
fn normalize_col(s: &str) -> String {
    s.to_uppercase().replace('_', "")
}

/// Execute a CALL procedure for each input binding.
///
/// Accepts an optional `EvalContext` so that query parameters (`$param`)
/// and other context (temporal, options) are available during argument
/// evaluation. When `None`, a default context is built from the graph
/// and builtin function registry.
pub(super) fn execute_call(
    bindings: Vec<Binding>,
    call: &ProcedureCall,
    graph: &SeleneGraph,
    hot_tier: Option<&selene_ts::HotTier>,
    procedures: Option<&ProcedureRegistry>,
    scope: Option<&roaring::RoaringBitmap>,
    ctx: Option<&EvalContext<'_>>,
) -> Result<Vec<Binding>, GqlError> {
    let registry =
        procedures.ok_or_else(|| GqlError::internal("no procedure registry available"))?;

    // Case-insensitive procedure lookup: normalize to lowercase before
    // querying the registry so `CALL graph.Labels()` finds `graph.labels`.
    let lookup_name = IStr::new(&call.name.as_str().to_lowercase());
    let proc = registry
        .get(&lookup_name)
        .ok_or_else(|| GqlError::UnknownProcedure {
            name: call.name.as_str().to_string(),
        })?;

    // Validate argument count against the procedure signature.
    {
        let sig = proc.signature();
        let required = sig.params.len();
        let provided = call.args.len();
        if provided < required {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "procedure '{}' requires {} argument(s) but {} provided; expected: {}",
                    call.name.as_str(),
                    required,
                    provided,
                    sig.params
                        .iter()
                        .map(|p| p.name)
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            });
        }
    }

    // Validate explicit YIELD columns against the procedure signature.
    if !call.yield_star && !call.yields.is_empty() {
        let sig = proc.signature();
        let available: Vec<String> = sig.yields.iter().map(|y| normalize_col(y.name)).collect();
        for yi in &call.yields {
            let normalized = normalize_col(yi.name.as_str());
            if !available.contains(&normalized) {
                let col_names: Vec<&str> = sig.yields.iter().map(|y| y.name).collect();
                return Err(GqlError::Type {
                    message: format!(
                        "procedure '{}' does not yield column '{}'; available columns: {:?}",
                        call.name.as_str(),
                        yi.name.as_str().to_lowercase(),
                        col_names,
                    ),
                });
            }
        }
    }

    // Build a fallback context when the caller does not supply one.
    let owned_ctx;
    let eval_ctx = if let Some(c) = ctx {
        c
    } else {
        owned_ctx = EvalContext::new(graph, FunctionRegistry::builtins());
        &owned_ctx
    };

    // Pre-compute normalized YIELD names once (avoids per-row allocations).
    // Each entry: (normalized_name, alias_istr) for matching against procedure columns.
    let yield_all = call.yield_star || call.yields.is_empty();
    let yield_lookup: Vec<(String, IStr)> = if yield_all {
        Vec::new()
    } else {
        call.yields
            .iter()
            .map(|yi| {
                let normalized = normalize_col(yi.name.as_str());
                let alias = yi.alias.unwrap_or(yi.name);
                (normalized, alias)
            })
            .collect()
    };

    let mut output = Vec::new();
    for binding in &bindings {
        // Evaluate arguments against current binding with full context
        // (includes query parameters, temporal resolver, options, etc.)
        let args: Vec<GqlValue> = call
            .args
            .iter()
            .map(|expr| crate::runtime::eval::eval_expr_ctx(expr, binding, eval_ctx))
            .collect::<Result<_, _>>()?;

        // Execute procedure with scope filtering
        let rows = proc.execute(&args, graph, hot_tier, scope)?;

        // Merge each procedure result row into the binding
        for row in rows {
            let mut extended = binding.clone();
            for (name, value) in &row {
                if yield_all {
                    // YIELD * or no explicit yields: include all columns
                    let upper_name = IStr::new(&name.as_str().to_uppercase());
                    extended.bind(upper_name, BoundValue::Scalar(value.clone()));
                } else {
                    // Match procedure column name against pre-computed YIELD names.
                    let norm_name = normalize_col(name.as_str());
                    if let Some((_, alias)) = yield_lookup.iter().find(|(n, _)| *n == norm_name) {
                        extended.bind(*alias, BoundValue::Scalar(value.clone()));
                    }
                    // else: column not in YIELD list, skip it
                }
            }
            output.push(extended);
        }
        // Inner-join semantics: if procedure returns 0 rows, binding is dropped
    }

    Ok(output)
}

/// Execute a CALL { subquery } for each input binding.
///
/// When `parent_ctx` is provided, query parameters (`$param` bindings) are
/// inherited so that subquery expressions can reference them.
pub(super) fn execute_subquery(
    bindings: Vec<Binding>,
    sub_plan: &ExecutionPlan,
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    hot_tier: Option<&selene_ts::HotTier>,
    procedures: Option<&ProcedureRegistry>,
    parent_ctx: Option<&super::eval::EvalContext<'_>>,
) -> Result<Vec<Binding>, GqlError> {
    let registry = FunctionRegistry::builtins();
    let mut ctx = super::eval::EvalContext::new(graph, registry).with_scope(scope);
    if let Some(parent) = parent_ctx
        && let Some(params) = parent.parameters
    {
        ctx = ctx.with_parameters(params);
    }

    let mut output = Vec::new();
    for outer_binding in &bindings {
        let mut sub_bindings =
            execute_pattern_ops_with_eval_ctx(&sub_plan.pattern_ops, graph, scope, &ctx)?;

        // Filter sub_bindings: keep only those consistent with outer binding's variables
        sub_bindings.retain(|sub_b| {
            outer_binding
                .iter()
                .all(|(var, outer_val)| match sub_b.get(var) {
                    Some(inner_val) => match (outer_val, inner_val) {
                        (BoundValue::Node(a), BoundValue::Node(b)) => a == b,
                        (BoundValue::Edge(a), BoundValue::Edge(b)) => a == b,
                        _ => true,
                    },
                    None => true,
                })
        });

        if sub_bindings.is_empty() && !sub_plan.pattern_ops.is_empty() {
            continue; // inner join: no match → drop outer row
        }
        if sub_bindings.is_empty() {
            sub_bindings = vec![outer_binding.clone()];
        } else {
            for sub_b in &mut sub_bindings {
                for (var, val) in outer_binding.iter() {
                    if !sub_b.contains(var) {
                        sub_b.bind(*var, val.clone());
                    }
                }
            }
        }

        for op in &sub_plan.pipeline {
            match op {
                PipelineOp::Call { procedure: call } => {
                    sub_bindings = execute_call(
                        sub_bindings,
                        call,
                        graph,
                        hot_tier,
                        procedures,
                        scope,
                        Some(&ctx),
                    )?;
                }
                PipelineOp::Subquery { plan: nested } => {
                    sub_bindings = execute_subquery(
                        sub_bindings,
                        nested,
                        graph,
                        scope,
                        hot_tier,
                        procedures,
                        Some(&ctx),
                    )?;
                }
                PipelineOp::NestedMatch {
                    pattern_ops: nested_ops,
                    where_filter,
                } => {
                    sub_bindings = execute_nested_match(
                        sub_bindings,
                        nested_ops,
                        where_filter.as_ref(),
                        graph,
                        scope,
                        Some(&ctx),
                    )?;
                }
                _ => {
                    sub_bindings = stages::execute_pipeline_op(op, sub_bindings, &ctx)?;
                }
            }
        }

        for sub_result in &sub_bindings {
            let mut merged = outer_binding.clone();
            for (var, val) in sub_result.iter() {
                if !outer_binding.contains(var) {
                    merged.bind(*var, val.clone());
                }
            }
            output.push(merged);
        }
    }
    Ok(output)
}

/// Execute a NestedMatch: correlated MATCH after WITH.
///
/// For each input binding (produced by a prior WITH clause), runs the
/// pattern ops as a seeded correlated execution, then applies the optional
/// WHERE filter. Variables already present in the seed binding short-circuit
/// their LabelScan (the correlated path in `execute_single_pattern_op_chunk`).
/// Results are merged back with the outer binding to produce the final output.
pub(super) fn execute_nested_match(
    bindings: Vec<Binding>,
    nested_ops: &[crate::planner::plan::PatternOp],
    where_filter: Option<&Expr>,
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    parent_ctx: Option<&EvalContext<'_>>,
) -> Result<Vec<Binding>, GqlError> {
    let registry = FunctionRegistry::builtins();
    let mut ctx = EvalContext::new(graph, registry).with_scope(scope);
    if let Some(parent) = parent_ctx
        && let Some(params) = parent.parameters
    {
        ctx = ctx.with_parameters(params);
    }

    let mut output = Vec::new();
    for outer_binding in &bindings {
        // Run the nested pattern ops seeded by this binding.
        let mut inner_bindings = execute_pattern_ops_with_eval_ctx(nested_ops, graph, scope, &ctx)?;

        // Filter inner results to keep only those consistent with the seed.
        inner_bindings.retain(|inner_b| {
            outer_binding
                .iter()
                .all(|(var, outer_val)| match inner_b.get(var) {
                    Some(inner_val) => match (outer_val, inner_val) {
                        (BoundValue::Node(a), BoundValue::Node(b)) => a == b,
                        (BoundValue::Edge(a), BoundValue::Edge(b)) => a == b,
                        _ => true,
                    },
                    None => true,
                })
        });

        // Inner join: no inner results → drop this outer row.
        if inner_bindings.is_empty() {
            continue;
        }

        // Propagate outer variables into each inner binding, then apply WHERE.
        for inner_b in &mut inner_bindings {
            for (var, val) in outer_binding.iter() {
                if !inner_b.contains(var) {
                    inner_b.bind(*var, val.clone());
                }
            }
        }

        // Apply the optional WHERE filter from the MATCH clause.
        if let Some(pred) = where_filter {
            inner_bindings.retain(|b| {
                crate::runtime::eval::eval_predicate(pred, b, &ctx).is_ok_and(|t| t.is_true())
            });
        }

        output.extend(inner_bindings);
    }
    Ok(output)
}
