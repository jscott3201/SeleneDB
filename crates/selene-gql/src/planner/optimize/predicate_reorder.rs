//! Predicate reordering optimization rule.
//!
//! Reorders contiguous runs of Filter ops so that cheaper, more selective
//! filters execute first. Uses a cost model for evaluation expense and
//! property index cardinality for selectivity estimation when a graph
//! reference is available.

use selene_core::IStr;

use selene_graph::SeleneGraph;

use crate::ast::expr::{CompareOp, Expr, LogicOp};
use crate::ast::pattern::LabelExpr;
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct PredicateReorderRule;

impl GqlOptimizerRule for PredicateReorderRule {
    fn name(&self) -> &'static str {
        "PredicateReorder"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        // Find contiguous runs of Filter ops and sort by selectivity * eval_cost.
        // Most selective and cheapest filters execute first, reducing the binding
        // set size for subsequent (potentially expensive) filters.
        let mut changed = false;

        // Build variable-to-label map for selectivity estimation
        let var_labels: std::collections::HashMap<IStr, IStr> = plan
            .pattern_ops
            .iter()
            .filter_map(|op| {
                if let PatternOp::LabelScan {
                    var,
                    labels: Some(LabelExpr::Name(label)),
                    ..
                } = op
                {
                    Some((*var, *label))
                } else {
                    None
                }
            })
            .collect();

        let mut i = 0;
        while i < plan.pipeline.len() {
            // Find start of a contiguous Filter run
            if !matches!(plan.pipeline[i], PipelineOp::Filter { .. }) {
                i += 1;
                continue;
            }
            let run_start = i;
            while i < plan.pipeline.len() && matches!(plan.pipeline[i], PipelineOp::Filter { .. }) {
                i += 1;
            }
            let run_end = i;

            // Only reorder if there are 2+ consecutive filters
            if run_end - run_start < 2 {
                continue;
            }

            // Score each filter: selectivity * eval_cost (lower = evaluate first)
            let mut indexed: Vec<(usize, u64)> = (run_start..run_end)
                .map(|idx| {
                    let score = match &plan.pipeline[idx] {
                        PipelineOp::Filter { predicate } => {
                            let eval_cost = f64::from(estimate_predicate_cost(predicate));
                            let selectivity = if let Some(graph) = ctx.graph {
                                estimate_filter_selectivity(predicate, graph, &var_labels)
                            } else {
                                0.5
                            };
                            // Scale to integer for stable sorting (3 decimal places)
                            (selectivity * eval_cost * 1000.0) as u64
                        }
                        _ => 5000,
                    };
                    (idx, score)
                })
                .collect();

            // Sort by score (stable sort preserves order for equal scores)
            indexed.sort_by_key(|&(_, score)| score);

            // Check if the sort changed the order
            let already_sorted = indexed
                .iter()
                .enumerate()
                .all(|(pos, &(idx, _))| idx == run_start + pos);
            if already_sorted {
                continue;
            }

            // Apply the reordering via a permutation (no Clone needed).
            // Extract the run, reorder, put back.
            let mut run: Vec<PipelineOp> = plan.pipeline.drain(run_start..run_end).collect();
            // Build the permuted order: indexed[i].0 - run_start gives the
            // position within the extracted run.
            let mut sorted_run = Vec::with_capacity(run.len());
            // Create index mapping: indexed was [(original_idx, cost)]
            // but original_idx was relative to plan.pipeline, now the run
            // is extracted starting at 0. Adjust.
            let order: Vec<usize> = indexed.iter().map(|&(idx, _)| idx - run_start).collect();
            for &src in &order {
                // Use a placeholder swap to move items without Clone
                sorted_run.push(std::mem::replace(
                    &mut run[src],
                    PipelineOp::Limit { count: 0 }, // dummy, will be overwritten
                ));
            }
            // Splice the sorted run back in
            let insert_pos = run_start;
            for (j, op) in sorted_run.into_iter().enumerate() {
                plan.pipeline.insert(insert_pos + j, op);
            }
            changed = true;
            // Adjust i since we modified the pipeline
            let _ = order;
        }

        if changed {
            Ok(Transformed::yes(plan))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

/// Estimate the evaluation cost of a predicate expression.
/// Lower cost = cheaper to evaluate = should be evaluated first.
pub(super) fn estimate_predicate_cost(expr: &Expr) -> u32 {
    match expr {
        // Cheapest: literals, variables, null checks
        Expr::Literal(_) => 1,
        Expr::Var(_) => 1,
        Expr::IsNull { .. } => 2,

        // Property access
        Expr::Property(..) => 3,

        // Equality/comparison: cheap (single comparison after eval)
        Expr::Compare(..) => 4,
        Expr::Between { .. } => 5,
        Expr::InList { .. } => 6,

        // String operations: moderate
        Expr::Like { .. } => 8,
        Expr::StringMatch(..) => 8,

        // Function calls: generally medium
        Expr::Function(_) => 10,

        // Logical connectives: sum of children
        Expr::Not(inner) => 1 + estimate_predicate_cost(inner),
        Expr::Logic(a, _, b) => estimate_predicate_cost(a) + estimate_predicate_cost(b),

        // Subqueries: expensive
        Expr::Exists { .. } => 20,
        Expr::CountSubquery(_) => 20,

        // Everything else: medium
        _ => 10,
    }
}

/// Estimate the selectivity of a filter predicate (0.0 = filters everything,
/// 1.0 = filters nothing). Uses property index cardinality when available,
/// falls back to heuristics for complex predicates.
fn estimate_filter_selectivity(
    expr: &Expr,
    graph: &SeleneGraph,
    var_labels: &std::collections::HashMap<IStr, IStr>,
) -> f64 {
    match expr {
        // var.prop = literal with possible index lookup
        Expr::Compare(lhs, CompareOp::Eq, rhs) => {
            if let (Some((var, key)), Some(lit)) = (extract_var_prop(lhs), extract_literal(rhs)) {
                return index_selectivity(graph, var_labels, var, key, &lit);
            }
            if let (Some(lit), Some((var, key))) = (extract_literal(lhs), extract_var_prop(rhs)) {
                return index_selectivity(graph, var_labels, var, key, &lit);
            }
            0.5
        }

        // var.prop != literal: inverse of equality
        Expr::Compare(lhs, CompareOp::Neq, rhs) => {
            if let (Some((var, key)), Some(lit)) = (extract_var_prop(lhs), extract_literal(rhs)) {
                return 1.0 - index_selectivity(graph, var_labels, var, key, &lit);
            }
            if let (Some(lit), Some((var, key))) = (extract_literal(lhs), extract_var_prop(rhs)) {
                return 1.0 - index_selectivity(graph, var_labels, var, key, &lit);
            }
            0.5
        }

        // Range comparisons: use TypedIndex range selectivity when available,
        // otherwise fall back to the 0.33 heuristic.
        Expr::Compare(
            lhs,
            op @ (CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte),
            rhs,
        ) => {
            // Try var.prop <op> literal (normal form).
            // For the reversed form (literal <op> var.prop) we flip the operator
            // so the semantic meaning remains: e.g. "5 > n.age" => "n.age < 5".
            let resolved: Option<(IStr, IStr, CompareOp, GqlValue)> = if let (
                Some((var, key)),
                Some(lit),
            ) =
                (extract_var_prop(lhs), extract_literal(rhs))
            {
                Some((var, key, *op, lit))
            } else if let (Some(lit), Some((var, key))) =
                (extract_literal(lhs), extract_var_prop(rhs))
            {
                let flipped = match op {
                    CompareOp::Gt => CompareOp::Lt,
                    CompareOp::Gte => CompareOp::Lte,
                    CompareOp::Lt => CompareOp::Gt,
                    CompareOp::Lte => CompareOp::Gte,
                    other => *other,
                };
                Some((var, key, flipped, lit))
            } else {
                None
            };

            if let Some((var, key, op, lit)) = resolved
                && let Some(&label) = var_labels.get(&var)
                && let Some(index) = graph.property_index_entries(label, key)
            {
                let core_val = selene_core::Value::try_from(&lit).ok();
                let sel = match op {
                    CompareOp::Gt | CompareOp::Gte => {
                        index.selectivity(&selene_graph::typed_index::SelectivityOp::Range {
                            lower: core_val.as_ref(),
                            upper: None,
                        })
                    }
                    CompareOp::Lt | CompareOp::Lte => {
                        index.selectivity(&selene_graph::typed_index::SelectivityOp::Range {
                            lower: None,
                            upper: core_val.as_ref(),
                        })
                    }
                    _ => 0.33,
                };
                return sel.max(0.001);
            }
            0.33
        }

        // IN list: sum of per-value selectivities
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                return 0.5;
            }
            if let Some((var, key)) = extract_var_prop(expr) {
                let mut total = 0.0;
                for item in list {
                    if let Some(lit) = extract_literal(item) {
                        total += index_selectivity(graph, var_labels, var, key, &lit);
                    } else {
                        total += 0.1;
                    }
                }
                return total.min(1.0);
            }
            0.5
        }

        // Boolean connectives
        Expr::Not(inner) => 1.0 - estimate_filter_selectivity(inner, graph, var_labels),
        Expr::Logic(a, LogicOp::And, b) => {
            let sa = estimate_filter_selectivity(a, graph, var_labels);
            let sb = estimate_filter_selectivity(b, graph, var_labels);
            sa * sb
        }
        Expr::Logic(a, LogicOp::Or, b) => {
            let sa = estimate_filter_selectivity(a, graph, var_labels);
            let sb = estimate_filter_selectivity(b, graph, var_labels);
            sa + sb - sa * sb
        }

        // IS NULL: typically selective
        Expr::IsNull { negated, .. } => {
            if *negated {
                0.9
            } else {
                0.1
            }
        }

        // String matching: moderate selectivity
        Expr::Like { .. } | Expr::StringMatch(..) => 0.2,

        // Default: unknown
        _ => 0.5,
    }
}

/// Extract (var, property_key) from Expr::Property(Expr::Var(v), key).
fn extract_var_prop(expr: &Expr) -> Option<(IStr, IStr)> {
    if let Expr::Property(target, key) = expr
        && let Expr::Var(var) = target.as_ref()
    {
        Some((*var, *key))
    } else {
        None
    }
}

/// Extract a literal GqlValue from an Expr::Literal.
fn extract_literal(expr: &Expr) -> Option<GqlValue> {
    if let Expr::Literal(v) = expr {
        Some(v.clone())
    } else {
        None
    }
}

/// Compute selectivity for var.key = value using property index cardinality.
/// Returns match_count / label_count. Falls back to 0.1 if no index exists.
fn index_selectivity(
    graph: &SeleneGraph,
    var_labels: &std::collections::HashMap<IStr, IStr>,
    var: IStr,
    key: IStr,
    value: &GqlValue,
) -> f64 {
    let Some(&label) = var_labels.get(&var) else {
        return 0.1;
    };
    let label_count = graph.nodes_by_label_count(label.as_str()) as f64;
    if label_count == 0.0 {
        return 0.0;
    }
    if let Ok(core_val) = selene_core::Value::try_from(value) {
        if let Some(matches) = graph.property_index_lookup(label, key, &core_val) {
            // Exact match found: use actual bucket size / label count.
            return (matches.len() as f64 / label_count).max(0.001);
        }
        // Value not found in index: use uniform estimate from distinct count.
        if let Some(index) = graph.property_index_entries(label, key) {
            let distinct = index.distinct_count().max(1) as f64;
            return (1.0 / distinct).max(0.001);
        }
    }
    0.1 // no index at all, heuristic for equality
}
