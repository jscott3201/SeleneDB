//! Filter interleaving optimization rule.
//!
//! Moves pipeline Filter ops between pattern Expand ops based on variable
//! dependency analysis. When a filter only depends on variables already
//! bound by earlier pattern ops, it can be interleaved before later
//! pattern ops, reducing the binding set size for expensive expansions.

use selene_core::IStr;

use crate::ast::expr::Expr;
use crate::types::error::GqlError;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct FilterInterleavingRule;

impl GqlOptimizerRule for FilterInterleavingRule {
    fn name(&self) -> &'static str {
        "FilterInterleaving"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        // Need at least 2 pattern ops for interleaving to matter
        if plan.pattern_ops.len() < 2 {
            return Ok(Transformed::no(plan));
        }

        // 1. Collect Join right-side ranges. Positions inside these ranges
        //    are executed independently (no access to left-side bindings),
        //    so we must not interleave filters into them.
        let mut join_ranges: Vec<std::ops::Range<usize>> = Vec::new();
        for op in &plan.pattern_ops {
            if let PatternOp::Join {
                right_start,
                right_end,
                ..
            } = op
            {
                join_ranges.push(*right_start..*right_end);
            }
        }
        let in_join_range = |pos: usize| -> bool { join_ranges.iter().any(|r| r.contains(&pos)) };

        // 2. Build cumulative bound-variable sets per pattern op position
        let mut bound_at: Vec<std::collections::HashSet<IStr>> = Vec::new();
        let mut cumulative = std::collections::HashSet::new();
        for op in &plan.pattern_ops {
            op.collect_vars(&mut cumulative);
            bound_at.push(cumulative.clone());
        }

        // 3. For each pipeline Filter, check if it can be interleaved
        let mut to_interleave: Vec<(usize, Expr)> = Vec::new();
        plan.pipeline.retain(|op| {
            if let PipelineOp::Filter { predicate } = op {
                // Skip complex expressions that may have side effects or
                // depend on full context (subqueries, aggregation)
                if contains_subquery(predicate) {
                    return true;
                }

                let required_vars = extract_referenced_vars(predicate);
                if required_vars.is_empty() {
                    return true; // no variable references, keep in pipeline
                }

                // Find earliest position where all required vars are bound
                for (pos, bound) in bound_at.iter().enumerate() {
                    if required_vars.iter().all(|v| bound.contains(v)) {
                        let insert_pos = pos + 1;
                        // Only interleave if: (a) not at the end of pattern ops,
                        // and (b) the insertion point is not inside a Join's
                        // right-side range (which executes independently).
                        if insert_pos < plan.pattern_ops.len() && !in_join_range(insert_pos) {
                            to_interleave.push((insert_pos, predicate.clone()));
                            return false; // remove from pipeline
                        }
                        break;
                    }
                }
            }
            true
        });

        if to_interleave.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // 4. Insert IntermediateFilters in reverse order to maintain
        //    insertion positions, then adjust Join indices for each insert.
        to_interleave.sort_by(|a, b| b.0.cmp(&a.0));
        for (pos, predicate) in to_interleave {
            plan.pattern_ops
                .insert(pos, PatternOp::IntermediateFilter { predicate });

            // Adjust all Join right_start/right_end for this insertion
            for op in &mut plan.pattern_ops {
                if let PatternOp::Join {
                    right_start,
                    right_end,
                    ..
                } = op
                {
                    if *right_start >= pos {
                        *right_start += 1;
                    }
                    if *right_end >= pos {
                        *right_end += 1;
                    }
                }
            }
        }

        Ok(Transformed::yes(plan))
    }
}

/// Extract all variable names referenced in an expression.
fn extract_referenced_vars(expr: &Expr) -> std::collections::HashSet<IStr> {
    let mut vars = std::collections::HashSet::new();
    collect_vars_from_expr(expr, &mut vars);
    vars
}

fn collect_vars_from_expr(expr: &Expr, vars: &mut std::collections::HashSet<IStr>) {
    expr.walk(&mut |e| {
        if let Expr::Var(v) = e {
            vars.insert(*v);
        }
    });
}

/// Check if an expression contains a subquery (EXISTS, COUNT, VALUE, COLLECT).
pub(super) fn contains_subquery(expr: &Expr) -> bool {
    let mut found = false;
    expr.walk(&mut |e| {
        if !found {
            match e {
                Expr::Exists { .. }
                | Expr::CountSubquery(_)
                | Expr::ValueSubquery(_)
                | Expr::CollectSubquery(_) => found = true,
                _ => {}
            }
        }
    });
    found
}
