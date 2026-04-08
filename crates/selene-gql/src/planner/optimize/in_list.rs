//! IN-list optimization rule.
//!
//! Detects literal IN-lists on scan variable properties and produces an
//! InListHint for multi-probe index lookups. Removes the consumed filter
//! from the pipeline and assigns the hint to the matching LabelScan.

use selene_core::IStr;

use crate::ast::expr::Expr;
use crate::ast::pattern::LabelExpr;
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct InListOptimizationRule;

impl GqlOptimizerRule for InListOptimizationRule {
    fn name(&self) -> &'static str {
        "InListOptimization"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        // Collect scan variables
        let scan_vars: Vec<(IStr, Option<IStr>)> = plan
            .pattern_ops
            .iter()
            .filter_map(|op| match op {
                PatternOp::LabelScan { var, labels, .. } => {
                    let label = labels.as_ref().and_then(|l| match l {
                        LabelExpr::Name(n) => IStr::try_get(n.as_str()),
                        _ => None,
                    });
                    Some((*var, label))
                }
                _ => None,
            })
            .collect();

        if scan_vars.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let mut changed = false;
        let mut hints: Vec<(IStr, InListHint)> = Vec::new();
        // Track which scan variables already have a hint queued.
        // Only the first IN-list per variable is claimed; subsequent ones
        // stay in the pipeline as fallback filters.
        let mut claimed_vars: std::collections::HashSet<IStr> = std::collections::HashSet::new();

        plan.pipeline.retain(|op| {
            if let PipelineOp::Filter { predicate } = op
                && let Expr::InList {
                    expr,
                    list,
                    negated,
                } = predicate
                && !negated
            {
                // Check if expr is a property on a scan variable
                if let Expr::Property(target, key) = expr.as_ref()
                    && let Expr::Var(var) = target.as_ref()
                    && scan_vars.iter().any(|(sv, _)| sv == var)
                {
                    // Only claim first IN-list per scan variable
                    if claimed_vars.contains(var) {
                        return true; // keep in pipeline
                    }

                    // Check all list elements are literals
                    let literals: Vec<GqlValue> = list
                        .iter()
                        .filter_map(|e| match e {
                            Expr::Literal(v) => Some(v.clone()),
                            _ => None,
                        })
                        .collect();

                    if literals.len() == list.len() {
                        // Only claim this filter (removing it from the pipeline)
                        // if a property index exists for the target property.
                        // Without an index, the in_list_hint produces an empty
                        // bitmap and silently drops all results.
                        let has_index = ctx
                            .graph
                            .and_then(|g| {
                                let label = scan_vars
                                    .iter()
                                    .find(|(sv, _)| sv == var)
                                    .and_then(|(_, l)| *l)?;
                                Some(g.has_property_index(label, *key))
                            })
                            .unwrap_or(false);

                        if has_index {
                            claimed_vars.insert(*var);
                            hints.push((
                                *var,
                                InListHint {
                                    key: *key,
                                    values: literals,
                                },
                            ));
                            changed = true;
                            return false; // remove from pipeline
                        }
                    }
                }
            }
            true
        });

        // Assign hints to the matching LabelScan
        for (var, hint) in hints {
            for op in &mut plan.pattern_ops {
                if let PatternOp::LabelScan {
                    var: scan_var,
                    in_list_hint,
                    ..
                } = op
                    && *scan_var == var
                {
                    *in_list_hint = Some(hint);
                    break;
                }
            }
        }

        Ok(if changed {
            Transformed::yes(plan)
        } else {
            Transformed::no(plan)
        })
    }
}
