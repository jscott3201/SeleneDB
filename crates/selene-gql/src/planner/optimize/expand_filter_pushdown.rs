//! Expand filter pushdown optimization rule.
//!
//! Pushes property filters on target and edge variables into the Expand op
//! for pre-clone filtering. This avoids materializing bindings for edges
//! and target nodes that would be immediately filtered out.

use selene_core::IStr;

use crate::pattern::scan::PropertyFilter;
use crate::types::error::GqlError;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed, try_extract_pushable_filter};

#[derive(Debug)]
pub struct ExpandFilterPushdownRule;

impl GqlOptimizerRule for ExpandFilterPushdownRule {
    fn name(&self) -> &'static str {
        "ExpandFilterPushdown"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        // Build a map: variable -> (index in pattern_ops, role: target/edge)
        let mut var_to_expand: Vec<(IStr, usize, bool)> = Vec::new(); // (var, idx, is_target)
        for (i, op) in plan.pattern_ops.iter().enumerate() {
            if let PatternOp::Expand {
                target_var,
                edge_var,
                ..
            } = op
            {
                var_to_expand.push((*target_var, i, true));
                if let Some(ev) = edge_var {
                    var_to_expand.push((*ev, i, false));
                }
            }
        }

        if var_to_expand.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let mut changed = false;
        let mut pushed: Vec<(usize, bool, PropertyFilter)> = Vec::new(); // (expand_idx, is_target, filter)

        plan.pipeline.retain(|op| {
            if let PipelineOp::Filter { predicate } = op {
                // Reuse try_extract_pushable_filter but check against expand variables
                let all_vars: Vec<IStr> = var_to_expand.iter().map(|(v, _, _)| *v).collect();
                if let Some((var, filter)) = try_extract_pushable_filter(predicate, &all_vars) {
                    // Find which expand and role
                    if let Some((_, idx, is_target)) =
                        var_to_expand.iter().find(|(v, _, _)| *v == var)
                    {
                        pushed.push((*idx, *is_target, filter));
                        changed = true;
                        return false;
                    }
                }
            }
            true
        });

        // Apply pushed filters to the Expand ops
        for (idx, is_target, filter) in pushed {
            if let PatternOp::Expand {
                target_property_filters,
                edge_property_filters,
                ..
            } = &mut plan.pattern_ops[idx]
            {
                if is_target {
                    target_property_filters.push(filter);
                } else {
                    edge_property_filters.push(filter);
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
