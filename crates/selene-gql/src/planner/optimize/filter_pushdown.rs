//! Filter pushdown optimization rule.
//!
//! Pushes equality and inequality property filters from the pipeline into
//! LabelScan pattern ops, enabling index-accelerated filtering during the
//! scan phase rather than post-scan pipeline filtering.

use selene_core::IStr;

use crate::pattern::scan::PropertyFilter;
use crate::types::error::GqlError;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed, try_extract_pushable_filter};

#[derive(Debug)]
pub struct FilterPushdownRule;

impl GqlOptimizerRule for FilterPushdownRule {
    fn name(&self) -> &'static str {
        "FilterPushdown"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        let scan_vars: Vec<IStr> = plan
            .pattern_ops
            .iter()
            .filter_map(|op| match op {
                PatternOp::LabelScan { var, .. } => Some(*var),
                _ => None,
            })
            .collect();

        if scan_vars.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let mut changed = false;
        let mut pushed: Vec<(IStr, PropertyFilter)> = Vec::new();

        plan.pipeline.retain(|op| {
            if let PipelineOp::Filter { predicate } = op
                && let Some((var, filter)) = try_extract_pushable_filter(predicate, &scan_vars)
            {
                pushed.push((var, filter));
                changed = true;
                return false;
            }
            true
        });

        for (var, filter) in pushed {
            for op in &mut plan.pattern_ops {
                if let PatternOp::LabelScan {
                    var: scan_var,
                    property_filters,
                    ..
                } = op
                    && *scan_var == var
                {
                    property_filters.push(filter);
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
