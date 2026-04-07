//! TopK optimization rule.
//!
//! Fuses adjacent ORDER BY + LIMIT pipeline ops into a single TopK op
//! that uses a bounded heap, avoiding a full sort of the result set.

use crate::types::error::GqlError;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct TopKRule;

impl GqlOptimizerRule for TopKRule {
    fn name(&self) -> &'static str {
        "TopK"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        let mut i = 0;
        let mut changed = false;
        while i + 1 < plan.pipeline.len() {
            if let (PipelineOp::Sort { terms }, PipelineOp::Limit { value }) =
                (&plan.pipeline[i], &plan.pipeline[i + 1])
            {
                let terms = terms.clone();
                let limit = value.clone();
                plan.pipeline[i] = PipelineOp::TopK { terms, limit };
                plan.pipeline.remove(i + 1);
                changed = true;
            }
            i += 1;
        }
        Ok(if changed {
            Transformed::yes(plan)
        } else {
            Transformed::no(plan)
        })
    }
}
