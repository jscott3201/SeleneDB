//! AND splitting optimization rule.
//!
//! Splits conjunctive (AND) filter predicates into separate Filter ops
//! so that downstream rules (e.g. filter pushdown) can independently
//! push each conjunct into the appropriate pattern op.

use crate::ast::expr::{Expr, LogicOp};
use crate::types::error::GqlError;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct AndSplittingRule;

impl GqlOptimizerRule for AndSplittingRule {
    fn name(&self) -> &'static str {
        "AndSplitting"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        let mut new_pipeline = Vec::new();
        let mut changed = false;
        for op in plan.pipeline {
            match op {
                PipelineOp::Filter { predicate } => {
                    let conjuncts = split_and(&predicate);
                    if conjuncts.len() > 1 {
                        changed = true;
                        for c in conjuncts {
                            new_pipeline.push(PipelineOp::Filter { predicate: c });
                        }
                    } else {
                        new_pipeline.push(PipelineOp::Filter { predicate });
                    }
                }
                other => new_pipeline.push(other),
            }
        }
        plan.pipeline = new_pipeline;
        Ok(if changed {
            Transformed::yes(plan)
        } else {
            Transformed::no(plan)
        })
    }
}

fn split_and(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::Logic(left, LogicOp::And, right) => {
            let mut result = split_and(left);
            result.extend(split_and(right));
            result
        }
        _ => vec![expr.clone()],
    }
}
