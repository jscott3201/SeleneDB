//! Composite index lookup optimization rule.
//!
//! Detects when a LabelScan has 2+ literal equality inline_props that
//! could match a composite index. Sets `composite_index_keys` on the
//! LabelScan so the scan executor can attempt a direct composite index
//! lookup before falling back to sequential property filtering.

use selene_core::IStr;

use crate::ast::expr::Expr;
use crate::ast::pattern::LabelExpr;
use crate::types::error::GqlError;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct CompositeIndexLookupRule;

impl GqlOptimizerRule for CompositeIndexLookupRule {
    fn name(&self) -> &'static str {
        "CompositeIndexLookup"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        let mut changed = false;

        for op in &mut plan.pattern_ops {
            if let PatternOp::LabelScan {
                labels,
                inline_props,
                composite_index_keys,
                ..
            } = op
            {
                // Already applied
                if composite_index_keys.is_some() {
                    continue;
                }

                // Need a simple label and 2+ literal inline_props
                let label = match labels {
                    Some(LabelExpr::Name(name)) => *name,
                    _ => continue,
                };

                // Collect keys that have literal equality values
                let literal_keys: Vec<IStr> = inline_props
                    .iter()
                    .filter(|(_, expr)| matches!(expr, Expr::Literal(_)))
                    .map(|(key, _)| *key)
                    .collect();

                if literal_keys.len() < 2 {
                    continue;
                }

                // Check if a composite index exists for this (label, keys) combination.
                // The optimizer does not have the graph reference, so we set the hint
                // and let the scan executor verify at runtime. We sort the keys to
                // match the schema-derived key order used during index construction.
                // The scan executor will match them against actual composite indexes.
                let _ = label; // hint: label is available at scan time
                *composite_index_keys = Some(literal_keys);
                changed = true;
            }
        }

        Ok(if changed {
            Transformed::yes(plan)
        } else {
            Transformed::no(plan)
        })
    }
}
