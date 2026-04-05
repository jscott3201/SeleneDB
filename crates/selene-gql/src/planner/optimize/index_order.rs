//! Index order optimization rule.
//!
//! Detects a single LabelScan with a TopK (single sort term) referencing
//! a property on the scan variable. Sets an `index_order` hint so the
//! scan executor can use a BTreeMap index for ordered retrieval instead
//! of scanning all nodes and sorting.

use selene_core::IStr;

use crate::ast::expr::Expr;
use crate::types::error::GqlError;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct IndexOrderRule;

impl GqlOptimizerRule for IndexOrderRule {
    fn name(&self) -> &'static str {
        "IndexOrder"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        // Precondition: single LabelScan pattern op.
        // Property filters (from FilterPushdownRule) are allowed -- they
        // will be evaluated inline during the ordered index scan.
        // Inline props (equality checks embedded in the pattern) are not
        // supported by the ordered scan path.
        if plan.pattern_ops.len() != 1 {
            return Ok(Transformed::no(plan));
        }
        let PatternOp::LabelScan {
            var,
            index_order,
            inline_props,
            ..
        } = &plan.pattern_ops[0]
        else {
            return Ok(Transformed::no(plan));
        };
        if index_order.is_some() {
            return Ok(Transformed::no(plan)); // already applied
        }
        if !inline_props.is_empty() {
            return Ok(Transformed::no(plan)); // inline props need different handling
        }
        let scan_var = *var;

        // Find TopK in the pipeline with a single sort term that is
        // a simple property access on the scan variable.
        let topk_idx = plan
            .pipeline
            .iter()
            .position(|op| matches!(op, PipelineOp::TopK { .. }));
        let Some(topk_idx) = topk_idx else {
            return Ok(Transformed::no(plan));
        };
        let PipelineOp::TopK { terms, limit } = &plan.pipeline[topk_idx] else {
            unreachable!();
        };
        if terms.len() != 1 {
            return Ok(Transformed::no(plan));
        }
        let term = &terms[0];
        let limit = *limit as usize;

        // Check if sort expression is var.property
        let Some(sort_key) = extract_simple_property(&term.expr, scan_var) else {
            return Ok(Transformed::no(plan));
        };

        // Apply: set index_order hint on LabelScan.
        // Keep the TopK in the pipeline as a safety net -- if the index
        // doesn't exist at execution time, the fallback scan needs the
        // TopK to provide correct sort + limit semantics. When the index
        // scan succeeds, TopK receives pre-sorted K items (near-zero cost).
        if let PatternOp::LabelScan { index_order, .. } = &mut plan.pattern_ops[0] {
            *index_order = Some(super::plan::IndexOrder {
                key: sort_key,
                descending: term.descending,
                limit,
            });
        }

        Ok(Transformed::yes(plan))
    }
}

/// Extract the property key if expr is `var.property`.
fn extract_simple_property(expr: &Expr, expected_var: IStr) -> Option<IStr> {
    if let Expr::Property(target, key) = expr
        && let Expr::Var(v) = target.as_ref()
        && *v == expected_var
    {
        return Some(*key);
    }
    None
}
