//! GQL optimizer -- rule-based plan optimization pipeline.
//!
//! Each rule rewrites the ExecutionPlan, signaling via `Transformed<T>` whether
//! changes occurred. The optimizer loops rules until a fixed point (no changes).

use selene_core::IStr;

use selene_graph::SeleneGraph;

use crate::ast::expr::{CompareOp, Expr};
use crate::pattern::scan::PropertyFilter;
use crate::types::error::GqlError;

use super::plan;
use super::plan::*;

mod and_splitting;
mod composite_index;
mod constant_folding;
mod expand_filter_pushdown;
mod filter_interleaving;
mod filter_pushdown;
mod in_list;
mod index_order;
mod predicate_reorder;
mod range_index_scan;
mod symmetry_breaking;
mod topk;

pub use and_splitting::AndSplittingRule;
pub use composite_index::CompositeIndexLookupRule;
pub use constant_folding::ConstantFoldingRule;
pub use expand_filter_pushdown::ExpandFilterPushdownRule;
pub use filter_interleaving::FilterInterleavingRule;
pub use filter_pushdown::FilterPushdownRule;
pub use in_list::InListOptimizationRule;
pub use index_order::IndexOrderRule;
pub use predicate_reorder::PredicateReorderRule;
pub use range_index_scan::RangeIndexScanRule;
pub use symmetry_breaking::SymmetryBreakingRule;
pub use topk::TopKRule;

#[cfg(test)]
mod tests;

// Re-export helper functions used by tests
#[cfg(test)]
use filter_interleaving::contains_subquery;
#[cfg(test)]
use predicate_reorder::estimate_predicate_cost;

// ── OptimizeContext ────────────────────────────────────────────────

/// Context passed to optimizer rules. Provides access to the graph
/// for rules that need runtime data (e.g., index cardinality for
/// selectivity estimation).
pub(crate) struct OptimizeContext<'a> {
    pub graph: Option<&'a SeleneGraph>,
}

impl<'a> OptimizeContext<'a> {
    pub fn new(graph: &'a SeleneGraph) -> Self {
        Self { graph: Some(graph) }
    }
}

#[cfg(test)]
impl OptimizeContext<'static> {
    pub fn empty() -> Self {
        OptimizeContext { graph: None }
    }
}

// ── Transformed<T> ──────────────────────────────────────────────────

/// Result of an optimization rule: the plan and whether it changed.
pub(crate) struct Transformed<T> {
    pub data: T,
    pub changed: bool,
}

impl<T> Transformed<T> {
    pub fn yes(data: T) -> Self {
        Self {
            data,
            changed: true,
        }
    }
    pub fn no(data: T) -> Self {
        Self {
            data,
            changed: false,
        }
    }
}

// ── Rule trait ──────────────────────────────────────────────────────

/// An optimization rule that rewrites an ExecutionPlan.
pub(crate) trait GqlOptimizerRule: std::fmt::Debug {
    fn name(&self) -> &'static str;
    fn rewrite(
        &self,
        plan: ExecutionPlan,
        ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError>;
}

// ── Optimizer ───────────────────────────────────────────────────────

/// The optimizer -- runs rules in fixed-point loops until no changes.
pub(crate) struct GqlOptimizer {
    rules: Vec<Box<dyn GqlOptimizerRule>>,
    max_iterations: usize,
}

impl GqlOptimizer {
    /// Create an optimizer with the default rule set.
    pub fn with_default_rules() -> Self {
        Self {
            rules: vec![
                Box::new(ConstantFoldingRule),
                Box::new(AndSplittingRule),
                Box::new(FilterPushdownRule),
                Box::new(RangeIndexScanRule),
                Box::new(InListOptimizationRule),
                Box::new(ExpandFilterPushdownRule),
                Box::new(FilterInterleavingRule),
                Box::new(super::wco_rule::WcoJoinRule),
                Box::new(SymmetryBreakingRule),
                Box::new(PredicateReorderRule),
                Box::new(TopKRule),
                Box::new(IndexOrderRule),
                Box::new(CompositeIndexLookupRule),
            ],
            max_iterations: 8,
        }
    }

    /// Run all optimization rules until fixed point or max iterations.
    pub fn optimize(
        &self,
        mut plan: ExecutionPlan,
        ctx: &OptimizeContext<'_>,
    ) -> Result<ExecutionPlan, GqlError> {
        for _ in 0..self.max_iterations {
            let mut any_changed = false;
            for rule in &self.rules {
                let result = rule.rewrite(plan, ctx)?;
                if result.changed {
                    tracing::trace!(rule = rule.name(), "optimizer rule applied");
                }
                any_changed |= result.changed;
                plan = result.data;
            }
            if !any_changed {
                break;
            }
        }
        Ok(plan)
    }
}

#[cfg(test)]
impl GqlOptimizer {
    /// Create an optimizer with no rules (passthrough).
    pub fn empty() -> Self {
        Self {
            rules: vec![],
            max_iterations: 1,
        }
    }
}

// ── Shared helpers ──────────────────────────────────────────────────

/// Extract a pushable property filter from a comparison expression.
/// Used by both FilterPushdownRule and ExpandFilterPushdownRule.
pub(super) fn try_extract_pushable_filter(
    predicate: &Expr,
    scan_vars: &[IStr],
) -> Option<(IStr, PropertyFilter)> {
    match predicate {
        Expr::Compare(left, op, right) => {
            if let (Expr::Property(target, key), Expr::Literal(val)) =
                (left.as_ref(), right.as_ref())
                && let Expr::Var(var) = target.as_ref()
                && scan_vars.contains(var)
            {
                return Some((
                    *var,
                    PropertyFilter {
                        key: *key,
                        op: *op,
                        value: val.clone(),
                    },
                ));
            }
            if let (Expr::Literal(val), Expr::Property(target, key)) =
                (left.as_ref(), right.as_ref())
                && let Expr::Var(var) = target.as_ref()
                && scan_vars.contains(var)
            {
                let reversed_op = match op {
                    CompareOp::Eq => CompareOp::Eq,
                    CompareOp::Neq => CompareOp::Neq,
                    CompareOp::Lt => CompareOp::Gt,
                    CompareOp::Gt => CompareOp::Lt,
                    CompareOp::Lte => CompareOp::Gte,
                    CompareOp::Gte => CompareOp::Lte,
                };
                return Some((
                    *var,
                    PropertyFilter {
                        key: *key,
                        op: reversed_op,
                        value: val.clone(),
                    },
                ));
            }
            None
        }
        _ => None,
    }
}
