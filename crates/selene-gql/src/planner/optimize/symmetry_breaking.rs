//! Symmetry breaking optimization rule.
//!
//! For undirected MATCH patterns like `(a:L)-[:E]-(b:L)`, both bindings
//! `(a=1, b=2)` and `(a=2, b=1)` are produced. This rule injects a
//! `var1.id < var2.id` filter to eliminate the symmetric duplicate,
//! yielding a ~2x speedup on affected queries.

use selene_core::IStr;

use crate::ast::expr::{CompareOp, Expr};
use crate::ast::pattern::{EdgeDirection, LabelExpr};
use crate::types::error::GqlError;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

/// Applicability conditions:
/// - The edge is undirected (`EdgeDirection::Any`)
/// - Both endpoint variables have the same simple label (`LabelExpr::Name`)
/// - No existing filter already compares the `id` of the two variables
#[derive(Debug)]
pub struct SymmetryBreakingRule;

impl GqlOptimizerRule for SymmetryBreakingRule {
    fn name(&self) -> &'static str {
        "SymmetryBreaking"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        let mut changed = false;

        // Collect (source_var, target_var) pairs from undirected Expand ops.
        let undirected_pairs: Vec<(IStr, IStr)> = plan
            .pattern_ops
            .iter()
            .filter_map(|op| {
                if let PatternOp::Expand {
                    source_var,
                    target_var,
                    direction: EdgeDirection::Any,
                    ..
                } = op
                {
                    Some((*source_var, *target_var))
                } else {
                    None
                }
            })
            .collect();

        if undirected_pairs.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // Build a lookup: var -> label IStr (only for LabelScan with a simple Name label).
        let scan_labels: Vec<(IStr, IStr)> = plan
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

        // For target nodes of Expand ops, labels come from Expand.target_labels
        // (the planner does not emit a separate LabelScan for them).
        let expand_target_labels: Vec<(IStr, IStr)> = plan
            .pattern_ops
            .iter()
            .filter_map(|op| {
                if let PatternOp::Expand {
                    target_var,
                    target_labels: Some(LabelExpr::Name(label)),
                    ..
                } = op
                {
                    Some((*target_var, *label))
                } else {
                    None
                }
            })
            .collect();

        let label_for = |v: IStr| -> Option<IStr> {
            scan_labels
                .iter()
                .find(|(var, _)| *var == v)
                .or_else(|| expand_target_labels.iter().find(|(var, _)| *var == v))
                .map(|(_, l)| *l)
        };

        for (src, tgt) in &undirected_pairs {
            let src_label = label_for(*src);
            let tgt_label = label_for(*tgt);

            // Only apply when both endpoints have the same simple label.
            let (Some(sl), Some(tl)) = (src_label, tgt_label) else {
                continue;
            };
            if sl != tl {
                continue;
            }

            // Determine canonical ordering: lexicographic by resolved string
            // for deterministic, insertion-order-independent results.
            let (lo, hi) = if src.as_str() <= tgt.as_str() {
                (*src, *tgt)
            } else {
                (*tgt, *src)
            };

            // Skip if there is already a filter comparing id(lo) and id(hi).
            let already_filtered = plan.pipeline.iter().any(|op| {
                if let PipelineOp::Filter { predicate } = op {
                    is_id_comparison(predicate, lo, hi)
                } else {
                    false
                }
            });
            if already_filtered {
                continue;
            }

            // Inject: lo.id < hi.id
            let id_key = IStr::new("id");
            let filter = PipelineOp::Filter {
                predicate: Expr::Compare(
                    Box::new(Expr::Property(Box::new(Expr::Var(lo)), id_key)),
                    CompareOp::Lt,
                    Box::new(Expr::Property(Box::new(Expr::Var(hi)), id_key)),
                ),
            };

            // Insert the symmetry-breaking filter at the front of the pipeline
            // so it runs as early as possible.
            plan.pipeline.insert(0, filter);
            changed = true;
        }

        Ok(if changed {
            Transformed::yes(plan)
        } else {
            Transformed::no(plan)
        })
    }
}

/// Check whether an expression is a comparison of `a.id` and `b.id`
/// (in either operand order) using any comparison operator.
fn is_id_comparison(expr: &Expr, var_a: IStr, var_b: IStr) -> bool {
    if let Expr::Compare(left, _, right) = expr {
        let left_var = extract_id_var(left);
        let right_var = extract_id_var(right);
        if let (Some(lv), Some(rv)) = (left_var, right_var) {
            return (lv == var_a && rv == var_b) || (lv == var_b && rv == var_a);
        }
    }
    false
}

/// If `expr` is `var.id` (property access with key "id"), return the variable.
/// Also handles the function-call form `id(var)`.
fn extract_id_var(expr: &Expr) -> Option<IStr> {
    if let Expr::Property(target, key) = expr
        && key.as_str() == "id"
        && let Expr::Var(v) = target.as_ref()
    {
        return Some(*v);
    }
    if let Expr::Function(f) = expr
        && f.name.as_str() == "id"
        && f.args.len() == 1
        && let Expr::Var(v) = &f.args[0]
    {
        return Some(*v);
    }
    None
}
