//! Range index scan optimization rule.
//!
//! Converts range PropertyFilters (GT, GTE, LT, LTE) on a LabelScan into
//! a RangeIndexHint, enabling the scan executor to use BTreeMap::range()
//! for efficient bounded scans instead of full label scans with post-filtering.

use crate::ast::expr::CompareOp;
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

use super::plan::*;
use super::{GqlOptimizerRule, OptimizeContext, Transformed};

#[derive(Debug)]
pub struct RangeIndexScanRule;

impl GqlOptimizerRule for RangeIndexScanRule {
    fn name(&self) -> &'static str {
        "RangeIndexScan"
    }

    fn rewrite(
        &self,
        mut plan: ExecutionPlan,
        _ctx: &OptimizeContext<'_>,
    ) -> Result<Transformed<ExecutionPlan>, GqlError> {
        let mut changed = false;

        for op in &mut plan.pattern_ops {
            let PatternOp::LabelScan {
                property_filters,
                range_index_hint,
                ..
            } = op
            else {
                continue;
            };

            // Skip if a range hint already exists (idempotent)
            if range_index_hint.is_some() {
                continue;
            }

            // Group range filters by key. We only produce one hint (the first
            // eligible key) to keep the implementation simple.
            let mut best_key: Option<selene_core::IStr> = None;
            let mut lower: Option<(GqlValue, bool)> = None;
            let mut upper: Option<(GqlValue, bool)> = None;
            let mut consumed_indices: Vec<usize> = Vec::new();

            for (i, f) in property_filters.iter().enumerate() {
                match f.op {
                    CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {}
                    _ => continue, // only range ops
                }

                let key = f.key;
                // If we haven't picked a key yet, pick this one.
                // If we already have a key, only merge filters on the same key.
                if let Some(bk) = best_key {
                    if bk != key {
                        continue;
                    }
                } else {
                    best_key = Some(key);
                }

                // Keep tighter bound when multiple same-direction filters
                // exist (e.g., temp > 70 AND temp > 50 keeps 70).
                match f.op {
                    CompareOp::Gt => {
                        let tighter = lower.as_ref().is_none_or(|(existing, _)| {
                            f.value
                                .gql_order(existing)
                                .is_ok_and(|ord| ord == std::cmp::Ordering::Greater)
                        });
                        if tighter {
                            lower = Some((f.value.clone(), false));
                        }
                        consumed_indices.push(i);
                    }
                    CompareOp::Gte => {
                        let tighter = lower.as_ref().is_none_or(|(existing, incl)| {
                            f.value.gql_order(existing).is_ok_and(|ord| {
                                ord == std::cmp::Ordering::Greater
                                    || (ord == std::cmp::Ordering::Equal && !incl)
                            })
                        });
                        if tighter {
                            lower = Some((f.value.clone(), true));
                        }
                        consumed_indices.push(i);
                    }
                    CompareOp::Lt => {
                        let tighter = upper.as_ref().is_none_or(|(existing, _)| {
                            f.value
                                .gql_order(existing)
                                .is_ok_and(|ord| ord == std::cmp::Ordering::Less)
                        });
                        if tighter {
                            upper = Some((f.value.clone(), false));
                        }
                        consumed_indices.push(i);
                    }
                    CompareOp::Lte => {
                        let tighter = upper.as_ref().is_none_or(|(existing, incl)| {
                            f.value.gql_order(existing).is_ok_and(|ord| {
                                ord == std::cmp::Ordering::Less
                                    || (ord == std::cmp::Ordering::Equal && !incl)
                            })
                        });
                        if tighter {
                            upper = Some((f.value.clone(), true));
                        }
                        consumed_indices.push(i);
                    }
                    _ => {}
                }
            }

            if let Some(key) = best_key
                && (lower.is_some() || upper.is_some())
            {
                *range_index_hint = Some(RangeIndexHint { key, lower, upper });
                // Keep property_filters intact as a fallback -- the range hint
                // narrows the bitmap when an index exists, but the filters still
                // apply if no index is available at runtime. The redundant check
                // on indexed nodes is cheap (O(result_set) comparisons).
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
