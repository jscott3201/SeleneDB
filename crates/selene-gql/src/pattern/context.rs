//! Pattern execution context with Sideways Information Passing (SIP) bitmaps.
//!
//! After each pattern operator produces results, the context builds RoaringBitmaps
//! of which NodeIds are valid for each bound variable. Subsequent Expand operators
//! check these bitmaps to skip adjacency lookups for nodes that were already filtered
//! out by earlier operators.

use std::collections::HashMap;

use arrow::array::Array;
use roaring::RoaringBitmap;
use selene_core::IStr;
use selene_core::entity::NodeId;

use crate::types::binding::{Binding, BoundValue};
use crate::types::chunk::{ColumnKind, DataChunk};

/// Context threaded through pattern operator execution.
/// Carries SIP (Sideways Information Passing) bitmaps that allow
/// later operators to skip work based on earlier operator results.
pub(crate) struct PatternContext {
    /// Bitmaps of valid NodeIds per bound variable.
    /// Built from previous pattern op results.
    sip_filters: HashMap<IStr, RoaringBitmap>,
}

impl PatternContext {
    pub fn new() -> Self {
        Self {
            sip_filters: HashMap::new(),
        }
    }

    /// Update SIP filters from result bindings.
    /// For each listed variable, collect all bound NodeIds into a bitmap.
    #[allow(dead_code)]
    pub fn update_from_bindings(&mut self, bindings: &[Binding], vars: &[IStr]) {
        for var in vars {
            let mut bitmap = RoaringBitmap::new();
            for b in bindings {
                if let Some(BoundValue::Node(nid)) = b.get(var) {
                    bitmap.insert(nid.0 as u32);
                }
            }
            // Only set filter if we have results; empty bitmaps would filter everything
            if !bitmap.is_empty() {
                self.sip_filters.insert(*var, bitmap);
            }
        }
    }

    /// Check if a node is valid for a variable's SIP filter.
    /// Returns true if no filter exists (open) or the node is in the filter.
    pub fn check(&self, var: IStr, node_id: NodeId) -> bool {
        self.sip_filters
            .get(&var)
            .is_none_or(|bitmap| bitmap.contains(node_id.0 as u32))
    }

    /// Update SIP filters from a DataChunk's NodeId columns.
    ///
    /// Reads NodeId values directly from the chunk's UInt64Array columns
    /// (zero allocation per value, vs per-binding get() in the row path).
    pub fn update_from_chunk(&mut self, chunk: &DataChunk, vars: &[IStr]) {
        for var in vars {
            if let Some(slot) = chunk.schema().slot_of(var)
                && chunk.schema().kind_at(slot) == Some(&ColumnKind::NodeId)
            {
                let mut bitmap = RoaringBitmap::new();
                if let Ok(arr) = chunk.node_id_column(var) {
                    for row_idx in chunk.active_indices() {
                        if !arr.is_null(row_idx) {
                            bitmap.insert(arr.value(row_idx) as u32);
                        }
                    }
                }
                if !bitmap.is_empty() {
                    self.sip_filters.insert(*var, bitmap);
                }
            }
        }
    }

    /// Whether any SIP filters have been set.
    pub fn has_filters(&self) -> bool {
        !self.sip_filters.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sip_check_no_filter_returns_true() {
        let ctx = PatternContext::new();
        assert!(ctx.check(IStr::new("x"), NodeId(42)));
    }

    #[test]
    fn sip_check_with_filter() {
        let var = IStr::new("x");
        let bindings = vec![
            Binding::single(var, BoundValue::Node(NodeId(1))),
            Binding::single(var, BoundValue::Node(NodeId(3))),
        ];

        let mut ctx = PatternContext::new();
        ctx.update_from_bindings(&bindings, &[var]);

        assert!(ctx.check(var, NodeId(1)));
        assert!(ctx.check(var, NodeId(3)));
        assert!(!ctx.check(var, NodeId(2)));
        assert!(!ctx.check(var, NodeId(99)));
    }

    #[test]
    fn sip_unrelated_var_not_affected() {
        let var_a = IStr::new("a");
        let var_b = IStr::new("b");
        let bindings = vec![Binding::single(var_a, BoundValue::Node(NodeId(1)))];

        let mut ctx = PatternContext::new();
        ctx.update_from_bindings(&bindings, &[var_a]);

        // var_b has no filter, so any NodeId is valid
        assert!(ctx.check(var_b, NodeId(999)));
    }

    #[test]
    fn sip_empty_bindings_no_filter() {
        let var = IStr::new("x");
        let mut ctx = PatternContext::new();
        ctx.update_from_bindings(&[], &[var]);

        // Empty bindings should NOT set a filter (would block everything)
        assert!(ctx.check(var, NodeId(1)));
    }
}
