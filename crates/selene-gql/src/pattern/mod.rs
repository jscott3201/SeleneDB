//! Graph pattern matching -- LabelScan, Expand, VarExpand, Join.
//!
//! Operators that materialize bindings by exploring the graph's
//! RoaringBitmap label indexes and adjacency lists.

pub mod context;
pub mod expand;
pub mod factorized_expand;
pub mod join;
pub mod scan;
pub mod varlength;
pub mod wco;
