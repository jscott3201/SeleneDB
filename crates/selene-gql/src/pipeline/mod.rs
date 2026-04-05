//! Pipeline execution: LET, FILTER, ORDER BY, OFFSET, LIMIT, RETURN.
//!
//! Processes materialized bindings from pattern matching through a sequence
//! of streaming operators and pipeline breakers.

pub mod stages;
