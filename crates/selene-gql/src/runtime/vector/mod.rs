//! Vectorized expression evaluation: batch property gathering and columnar
//! expression dispatch for the DataChunk pipeline.
//!
//! This module replaces the per-row `RowView.to_binding()` + `eval_expr_ctx()`
//! path with column-level operations. Each expression node produces a `Column`
//! rather than a scalar `GqlValue`, amortizing dispatch overhead across all
//! active rows in a chunk.

pub(crate) mod expr;
pub(crate) mod gather;

pub(crate) use expr::eval_vec;
