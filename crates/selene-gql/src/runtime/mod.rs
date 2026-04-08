//! GQL execution runtime: expression evaluation, context, transactions.

pub mod cache;
pub mod embed;
pub mod eval;
pub(crate) mod eval_aggregate;
pub(crate) mod eval_arithmetic;
pub(crate) mod eval_helpers;
pub mod execute;
pub mod explain;
pub mod functions;
pub mod procedures;
pub mod scope;
pub mod triggers;
pub mod vector;
