#![deny(unsafe_code)]
//! selene-gql: ISO GQL (ISO/IEC 39075) query engine for Selene.
//!
//! Implements graph pattern matching, mutations, transactions, and
//! time-series procedures against Selene's in-memory property graph.
//!
//! # Quick Start
//!
//! ```ignore
//! use selene_gql::QueryBuilder;
//!
//! let result = QueryBuilder::new("MATCH (s:sensor) RETURN s.name AS name", &graph)
//!     .execute()?;
//! println!("{} rows", result.row_count());
//! ```
//!
//! # Architecture
//!
//! ```text
//! GQL text → Parser → AST → Planner → ExecutionPlan
//!                                         ↓
//!                              PatternPlan + PipelinePlan
//!                                    ↓            ↓
//!                           Pattern Executor  Pipeline Executor
//!                           (materialized)    (streaming)
//!                                    ↓
//!                           Mutation Executor (if write)
//!                                    ↓
//!                           Arrow RecordBatch
//! ```

pub(crate) mod ast;
pub(crate) mod parallel;
pub(crate) mod parser;
pub(crate) mod pattern;
pub(crate) mod pipeline;
pub(crate) mod planner;
pub mod runtime;
pub mod types;

/// Options controlling GQL execution behaviour.
///
/// `strict_coercion` (default `false`):
///   When `false`, implicit coercion is allowed (e.g. `"72" = 72` yields `true`).
///   When `true`, cross-type comparisons/arithmetic/concat with mismatched
///   types produce an error with a CAST hint instead of silently coercing.
///
/// `factorized` (default `true`):
///   When `true`, multi-hop patterns use factorized representations that
///   avoid materializing the full Cartesian product. Each expansion level
///   stores parent linkage pointers instead of replicating parent columns.
///   Falls back to flat execution for patterns containing VarExpand,
///   Optional, or Join with disjoint branches.
#[derive(Debug, Clone)]
pub struct GqlOptions {
    pub strict_coercion: bool,
    pub factorized: bool,
}

impl Default for GqlOptions {
    fn default() -> Self {
        Self {
            strict_coercion: false,
            factorized: true,
        }
    }
}

// ── Public API re-exports ────────────────────────────────────────
pub use runtime::cache::PlanCache;
pub use runtime::execute::{MutationBuilder, ParameterMap, QueryBuilder};
pub use runtime::explain::{ProfileStats, format_plan, format_profile};
pub use runtime::functions::{FunctionRegistry, ScalarFunction};
pub use runtime::scope::scope_to_bitmap;
pub use types::error::{GqlError, GqlStatus, MutationStats};
pub use types::result::GqlResult;
pub use types::value::{GqlValue, ZonedDateTime};

// ── Re-exports for server integration (EXPLAIN/PROFILE, statement routing) ──
pub use ast::statement::GqlStatement;
pub use parser::parse_statement;
pub use planner::{plan_mutation, plan_query};

// ── Mutation AST re-exports for server-side policy scanning ──
// Exposed so server code can walk a parsed mutation pipeline to enforce
// out-of-band policies (e.g., reserved-label reservation) without re-parsing.
pub use ast::mutation::{
    InsertElement, InsertGraphPattern, InsertPathPattern, MutationOp, MutationPipeline,
};
