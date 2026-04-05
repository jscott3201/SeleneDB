#![forbid(unsafe_code)]
//! selene-graph: In-memory property graph with index management.

pub mod algorithms;
pub(crate) mod bitset;
pub mod change_applier;
pub mod changelog;
pub(crate) mod chunked_vec;
pub mod csr;
pub mod edge_statistics;
pub mod edge_store;
pub mod error;
pub mod graph;
pub mod hnsw;
pub mod multi_graph;
pub mod mutation;
pub mod node_store;
pub mod schema;
pub mod schema_compat;
pub mod schema_validate;
pub mod shared;
pub mod snapshot;
pub(crate) mod timestamp_column;
pub(crate) mod trigger;
pub mod typed_index;
pub mod view_registry;

pub use changelog::{ChangelogBuffer, ChangelogEntry};
pub use csr::{CsrAdjacency, CsrNeighbor};
pub use edge_statistics::EdgeStatistics;
pub use edge_store::{EdgeRef, EdgeStore};
pub use error::GraphError;
pub use graph::SeleneGraph;
pub use hnsw::HnswIndex;
pub use multi_graph::GraphCatalog;
pub use mutation::TrackedMutation;
pub use node_store::{NodeRef, NodeStore};
pub use schema::{SchemaValidator, ValidationIssue};
pub use shared::{SharedGraph, TransactionHandle};
pub use trigger::TriggerRegistry;
pub use view_registry::{
    ViewAggregate, ViewAggregateKind, ViewDefinition, ViewRegistry, ViewRegistryError,
};
