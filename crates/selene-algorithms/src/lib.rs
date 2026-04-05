#![forbid(unsafe_code)]
//! Graph algorithms for Selene.
//!
//! All algorithms operate on [`GraphProjection`] views (or raw `&SeleneGraph`).
//! Pure read-only functions. Auth scope is enforced at projection creation
//! time via bitmap intersection.
//!
//! # Crate organization
//!
//! - [`projection`] - `GraphProjection` (filtered subgraph with CSR adjacency)
//! - [`catalog`] - `ProjectionCatalog` (named projection store with invalidation)
//! - [`structural`] - WCC, SCC, topological sort, articulation points, bridges
//! - [`pathfinding`] - Dijkstra, SSSP, APSP
//! - [`centrality`] - PageRank, betweenness centrality
//! - [`community`] - Label propagation, Louvain modularity, triangle count

pub mod catalog;
pub mod centrality;
pub mod community;
pub mod pathfinding;
pub mod projection;
pub mod structural;

pub use catalog::ProjectionCatalog;
pub use centrality::{betweenness, pagerank};
pub use community::{label_propagation, louvain, triangle_count};
pub use pathfinding::{ApspError, PathResult, apsp, dijkstra, sssp};
pub use projection::{GraphProjection, ProjNeighbor, ProjectionConfig};
pub use structural::{
    ContainmentIndex, IssueSeverity, TopoSortError, ValidationIssue, articulation_points, bridges,
    scc, scc_count, topological_sort, validate, wcc, wcc_count,
};
