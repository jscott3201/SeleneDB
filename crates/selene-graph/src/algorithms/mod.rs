//! Graph algorithms operating directly on `SeleneGraph` in-memory data.
//!
//! All functions take `&SeleneGraph` -- pure read-only operations
//! with no database round-trips.

pub mod containment;
pub mod path;
pub mod traversal;

// Re-export all functions at the module level for backwards compatibility.
pub use containment::{containment_children, containment_walk_up, walk_ancestors};
pub use path::shortest_path;
pub use traversal::{bfs, bfs_with_depth, dfs, reachable};
