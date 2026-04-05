//! Multi-graph catalog -- manages multiple named property graphs.
//!
//! Each named graph is an independent `SharedGraph` with its own nodes, edges,
//! indexes, and schema. The "default" graph is the main graph; additional
//! graphs are created via `CREATE GRAPH` DDL.
//!
//! The encrypted vault (`USE secure`) is NOT part of this catalog --
//! it remains a separate security domain.

use std::collections::HashMap;

use crate::error::GraphError;
use crate::{SeleneGraph, SharedGraph};

/// Multi-graph catalog for managing named graphs.
pub struct GraphCatalog {
    /// Named graphs (excludes the default graph, which is held separately).
    graphs: HashMap<String, SharedGraph>,
}

impl GraphCatalog {
    pub fn new() -> Self {
        Self {
            graphs: HashMap::new(),
        }
    }

    /// Create a new empty named graph. Returns error if name already exists.
    pub fn create_graph(&mut self, name: &str) -> Result<(), GraphError> {
        if self.graphs.contains_key(name) {
            return Err(GraphError::AlreadyExists(format!(
                "graph '{name}' already exists"
            )));
        }
        let graph = SeleneGraph::new();
        self.graphs
            .insert(name.to_string(), SharedGraph::new(graph));
        Ok(())
    }

    /// Create a named graph, replacing if it already exists.
    pub fn create_or_replace_graph(&mut self, name: &str) -> Result<(), GraphError> {
        let graph = SeleneGraph::new();
        self.graphs
            .insert(name.to_string(), SharedGraph::new(graph));
        Ok(())
    }

    /// Drop a named graph. Returns error if not found.
    pub fn drop_graph(&mut self, name: &str) -> Result<(), GraphError> {
        if self.graphs.remove(name).is_none() {
            return Err(GraphError::NotFound(format!("graph '{name}' not found")));
        }
        Ok(())
    }

    /// Drop a named graph if it exists. No error if not found.
    pub fn drop_graph_if_exists(&mut self, name: &str) -> bool {
        self.graphs.remove(name).is_some()
    }

    /// Get a named graph by name (read-only reference).
    pub fn get_graph(&self, name: &str) -> Option<&SharedGraph> {
        self.graphs.get(name)
    }

    /// List all named graph names.
    pub fn list_graphs(&self) -> Vec<&str> {
        self.graphs.keys().map(|s| s.as_str()).collect()
    }

    /// Number of named graphs (excludes default).
    pub fn graph_count(&self) -> usize {
        self.graphs.len()
    }
}

impl Default for GraphCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_list() {
        let mut cat = GraphCatalog::new();
        cat.create_graph("analytics").unwrap();
        cat.create_graph("staging").unwrap();
        assert_eq!(cat.graph_count(), 2);
        assert!(cat.get_graph("analytics").is_some());
    }

    #[test]
    fn create_duplicate_fails() {
        let mut cat = GraphCatalog::new();
        cat.create_graph("test").unwrap();
        assert!(cat.create_graph("test").is_err());
    }

    #[test]
    fn drop_graph() {
        let mut cat = GraphCatalog::new();
        cat.create_graph("tmp").unwrap();
        cat.drop_graph("tmp").unwrap();
        assert_eq!(cat.graph_count(), 0);
    }

    #[test]
    fn drop_nonexistent_fails() {
        let mut cat = GraphCatalog::new();
        assert!(cat.drop_graph("nope").is_err());
    }
}
