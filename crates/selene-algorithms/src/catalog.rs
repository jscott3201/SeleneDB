//! Projection catalog: in-memory store of named graph projections.
//!
//! Projections are cached and lazily rebuilt when the graph generation
//! changes. Configs are preserved so user-created projections survive
//! mutations without losing their label/edge filters.

use std::collections::HashMap;

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use selene_graph::SeleneGraph;

use crate::projection::{GraphProjection, ProjectionConfig};

/// A cached projection paired with the config that created it.
struct CatalogEntry {
    projection: GraphProjection,
    config: ProjectionConfig,
}

/// In-memory catalog of named graph projections with lazy rebuild.
pub struct ProjectionCatalog {
    entries: RwLock<HashMap<String, CatalogEntry>>,
}

impl Default for ProjectionCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionCatalog {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Create or rebuild a projection. Returns (node_count, edge_count).
    pub fn project(
        &self,
        graph: &SeleneGraph,
        config: &ProjectionConfig,
        scope: Option<&RoaringBitmap>,
    ) -> (u64, usize) {
        let proj = GraphProjection::build(graph, config, scope);
        let node_count = proj.node_count();
        let edge_count = proj.edge_count();

        self.entries.write().insert(
            config.name.clone(),
            CatalogEntry {
                projection: proj,
                config: config.clone(),
            },
        );
        (node_count, edge_count)
    }

    /// Ensure the named projection exists and is fresh (matches the current
    /// graph generation). If it exists but is stale, rebuild it from the
    /// stored config. If it does not exist, build a default all-nodes
    /// projection so algorithms work without an explicit graph.project().
    pub fn ensure_fresh(&self, graph: &SeleneGraph, name: &str) {
        let current_gen = graph.generation();
        let mut guard = self.entries.write();

        if let Some(entry) = guard.get(name) {
            if entry.projection.generation() == current_gen {
                return; // already fresh
            }
            // Stale: rebuild from stored config.
            let config = entry.config.clone();
            let proj = GraphProjection::build(graph, &config, None);
            guard.insert(
                name.to_string(),
                CatalogEntry {
                    projection: proj,
                    config,
                },
            );
        } else {
            // No projection with this name; build a default all-nodes projection.
            let config = ProjectionConfig {
                name: name.to_string(),
                node_labels: vec![],
                edge_labels: vec![],
                weight_property: None,
            };
            let proj = GraphProjection::build(graph, &config, None);
            guard.insert(
                name.to_string(),
                CatalogEntry {
                    projection: proj,
                    config,
                },
            );
        }
    }

    /// Get a projection by name. Returns a read guard valid for the guard's lifetime.
    pub fn get(&self, name: &str) -> Option<ProjectionRef<'_>> {
        let guard = self.entries.read();
        if guard.contains_key(name) {
            Some(ProjectionRef {
                guard,
                name: name.to_string(),
            })
        } else {
            None
        }
    }

    /// Drop a projection by name. Returns true if it existed.
    pub fn drop_projection(&self, name: &str) -> bool {
        self.entries.write().remove(name).is_some()
    }

    /// List all projection names with their node and edge counts.
    pub fn list(&self) -> Vec<(String, u64, usize)> {
        self.entries
            .read()
            .iter()
            .map(|(name, e)| {
                (
                    name.clone(),
                    e.projection.node_count(),
                    e.projection.edge_count(),
                )
            })
            .collect()
    }

    /// Check if a projection exists.
    pub fn contains(&self, name: &str) -> bool {
        self.entries.read().contains_key(name)
    }
}

/// A read guard that provides access to a projection.
pub struct ProjectionRef<'a> {
    guard: parking_lot::RwLockReadGuard<'a, HashMap<String, CatalogEntry>>,
    name: String,
}

impl ProjectionRef<'_> {
    pub fn projection(&self) -> &GraphProjection {
        &self.guard.get(&self.name).unwrap().projection
    }
}

impl std::ops::Deref for ProjectionRef<'_> {
    type Target = GraphProjection;
    fn deref(&self) -> &Self::Target {
        self.projection()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, NodeId, PropertyMap};

    fn test_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["a"]), PropertyMap::new())
            .unwrap();
        m.create_node(LabelSet::from_strs(&["b"]), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(1), IStr::new("link"), NodeId(2), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        g
    }

    #[test]
    fn catalog_project_and_get() {
        let g = test_graph();
        let cat = ProjectionCatalog::new();
        let config = ProjectionConfig {
            name: "test".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let (nc, ec) = cat.project(&g, &config, None);
        assert_eq!(nc, 2);
        assert_eq!(ec, 1);

        let proj = cat.get("test").unwrap();
        assert_eq!(proj.node_count(), 2);
    }

    #[test]
    fn catalog_drop() {
        let g = test_graph();
        let cat = ProjectionCatalog::new();
        let config = ProjectionConfig {
            name: "tmp".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &config, None);
        assert!(cat.contains("tmp"));
        assert!(cat.drop_projection("tmp"));
        assert!(!cat.contains("tmp"));
    }

    #[test]
    fn ensure_fresh_rebuilds_stale_projection() {
        let mut g = test_graph();
        let cat = ProjectionCatalog::new();
        let config = ProjectionConfig {
            name: "filtered".into(),
            node_labels: vec![IStr::new("a")],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &config, None);
        assert_eq!(cat.get("filtered").unwrap().node_count(), 1);

        // Mutate graph to advance generation.
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["a"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        // ensure_fresh rebuilds from the stored config (label "a" filter preserved).
        cat.ensure_fresh(&g, "filtered");
        assert_eq!(
            cat.get("filtered").unwrap().node_count(),
            2,
            "rebuilt projection should include the new 'a' node"
        );
    }

    #[test]
    fn ensure_fresh_creates_default_for_unknown() {
        let g = test_graph();
        let cat = ProjectionCatalog::new();

        cat.ensure_fresh(&g, "auto");
        // Default projection includes all nodes.
        assert_eq!(cat.get("auto").unwrap().node_count(), 2);
    }

    #[test]
    fn stale_projection_survives_other_project_calls() {
        let mut g = test_graph();
        let cat = ProjectionCatalog::new();

        // Create two projections.
        let c1 = ProjectionConfig {
            name: "keep".into(),
            node_labels: vec![IStr::new("a")],
            edge_labels: vec![],
            weight_property: None,
        };
        let c2 = ProjectionConfig {
            name: "also".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &c1, None);
        cat.project(&g, &c2, None);

        // Mutate graph.
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["c"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        // Creating a new projection no longer clears "keep".
        let c3 = ProjectionConfig {
            name: "new".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &c3, None);
        assert!(
            cat.contains("keep"),
            "user-created projection should survive"
        );
        assert!(cat.contains("also"));
        assert!(cat.contains("new"));
    }

    #[test]
    fn catalog_get_nonexistent() {
        let cat = ProjectionCatalog::new();
        assert!(cat.get("nope").is_none());
    }

    #[test]
    fn catalog_drop_nonexistent() {
        let cat = ProjectionCatalog::new();
        assert!(!cat.drop_projection("nope"));
    }

    #[test]
    fn catalog_same_generation_no_rebuild() {
        let g = test_graph();
        let cat = ProjectionCatalog::new();
        let config = ProjectionConfig {
            name: "keep".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &config, None);

        // Project again without mutating graph; "keep" should survive.
        let config2 = ProjectionConfig {
            name: "also".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &config2, None);
        assert!(cat.contains("keep"));
        assert!(cat.contains("also"));
    }

    #[test]
    fn catalog_list() {
        let g = test_graph();
        let cat = ProjectionCatalog::new();
        let c1 = ProjectionConfig {
            name: "a".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        let c2 = ProjectionConfig {
            name: "b".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &c1, None);
        cat.project(&g, &c2, None);
        let list = cat.list();
        assert_eq!(list.len(), 2);
    }
}
