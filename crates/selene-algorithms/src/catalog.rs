//! Projection catalog: in-memory store of named graph projections.
//!
//! Projections are cached until the graph generation changes.
//! On generation change, all projections are invalidated and must be rebuilt.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use selene_graph::SeleneGraph;

use crate::projection::{GraphProjection, ProjectionConfig};

/// In-memory catalog of named graph projections with generation-based invalidation.
pub struct ProjectionCatalog {
    projections: RwLock<HashMap<String, GraphProjection>>,
    generation: AtomicU64,
}

impl Default for ProjectionCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionCatalog {
    pub fn new() -> Self {
        Self {
            projections: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    /// Create or rebuild a projection. Returns (node_count, edge_count).
    pub fn project(
        &self,
        graph: &SeleneGraph,
        config: &ProjectionConfig,
        scope: Option<&RoaringBitmap>,
    ) -> (u64, usize) {
        self.invalidate_if_stale(graph);

        let proj = GraphProjection::build(graph, config, scope);
        let node_count = proj.node_count();
        let edge_count = proj.edge_count();

        self.projections.write().insert(config.name.clone(), proj);
        (node_count, edge_count)
    }

    /// Get a projection by name. Returns a read guard valid for the guard's lifetime.
    pub fn get(&self, name: &str) -> Option<ProjectionRef<'_>> {
        let guard = self.projections.read();
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
        self.projections.write().remove(name).is_some()
    }

    /// List all projection names with their node and edge counts.
    pub fn list(&self) -> Vec<(String, u64, usize)> {
        self.projections
            .read()
            .iter()
            .map(|(name, proj)| (name.clone(), proj.node_count(), proj.edge_count()))
            .collect()
    }

    /// Invalidate all projections if graph generation has changed.
    /// The generation check is inside the write lock to prevent a TOCTOU race
    /// where a concurrent thread could clear freshly rebuilt projections.
    fn invalidate_if_stale(&self, graph: &SeleneGraph) {
        let current = graph.generation();
        let mut guard = self.projections.write();
        let cached = self.generation.load(Ordering::Acquire);
        if cached != current {
            guard.clear();
            self.generation.store(current, Ordering::Release);
        }
    }

    /// Check if a projection exists.
    pub fn contains(&self, name: &str) -> bool {
        self.projections.read().contains_key(name)
    }
}

/// A read guard that provides access to a projection.
pub struct ProjectionRef<'a> {
    guard: parking_lot::RwLockReadGuard<'a, HashMap<String, GraphProjection>>,
    name: String,
}

impl ProjectionRef<'_> {
    pub fn projection(&self) -> &GraphProjection {
        self.guard.get(&self.name).unwrap()
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
    fn catalog_invalidation() {
        let mut g = test_graph();
        let cat = ProjectionCatalog::new();
        let config = ProjectionConfig {
            name: "test".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &config, None);
        assert!(cat.contains("test"));

        // Mutate graph; should invalidate on next project()
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["c"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        // The stale check happens on next project() call
        let config2 = ProjectionConfig {
            name: "test2".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &config2, None);
        // Old projection should be cleared
        assert!(!cat.contains("test"));
        assert!(cat.contains("test2"));
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
    fn catalog_invalidate_all_clears_everything() {
        let mut g = test_graph();
        let cat = ProjectionCatalog::new();
        // Create two projections
        for name in &["a", "b", "c"] {
            let config = ProjectionConfig {
                name: (*name).into(),
                node_labels: vec![],
                edge_labels: vec![],
                weight_property: None,
            };
            cat.project(&g, &config, None);
        }
        assert_eq!(cat.list().len(), 3);

        // Mutate graph to advance generation
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["d"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        // Next project() call triggers invalidation of all stale projections
        let config = ProjectionConfig {
            name: "new".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &config, None);

        // Old projections gone, only "new" remains
        assert!(!cat.contains("a"));
        assert!(!cat.contains("b"));
        assert!(!cat.contains("c"));
        assert!(cat.contains("new"));
    }

    #[test]
    fn catalog_same_generation_no_invalidation() {
        let g = test_graph();
        let cat = ProjectionCatalog::new();
        let config = ProjectionConfig {
            name: "keep".into(),
            node_labels: vec![],
            edge_labels: vec![],
            weight_property: None,
        };
        cat.project(&g, &config, None);

        // Project again without mutating graph; "keep" should survive
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
