//! Containment hierarchy algorithms.

use std::collections::HashSet;

use selene_core::NodeId;

use super::traversal::bfs;
use crate::graph::SeleneGraph;

/// Walk edges upward from `start`, following any edge whose label
/// appears in `edge_labels`.
///
/// Returns `[start, parent, grandparent, ...]` -- the ancestry chain.
/// Stops when a node has no matching incoming edge or a cycle is
/// detected.
pub fn walk_ancestors(graph: &SeleneGraph, start: NodeId, edge_labels: &[&str]) -> Vec<NodeId> {
    if !graph.contains_node(start) {
        return vec![];
    }

    let mut path = vec![start];
    let mut visited = HashSet::new();
    visited.insert(start);
    let mut current = start;

    loop {
        let mut found_parent = None;
        for &edge_id in graph.incoming(current) {
            if let Some(edge) = graph.get_edge(edge_id)
                && edge_labels.contains(&edge.label.as_str())
            {
                found_parent = Some(edge.source);
                break;
            }
        }

        match found_parent {
            Some(parent) if visited.insert(parent) => {
                path.push(parent);
                current = parent;
            }
            _ => break,
        }
    }

    path
}

/// Walk "contains" edges upward from `start`.
///
/// Returns `[start, parent, grandparent, ...]` -- the containment
/// ancestry chain.  Stops when a node has no incoming "contains" edge
/// or a cycle is detected.
pub fn containment_walk_up(graph: &SeleneGraph, start: NodeId) -> Vec<NodeId> {
    walk_ancestors(graph, start, &["contains"])
}

/// All descendant nodes under `root` via "contains" edges.
///
/// BFS down the containment hierarchy.  Returns descendants in
/// breadth-first order (root is NOT included).  Optionally limited
/// by `max_depth`.
pub fn containment_children(
    graph: &SeleneGraph,
    root: NodeId,
    max_depth: Option<u32>,
) -> Vec<NodeId> {
    bfs(graph, root, Some("contains"), max_depth.unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{Edge, EdgeId, IStr, LabelSet, Node, PropertyMap};

    fn node(id: u64, lbls: &[&str]) -> Node {
        Node::new(NodeId(id), LabelSet::from_strs(lbls), PropertyMap::new())
    }

    fn edge(id: u64, src: u64, tgt: u64, label: &str) -> Edge {
        Edge::new(
            EdgeId(id),
            NodeId(src),
            NodeId(tgt),
            IStr::new(label),
            PropertyMap::new(),
        )
    }

    fn containment_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        for (id, lbl) in [
            (1, "site"),
            (2, "building"),
            (3, "floor"),
            (4, "floor"),
            (5, "zone"),
            (6, "zone"),
        ] {
            g.insert_node_raw(node(id, &[lbl]));
        }
        g.insert_edge_raw(edge(1, 1, 2, "contains"));
        g.insert_edge_raw(edge(2, 2, 3, "contains"));
        g.insert_edge_raw(edge(3, 2, 4, "contains"));
        g.insert_edge_raw(edge(4, 3, 5, "contains"));
        g.insert_edge_raw(edge(5, 4, 6, "contains"));
        g
    }

    #[test]
    fn containment_walk_from_zone() {
        let g = containment_graph();
        let path = containment_walk_up(&g, NodeId(5));
        assert_eq!(path, vec![NodeId(5), NodeId(3), NodeId(2), NodeId(1)]);
    }

    #[test]
    fn containment_walk_from_root() {
        let g = containment_graph();
        assert_eq!(containment_walk_up(&g, NodeId(1)), vec![NodeId(1)]);
    }

    #[test]
    fn containment_walk_nonexistent() {
        assert!(containment_walk_up(&SeleneGraph::new(), NodeId(999)).is_empty());
    }

    #[test]
    fn containment_children_from_site() {
        let g = containment_graph();
        assert_eq!(containment_children(&g, NodeId(1), None).len(), 5);
    }

    #[test]
    fn containment_children_depth_1() {
        let g = containment_graph();
        let children = containment_children(&g, NodeId(1), Some(1));
        assert_eq!(children.len(), 1);
        assert_eq!(children[0], NodeId(2));
    }

    #[test]
    fn containment_children_from_leaf() {
        let g = containment_graph();
        assert!(containment_children(&g, NodeId(5), None).is_empty());
    }

    /// Building -[contains]-> Zone -[has_sensor]-> Sensor
    fn mixed_edge_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(node(1, &["Building"]));
        g.insert_node_raw(node(2, &["Zone"]));
        g.insert_node_raw(node(3, &["Sensor"]));
        g.insert_edge_raw(edge(1, 1, 2, "contains"));
        g.insert_edge_raw(edge(2, 2, 3, "has_sensor"));
        g
    }

    #[test]
    fn walk_up_only_follows_contains() {
        let g = mixed_edge_graph();
        // containment_walk_up only follows "contains", so from Sensor
        // it cannot reach Zone (the edge is "has_sensor").
        let path = containment_walk_up(&g, NodeId(3));
        assert_eq!(path, vec![NodeId(3)]);
    }

    #[test]
    fn walk_ancestors_follows_multiple_labels() {
        let g = mixed_edge_graph();
        // walk_ancestors with both labels traverses has_sensor then contains.
        let path = walk_ancestors(&g, NodeId(3), &["contains", "has_sensor"]);
        assert_eq!(path, vec![NodeId(3), NodeId(2), NodeId(1)]);
    }
}
