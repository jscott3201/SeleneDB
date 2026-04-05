//! Graph slicing -- extract subgraphs for export or federation.
//!
//! Slices clone matching nodes and their connecting edges. The returned
//! data is owned and suitable for serialization.

use std::collections::HashSet;

use selene_core::{Edge, Node, NodeId};

use crate::algorithms::containment_children;
use crate::graph::SeleneGraph;

/// A subgraph extracted from [`SeleneGraph`].
#[derive(Debug, Clone)]
pub struct GraphSlice {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

/// Clone the entire graph.
pub fn slice_full(graph: &SeleneGraph) -> GraphSlice {
    let nodes: Vec<Node> = graph
        .all_node_ids()
        .filter_map(|id| graph.get_node(id).map(|n| n.to_owned_node()))
        .collect();

    let edges: Vec<Edge> = graph
        .all_edge_ids()
        .filter_map(|id| graph.get_edge(id).map(|e| e.to_owned_edge()))
        .collect();

    GraphSlice { nodes, edges }
}

/// Clone nodes matching any of the given labels and all edges where
/// BOTH source and target are in the slice.
pub fn slice_by_labels(graph: &SeleneGraph, labels: &[&str]) -> GraphSlice {
    let mut node_ids = HashSet::new();
    for label in labels {
        for id in graph.nodes_by_label(label) {
            node_ids.insert(id);
        }
    }

    let nodes: Vec<Node> = node_ids
        .iter()
        .filter_map(|id| graph.get_node(*id).map(|n| n.to_owned_node()))
        .collect();

    let edges: Vec<Edge> = collect_edges_within(graph, &node_ids);

    GraphSlice { nodes, edges }
}

/// Clone the containment subtree rooted at `root` (root + all
/// descendants via "contains" edges) and all edges between included
/// nodes.
pub fn slice_containment(graph: &SeleneGraph, root: NodeId, max_depth: Option<u32>) -> GraphSlice {
    let mut node_ids: HashSet<NodeId> = containment_children(graph, root, max_depth)
        .into_iter()
        .collect();
    node_ids.insert(root);

    let nodes: Vec<Node> = node_ids
        .iter()
        .filter_map(|id| graph.get_node(*id).map(|n| n.to_owned_node()))
        .collect();

    let edges: Vec<Edge> = collect_edges_within(graph, &node_ids);

    GraphSlice { nodes, edges }
}

/// Collect all edges where both source and target are in `node_ids`.
fn collect_edges_within(graph: &SeleneGraph, node_ids: &HashSet<NodeId>) -> Vec<Edge> {
    let mut edges = Vec::new();
    let mut seen = HashSet::new();

    for &node_id in node_ids {
        for &edge_id in graph.outgoing(node_id) {
            if let Some(edge) = graph.get_edge(edge_id)
                && node_ids.contains(&edge.target)
                && seen.insert(edge_id)
            {
                edges.push(edge.to_owned_edge());
            }
        }
    }

    edges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::SeleneGraph;
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

    fn sample_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(node(1, &["site"]));
        g.insert_node_raw(node(2, &["building"]));
        g.insert_node_raw(node(3, &["floor"]));
        g.insert_node_raw(node(4, &["floor"]));
        g.insert_node_raw(node(5, &["zone", "sensor"]));
        g.insert_node_raw(node(6, &["zone"]));

        g.insert_edge_raw(edge(1, 1, 2, "contains"));
        g.insert_edge_raw(edge(2, 2, 3, "contains"));
        g.insert_edge_raw(edge(3, 2, 4, "contains"));
        g.insert_edge_raw(edge(4, 3, 5, "contains"));
        g.insert_edge_raw(edge(5, 4, 6, "contains"));
        g.insert_edge_raw(edge(6, 5, 6, "feeds")); // cross-edge within zones
        g
    }

    #[test]
    fn full_slice_matches_graph() {
        let g = sample_graph();
        let s = slice_full(&g);
        assert_eq!(s.nodes.len(), 6);
        assert_eq!(s.edges.len(), 6);
    }

    #[test]
    fn label_slice_matching_nodes() {
        let g = sample_graph();
        let s = slice_by_labels(&g, &["floor"]);
        assert_eq!(s.nodes.len(), 2); // nodes 3 and 4
        // No edges: floor→zone edges have targets outside the slice
        assert_eq!(s.edges.len(), 0);
    }

    #[test]
    fn label_slice_includes_internal_edges() {
        let g = sample_graph();
        let s = slice_by_labels(&g, &["zone", "sensor"]);
        // Nodes: 5 (zone+sensor) and 6 (zone) = 2 nodes
        assert_eq!(s.nodes.len(), 2);
        // Edge 6 (5→6 "feeds") has both endpoints in the slice
        assert_eq!(s.edges.len(), 1);
        assert_eq!(s.edges[0].id, EdgeId(6));
    }

    #[test]
    fn label_slice_excludes_external_edges() {
        let g = sample_graph();
        let s = slice_by_labels(&g, &["building"]);
        assert_eq!(s.nodes.len(), 1); // just node 2
        // The contains edges from/to building have one endpoint outside
        assert_eq!(s.edges.len(), 0);
    }

    #[test]
    fn containment_slice_from_root() {
        let g = sample_graph();
        let s = slice_containment(&g, NodeId(1), None);
        assert_eq!(s.nodes.len(), 6); // everything
        assert_eq!(s.edges.len(), 6); // all edges are internal
    }

    #[test]
    fn containment_slice_depth_limited() {
        let g = sample_graph();
        let s = slice_containment(&g, NodeId(1), Some(1));
        // Depth 1: site + building = 2 nodes
        assert_eq!(s.nodes.len(), 2);
        // Only the site→building edge
        assert_eq!(s.edges.len(), 1);
    }

    #[test]
    fn containment_slice_subtree() {
        let g = sample_graph();
        let s = slice_containment(&g, NodeId(2), None);
        // building + 2 floors + 2 zones = 5
        assert_eq!(s.nodes.len(), 5);
    }

    #[test]
    fn empty_graph_slice() {
        let g = SeleneGraph::new();
        let s = slice_full(&g);
        assert!(s.nodes.is_empty());
        assert!(s.edges.is_empty());
    }

    #[test]
    fn label_slice_no_matches() {
        let g = sample_graph();
        let s = slice_by_labels(&g, &["nonexistent"]);
        assert!(s.nodes.is_empty());
        assert!(s.edges.is_empty());
    }
}
