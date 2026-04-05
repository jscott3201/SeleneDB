//! Factory functions for creating test edges.

use selene_core::{Edge, EdgeId, IStr, NodeId, PropertyMap, Value};

/// Create a simple test edge.
pub fn test_edge(id: u64, source: u64, target: u64, label: &str) -> Edge {
    Edge::new(
        EdgeId(id),
        NodeId(source),
        NodeId(target),
        IStr::new(label),
        PropertyMap::new(),
    )
}

/// Create a test edge with properties.
pub fn test_edge_with_props(
    id: u64,
    source: u64,
    target: u64,
    label: &str,
    props: &[(&str, Value)],
) -> Edge {
    let prop_map = PropertyMap::from_pairs(props.iter().map(|(k, v)| (IStr::new(k), v.clone())));
    Edge::new(
        EdgeId(id),
        NodeId(source),
        NodeId(target),
        IStr::new(label),
        prop_map,
    )
}

/// Create a containment hierarchy: site -> building -> 2 floors -> 2 zones.
pub fn test_containment_hierarchy() -> (Vec<selene_core::Node>, Vec<Edge>) {
    use crate::nodes::test_node;

    let nodes = vec![
        test_node(1, &["site"]),
        test_node(2, &["building"]),
        test_node(3, &["floor"]),
        test_node(4, &["floor"]),
        test_node(5, &["zone"]),
        test_node(6, &["zone"]),
    ];

    let edges = vec![
        test_edge(1, 1, 2, "contains"),
        test_edge(2, 2, 3, "contains"),
        test_edge(3, 2, 4, "contains"),
        test_edge(4, 3, 5, "contains"),
        test_edge(5, 4, 6, "contains"),
    ];

    (nodes, edges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_creation() {
        let edge = test_edge(1, 10, 20, "feeds");
        assert_eq!(edge.source, NodeId(10));
        assert_eq!(edge.target, NodeId(20));
        assert_eq!(edge.label.as_str(), "feeds");
    }

    #[test]
    fn test_containment() {
        let (nodes, edges) = test_containment_hierarchy();
        assert_eq!(nodes.len(), 6);
        assert_eq!(edges.len(), 5);
        assert!(edges.iter().all(|e| e.label.as_str() == "contains"));
    }
}
