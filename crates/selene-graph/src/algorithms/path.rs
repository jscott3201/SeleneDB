//! Path algorithms: shortest path (unweighted).

use std::collections::{HashMap, HashSet, VecDeque};

use selene_core::NodeId;

use crate::graph::SeleneGraph;

/// Shortest unweighted path from `from` to `to`, optionally filtering by
/// edge label.
///
/// Returns the full path including `from` and `to`, or `None` if
/// unreachable.  Uses BFS (guarantees shortest for unweighted graphs).
pub fn shortest_path(
    graph: &SeleneGraph,
    from: NodeId,
    to: NodeId,
    edge_label: Option<&str>,
) -> Option<Vec<NodeId>> {
    if from == to {
        return Some(vec![from]);
    }
    if !graph.contains_node(from) || !graph.contains_node(to) {
        return None;
    }

    let mut visited = HashSet::new();
    visited.insert(from);
    let mut queue = VecDeque::new();
    queue.push_back(from);
    let mut parent = HashMap::new();

    while let Some(current) = queue.pop_front() {
        for &edge_id in graph.outgoing(current) {
            let Some(edge) = graph.get_edge(edge_id) else {
                continue;
            };
            if let Some(label) = edge_label
                && edge.label.as_str() != label
            {
                continue;
            }
            if visited.insert(edge.target) {
                parent.insert(edge.target, current);
                if edge.target == to {
                    // Reconstruct path
                    let mut path = vec![to];
                    let mut cur = to;
                    while let Some(&p) = parent.get(&cur) {
                        path.push(p);
                        cur = p;
                    }
                    path.reverse();
                    return Some(path);
                }
                queue.push_back(edge.target);
            }
        }
    }

    None
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

    fn chain_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        for i in 1..=5 {
            g.insert_node_raw(node(i, &["node"]));
        }
        for i in 1..=4 {
            g.insert_edge_raw(edge(i, i, i + 1, "next"));
        }
        g
    }

    fn mixed_edge_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        for i in 1..=4 {
            g.insert_node_raw(node(i, &["equip"]));
        }
        g.insert_edge_raw(edge(1, 1, 2, "contains"));
        g.insert_edge_raw(edge(2, 1, 3, "feeds"));
        g.insert_edge_raw(edge(3, 3, 4, "feeds"));
        g
    }

    #[test]
    fn shortest_path_chain() {
        let g = chain_graph();
        let path = shortest_path(&g, NodeId(1), NodeId(5), None).unwrap();
        assert_eq!(
            path,
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
        );
    }

    #[test]
    fn shortest_path_same_node() {
        let g = chain_graph();
        assert_eq!(
            shortest_path(&g, NodeId(3), NodeId(3), None).unwrap(),
            vec![NodeId(3)]
        );
    }

    #[test]
    fn shortest_path_unreachable() {
        let g = chain_graph();
        assert!(shortest_path(&g, NodeId(5), NodeId(1), None).is_none());
    }

    #[test]
    fn shortest_path_nonexistent() {
        let g = chain_graph();
        assert!(shortest_path(&g, NodeId(1), NodeId(999), None).is_none());
    }

    #[test]
    fn shortest_path_with_label_filter() {
        let g = mixed_edge_graph();
        let path = shortest_path(&g, NodeId(1), NodeId(4), Some("feeds")).unwrap();
        assert_eq!(path, vec![NodeId(1), NodeId(3), NodeId(4)]);
    }
}
