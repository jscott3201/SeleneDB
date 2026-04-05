//! Graph traversal algorithms: BFS, DFS, reachable.

use std::collections::{HashSet, VecDeque};

use selene_core::NodeId;

use crate::graph::SeleneGraph;

/// BFS from `start`, optionally filtering by edge label.
///
/// Returns nodes in visit order (breadth-first), excluding the start node.
/// Respects `max_depth` -- depth 1 returns only direct neighbors.
pub fn bfs(
    graph: &SeleneGraph,
    start: NodeId,
    edge_label: Option<&str>,
    max_depth: u32,
) -> Vec<NodeId> {
    if !graph.contains_node(start) || max_depth == 0 {
        return vec![];
    }

    let mut visited = HashSet::new();
    visited.insert(start);
    let mut queue = VecDeque::new();
    queue.push_back((start, 0u32));
    let mut result = Vec::new();

    while let Some((current, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
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
                result.push(edge.target);
                queue.push_back((edge.target, depth + 1));
            }
        }
    }

    result
}

/// DFS from `start`, optionally filtering by edge label.
///
/// Returns nodes in visit order (depth-first), excluding the start node.
/// Uses iterative DFS with an explicit stack.
pub fn dfs(
    graph: &SeleneGraph,
    start: NodeId,
    edge_label: Option<&str>,
    max_depth: u32,
) -> Vec<NodeId> {
    if !graph.contains_node(start) || max_depth == 0 {
        return vec![];
    }

    let mut visited = HashSet::new();
    visited.insert(start);
    let mut stack = vec![(start, 0u32)];
    let mut result = Vec::new();

    while let Some((current, depth)) = stack.pop() {
        if depth >= max_depth {
            continue;
        }
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
                result.push(edge.target);
                stack.push((edge.target, depth + 1));
            }
        }
    }

    result
}

/// All nodes reachable from `start` within `max_depth` hops.
///
/// Returns a set (no duplicates, no ordering guarantee).
/// Includes the start node itself.
pub fn reachable(
    graph: &SeleneGraph,
    start: NodeId,
    edge_label: Option<&str>,
    max_depth: u32,
) -> HashSet<NodeId> {
    let mut result: HashSet<NodeId> = bfs(graph, start, edge_label, max_depth)
        .into_iter()
        .collect();
    result.insert(start);
    result
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

    /// Chain: 1 → 2 → 3 → 4 → 5
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

    /// Containment tree:
    ///   1(site) → 2(building) → 3(floor) → 5(zone)
    ///                          → 4(floor) → 6(zone)
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

    /// Mixed edges: contains + feeds
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
    fn bfs_depth_1() {
        let g = containment_graph();
        let result = bfs(&g, NodeId(2), Some("contains"), 1);
        let set: std::collections::HashSet<_> = result.into_iter().collect();
        assert_eq!(set, [NodeId(3), NodeId(4)].into_iter().collect());
    }

    #[test]
    fn bfs_full_depth() {
        let g = containment_graph();
        let result = bfs(&g, NodeId(1), Some("contains"), 10);
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn bfs_with_label_filter() {
        let g = mixed_edge_graph();
        let feeds = bfs(&g, NodeId(1), Some("feeds"), 10);
        assert_eq!(feeds.len(), 2);
        assert!(feeds.contains(&NodeId(3)));
        assert!(feeds.contains(&NodeId(4)));
    }

    #[test]
    fn bfs_no_filter_all_edges() {
        let g = mixed_edge_graph();
        let all = bfs(&g, NodeId(1), None, 10);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn bfs_nonexistent_start() {
        assert!(bfs(&SeleneGraph::new(), NodeId(999), None, 10).is_empty());
    }

    #[test]
    fn bfs_zero_depth() {
        let g = chain_graph();
        assert!(bfs(&g, NodeId(1), None, 0).is_empty());
    }

    #[test]
    fn bfs_respects_max_depth() {
        let g = chain_graph();
        let result = bfs(&g, NodeId(1), None, 2);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&NodeId(2)));
        assert!(result.contains(&NodeId(3)));
    }

    #[test]
    fn dfs_visits_all_reachable() {
        let g = chain_graph();
        let result = dfs(&g, NodeId(1), None, 10);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn dfs_with_depth_limit() {
        let g = chain_graph();
        let result = dfs(&g, NodeId(1), None, 2);
        assert!(result.len() <= 2);
        assert!(result.contains(&NodeId(2)));
    }

    #[test]
    fn dfs_handles_cycle() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(node(1, &["a"]));
        g.insert_node_raw(node(2, &["b"]));
        g.insert_edge_raw(edge(1, 1, 2, "link"));
        g.insert_edge_raw(edge(2, 2, 1, "link"));
        let result = dfs(&g, NodeId(1), None, 100);
        assert_eq!(result, vec![NodeId(2)]);
    }

    #[test]
    fn reachable_includes_start() {
        let g = chain_graph();
        let r = reachable(&g, NodeId(1), None, 10);
        assert!(r.contains(&NodeId(1)));
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn reachable_with_depth_limit() {
        let g = chain_graph();
        let r = reachable(&g, NodeId(1), None, 2);
        assert!(r.contains(&NodeId(1)));
        assert!(r.contains(&NodeId(2)));
        assert!(r.contains(&NodeId(3)));
        assert!(!r.contains(&NodeId(4)));
    }
}
