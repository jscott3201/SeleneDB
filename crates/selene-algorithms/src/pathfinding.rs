//! Path finding algorithms: Dijkstra, SSSP, APSP.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use selene_core::NodeId;

use crate::projection::GraphProjection;

/// A path result: sequence of nodes and total cost.
#[derive(Debug, Clone)]
pub struct PathResult {
    pub nodes: Vec<NodeId>,
    pub cost: f64,
}

/// Priority queue entry for Dijkstra's algorithm.
#[derive(Debug)]
struct DijkstraEntry {
    node: u32,
    cost: f64,
}

impl PartialEq for DijkstraEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cost.total_cmp(&other.cost) == Ordering::Equal
    }
}
impl Eq for DijkstraEntry {}

impl PartialOrd for DijkstraEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DijkstraEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap; total_cmp handles NaN soundly
        other.cost.total_cmp(&self.cost)
    }
}

/// Dijkstra's shortest path between two nodes.
///
/// Uses edge weights from the projection. Returns None if unreachable.
/// For unweighted projections, all edges have weight 1.0.
///
/// Uses Vec-based storage sized to `max_node + 1` for O(1) indexed access
/// instead of HashMap lookups. Sentinel values: `f64::INFINITY` for distance,
/// `u32::MAX` for "no predecessor".
pub fn dijkstra(proj: &GraphProjection, from: NodeId, to: NodeId) -> Option<PathResult> {
    if !proj.contains_node(from) || !proj.contains_node(to) {
        return None;
    }
    if from == to {
        return Some(PathResult {
            nodes: vec![from],
            cost: 0.0,
        });
    }

    let max_node = proj.nodes.max().unwrap_or(0) as usize;
    let size = max_node + 1;
    let mut dist: Vec<f64> = vec![f64::INFINITY; size];
    let mut prev: Vec<u32> = vec![u32::MAX; size];
    let mut heap = BinaryHeap::new();

    let from_u32 = from.0 as u32;
    let to_u32 = to.0 as u32;

    dist[from_u32 as usize] = 0.0;
    heap.push(DijkstraEntry {
        node: from_u32,
        cost: 0.0,
    });

    while let Some(DijkstraEntry { node, cost }) = heap.pop() {
        if node == to_u32 {
            let mut path = vec![to];
            let mut cur = to_u32;
            while prev[cur as usize] != u32::MAX {
                let p = prev[cur as usize];
                path.push(NodeId(u64::from(p)));
                cur = p;
            }
            path.reverse();
            return Some(PathResult { nodes: path, cost });
        }

        if cost > dist[node as usize] {
            continue;
        }

        for nb in proj.outgoing(NodeId(u64::from(node))) {
            let next = nb.node_id.0 as u32;
            let next_idx = next as usize;
            let new_cost = cost + nb.weight;

            if next_idx < size && new_cost < dist[next_idx] {
                dist[next_idx] = new_cost;
                prev[next_idx] = node;
                heap.push(DijkstraEntry {
                    node: next,
                    cost: new_cost,
                });
            }
        }
    }

    None
}

/// Single-Source Shortest Path (SSSP).
///
/// Returns distances from `source` to all reachable nodes via Dijkstra.
///
/// Uses Vec-based storage sized to `max_node + 1` for O(1) indexed access
/// instead of HashMap lookups.
pub fn sssp(proj: &GraphProjection, source: NodeId) -> Vec<(NodeId, f64)> {
    if !proj.contains_node(source) {
        return vec![];
    }

    let max_node = proj.nodes.max().unwrap_or(0) as usize;
    let size = max_node + 1;
    let mut dist: Vec<f64> = vec![f64::INFINITY; size];
    let mut heap = BinaryHeap::new();

    let source_u32 = source.0 as u32;
    dist[source_u32 as usize] = 0.0;
    heap.push(DijkstraEntry {
        node: source_u32,
        cost: 0.0,
    });

    while let Some(DijkstraEntry { node, cost }) = heap.pop() {
        if cost > dist[node as usize] {
            continue;
        }

        for nb in proj.outgoing(NodeId(u64::from(node))) {
            let next = nb.node_id.0 as u32;
            let next_idx = next as usize;
            let new_cost = cost + nb.weight;

            if next_idx < size && new_cost < dist[next_idx] {
                dist[next_idx] = new_cost;
                heap.push(DijkstraEntry {
                    node: next,
                    cost: new_cost,
                });
            }
        }
    }

    let mut result: Vec<(NodeId, f64)> = proj
        .nodes
        .iter()
        .filter_map(|nid| {
            let d = dist[nid as usize];
            if d < f64::INFINITY {
                Some((NodeId(u64::from(nid)), d))
            } else {
                None
            }
        })
        .collect();
    result.sort_by_key(|&(nid, _)| nid.0);
    result
}

/// All-Pairs Shortest Path (APSP).
///
/// Runs Dijkstra from every node. Rejects projections with >= `max_nodes`
/// nodes to prevent runaway computation.
pub fn apsp(
    proj: &GraphProjection,
    max_nodes: usize,
) -> Result<Vec<(NodeId, NodeId, f64)>, ApspError> {
    let n = proj.node_count() as usize;
    if n > max_nodes {
        return Err(ApspError::TooLarge {
            nodes: n,
            limit: max_nodes,
        });
    }

    let mut result = Vec::new();
    for nid in &proj.nodes {
        let source = NodeId(u64::from(nid));
        for (target, dist) in sssp(proj, source) {
            if source != target {
                result.push((source, target, dist));
            }
        }
    }

    result.sort_by(|a, b| a.0.0.cmp(&b.0.0).then(a.1.0.cmp(&b.1.0)));
    Ok(result)
}

/// APSP rejected because the projection exceeds the node limit.
#[derive(Debug, thiserror::Error)]
pub enum ApspError {
    #[error("projection has {nodes} nodes, exceeding limit of {limit} -- use SSSP instead")]
    TooLarge { nodes: usize, limit: usize },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::projection::ProjectionConfig;
    use selene_core::{IStr, LabelSet, PropertyMap, Value};
    use selene_graph::SeleneGraph;

    fn weighted_graph() -> SeleneGraph {
        // A--10-->B--5-->C
        // A--20-->C (longer direct route)
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["a"]), PropertyMap::new())
            .unwrap(); // 1
        m.create_node(LabelSet::from_strs(&["b"]), PropertyMap::new())
            .unwrap(); // 2
        m.create_node(LabelSet::from_strs(&["c"]), PropertyMap::new())
            .unwrap(); // 3

        m.create_edge(
            NodeId(1),
            IStr::new("road"),
            NodeId(2),
            PropertyMap::from_pairs([(IStr::new("dist"), Value::Float(10.0))]),
        )
        .unwrap();
        m.create_edge(
            NodeId(2),
            IStr::new("road"),
            NodeId(3),
            PropertyMap::from_pairs([(IStr::new("dist"), Value::Float(5.0))]),
        )
        .unwrap();
        m.create_edge(
            NodeId(1),
            IStr::new("road"),
            NodeId(3),
            PropertyMap::from_pairs([(IStr::new("dist"), Value::Float(20.0))]),
        )
        .unwrap();

        m.commit(0).unwrap();
        g
    }

    fn project_weighted(g: &SeleneGraph) -> GraphProjection {
        GraphProjection::build(
            g,
            &ProjectionConfig {
                name: "test".into(),
                node_labels: vec![],
                edge_labels: vec![],
                weight_property: Some(IStr::new("dist")),
            },
            None,
        )
    }

    fn project_unweighted(g: &SeleneGraph) -> GraphProjection {
        GraphProjection::build(
            g,
            &ProjectionConfig {
                name: "test".into(),
                node_labels: vec![],
                edge_labels: vec![],
                weight_property: None,
            },
            None,
        )
    }

    // ── Dijkstra Tests ──────────────────────────────────────────────

    #[test]
    fn dijkstra_weighted_shortest() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        let result = dijkstra(&proj, NodeId(1), NodeId(3)).unwrap();
        // Should take A->B->C (cost 15) not A->C (cost 20)
        assert!((result.cost - 15.0).abs() < f64::EPSILON);
        assert_eq!(result.nodes, vec![NodeId(1), NodeId(2), NodeId(3)]);
    }

    #[test]
    fn dijkstra_unweighted() {
        let g = weighted_graph();
        let proj = project_unweighted(&g);
        let result = dijkstra(&proj, NodeId(1), NodeId(3)).unwrap();
        // Unweighted: all edges weight 1.0, direct A->C is shorter (1 hop vs 2)
        assert!((result.cost - 1.0).abs() < f64::EPSILON);
        assert_eq!(result.nodes, vec![NodeId(1), NodeId(3)]);
    }

    #[test]
    fn dijkstra_same_node() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        let result = dijkstra(&proj, NodeId(1), NodeId(1)).unwrap();
        assert_eq!(result.cost, 0.0);
        assert_eq!(result.nodes, vec![NodeId(1)]);
    }

    #[test]
    fn dijkstra_unreachable() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        // C has no outgoing edges, so C->A is unreachable
        assert!(dijkstra(&proj, NodeId(3), NodeId(1)).is_none());
    }

    #[test]
    fn dijkstra_nonexistent() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        assert!(dijkstra(&proj, NodeId(99), NodeId(1)).is_none());
    }

    // ── SSSP Tests ──────────────────────────────────────────────────

    #[test]
    fn sssp_from_source() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        let result = sssp(&proj, NodeId(1));
        // Should find: 1->1 (0), 1->2 (10), 1->3 (15 via B)
        assert_eq!(result.len(), 3);
        let dist_map: HashMap<u64, f64> = result.into_iter().map(|(n, d)| (n.0, d)).collect();
        assert!((dist_map[&1] - 0.0).abs() < f64::EPSILON);
        assert!((dist_map[&2] - 10.0).abs() < f64::EPSILON);
        assert!((dist_map[&3] - 15.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sssp_from_leaf() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        let result = sssp(&proj, NodeId(3));
        // Node 3 has no outgoing edges, only itself reachable
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, NodeId(3));
    }

    // ── APSP Tests ──────────────────────────────────────────────────

    #[test]
    fn apsp_small_graph() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        let result = apsp(&proj, 1000).unwrap();
        // Should have entries for all reachable pairs
        assert!(!result.is_empty());
        // 1->2 should be 10, 1->3 should be 15, 2->3 should be 5
        let find = |s: u64, t: u64| {
            result
                .iter()
                .find(|r| r.0.0 == s && r.1.0 == t)
                .map(|r| r.2)
        };
        assert!((find(1, 2).unwrap() - 10.0).abs() < f64::EPSILON);
        assert!((find(1, 3).unwrap() - 15.0).abs() < f64::EPSILON);
        assert!((find(2, 3).unwrap() - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn apsp_too_large() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        // Limit to 2 nodes; should fail with 3
        assert!(apsp(&proj, 2).is_err());
    }

    // ── Dijkstra edge cases ─────────────────────────────────────────

    #[test]
    fn dijkstra_negative_weight_treated_as_lower_cost() {
        // Dijkstra does not detect negative weights. Negative edges can
        // produce suboptimal paths, but the algorithm must not panic.
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap(); // 1
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap(); // 2
        m.create_edge(
            NodeId(1),
            IStr::new("e"),
            NodeId(2),
            PropertyMap::from_pairs([(IStr::new("dist"), Value::Float(-5.0))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let proj = project_weighted(&g);
        // Should not panic; may return a path with negative cost
        let result = dijkstra(&proj, NodeId(1), NodeId(2));
        assert!(result.is_some());
        assert!(result.unwrap().cost < 0.0);
    }

    #[test]
    fn dijkstra_both_nodes_nonexistent() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        assert!(dijkstra(&proj, NodeId(88), NodeId(99)).is_none());
    }

    #[test]
    fn dijkstra_target_nonexistent() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        assert!(dijkstra(&proj, NodeId(1), NodeId(99)).is_none());
    }

    #[test]
    fn dijkstra_single_node_graph() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        let proj = project_weighted(&g);
        let result = dijkstra(&proj, NodeId(1), NodeId(1)).unwrap();
        assert_eq!(result.cost, 0.0);
        assert_eq!(result.nodes, vec![NodeId(1)]);
    }

    #[test]
    fn dijkstra_zero_weight_edges() {
        // Zero-weight edges are valid (distance 0 between adjacent nodes)
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap(); // 1
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap(); // 2
        m.create_edge(
            NodeId(1),
            IStr::new("e"),
            NodeId(2),
            PropertyMap::from_pairs([(IStr::new("dist"), Value::Float(0.0))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let proj = project_weighted(&g);
        let result = dijkstra(&proj, NodeId(1), NodeId(2)).unwrap();
        assert_eq!(result.cost, 0.0);
        assert_eq!(result.nodes, vec![NodeId(1), NodeId(2)]);
    }

    // ── SSSP edge cases ────────────────────────────────────────────

    #[test]
    fn sssp_disconnected_graph_unreachable_nodes_omitted() {
        // Nodes in the projection but unreachable from source are not in results
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap(); // 1
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap(); // 2
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap(); // 3 (disconnected)
        m.create_edge(
            NodeId(1),
            IStr::new("e"),
            NodeId(2),
            PropertyMap::from_pairs([(IStr::new("dist"), Value::Float(3.0))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let proj = project_weighted(&g);
        let result = sssp(&proj, NodeId(1));
        let dist_map: HashMap<u64, f64> = result.into_iter().map(|(n, d)| (n.0, d)).collect();
        assert!(dist_map.contains_key(&1)); // source itself
        assert!(dist_map.contains_key(&2)); // reachable
        assert!(!dist_map.contains_key(&3)); // unreachable, omitted
    }

    #[test]
    fn sssp_nonexistent_source_returns_empty() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        let result = sssp(&proj, NodeId(999));
        assert!(result.is_empty());
    }

    #[test]
    fn sssp_single_node_returns_self() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        let proj = project_unweighted(&g);
        let result = sssp(&proj, NodeId(1));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (NodeId(1), 0.0));
    }

    // ── APSP edge cases ────────────────────────────────────────────

    #[test]
    fn apsp_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_unweighted(&g);
        let result = apsp(&proj, 1000).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn apsp_single_node_no_pairs() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        let proj = project_unweighted(&g);
        // APSP excludes self-pairs (source != target), so single node yields empty
        let result = apsp(&proj, 1000).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn apsp_limit_exact_boundary() {
        let g = weighted_graph();
        let proj = project_weighted(&g);
        // 3 nodes, limit exactly 3 should succeed
        assert!(apsp(&proj, 3).is_ok());
        // limit 2 should fail
        assert!(apsp(&proj, 2).is_err());
    }

    // ── Reference Building Tests ────────────────────────────────────

    #[test]
    fn reference_building_weighted_dijkstra() {
        let g = selene_testing::reference_building::reference_building(1);
        let proj = GraphProjection::build(
            &g,
            &ProjectionConfig {
                name: "feeds".into(),
                node_labels: vec![],
                edge_labels: vec![IStr::new("feeds")],
                weight_property: Some(IStr::new("pipe_length_ft")),
            },
            None,
        );

        // Find AHU nodes: first two equipment nodes that have outgoing feeds edges
        let mut ahu_ids: Vec<NodeId> = Vec::new();
        let mut vav_ids: Vec<NodeId> = Vec::new();
        for nid in &proj.nodes {
            let node_id = NodeId(u64::from(nid));
            if proj.out_degree(node_id) > 0 {
                ahu_ids.push(node_id);
            }
            if proj.in_degree(node_id) > 0 && proj.out_degree(node_id) == 0 {
                vav_ids.push(node_id);
            }
        }

        if !ahu_ids.is_empty() && !vav_ids.is_empty() {
            let result = dijkstra(&proj, ahu_ids[0], vav_ids[0]);
            // Should find a path through the feeds network
            assert!(
                result.is_some(),
                "should find path from AHU to VAV via feeds"
            );
            assert!(result.unwrap().cost > 0.0);
        }
    }
}
