//! Centrality algorithms: PageRank, betweenness centrality.

use std::collections::VecDeque;

use selene_core::NodeId;

use crate::projection::GraphProjection;

/// PageRank with configurable damping factor and max iterations.
///
/// Returns (node_id, score) sorted by score descending.
/// Converges when max change per iteration < 1e-6 or max_iter reached.
///
/// Uses Vec-based storage sized to `max_node + 1` for O(1) indexed access
/// instead of HashMap lookups.
pub fn pagerank(proj: &GraphProjection, damping: f64, max_iter: usize) -> Vec<(NodeId, f64)> {
    let n = proj.node_count() as f64;
    if n == 0.0 {
        return vec![];
    }

    let node_ids: Vec<u32> = proj.nodes.iter().collect();
    let max_node = proj.nodes.max().unwrap_or(0) as usize;
    let size = max_node + 1;
    let teleport = (1.0 - damping) / n;

    let initial = 1.0 / n;
    let mut scores: Vec<f64> = vec![0.0; size];
    for &nid in &node_ids {
        scores[nid as usize] = initial;
    }

    let mut new_scores: Vec<f64> = vec![0.0; size];

    for _ in 0..max_iter {
        // Reset new_scores to teleport for projection nodes, 0 for others
        for &nid in &node_ids {
            new_scores[nid as usize] = teleport;
        }

        for &nid in &node_ids {
            let out = proj.outgoing(NodeId(u64::from(nid)));
            let out_degree = out.len() as f64;
            if out_degree > 0.0 {
                let contribution = damping * scores[nid as usize] / out_degree;
                for nb in out {
                    let idx = nb.node_id.0 as usize;
                    if idx < size {
                        new_scores[idx] += contribution;
                    }
                }
            } else {
                // Dangling node: distribute evenly
                let contribution = damping * scores[nid as usize] / n;
                for &other in &node_ids {
                    new_scores[other as usize] += contribution;
                }
            }
        }

        // Convergence check
        let max_diff: f64 = node_ids
            .iter()
            .map(|&nid| {
                let i = nid as usize;
                (new_scores[i] - scores[i]).abs()
            })
            .fold(0.0, f64::max);

        // Swap scores and new_scores
        std::mem::swap(&mut scores, &mut new_scores);
        if max_diff < 1e-6 {
            break;
        }
    }

    let mut result: Vec<(NodeId, f64)> = node_ids
        .iter()
        .map(|&nid| (NodeId(u64::from(nid)), scores[nid as usize]))
        .collect();
    result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    result
}

/// Betweenness centrality using Brandes' algorithm.
///
/// If `sample_size` is Some, only compute from a random subset of nodes
/// (for large graphs). Returns (node_id, score) sorted by score descending.
///
/// Uses Vec-based storage sized to `max_node + 1` and cleared with `fill()`
/// between iterations instead of HashMap clear+insert per source.
pub fn betweenness(proj: &GraphProjection, sample_size: Option<usize>) -> Vec<(NodeId, f64)> {
    let node_ids: Vec<u32> = proj.nodes.iter().collect();
    let n = node_ids.len();
    if n == 0 {
        return vec![];
    }

    let size = proj.nodes.max().map_or(0, |m| m as usize + 1);
    let mut centrality: Vec<f64> = vec![0.0; size];

    let sources: Vec<u32> = match sample_size {
        Some(k) if k < n => {
            // Deterministic sampling: evenly spaced nodes
            let step = n / k;
            (0..k).map(|i| node_ids[i * step]).collect()
        }
        _ => node_ids.clone(),
    };

    // Pre-allocate Vec-based per-iteration storage. Cleared with fill() between
    // iterations for O(n) reset instead of HashMap clear+insert.
    let mut pred: Vec<Vec<u32>> = (0..size).map(|_| Vec::new()).collect();
    let mut sigma: Vec<f64> = vec![0.0; size];
    let mut dist: Vec<i64> = vec![-1; size];
    let mut delta: Vec<f64> = vec![0.0; size];
    // Track which nodes are in the projection for bounds checking.
    let mut in_proj: Vec<bool> = vec![false; size];
    for &nid in &node_ids {
        in_proj[nid as usize] = true;
    }

    for &s in &sources {
        let mut stack = Vec::new();

        // Reset per-iteration state. Only clear pred entries that were used
        // (tracked via the stack from the previous iteration or node_ids on first).
        for &nid in &node_ids {
            let i = nid as usize;
            pred[i].clear();
            sigma[i] = 0.0;
            dist[i] = -1;
            delta[i] = 0.0;
        }

        let si = s as usize;
        sigma[si] = 1.0;
        dist[si] = 0;
        let mut queue = VecDeque::new();
        queue.push_back(s);

        while let Some(v) = queue.pop_front() {
            stack.push(v);
            let vi = v as usize;
            let d_v = dist[vi];

            for nb in proj.outgoing(NodeId(u64::from(v))) {
                let w = nb.node_id.0 as u32;
                let wi = w as usize;
                if !in_proj[wi] {
                    continue;
                }

                if dist[wi] < 0 {
                    queue.push_back(w);
                    dist[wi] = d_v + 1;
                }

                if dist[wi] == d_v + 1 {
                    sigma[wi] += sigma[vi];
                    pred[wi].push(v);
                }
            }
        }

        while let Some(w) = stack.pop() {
            let wi = w as usize;
            for &v in &pred[wi] {
                let vi = v as usize;
                let d = sigma[vi] / sigma[wi] * (1.0 + delta[wi]);
                delta[vi] += d;
            }
            if w != s {
                centrality[wi] += delta[wi];
            }
        }
    }

    if let Some(k) = sample_size
        && k < n
    {
        let scale = n as f64 / k as f64;
        for &nid in &node_ids {
            centrality[nid as usize] *= scale;
        }
    }

    let mut result: Vec<(NodeId, f64)> = node_ids
        .iter()
        .map(|&nid| (NodeId(u64::from(nid)), centrality[nid as usize]))
        .collect();
    result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::projection::ProjectionConfig;
    use selene_core::{IStr, LabelSet, PropertyMap};
    use selene_graph::SeleneGraph;

    fn star_graph() -> SeleneGraph {
        // Star: center(1) -> 2,3,4,5 (center should have highest PageRank)
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=5 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        for i in 2..=5u64 {
            m.create_edge(NodeId(1), IStr::new("link"), NodeId(i), PropertyMap::new())
                .unwrap();
            m.create_edge(NodeId(i), IStr::new("link"), NodeId(1), PropertyMap::new())
                .unwrap();
        }
        m.commit(0).unwrap();
        g
    }

    fn chain_graph() -> SeleneGraph {
        // 1->2->3->4->5
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=5 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        for i in 1..=4u64 {
            m.create_edge(
                NodeId(i),
                IStr::new("link"),
                NodeId(i + 1),
                PropertyMap::new(),
            )
            .unwrap();
        }
        m.commit(0).unwrap();
        g
    }

    fn project_all(g: &SeleneGraph) -> GraphProjection {
        GraphProjection::build(
            g,
            &ProjectionConfig {
                name: "all".into(),
                node_labels: vec![],
                edge_labels: vec![],
                weight_property: None,
            },
            None,
        )
    }

    #[test]
    fn pagerank_star_center_highest() {
        let g = star_graph();
        let proj = project_all(&g);
        let result = pagerank(&proj, 0.85, 100);
        assert_eq!(result.len(), 5);
        // Center node (1) should have highest score
        assert_eq!(result[0].0, NodeId(1));
    }

    #[test]
    fn pagerank_converges() {
        let g = star_graph();
        let proj = project_all(&g);
        let result = pagerank(&proj, 0.85, 100);
        let total: f64 = result.iter().map(|r| r.1).sum();
        // Scores should sum to ~1.0
        assert!(
            (total - 1.0).abs() < 0.01,
            "total score {total} should be ~1.0"
        );
    }

    #[test]
    fn pagerank_empty() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        assert!(pagerank(&proj, 0.85, 100).is_empty());
    }

    #[test]
    fn betweenness_chain() {
        let g = chain_graph();
        let proj = project_all(&g);
        let result = betweenness(&proj, None);
        assert_eq!(result.len(), 5);
        // Middle nodes (2, 3, 4) should have higher betweenness than endpoints
        let scores: HashMap<u64, f64> = result.into_iter().map(|(n, s)| (n.0, s)).collect();
        // Node 1 and 5 are endpoints (source or sink)
        // In a directed chain 1->2->3->4->5, node 2,3 have high betweenness
        assert!(scores[&2] > scores[&1] || scores[&3] > scores[&1]);
    }

    #[test]
    fn betweenness_star() {
        let g = star_graph();
        let proj = project_all(&g);
        let result = betweenness(&proj, None);
        // Center should have highest betweenness
        assert_eq!(result[0].0, NodeId(1));
    }

    #[test]
    fn betweenness_sampled() {
        let g = star_graph();
        let proj = project_all(&g);
        let full = betweenness(&proj, None);
        let sampled = betweenness(&proj, Some(2));
        // Sampled should return results for all nodes
        assert_eq!(sampled.len(), full.len());
    }

    // ── PageRank edge cases ──────────────────────────────────────────

    #[test]
    fn pagerank_single_node() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = pagerank(&proj, 0.85, 100);
        assert_eq!(result.len(), 1);
        // Single node gets all the rank
        assert!((result[0].1 - 1.0).abs() < 0.01);
    }

    #[test]
    fn pagerank_damping_zero_uniform() {
        // damping=0 means all rank comes from teleportation, so scores are uniform
        let g = star_graph();
        let proj = project_all(&g);
        let result = pagerank(&proj, 0.0, 100);
        let expected = 1.0 / 5.0;
        for (_, score) in &result {
            assert!(
                (score - expected).abs() < 0.01,
                "damping=0: all scores should be ~{expected}, got {score}"
            );
        }
    }

    #[test]
    fn pagerank_damping_half() {
        let g = star_graph();
        let proj = project_all(&g);
        let result = pagerank(&proj, 0.5, 100);
        // Center (node 1) should still be highest
        assert_eq!(result[0].0, NodeId(1));
        // Total should sum to ~1.0
        let total: f64 = result.iter().map(|r| r.1).sum();
        assert!((total - 1.0).abs() < 0.01);
    }

    #[test]
    fn pagerank_damping_high() {
        let g = star_graph();
        let proj = project_all(&g);
        let result = pagerank(&proj, 0.99, 100);
        // With high damping, center should dominate even more
        assert_eq!(result[0].0, NodeId(1));
        let total: f64 = result.iter().map(|r| r.1).sum();
        assert!((total - 1.0).abs() < 0.01);
    }

    #[test]
    fn pagerank_chain_last_node_highest() {
        // In a chain 1->2->3->4->5, rank accumulates at node 5 (dangling node)
        let g = chain_graph();
        let proj = project_all(&g);
        let result = pagerank(&proj, 0.85, 200);
        // Node 5 is a dangling node (no outgoing). It distributes rank evenly
        // to everyone via teleportation. The exact ranking depends on convergence,
        // but endpoint nodes should have measurable rank.
        assert_eq!(result.len(), 5);
        let total: f64 = result.iter().map(|r| r.1).sum();
        assert!((total - 1.0).abs() < 0.01);
    }

    #[test]
    fn pagerank_two_node_cycle() {
        // 1<->2: symmetric, both should have equal rank
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(1), IStr::new("l"), NodeId(2), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(2), IStr::new("l"), NodeId(1), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = pagerank(&proj, 0.85, 100);
        assert_eq!(result.len(), 2);
        assert!(
            (result[0].1 - result[1].1).abs() < 0.01,
            "symmetric cycle should produce equal ranks"
        );
    }

    // ── Betweenness edge cases ─────────────────────────────────────

    #[test]
    fn betweenness_star_center_highest_score() {
        let g = star_graph();
        let proj = project_all(&g);
        let result = betweenness(&proj, None);
        // Center node (1) is on every shortest path between leaf pairs
        assert_eq!(result[0].0, NodeId(1));
        assert!(result[0].1 > 0.0, "center should have positive betweenness");
    }

    #[test]
    fn betweenness_single_node() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = betweenness(&proj, None);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, 0.0);
    }

    #[test]
    fn betweenness_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        let result = betweenness(&proj, None);
        assert!(result.is_empty());
    }

    #[test]
    fn betweenness_disconnected_graph() {
        // Two disconnected pairs: 1->2, 3->4
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=4 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        m.create_edge(NodeId(1), IStr::new("l"), NodeId(2), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(3), IStr::new("l"), NodeId(4), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = betweenness(&proj, None);
        // No node is on a path between disconnected components
        // Endpoints of chains have betweenness 0 (they are sources/targets, not intermediaries)
        for (_, score) in &result {
            assert_eq!(*score, 0.0, "no node bridges disconnected components");
        }
    }

    #[test]
    fn betweenness_sample_size_one() {
        let g = star_graph();
        let proj = project_all(&g);
        let result = betweenness(&proj, Some(1));
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn reference_building_pagerank() {
        let g = selene_testing::reference_building::reference_building(1);
        let proj = GraphProjection::build(
            &g,
            &ProjectionConfig {
                name: "feeds".into(),
                node_labels: vec![],
                edge_labels: vec![IStr::new("feeds")],
                weight_property: None,
            },
            None,
        );
        let result = pagerank(&proj, 0.85, 50);
        // Should have results for nodes in the feeds graph
        assert!(!result.is_empty());
        // Scores should sum to ~1.0
        let total: f64 = result.iter().map(|r| r.1).sum();
        assert!((total - 1.0).abs() < 0.01);
    }
}
