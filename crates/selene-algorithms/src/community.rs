//! Community detection algorithms: label propagation, Louvain modularity, triangle count.

use std::collections::HashMap;

use selene_core::NodeId;

use crate::projection::GraphProjection;

/// Label Propagation for community detection.
///
/// Each node starts with its own label. In each iteration, each node adopts
/// the label most common among its neighbors. Converges when no labels change
/// or max_iter reached. Treats edges as undirected.
///
/// Returns (node_id, community_id) sorted by node_id.
///
/// Uses Vec-based storage sized to `max_node + 1` for O(1) indexed access
/// instead of HashMap lookups.
pub fn label_propagation(proj: &GraphProjection, max_iter: usize) -> Vec<(NodeId, u64)> {
    let node_ids: Vec<u32> = proj.nodes.iter().collect();
    if node_ids.is_empty() {
        return vec![];
    }

    let max_node = proj.nodes.max().unwrap_or(0) as usize;
    let size = max_node + 1;
    let mut labels: Vec<u32> = vec![u32::MAX; size];
    for &nid in &node_ids {
        labels[nid as usize] = nid;
    }

    let mut label_counts: HashMap<u32, usize> = HashMap::new();
    for _ in 0..max_iter {
        let mut changed = false;

        for &nid in &node_ids {
            label_counts.clear();

            for nb in proj.outgoing(NodeId(u64::from(nid))) {
                let nb_label = labels[nb.node_id.0 as usize];
                *label_counts.entry(nb_label).or_insert(0) += 1;
            }
            for nb in proj.incoming(NodeId(u64::from(nid))) {
                let nb_label = labels[nb.node_id.0 as usize];
                *label_counts.entry(nb_label).or_insert(0) += 1;
            }

            if label_counts.is_empty() {
                continue;
            }

            // Pick the most common label (ties broken by smallest label ID)
            let max_count = *label_counts.values().max().unwrap();
            let best_label = label_counts
                .iter()
                .filter(|&(_, &count)| count == max_count)
                .map(|(&label, _)| label)
                .min()
                .unwrap();

            if labels[nid as usize] != best_label {
                labels[nid as usize] = best_label;
                changed = true;
            }
        }

        if !changed {
            break;
        }
    }

    let mut result: Vec<(NodeId, u64)> = node_ids
        .iter()
        .map(|&nid| (NodeId(u64::from(nid)), u64::from(labels[nid as usize])))
        .collect();
    result.sort_by_key(|&(nid, _)| nid.0);
    result
}

/// Louvain modularity optimization for hierarchical community detection.
///
/// Returns (node_id, community_id, level) where level 0 is the finest partition.
/// Treats edges as undirected. Assumes bidirectional projections; directed-only
/// graphs may produce suboptimal community assignments.
///
/// Uses Vec-based storage sized to `max_node + 1` for O(1) indexed access.
/// `community` and `weighted_degree` are flat Vecs; `comm_degree_sum` remains a
/// HashMap because it is keyed by community ID (which is sparse and changes).
pub fn louvain(proj: &GraphProjection) -> Vec<(NodeId, u64, u32)> {
    let node_ids: Vec<u32> = proj.nodes.iter().collect();
    if node_ids.is_empty() {
        return vec![];
    }

    let max_node = proj.nodes.max().unwrap_or(0) as usize;
    let size = max_node + 1;

    let mut total_weight = 0.0f64;
    for &nid in &node_ids {
        for nb in proj.outgoing(NodeId(u64::from(nid))) {
            total_weight += nb.weight;
        }
    }
    if total_weight == 0.0 {
        total_weight = 1.0;
    }

    let mut community: Vec<u32> = vec![u32::MAX; size];
    for &nid in &node_ids {
        community[nid as usize] = nid;
    }

    let mut weighted_degree: Vec<f64> = vec![0.0; size];
    for &nid in &node_ids {
        let mut deg = 0.0;
        for nb in proj.outgoing(NodeId(u64::from(nid))) {
            deg += nb.weight;
        }
        for nb in proj.incoming(NodeId(u64::from(nid))) {
            deg += nb.weight;
        }
        weighted_degree[nid as usize] = deg;
    }

    // Incremental community degree sums (sparse, keyed by community ID)
    let mut comm_degree_sum: HashMap<u32, f64> = HashMap::new();
    for &nid in &node_ids {
        let c = community[nid as usize];
        *comm_degree_sum.entry(c).or_insert(0.0) += weighted_degree[nid as usize];
    }

    let mut improved = true;
    let mut iterations = 0;
    let m2 = 2.0 * total_weight;

    let mut comm_weights: HashMap<u32, f64> = HashMap::new();
    while improved && iterations < 50 {
        improved = false;
        iterations += 1;

        for &nid in &node_ids {
            let idx = nid as usize;
            let current_comm = community[idx];
            let ki = weighted_degree[idx];

            comm_weights.clear();
            for nb in proj.outgoing(NodeId(u64::from(nid))) {
                let nb_comm = community[nb.node_id.0 as usize];
                *comm_weights.entry(nb_comm).or_insert(0.0) += nb.weight;
            }
            for nb in proj.incoming(NodeId(u64::from(nid))) {
                let nb_comm = community[nb.node_id.0 as usize];
                *comm_weights.entry(nb_comm).or_insert(0.0) += nb.weight;
            }

            let mut best_comm = current_comm;
            let mut best_delta = 0.0;

            let ki_in_current = comm_weights.get(&current_comm).copied().unwrap_or(0.0);
            let sigma_current = comm_degree_sum.get(&current_comm).copied().unwrap_or(0.0) - ki;

            for (&candidate_comm, &ki_in_candidate) in &comm_weights {
                if candidate_comm == current_comm {
                    continue;
                }
                let sigma_candidate = comm_degree_sum.get(&candidate_comm).copied().unwrap_or(0.0);

                let delta = (ki_in_candidate - ki_in_current) / total_weight
                    + ki * (sigma_current - sigma_candidate) / (m2 * m2 / 2.0);

                if delta > best_delta {
                    best_delta = delta;
                    best_comm = candidate_comm;
                }
            }

            if best_comm != current_comm {
                *comm_degree_sum.get_mut(&current_comm).unwrap() -= ki;
                *comm_degree_sum.entry(best_comm).or_insert(0.0) += ki;
                community[idx] = best_comm;
                improved = true;
            }
        }
    }

    let mut result: Vec<(NodeId, u64, u32)> = node_ids
        .iter()
        .map(|&nid| {
            (
                NodeId(u64::from(nid)),
                u64::from(community[nid as usize]),
                0,
            )
        })
        .collect();
    result.sort_by_key(|&(nid, _, _)| nid.0);
    result
}

/// Count triangles per node.
///
/// A triangle is three nodes mutually connected (treating edges as undirected).
/// Returns (node_id, triangle_count) sorted by count descending.
///
/// Uses Vec-based adjacency lists sized to `max_node + 1` for O(1) indexed
/// access. Each adjacency list is a sorted `Vec<u32>` enabling binary-search
/// intersection checks instead of `HashSet` contains.
pub fn triangle_count(proj: &GraphProjection) -> Vec<(NodeId, usize)> {
    let node_ids: Vec<u32> = proj.nodes.iter().collect();
    let max_node = proj.nodes.max().unwrap_or(0) as usize;
    let size = max_node + 1;

    let mut counts: Vec<usize> = vec![0; size];

    // Build sorted adjacency lists
    let mut adj: Vec<Vec<u32>> = vec![Vec::new(); size];
    for &nid in &node_ids {
        let neighbors = &mut adj[nid as usize];
        for nb in proj.outgoing(NodeId(u64::from(nid))) {
            neighbors.push(nb.node_id.0 as u32);
        }
        for nb in proj.incoming(NodeId(u64::from(nid))) {
            neighbors.push(nb.node_id.0 as u32);
        }
        neighbors.sort_unstable();
        neighbors.dedup();
    }

    for &u in &node_ids {
        let u_neighbors = &adj[u as usize];
        for i in 0..u_neighbors.len() {
            for j in (i + 1)..u_neighbors.len() {
                let v = u_neighbors[i];
                let w = u_neighbors[j];
                if adj[v as usize].binary_search(&w).is_ok() {
                    counts[u as usize] += 1;
                }
            }
        }
    }

    let mut result: Vec<(NodeId, usize)> = node_ids
        .iter()
        .map(|&nid| (NodeId(u64::from(nid)), counts[nid as usize]))
        .collect();
    result.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.0.cmp(&b.0.0)));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::ProjectionConfig;
    use selene_core::{IStr, LabelSet, PropertyMap};
    use selene_graph::SeleneGraph;

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

    fn two_cliques() -> SeleneGraph {
        // Two triangles connected by a bridge: (1,2,3) -- 3-4 -- (4,5,6)
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=6 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        // Clique 1: 1-2-3
        for &(s, t) in &[(1, 2), (2, 1), (2, 3), (3, 2), (1, 3), (3, 1)] {
            m.create_edge(NodeId(s), IStr::new("l"), NodeId(t), PropertyMap::new())
                .unwrap();
        }
        // Bridge: 3-4
        m.create_edge(NodeId(3), IStr::new("l"), NodeId(4), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(4), IStr::new("l"), NodeId(3), PropertyMap::new())
            .unwrap();
        // Clique 2: 4-5-6
        for &(s, t) in &[(4, 5), (5, 4), (5, 6), (6, 5), (4, 6), (6, 4)] {
            m.create_edge(NodeId(s), IStr::new("l"), NodeId(t), PropertyMap::new())
                .unwrap();
        }
        m.commit(0).unwrap();
        g
    }

    #[test]
    fn label_propagation_two_cliques() {
        let g = two_cliques();
        let proj = project_all(&g);
        let result = label_propagation(&proj, 100);
        assert_eq!(result.len(), 6);
        // Should identify ~2 communities
        let communities: std::collections::HashSet<u64> = result.iter().map(|r| r.1).collect();
        assert!(
            communities.len() <= 3,
            "expected <=3 communities, got {}",
            communities.len()
        );
    }

    #[test]
    fn label_propagation_single_node() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = label_propagation(&proj, 10);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn louvain_two_cliques() {
        let g = two_cliques();
        let proj = project_all(&g);
        let result = louvain(&proj);
        assert_eq!(result.len(), 6);
        // Louvain should group cliques together
        let communities: std::collections::HashSet<u64> = result.iter().map(|r| r.1).collect();
        // Should find 2-3 communities
        assert!(!communities.is_empty() && communities.len() <= 4);
    }

    #[test]
    fn triangle_count_clique() {
        // Triangle: 1-2-3 fully connected (undirected)
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=3 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        for &(s, t) in &[(1, 2), (2, 1), (2, 3), (3, 2), (1, 3), (3, 1)] {
            m.create_edge(NodeId(s), IStr::new("l"), NodeId(t), PropertyMap::new())
                .unwrap();
        }
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = triangle_count(&proj);
        assert_eq!(result.len(), 3);
        // Each node participates in 1 triangle
        for (_, count) in &result {
            assert_eq!(*count, 1);
        }
    }

    #[test]
    fn triangle_count_chain() {
        // Chain: 1-2-3 (no triangles)
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=3 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        m.create_edge(NodeId(1), IStr::new("l"), NodeId(2), PropertyMap::new())
            .unwrap();
        m.create_edge(NodeId(2), IStr::new("l"), NodeId(3), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = triangle_count(&proj);
        for (_, count) in &result {
            assert_eq!(*count, 0);
        }
    }

    #[test]
    fn triangle_count_two_cliques() {
        let g = two_cliques();
        let proj = project_all(&g);
        let result = triangle_count(&proj);
        // Nodes in the cliques should have triangles
        let scores: HashMap<u64, usize> = result.into_iter().map(|(n, c)| (n.0, c)).collect();
        // Nodes 1,2 participate in the (1,2,3) triangle
        assert!(scores[&1] >= 1);
        assert!(scores[&2] >= 1);
    }

    // ── Label Propagation edge cases ───────────────────────────────

    #[test]
    fn label_propagation_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        let result = label_propagation(&proj, 100);
        assert!(result.is_empty());
    }

    #[test]
    fn label_propagation_max_iterations_zero() {
        // max_iter=0 means no propagation occurs; each node keeps its own label
        let g = two_cliques();
        let proj = project_all(&g);
        let result = label_propagation(&proj, 0);
        // Every node retains its initial label (which equals its own ID)
        let communities: std::collections::HashSet<u64> = result.iter().map(|r| r.1).collect();
        assert_eq!(
            communities.len(),
            6,
            "zero iterations means 6 distinct labels"
        );
    }

    #[test]
    fn label_propagation_max_iterations_one() {
        // A single iteration should start grouping neighbors
        let g = two_cliques();
        let proj = project_all(&g);
        let result = label_propagation(&proj, 1);
        assert_eq!(result.len(), 6);
        // After one iteration, communities should have started forming
        let communities: std::collections::HashSet<u64> = result.iter().map(|r| r.1).collect();
        assert!(
            communities.len() < 6,
            "one iteration should merge some labels"
        );
    }

    #[test]
    fn label_propagation_disconnected_nodes() {
        // Isolated nodes each stay in their own community
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=4 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = label_propagation(&proj, 100);
        let communities: std::collections::HashSet<u64> = result.iter().map(|r| r.1).collect();
        assert_eq!(communities.len(), 4, "isolated nodes = 4 communities");
    }

    // ── Louvain edge cases ─────────────────────────────────────────

    #[test]
    fn louvain_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        let result = louvain(&proj);
        assert!(result.is_empty());
    }

    #[test]
    fn louvain_no_edges_each_node_own_community() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=5 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = louvain(&proj);
        let communities: std::collections::HashSet<u64> = result.iter().map(|r| r.1).collect();
        assert_eq!(
            communities.len(),
            5,
            "no edges means each node is its own community"
        );
    }

    #[test]
    fn louvain_single_node() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = louvain(&proj);
        assert_eq!(result.len(), 1);
    }

    // ── Triangle Count edge cases ──────────────────────────────────

    #[test]
    fn triangle_count_empty_graph() {
        let g = SeleneGraph::new();
        let proj = project_all(&g);
        let result = triangle_count(&proj);
        assert!(result.is_empty());
    }

    #[test]
    fn triangle_count_single_node() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = triangle_count(&proj);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, 0);
    }

    #[test]
    fn triangle_count_complete_k4() {
        // K4 has C(4,3) = 4 triangles. Each node participates in 3.
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=4 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        for &(s, t) in &[
            (1, 2),
            (2, 1),
            (1, 3),
            (3, 1),
            (1, 4),
            (4, 1),
            (2, 3),
            (3, 2),
            (2, 4),
            (4, 2),
            (3, 4),
            (4, 3),
        ] {
            m.create_edge(NodeId(s), IStr::new("l"), NodeId(t), PropertyMap::new())
                .unwrap();
        }
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = triangle_count(&proj);
        // Each of the 4 nodes participates in 3 triangles
        let total: usize = result.iter().map(|(_, c)| c).sum();
        // Each triangle counted once per participating node = 4 * 3 = 12
        assert_eq!(total, 12, "K4: 4 triangles * 3 nodes each = 12 total");
        for (_, count) in &result {
            assert_eq!(*count, 3, "each K4 node participates in 3 triangles");
        }
    }

    #[test]
    fn triangle_count_no_triangles_star() {
        // Star graph: center connected to leaves, no triangles
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 1..=5 {
            m.create_node(LabelSet::from_strs(&["n"]), PropertyMap::new())
                .unwrap();
        }
        for i in 2..=5u64 {
            m.create_edge(NodeId(1), IStr::new("l"), NodeId(i), PropertyMap::new())
                .unwrap();
            m.create_edge(NodeId(i), IStr::new("l"), NodeId(1), PropertyMap::new())
                .unwrap();
        }
        m.commit(0).unwrap();
        let proj = project_all(&g);
        let result = triangle_count(&proj);
        for (_, count) in &result {
            assert_eq!(*count, 0, "star graph has no triangles");
        }
    }

    #[test]
    fn reference_building_louvain() {
        let g = selene_testing::reference_building::reference_building(1);
        let proj = GraphProjection::build(
            &g,
            &ProjectionConfig {
                name: "adj".into(),
                node_labels: vec![],
                edge_labels: vec![IStr::new("adjacent_to")],
                weight_property: None,
            },
            None,
        );
        let result = louvain(&proj);
        // Should find communities among adjacent zones
        assert!(!result.is_empty());
    }
}
