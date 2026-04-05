//! Synthetic graph generators for benchmarking different topologies.
//!
//! These complement the reference building model with pure-structure graphs
//! that expose algorithm performance characteristics invisible in a single
//! topology.

use selene_core::{IStr, LabelSet, NodeId, PropertyMap};
use selene_graph::SeleneGraph;

/// Star graph: one hub node with `n` spoke nodes, all connected by `connects` edges.
/// Tests high-degree-hub scenarios (PageRank, betweenness hot paths).
pub fn star_graph(n: usize) -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let connects = IStr::new("connects");
    // Hub node
    let hub = m
        .create_node(LabelSet::from_strs(&["hub"]), PropertyMap::new())
        .unwrap();
    // Spoke nodes + edges
    for _ in 0..n {
        let spoke = m
            .create_node(LabelSet::from_strs(&["spoke"]), PropertyMap::new())
            .unwrap();
        m.create_edge(hub, connects, spoke, PropertyMap::new())
            .unwrap();
    }
    m.commit(0).unwrap();
    g
}

/// Chain graph: `n` nodes in a linear path.
/// Worst case for BFS depth and Dijkstra single-source.
pub fn chain_graph(n: usize) -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let next = IStr::new("next");
    let mut prev = m
        .create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
        .unwrap();
    for _ in 1..n {
        let curr = m
            .create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
            .unwrap();
        m.create_edge(prev, next, curr, PropertyMap::new()).unwrap();
        prev = curr;
    }
    m.commit(0).unwrap();
    g
}

/// Complete graph: `n` nodes, every pair connected bidirectionally.
/// Dense graph stress test. Warning: O(n^2) edges.
pub fn complete_graph(n: usize) -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let connects = IStr::new("connects");
    let nodes: Vec<NodeId> = (0..n)
        .map(|_| {
            m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
                .unwrap()
        })
        .collect();
    for i in 0..n {
        for j in (i + 1)..n {
            m.create_edge(nodes[i], connects, nodes[j], PropertyMap::new())
                .unwrap();
            m.create_edge(nodes[j], connects, nodes[i], PropertyMap::new())
                .unwrap();
        }
    }
    m.commit(0).unwrap();
    g
}

/// Random graph (deterministic Erdos-Renyi): `n` nodes, each directed edge exists
/// with probability approximately `edge_pct / 100`. Uses a simple deterministic
/// PRNG seeded with `seed` for reproducibility.
pub fn random_graph(n: usize, edge_pct: u32, seed: u64) -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let connects = IStr::new("connects");
    let nodes: Vec<NodeId> = (0..n)
        .map(|_| {
            m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
                .unwrap()
        })
        .collect();
    // Simple xorshift64 PRNG for determinism without external deps
    let mut rng = seed;
    for i in 0..n {
        for j in 0..n {
            if i == j {
                continue;
            }
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            if (rng % 100) < u64::from(edge_pct) {
                m.create_edge(nodes[i], connects, nodes[j], PropertyMap::new())
                    .unwrap();
            }
        }
    }
    m.commit(0).unwrap();
    g
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn star_graph_structure() {
        let g = star_graph(10);
        assert_eq!(g.node_count(), 11); // 1 hub + 10 spokes
        assert_eq!(g.edge_count(), 10);
    }

    #[test]
    fn chain_graph_structure() {
        let g = chain_graph(5);
        assert_eq!(g.node_count(), 5);
        assert_eq!(g.edge_count(), 4);
    }

    #[test]
    fn complete_graph_structure() {
        let g = complete_graph(4);
        assert_eq!(g.node_count(), 4);
        assert_eq!(g.edge_count(), 12); // 4*3 = 12 directed edges
    }

    #[test]
    fn random_graph_deterministic() {
        let g1 = random_graph(20, 30, 42);
        let g2 = random_graph(20, 30, 42);
        assert_eq!(g1.edge_count(), g2.edge_count()); // same seed = same graph
    }
}
