//! HNSW index construction with heuristic neighbor selection.
//!
//! Implements the build algorithm from Malkov & Yashunin (2018). Each vector is
//! inserted one at a time: greedy descent through upper layers finds the
//! approximate region, then beam search + heuristic pruning selects diverse
//! neighbors at each layer the new node participates in.
//!
//! The heuristic neighbor selection (`select_neighbors_heuristic`) ensures that
//! each node's neighbor list covers diverse directions in the vector space,
//! which is the key to high recall at 384-dim (MiniLM embeddings).

use std::sync::Arc;

use smallvec::SmallVec;

use selene_core::NodeId;

use super::distance::cosine_similarity;
use super::graph::{HnswGraph, HnswNode};
use super::params::HnswParams;
use super::search::{Candidate, beam_search, greedy_search};

// ---------------------------------------------------------------------------
// Layer assignment
// ---------------------------------------------------------------------------

/// Generate a random layer using exponential decay.
///
/// The probability of a node appearing on layer L is proportional to
/// `exp(-L / level_factor)`, producing the logarithmic layer structure that
/// gives HNSW its O(log N) search complexity. The result is capped at 32
/// to prevent degenerate graphs from pathological RNG sequences.
pub(crate) fn random_layer(level_factor: f64) -> u8 {
    let r: f64 = rand::random();
    let level = (-r.ln() * level_factor).floor() as u32;
    level.min(32) as u8
}

// ---------------------------------------------------------------------------
// Heuristic neighbor selection
// ---------------------------------------------------------------------------

/// Select up to `m` neighbors using the heuristic pruning algorithm.
///
/// Instead of simply picking the `m` closest candidates, this algorithm
/// prefers diversity: a candidate is kept only if it is closer to the target
/// than to any already-selected neighbor. This prevents neighbor lists from
/// clustering around a single dense region and produces better recall at
/// high dimensions.
///
/// `candidates` is consumed and sorted internally. The returned vector
/// contains HNSW internal indices of the selected neighbors.
pub(crate) fn select_neighbors_heuristic(
    graph: &HnswGraph,
    _target_vec: &[f32],
    mut candidates: Vec<Candidate>,
    m: usize,
) -> Vec<u32> {
    if candidates.is_empty() {
        return Vec::new();
    }

    // Sort by descending similarity to target (best first).
    candidates.sort_unstable_by(|a, b| {
        b.similarity
            .partial_cmp(&a.similarity)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut selected: Vec<u32> = Vec::with_capacity(m);
    // Cache the vectors of selected neighbors to avoid repeated graph lookups
    // across the mutation boundary (Arc clone is just an atomic refcount bump).
    let mut selected_vecs: Vec<Arc<[f32]>> = Vec::with_capacity(m);

    for candidate in &candidates {
        if selected.len() >= m {
            break;
        }

        let candidate_vec = &graph.nodes[candidate.idx as usize].vector;

        // Keep the candidate if it is closer to the target than to any
        // already-selected neighbor. This is the diversity heuristic.
        let candidate_to_target = candidate.similarity;
        let too_close_to_existing = selected_vecs
            .iter()
            .any(|sel_vec| cosine_similarity(candidate_vec, sel_vec) > candidate_to_target);

        if !too_close_to_existing {
            selected.push(candidate.idx);
            selected_vecs.push(Arc::clone(candidate_vec));
        }
    }

    // If heuristic was too aggressive and we have fewer than m neighbors,
    // fill from remaining candidates in similarity order.
    if selected.len() < m {
        for candidate in &candidates {
            if selected.len() >= m {
                break;
            }
            if !selected.contains(&candidate.idx) {
                selected.push(candidate.idx);
            }
        }
    }

    selected
}

// ---------------------------------------------------------------------------
// Single-node insertion
// ---------------------------------------------------------------------------

/// Insert one node into the HNSW graph.
///
/// Follows the HNSW insertion algorithm:
/// 1. If the graph is empty, set this node as the entry point and return.
/// 2. Greedy search from the top layer down to `node.max_layer + 1`.
/// 3. For each layer from `min(node.max_layer, graph.max_layer)` down to 0:
///    a. Beam search for `ef_construction` candidates.
///    b. Heuristic neighbor selection to pick M (or M0 for layer 0) neighbors.
///    c. Set the new node's neighbors at this layer.
///    d. Add the new node to each selected neighbor's list (bidirectional).
///    e. If a neighbor exceeds M connections, prune via heuristic selection.
/// 4. If `node.max_layer > graph.max_layer`, update the entry point.
pub fn insert_node(graph: &mut HnswGraph, node: HnswNode, params: &HnswParams) {
    let new_idx = graph.nodes.len() as u32;
    let node_max_layer = node.max_layer;
    let node_id_raw = node.node_id.0;

    graph.nodes.push(node);
    graph.node_id_to_idx.insert(node_id_raw, new_idx);

    // First node: just set as entry point.
    if graph.nodes.len() == 1 {
        graph.entry_point = Some(new_idx);
        graph.max_layer = node_max_layer;
        return;
    }

    let Some(entry) = graph.entry_point else {
        // Should not happen after the first-node check, but be defensive.
        graph.entry_point = Some(new_idx);
        graph.max_layer = node_max_layer;
        return;
    };

    // Clone the new node's vector for distance calculations (Arc clone is cheap).
    let query_vec = Arc::clone(&graph.nodes[new_idx as usize].vector);

    // Phase 1: Greedy descent through upper layers above node_max_layer.
    let mut current_entry = entry;
    let top_layer = graph.max_layer;

    if top_layer > node_max_layer {
        for layer in ((node_max_layer + 1)..=top_layer).rev() {
            current_entry = greedy_search(graph, &query_vec, current_entry, layer);
        }
    }

    // Phase 2: For each layer the new node participates in, beam search +
    // heuristic selection + bidirectional linking.
    let insert_top = node_max_layer.min(top_layer);

    for layer in (0..=insert_top).rev() {
        let max_neighbors = params.max_neighbors(layer);

        // Beam search for ef_construction candidates at this layer.
        let candidates = beam_search(
            graph,
            &query_vec,
            current_entry,
            params.ef_construction,
            layer,
            None,
        );

        // Update the entry for the next layer down (use the best candidate).
        if let Some(best) = candidates.first() {
            current_entry = best.idx;
        }

        // Heuristic neighbor selection.
        let selected = select_neighbors_heuristic(graph, &query_vec, candidates, max_neighbors);

        // Set the new node's neighbors at this layer.
        // Ensure the neighbors SmallVec has enough layers.
        let new_node = &mut graph.nodes[new_idx as usize];
        while new_node.neighbors.len() <= layer as usize {
            new_node.neighbors.push(Vec::new());
        }
        new_node.neighbors[layer as usize] = selected.clone();

        // Bidirectional linking: add new_idx to each selected neighbor's list,
        // and prune if the neighbor exceeds max connections.
        for &neighbor_idx in &selected {
            // Add new_idx to the neighbor's list for this layer.
            let neighbor = &mut graph.nodes[neighbor_idx as usize];
            while neighbor.neighbors.len() <= layer as usize {
                neighbor.neighbors.push(Vec::new());
            }
            neighbor.neighbors[layer as usize].push(new_idx);

            // Check if pruning is needed.
            let neighbor_count = graph.nodes[neighbor_idx as usize].neighbors[layer as usize].len();
            if neighbor_count > max_neighbors {
                // Build candidates from the neighbor's current connections.
                let neighbor_vec = Arc::clone(&graph.nodes[neighbor_idx as usize].vector);
                let neighbor_list: Vec<u32> =
                    graph.nodes[neighbor_idx as usize].neighbors[layer as usize].clone();

                let prune_candidates: Vec<Candidate> = neighbor_list
                    .iter()
                    .map(|&idx| {
                        let sim =
                            cosine_similarity(&graph.nodes[idx as usize].vector, &neighbor_vec);
                        Candidate {
                            idx,
                            similarity: sim,
                        }
                    })
                    .collect();

                let pruned = select_neighbors_heuristic(
                    graph,
                    &neighbor_vec,
                    prune_candidates,
                    max_neighbors,
                );

                graph.nodes[neighbor_idx as usize].neighbors[layer as usize] = pruned;
            }
        }
    }

    // If the new node's max_layer exceeds the graph's current max, update
    // the entry point.
    if node_max_layer > graph.max_layer {
        graph.max_layer = node_max_layer;
        graph.entry_point = Some(new_idx);
    }
}

// ---------------------------------------------------------------------------
// Full build
// ---------------------------------------------------------------------------

/// Build a complete HNSW graph from a set of vectors.
///
/// Each `(NodeId, Arc<[f32]>)` pair represents a property-graph node and its
/// embedding vector. Vectors are inserted sequentially; the resulting graph is
/// ready for search immediately.
///
/// When `params.quantization` is `Some`, all vectors are quantized after
/// insertion and a `QuantizedStorage` is attached to the graph.
///
/// Returns an empty graph when `vectors` is empty.
pub fn build(vectors: Vec<(NodeId, Arc<[f32]>)>, params: &HnswParams) -> HnswGraph {
    if vectors.is_empty() {
        return HnswGraph::empty(0);
    }

    let dimensions = vectors[0].1.len() as u16;
    let mut graph = HnswGraph::empty(dimensions);

    for (node_id, vector) in vectors {
        let max_layer = random_layer(params.level_factor);
        let mut neighbors = SmallVec::new();
        // Pre-allocate neighbor lists for each layer.
        for _ in 0..=max_layer {
            neighbors.push(Vec::new());
        }

        let node = HnswNode {
            node_id,
            vector,
            neighbors,
            max_layer,
        };

        insert_node(&mut graph, node, params);
    }

    // Quantize all vectors if configured.
    if let Some(ref qconfig) = params.quantization {
        let storage = super::quantize::QuantizedStorage::build(
            qconfig,
            dimensions as usize,
            graph.nodes.iter().map(|n| &*n.vector),
        );
        graph.quantized = Some(storage);
    }

    graph
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use selene_core::NodeId;

    use super::super::distance::cosine_similarity;
    use super::super::graph::HnswGraph;
    use super::super::params::HnswParams;
    use super::super::search::search;
    use super::*;

    #[test]
    fn build_empty() {
        let graph = build(vec![], &HnswParams::default());
        assert!(graph.is_empty());
        assert!(graph.entry_point.is_none());
    }

    #[test]
    fn build_single() {
        let vectors = vec![(NodeId(1), Arc::from(vec![1.0f32, 0.0, 0.0]))];
        let graph = build(vectors, &HnswParams::default());

        assert_eq!(graph.len(), 1);
        assert!(graph.entry_point.is_some());
        assert_eq!(graph.get(0).node_id, NodeId(1));
    }

    #[test]
    fn build_small_structure() {
        // Build a graph with 10 vectors and verify structural invariants.
        let mut vectors = Vec::new();
        for i in 0..10u64 {
            let angle = (i as f32) * std::f32::consts::TAU / 10.0;
            let v: Arc<[f32]> = Arc::from(vec![angle.cos(), angle.sin(), 0.0]);
            vectors.push((NodeId(i + 1), v));
        }

        let params = HnswParams::new(4); // M=4, M0=8
        let graph = build(vectors, &params);

        assert_eq!(graph.len(), 10);
        assert!(graph.entry_point.is_some());

        // Every node should have at least one neighbor on layer 0
        // (except possibly the first inserted node if M is large relative to N,
        // but with 10 nodes and M=4 all should be connected).
        for i in 0..10u32 {
            let node = graph.get(i);
            // Neighbor counts should not exceed M0 (layer 0) or M (upper layers).
            if !node.neighbors.is_empty() {
                assert!(
                    node.neighbors[0].len() <= params.m0,
                    "node {} has {} layer-0 neighbors, max is {}",
                    i,
                    node.neighbors[0].len(),
                    params.m0
                );
            }
            for layer in 1..node.neighbors.len() {
                assert!(
                    node.neighbors[layer].len() <= params.m,
                    "node {} has {} layer-{} neighbors, max is {}",
                    i,
                    node.neighbors[layer].len(),
                    layer,
                    params.m
                );
            }
        }

        // All nodes should be reachable: verify all node IDs are in the lookup.
        for i in 1..=10u64 {
            assert!(
                graph.get_by_node_id(NodeId(i)).is_some(),
                "NodeId({i}) missing from graph"
            );
        }
    }

    #[test]
    fn build_and_search_recall() {
        // Build 200 random 32-dim vectors and verify self-search recall.
        let mut vectors: Vec<(NodeId, Arc<[f32]>)> = Vec::new();
        for i in 0..200u64 {
            let v: Vec<f32> = (0..32).map(|_| rand::random::<f32>() - 0.5).collect();
            vectors.push((NodeId(i), Arc::from(v)));
        }

        // Keep copies for querying.
        let queries: Vec<(NodeId, Arc<[f32]>)> = vectors[0..5]
            .iter()
            .map(|(id, v)| (*id, Arc::clone(v)))
            .collect();

        let params = HnswParams::new(16);
        let graph = build(vectors, &params);

        assert_eq!(graph.len(), 200);

        // Each query vector should find itself as the top-1 result.
        for (node_id, query_vec) in &queries {
            let results = search(&graph, query_vec, 1, params.ef_search, None);
            assert!(
                !results.is_empty(),
                "search returned no results for NodeId({})",
                node_id.0
            );
            assert_eq!(
                results[0].0, *node_id,
                "self-search for NodeId({}) returned NodeId({}) instead",
                node_id.0, results[0].0.0
            );
        }
    }

    #[test]
    fn heuristic_prefers_diversity() {
        // Set up a small graph with vectors at known positions to verify that
        // the heuristic selects diverse neighbors rather than just the closest.
        //
        // Target: [1, 0, 0]
        // Candidates:
        //   0: [0.98, 0.20, 0]  -- closest to target (sim ~0.980)
        //   1: [0.95, 0.31, 0]  -- similar direction to c0 (sim ~0.951)
        //   2: [0.93, 0.37, 0]  -- similar direction to c0 (sim ~0.929)
        //   3: [0.95,-0.31, 0]  -- diverse: opposite y-direction (sim ~0.951)
        //
        // With m=2, heuristic should pick candidate 0 (closest) and candidate 3
        // (diverse direction), skipping candidates 1 and 2 which are redundant
        // with 0 (cosine_similarity(c1, c0) > cosine_similarity(c1, target)).
        let mut graph = HnswGraph::empty(3);

        let vecs: Vec<Vec<f32>> = vec![
            vec![0.98, 0.20, 0.0],
            vec![0.95, 0.31, 0.0],
            vec![0.93, 0.37, 0.0],
            vec![0.95, -0.31, 0.0],
        ];

        for (i, v) in vecs.iter().enumerate() {
            let node = HnswNode {
                node_id: NodeId(i as u64),
                vector: Arc::from(v.clone()),
                neighbors: SmallVec::new(),
                max_layer: 0,
            };
            graph.nodes.push(node);
            graph.node_id_to_idx.insert(i as u64, i as u32);
        }

        let target_vec: Vec<f32> = vec![1.0, 0.0, 0.0];

        let candidates: Vec<Candidate> = (0..4u32)
            .map(|i| {
                let sim = cosine_similarity(&graph.nodes[i as usize].vector, &target_vec);
                Candidate {
                    idx: i,
                    similarity: sim,
                }
            })
            .collect();

        let selected = select_neighbors_heuristic(&graph, &target_vec, candidates, 2);

        assert_eq!(selected.len(), 2, "should select exactly 2 neighbors");

        // Candidate 0 (closest) should always be selected.
        assert!(
            selected.contains(&0),
            "closest candidate (0) should be selected"
        );

        // Candidate 3 (diverse direction) should be selected over 1 and 2.
        assert!(
            selected.contains(&3),
            "diverse candidate (3) should be selected, got {selected:?}"
        );
    }

    #[test]
    fn random_layer_distribution() {
        // Generate 10000 layers and verify the distribution matches expectations.
        let level_factor = HnswParams::default().level_factor;
        let mut counts = [0u32; 34]; // layers 0..=33

        for _ in 0..10_000 {
            let layer = random_layer(level_factor);
            counts[layer as usize] += 1;
        }

        let layer_0_pct = f64::from(counts[0]) / 10_000.0;
        assert!(
            layer_0_pct > 0.85,
            "expected >85% layer 0, got {:.1}%",
            layer_0_pct * 100.0
        );

        let layer_3_plus: u32 = counts[3..].iter().sum();
        let layer_3_plus_pct = f64::from(layer_3_plus) / 10_000.0;
        assert!(
            layer_3_plus_pct < 0.02,
            "expected <2% layer 3+, got {:.1}%",
            layer_3_plus_pct * 100.0
        );
    }

    #[test]
    fn bidirectional_links() {
        // Verify that all neighbor links are bidirectional at layer 0.
        let mut vectors = Vec::new();
        for i in 0..20u64 {
            let angle = (i as f32) * std::f32::consts::TAU / 20.0;
            let v: Arc<[f32]> = Arc::from(vec![angle.cos(), angle.sin(), 0.5]);
            vectors.push((NodeId(i), v));
        }

        let params = HnswParams::new(4);
        let graph = build(vectors, &params);

        for i in 0..graph.len() as u32 {
            let neighbors = graph.neighbors(i, 0);
            for &neighbor in neighbors {
                let back_neighbors = graph.neighbors(neighbor, 0);
                assert!(
                    back_neighbors.contains(&i),
                    "node {i} has neighbor {neighbor} at layer 0, but {neighbor} does not link back to {i}"
                );
            }
        }
    }
}
