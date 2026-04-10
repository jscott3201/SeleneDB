//! HNSW search: greedy descent through upper layers + beam search at layer 0.
//!
//! Implements the two-phase search from Malkov & Yashunin (2018):
//! 1. Greedy single-neighbor descent from the top layer down to layer 1 for
//!    O(log N) navigation to the query's approximate region.
//! 2. Beam search (ef-wide) at layer 0 for high-recall local exploration.
//!
//! A `RoaringBitmap` pre-filter can restrict results to a label-scoped subset
//! without wasting beam slots on wrong-label nodes.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use roaring::RoaringBitmap;

use selene_core::NodeId;

use super::distance::cosine_similarity;
use super::graph::HnswGraph;
use super::quantize::QuantizedStorage;

// ---------------------------------------------------------------------------
// Candidate
// ---------------------------------------------------------------------------

/// A scored HNSW candidate used inside search heaps.
///
/// The `Ord` implementation is **inverted**: a higher similarity is considered
/// "smaller", so a plain `BinaryHeap<Candidate>` is effectively a **min-heap
/// by similarity** -- the *worst* result (lowest similarity) sits at the top.
/// This is exactly what we need for the results heap: `pop()` evicts the worst
/// candidate when the heap exceeds `ef`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Candidate {
    /// Internal HNSW node index.
    pub idx: u32,
    /// Cosine similarity to the query vector (higher is better).
    pub similarity: f32,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Inverted order: higher similarity => "Less", so that BinaryHeap
        // (which is a max-heap by default) surfaces the *lowest* similarity
        // at the top. This makes BinaryHeap<Candidate> behave as a min-heap
        // by similarity, enabling O(log n) eviction of the worst result.
        // NaN-safe: treat NaN as Greater (i.e. "worst possible") so it is
        // evicted first.
        other
            .similarity
            .partial_cmp(&self.similarity)
            .unwrap_or(std::cmp::Ordering::Less)
    }
}

// ---------------------------------------------------------------------------
// greedy_search
// ---------------------------------------------------------------------------

/// Single-layer greedy search.
///
/// Starting from `entry`, follow the neighbor with the highest cosine
/// similarity to `query` until no neighbor improves on the current best.
/// Returns the internal HNSW index of the closest node found.
///
/// This is used for navigating through upper layers (layer 1 and above) where
/// only a single entry point per layer is needed.
pub(crate) fn greedy_search(graph: &HnswGraph, query: &[f32], entry: u32, layer: u8) -> u32 {
    let mut current = entry;
    let mut best_sim = cosine_similarity(&graph.get(current).vector, query);

    loop {
        let mut improved = false;
        for &neighbor in graph.neighbors(current, layer) {
            let sim = cosine_similarity(&graph.get(neighbor).vector, query);
            if sim > best_sim {
                best_sim = sim;
                current = neighbor;
                improved = true;
            }
        }
        if !improved {
            break;
        }
    }

    current
}

// ---------------------------------------------------------------------------
// beam_search
// ---------------------------------------------------------------------------

/// Beam search at a single layer (typically layer 0).
///
/// Maintains an `ef`-wide frontier of the best candidates seen so far. The
/// optional `filter` bitmap restricts which nodes may appear in the returned
/// results: candidates whose `node_id` is not in the bitmap are explored for
/// connectivity but are never added to the result set.
///
/// Returns up to `ef` candidates sorted by descending similarity (best first).
pub(crate) fn beam_search(
    graph: &HnswGraph,
    query: &[f32],
    entry: u32,
    ef: usize,
    layer: u8,
    filter: Option<&RoaringBitmap>,
) -> Vec<Candidate> {
    // candidates: max-heap -- pop the best unvisited candidate first.
    // We wrap with Reverse so that BinaryHeap (max-heap by default) gives us
    // the *highest* similarity at pop time.
    let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
    // results: min-heap -- the worst result is at the top for O(log n) eviction.
    let mut results: BinaryHeap<Candidate> = BinaryHeap::new();
    let mut visited: HashSet<u32> = HashSet::new();

    let entry_sim = cosine_similarity(&graph.get(entry).vector, query);
    visited.insert(entry);
    candidates.push(Reverse(Candidate {
        idx: entry,
        similarity: entry_sim,
    }));

    // Seed results with the entry point if it passes the filter.
    let entry_node_id = graph.get(entry).node_id.0 as u32;
    if filter.is_none_or(|bm| bm.contains(entry_node_id)) {
        results.push(Candidate {
            idx: entry,
            similarity: entry_sim,
        });
    }

    loop {
        // The best unvisited candidate.
        let Some(Reverse(current)) = candidates.pop() else {
            break;
        };

        // Early termination: the best unvisited candidate is worse than our
        // worst current result and we already have ef results.
        if results.len() >= ef {
            let worst_result_sim = results.peek().map_or(f32::NEG_INFINITY, |c| c.similarity);
            if current.similarity < worst_result_sim {
                break;
            }
        }

        for &neighbor in graph.neighbors(current.idx, layer) {
            if !visited.insert(neighbor) {
                continue;
            }

            let sim = cosine_similarity(&graph.get(neighbor).vector, query);

            // Always add to the candidate frontier for traversal.
            candidates.push(Reverse(Candidate {
                idx: neighbor,
                similarity: sim,
            }));

            // Only add to results if the node passes the label filter.
            let neighbor_node_id = graph.get(neighbor).node_id.0 as u32;
            if filter.is_none_or(|bm| bm.contains(neighbor_node_id)) {
                results.push(Candidate {
                    idx: neighbor,
                    similarity: sim,
                });

                // Evict the worst result when we exceed ef.
                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }

    // Convert the min-heap to a sorted Vec (best first).
    let mut out: Vec<Candidate> = results.into_vec();
    out.sort_unstable_by(|a, b| {
        b.similarity
            .partial_cmp(&a.similarity)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

// ---------------------------------------------------------------------------
// beam_search_quantized
// ---------------------------------------------------------------------------

/// Beam search at layer 0 using asymmetric dot product with quantized codes.
///
/// Identical traversal logic to `beam_search`, but scores are computed via the
/// `PolarQuantizer::asymmetric_dot()` function instead of full f32 cosine
/// similarity. The query must be pre-rotated by calling
/// `quantizer.rotate_query()` before this function.
///
/// The approximation is one-sided (asymmetric): the query stays full-precision,
/// only the database vectors are quantized. This preserves the traversal graph
/// structure while saving ~8× on distance computation memory bandwidth.
pub(crate) fn beam_search_quantized(
    graph: &HnswGraph,
    quantized: &QuantizedStorage,
    query_rotated: &[f32],
    entry: u32,
    ef: usize,
    layer: u8,
    filter: Option<&RoaringBitmap>,
) -> Vec<Candidate> {
    let quantizer = quantized.quantizer();

    let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
    let mut results: BinaryHeap<Candidate> = BinaryHeap::new();
    let mut visited: HashSet<u32> = HashSet::new();

    let entry_sim = quantizer.asymmetric_dot(query_rotated, quantized.codes(entry));
    visited.insert(entry);
    candidates.push(Reverse(Candidate {
        idx: entry,
        similarity: entry_sim,
    }));

    let entry_node_id = graph.get(entry).node_id.0 as u32;
    if filter.is_none_or(|bm| bm.contains(entry_node_id)) {
        results.push(Candidate {
            idx: entry,
            similarity: entry_sim,
        });
    }

    loop {
        let Some(Reverse(current)) = candidates.pop() else {
            break;
        };

        if results.len() >= ef {
            let worst_result_sim = results.peek().map_or(f32::NEG_INFINITY, |c| c.similarity);
            if current.similarity < worst_result_sim {
                break;
            }
        }

        for &neighbor in graph.neighbors(current.idx, layer) {
            if !visited.insert(neighbor) {
                continue;
            }

            let sim = quantizer.asymmetric_dot(query_rotated, quantized.codes(neighbor));

            candidates.push(Reverse(Candidate {
                idx: neighbor,
                similarity: sim,
            }));

            let neighbor_node_id = graph.get(neighbor).node_id.0 as u32;
            if filter.is_none_or(|bm| bm.contains(neighbor_node_id)) {
                results.push(Candidate {
                    idx: neighbor,
                    similarity: sim,
                });

                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }

    let mut out: Vec<Candidate> = results.into_vec();
    out.sort_unstable_by(|a, b| {
        b.similarity
            .partial_cmp(&a.similarity)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

// ---------------------------------------------------------------------------
// search
// ---------------------------------------------------------------------------

/// Full HNSW approximate nearest-neighbor search.
///
/// Phase 1: greedy descent from the top layer down to layer 1, narrowing to a
/// single entry point close to the query.
/// Phase 2: beam search at layer 0 with `ef` candidates for high-recall
/// exploration.
///
/// Returns up to `k` results as `(NodeId, similarity)` pairs in descending
/// similarity order.
///
/// The optional `filter` restricts results to nodes whose raw `NodeId` value
/// (lower 32 bits) appears in the bitmap.
pub fn search(
    graph: &HnswGraph,
    query: &[f32],
    k: usize,
    ef: usize,
    filter: Option<&RoaringBitmap>,
) -> Vec<(NodeId, f32)> {
    if graph.is_empty() {
        return Vec::new();
    }

    let ef = ef.max(k);

    let Some(entry) = graph.entry_point else {
        return Vec::new();
    };

    // Phase 1: greedy descent through upper layers.
    let mut current_entry = entry;
    let top_layer = graph.max_layer;
    if top_layer >= 1 {
        for layer in (1..=top_layer).rev() {
            current_entry = greedy_search(graph, query, current_entry, layer);
        }
    }

    // Phase 2: beam search at layer 0.
    let candidates = beam_search(graph, query, current_entry, ef, 0, filter);

    // Return top-k as (NodeId, similarity).
    candidates
        .into_iter()
        .take(k)
        .map(|c| (graph.get(c.idx).node_id, c.similarity))
        .collect()
}

// ---------------------------------------------------------------------------
// search_quantized
// ---------------------------------------------------------------------------

/// HNSW search using asymmetric quantized distance at layer 0.
///
/// Same structure as `search()` but uses `beam_search_quantized()` for the
/// bulk of the distance computation. Upper-layer greedy descent still uses f32
/// cosine (few comparisons, high accuracy needed for entry point selection).
///
/// When `rescore` is true, the top `k` candidates are re-ranked using full f32
/// cosine similarity to recover any ordering errors from quantization.
pub fn search_quantized(
    graph: &HnswGraph,
    quantized: &QuantizedStorage,
    query: &[f32],
    k: usize,
    ef: usize,
    rescore: bool,
    filter: Option<&RoaringBitmap>,
) -> Vec<(NodeId, f32)> {
    if graph.is_empty() {
        return Vec::new();
    }

    let ef = ef.max(k);

    let Some(entry) = graph.entry_point else {
        return Vec::new();
    };

    // Phase 1: greedy descent through upper layers (f32 cosine).
    let mut current_entry = entry;
    let top_layer = graph.max_layer;
    if top_layer >= 1 {
        for layer in (1..=top_layer).rev() {
            current_entry = greedy_search(graph, query, current_entry, layer);
        }
    }

    // Phase 2: quantized beam search at layer 0.
    let query_rotated = quantized.quantizer().rotate_query(query);
    let candidates = beam_search_quantized(
        graph,
        quantized,
        &query_rotated,
        current_entry,
        ef,
        0,
        filter,
    );

    if rescore {
        // Re-score top candidates with full f32 cosine similarity.
        let mut rescored: Vec<(NodeId, f32)> = candidates
            .into_iter()
            .take(ef) // rescore the full ef set for best accuracy
            .map(|c| {
                let node = graph.get(c.idx);
                let sim = cosine_similarity(&node.vector, query);
                (node.node_id, sim)
            })
            .collect();
        rescored.sort_unstable_by(|(_, a), (_, b)| {
            b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)
        });
        rescored.truncate(k);
        rescored
    } else {
        // Return quantized scores directly.
        candidates
            .into_iter()
            .take(k)
            .map(|c| (graph.get(c.idx).node_id, c.similarity))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use smallvec::SmallVec;

    use selene_core::NodeId;

    use super::super::graph::{HnswGraph, HnswNode};
    use super::*;

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn make_node(raw_id: u64, vector: Vec<f32>, max_layer: u8) -> HnswNode {
        HnswNode {
            node_id: NodeId(raw_id),
            vector: vector.into(),
            neighbors: SmallVec::new(),
            max_layer,
        }
    }

    /// Build a three-node graph for reuse across tests.
    ///
    /// Nodes:
    ///   0 -> NodeId(1), vector [1, 0, 0]
    ///   1 -> NodeId(2), vector [0, 1, 0]
    ///   2 -> NodeId(3), vector [0.9, 0.1, 0] (normalized inline)
    ///
    /// All nodes on layer 0 only. Fully connected bidirectionally.
    fn three_node_graph() -> HnswGraph {
        let mut g = HnswGraph::empty(3);

        // Node 0: [1, 0, 0]
        let mut n0 = make_node(1, vec![1.0, 0.0, 0.0], 0);
        n0.neighbors.push(vec![1, 2]); // layer 0

        // Node 1: [0, 1, 0]
        let mut n1 = make_node(2, vec![0.0, 1.0, 0.0], 0);
        n1.neighbors.push(vec![0, 2]); // layer 0

        // Node 2: [0.9, 0.1, 0] (not unit; cosine_similarity handles normalisation)
        let mut n2 = make_node(3, vec![0.9, 0.1, 0.0], 0);
        n2.neighbors.push(vec![0, 1]); // layer 0

        g.nodes.push(n0);
        g.nodes.push(n1);
        g.nodes.push(n2);

        g.node_id_to_idx.insert(1, 0);
        g.node_id_to_idx.insert(2, 1);
        g.node_id_to_idx.insert(3, 2);

        g.entry_point = Some(0);
        g.max_layer = 0;
        g
    }

    // ------------------------------------------------------------------
    // search tests
    // ------------------------------------------------------------------

    #[test]
    fn search_empty_graph() {
        let g = HnswGraph::empty(3);
        let result = search(&g, &[1.0, 0.0, 0.0], 5, 10, None);
        assert!(result.is_empty());
    }

    #[test]
    fn search_single_node() {
        let mut g = HnswGraph::empty(3);
        let mut n = make_node(42, vec![1.0, 0.0, 0.0], 0);
        n.neighbors.push(vec![]); // layer 0, no neighbors
        g.nodes.push(n);
        g.node_id_to_idx.insert(42, 0);
        g.entry_point = Some(0);
        g.max_layer = 0;

        let result = search(&g, &[1.0, 0.0, 0.0], 5, 10, None);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, NodeId(42));
        assert!((result[0].1 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn search_finds_nearest_3_nodes() {
        let g = three_node_graph();

        // Query [1, 0, 0] should rank:
        //   NodeId(1) [1,0,0]      -> similarity 1.0   (exact match)
        //   NodeId(3) [0.9,0.1,0]  -> similarity ~0.994 (close)
        //   NodeId(2) [0,1,0]      -> similarity 0.0   (orthogonal)
        let result = search(&g, &[1.0, 0.0, 0.0], 3, 10, None);

        assert_eq!(result.len(), 3, "should return all 3 nodes");

        // Best result must be NodeId(1) (exact match).
        assert_eq!(result[0].0, NodeId(1), "closest node must be [1,0,0]");
        assert!((result[0].1 - 1.0).abs() < 1e-6, "similarity must be ~1.0");

        // Second result must be NodeId(3) (most similar after exact match).
        assert_eq!(result[1].0, NodeId(3), "second closest must be [0.9,0.1,0]");
        assert!(
            result[1].1 > result[2].1,
            "second result must be better than third"
        );
    }

    #[test]
    fn search_with_prefilter() {
        let g = three_node_graph();

        // Exclude NodeId(3) (raw id 3, so bitmap should NOT contain 3).
        // Filter bitmap contains only raw ids 1 and 2.
        let mut bm = RoaringBitmap::new();
        bm.insert(1); // NodeId(1)
        bm.insert(2); // NodeId(2)
        // NodeId(3) is excluded.

        let result = search(&g, &[1.0, 0.0, 0.0], 3, 10, Some(&bm));

        // Should never return NodeId(3).
        for (node_id, _) in &result {
            assert_ne!(*node_id, NodeId(3), "filtered node must not appear");
        }

        // NodeId(1) must still be first (best match among allowed nodes).
        assert!(!result.is_empty());
        assert_eq!(result[0].0, NodeId(1));
    }

    // ------------------------------------------------------------------
    // greedy_search tests
    // ------------------------------------------------------------------

    #[test]
    fn greedy_search_converges() {
        // Build a small two-layer graph:
        //   Node 0 (entry, layer 1): [1, 0, 0]
        //   Node 1 (layer 0 only):   [0.5, 0.5, 0]
        //   Node 2 (layer 0 only):   [0, 1, 0]
        //
        // On layer 1, only node 0 appears (no layer-1 neighbors pointing
        // elsewhere), so greedy_search on layer 1 stays at node 0.
        let mut g = HnswGraph::empty(3);

        let mut n0 = make_node(10, vec![1.0, 0.0, 0.0], 1);
        n0.neighbors.push(vec![1, 2]); // layer 0
        n0.neighbors.push(vec![]); // layer 1: no neighbors

        let mut n1 = make_node(20, vec![0.5, 0.5, 0.0], 0);
        n1.neighbors.push(vec![0, 2]); // layer 0

        let mut n2 = make_node(30, vec![0.0, 1.0, 0.0], 0);
        n2.neighbors.push(vec![0, 1]); // layer 0

        g.nodes.push(n0);
        g.nodes.push(n1);
        g.nodes.push(n2);
        g.node_id_to_idx.insert(10, 0);
        g.node_id_to_idx.insert(20, 1);
        g.node_id_to_idx.insert(30, 2);
        g.entry_point = Some(0);
        g.max_layer = 1;

        // Query [1, 0, 0]: node 0 is closest. Greedy on layer 1 should stay
        // at node 0 (no layer-1 neighbors improve similarity).
        let found = greedy_search(&g, &[1.0, 0.0, 0.0], 0, 1);
        assert_eq!(found, 0, "greedy must converge at node 0 for query [1,0,0]");

        // Query [0, 1, 0]: node 2 is closest but layer-1 neighbor list of
        // node 0 is empty, so greedy on layer 1 stays at node 0.
        let found2 = greedy_search(&g, &[0.0, 1.0, 0.0], 0, 1);
        assert_eq!(
            found2, 0,
            "greedy on layer 1 stays at 0 (no layer-1 neighbors)"
        );
    }

    // ------------------------------------------------------------------
    // Ordering invariant
    // ------------------------------------------------------------------

    #[test]
    fn candidate_ord_min_heap() {
        // Candidate's Ord is inverted: higher similarity => "Less".
        // BinaryHeap<Candidate> therefore surfaces the *lowest* similarity at
        // the top (min-heap by similarity). This is the correct behavior for
        // the results heap in beam_search: pop() evicts the worst result.
        let mut heap: BinaryHeap<Candidate> = BinaryHeap::new();
        heap.push(Candidate {
            idx: 0,
            similarity: 0.9,
        });
        heap.push(Candidate {
            idx: 1,
            similarity: 0.5,
        });
        heap.push(Candidate {
            idx: 2,
            similarity: 0.7,
        });

        // peek() shows the worst (lowest similarity) result.
        let top = heap.peek().unwrap();
        assert_eq!(
            top.similarity, 0.5,
            "BinaryHeap<Candidate> top must be lowest similarity (worst result)"
        );

        // pop() removes the lowest similarity, leaving 0.7 at the top.
        heap.pop();
        assert_eq!(
            heap.peek().unwrap().similarity,
            0.7,
            "after pop, next-lowest must be at top"
        );
    }

    #[test]
    fn reverse_candidate_max_heap() {
        // Verify that BinaryHeap<Reverse<Candidate>> surfaces the *highest*
        // similarity first, as used for the candidates frontier in beam_search.
        let mut heap: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
        heap.push(Reverse(Candidate {
            idx: 0,
            similarity: 0.9,
        }));
        heap.push(Reverse(Candidate {
            idx: 1,
            similarity: 0.5,
        }));
        heap.push(Reverse(Candidate {
            idx: 2,
            similarity: 0.7,
        }));

        let Reverse(top) = heap.peek().unwrap();
        assert_eq!(
            top.similarity, 0.9,
            "Reverse<Candidate> heap top must be highest similarity (best candidate)"
        );
    }

    // ------------------------------------------------------------------
    // Quantized search tests
    // ------------------------------------------------------------------

    use super::super::quantize::{QuantBits, QuantizationConfig, QuantizedStorage};

    /// Generate a deterministic pseudo-random unit vector.
    fn pseudo_unit_vector(seed: u64, dims: usize) -> Vec<f32> {
        let mut v: Vec<f32> = Vec::with_capacity(dims);
        let mut state = seed.wrapping_add(1).wrapping_mul(6_364_136_223_846_793_005);
        for _ in 0..dims {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let top = (state >> 32) as i32;
            v.push(top as f32 / i32::MAX as f32);
        }
        let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if mag > 0.0 {
            for x in &mut v {
                *x /= mag;
            }
        }
        v
    }

    /// Build a connected graph from pseudorandom vectors and quantize it.
    fn build_quantized_graph(
        n: usize,
        dims: usize,
        bits: QuantBits,
    ) -> (HnswGraph, QuantizedStorage) {
        let config = QuantizationConfig {
            bits,
            seed: 42,
            rescore: false,
        };

        let mut g = HnswGraph::empty(dims as u16);
        for i in 0..n {
            let v = pseudo_unit_vector(i as u64, dims);
            let mut node = make_node(i as u64 + 1, v, 0);
            // Fully connect all nodes on layer 0 (small graph).
            let neighbors: Vec<u32> = (0..n as u32).filter(|&x| x != i as u32).collect();
            node.neighbors.push(neighbors);
            g.nodes.push(node);
            g.node_id_to_idx.insert(i as u64 + 1, i as u32);
        }
        g.entry_point = Some(0);
        g.max_layer = 0;

        let qs = QuantizedStorage::build(
            &config,
            dims,
            g.nodes.iter().map(|n| &*n.vector),
        );

        (g, qs)
    }

    #[test]
    fn search_quantized_returns_results() {
        let (g, qs) = build_quantized_graph(20, 64, QuantBits::Four);
        let query = pseudo_unit_vector(0, 64);
        let results = search_quantized(&g, &qs, &query, 5, 20, false, None);
        assert!(!results.is_empty());
        // Closest to seed 0 should be NodeId(1) (which is seed 0's vector).
        assert_eq!(results[0].0, NodeId(1));
    }

    #[test]
    fn search_quantized_ranking_consistency() {
        let (g, qs) = build_quantized_graph(50, 64, QuantBits::Four);
        let query = pseudo_unit_vector(0, 64);

        let exact = search(&g, &query, 10, 50, None);
        let quantized = search_quantized(&g, &qs, &query, 10, 50, false, None);

        // The top-1 result should match (4-bit at 64 dims is very accurate).
        assert_eq!(
            exact[0].0, quantized[0].0,
            "top-1 must match between exact and quantized search"
        );

        // At least 7 of top-10 should overlap (recall@10 > 70%).
        let exact_ids: HashSet<NodeId> = exact.iter().map(|r| r.0).collect();
        let quant_ids: HashSet<NodeId> = quantized.iter().map(|r| r.0).collect();
        let overlap = exact_ids.intersection(&quant_ids).count();
        assert!(
            overlap >= 7,
            "recall@10 must be >= 70%, got {overlap}/10"
        );
    }

    #[test]
    fn search_quantized_with_rescore() {
        let (g, qs) = build_quantized_graph(50, 64, QuantBits::Four);
        let query = pseudo_unit_vector(5, 64);

        let rescored = search_quantized(&g, &qs, &query, 10, 50, true, None);
        let exact = search(&g, &query, 10, 50, None);

        // Rescored results should have exact f32 similarity values.
        assert!(!rescored.is_empty());
        // The similarities should be cosine values (between -1 and 1).
        for &(_, sim) in &rescored {
            assert!((-1.0..=1.0001).contains(&sim), "sim {sim} out of cosine range");
        }

        // Rescore should improve recall vs non-rescored.
        let rescored_ids: HashSet<NodeId> = rescored.iter().map(|r| r.0).collect();
        let exact_ids: HashSet<NodeId> = exact.iter().map(|r| r.0).collect();
        let overlap = rescored_ids.intersection(&exact_ids).count();
        assert!(
            overlap >= 8,
            "rescored recall@10 must be >= 80%, got {overlap}/10"
        );
    }

    #[test]
    fn search_quantized_8bit_high_recall() {
        let (g, qs) = build_quantized_graph(50, 64, QuantBits::Eight);
        let query = pseudo_unit_vector(10, 64);

        let exact = search(&g, &query, 10, 50, None);
        let quantized = search_quantized(&g, &qs, &query, 10, 50, false, None);

        // 8-bit should have near-perfect recall.
        let exact_ids: HashSet<NodeId> = exact.iter().map(|r| r.0).collect();
        let quant_ids: HashSet<NodeId> = quantized.iter().map(|r| r.0).collect();
        let overlap = exact_ids.intersection(&quant_ids).count();
        assert!(
            overlap >= 9,
            "8-bit recall@10 must be >= 90%, got {overlap}/10"
        );
    }

    #[test]
    fn search_quantized_empty_graph() {
        let g = HnswGraph::empty(3);
        let config = QuantizationConfig::default();
        let qs = QuantizedStorage::build(&config, 3, std::iter::empty());
        let results = search_quantized(&g, &qs, &[1.0, 0.0, 0.0], 5, 10, false, None);
        assert!(results.is_empty());
    }

    #[test]
    fn search_quantized_with_filter() {
        let (g, qs) = build_quantized_graph(20, 64, QuantBits::Four);
        let query = pseudo_unit_vector(0, 64);

        // Only allow NodeIds 1-10 in results.
        let mut bm = RoaringBitmap::new();
        for i in 1..=10_u32 {
            bm.insert(i);
        }

        let results = search_quantized(&g, &qs, &query, 5, 20, false, Some(&bm));
        for (id, _) in &results {
            assert!(
                (1..=10).contains(&id.0),
                "filtered result must be in range 1-10, got {}", id.0
            );
        }
    }
}
