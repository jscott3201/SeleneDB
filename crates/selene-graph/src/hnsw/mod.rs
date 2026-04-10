//! HNSW (Hierarchical Navigable Small World) approximate nearest neighbor index.
//!
//! Provides sub-millisecond vector similarity search for 10K-100K vectors
//! with >95% recall, replacing brute-force O(N) scans.
//!
//! # Public API
//!
//! `HnswIndex` is the primary entry point. It uses a hybrid architecture:
//! `ArcSwap<HnswGraph>` for lock-free reads (~1ns) and `RwLock<HnswGraph>`
//! for mutable incremental inserts (O(log N) per insert).
//!
//! ```text
//! HnswIndex
//!   read_snapshot:       ArcSwap<HnswGraph>   -- immutable, lock-free (~1 ns load)
//!   write_graph:         RwLock<HnswGraph>     -- mutable, incremental inserts
//!   generation:          AtomicU64             -- monotonic mutation counter
//!   snapshot_generation: AtomicU64             -- generation at last snapshot
//!   tombstones:          Mutex<RoaringBitmap>  -- deleted node IDs
//!   params:              HnswParams            -- M, ef_construction, ...
//! ```
//!
//! Snapshots clone the mutable graph into the ArcSwap, maintaining the
//! lock-free read path for the common case (no pending mutations).

pub mod build;
pub mod distance;
pub mod graph;
pub mod params;
pub mod quantize;
pub mod search;

pub use build::build;
pub use graph::HnswGraph;
pub use params::HnswParams;
pub use search::search;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use selene_core::NodeId;

use self::graph::HnswNode;

// ---------------------------------------------------------------------------
// HnswIndex
// ---------------------------------------------------------------------------

/// Hybrid HNSW index with incremental inserts and lock-free snapshot reads.
///
/// The read path loads an immutable `HnswGraph` snapshot via `ArcSwap` (~1ns,
/// no refcount increment). When mutations are pending (generation >
/// snapshot_generation), search also acquires a read lock on the mutable
/// `write_graph` and merges results from both graphs.
///
/// Write operations (`insert`, `remove`) acquire the `write_graph` write lock
/// and bump the generation counter. Periodic `snapshot()` calls clone the
/// mutable graph (excluding tombstones) into the `ArcSwap`, restoring the
/// fast lock-free read path.
pub struct HnswIndex {
    /// Immutable snapshot for the read-hot-path. Lock-free load (~1ns).
    read_snapshot: ArcSwap<HnswGraph>,
    /// Mutable graph for incremental inserts.
    write_graph: RwLock<HnswGraph>,
    /// Monotonic counter, bumped on each mutation.
    generation: AtomicU64,
    /// Generation at last snapshot.
    snapshot_generation: AtomicU64,
    /// Deleted node IDs.
    tombstones: Mutex<RoaringBitmap>,
    /// Index configuration.
    params: HnswParams,
}

impl HnswIndex {
    /// Create an empty index for vectors of the given `dimensions`.
    pub fn new(params: HnswParams, dimensions: u16) -> Self {
        let empty = HnswGraph::empty(dimensions);
        Self {
            read_snapshot: ArcSwap::from(Arc::new(empty.clone())),
            write_graph: RwLock::new(empty),
            generation: AtomicU64::new(0),
            snapshot_generation: AtomicU64::new(0),
            tombstones: Mutex::new(RoaringBitmap::new()),
            params,
        }
    }

    /// Create an index from an already-built `HnswGraph`.
    ///
    /// Both the read snapshot and write graph are initialized from `graph`.
    /// Use this after deserializing a persisted graph from a snapshot.
    pub fn from_graph(graph: HnswGraph, params: HnswParams) -> Self {
        Self {
            read_snapshot: ArcSwap::from(Arc::new(graph.clone())),
            write_graph: RwLock::new(graph),
            generation: AtomicU64::new(0),
            snapshot_generation: AtomicU64::new(0),
            tombstones: Mutex::new(RoaringBitmap::new()),
            params,
        }
    }

    /// Insert a vector into the index incrementally.
    ///
    /// Acquires the write lock, inserts the node using the HNSW insertion
    /// algorithm (O(log N)), and bumps the generation counter. The vector is
    /// immediately visible to subsequent `search()` calls via the write_graph
    /// merge path.
    ///
    /// If quantization is enabled, the vector is also encoded and appended to
    /// the quantized storage. On the first insert with quantization configured,
    /// the storage is lazily initialized from all existing vectors.
    pub fn insert(&self, node_id: NodeId, vector: Arc<[f32]>) {
        let max_layer = build::random_layer(self.params.level_factor);
        let mut neighbors = SmallVec::new();
        for _ in 0..=max_layer {
            neighbors.push(Vec::new());
        }
        let node = HnswNode {
            node_id,
            vector: Arc::clone(&vector),
            neighbors,
            max_layer,
        };

        let mut wg = self.write_graph.write();
        build::insert_node(&mut wg, node, &self.params);

        // Encode for quantized storage if quantization is configured.
        if let Some(ref qconfig) = self.params.quantization {
            if wg.quantized.is_none() {
                // Lazy init: encode all existing vectors (including the one just inserted).
                let storage = quantize::QuantizedStorage::build(
                    qconfig,
                    vector.len(),
                    wg.nodes.iter().map(|n| &*n.vector),
                );
                wg.quantized = Some(storage);
            } else {
                wg.quantized.as_mut().unwrap().encode_and_push(&vector);
            }
        }
        drop(wg);

        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Mark a node as deleted, reconnecting its neighbors first.
    ///
    /// Before tombstoning, repairs the write graph by removing the deleted
    /// node from its neighbors' adjacency lists and filling empty slots with
    /// connections to the deleted node's other neighbors (MN-RU). This
    /// suppresses the "unreachable points" phenomenon at high tombstone ratios.
    ///
    /// Tombstoned nodes are excluded from all future search results. The node
    /// remains in the graph structure until the next `snapshot()` or `rebuild()`
    /// removes it physically.
    pub fn remove(&self, node_id: NodeId) {
        // Reconnect mutual neighbors before tombstoning.
        {
            let mut wg = self.write_graph.write();
            if let Some(&idx) = wg.node_id_to_idx.get(&node_id.0) {
                wg.reconnect_neighbors(idx, &self.params);
            }
        }
        self.tombstones.lock().insert(node_id.0 as u32);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Snapshot the mutable graph into the read-only ArcSwap.
    ///
    /// Clones the write_graph (excluding tombstoned nodes), atomically
    /// publishes it via ArcSwap, clears tombstones, and updates the
    /// snapshot_generation. After this call, `has_pending_mutations()` returns
    /// false and reads use the lock-free path.
    pub fn snapshot(&self) {
        let tombstones = {
            let mut ts = self.tombstones.lock();
            std::mem::take(&mut *ts)
        };

        // Hold the write lock for the entire clone-and-replace to prevent
        // concurrent inserts from being lost between the clone and swap.
        let new_graph = {
            let mut wg = self.write_graph.write();
            let clean = wg.clone_without(&tombstones);
            *wg = clean.clone();
            clean
        };

        self.read_snapshot.store(Arc::new(new_graph));

        let current_gen = self.generation.load(Ordering::Acquire);
        self.snapshot_generation
            .store(current_gen, Ordering::Release);
    }

    /// Returns `true` when mutations have occurred since the last snapshot.
    pub fn has_pending_mutations(&self) -> bool {
        self.generation.load(Ordering::Acquire) > self.snapshot_generation.load(Ordering::Acquire)
    }

    /// Fraction of nodes that are tombstoned relative to write_graph size.
    ///
    /// Returns 0.0 when the graph is empty.
    pub fn tombstone_ratio(&self) -> f64 {
        let tombstone_count = self.tombstones.lock().len() as f64;
        let total = self.write_graph.read().len() as f64;
        if total == 0.0 {
            0.0
        } else {
            tombstone_count / total
        }
    }

    /// Number of mutations since the last snapshot.
    pub fn pending_count(&self) -> u64 {
        let current_gen = self.generation.load(Ordering::Acquire);
        let snap_gen = self.snapshot_generation.load(Ordering::Acquire);
        current_gen.saturating_sub(snap_gen)
    }

    /// Number of vectors in the read snapshot graph.
    pub fn len(&self) -> usize {
        self.read_snapshot.load().len()
    }

    /// Returns `true` when both the read snapshot and write graph are empty.
    pub fn is_empty(&self) -> bool {
        self.read_snapshot.load().is_empty() && self.write_graph.read().is_empty()
    }

    /// Search for the `k` most similar vectors to `query`.
    ///
    /// Combines up to two phases:
    /// 1. Search the immutable read_snapshot via ArcSwap guard (~1ns load).
    /// 2. If mutations are pending, also search the write_graph under a read
    ///    lock and merge results.
    ///
    /// When quantized storage is available, layer-0 beam search uses asymmetric
    /// dot product (~8× less memory bandwidth) instead of full f32 cosine
    /// similarity. If `rescore` is enabled in the quantization config, top-k
    /// candidates are re-ranked with exact f32 cosine.
    ///
    /// Tombstoned node IDs are filtered from the merged result list. The final
    /// result is sorted by descending similarity and truncated to `k`.
    ///
    /// `ef` is the beam width for HNSW search. When `None`, `params.ef_search`
    /// is used.
    ///
    /// The optional `filter` restricts results to nodes whose raw `NodeId`
    /// value (lower 32 bits) appears in the bitmap.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
        filter: Option<&RoaringBitmap>,
    ) -> Vec<(NodeId, f32)> {
        if k == 0 {
            return Vec::new();
        }

        let ef = ef.unwrap_or(self.params.ef_search);
        let rescore = self
            .params
            .quantization
            .as_ref()
            .is_some_and(|q| q.rescore);

        // Snapshot tombstones once to avoid locking twice.
        let tombstones_snapshot = self.tombstones.lock().clone();
        let k_expanded = k.saturating_add(tombstones_snapshot.len() as usize);

        // --- Phase 1: Search the immutable read snapshot ---
        let guard = self.read_snapshot.load();

        let mut results: Vec<(NodeId, f32)> = if guard.is_empty() {
            Vec::new()
        } else if let Some(qs) = guard.quantized() {
            search::search_quantized(&guard, qs, query, k_expanded, ef, rescore, filter)
        } else {
            search::search(&guard, query, k_expanded, ef, filter)
        };

        // --- Phase 2: Search write_graph if mutations are pending ---
        if self.has_pending_mutations() {
            let wg = self.write_graph.read();
            if !wg.is_empty() {
                let write_results = if let Some(qs) = wg.quantized() {
                    search::search_quantized(&wg, qs, query, k_expanded, ef, rescore, filter)
                } else {
                    search::search(&wg, query, k_expanded, ef, filter)
                };

                // Merge: union by NodeId, keep highest similarity score.
                // For small result sets (typical k < 64), linear scan is
                // faster than HashMap allocation.
                for (id, sim) in write_results {
                    if let Some(pos) = results.iter().position(|(eid, _)| *eid == id) {
                        if sim > results[pos].1 {
                            results[pos].1 = sim;
                        }
                    } else {
                        results.push((id, sim));
                    }
                }
            }
        }

        // --- Phase 3: Filter tombstones ---
        if !tombstones_snapshot.is_empty() {
            results.retain(|(node_id, _)| !tombstones_snapshot.contains(node_id.0 as u32));
        }

        // --- Phase 4: Sort by descending similarity and return top-k ---
        results.sort_unstable_by(|(_, a), (_, b)| {
            b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(k);
        results
    }

    /// Build a new `HnswGraph` from `vectors` and publish it to both graphs.
    ///
    /// After the rebuild, tombstones are cleared and snapshot_generation is
    /// updated. Used for bulk operations (initial build, full rebuild).
    ///
    /// # Concurrency
    ///
    /// Not safe to call concurrently with `insert()`. The build phase runs
    /// without holding the write lock, so inserts between build and swap are
    /// lost. The caller must ensure no inserts are in flight during rebuild.
    pub fn rebuild(&self, vectors: Vec<(NodeId, Arc<[f32]>)>) {
        let new_graph = build::build(vectors, &self.params);
        // Update write_graph.
        *self.write_graph.write() = new_graph.clone();
        // Update read_snapshot.
        self.read_snapshot.store(Arc::new(new_graph));
        // Clear state.
        self.tombstones.lock().clear();
        let current_gen = self.generation.load(Ordering::Acquire);
        self.snapshot_generation
            .store(current_gen, Ordering::Release);
    }

    /// Return an `Arc<HnswGraph>` for serialization or inspection.
    ///
    /// Returns the immutable read snapshot. Use `search` for hot paths; use
    /// this only for snapshot persistence.
    pub fn load_graph(&self) -> Arc<HnswGraph> {
        self.read_snapshot.load_full()
    }

    /// Return a reference to the index configuration.
    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    /// Clone the current tombstone bitmap.
    ///
    /// Used by the rebuild coordinator to exclude deleted node IDs from the
    /// vector set passed to `rebuild`.
    pub fn tombstones(&self) -> RoaringBitmap {
        self.tombstones.lock().clone()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use selene_core::NodeId;

    use super::*;

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Unit vector along the i-th canonical axis in `dims` dimensions.
    fn axis_vector(i: usize, dims: usize) -> Arc<[f32]> {
        let mut v = vec![0.0f32; dims];
        v[i % dims] = 1.0;
        Arc::from(v)
    }

    /// Deterministic unit vector seeded by `seed` (no rand dep required).
    ///
    /// Uses a LCG per-dimension so each seed produces a distinct direction.
    fn pseudo_unit_vector(seed: u64, dims: usize) -> Arc<[f32]> {
        let mut v: Vec<f32> = Vec::with_capacity(dims);
        // Per-dimension LCG: mix seed with dimension index for independence.
        let mut state = seed.wrapping_add(1).wrapping_mul(6_364_136_223_846_793_005);
        for _ in 0..dims {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            // Map top bits to [-1, 1].
            let top = (state >> 32) as i32;
            v.push(top as f32 / i32::MAX as f32);
        }
        let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if mag > 0.0 {
            for x in &mut v {
                *x /= mag;
            }
        }
        Arc::from(v)
    }

    // ------------------------------------------------------------------
    // new_empty_index
    // ------------------------------------------------------------------

    #[test]
    fn new_empty_index() {
        let index = HnswIndex::new(HnswParams::default(), 32);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert!(!index.has_pending_mutations());
        assert_eq!(index.pending_count(), 0);

        let results = index.search(&[0.0f32; 32], 10, None, None);
        assert!(results.is_empty(), "empty index must return no results");
    }

    // ------------------------------------------------------------------
    // insert_and_search (adapted from stage_insert_and_search)
    // ------------------------------------------------------------------

    #[test]
    fn insert_and_search() {
        let index = HnswIndex::new(HnswParams::default(), 4);

        // Insert a single vector.
        let v = axis_vector(0, 4); // [1, 0, 0, 0]
        index.insert(NodeId(1), Arc::clone(&v));

        // Search must find the inserted vector via the write_graph merge path.
        let query = [1.0f32, 0.0, 0.0, 0.0];
        let results = index.search(&query, 5, None, None);

        assert_eq!(results.len(), 1, "should find the one inserted vector");
        assert_eq!(results[0].0, NodeId(1));
        assert!(
            (results[0].1 - 1.0).abs() < 1e-6,
            "similarity should be ~1.0"
        );
    }

    // ------------------------------------------------------------------
    // tombstone_excludes_from_search
    // ------------------------------------------------------------------

    #[test]
    fn tombstone_excludes_from_search() {
        let index = HnswIndex::new(HnswParams::default(), 3);

        // Insert two vectors.
        index.insert(NodeId(1), axis_vector(0, 3)); // [1, 0, 0]
        index.insert(NodeId(2), axis_vector(1, 3)); // [0, 1, 0]

        // Tombstone node 1 (the one closest to the query).
        index.remove(NodeId(1));

        let query = [1.0f32, 0.0, 0.0];
        let results = index.search(&query, 5, None, None);

        // Only NodeId(2) should be returned.
        assert_eq!(results.len(), 1, "tombstoned node must be excluded");
        assert_eq!(
            results[0].0,
            NodeId(2),
            "only non-tombstoned node should appear"
        );
    }

    // ------------------------------------------------------------------
    // rebuild_clears_state
    // ------------------------------------------------------------------

    #[test]
    fn rebuild_clears_state() {
        let index = HnswIndex::new(HnswParams::new(4), 3);

        // Insert some vectors and add a tombstone.
        index.insert(NodeId(1), axis_vector(0, 3));
        index.insert(NodeId(2), axis_vector(1, 3));
        index.remove(NodeId(99));

        // Rebuild from a full set of vectors.
        let vectors = vec![
            (NodeId(1), axis_vector(0, 3)),
            (NodeId(2), axis_vector(1, 3)),
        ];
        index.rebuild(vectors);

        // Tombstones must be cleared.
        assert!(
            index.tombstones.lock().is_empty(),
            "tombstones must be cleared after rebuild"
        );

        // Graph must now contain the 2 built nodes.
        assert_eq!(index.len(), 2);
        assert!(
            !index.has_pending_mutations(),
            "rebuild should reset pending mutations"
        );

        // Search should still work.
        let results = index.search(&[1.0f32, 0.0, 0.0], 1, None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, NodeId(1));
    }

    // ------------------------------------------------------------------
    // rebuild_and_search_recall
    // ------------------------------------------------------------------

    #[test]
    fn rebuild_and_search_recall() {
        // Build from 50 deterministic unit vectors; every vector must find
        // itself as the top-1 result (self-search recall = 100%).
        const N: u64 = 50;
        const DIMS: usize = 32;

        let vectors: Vec<(NodeId, Arc<[f32]>)> = (0..N)
            .map(|i| (NodeId(i), pseudo_unit_vector(i, DIMS)))
            .collect();

        // Keep query copies before consuming `vectors`.
        let queries: Vec<(NodeId, Arc<[f32]>)> =
            vectors.iter().map(|(id, v)| (*id, Arc::clone(v))).collect();

        let params = HnswParams::new(16);
        let index = HnswIndex::new(params, DIMS as u16);
        index.rebuild(vectors);

        assert_eq!(index.len(), N as usize);
        assert!(index.tombstones.lock().is_empty());

        let mut hits = 0usize;
        for (node_id, query_vec) in &queries {
            let results = index.search(query_vec, 1, None, None);
            if !results.is_empty() && results[0].0 == *node_id {
                hits += 1;
            }
        }

        // Expect at least 95% self-search recall.
        let recall = hits as f64 / N as f64;
        assert!(
            recall >= 0.95,
            "self-search recall {:.1}% is below 95% threshold",
            recall * 100.0
        );
    }

    // ------------------------------------------------------------------
    // from_graph
    // ------------------------------------------------------------------

    #[test]
    fn from_graph_wraps_existing_graph() {
        // Build a graph independently and wrap it.
        let vectors: Vec<(NodeId, Arc<[f32]>)> = (0..5u64)
            .map(|i| (NodeId(i), axis_vector(i as usize, 4)))
            .collect();

        let params = HnswParams::new(4);
        let graph = build::build(vectors, &params);
        let graph_len = graph.len();

        let index = HnswIndex::from_graph(graph, params);
        assert_eq!(index.len(), graph_len);
        assert!(index.tombstones.lock().is_empty());
        assert!(!index.has_pending_mutations());
    }

    // ------------------------------------------------------------------
    // tombstones
    // ------------------------------------------------------------------

    #[test]
    fn tombstones_returns_clone() {
        let index = HnswIndex::new(HnswParams::default(), 2);
        index.remove(NodeId(5));
        index.remove(NodeId(10));

        let bm = index.tombstones();
        assert!(bm.contains(5));
        assert!(bm.contains(10));
        assert_eq!(bm.len(), 2);
    }

    // ------------------------------------------------------------------
    // New tests: insert, snapshot, generation tracking
    // ------------------------------------------------------------------

    #[test]
    fn insert_single_vector() {
        let index = HnswIndex::new(HnswParams::default(), 4);
        index.insert(NodeId(1), axis_vector(0, 4));

        // Should find it via write_graph merge, without snapshot.
        let results = index.search(&[1.0, 0.0, 0.0, 0.0], 1, None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, NodeId(1));
        assert!(index.has_pending_mutations());
    }

    #[test]
    fn insert_multiple_and_search() {
        let index = HnswIndex::new(HnswParams::new(8), 4);

        for i in 1..=10u64 {
            index.insert(NodeId(i), pseudo_unit_vector(i, 4));
        }

        // Search should return results from the write_graph.
        let results = index.search(&pseudo_unit_vector(1, 4), 5, None, None);
        assert!(!results.is_empty());
        // The query vector for seed=1 should find NodeId(1) as the best match.
        assert_eq!(results[0].0, NodeId(1));
    }

    #[test]
    fn insert_then_snapshot() {
        let index = HnswIndex::new(HnswParams::default(), 4);

        index.insert(NodeId(1), axis_vector(0, 4));
        index.insert(NodeId(2), axis_vector(1, 4));
        assert!(index.has_pending_mutations());

        index.snapshot();

        assert!(!index.has_pending_mutations());
        // Read snapshot should have the data.
        assert_eq!(index.len(), 2);
        // Search should work via the snapshot (no write_graph merge needed).
        let results = index.search(&[1.0, 0.0, 0.0, 0.0], 1, None, None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, NodeId(1));
    }

    #[test]
    fn search_merges_snapshot_and_write() {
        let index = HnswIndex::new(HnswParams::new(8), 4);

        // Insert some vectors and snapshot.
        index.insert(NodeId(1), axis_vector(0, 4)); // [1,0,0,0]
        index.insert(NodeId(2), axis_vector(1, 4)); // [0,1,0,0]
        index.snapshot();

        // Insert more vectors without snapshot.
        index.insert(NodeId(3), axis_vector(2, 4)); // [0,0,1,0]
        index.insert(NodeId(4), axis_vector(3, 4)); // [0,0,0,1]

        assert!(index.has_pending_mutations());

        // Search should find vectors from both snapshot and write_graph.
        let results = index.search(&[0.5, 0.5, 0.5, 0.5], 4, None, None);
        let ids: Vec<NodeId> = results.iter().map(|(id, _)| *id).collect();

        assert_eq!(results.len(), 4, "should find all 4 vectors");
        assert!(ids.contains(&NodeId(1)));
        assert!(ids.contains(&NodeId(2)));
        assert!(ids.contains(&NodeId(3)));
        assert!(ids.contains(&NodeId(4)));
    }

    #[test]
    fn remove_excludes_from_search() {
        let index = HnswIndex::new(HnswParams::default(), 3);

        index.insert(NodeId(1), axis_vector(0, 3));
        index.insert(NodeId(2), axis_vector(1, 3));
        index.insert(NodeId(3), axis_vector(2, 3));

        // Remove NodeId(1).
        index.remove(NodeId(1));

        let results = index.search(&[1.0, 0.0, 0.0], 5, None, None);
        let ids: Vec<NodeId> = results.iter().map(|(id, _)| *id).collect();

        assert!(
            !ids.contains(&NodeId(1)),
            "removed node must not appear in results"
        );
        assert!(ids.contains(&NodeId(2)));
        assert!(ids.contains(&NodeId(3)));
    }

    #[test]
    fn tombstone_ratio_calculation() {
        let index = HnswIndex::new(HnswParams::default(), 4);

        // Empty graph: ratio is 0.
        assert_eq!(index.tombstone_ratio(), 0.0);

        // Insert 10 vectors.
        for i in 1..=10u64 {
            index.insert(NodeId(i), pseudo_unit_vector(i, 4));
        }

        // Remove 2: ratio should be ~0.2.
        index.remove(NodeId(1));
        index.remove(NodeId(2));

        let ratio = index.tombstone_ratio();
        assert!((ratio - 0.2).abs() < 1e-6, "expected ~0.2, got {ratio}");
    }

    #[test]
    fn generation_tracking() {
        let index = HnswIndex::new(HnswParams::default(), 4);

        assert_eq!(index.pending_count(), 0);
        assert!(!index.has_pending_mutations());

        index.insert(NodeId(1), axis_vector(0, 4));
        assert_eq!(index.pending_count(), 1);
        assert!(index.has_pending_mutations());

        index.insert(NodeId(2), axis_vector(1, 4));
        assert_eq!(index.pending_count(), 2);

        // Remove also bumps generation.
        index.remove(NodeId(1));
        assert_eq!(index.pending_count(), 3);
    }

    #[test]
    fn snapshot_resets_generation_gap() {
        let index = HnswIndex::new(HnswParams::default(), 4);

        index.insert(NodeId(1), axis_vector(0, 4));
        index.insert(NodeId(2), axis_vector(1, 4));
        assert_eq!(index.pending_count(), 2);

        index.snapshot();
        assert_eq!(index.pending_count(), 0);
        assert!(!index.has_pending_mutations());

        // New mutations should create a new gap.
        index.insert(NodeId(3), axis_vector(2, 4));
        assert_eq!(index.pending_count(), 1);
        assert!(index.has_pending_mutations());
    }

    #[test]
    fn has_pending_mutations_tracking() {
        let index = HnswIndex::new(HnswParams::default(), 4);

        assert!(!index.has_pending_mutations());

        // Insert triggers pending.
        index.insert(NodeId(1), axis_vector(0, 4));
        assert!(index.has_pending_mutations());

        // Snapshot clears pending.
        index.snapshot();
        assert!(!index.has_pending_mutations());

        // Remove triggers pending.
        index.remove(NodeId(1));
        assert!(index.has_pending_mutations());

        // Rebuild clears pending.
        index.rebuild(vec![(NodeId(2), axis_vector(1, 4))]);
        assert!(!index.has_pending_mutations());
    }

    #[test]
    fn remove_reconnects_mutual_neighbors() {
        // Build a small graph: nodes 1-5 in a 4D space.
        let vectors: Vec<(NodeId, Arc<[f32]>)> = (1..=5)
            .map(|i| (NodeId(i), axis_vector(((i - 1) % 4) as usize, 4)))
            .collect();
        let index = HnswIndex::new(HnswParams::new(4), 4);
        for (id, vec) in &vectors {
            index.insert(*id, Arc::clone(vec));
        }
        index.snapshot();

        // Before deletion: node 3 should have neighbors.
        let wg = index.write_graph.read();
        let idx3 = *wg.node_id_to_idx.get(&3).unwrap();
        let neighbors_of_3: Vec<u32> = wg.nodes[idx3 as usize].neighbors[0].clone();
        assert!(!neighbors_of_3.is_empty(), "node 3 should have neighbors");
        drop(wg);

        // Delete node 3. MN-RU should reconnect its neighbors.
        index.remove(NodeId(3));

        // After deletion: node 3's former neighbors should no longer reference
        // node 3 in their adjacency lists (in the write graph).
        let wg = index.write_graph.read();
        let idx3_after = *wg.node_id_to_idx.get(&3).unwrap();
        for &neighbor_idx in &neighbors_of_3 {
            let nb = &wg.nodes[neighbor_idx as usize];
            assert!(
                !nb.neighbors[0].contains(&idx3_after),
                "neighbor {neighbor_idx} should not reference deleted node {idx3_after}"
            );
        }
        drop(wg);
    }

    // ------------------------------------------------------------------
    // Quantized storage integration
    // ------------------------------------------------------------------

    fn quantized_params() -> HnswParams {
        HnswParams::default().with_quantization(quantize::QuantizationConfig {
            bits: quantize::QuantBits::Four,
            seed: 42,
            rescore: false,
        })
    }

    #[test]
    fn rebuild_with_quantization() {
        let dims = 64;
        let n = 50;
        let vectors: Vec<_> = (0..n)
            .map(|i| (NodeId(i), pseudo_unit_vector(i, dims)))
            .collect();

        let index = HnswIndex::new(quantized_params(), dims as u16);
        index.rebuild(vectors);
        index.snapshot();

        let snap = index.load_graph();
        assert!(snap.quantized().is_some(), "quantized storage must exist after rebuild");
        let qs = snap.quantized().unwrap();
        assert_eq!(qs.len(), n as usize);
    }

    #[test]
    fn insert_with_quantization_lazy_init() {
        let dims = 32;
        let index = HnswIndex::new(quantized_params(), dims as u16);

        // First insert triggers lazy initialization.
        index.insert(NodeId(1), pseudo_unit_vector(1, dims));
        {
            let wg = index.write_graph.read();
            assert!(wg.quantized.is_some(), "lazy init on first insert");
            assert_eq!(wg.quantized.as_ref().unwrap().len(), 1);
        }

        // Subsequent inserts append incrementally.
        index.insert(NodeId(2), pseudo_unit_vector(2, dims));
        index.insert(NodeId(3), pseudo_unit_vector(3, dims));
        {
            let wg = index.write_graph.read();
            assert_eq!(wg.quantized.as_ref().unwrap().len(), 3);
        }
    }

    #[test]
    fn snapshot_preserves_quantized_storage() {
        let dims = 48;
        let n = 20;
        let vectors: Vec<_> = (0..n)
            .map(|i| (NodeId(i), pseudo_unit_vector(i, dims)))
            .collect();

        let index = HnswIndex::new(quantized_params(), dims as u16);
        index.rebuild(vectors);
        // Tombstone one node, then snapshot.
        index.remove(NodeId(5));
        index.snapshot();

        let snap = index.load_graph();
        let qs = snap.quantized().expect("quantized must survive snapshot");
        assert_eq!(qs.len(), (n - 1) as usize, "tombstoned node should be remapped out");
    }

    #[test]
    fn quantized_search_returns_results() {
        let dims = 64;
        let n = 100;
        let vectors: Vec<_> = (0..n)
            .map(|i| (NodeId(i), pseudo_unit_vector(i, dims)))
            .collect();

        let index = HnswIndex::new(quantized_params(), dims as u16);
        index.rebuild(vectors);
        index.snapshot();

        // Search still works (uses f32 distance for now; Phase 3 adds asymmetric).
        let query = pseudo_unit_vector(0, dims);
        let results = index.search(&query, 10, None, None);
        assert!(!results.is_empty(), "quantized index must return search results");
        assert_eq!(results[0].0, NodeId(0), "closest to seed 0 should be NodeId(0)");
    }
}
