//! HNSW (Hierarchical Navigable Small World) approximate nearest neighbor index.
//!
//! Provides sub-millisecond vector similarity search for 10K-100K vectors
//! with >95% recall, replacing brute-force O(N) scans.
//!
//! # Public API
//!
//! `HnswIndex` is the primary entry point. It wraps an immutable `HnswGraph`
//! behind an `ArcSwap` for lock-free reads, and buffers new inserts in a
//! staging `Vec` until a rebuild is triggered.
//!
//! ```text
//! HnswIndex
//!   graph:      ArcSwap<HnswGraph>  -- read-only, lock-free (~1 ns load)
//!   staging:    Mutex<Vec<...>>     -- pending inserts, brute-forced at query time
//!   tombstones: Mutex<RoaringBitmap> -- deleted node IDs, filtered from results
//!   params:     HnswParams          -- M, ef_construction, staging_capacity, ...
//! ```
//!
//! Rebuilds atomically swap in a new `Arc<HnswGraph>` without blocking readers.

pub mod build;
pub mod distance;
pub mod graph;
pub mod params;
pub mod search;

pub use build::build;
pub use graph::HnswGraph;
pub use params::HnswParams;
pub use search::search;

use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use roaring::RoaringBitmap;

use selene_core::NodeId;

use self::distance::cosine_similarity;

// ---------------------------------------------------------------------------
// StagingEntry
// ---------------------------------------------------------------------------

/// A vector pending insertion into the HNSW graph.
///
/// Staging entries are brute-force scanned during search and incorporated into
/// the main graph on the next rebuild.
#[derive(Debug, Clone)]
pub struct StagingEntry {
    /// Property-graph node ID.
    pub node_id: NodeId,
    /// Embedding vector.
    pub vector: Arc<[f32]>,
}

// ---------------------------------------------------------------------------
// HnswIndex
// ---------------------------------------------------------------------------

/// Lock-free HNSW index with staging buffer and tombstone tracking.
///
/// Reads (`search`) load the immutable `HnswGraph` via an `ArcSwap` guard
/// (~1 ns, no refcount increment) and brute-force scan the small staging
/// buffer in the same call. Tombstoned node IDs are filtered from the merged
/// result list before returning.
///
/// Writes are split into two categories:
/// - **Staging inserts** (`stage_insert`): O(1) push to the staging `Vec`.
/// - **Rebuilds** (`rebuild`): build a new `HnswGraph` from all vectors, swap
///   it atomically, clear staging and tombstones.
///
/// The caller drives rebuild timing via `needs_rebuild()`.
pub struct HnswIndex {
    /// Immutable HNSW graph, atomically swappable.
    graph: ArcSwap<HnswGraph>,
    /// Pending inserts awaiting incorporation into the main graph.
    staging: Mutex<Vec<StagingEntry>>,
    /// Deleted node IDs. Results containing these are filtered on query.
    tombstones: Mutex<RoaringBitmap>,
    /// Index configuration.
    params: HnswParams,
}

impl HnswIndex {
    /// Create an empty index for vectors of the given `dimensions`.
    pub fn new(params: HnswParams, dimensions: u16) -> Self {
        Self {
            graph: ArcSwap::from(Arc::new(HnswGraph::empty(dimensions))),
            staging: Mutex::new(Vec::new()),
            tombstones: Mutex::new(RoaringBitmap::new()),
            params,
        }
    }

    /// Create an index from an already-built `HnswGraph`.
    ///
    /// Staging and tombstones start empty. Use this after deserializing a
    /// persisted graph from a snapshot.
    pub fn from_graph(graph: HnswGraph, params: HnswParams) -> Self {
        Self {
            graph: ArcSwap::from(Arc::new(graph)),
            staging: Mutex::new(Vec::new()),
            tombstones: Mutex::new(RoaringBitmap::new()),
            params,
        }
    }

    /// Push a vector into the staging buffer.
    ///
    /// O(1). The vector is not immediately visible in HNSW graph traversal but
    /// is included in brute-force scanning during `search` calls.
    pub fn stage_insert(&self, node_id: NodeId, vector: Arc<[f32]>) {
        self.staging.lock().push(StagingEntry { node_id, vector });
    }

    /// Mark a node as deleted.
    ///
    /// Tombstoned nodes are excluded from all future search results. O(1)
    /// amortized (RoaringBitmap insert).
    ///
    /// Node IDs are stored using the lower 32 bits of the raw `u64` value,
    /// consistent with the filter bitmap convention used elsewhere in the
    /// property graph.
    pub fn mark_tombstoned(&self, node_id: NodeId) {
        self.tombstones.lock().insert(node_id.0 as u32);
    }

    /// Returns `true` when the staging buffer has reached `staging_capacity`.
    ///
    /// At this point the caller should call `rebuild` to incorporate staged
    /// vectors into the main graph.
    pub fn needs_rebuild(&self) -> bool {
        self.staging.lock().len() >= self.params.staging_capacity
    }

    /// Number of vectors in the main (built) HNSW graph.
    ///
    /// Does not include staging entries.
    pub fn len(&self) -> usize {
        self.graph.load().len()
    }

    /// Returns `true` when the main graph is empty and staging is empty.
    pub fn is_empty(&self) -> bool {
        self.graph.load().is_empty() && self.staging.lock().is_empty()
    }

    /// Search for the `k` most similar vectors to `query`.
    ///
    /// Combines two phases:
    /// 1. HNSW graph search (if graph is non-empty) via greedy descent + beam
    ///    search. Requests `k + tombstone_count` candidates to account for
    ///    post-filter removal.
    /// 2. Brute-force cosine scan of the staging buffer.
    ///
    /// Tombstoned node IDs are removed from the combined candidate list. The
    /// final result is sorted by descending similarity and truncated to `k`.
    ///
    /// `ef` is the beam width for HNSW search. When `None`, `params.ef_search`
    /// is used. Pass a larger value for higher recall at the cost of latency.
    ///
    /// The optional `filter` restricts HNSW results to nodes whose raw
    /// `NodeId` value (lower 32 bits) appears in the bitmap. Staging entries
    /// are not affected by the filter.
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

        // --- Phase 1: HNSW graph search ---
        // Load the immutable graph via ArcSwap guard (~1 ns, no refcount bump).
        let guard = self.graph.load();
        let tombstone_count = self.tombstones.lock().len() as usize;
        let k_expanded = k.saturating_add(tombstone_count);

        let mut results: Vec<(NodeId, f32)> = if guard.is_empty() {
            Vec::new()
        } else {
            search::search(&guard, query, k_expanded, ef, filter)
        };

        // --- Phase 2: Brute-force staging scan ---
        {
            let staging = self.staging.lock();
            for entry in staging.iter() {
                let sim = cosine_similarity(&entry.vector, query);
                results.push((entry.node_id, sim));
            }
        }

        // --- Phase 3: Filter tombstones ---
        {
            let tombstones = self.tombstones.lock();
            if !tombstones.is_empty() {
                results.retain(|(node_id, _)| !tombstones.contains(node_id.0 as u32));
            }
        }

        // --- Phase 4: Sort by descending similarity and return top-k ---
        results.sort_unstable_by(|(_, a), (_, b)| {
            b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(k);
        results
    }

    /// Build a new `HnswGraph` from `vectors` and atomically publish it.
    ///
    /// After the swap, staging entries and tombstones are cleared. All
    /// subsequent reads see the new graph immediately (ArcSwap guarantee).
    ///
    /// `vectors` should include all property-graph vectors for this index,
    /// not just the delta. The caller is responsible for collecting the full
    /// set (existing graph nodes + staged inserts, minus tombstoned IDs).
    pub fn rebuild(&self, vectors: Vec<(NodeId, Arc<[f32]>)>) {
        let new_graph = build::build(vectors, &self.params);
        self.graph.store(Arc::new(new_graph));
        self.staging.lock().clear();
        self.tombstones.lock().clear();
    }

    /// Return an `Arc<HnswGraph>` for serialization or inspection.
    ///
    /// This increments the `Arc` refcount (vs. the ~1 ns guard path used by
    /// `search`). Prefer `search` for hot paths; use this only for snapshots.
    pub fn load_graph(&self) -> Arc<HnswGraph> {
        self.graph.load_full()
    }

    /// Return a reference to the index configuration.
    pub fn params(&self) -> &HnswParams {
        &self.params
    }

    /// Drain and return all staging entries.
    ///
    /// Used by the rebuild coordinator to collect staged vectors alongside the
    /// existing graph nodes before calling `rebuild`.
    pub fn take_staging(&self) -> Vec<StagingEntry> {
        std::mem::take(&mut self.staging.lock())
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
        assert!(!index.needs_rebuild());

        let results = index.search(&[0.0f32; 32], 10, None, None);
        assert!(results.is_empty(), "empty index must return no results");
    }

    // ------------------------------------------------------------------
    // stage_insert_and_search
    // ------------------------------------------------------------------

    #[test]
    fn stage_insert_and_search() {
        let index = HnswIndex::new(HnswParams::default(), 4);

        // Insert a single vector into staging.
        let v = axis_vector(0, 4); // [1, 0, 0, 0]
        index.stage_insert(NodeId(1), Arc::clone(&v));

        // The index has no built graph -- search must find the staged vector.
        let query = [1.0f32, 0.0, 0.0, 0.0];
        let results = index.search(&query, 5, None, None);

        assert_eq!(results.len(), 1, "should find the one staged vector");
        assert_eq!(results[0].0, NodeId(1));
        assert!(
            (results[0].1 - 1.0).abs() < 1e-6,
            "similarity should be ~1.0"
        );
    }

    // ------------------------------------------------------------------
    // needs_rebuild_at_capacity
    // ------------------------------------------------------------------

    #[test]
    fn needs_rebuild_at_capacity() {
        let params = HnswParams {
            staging_capacity: 4,
            ..HnswParams::new(4)
        };
        let index = HnswIndex::new(params, 2);

        assert!(!index.needs_rebuild());

        for i in 0..3u64 {
            index.stage_insert(NodeId(i), axis_vector(0, 2));
            assert!(
                !index.needs_rebuild(),
                "should not need rebuild after {i} inserts"
            );
        }

        // Fourth insert reaches capacity.
        index.stage_insert(NodeId(3), axis_vector(1, 2));
        assert!(
            index.needs_rebuild(),
            "should need rebuild once staging_capacity is reached"
        );
    }

    // ------------------------------------------------------------------
    // tombstone_excludes_from_search
    // ------------------------------------------------------------------

    #[test]
    fn tombstone_excludes_from_search() {
        let index = HnswIndex::new(HnswParams::default(), 3);

        // Stage two vectors.
        index.stage_insert(NodeId(1), axis_vector(0, 3)); // [1, 0, 0]
        index.stage_insert(NodeId(2), axis_vector(1, 3)); // [0, 1, 0]

        // Tombstone node 1 (the one closest to the query).
        index.mark_tombstoned(NodeId(1));

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

        // Stage some vectors and add a tombstone.
        index.stage_insert(NodeId(1), axis_vector(0, 3));
        index.stage_insert(NodeId(2), axis_vector(1, 3));
        index.mark_tombstoned(NodeId(99));

        // Rebuild from a full set of vectors.
        let vectors = vec![
            (NodeId(1), axis_vector(0, 3)),
            (NodeId(2), axis_vector(1, 3)),
        ];
        index.rebuild(vectors);

        // Staging and tombstones must be cleared.
        assert!(
            index.staging.lock().is_empty(),
            "staging must be cleared after rebuild"
        );
        assert!(
            index.tombstones.lock().is_empty(),
            "tombstones must be cleared after rebuild"
        );

        // Graph must now contain the 2 built nodes.
        assert_eq!(index.len(), 2);
        assert!(!index.needs_rebuild());

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
        assert!(index.staging.lock().is_empty());
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
        assert!(index.staging.lock().is_empty());
        assert!(index.tombstones.lock().is_empty());
    }

    // ------------------------------------------------------------------
    // take_staging / tombstones
    // ------------------------------------------------------------------

    #[test]
    fn take_staging_drains_buffer() {
        let index = HnswIndex::new(HnswParams::default(), 2);
        index.stage_insert(NodeId(10), axis_vector(0, 2));
        index.stage_insert(NodeId(20), axis_vector(1, 2));

        let drained = index.take_staging();
        assert_eq!(drained.len(), 2);
        assert!(
            index.staging.lock().is_empty(),
            "staging must be empty after take"
        );
    }

    #[test]
    fn tombstones_returns_clone() {
        let index = HnswIndex::new(HnswParams::default(), 2);
        index.mark_tombstoned(NodeId(5));
        index.mark_tombstoned(NodeId(10));

        let bm = index.tombstones();
        assert!(bm.contains(5));
        assert!(bm.contains(10));
        assert_eq!(bm.len(), 2);
    }
}
