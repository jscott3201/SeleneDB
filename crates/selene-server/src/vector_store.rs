//! Contiguous vector store — flat buffer for cache-friendly brute-force scan.
//!
//! Replaces scattered `Arc<[f32]>` heap allocations with a packed `Vec<f32>`.
//! Thread-safe via `Arc<RwLock<VectorStore>>`. Read lock held for scan duration.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use parking_lot::RwLock;
use selene_core::{IStr, NodeId, Value};
use selene_graph::SeleneGraph;

/// Contiguous vector storage for brute-force search.
pub struct VectorStore {
    /// Flat contiguous buffer: all vectors packed end-to-end.
    data: Vec<f32>,
    /// Int8 quantized buffer: parallel to `data`, same layout.
    /// Populated on every upsert for L2-normalized vectors (fixed [-1,1] range).
    data_u8: Vec<u8>,
    /// Maps (node_id, property_key) to slot index in `data` (offset = slot * dims).
    index: FxHashMap<(u64, IStr), usize>,
    /// Reverse index: node_id to property keys (for O(1) remove_node).
    node_keys: FxHashMap<u64, Vec<IStr>>,
    /// Per-slot metadata: whether the vector is unit-length (for dot product shortcut).
    normalized: Vec<bool>,
    /// Free slot indices from removed vectors (reused before appending).
    free_slots: Vec<usize>,
    /// Vector dimension (set by the first inserted vector). 0 = unset.
    dims: usize,
}

impl VectorStore {
    /// Create an empty store. Dimension is set by the first inserted vector.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            data_u8: Vec::new(),
            index: FxHashMap::default(),
            node_keys: FxHashMap::default(),
            normalized: Vec::new(),
            free_slots: Vec::new(),
            dims: 0,
        }
    }

    /// Insert or update a vector for (node_id, property_key).
    pub fn upsert(&mut self, node_id: NodeId, key: IStr, vector: &[f32]) {
        if vector.is_empty() {
            return;
        }
        // Set dimension on first insert
        if self.dims == 0 {
            self.dims = vector.len();
        }
        // Skip mismatched dimensions
        if vector.len() != self.dims {
            tracing::warn!(
                node_id = node_id.0,
                expected = self.dims,
                got = vector.len(),
                "vector dimension mismatch — skipping"
            );
            return;
        }

        let is_norm = is_approximately_unit(vector);

        if let Some(&slot) = self.index.get(&(node_id.0, key)) {
            // Update in-place
            let offset = slot * self.dims;
            self.data[offset..offset + self.dims].copy_from_slice(vector);
            self.normalized[slot] = is_norm;
            // Update int8 quantization
            quantize_vector(
                &self.data[offset..offset + self.dims],
                &mut self.data_u8[offset..offset + self.dims],
            );
        } else {
            // Reuse a free slot if available, otherwise append
            let slot = if let Some(free) = self.free_slots.pop() {
                let offset = free * self.dims;
                self.data[offset..offset + self.dims].copy_from_slice(vector);
                self.normalized[free] = is_norm;
                free
            } else {
                let slot = self.normalized.len();
                self.data.extend_from_slice(vector);
                self.data_u8.resize(self.data.len(), 128); // grow u8 buffer to match
                self.normalized.push(is_norm);
                slot
            };
            // Quantize the new/reused slot
            let offset = slot * self.dims;
            quantize_vector(
                &self.data[offset..offset + self.dims],
                &mut self.data_u8[offset..offset + self.dims],
            );
            self.index.insert((node_id.0, key), slot);
            self.node_keys.entry(node_id.0).or_default().push(key);
        }
    }

    /// Remove a vector for (node_id, property_key).
    /// Adds the slot to the free list for reuse by future upserts.
    pub fn remove(&mut self, node_id: NodeId, key: IStr) {
        if let Some(slot) = self.index.remove(&(node_id.0, key)) {
            let offset = slot * self.dims;
            for v in &mut self.data[offset..offset + self.dims] {
                *v = 0.0;
            }
            for v in &mut self.data_u8[offset..offset + self.dims] {
                *v = 128; // zero point
            }
            self.normalized[slot] = false;
            self.free_slots.push(slot);
            // Remove from reverse index
            if let Some(keys) = self.node_keys.get_mut(&node_id.0) {
                keys.retain(|k| *k != key);
                if keys.is_empty() {
                    self.node_keys.remove(&node_id.0);
                }
            }
        }
    }

    /// Remove all vectors for a node (all property keys). O(keys per node).
    pub fn remove_node(&mut self, node_id: NodeId) {
        if let Some(keys) = self.node_keys.remove(&node_id.0) {
            for key in keys {
                if let Some(slot) = self.index.remove(&(node_id.0, key)) {
                    let offset = slot * self.dims;
                    for v in &mut self.data[offset..offset + self.dims] {
                        *v = 0.0;
                    }
                    for v in &mut self.data_u8[offset..offset + self.dims] {
                        *v = 128;
                    }
                    self.normalized[slot] = false;
                    self.free_slots.push(slot);
                }
            }
        }
    }

    /// Number of stored vectors.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Populate from an existing graph (used at bootstrap).
    pub fn rebuild_from_graph(&mut self, graph: &SeleneGraph) {
        self.data.clear();
        self.data_u8.clear();
        self.index.clear();
        self.node_keys.clear();
        self.normalized.clear();
        self.free_slots.clear();
        self.dims = 0;

        for node_id in graph.all_node_ids() {
            if let Some(node) = graph.get_node(node_id) {
                for (key, value) in node.properties.iter() {
                    if let Value::Vector(v) = value {
                        self.upsert(node_id, *key, v);
                    }
                }
            }
        }
    }
}

// ── Int8 scalar quantization ──────────────────────────────────────

/// Quantize f32 in [-1.0, 1.0] to u8 in [0, 255].
/// Forward: u8 = round((f32 + 1.0) * 127.5)
#[inline]
fn quantize_f32_to_u8(v: f32) -> u8 {
    let clamped = v.clamp(-1.0, 1.0);
    ((clamped + 1.0) * 127.5).round() as u8
}

/// Quantize a full vector from f32 to u8.
fn quantize_vector(src: &[f32], dst: &mut [u8]) {
    debug_assert_eq!(src.len(), dst.len());
    for (s, d) in src.iter().zip(dst.iter_mut()) {
        *d = quantize_f32_to_u8(*s);
    }
}

/// Int8 dot product using centered u8 values.
///
/// Uses chunks_exact(16) with 4×4 accumulator groups for LLVM auto-vectorization
/// to ARM SDOT on targets with FEAT_DotProd (M1, RPi 5 Cortex-A76).
/// Scales result back to approximate f32 dot product.
fn int8_dot_product(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = [0i32; 4];
    let chunks_a = a.chunks_exact(16);
    let chunks_b = b.chunks_exact(16);
    let rem_a = chunks_a.remainder();
    let rem_b = chunks_b.remainder();
    for (a16, b16) in chunks_a.zip(chunks_b) {
        for (g, sum_g) in sum.iter_mut().enumerate() {
            let base = g * 4;
            for i in 0..4 {
                let ai = i32::from(a16[base + i]) - 128;
                let bi = i32::from(b16[base + i]) - 128;
                *sum_g += ai * bi;
            }
        }
    }
    let mut total: i32 = sum[0] + sum[1] + sum[2] + sum[3];
    for (a, b) in rem_a.iter().zip(rem_b) {
        total += (i32::from(*a) - 128) * (i32::from(*b) - 128);
    }
    total as f32 / (127.5 * 127.5)
}

/// Cosine similarity (f32) — local copy for non-normalized vector fallback.
fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    let mut mag_a = 0.0f32;
    let mut mag_b = 0.0f32;
    for (x, y) in a.iter().zip(b) {
        dot += x * y;
        mag_a += x * x;
        mag_b += y * y;
    }
    let denom = mag_a.sqrt() * mag_b.sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}

/// Check if a vector is approximately unit-length (L2 norm ≈ 1.0).
fn is_approximately_unit(v: &[f32]) -> bool {
    let mag_sq: f32 = v.iter().map(|x| x * x).sum();
    (1.0 - mag_sq).abs() < 1e-5
}

// ── VectorProvider implementation ──────────────────────────────────

/// Thread-safe wrapper implementing VectorProvider.
pub struct SharedVectorStore {
    inner: Arc<RwLock<VectorStore>>,
}

impl SharedVectorStore {
    pub fn new(store: Arc<RwLock<VectorStore>>) -> Self {
        Self { inner: store }
    }
}

impl selene_gql::runtime::procedures::vector_provider::VectorProvider for SharedVectorStore {
    fn scan_vectors(
        &self,
        node_ids: &mut dyn Iterator<Item = NodeId>,
        key: &IStr,
        query_dim: usize,
        f: &mut dyn FnMut(NodeId, &[f32], bool),
    ) {
        let store = self.inner.read();
        if store.dims == 0 || store.dims != query_dim {
            return;
        }
        let dims = store.dims;
        for node_id in node_ids {
            if let Some(&slot) = store.index.get(&(node_id.0, *key)) {
                let offset = slot * dims;
                let vec_data = &store.data[offset..offset + dims];
                let is_norm = store.normalized[slot];
                f(node_id, vec_data, is_norm);
            }
        }
    }

    fn scan_with_scores(
        &self,
        node_ids: &mut dyn Iterator<Item = NodeId>,
        key: &IStr,
        query_vec: &[f32],
        f: &mut dyn FnMut(NodeId, f32),
    ) -> bool {
        let store = self.inner.read();
        if store.dims == 0 || store.dims != query_vec.len() || store.data_u8.is_empty() {
            return false;
        }
        let dims = store.dims;

        // Quantize the query vector once
        let mut query_u8 = vec![0u8; dims];
        quantize_vector(query_vec, &mut query_u8);

        let query_is_unit = is_approximately_unit(query_vec);

        for node_id in node_ids {
            if let Some(&slot) = store.index.get(&(node_id.0, *key)) {
                let offset = slot * dims;
                let is_norm = store.normalized[slot];

                let score = if query_is_unit && is_norm {
                    // Both normalized: int8 dot product approximates cosine similarity
                    int8_dot_product(&store.data_u8[offset..offset + dims], &query_u8)
                } else {
                    // Non-normalized: fall back to f32 cosine
                    let vec_data = &store.data[offset..offset + dims];
                    cosine_similarity_f32(vec_data, query_vec)
                };
                f(node_id, score);
            }
        }
        true
    }
}

/// Initialize the VectorProvider OnceLock in selene-gql.
pub fn init_vector_provider(store: Arc<RwLock<VectorStore>>) {
    let provider = Arc::new(SharedVectorStore::new(store));
    selene_gql::runtime::procedures::vector_provider::set_vector_provider(provider);
}

// ── Service wrapper ──────────────────────────────────────────────────

/// VectorStore as a registered service in the ServiceRegistry.
pub struct VectorStoreService {
    pub store: Arc<RwLock<VectorStore>>,
}

impl VectorStoreService {
    pub fn new(store: Arc<RwLock<VectorStore>>) -> Self {
        Self { store }
    }
}

impl crate::service_registry::Service for VectorStoreService {
    fn name(&self) -> &'static str {
        "vector"
    }

    fn health(&self) -> crate::service_registry::ServiceHealth {
        crate::service_registry::ServiceHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap};

    #[test]
    fn upsert_and_len() {
        let mut store = VectorStore::new();
        store.upsert(NodeId(1), IStr::new("emb"), &[1.0, 0.0, 0.0]);
        store.upsert(NodeId(2), IStr::new("emb"), &[0.0, 1.0, 0.0]);
        assert_eq!(store.len(), 2);
        assert_eq!(store.dims, 3);
    }

    #[test]
    fn upsert_overwrites() {
        let mut store = VectorStore::new();
        store.upsert(NodeId(1), IStr::new("emb"), &[1.0, 0.0]);
        store.upsert(NodeId(1), IStr::new("emb"), &[0.0, 1.0]);
        assert_eq!(store.len(), 1);
        // Check data was overwritten
        let slot = store.index[&(1, IStr::new("emb"))];
        assert_eq!(store.data[slot * 2], 0.0);
        assert_eq!(store.data[slot * 2 + 1], 1.0);
    }

    #[test]
    fn remove_adds_to_free_list() {
        let mut store = VectorStore::new();
        store.upsert(NodeId(1), IStr::new("emb"), &[1.0, 0.0]);
        store.remove(NodeId(1), IStr::new("emb"));
        assert_eq!(store.len(), 0);
        assert_eq!(store.free_slots.len(), 1);
        // Next upsert reuses the free slot
        store.upsert(NodeId(2), IStr::new("emb"), &[0.0, 1.0]);
        assert_eq!(store.len(), 1);
        assert_eq!(store.free_slots.len(), 0);
        assert_eq!(store.data.len(), 2); // No growth
    }

    #[test]
    fn remove_node_all_keys() {
        let mut store = VectorStore::new();
        store.upsert(NodeId(1), IStr::new("emb1"), &[1.0, 0.0]);
        store.upsert(NodeId(1), IStr::new("emb2"), &[0.0, 1.0]);
        store.upsert(NodeId(2), IStr::new("emb1"), &[0.5, 0.5]);
        store.remove_node(NodeId(1));
        assert_eq!(store.len(), 1); // Only node 2 remains
        assert_eq!(store.free_slots.len(), 2);
    }

    #[test]
    fn rebuild_from_graph() {
        let mut g = SeleneGraph::new();
        {
            let mut props = PropertyMap::new();
            props.insert(
                IStr::new("emb"),
                Value::Vector(Arc::from(vec![1.0f32, 0.0])),
            );
            let mut m = g.mutate();
            m.create_node(LabelSet::from_strs(&["sensor"]), props)
                .unwrap();
            m.commit(0).unwrap();
        }
        {
            let mut props = PropertyMap::new();
            props.insert(
                IStr::new("emb"),
                Value::Vector(Arc::from(vec![0.0f32, 1.0])),
            );
            let mut m = g.mutate();
            m.create_node(LabelSet::from_strs(&["sensor"]), props)
                .unwrap();
            m.commit(0).unwrap();
        }

        let mut store = VectorStore::new();
        store.rebuild_from_graph(&g);
        assert_eq!(store.len(), 2);
        assert_eq!(store.dims, 2);
    }

    #[test]
    fn skips_mismatched_dimensions() {
        let mut store = VectorStore::new();
        store.upsert(NodeId(1), IStr::new("emb"), &[1.0, 0.0]); // dims = 2
        store.upsert(NodeId(2), IStr::new("emb"), &[1.0, 0.0, 0.0]); // dims = 3, skipped
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn normalized_flag_set_correctly() {
        let mut store = VectorStore::new();
        // Unit vector
        store.upsert(NodeId(1), IStr::new("emb"), &[1.0, 0.0]);
        assert!(store.normalized[0]);
        // Non-unit vector
        store.upsert(NodeId(2), IStr::new("emb"), &[2.0, 0.0]);
        assert!(!store.normalized[1]);
    }
}
