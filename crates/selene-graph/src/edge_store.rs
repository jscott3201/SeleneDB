//! Column-oriented edge storage with copy-on-write chunks.
//!
//! `EdgeStore` splits `Edge` fields into parallel `ChunkedVec`s (Struct-of-Arrays).
//! Clone copies chunk pointers (O(N/256) Arc increments) -- near-instant snapshots.
//! `EdgeRef<'a>` borrows from these columns for zero-copy reads.

use selene_core::{Edge, EdgeId, IStr, NodeId, PropertyMap};

use crate::bitset::{BitIter, bit_clear, bit_set, bit_test};
use crate::chunked_vec::ChunkedVec;

/// Read-only borrow of an edge from the column store.
///
/// Field names match `Edge` so most call sites work unchanged.
#[derive(Debug, Clone)]
pub struct EdgeRef<'a> {
    pub id: EdgeId,
    pub source: NodeId,
    pub target: NodeId,
    pub label: IStr,
    pub properties: &'a PropertyMap,
    pub created_at: i64,
}

impl EdgeRef<'_> {
    /// Construct an owned `Edge` (for snapshot/serialization call sites).
    pub fn to_owned_edge(&self) -> Edge {
        Edge {
            id: self.id,
            source: self.source,
            target: self.target,
            label: self.label,
            properties: self.properties.clone(),
            created_at: self.created_at,
        }
    }
}

use roaring::RoaringBitmap;

/// Column-oriented edge storage with copy-on-write chunks.
///
/// Each field of `Edge` is stored in a separate `ChunkedVec`, indexed by `EdgeId.0`.
/// Slot 0 is always dead (IDs start at 1).
/// Alive mask uses u64 bitset (8x memory reduction vs `Vec<bool>`).
///
/// Clone copies chunk pointers (O(N/256) Arc increments). Writes trigger
/// CoW on only the modified chunk via `Arc::make_mut`.
#[derive(Clone)]
pub struct EdgeStore {
    sources: ChunkedVec<NodeId>,
    targets: ChunkedVec<NodeId>,
    labels: ChunkedVec<IStr>,
    properties: ChunkedVec<PropertyMap>,
    created_at: crate::timestamp_column::TimestampColumn,
    alive: Vec<u64>,
    /// Incrementally maintained bitmap of live edge IDs.
    alive_bm: RoaringBitmap,
    count: usize,
    slot_count: usize,
}

impl std::fmt::Debug for EdgeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeStore")
            .field("count", &self.count)
            .field("capacity", &self.slot_count)
            .finish()
    }
}

impl EdgeStore {
    /// Create an empty store with slot 0 reserved.
    ///
    /// Slot 0 is a sentinel (never used for real edges). The `IStr::new("")`
    /// placeholder is cheap -- empty string is interned once and reused globally.
    pub fn new() -> Self {
        let mut sources = ChunkedVec::new();
        sources.resize(1, NodeId(0));
        let mut targets = ChunkedVec::new();
        targets.resize(1, NodeId(0));
        let mut labels = ChunkedVec::new();
        labels.resize(1, IStr::new(""));
        let mut properties = ChunkedVec::new();
        properties.resize_with(1, PropertyMap::new);
        let mut created_at = crate::timestamp_column::TimestampColumn::new(
            crate::timestamp_column::DEFAULT_EPOCH_NANOS,
        );
        created_at.resize(1, 0);

        Self {
            sources,
            targets,
            labels,
            properties,
            created_at,
            alive: vec![0],
            alive_bm: RoaringBitmap::new(),
            count: 0,
            slot_count: 1,
        }
    }

    /// Return a RoaringBitmap of all live edge IDs.
    /// Maintained incrementally on insert/remove; clone cost is O(containers).
    pub fn alive_bitmap(&self) -> RoaringBitmap {
        self.alive_bm.clone()
    }

    /// Number of live edges.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Check if an edge exists.
    pub fn contains(&self, id: EdgeId) -> bool {
        let idx = id.0 as usize;
        idx < self.slot_count && bit_test(&self.alive, idx)
    }

    /// Get a read-only reference to an edge.
    pub fn get(&self, id: EdgeId) -> Option<EdgeRef<'_>> {
        let idx = id.0 as usize;
        if idx >= self.slot_count || !bit_test(&self.alive, idx) {
            return None;
        }
        Some(EdgeRef {
            id,
            source: self.sources[idx],
            target: self.targets[idx],
            label: self.labels[idx],
            properties: &self.properties[idx],
            created_at: self.created_at.get(idx as u32),
        })
    }

    /// Ensure capacity for the given ID. Grows with doubling strategy.
    fn ensure_capacity(&mut self, id: EdgeId) {
        let idx = id.0 as usize;
        if idx >= self.slot_count {
            let new_len = (self.slot_count * 2).max(idx + 1).max(64);
            self.sources.resize(new_len, NodeId(0));
            self.targets.resize(new_len, NodeId(0));
            self.labels.resize(new_len, IStr::new(""));
            self.properties.resize_with(new_len, PropertyMap::new);
            self.created_at.resize(new_len, 0);
            let new_words = new_len.div_ceil(64);
            self.alive.resize(new_words, 0);
            self.slot_count = new_len;
        }
    }

    /// Insert an edge, overwriting any existing edge at that ID.
    /// Returns source, target, and label for index updates.
    pub fn insert(&mut self, edge: Edge) -> (NodeId, NodeId, IStr) {
        let idx = edge.id.0 as usize;
        self.ensure_capacity(edge.id);

        if !bit_test(&self.alive, idx) {
            self.count += 1;
        }

        let source = edge.source;
        let target = edge.target;
        let label = edge.label;

        self.sources.set(idx, source);
        self.targets.set(idx, target);
        self.labels.set(idx, label);
        self.properties.set(idx, edge.properties);
        self.created_at.set(idx as u32, edge.created_at);
        bit_set(&mut self.alive, idx);
        self.alive_bm.insert(idx as u32);

        (source, target, label)
    }

    /// Remove an edge, returning the owned Edge if it existed.
    pub fn remove(&mut self, id: EdgeId) -> Option<Edge> {
        let idx = id.0 as usize;
        if idx >= self.slot_count || !bit_test(&self.alive, idx) {
            return None;
        }
        bit_clear(&mut self.alive, idx);
        self.alive_bm.remove(idx as u32);
        self.count -= 1;

        Some(Edge {
            id,
            source: self.sources[idx],
            target: self.targets[idx],
            label: self.labels[idx],
            properties: std::mem::replace(
                self.properties.get_mut(idx).unwrap(),
                PropertyMap::new(),
            ),
            created_at: self.created_at.get(idx as u32),
        })
    }

    /// Iterate over all live edge IDs.
    pub fn all_ids(&self) -> impl Iterator<Item = EdgeId> + '_ {
        self.alive
            .iter()
            .enumerate()
            .flat_map(|(word_idx, &word)| {
                let w = if word_idx == 0 { word & !1 } else { word };
                BitIter {
                    word: w,
                    base: (word_idx * 64) as u64,
                }
            })
            .map(EdgeId)
    }

    // ── Per-column mutable access (for mutation.rs) ──────────────────

    /// Get mutable access to an edge's properties. Triggers CoW on the chunk.
    pub fn properties_mut(&mut self, id: EdgeId) -> Option<&mut PropertyMap> {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            self.properties.get_mut(idx)
        } else {
            None
        }
    }

    /// Get source and target for an edge (for adjacency cleanup).
    pub fn endpoints(&self, id: EdgeId) -> Option<(NodeId, NodeId, IStr)> {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            Some((self.sources[idx], self.targets[idx], self.labels[idx]))
        } else {
            None
        }
    }

    // ── Column accessors (for scans) ─────────────────────────────────

    pub fn alive_words(&self) -> &[u64] {
        &self.alive
    }

    pub fn slot_count(&self) -> usize {
        self.slot_count
    }

    pub fn sources_column(&self) -> &ChunkedVec<NodeId> {
        &self.sources
    }

    pub fn targets_column(&self) -> &ChunkedVec<NodeId> {
        &self.targets
    }

    pub fn labels_column(&self) -> &ChunkedVec<IStr> {
        &self.labels
    }
}

impl<'a> From<&'a Edge> for EdgeRef<'a> {
    fn from(edge: &'a Edge) -> Self {
        EdgeRef {
            id: edge.id,
            source: edge.source,
            target: edge.target,
            label: edge.label,
            properties: &edge.properties,
            created_at: edge.created_at,
        }
    }
}

impl Default for EdgeStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_edge(id: u64, src: u64, tgt: u64, label: &str) -> Edge {
        Edge::new(
            EdgeId(id),
            NodeId(src),
            NodeId(tgt),
            IStr::new(label),
            PropertyMap::new(),
        )
    }

    #[test]
    fn empty_store() {
        let store = EdgeStore::new();
        assert_eq!(store.count(), 0);
        assert!(!store.contains(EdgeId(1)));
    }

    #[test]
    fn insert_and_get() {
        let mut store = EdgeStore::new();
        store.insert(test_edge(1, 10, 20, "contains"));

        assert_eq!(store.count(), 1);
        let edge = store.get(EdgeId(1)).unwrap();
        assert_eq!(edge.source, NodeId(10));
        assert_eq!(edge.target, NodeId(20));
        assert_eq!(edge.label.as_str(), "contains");
    }

    #[test]
    fn remove_returns_owned() {
        let mut store = EdgeStore::new();
        store.insert(test_edge(3, 1, 2, "feeds"));

        let removed = store.remove(EdgeId(3)).unwrap();
        assert_eq!(removed.source, NodeId(1));
        assert_eq!(store.count(), 0);
    }

    #[test]
    fn all_ids() {
        let mut store = EdgeStore::new();
        store.insert(test_edge(2, 1, 2, "a"));
        store.insert(test_edge(5, 3, 4, "b"));

        let mut ids: Vec<EdgeId> = store.all_ids().collect();
        ids.sort_by_key(|e| e.0);
        assert_eq!(ids, vec![EdgeId(2), EdgeId(5)]);
    }

    #[test]
    fn to_owned_edge() {
        let mut store = EdgeStore::new();
        store.insert(test_edge(1, 10, 20, "contains"));

        let edge_ref = store.get(EdgeId(1)).unwrap();
        let owned = edge_ref.to_owned_edge();
        assert_eq!(owned.source, NodeId(10));
        assert_eq!(owned.label.as_str(), "contains");
    }

    #[test]
    fn cow_clone_independence() {
        let mut store = EdgeStore::new();
        store.insert(test_edge(1, 10, 20, "contains"));

        let original = store.clone();

        // Mutate the clone — should not affect original
        store
            .properties_mut(EdgeId(1))
            .unwrap()
            .insert(IStr::new("key"), selene_core::Value::str("val"));

        let orig_edge = original.get(EdgeId(1)).unwrap();
        assert!(orig_edge.properties.is_empty());

        let clone_edge = store.get(EdgeId(1)).unwrap();
        assert!(!clone_edge.properties.is_empty());
    }
}
