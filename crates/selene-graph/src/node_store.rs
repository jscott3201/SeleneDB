//! Column-oriented node storage with copy-on-write chunks.
//!
//! `NodeStore` splits `Node` fields into parallel `ChunkedVec`s (Struct-of-Arrays).
//! Clone copies chunk pointers (O(N/256) Arc increments) -- near-instant snapshots.
//! `NodeRef<'a>` borrows from these columns for zero-copy reads.
//! The `Node` type in selene-core remains the construction/transfer type.

use std::sync::Arc;

use roaring::RoaringBitmap;
use selene_core::{LabelSet, Node, NodeId, PropertyMap, Value};

use crate::bitset::{BitIter, bit_clear, bit_set, bit_test};
use crate::chunked_vec::ChunkedVec;

/// Read-only borrow of a node from the column store.
///
/// Field names match `Node` so most call sites work unchanged.
#[derive(Debug, Clone)]
pub struct NodeRef<'a> {
    pub id: NodeId,
    pub labels: &'a LabelSet,
    pub properties: &'a PropertyMap,
    pub created_at: i64,
    pub updated_at: i64,
    pub version: u64,
    pub cached_json: &'a Option<Arc<str>>,
}

impl NodeRef<'_> {
    /// Check if this node has a given label.
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.contains_str(label)
    }

    /// Get a property value by key.
    pub fn property(&self, key: &str) -> Option<&Value> {
        self.properties.get_by_str(key)
    }

    /// Construct an owned `Node` (for snapshot/serialization call sites).
    pub fn to_owned_node(&self) -> Node {
        Node {
            id: self.id,
            labels: self.labels.clone(),
            properties: self.properties.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            version: self.version,
            cached_json: self.cached_json.clone(),
        }
    }
}

/// Column-oriented node storage with copy-on-write chunks.
///
/// Each field of `Node` is stored in a separate `ChunkedVec`, indexed by `NodeId.0`.
/// Slot 0 is always dead (IDs start at 1).
/// Alive mask uses u64 bitset (8x memory reduction vs `Vec<bool>`).
///
/// Clone copies chunk pointers (O(N/256) Arc increments). Writes trigger
/// CoW on only the modified chunk via `Arc::make_mut`.
#[derive(Clone)]
pub struct NodeStore {
    labels: ChunkedVec<LabelSet>,
    properties: ChunkedVec<PropertyMap>,
    created_at: crate::timestamp_column::TimestampColumn,
    updated_at: crate::timestamp_column::TimestampColumn,
    versions: ChunkedVec<u64>,
    cached_json: ChunkedVec<Option<Arc<str>>>,
    alive: Vec<u64>,
    /// Incrementally maintained bitmap of live node IDs.
    /// Updated on insert/remove to avoid O(N/64) rebuilds in alive_bitmap().
    alive_bm: RoaringBitmap,
    count: usize,
    /// Number of logical slots (column length). The alive bitvec may be
    /// longer (rounded up to u64 words).
    slot_count: usize,
}

impl std::fmt::Debug for NodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeStore")
            .field("count", &self.count)
            .field("capacity", &self.slot_count)
            .finish()
    }
}

impl NodeStore {
    /// Create an empty store with slot 0 reserved.
    pub fn new() -> Self {
        let mut labels = ChunkedVec::new();
        labels.resize(1, LabelSet::new());
        let mut properties = ChunkedVec::new();
        properties.resize_with(1, PropertyMap::new);
        let epoch = crate::timestamp_column::DEFAULT_EPOCH_NANOS;
        let mut created_at = crate::timestamp_column::TimestampColumn::new(epoch);
        created_at.resize(1, 0);
        let mut updated_at = crate::timestamp_column::TimestampColumn::new(epoch);
        updated_at.resize(1, 0);
        let mut versions = ChunkedVec::new();
        versions.resize(1, 0);
        let mut cached_json = ChunkedVec::new();
        cached_json.resize(1, None);

        Self {
            labels,
            properties,
            created_at,
            updated_at,
            versions,
            cached_json,
            alive: vec![0], // 1 word = 64 slots, slot 0 is dead
            alive_bm: RoaringBitmap::new(),
            count: 0,
            slot_count: 1,
        }
    }

    /// Number of live nodes.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Total allocated slots (including dead ones).
    pub fn capacity(&self) -> usize {
        self.slot_count
    }

    /// Return a RoaringBitmap of all live node IDs.
    /// Maintained incrementally on insert/remove; clone cost is O(containers).
    pub fn alive_bitmap(&self) -> RoaringBitmap {
        self.alive_bm.clone()
    }

    /// Check if a node exists.
    pub fn contains(&self, id: NodeId) -> bool {
        let idx = id.0 as usize;
        idx < self.slot_count && bit_test(&self.alive, idx)
    }

    /// Get a read-only reference to a node.
    pub fn get(&self, id: NodeId) -> Option<NodeRef<'_>> {
        let idx = id.0 as usize;
        if idx >= self.slot_count || !bit_test(&self.alive, idx) {
            return None;
        }
        Some(NodeRef {
            id,
            labels: &self.labels[idx],
            properties: &self.properties[idx],
            created_at: self.created_at.get(idx as u32),
            updated_at: self.updated_at.get(idx as u32),
            version: self.versions[idx],
            cached_json: &self.cached_json[idx],
        })
    }

    /// Ensure capacity for the given ID. Grows with doubling strategy
    /// to amortize resize cost across 6 column ChunkedVecs.
    fn ensure_capacity(&mut self, id: NodeId) {
        let idx = id.0 as usize;
        if idx >= self.slot_count {
            // Double or grow to fit, whichever is larger
            let new_len = (self.slot_count * 2).max(idx + 1).max(64);
            self.labels.resize(new_len, LabelSet::new());
            self.properties.resize_with(new_len, PropertyMap::new);
            self.created_at.resize(new_len, 0);
            self.updated_at.resize(new_len, 0);
            self.versions.resize(new_len, 0);
            self.cached_json.resize(new_len, None);
            // Grow alive bitvec to cover new_len slots
            let new_words = new_len.div_ceil(64);
            self.alive.resize(new_words, 0);
            self.slot_count = new_len;
        }
    }

    /// Insert a node, overwriting any existing node at that ID.
    pub fn insert(&mut self, node: Node) {
        let idx = node.id.0 as usize;
        self.ensure_capacity(node.id);

        if !bit_test(&self.alive, idx) {
            self.count += 1;
        }

        self.labels.set(idx, node.labels);
        self.properties.set(idx, node.properties);
        self.created_at.set(idx as u32, node.created_at);
        self.updated_at.set(idx as u32, node.updated_at);
        self.versions.set(idx, node.version);
        self.cached_json.set(idx, node.cached_json);
        bit_set(&mut self.alive, idx);
        self.alive_bm.insert(idx as u32);
    }

    /// Remove a node, returning the owned Node if it existed.
    ///
    /// Uses `std::mem::replace` to swap owned fields (LabelSet, PropertyMap)
    /// with empty defaults. This is intentional: it moves the heap-allocated
    /// data out of the slot so the caller receives it, while leaving valid
    /// (but empty) values behind for proper drop semantics on the dead slot.
    pub fn remove(&mut self, id: NodeId) -> Option<Node> {
        let idx = id.0 as usize;
        if idx >= self.slot_count || !bit_test(&self.alive, idx) {
            return None;
        }
        bit_clear(&mut self.alive, idx);
        self.alive_bm.remove(idx as u32);
        self.count -= 1;

        Some(Node {
            id,
            labels: std::mem::replace(
                self.labels
                    .get_mut(idx)
                    .expect("alive bit set but slot missing"),
                LabelSet::new(),
            ),
            properties: std::mem::replace(
                self.properties
                    .get_mut(idx)
                    .expect("alive bit set but slot missing"),
                PropertyMap::new(),
            ),
            created_at: self.created_at.get(idx as u32),
            updated_at: self.updated_at.get(idx as u32),
            version: self.versions[idx],
            cached_json: self
                .cached_json
                .get_mut(idx)
                .expect("alive bit set but slot missing")
                .take(),
        })
    }

    /// Iterate over all live node IDs.
    pub fn all_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.alive
            .iter()
            .enumerate()
            .flat_map(|(word_idx, &word)| {
                // Mask out slot 0 (always dead) from the first word
                let w = if word_idx == 0 { word & !1 } else { word };
                BitIter {
                    word: w,
                    base: (word_idx * 64) as u64,
                }
            })
            .map(NodeId)
    }

    // ── Per-column mutable access (for mutation.rs) ──────────────────

    /// Get mutable access to a node's labels. Triggers CoW on the chunk.
    pub fn labels_mut(&mut self, id: NodeId) -> Option<&mut LabelSet> {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            self.labels.get_mut(idx)
        } else {
            None
        }
    }

    /// Get mutable access to a node's properties. Triggers CoW on the chunk.
    pub fn properties_mut(&mut self, id: NodeId) -> Option<&mut PropertyMap> {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            self.properties.get_mut(idx)
        } else {
            None
        }
    }

    /// Get a node's current version.
    pub fn version(&self, id: NodeId) -> Option<u64> {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            Some(self.versions[idx])
        } else {
            None
        }
    }

    /// Set version and updated_at for a node. Triggers CoW on both chunks.
    pub fn set_version(&mut self, id: NodeId, version: u64, updated_at: i64) {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            self.versions.set(idx, version);
            self.updated_at.set(idx as u32, updated_at);
        }
    }

    /// Bump version by 1 and set updated_at. Returns (old_version, old_updated_at).
    pub fn bump_version(&mut self, id: NodeId, updated_at: i64) -> Option<(u64, i64)> {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            let old_version = self.versions[idx];
            let old_updated_at = self.updated_at.get(idx as u32);
            self.versions.set(idx, old_version + 1);
            self.updated_at.set(idx as u32, updated_at);
            Some((old_version, old_updated_at))
        } else {
            None
        }
    }

    /// Invalidate the cached JSON for a node.
    pub fn invalidate_json_cache(&mut self, id: NodeId) {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            self.cached_json.set(idx, None);
        }
    }

    /// Set the cached JSON for a node.
    pub fn set_json_cache(&mut self, id: NodeId, json: Arc<str>) {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            self.cached_json.set(idx, Some(json));
        }
    }

    /// Get read-only access to labels (for index operations during insert/remove).
    pub fn labels(&self, id: NodeId) -> Option<&LabelSet> {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            Some(&self.labels[idx])
        } else {
            None
        }
    }

    /// Get read-only access to properties (for index operations).
    pub fn properties(&self, id: NodeId) -> Option<&PropertyMap> {
        let idx = id.0 as usize;
        if idx < self.slot_count && bit_test(&self.alive, idx) {
            Some(&self.properties[idx])
        } else {
            None
        }
    }

    // ── Column accessors (for scans) ─────────────────────────────────

    /// Raw alive mask as u64 bitset for column scanning.
    pub fn alive_words(&self) -> &[u64] {
        &self.alive
    }

    /// Number of logical slots (column Vec length).
    pub fn slot_count(&self) -> usize {
        self.slot_count
    }

    /// Labels column (index with slot index, not NodeId).
    pub fn labels_column(&self) -> &ChunkedVec<LabelSet> {
        &self.labels
    }

    /// cached_json column.
    pub fn cached_json_column(&self) -> &ChunkedVec<Option<Arc<str>>> {
        &self.cached_json
    }
}

impl<'a> From<&'a Node> for NodeRef<'a> {
    fn from(node: &'a Node) -> Self {
        NodeRef {
            id: node.id,
            labels: &node.labels,
            properties: &node.properties,
            created_at: node.created_at,
            updated_at: node.updated_at,
            version: node.version,
            cached_json: &node.cached_json,
        }
    }
}

impl Default for NodeStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::IStr;

    fn test_node(id: u64, labels: &[&str]) -> Node {
        Node::new(NodeId(id), LabelSet::from_strs(labels), PropertyMap::new())
    }

    #[test]
    fn empty_store() {
        let store = NodeStore::new();
        assert_eq!(store.count(), 0);
        assert!(!store.contains(NodeId(1)));
        assert!(store.get(NodeId(1)).is_none());
    }

    #[test]
    fn insert_and_get() {
        let mut store = NodeStore::new();
        store.insert(test_node(1, &["sensor"]));

        assert_eq!(store.count(), 1);
        assert!(store.contains(NodeId(1)));

        let node = store.get(NodeId(1)).unwrap();
        assert_eq!(node.id, NodeId(1));
        assert!(node.has_label("sensor"));
    }

    #[test]
    fn remove_returns_owned() {
        let mut store = NodeStore::new();
        store.insert(test_node(5, &["zone"]));
        assert_eq!(store.count(), 1);

        let removed = store.remove(NodeId(5)).unwrap();
        assert_eq!(removed.id, NodeId(5));
        assert!(removed.has_label("zone"));
        assert_eq!(store.count(), 0);
        assert!(store.get(NodeId(5)).is_none());
    }

    #[test]
    fn remove_nonexistent() {
        let mut store = NodeStore::new();
        assert!(store.remove(NodeId(99)).is_none());
    }

    #[test]
    fn all_ids() {
        let mut store = NodeStore::new();
        store.insert(test_node(3, &["a"]));
        store.insert(test_node(7, &["b"]));
        store.insert(test_node(5, &["c"]));

        let mut ids: Vec<NodeId> = store.all_ids().collect();
        ids.sort_by_key(|n| n.0);
        assert_eq!(ids, vec![NodeId(3), NodeId(5), NodeId(7)]);
    }

    #[test]
    fn column_mutators() {
        let mut store = NodeStore::new();
        let mut node = test_node(1, &["sensor"]);
        node.properties.insert(IStr::new("unit"), Value::str("F"));
        store.insert(node);

        // Bump version
        let (old_ver, _) = store.bump_version(NodeId(1), 999).unwrap();
        assert_eq!(old_ver, 1);
        assert_eq!(store.version(NodeId(1)), Some(2));

        // Mutate labels
        store
            .labels_mut(NodeId(1))
            .unwrap()
            .insert(IStr::new("temperature"));
        let node = store.get(NodeId(1)).unwrap();
        assert!(node.has_label("temperature"));

        // Mutate properties
        store
            .properties_mut(NodeId(1))
            .unwrap()
            .insert(IStr::new("value"), Value::Float(72.5));
        let node = store.get(NodeId(1)).unwrap();
        assert_eq!(node.property("value"), Some(&Value::Float(72.5)));
    }

    #[test]
    fn to_owned_node() {
        let mut store = NodeStore::new();
        store.insert(test_node(1, &["sensor"]));

        let node_ref = store.get(NodeId(1)).unwrap();
        let owned = node_ref.to_owned_node();
        assert_eq!(owned.id, NodeId(1));
        assert!(owned.has_label("sensor"));
    }

    #[test]
    fn sparse_ids() {
        let mut store = NodeStore::new();
        store.insert(test_node(100, &["a"]));
        store.insert(test_node(200, &["b"]));

        assert_eq!(store.count(), 2);
        assert!(store.capacity() > 200);
        assert!(store.contains(NodeId(100)));
        assert!(store.contains(NodeId(200)));
        assert!(!store.contains(NodeId(150)));
    }

    #[test]
    fn bitset_word_boundaries() {
        let mut store = NodeStore::new();
        // Test IDs at u64 word boundaries: 1, 63, 64, 127, 128
        for id in [1, 63, 64, 127, 128] {
            store.insert(test_node(id, &["boundary"]));
        }
        assert_eq!(store.count(), 5);
        for id in [1, 63, 64, 127, 128] {
            assert!(store.contains(NodeId(id)), "should contain {id}");
        }
        assert!(!store.contains(NodeId(0)));
        assert!(!store.contains(NodeId(62)));
        assert!(!store.contains(NodeId(65)));

        // Remove at boundary and verify
        store.remove(NodeId(64));
        assert_eq!(store.count(), 4);
        assert!(!store.contains(NodeId(64)));
        assert!(store.contains(NodeId(63)));
        assert!(store.contains(NodeId(127)));
    }

    #[test]
    fn alive_bitmap_matches_bitset() {
        let mut store = NodeStore::new();
        store.insert(test_node(1, &["a"]));
        store.insert(test_node(5, &["b"]));
        store.insert(test_node(64, &["c"]));
        store.insert(test_node(100, &["d"]));
        store.remove(NodeId(5));

        let bitmap = store.alive_bitmap();
        assert!(bitmap.contains(1));
        assert!(!bitmap.contains(5));
        assert!(bitmap.contains(64));
        assert!(bitmap.contains(100));
        assert_eq!(bitmap.len(), 3);
    }

    #[test]
    fn all_ids_with_bitset() {
        let mut store = NodeStore::new();
        store.insert(test_node(1, &["a"]));
        store.insert(test_node(63, &["b"]));
        store.insert(test_node(64, &["c"]));
        store.insert(test_node(130, &["d"]));

        let mut ids: Vec<u64> = store.all_ids().map(|n| n.0).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 63, 64, 130]);
    }

    #[test]
    fn cow_clone_independence() {
        let mut store = NodeStore::new();
        store.insert(test_node(1, &["sensor"]));

        let original = store.clone();

        // Mutate the clone — should not affect original
        store
            .labels_mut(NodeId(1))
            .unwrap()
            .insert(IStr::new("mutated"));

        let orig_node = original.get(NodeId(1)).unwrap();
        assert!(!orig_node.has_label("mutated"));

        let clone_node = store.get(NodeId(1)).unwrap();
        assert!(clone_node.has_label("mutated"));
    }
}
