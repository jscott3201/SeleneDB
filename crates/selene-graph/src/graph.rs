//! The in-memory property graph -- core data structures and indexes.
//!
//! [`SeleneGraph`] stores nodes and edges in CoW `ChunkedVec`s with secondary indexes
//! for label lookups and adjacency traversal.  All mutation flows through
//! `pub(crate)` raw helpers so that [`TrackedMutation`](crate::mutation::TrackedMutation)
//! can record changes atomically.

use rustc_hash::FxHashMap;

use imbl::HashMap as ImblMap;
use roaring::RoaringBitmap;

use selene_core::schema::ValidationMode;
use selene_core::{Edge, EdgeId, IStr, Node, NodeId, Value};

use crate::edge_store::{EdgeRef, EdgeStore};
use crate::node_store::{NodeRef, NodeStore};

use crate::changelog::ChangelogBuffer;
use crate::schema::SchemaValidator;

// Empty slice for nodes with no adjacency entries.
const EMPTY_EDGES: &[EdgeId] = &[];

/// The core in-memory property graph.
///
/// Nodes and edges are stored in column-oriented `ChunkedVec` stores indexed by ID.
/// Slot 0 is always dead (IDs start at 1). Alive mask tracks live entries.
/// Clone copies chunk pointers -- O(N/256) Arc increments for near-instant snapshots.
#[derive(Clone)]
pub struct SeleneGraph {
    // Core storage -- column-oriented (SoA) stores
    pub(crate) nodes: NodeStore,
    pub(crate) edges: EdgeStore,

    // Indexes -- persistent (imbl) for O(log N) clone via structural sharing
    idx_label: ImblMap<IStr, RoaringBitmap>,
    idx_edge_label: ImblMap<IStr, RoaringBitmap>,
    adjacency_out: ImblMap<NodeId, Vec<EdgeId>>,
    adjacency_in: ImblMap<NodeId, Vec<EdgeId>>,

    // ID generation
    next_node_id: u64,
    next_edge_id: u64,

    // Property secondary index: (label, prop_key) → TypedIndex
    // Only populated for schema properties with `indexed: true`.
    // TypedIndex uses native BTreeMap per value type for correct sort order.
    // Wrapped in Arc for O(1) clone during snapshot publish (CoW on mutation).
    pub(crate) property_index:
        FxHashMap<(IStr, IStr), std::sync::Arc<crate::typed_index::TypedIndex>>,

    // Composite property index: (label, [key1, key2, ...]) → CompositeTypedIndex
    // Populated for schemas with key_properties.len() >= 2.
    // Wrapped in Arc for O(1) clone during snapshot publish (CoW on mutation).
    pub(crate) composite_indexes:
        FxHashMap<(IStr, Vec<IStr>), std::sync::Arc<crate::typed_index::CompositeTypedIndex>>,

    /// HNSW approximate nearest neighbor indexes, keyed by namespace.
    /// Default namespace `""` holds non-system vectors. System labels (e.g.
    /// `__CommunitySummary`) route to namespaces like `"__communitysummary"`.
    pub(crate) hnsw_indexes: rustc_hash::FxHashMap<String, std::sync::Arc<crate::hnsw::HnswIndex>>,

    // Mutation generation -- incremented on every commit for cache invalidation
    generation: u64,

    // Schema + changelog + triggers + materialized views
    pub(crate) schema: SchemaValidator,
    pub(crate) changelog: ChangelogBuffer,
    pub(crate) trigger_registry: crate::trigger::TriggerRegistry,
    pub(crate) view_registry: crate::view_registry::ViewRegistry,
}

// ── Constructors ────────────────────────────────────────────────────────────

impl std::fmt::Debug for SeleneGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeleneGraph")
            .field("nodes", &self.nodes.count())
            .field("edges", &self.edges.count())
            .finish()
    }
}

impl SeleneGraph {
    /// Create a new empty graph with default settings.
    pub fn new() -> Self {
        Self::with_config(SchemaValidator::new(ValidationMode::Warn), 1_000)
    }

    /// Create a new empty graph with the given schema validator and changelog capacity.
    pub fn with_config(schema: SchemaValidator, changelog_capacity: usize) -> Self {
        Self {
            nodes: NodeStore::new(),
            edges: EdgeStore::new(),
            idx_label: ImblMap::new(),
            idx_edge_label: ImblMap::new(),
            adjacency_out: ImblMap::new(),
            adjacency_in: ImblMap::new(),
            next_node_id: 1,
            next_edge_id: 1,
            property_index: FxHashMap::default(),
            composite_indexes: FxHashMap::default(),
            hnsw_indexes: rustc_hash::FxHashMap::default(),
            generation: 0,
            schema,
            changelog: ChangelogBuffer::new(changelog_capacity),
            trigger_registry: crate::trigger::TriggerRegistry::new(),
            view_registry: crate::view_registry::ViewRegistry::new(),
        }
    }
}

impl Default for SeleneGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ── Read API ────────────────────────────────────────────────────────────────

impl SeleneGraph {
    // -- Node reads --

    /// Look up a node by ID.
    pub fn get_node(&self, id: NodeId) -> Option<NodeRef<'_>> {
        self.nodes.get(id)
    }

    /// Returns `true` if a node with the given ID exists.
    pub fn contains_node(&self, id: NodeId) -> bool {
        self.nodes.contains(id)
    }

    /// The number of live nodes currently stored.
    pub fn node_count(&self) -> usize {
        self.nodes.count()
    }

    /// Iterate over all node IDs that carry the given label.
    /// Uses `try_get` to avoid interning unknown labels from external input.
    pub fn nodes_by_label(&self, label: &str) -> impl Iterator<Item = NodeId> + '_ {
        let key = IStr::try_get(label);
        key.and_then(|k| self.idx_label.get(&k))
            .into_iter()
            .flat_map(|bm| bm.iter().map(|id| NodeId(u64::from(id))))
    }

    /// Count nodes with a given label. O(1) via bitmap cardinality.
    /// Uses `try_get` to avoid interning unknown labels from external input.
    pub fn nodes_by_label_count(&self, label: &str) -> usize {
        let key = IStr::try_get(label);
        key.and_then(|k| self.idx_label.get(&k))
            .map_or(0, |bm| bm.len() as usize)
    }

    /// Iterate over every node ID in the graph (unordered).
    pub fn all_node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.all_ids()
    }

    // -- Bitmap accessors (for GQL label expression evaluation) --

    /// Get the RoaringBitmap for nodes with a specific label.
    /// Returns None if the label has never been interned (avoids polluting the interner).
    pub fn label_bitmap(&self, label: &str) -> Option<&RoaringBitmap> {
        let istr = IStr::try_get(label)?;
        self.idx_label.get(&istr)
    }

    /// Get a RoaringBitmap of all live node IDs.
    pub fn all_node_bitmap(&self) -> RoaringBitmap {
        self.nodes.alive_bitmap()
    }

    /// Get a RoaringBitmap of all live edge IDs.
    pub fn all_edge_bitmap(&self) -> RoaringBitmap {
        self.edges.alive_bitmap()
    }

    /// Get the RoaringBitmap for edges with a specific label.
    /// Returns None if the label has never been interned.
    pub fn edge_label_bitmap(&self, label: &str) -> Option<&RoaringBitmap> {
        let istr = IStr::try_get(label)?;
        self.idx_edge_label.get(&istr)
    }

    /// Per-label node counts from the bitmap indexes.
    /// Used by `StatsCollector::rebuild_from_graph` at startup.
    pub fn node_label_counts(&self) -> std::collections::HashMap<IStr, i64> {
        self.idx_label
            .iter()
            .map(|(label, bm)| (*label, bm.len() as i64))
            .collect()
    }

    /// Per-label edge counts from the bitmap indexes.
    /// Used by `StatsCollector::rebuild_from_graph` at startup.
    pub fn edge_label_counts(&self) -> std::collections::HashMap<IStr, i64> {
        self.idx_edge_label
            .iter()
            .map(|(label, bm)| (*label, bm.len() as i64))
            .collect()
    }

    // -- Edge reads --

    /// Look up an edge by ID.
    pub fn get_edge(&self, id: EdgeId) -> Option<EdgeRef<'_>> {
        self.edges.get(id)
    }

    /// Returns `true` if an edge with the given ID exists.
    pub fn contains_edge(&self, id: EdgeId) -> bool {
        self.edges.contains(id)
    }

    /// The number of live edges currently stored.
    pub fn edge_count(&self) -> usize {
        self.edges.count()
    }

    /// Approximate memory footprint of graph data in bytes.
    ///
    /// Directionally correct, not byte-exact. Used by OOM protection in
    /// MutationBatcher. O(L) where L = distinct labels.
    pub fn memory_estimate_bytes(&self) -> usize {
        let node_count = self.node_count();
        let edge_count = self.edge_count();

        // Node store: ~200 bytes per node (labels, properties, metadata)
        let nodes = node_count * 200;
        // Edge store: ~150 bytes per edge (endpoints, label, properties)
        let edges = edge_count * 150;

        // Label indexes: RoaringBitmap serialized size (close to in-memory size)
        let label_bitmaps: usize = self
            .idx_label
            .iter()
            .map(|(_, bm)| bm.serialized_size())
            .sum();
        let edge_label_bitmaps: usize = self
            .idx_edge_label
            .iter()
            .map(|(_, bm)| bm.serialized_size())
            .sum();

        // Adjacency indexes: ~120 bytes per entry (ImblMap tree overhead + Vec<EdgeId>)
        let adjacency = (self.adjacency_out.len() + self.adjacency_in.len()) * 120;

        // Property indexes: rough per-index estimate
        let prop_indexes = self.property_index.len() * 256;

        // Composite indexes: rough per-index estimate (BTreeMap overhead + entry vecs)
        let composite_indexes = self.composite_indexes.len() * 256;

        nodes
            + edges
            + label_bitmaps
            + edge_label_bitmaps
            + adjacency
            + prop_indexes
            + composite_indexes
    }

    /// Iterate over every edge ID in the graph (unordered).
    pub fn all_edge_ids(&self) -> impl Iterator<Item = EdgeId> + '_ {
        self.edges.all_ids()
    }

    /// Iterate over all edge IDs that carry the given label.
    /// Uses `try_get` to avoid interning unknown labels from external input.
    pub fn edges_by_label(&self, label: &str) -> impl Iterator<Item = EdgeId> + '_ {
        let key = IStr::try_get(label);
        key.and_then(|k| self.idx_edge_label.get(&k))
            .into_iter()
            .flat_map(|bm| bm.iter().map(|id| EdgeId(u64::from(id))))
    }

    /// Outgoing edge IDs for a node.
    pub fn outgoing(&self, id: NodeId) -> &[EdgeId] {
        self.adjacency_out
            .get(&id)
            .map_or(EMPTY_EDGES, Vec::as_slice)
    }

    /// Incoming edge IDs for a node.
    pub fn incoming(&self, id: NodeId) -> &[EdgeId] {
        self.adjacency_in
            .get(&id)
            .map_or(EMPTY_EDGES, Vec::as_slice)
    }

    // -- Property index --

    /// Look up nodes by (label, property_key, property_value) in the secondary index.
    /// Returns `None` if no index exists or value not found.
    pub fn property_index_lookup(
        &self,
        label: IStr,
        prop_key: IStr,
        value: &selene_core::Value,
    ) -> Option<&Vec<NodeId>> {
        self.property_index
            .get(&(label, prop_key))
            .and_then(|idx| idx.lookup(value))
    }

    /// Check if a property index exists for the given (label, property_key) pair.
    pub fn has_property_index(&self, label: IStr, prop_key: IStr) -> bool {
        self.property_index.contains_key(&(label, prop_key))
    }

    /// Create or replace a property index for the given (label, property_key) pair,
    /// populating it from existing nodes that carry the label and have the property set.
    pub fn create_property_index(&mut self, label: IStr, prop_key: IStr) {
        use crate::typed_index::TypedIndex;
        use selene_core::ValueType;

        // Determine the value type from the first matching node's property.
        let value_type = self
            .all_node_bitmap()
            .iter()
            .filter_map(|nid| self.get_node(NodeId(u64::from(nid))))
            .filter(|n| n.labels.contains(label))
            .find_map(|n| {
                n.properties.get(prop_key).map(|v| match v {
                    selene_core::Value::Int(_) => ValueType::Int,
                    selene_core::Value::UInt(_) => ValueType::UInt,
                    selene_core::Value::Float(_) => ValueType::Float,
                    _ => ValueType::String,
                })
            })
            .unwrap_or(ValueType::String);

        let mut idx = TypedIndex::new_for_type(&value_type);
        for nid in &self.all_node_bitmap() {
            let node_id = NodeId(u64::from(nid));
            if let Some(node) = self.get_node(node_id)
                && node.labels.contains(label)
                && let Some(val) = node.properties.get(prop_key)
            {
                idx.insert(val, node_id);
            }
        }
        self.property_index
            .insert((label, prop_key), std::sync::Arc::new(idx));
    }

    /// Get the TypedIndex for sorted iteration (index-ordered scan).
    /// Returns None if no index exists for this (label, key) pair.
    pub fn property_index_entries(
        &self,
        label: IStr,
        prop_key: IStr,
    ) -> Option<&crate::typed_index::TypedIndex> {
        self.property_index.get(&(label, prop_key)).map(|v| &**v)
    }

    /// Look up nodes by composite key (label, [key1, key2, ...], [val1, val2, ...]).
    /// Returns `None` if no composite index exists or values not found.
    ///
    /// Uses linear scan over composite indexes (typically 0-2 per label) to
    /// avoid allocating a `Vec<IStr>` for the HashMap lookup key on every call.
    pub fn composite_index_lookup(
        &self,
        label: IStr,
        keys: &[IStr],
        values: &[&selene_core::Value],
    ) -> Option<&Vec<NodeId>> {
        self.composite_indexes
            .iter()
            .find(|((l, k), _)| *l == label && k.as_slice() == keys)
            .and_then(|(_, idx)| idx.lookup(values))
    }

    /// Iterate over all composite indexes for a given label.
    /// Returns references to the (label, keys) tuple and the index.
    pub fn composite_indexes_for_label(
        &self,
        label: IStr,
    ) -> impl Iterator<Item = (&(IStr, Vec<IStr>), &crate::typed_index::CompositeTypedIndex)> {
        self.composite_indexes
            .iter()
            .filter(move |((l, _), _)| *l == label)
            .map(|(k, v)| (k, &**v))
    }

    // -- Accessors --

    pub fn changelog(&self) -> &ChangelogBuffer {
        &self.changelog
    }

    pub fn schema(&self) -> &SchemaValidator {
        &self.schema
    }

    pub fn schema_mut(&mut self) -> &mut SchemaValidator {
        &mut self.schema
    }

    /// Get a property value, falling back to schema default if not set.
    ///
    /// Returns the explicit property value if present, otherwise checks
    /// the schema for a default value. Returns None only if neither exists.
    /// Used by the GQL engine for lazy schema migration support.
    pub fn resolve_property(&self, node_id: NodeId, key: IStr) -> Option<Value> {
        let node = self.get_node(node_id)?;
        if let Some(value) = node.properties.get(key) {
            return Some(value.clone());
        }
        self.schema.property_default(node.labels, key)
    }

    pub fn trigger_registry(&self) -> &crate::trigger::TriggerRegistry {
        &self.trigger_registry
    }

    pub fn trigger_registry_mut(&mut self) -> &mut crate::trigger::TriggerRegistry {
        &mut self.trigger_registry
    }

    pub fn view_registry(&self) -> &crate::view_registry::ViewRegistry {
        &self.view_registry
    }

    pub fn view_registry_mut(&mut self) -> &mut crate::view_registry::ViewRegistry {
        &mut self.view_registry
    }

    /// Get the default-namespace HNSW vector index.
    pub fn hnsw_index(&self) -> Option<&std::sync::Arc<crate::hnsw::HnswIndex>> {
        self.hnsw_indexes.get("")
    }

    /// Get the HNSW vector index for a specific namespace.
    pub fn hnsw_index_for(
        &self,
        namespace: &str,
    ) -> Option<&std::sync::Arc<crate::hnsw::HnswIndex>> {
        self.hnsw_indexes.get(namespace)
    }

    /// Set the HNSW vector index for a namespace (empty string = default).
    pub fn set_hnsw_index_for(
        &mut self,
        namespace: String,
        index: std::sync::Arc<crate::hnsw::HnswIndex>,
    ) {
        self.hnsw_indexes.insert(namespace, index);
    }

    /// Set the default-namespace HNSW vector index.
    pub fn set_hnsw_index(&mut self, index: std::sync::Arc<crate::hnsw::HnswIndex>) {
        self.hnsw_indexes.insert(String::new(), index);
    }

    /// Iterate all HNSW indexes by namespace.
    pub fn hnsw_indexes(
        &self,
    ) -> &rustc_hash::FxHashMap<String, std::sync::Arc<crate::hnsw::HnswIndex>> {
        &self.hnsw_indexes
    }

    /// Read-only access to the node store (for column-level scans).
    pub fn node_store(&self) -> &NodeStore {
        &self.nodes
    }

    /// Read-only access to the edge store (for column-level scans).
    pub fn edge_store(&self) -> &EdgeStore {
        &self.edges
    }

    /// Mutation generation counter. Incremented on every commit.
    /// Used by query-layer caches to detect staleness.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Highest node ID that has been allocated (may be deleted).
    pub fn max_node_id(&self) -> u64 {
        self.next_node_id.saturating_sub(1)
    }

    /// Increment the generation counter. Called after a successful mutation commit.
    pub(crate) fn bump_generation(&mut self) {
        self.generation += 1;
    }
}

// ── Mutate entry point ──────────────────────────────────────────────────────

impl SeleneGraph {
    pub fn mutate(&mut self) -> crate::mutation::TrackedMutation<'_> {
        crate::mutation::TrackedMutation::new(self)
    }
}

// ── Internal mutation helpers (pub(crate)) ──────────────────────────────────

impl SeleneGraph {
    /// Allocate and return the next [`NodeId`].
    ///
    /// Returns an error if the ID would exceed `u32::MAX` (RoaringBitmap limit).
    pub(crate) fn allocate_node_id(&mut self) -> Result<NodeId, crate::error::GraphError> {
        if self.next_node_id > u64::from(u32::MAX) {
            return Err(crate::error::GraphError::CapacityExceeded(
                "node ID overflow: label indexes require IDs <= u32::MAX".into(),
            ));
        }
        let id = NodeId(self.next_node_id);
        self.next_node_id += 1;
        Ok(id)
    }

    /// Allocate and return the next [`EdgeId`].
    ///
    /// Returns an error if the ID would exceed `u32::MAX` (RoaringBitmap limit).
    pub(crate) fn allocate_edge_id(&mut self) -> Result<EdgeId, crate::error::GraphError> {
        if self.next_edge_id > u64::from(u32::MAX) {
            return Err(crate::error::GraphError::CapacityExceeded(
                "edge ID overflow: label indexes require IDs <= u32::MAX".into(),
            ));
        }
        let id = EdgeId(self.next_edge_id);
        self.next_edge_id += 1;
        Ok(id)
    }

    /// Ensure next_node_id is above the given ID.
    /// Used by change applier so promoted replicas don't collide.
    pub fn ensure_next_node_id_above(&mut self, id: NodeId) {
        if id.0 + 1 > self.next_node_id {
            self.next_node_id = id.0 + 1;
        }
    }

    /// Ensure next_edge_id is above the given ID.
    /// Used by change applier so promoted replicas don't collide.
    pub fn ensure_next_edge_id_above(&mut self, id: EdgeId) {
        if id.0 + 1 > self.next_edge_id {
            self.next_edge_id = id.0 + 1;
        }
    }

    /// Insert a node into storage and update all secondary indexes.
    /// Use `insert_node_raw_recovery` if the ID may already be occupied (e.g. during recovery).
    pub(crate) fn insert_node_raw(&mut self, node: Node) {
        let id = node.id;

        // Update label index
        for label in node.labels.iter() {
            self.idx_label.entry(label).or_default().insert(id.0 as u32);
        }

        // Ensure adjacency entries exist
        self.adjacency_out.entry(id).or_default();
        self.adjacency_in.entry(id).or_default();

        // Update property index for indexed properties
        for label in node.labels.iter() {
            for (prop_key, prop_value) in node.properties.iter() {
                let index_key = (label, *prop_key);
                if let Some(idx) = self.property_index.get_mut(&index_key) {
                    std::sync::Arc::make_mut(idx).insert(prop_value, id);
                }
            }
        }

        // Update composite indexes
        for label in node.labels.iter() {
            self.composite_index_insert(label, id, &node.properties, None);
        }

        self.nodes.insert(node);
    }

    /// Insert a node with ID reuse protection (for recovery/bulk load).
    /// Cleans up old label, property, and composite index entries if the
    /// slot is already occupied.
    pub(crate) fn insert_node_raw_recovery(&mut self, node: Node) {
        let id = node.id;
        if self.nodes.contains(id) {
            let old_labels: Vec<IStr> = self
                .nodes
                .labels(id)
                .map(|ls| ls.iter().collect())
                .unwrap_or_default();

            // Label bitmap cleanup
            for &label in &old_labels {
                if let Some(bm) = self.idx_label.get_mut(&label) {
                    bm.remove(id.0 as u32);
                }
            }

            // Property index cleanup
            if let Some(old_props) = self.nodes.properties(id) {
                for (prop_key, prop_value) in old_props.iter() {
                    for &label in &old_labels {
                        let index_key = (label, *prop_key);
                        if let Some(idx) = self.property_index.get_mut(&index_key) {
                            std::sync::Arc::make_mut(idx).remove(prop_value, id);
                        }
                    }
                }
            }

            // Composite index cleanup
            if let Some(old_props) = self.nodes.properties(id) {
                let owned_props = old_props.clone();
                for &label in &old_labels {
                    self.composite_index_remove(label, id, &owned_props, None);
                }
            }
        }
        self.insert_node_raw(node);
    }

    /// Remove a node from storage and all secondary indexes.
    pub(crate) fn remove_node_raw(&mut self, id: NodeId) -> Option<Node> {
        let node = self.nodes.remove(id)?;

        // Remove from property index
        for label in node.labels.iter() {
            for (prop_key, prop_value) in node.properties.iter() {
                let index_key = (label, *prop_key);
                if let Some(idx) = self.property_index.get_mut(&index_key) {
                    std::sync::Arc::make_mut(idx).remove(prop_value, id);
                }
            }
        }

        // Remove from composite indexes
        for label in node.labels.iter() {
            self.composite_index_remove(label, id, &node.properties, None);
        }

        // Remove from label indexes
        for label in node.labels.iter() {
            if let Some(set) = self.idx_label.get_mut(&label) {
                set.remove(id.0 as u32);
                if set.is_empty() {
                    self.idx_label.remove(&label);
                }
            }
        }

        // Remove adjacency entries
        self.adjacency_out.remove(&id);
        self.adjacency_in.remove(&id);

        Some(node)
    }

    /// Insert an edge into storage and update edge-label and adjacency indexes.
    pub(crate) fn insert_edge_raw(&mut self, edge: Edge) {
        let eid = edge.id;
        let (source, target, label) = self.edges.insert(edge);

        // Edge-label index
        self.idx_edge_label
            .entry(label)
            .or_default()
            .insert(eid.0 as u32);

        // Adjacency lists
        self.adjacency_out.entry(source).or_default().push(eid);
        self.adjacency_in.entry(target).or_default().push(eid);
    }

    /// Remove an edge from storage, edge-label index, and adjacency lists.
    pub(crate) fn remove_edge_raw(&mut self, id: EdgeId) -> Option<Edge> {
        let edge = self.edges.remove(id)?;

        // Edge-label index
        if let Some(set) = self.idx_edge_label.get_mut(&edge.label) {
            set.remove(id.0 as u32);
            if set.is_empty() {
                self.idx_edge_label.remove(&edge.label);
            }
        }

        // Adjacency out
        if let Some(vec) = self.adjacency_out.get_mut(&edge.source) {
            vec.retain(|e| *e != id);
        }

        // Adjacency in
        if let Some(vec) = self.adjacency_in.get_mut(&edge.target) {
            vec.retain(|e| *e != id);
        }

        Some(edge)
    }

    /// Add a label to an existing node and update the label index.
    pub(crate) fn add_label_raw(&mut self, id: NodeId, label: IStr) {
        if let Some(labels) = self.nodes.labels_mut(id) {
            labels.insert(label);
            self.idx_label.entry(label).or_default().insert(id.0 as u32);
        }
    }

    /// Remove a label from an existing node and update the label index.
    pub(crate) fn remove_label_raw(&mut self, id: NodeId, label: IStr) {
        if let Some(labels) = self.nodes.labels_mut(id)
            && labels.remove(label)
            && let Some(set) = self.idx_label.get_mut(&label)
        {
            set.remove(id.0 as u32);
            if set.is_empty() {
                self.idx_label.remove(&label);
            }
        }
    }

    /// Set a property on a node. Bypasses changelog. Used by the replica change applier.
    pub(crate) fn set_property_raw(&mut self, id: NodeId, key: IStr, value: Value) {
        // Remove old value from property index if indexed
        if let Some(node) = self.nodes.get(id) {
            for label in node.labels.iter() {
                let index_key = (label, key);
                if let Some(idx) = self.property_index.get_mut(&index_key)
                    && let Some(old_val) = node.properties.get(key)
                {
                    std::sync::Arc::make_mut(idx).remove(old_val, id);
                }
            }
            // Remove old composite index entry
            for label in node.labels.iter() {
                composite_index_apply(
                    &mut self.composite_indexes,
                    label,
                    id,
                    node.properties,
                    Some(key),
                    false,
                );
            }
        }
        if let Some(props) = self.nodes.properties_mut(id) {
            props.insert(key, value.clone());
        }
        // Re-index with new value
        if let Some(node) = self.nodes.get(id) {
            for label in node.labels.iter() {
                let index_key = (label, key);
                if let Some(idx) = self.property_index.get_mut(&index_key) {
                    std::sync::Arc::make_mut(idx).insert(&value, id);
                }
            }
            // Insert new composite index entry
            for label in node.labels.iter() {
                composite_index_apply(
                    &mut self.composite_indexes,
                    label,
                    id,
                    node.properties,
                    Some(key),
                    true,
                );
            }
        }
    }

    /// Remove a property from a node. Bypasses changelog. Used by the replica change applier.
    pub(crate) fn remove_property_raw(&mut self, id: NodeId, key: IStr) {
        // Remove from property index if indexed
        if let Some(node) = self.nodes.get(id) {
            for label in node.labels.iter() {
                let index_key = (label, key);
                if let Some(idx) = self.property_index.get_mut(&index_key)
                    && let Some(val) = node.properties.get(key)
                {
                    std::sync::Arc::make_mut(idx).remove(val, id);
                }
            }
            // Remove from composite indexes
            for label in node.labels.iter() {
                composite_index_apply(
                    &mut self.composite_indexes,
                    label,
                    id,
                    node.properties,
                    Some(key),
                    false,
                );
            }
        }
        if let Some(props) = self.nodes.properties_mut(id) {
            props.remove(key);
        }
    }

    /// Set a property on an edge. Bypasses changelog. Used by the replica change applier.
    pub(crate) fn set_edge_property_raw(&mut self, id: EdgeId, key: IStr, value: Value) {
        if let Some(props) = self.edges.properties_mut(id) {
            props.insert(key, value);
        }
    }

    /// Remove a property from an edge. Bypasses changelog. Used by the replica change applier.
    pub(crate) fn remove_edge_property_raw(&mut self, id: EdgeId, key: IStr) {
        if let Some(props) = self.edges.properties_mut(id) {
            props.remove(key);
        }
    }

    /// Insert a node into all matching composite indexes for the given label.
    ///
    /// If `key_filter` is `Some(k)`, only composite indexes that contain
    /// property `k` are updated. If `None`, all composite indexes for the
    /// label are updated.
    ///
    /// `properties` supplies the values for building the composite key.
    pub(crate) fn composite_index_insert(
        &mut self,
        label: IStr,
        node_id: NodeId,
        properties: &selene_core::PropertyMap,
        key_filter: Option<IStr>,
    ) {
        composite_index_apply(
            &mut self.composite_indexes,
            label,
            node_id,
            properties,
            key_filter,
            true,
        );
    }

    /// Remove a node from all matching composite indexes for the given label.
    ///
    /// If `key_filter` is `Some(k)`, only composite indexes that contain
    /// property `k` are updated. If `None`, all composite indexes for the
    /// label are updated.
    ///
    /// `properties` supplies the values for building the composite key.
    pub(crate) fn composite_index_remove(
        &mut self,
        label: IStr,
        node_id: NodeId,
        properties: &selene_core::PropertyMap,
        key_filter: Option<IStr>,
    ) {
        composite_index_apply(
            &mut self.composite_indexes,
            label,
            node_id,
            properties,
            key_filter,
            false,
        );
    }
}

/// Shared logic for inserting into or removing from composite indexes.
///
/// Takes the composite_indexes map directly so callers can hold disjoint
/// borrows on other `SeleneGraph` fields (e.g., `nodes`) concurrently.
///
/// If `key_filter` is `Some(k)`, only composite indexes containing property
/// `k` are updated. If `insert` is true, inserts the node; otherwise removes.
pub(crate) fn composite_index_apply(
    indexes: &mut FxHashMap<
        (IStr, Vec<IStr>),
        std::sync::Arc<crate::typed_index::CompositeTypedIndex>,
    >,
    label: IStr,
    node_id: NodeId,
    properties: &selene_core::PropertyMap,
    key_filter: Option<IStr>,
    insert: bool,
) {
    let matching_keys: Vec<Vec<IStr>> = indexes
        .keys()
        .filter(|(l, props)| *l == label && key_filter.is_none_or(|k| props.contains(&k)))
        .map(|(_, props)| props.clone())
        .collect();
    for props in matching_keys {
        let values: Vec<&Value> = props.iter().filter_map(|k| properties.get(*k)).collect();
        if values.len() == props.len()
            && let Some(cidx) = indexes.get_mut(&(label, props))
        {
            if insert {
                std::sync::Arc::make_mut(cidx).insert(&values, node_id);
            } else {
                std::sync::Arc::make_mut(cidx).remove(&values, node_id);
            }
        }
    }
}

// ── Bulk load (recovery) ────────────────────────────────────────────────────

impl SeleneGraph {
    /// Load a batch of nodes. Bypasses changelog.
    pub fn load_nodes(&mut self, nodes: Vec<Node>) {
        for node in nodes {
            let raw_id = node.id.0;
            if raw_id >= self.next_node_id {
                self.next_node_id = raw_id + 1;
            }
            self.insert_node_raw_recovery(node);
        }
    }

    /// Load a batch of edges. Bypasses changelog.
    pub fn load_edges(&mut self, edges: Vec<Edge>) {
        for edge in edges {
            let raw_id = edge.id.0;
            if raw_id >= self.next_edge_id {
                self.next_edge_id = raw_id + 1;
            }
            self.insert_edge_raw(edge);
        }
    }

    /// Current next node ID counter.
    pub fn next_node_id(&self) -> u64 {
        self.next_node_id
    }

    /// Current next edge ID counter.
    pub fn next_edge_id(&self) -> u64 {
        self.next_edge_id
    }

    /// Explicitly set the next ID counters.
    pub fn set_next_ids(
        &mut self,
        next_node: u64,
        next_edge: u64,
    ) -> Result<(), crate::error::GraphError> {
        if next_node > u64::from(u32::MAX) + 1 {
            return Err(crate::error::GraphError::CapacityExceeded(format!(
                "next_node_id {next_node} exceeds u32::MAX"
            )));
        }
        if next_edge > u64::from(u32::MAX) + 1 {
            return Err(crate::error::GraphError::CapacityExceeded(format!(
                "next_edge_id {next_edge} exceeds u32::MAX"
            )));
        }
        self.next_node_id = next_node;
        self.next_edge_id = next_edge;
        Ok(())
    }

    /// Partition the ID space so this node allocates IDs in a region
    /// that will not collide with other sync peers.
    /// Build property indexes from schema definitions.
    ///
    /// Scans all registered node schemas for `indexed: true` properties,
    /// creates empty index entries, then populates them from existing nodes.
    /// Call after schema registration or recovery.
    pub fn build_property_indexes(&mut self) {
        use crate::typed_index::TypedIndex;

        self.property_index.clear();

        // Collect indexed (label, property, value_type) triples from schemas
        let indexed_props: Vec<(IStr, IStr, selene_core::schema::ValueType)> = {
            let (node_schemas, _) = self.schema.export();
            let mut props = Vec::new();
            for schema in &node_schemas {
                let label = IStr::new(schema.label.as_ref());
                for prop in &schema.properties {
                    if prop.indexed {
                        props.push((
                            label,
                            IStr::new(prop.name.as_ref()),
                            prop.value_type.clone(),
                        ));
                    }
                }
            }
            props
        };

        if indexed_props.is_empty() {
            return;
        }

        // Create empty typed index entries
        for (label, prop_key, vt) in &indexed_props {
            self.property_index
                .entry((*label, *prop_key))
                .or_insert_with(|| std::sync::Arc::new(TypedIndex::new_for_type(vt)));
        }

        // Populate from existing nodes
        for node_id in self.all_node_ids().collect::<Vec<_>>() {
            if let Some(node) = self.nodes.get(node_id) {
                for (label, prop_key, _) in &indexed_props {
                    if node.labels.contains(*label)
                        && let Some(value) = node.properties.get(*prop_key)
                        && let Some(idx) = self.property_index.get_mut(&(*label, *prop_key))
                    {
                        std::sync::Arc::make_mut(idx).insert(value, node_id);
                    }
                }
            }
        }
    }

    /// Build composite property indexes from schema definitions.
    ///
    /// Scans all registered node schemas for `key_properties` with 2+ entries,
    /// creates empty composite index entries, then populates them from existing
    /// nodes. Call after `build_property_indexes()` or schema registration.
    pub fn build_composite_indexes(&mut self) {
        use crate::typed_index::CompositeTypedIndex;

        self.composite_indexes.clear();

        // Collect (label, key_properties) pairs from schemas with composite keys
        let composite_keys: Vec<(IStr, Vec<IStr>)> = {
            let mut keys = Vec::new();
            for schema in self.schema.all_node_schemas() {
                if schema.key_properties.len() >= 2 {
                    let label = IStr::new(schema.label.as_ref());
                    let props: Vec<IStr> = schema
                        .key_properties
                        .iter()
                        .map(|p| IStr::new(p.as_ref()))
                        .collect();
                    keys.push((label, props));
                }
            }
            keys
        };

        if composite_keys.is_empty() {
            return;
        }

        // Create empty composite indexes
        for (label, props) in &composite_keys {
            let index_key = (*label, props.clone());
            self.composite_indexes
                .entry(index_key)
                .or_insert_with(|| std::sync::Arc::new(CompositeTypedIndex::new(props.clone())));
        }

        // Populate from existing nodes
        for node_id in self.all_node_ids().collect::<Vec<_>>() {
            if let Some(node) = self.nodes.get(node_id) {
                for (label, props) in &composite_keys {
                    if !node.labels.contains(*label) {
                        continue;
                    }
                    // Collect property values; skip if any key is missing
                    let values: Vec<&selene_core::Value> = props
                        .iter()
                        .filter_map(|k| node.properties.get(*k))
                        .collect();
                    if values.len() != props.len() {
                        continue; // some key properties missing on this node
                    }
                    if let Some(idx) = self.composite_indexes.get_mut(&(*label, props.clone())) {
                        std::sync::Arc::make_mut(idx).insert(&values, node_id);
                    }
                }
            }
        }
    }
}

// ── Mutable column access (pub(crate)) ──────────────────────────────────────

impl SeleneGraph {
    /// Mutable access to node columns (for mutation.rs).
    pub(crate) fn node_store_mut(&mut self) -> &mut NodeStore {
        &mut self.nodes
    }

    /// Mutable access to edge columns (for mutation.rs).
    pub(crate) fn edge_store_mut(&mut self) -> &mut EdgeStore {
        &mut self.edges
    }
}

// ── Index consistency assertion ─────────────────────────────────────────────

impl SeleneGraph {
    /// Verify all index invariants. Panics with details if any invariant is violated.
    pub fn assert_indexes_consistent(&self) {
        // 1. Every NodeId in idx_label exists in nodes
        for (label, ids) in &self.idx_label {
            for id in ids {
                let node_id = NodeId(u64::from(id));
                assert!(
                    self.contains_node(node_id),
                    "idx_label[{label}] contains {node_id} which is not in nodes",
                );
            }
        }

        // 2. Every EdgeId in idx_edge_label exists in edges
        for (label, ids) in &self.idx_edge_label {
            for id in ids {
                let edge_id = EdgeId(u64::from(id));
                assert!(
                    self.contains_edge(edge_id),
                    "idx_edge_label[{label}] contains {edge_id} which is not in edges",
                );
            }
        }

        // 3. Every EdgeId in adjacency_out[n] has edge.source == n
        for (nid, eids) in &self.adjacency_out {
            for eid in eids {
                let edge = self.get_edge(*eid).unwrap_or_else(|| {
                    panic!("adjacency_out[{nid}] contains {eid} which is not in edges")
                });
                assert_eq!(
                    edge.source, *nid,
                    "adjacency_out[{nid}] contains {eid} but edge.source is {}",
                    edge.source,
                );
            }
        }

        // 4. Every EdgeId in adjacency_in[n] has edge.target == n
        for (nid, eids) in &self.adjacency_in {
            for eid in eids {
                let edge = self.get_edge(*eid).unwrap_or_else(|| {
                    panic!("adjacency_in[{nid}] contains {eid} which is not in edges")
                });
                assert_eq!(
                    edge.target, *nid,
                    "adjacency_in[{nid}] contains {eid} but edge.target is {}",
                    edge.target,
                );
            }
        }

        // 5. Every node in nodes has entries in adjacency_out and adjacency_in
        for node_id in self.all_node_ids() {
            assert!(
                self.adjacency_out.contains_key(&node_id),
                "node {node_id} has no adjacency_out entry",
            );
            assert!(
                self.adjacency_in.contains_key(&node_id),
                "node {node_id} has no adjacency_in entry",
            );
        }

        // 6. Every edge in edges appears in the correct adjacency lists
        for edge_id in self.all_edge_ids() {
            let edge = self.get_edge(edge_id).unwrap();
            let out_list = self.adjacency_out.get(&edge.source).unwrap_or_else(|| {
                panic!(
                    "edge {edge_id} source {} has no adjacency_out entry",
                    edge.source
                )
            });
            assert!(
                out_list.contains(&edge_id),
                "edge {edge_id} not found in adjacency_out[{}]",
                edge.source,
            );

            let in_list = self.adjacency_in.get(&edge.target).unwrap_or_else(|| {
                panic!(
                    "edge {edge_id} target {} has no adjacency_in entry",
                    edge.target
                )
            });
            assert!(
                in_list.contains(&edge_id),
                "edge {edge_id} not found in adjacency_in[{}]",
                edge.target,
            );
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use selene_core::{Edge, EdgeId, IStr, LabelSet, Node, NodeId, PropertyMap, Value};
    use selene_testing::edges::test_edge;
    use selene_testing::nodes::test_node;

    use super::*;

    #[test]
    fn empty_graph() {
        let g = SeleneGraph::new();
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);
        assert!(g.changelog().is_empty());
    }

    #[test]
    fn allocate_node_ids() {
        let mut g = SeleneGraph::new();
        let a = g.allocate_node_id().unwrap();
        let b = g.allocate_node_id().unwrap();
        let c = g.allocate_node_id().unwrap();
        assert_eq!(a, NodeId(1));
        assert_eq!(b, NodeId(2));
        assert_eq!(c, NodeId(3));
    }

    #[test]
    fn allocate_edge_ids() {
        let mut g = SeleneGraph::new();
        let a = g.allocate_edge_id().unwrap();
        let b = g.allocate_edge_id().unwrap();
        let c = g.allocate_edge_id().unwrap();
        assert_eq!(a, EdgeId(1));
        assert_eq!(b, EdgeId(2));
        assert_eq!(c, EdgeId(3));
    }

    #[test]
    fn insert_node_raw_and_get() {
        let mut g = SeleneGraph::new();
        let node = test_node(1, &["sensor"]);
        g.insert_node_raw(node);

        let got = g.get_node(NodeId(1)).expect("node should exist");
        assert_eq!(got.id, NodeId(1));
        assert!(got.has_label("sensor"));
    }

    #[test]
    fn insert_node_raw_updates_label_index() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["sensor"]));
        g.insert_node_raw(test_node(2, &["actuator"]));
        g.insert_node_raw(test_node(3, &["sensor"]));

        let sensors: Vec<NodeId> = g.nodes_by_label("sensor").collect();
        assert_eq!(sensors.len(), 2);
        assert!(sensors.contains(&NodeId(1)));
        assert!(sensors.contains(&NodeId(3)));
    }

    #[test]
    fn insert_node_with_multiple_labels() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["sensor", "temperature"]));

        assert_eq!(g.nodes_by_label("sensor").count(), 1);
        assert_eq!(g.nodes_by_label("temperature").count(), 1);
    }

    #[test]
    fn remove_node_raw() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["sensor"]));
        assert_eq!(g.node_count(), 1);

        let removed = g.remove_node_raw(NodeId(1));
        assert!(removed.is_some());
        assert_eq!(g.node_count(), 0);
        assert!(g.get_node(NodeId(1)).is_none());
        assert_eq!(g.nodes_by_label("sensor").count(), 0);
    }

    #[test]
    fn insert_edge_raw_and_get() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        let edge = test_edge(1, 1, 2, "feeds");
        g.insert_edge_raw(edge);

        let got = g.get_edge(EdgeId(1)).expect("edge should exist");
        assert_eq!(got.source, NodeId(1));
        assert_eq!(got.target, NodeId(2));
        assert_eq!(got.label.as_str(), "feeds");
    }

    #[test]
    fn insert_edge_raw_updates_adjacency() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        g.insert_edge_raw(test_edge(10, 1, 2, "feeds"));

        assert_eq!(g.outgoing(NodeId(1)), &[EdgeId(10)]);
        assert_eq!(g.incoming(NodeId(2)), &[EdgeId(10)]);
        assert!(g.outgoing(NodeId(2)).is_empty());
        assert!(g.incoming(NodeId(1)).is_empty());
    }

    #[test]
    fn insert_edge_raw_updates_edge_label_index() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        g.insert_edge_raw(test_edge(1, 1, 2, "feeds"));
        g.insert_edge_raw(test_edge(2, 1, 2, "contains"));
        g.insert_edge_raw(test_edge(3, 2, 1, "feeds"));

        let feeds: Vec<EdgeId> = g.edges_by_label("feeds").collect();
        assert_eq!(feeds.len(), 2);
        assert!(feeds.contains(&EdgeId(1)));
        assert!(feeds.contains(&EdgeId(3)));

        assert_eq!(g.edges_by_label("contains").count(), 1);
    }

    #[test]
    fn remove_edge_raw() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        g.insert_edge_raw(test_edge(1, 1, 2, "feeds"));
        assert_eq!(g.edge_count(), 1);

        let removed = g.remove_edge_raw(EdgeId(1));
        assert!(removed.is_some());
        assert_eq!(g.edge_count(), 0);
        assert!(g.get_edge(EdgeId(1)).is_none());
        assert!(g.outgoing(NodeId(1)).is_empty());
        assert!(g.incoming(NodeId(2)).is_empty());
        assert_eq!(g.edges_by_label("feeds").count(), 0);
    }

    #[test]
    fn outgoing_empty_for_unknown_node() {
        let g = SeleneGraph::new();
        assert!(g.outgoing(NodeId(999)).is_empty());
    }

    #[test]
    fn incoming_empty_for_unknown_node() {
        let g = SeleneGraph::new();
        assert!(g.incoming(NodeId(999)).is_empty());
    }

    #[test]
    fn contains_node_and_edge() {
        let mut g = SeleneGraph::new();
        assert!(!g.contains_node(NodeId(1)));
        assert!(!g.contains_edge(EdgeId(1)));

        g.insert_node_raw(test_node(1, &["x"]));
        g.insert_node_raw(test_node(2, &["x"]));
        g.insert_edge_raw(test_edge(1, 1, 2, "r"));

        assert!(g.contains_node(NodeId(1)));
        assert!(g.contains_edge(EdgeId(1)));
        assert!(!g.contains_node(NodeId(99)));
        assert!(!g.contains_edge(EdgeId(99)));
    }

    #[test]
    fn node_count_and_edge_count() {
        let mut g = SeleneGraph::new();
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);

        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        g.insert_edge_raw(test_edge(1, 1, 2, "r"));

        assert_eq!(g.node_count(), 2);
        assert_eq!(g.edge_count(), 1);
    }

    #[test]
    fn all_node_ids() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(5, &["x"]));
        g.insert_node_raw(test_node(10, &["y"]));
        g.insert_node_raw(test_node(15, &["z"]));

        let mut ids: Vec<NodeId> = g.all_node_ids().collect();
        ids.sort_by_key(|n| n.0);
        assert_eq!(ids, vec![NodeId(5), NodeId(10), NodeId(15)]);
    }

    #[test]
    fn add_label_raw_and_remove_label_raw() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["sensor"]));

        g.add_label_raw(NodeId(1), IStr::new("temperature"));
        let node = g.get_node(NodeId(1)).unwrap();
        assert!(node.has_label("sensor"));
        assert!(node.has_label("temperature"));
        assert_eq!(g.nodes_by_label("temperature").count(), 1);

        g.remove_label_raw(NodeId(1), IStr::new("sensor"));
        let node = g.get_node(NodeId(1)).unwrap();
        assert!(!node.has_label("sensor"));
        assert!(node.has_label("temperature"));
        assert_eq!(g.nodes_by_label("sensor").count(), 0);
    }

    #[test]
    fn load_nodes_bulk() {
        let mut g = SeleneGraph::new();
        let nodes: Vec<Node> = (1..=100).map(|i| test_node(i, &["device"])).collect();
        g.load_nodes(nodes);

        assert_eq!(g.node_count(), 100);
        assert_eq!(g.nodes_by_label("device").count(), 100);
        assert!(g.contains_node(NodeId(1)));
        assert!(g.contains_node(NodeId(100)));
        g.assert_indexes_consistent();
    }

    #[test]
    fn load_edges_bulk() {
        let mut g = SeleneGraph::new();
        g.load_nodes((1..=10).map(|i| test_node(i, &["n"])).collect());
        let edges: Vec<Edge> = (1..10).map(|i| test_edge(i, i, i + 1, "next")).collect();
        g.load_edges(edges);

        assert_eq!(g.edge_count(), 9);
        assert_eq!(g.outgoing(NodeId(1)).len(), 1);
        assert_eq!(g.incoming(NodeId(10)).len(), 1);
        assert_eq!(g.edges_by_label("next").count(), 9);
        g.assert_indexes_consistent();
    }

    #[test]
    fn set_next_ids() {
        let mut g = SeleneGraph::new();
        g.set_next_ids(500, 300).unwrap();
        assert_eq!(g.allocate_node_id().unwrap(), NodeId(500));
        assert_eq!(g.allocate_edge_id().unwrap(), EdgeId(300));
    }

    #[test]
    fn assert_indexes_consistent_on_valid_graph() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["sensor", "temperature"]));
        g.insert_node_raw(test_node(2, &["actuator"]));
        g.insert_node_raw(test_node(3, &["zone"]));
        g.insert_edge_raw(test_edge(1, 1, 2, "feeds"));
        g.insert_edge_raw(test_edge(2, 2, 3, "controls"));
        g.assert_indexes_consistent();
    }

    #[test]
    fn load_nodes_updates_next_id() {
        let mut g = SeleneGraph::new();
        g.load_nodes(vec![test_node(50, &["a"]), test_node(100, &["b"])]);
        let next = g.allocate_node_id().unwrap();
        assert_eq!(next, NodeId(101));
    }

    #[test]
    fn load_edges_updates_next_id() {
        let mut g = SeleneGraph::new();
        g.load_nodes(vec![test_node(1, &["a"]), test_node(2, &["b"])]);
        g.load_edges(vec![test_edge(42, 1, 2, "r")]);
        let next = g.allocate_edge_id().unwrap();
        assert_eq!(next, EdgeId(43));
    }

    #[test]
    fn remove_node_raw_nonexistent_returns_none() {
        let mut g = SeleneGraph::new();
        assert!(g.remove_node_raw(NodeId(999)).is_none());
    }

    #[test]
    fn remove_edge_raw_nonexistent_returns_none() {
        let mut g = SeleneGraph::new();
        assert!(g.remove_edge_raw(EdgeId(999)).is_none());
    }

    #[test]
    fn add_label_raw_on_nonexistent_node_is_noop() {
        let mut g = SeleneGraph::new();
        g.add_label_raw(NodeId(999), IStr::new("ghost"));
        assert_eq!(g.nodes_by_label("ghost").count(), 0);
    }

    #[test]
    fn remove_label_raw_on_nonexistent_node_is_noop() {
        let mut g = SeleneGraph::new();
        g.remove_label_raw(NodeId(999), IStr::new("ghost"));
    }

    #[test]
    fn with_config_custom_capacity() {
        let g = SeleneGraph::with_config(SchemaValidator::new(ValidationMode::Strict), 42);
        assert_eq!(g.schema().mode(), ValidationMode::Strict);
        assert_eq!(g.node_count(), 0);
    }

    #[test]
    fn default_trait() {
        let g = SeleneGraph::default();
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn multiple_edges_same_pair() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        g.insert_edge_raw(test_edge(1, 1, 2, "feeds"));
        g.insert_edge_raw(test_edge(2, 1, 2, "contains"));
        g.insert_edge_raw(test_edge(3, 2, 1, "feeds"));

        assert_eq!(g.outgoing(NodeId(1)).len(), 2);
        assert_eq!(g.incoming(NodeId(2)).len(), 2);
        assert_eq!(g.outgoing(NodeId(2)).len(), 1);
        assert_eq!(g.incoming(NodeId(1)).len(), 1);
        g.assert_indexes_consistent();
    }

    #[test]
    fn nodes_by_label_unknown_label() {
        let g = SeleneGraph::new();
        assert_eq!(g.nodes_by_label("nonexistent").count(), 0);
    }

    #[test]
    fn edges_by_label_unknown_label() {
        let g = SeleneGraph::new();
        assert_eq!(g.edges_by_label("nonexistent").count(), 0);
    }

    #[test]
    fn remove_node_cleans_up_empty_label_set() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["unique_label"]));
        assert_eq!(g.nodes_by_label("unique_label").count(), 1);

        g.remove_node_raw(NodeId(1));
        assert_eq!(g.nodes_by_label("unique_label").count(), 0);
    }

    #[test]
    fn containment_hierarchy_round_trip() {
        let mut g = SeleneGraph::new();
        let (nodes, edges) = selene_testing::edges::test_containment_hierarchy();
        g.load_nodes(nodes);
        g.load_edges(edges);

        assert_eq!(g.node_count(), 6);
        assert_eq!(g.edge_count(), 5);
        assert_eq!(g.edges_by_label("contains").count(), 5);
        assert_eq!(g.outgoing(NodeId(1)).len(), 1);
        assert_eq!(g.outgoing(NodeId(2)).len(), 2);
        g.assert_indexes_consistent();
    }

    #[test]
    fn schema_mut_allows_registration() {
        let mut g = SeleneGraph::new();
        use selene_core::schema::{NodeSchema, PropertyDef, ValueType};

        g.schema_mut()
            .register_node_schema(NodeSchema {
                label: Arc::from("sensor"),
                parent: None,
                properties: vec![PropertyDef::simple("unit", ValueType::String, true)],
                valid_edge_labels: vec![],
                description: String::new(),
                annotations: HashMap::new(),
                version: Default::default(),
                validation_mode: None,
                key_properties: vec![],
            })
            .unwrap();

        assert!(g.schema().node_schema("sensor").is_some());
    }

    #[test]
    fn slot_zero_is_dead() {
        let g = SeleneGraph::new();
        assert!(!g.contains_node(NodeId(0)));
        assert!(!g.contains_edge(EdgeId(0)));
    }

    #[test]
    fn node_count_tracks_live_not_capacity() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(5, &["b"]));
        // Store has capacity >= 6 slots (0-5), but only 2 live nodes
        assert_eq!(g.node_count(), 2);
        assert!(g.nodes.capacity() >= 6);
    }

    // -- Bitmap accessor tests (for GQL) --

    #[test]
    fn label_bitmap_returns_matching_nodes() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["sensor"]));
        g.insert_node_raw(test_node(2, &["sensor"]));
        g.insert_node_raw(test_node(3, &["building"]));

        let bitmap = g.label_bitmap("sensor").unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(!bitmap.contains(3));
    }

    #[test]
    fn label_bitmap_returns_none_for_unknown_label() {
        let g = SeleneGraph::new();
        assert!(g.label_bitmap("nonexistent").is_none());
    }

    #[test]
    fn label_bitmap_does_not_intern_unknown_labels() {
        let _g = SeleneGraph::new();
        // This label was never interned
        assert!(IStr::try_get("gql_bitmap_test_unknown").is_none());
        // label_bitmap should not intern it
        assert!(_g.label_bitmap("gql_bitmap_test_unknown").is_none());
        // Still not interned
        assert!(IStr::try_get("gql_bitmap_test_unknown").is_none());
    }

    #[test]
    fn all_node_bitmap_includes_all_live_nodes() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(3, &["b"]));
        g.insert_node_raw(test_node(5, &["c"]));

        let bitmap = g.all_node_bitmap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(1));
        assert!(!bitmap.contains(2)); // slot 2 is empty
        assert!(bitmap.contains(3));
        assert!(bitmap.contains(5));
    }

    #[test]
    fn edge_label_bitmap_returns_matching_edges() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        g.insert_node_raw(test_node(3, &["c"]));
        g.insert_edge_raw(test_edge(1, 1, 2, "feeds"));
        g.insert_edge_raw(test_edge(2, 2, 3, "feeds"));
        g.insert_edge_raw(test_edge(3, 1, 3, "contains"));

        let bitmap = g.edge_label_bitmap("feeds").unwrap();
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(2));
        assert!(!bitmap.contains(3));
    }

    // ── Raw property mutation tests ─────────────────────────────────

    #[test]
    fn set_property_raw_sets_value() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["sensor"]));
        g.set_property_raw(NodeId(1), IStr::new("temp"), Value::Float(22.5));
        let val = g
            .get_node(NodeId(1))
            .unwrap()
            .properties
            .get(IStr::new("temp"));
        assert_eq!(val, Some(&Value::Float(22.5)));
    }

    #[test]
    fn remove_property_raw_removes_value() {
        let mut g = SeleneGraph::new();
        let props = PropertyMap::from_pairs(vec![(IStr::new("temp"), Value::Float(22.5))]);
        let node = Node::new(NodeId(1), LabelSet::from_strs(&["sensor"]), props);
        g.insert_node_raw(node);
        g.remove_property_raw(NodeId(1), IStr::new("temp"));
        let val = g
            .get_node(NodeId(1))
            .unwrap()
            .properties
            .get(IStr::new("temp"));
        assert!(val.is_none());
    }

    #[test]
    fn set_edge_property_raw_sets_value() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        g.insert_edge_raw(test_edge(1, 1, 2, "rel"));
        g.set_edge_property_raw(EdgeId(1), IStr::new("weight"), Value::Float(1.0));
        let val = g
            .get_edge(EdgeId(1))
            .unwrap()
            .properties
            .get(IStr::new("weight"));
        assert_eq!(val, Some(&Value::Float(1.0)));
    }

    #[test]
    fn remove_edge_property_raw_removes_value() {
        let mut g = SeleneGraph::new();
        g.insert_node_raw(test_node(1, &["a"]));
        g.insert_node_raw(test_node(2, &["b"]));
        let props = PropertyMap::from_pairs(vec![(IStr::new("weight"), Value::Float(1.0))]);
        let edge = Edge::new(EdgeId(1), NodeId(1), NodeId(2), IStr::new("rel"), props);
        g.insert_edge_raw(edge);
        g.remove_edge_property_raw(EdgeId(1), IStr::new("weight"));
        let val = g
            .get_edge(EdgeId(1))
            .unwrap()
            .properties
            .get(IStr::new("weight"));
        assert!(val.is_none());
    }

    #[test]
    fn memory_estimate_empty_graph() {
        let g = SeleneGraph::new();
        let estimate = g.memory_estimate_bytes();
        assert_eq!(estimate, 0, "empty graph should have zero data footprint");
    }

    #[test]
    fn memory_estimate_grows_with_nodes() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        for _ in 0..100 {
            m.create_node(
                LabelSet::from_strs(&["sensor"]),
                PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String("test".into()))]),
            )
            .unwrap();
        }
        m.commit(0).unwrap();
        let estimate = g.memory_estimate_bytes();
        assert!(
            estimate > 10_000,
            "100 nodes with properties should be at least 10KB, got {estimate}"
        );
    }

    #[test]
    fn memory_estimate_includes_edges() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        let n1 = m
            .create_node(LabelSet::from_strs(&["a"]), PropertyMap::new())
            .unwrap();
        let n2 = m
            .create_node(LabelSet::from_strs(&["b"]), PropertyMap::new())
            .unwrap();
        m.create_edge(n1, IStr::new("rel"), n2, PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        let est_no_edge = 2 * 200; // 2 nodes
        let estimate = g.memory_estimate_bytes();
        assert!(
            estimate > est_no_edge,
            "graph with edges should estimate higher than nodes alone"
        );
    }

    // ── resolve_property tests ─────────────────────────────────────────

    #[test]
    fn resolve_property_prefers_explicit() {
        use selene_core::schema::{NodeSchema, PropertyDef, ValueType};

        let mut g = SeleneGraph::new();

        // Register a schema with a default for "version".
        let mut prop = PropertyDef::simple("version", ValueType::String, false);
        prop.default = Some(Value::str("0.0.0"));
        g.schema_mut()
            .register_node_schema(NodeSchema {
                label: Arc::from("firmware"),
                parent: None,
                properties: vec![prop],
                valid_edge_labels: vec![],
                description: String::new(),
                annotations: HashMap::new(),
                version: Default::default(),
                validation_mode: None,
                key_properties: vec![],
            })
            .unwrap();

        // Insert a node with an explicit value for "version".
        let props = PropertyMap::from_pairs(vec![(IStr::new("version"), Value::str("2.1.0"))]);
        let node = Node::new(NodeId(1), LabelSet::from_strs(&["firmware"]), props);
        g.insert_node_raw(node);

        // Explicit value must win over the schema default.
        let result = g.resolve_property(NodeId(1), IStr::new("version"));
        assert_eq!(result, Some(Value::str("2.1.0")));
    }

    #[test]
    fn resolve_property_falls_back_to_default() {
        use selene_core::schema::{NodeSchema, PropertyDef, ValueType};

        let mut g = SeleneGraph::new();

        // Register a schema with a default for "status".
        let mut prop = PropertyDef::simple("status", ValueType::String, false);
        prop.default = Some(Value::str("active"));
        g.schema_mut()
            .register_node_schema(NodeSchema {
                label: Arc::from("device"),
                parent: None,
                properties: vec![prop],
                valid_edge_labels: vec![],
                description: String::new(),
                annotations: HashMap::new(),
                version: Default::default(),
                validation_mode: None,
                key_properties: vec![],
            })
            .unwrap();

        // Insert a node without the "status" property.
        let node = Node::new(
            NodeId(1),
            LabelSet::from_strs(&["device"]),
            PropertyMap::new(),
        );
        g.insert_node_raw(node);

        // Schema default must be returned.
        let result = g.resolve_property(NodeId(1), IStr::new("status"));
        assert_eq!(result, Some(Value::str("active")));
    }

    #[test]
    fn resolve_property_no_node() {
        let g = SeleneGraph::new();
        // Non-existent node must return None.
        assert!(
            g.resolve_property(NodeId(999), IStr::new("anything"))
                .is_none()
        );
    }
}
