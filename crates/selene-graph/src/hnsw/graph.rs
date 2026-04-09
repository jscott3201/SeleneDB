//! Immutable HNSW graph snapshot.
//!
//! `HnswGraph` is a fully-built, read-only view of the HNSW index. It is
//! wrapped in `Arc` and swapped atomically via `ArcSwap` when the index is
//! rebuilt, so readers never block writers and writers never block readers.

use std::collections::HashMap;
use std::sync::Arc;

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use selene_core::NodeId;

use super::quantize::QuantizedStorage;

/// A single node in the HNSW graph.
///
/// `neighbors` is a per-layer adjacency list. `neighbors[0]` holds the
/// layer-0 (densest) neighbors; `neighbors[layer]` holds layer-`layer`
/// neighbors. The outer `SmallVec` is sized to 4 inline slots because most
/// nodes exist only on the lowest one or two layers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswNode {
    /// The property-graph node this HNSW node represents.
    pub node_id: NodeId,
    /// The vector embedding. Shared with `Value::Vector` -- no data
    /// duplication.
    pub vector: Arc<[f32]>,
    /// Per-layer neighbor lists. Index 0 is layer 0.
    pub neighbors: SmallVec<[Vec<u32>; 4]>,
    /// Highest layer on which this node appears.
    pub max_layer: u8,
}

/// Immutable snapshot of the HNSW index graph.
///
/// Constructed by the build algorithm and then atomically published. All
/// search operations work on this read-only view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswGraph {
    /// Dense node storage. Index in this `Vec` is the HNSW internal index
    /// (`u32`). Neighbor lists in `HnswNode` store these internal indices.
    pub(crate) nodes: Vec<HnswNode>,
    /// Internal index of the entry point (highest-layer node), or `None` when
    /// the graph is empty.
    pub(crate) entry_point: Option<u32>,
    /// Maximum layer present across all nodes.
    pub(crate) max_layer: u8,
    /// Lookup from `NodeId` (property-graph ID) to HNSW internal index.
    pub(crate) node_id_to_idx: HashMap<u64, u32>,
    /// Dimensionality of all vectors stored in this graph.
    pub(crate) dimensions: u16,
    /// Optional quantized vector storage for compressed search.
    pub(crate) quantized: Option<QuantizedStorage>,
}

impl HnswGraph {
    /// Create an empty graph for vectors of the given dimensionality.
    pub fn empty(dimensions: u16) -> Self {
        Self {
            nodes: Vec::new(),
            entry_point: None,
            max_layer: 0,
            node_id_to_idx: HashMap::new(),
            dimensions,
            quantized: None,
        }
    }

    /// Vector dimensionality for this graph.
    pub fn dimensions(&self) -> u16 {
        self.dimensions
    }

    /// Number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns `true` when the graph contains no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Look up a node by its property-graph `NodeId`.
    ///
    /// Returns the internal HNSW index together with a reference to the node,
    /// or `None` when the node is not present in this snapshot.
    pub fn get_by_node_id(&self, id: NodeId) -> Option<(u32, &HnswNode)> {
        let &idx = self.node_id_to_idx.get(&id.0)?;
        Some((idx, &self.nodes[idx as usize]))
    }

    /// Return the node at the given internal HNSW index.
    ///
    /// # Panics
    ///
    /// Panics when `idx` is out of bounds. Callers that hold an `HnswGraph`
    /// snapshot obtain indices from neighbor lists that were built by the same
    /// snapshot, so out-of-bounds access indicates a bug in the build
    /// algorithm.
    pub fn get(&self, idx: u32) -> &HnswNode {
        &self.nodes[idx as usize]
    }

    /// Return a reference to the quantized storage, if present.
    pub fn quantized(&self) -> Option<&QuantizedStorage> {
        self.quantized.as_ref()
    }

    /// Return the neighbors of node `idx` on the given `layer`.
    ///
    /// Returns an empty slice when the node does not appear on `layer` (i.e.
    /// `layer > node.max_layer`).
    pub fn neighbors(&self, idx: u32, layer: u8) -> &[u32] {
        let node = &self.nodes[idx as usize];
        node.neighbors
            .get(layer as usize)
            .map_or(&[], Vec::as_slice)
    }

    /// Deep clone this graph excluding nodes whose `NodeId` appears in `tombstones`.
    ///
    /// Internal HNSW indices (`u32`) are remapped so the resulting graph has a
    /// dense, contiguous node array. Neighbor lists are updated to reference
    /// the new indices, and any neighbor references to tombstoned nodes are
    /// dropped. The entry point is reassigned to the highest-layer surviving
    /// node if the original entry was tombstoned.
    pub fn clone_without(&self, tombstones: &RoaringBitmap) -> HnswGraph {
        if tombstones.is_empty() {
            return self.clone();
        }

        // Build a mapping from old internal index -> new internal index.
        // Nodes whose NodeId is in the tombstone set are skipped.
        let mut old_to_new: HashMap<u32, u32> = HashMap::new();
        let mut new_nodes: Vec<HnswNode> = Vec::new();
        let mut new_node_id_to_idx: HashMap<u64, u32> = HashMap::new();

        for (old_idx, node) in self.nodes.iter().enumerate() {
            if tombstones.contains(node.node_id.0 as u32) {
                continue;
            }
            let new_idx = new_nodes.len() as u32;
            old_to_new.insert(old_idx as u32, new_idx);
            new_node_id_to_idx.insert(node.node_id.0, new_idx);
            // Clone the node with empty neighbors (we remap below).
            new_nodes.push(HnswNode {
                node_id: node.node_id,
                vector: Arc::clone(&node.vector),
                neighbors: SmallVec::new(),
                max_layer: node.max_layer,
            });
        }

        // Remap neighbor lists: translate old indices to new indices, dropping
        // any references to tombstoned nodes (which have no mapping).
        for (old_idx, node) in self.nodes.iter().enumerate() {
            let Some(&new_idx) = old_to_new.get(&(old_idx as u32)) else {
                continue; // tombstoned node, skip
            };
            let mut remapped_neighbors: SmallVec<[Vec<u32>; 4]> = SmallVec::new();
            for layer_neighbors in &node.neighbors {
                let remapped: Vec<u32> = layer_neighbors
                    .iter()
                    .filter_map(|&old_neighbor| old_to_new.get(&old_neighbor).copied())
                    .collect();
                remapped_neighbors.push(remapped);
            }
            new_nodes[new_idx as usize].neighbors = remapped_neighbors;
        }

        // Determine the new entry point. If the old entry was tombstoned,
        // pick the surviving node with the highest max_layer.
        let new_entry = self
            .entry_point
            .and_then(|old_ep| old_to_new.get(&old_ep).copied())
            .or_else(|| {
                new_nodes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, n)| n.max_layer)
                    .map(|(idx, _)| idx as u32)
            });

        let new_max_layer = new_nodes.iter().map(|n| n.max_layer).max().unwrap_or(0);

        HnswGraph {
            nodes: new_nodes,
            entry_point: new_entry,
            max_layer: new_max_layer,
            node_id_to_idx: new_node_id_to_idx,
            dimensions: self.dimensions,
            quantized: self.quantized.as_ref().map(|qs| qs.remap(&old_to_new)),
        }
    }

    /// Reconnect neighbors of a deleted node to maintain graph connectivity.
    ///
    /// For each layer, removes the deleted node from its neighbors' adjacency
    /// lists and fills empty slots with connections to the deleted node's other
    /// neighbors, ranked by cosine similarity. This suppresses the "unreachable
    /// points" phenomenon that degrades recall at high tombstone ratios.
    pub(crate) fn reconnect_neighbors(
        &mut self,
        deleted_idx: u32,
        params: &super::params::HnswParams,
    ) {
        use super::distance::cosine_similarity;
        use std::collections::HashSet;

        // Snapshot the deleted node's data before mutating the graph.
        let deleted_neighbors = self.nodes[deleted_idx as usize].neighbors.clone();

        for (layer, layer_neighbors) in deleted_neighbors.iter().enumerate() {
            if layer_neighbors.is_empty() {
                continue;
            }
            let max_m = params.max_neighbors(layer as u8);

            for &neighbor_idx in layer_neighbors {
                // Remove the deleted node from this neighbor's list.
                self.nodes[neighbor_idx as usize].neighbors[layer]
                    .retain(|&idx| idx != deleted_idx);

                let current_len = self.nodes[neighbor_idx as usize].neighbors[layer].len();
                if current_len >= max_m {
                    continue; // already at capacity
                }
                let slots = max_m - current_len;

                // Existing connections (avoid duplicates).
                let existing: HashSet<u32> = self.nodes[neighbor_idx as usize].neighbors[layer]
                    .iter()
                    .copied()
                    .collect();

                // Score candidate replacements from the deleted node's other
                // neighbors on this layer, excluding self and already-connected.
                let neighbor_vec = Arc::clone(&self.nodes[neighbor_idx as usize].vector);
                let mut candidates: Vec<(u32, f32)> = layer_neighbors
                    .iter()
                    .filter(|&&c| c != neighbor_idx && c != deleted_idx && !existing.contains(&c))
                    .map(|&c| {
                        let sim = cosine_similarity(&neighbor_vec, &self.nodes[c as usize].vector);
                        (c, sim)
                    })
                    .collect();

                // Sort by descending similarity (best first).
                candidates.sort_unstable_by(|a, b| {
                    b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
                });

                // Add the top candidates up to available slots.
                for &(c, _) in candidates.iter().take(slots) {
                    self.nodes[neighbor_idx as usize].neighbors[layer].push(c);
                }
            }
        }

        // If the entry point was deleted, pick the surviving node with the
        // highest max_layer.
        if self.entry_point == Some(deleted_idx) {
            self.entry_point = self
                .nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| *i as u32 != deleted_idx)
                .max_by_key(|(_, n)| n.max_layer)
                .map(|(i, _)| i as u32);
        }
    }

    /// Serialize this graph to postcard bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, postcard::Error> {
        postcard::to_allocvec(self)
    }

    /// Deserialize a graph from postcard bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, postcard::Error> {
        postcard::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(raw_id: u64, vector: Vec<f32>, max_layer: u8) -> HnswNode {
        HnswNode {
            node_id: NodeId(raw_id),
            vector: vector.into(),
            neighbors: SmallVec::new(),
            max_layer,
        }
    }

    fn simple_graph() -> HnswGraph {
        let mut g = HnswGraph::empty(3);

        let mut n0 = make_node(10, vec![1.0, 0.0, 0.0], 1);
        // Layer 0 neighbors: [1]; Layer 1 neighbors: [1]
        n0.neighbors.push(vec![1]);
        n0.neighbors.push(vec![1]);

        let mut n1 = make_node(20, vec![0.0, 1.0, 0.0], 0);
        n1.neighbors.push(vec![0]);

        g.nodes.push(n0);
        g.nodes.push(n1);
        g.node_id_to_idx.insert(10, 0);
        g.node_id_to_idx.insert(20, 1);
        g.entry_point = Some(0);
        g.max_layer = 1;
        g
    }

    #[test]
    fn empty_graph() {
        let g = HnswGraph::empty(128);
        assert!(g.is_empty());
        assert_eq!(g.len(), 0);
        assert_eq!(g.dimensions, 128);
        assert!(g.entry_point.is_none());
    }

    #[test]
    fn get_by_node_id() {
        let g = simple_graph();

        let result = g.get_by_node_id(NodeId(10));
        assert!(result.is_some());
        let (idx, node) = result.unwrap();
        assert_eq!(idx, 0);
        assert_eq!(node.node_id, NodeId(10));

        let result2 = g.get_by_node_id(NodeId(20));
        assert!(result2.is_some());
        let (idx2, node2) = result2.unwrap();
        assert_eq!(idx2, 1);
        assert_eq!(node2.node_id, NodeId(20));

        assert!(g.get_by_node_id(NodeId(999)).is_none());
    }

    #[test]
    fn neighbors_lookup() {
        let g = simple_graph();

        // Node 0 has neighbors on layers 0 and 1.
        assert_eq!(g.neighbors(0, 0), &[1u32]);
        assert_eq!(g.neighbors(0, 1), &[1u32]);
        // Layer 2 does not exist for node 0.
        assert_eq!(g.neighbors(0, 2), &[] as &[u32]);

        // Node 1 only has layer-0 neighbors.
        assert_eq!(g.neighbors(1, 0), &[0u32]);
        assert_eq!(g.neighbors(1, 1), &[] as &[u32]);
    }

    // ------------------------------------------------------------------
    // clone_without tests
    // ------------------------------------------------------------------

    /// Build a three-node graph for clone_without tests.
    ///
    /// Nodes:
    ///   idx 0 -> NodeId(1), vector [1,0,0], max_layer 1
    ///   idx 1 -> NodeId(2), vector [0,1,0], max_layer 0
    ///   idx 2 -> NodeId(3), vector [0,0,1], max_layer 0
    ///
    /// Layer 0: fully connected (0-1, 0-2, 1-2).
    /// Layer 1: only node 0 (entry point).
    fn three_node_graph() -> HnswGraph {
        let mut g = HnswGraph::empty(3);

        let mut n0 = make_node(1, vec![1.0, 0.0, 0.0], 1);
        n0.neighbors.push(vec![1, 2]); // layer 0
        n0.neighbors.push(vec![]); // layer 1

        let mut n1 = make_node(2, vec![0.0, 1.0, 0.0], 0);
        n1.neighbors.push(vec![0, 2]); // layer 0

        let mut n2 = make_node(3, vec![0.0, 0.0, 1.0], 0);
        n2.neighbors.push(vec![0, 1]); // layer 0

        g.nodes.push(n0);
        g.nodes.push(n1);
        g.nodes.push(n2);
        g.node_id_to_idx.insert(1, 0);
        g.node_id_to_idx.insert(2, 1);
        g.node_id_to_idx.insert(3, 2);
        g.entry_point = Some(0);
        g.max_layer = 1;
        g
    }

    #[test]
    fn clone_without_empty_tombstones() {
        let g = three_node_graph();
        let tombstones = RoaringBitmap::new();
        let cloned = g.clone_without(&tombstones);

        assert_eq!(cloned.len(), 3);
        assert_eq!(cloned.entry_point, Some(0));
        assert_eq!(cloned.max_layer, 1);
        assert!(cloned.get_by_node_id(NodeId(1)).is_some());
        assert!(cloned.get_by_node_id(NodeId(2)).is_some());
        assert!(cloned.get_by_node_id(NodeId(3)).is_some());
    }

    #[test]
    fn clone_without_removes_node() {
        let g = three_node_graph();
        let mut tombstones = RoaringBitmap::new();
        tombstones.insert(2); // Remove NodeId(2)

        let cloned = g.clone_without(&tombstones);

        assert_eq!(cloned.len(), 2);
        assert!(cloned.get_by_node_id(NodeId(1)).is_some());
        assert!(
            cloned.get_by_node_id(NodeId(2)).is_none(),
            "tombstoned node must be absent"
        );
        assert!(cloned.get_by_node_id(NodeId(3)).is_some());

        // Verify neighbor lists do not reference the removed node.
        for i in 0..cloned.len() as u32 {
            for layer in 0..=cloned.max_layer {
                for &neighbor in cloned.neighbors(i, layer) {
                    let neighbor_node = cloned.get(neighbor);
                    assert_ne!(
                        neighbor_node.node_id,
                        NodeId(2),
                        "removed node must not appear in neighbor lists"
                    );
                }
            }
        }
    }

    #[test]
    fn clone_without_remaps_indices() {
        let g = three_node_graph();
        let mut tombstones = RoaringBitmap::new();
        tombstones.insert(1); // Remove NodeId(1) (old idx 0, the entry point)

        let cloned = g.clone_without(&tombstones);

        assert_eq!(cloned.len(), 2);
        // NodeId(2) should now be at idx 0, NodeId(3) at idx 1.
        let (idx2, _) = cloned.get_by_node_id(NodeId(2)).unwrap();
        let (idx3, _) = cloned.get_by_node_id(NodeId(3)).unwrap();
        assert_eq!(idx2, 0);
        assert_eq!(idx3, 1);

        // Neighbor lists should reference the new indices.
        let neighbors_of_2 = cloned.neighbors(idx2, 0);
        assert!(
            neighbors_of_2.contains(&idx3),
            "NodeId(2) should have NodeId(3) as neighbor"
        );
    }

    #[test]
    fn clone_without_entry_point_tombstoned() {
        let g = three_node_graph();
        let mut tombstones = RoaringBitmap::new();
        tombstones.insert(1); // Remove NodeId(1) which is the entry point

        let cloned = g.clone_without(&tombstones);

        // Entry point must exist and reference a valid node.
        assert!(cloned.entry_point.is_some());
        let ep = cloned.entry_point.unwrap();
        assert!(
            (ep as usize) < cloned.len(),
            "entry point must be in bounds"
        );
    }

    #[test]
    fn clone_without_all_nodes() {
        let g = three_node_graph();
        let mut tombstones = RoaringBitmap::new();
        tombstones.insert(1);
        tombstones.insert(2);
        tombstones.insert(3);

        let cloned = g.clone_without(&tombstones);
        assert!(cloned.is_empty());
        assert!(cloned.entry_point.is_none());
    }

    // ------------------------------------------------------------------
    // serialization tests
    // ------------------------------------------------------------------

    #[test]
    fn serialize_deserialize_round_trip() {
        let original = simple_graph();

        let bytes = original.to_bytes().expect("serialization failed");
        assert!(!bytes.is_empty());

        let restored = HnswGraph::from_bytes(&bytes).expect("deserialization failed");

        assert_eq!(restored.len(), original.len());
        assert_eq!(restored.entry_point, original.entry_point);
        assert_eq!(restored.max_layer, original.max_layer);
        assert_eq!(restored.dimensions, original.dimensions);

        // Verify node data survived the round-trip.
        let (idx, node) = restored.get_by_node_id(NodeId(10)).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(node.max_layer, 1);
        assert_eq!(&*node.vector, &[1.0f32, 0.0, 0.0]);
        assert_eq!(restored.neighbors(0, 0), &[1u32]);
        assert_eq!(restored.neighbors(0, 1), &[1u32]);

        let (_, node2) = restored.get_by_node_id(NodeId(20)).unwrap();
        assert_eq!(&*node2.vector, &[0.0f32, 1.0, 0.0]);
        assert_eq!(restored.neighbors(1, 0), &[0u32]);
    }
}
