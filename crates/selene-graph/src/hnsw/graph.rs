//! Immutable HNSW graph snapshot.
//!
//! `HnswGraph` is a fully-built, read-only view of the HNSW index. It is
//! wrapped in `Arc` and swapped atomically via `ArcSwap` when the index is
//! rebuilt, so readers never block writers and writers never block readers.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use selene_core::NodeId;

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
        }
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
