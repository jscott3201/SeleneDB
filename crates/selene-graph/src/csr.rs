//! CSR (Compressed Sparse Row) adjacency cache.
//!
//! Builds a flat-array representation of the adjacency lists for fast
//! sequential traversal. Cached until the graph generation changes.
//! 2-5x speedup for algorithm workloads compared to ImblMap adjacency.

use selene_core::{EdgeId, IStr, NodeId};

use crate::SeleneGraph;

/// Neighbor entry in the CSR neighbors array.
#[derive(Debug, Clone, Copy)]
pub struct CsrNeighbor {
    pub edge_id: EdgeId,
    pub node_id: NodeId,
    /// Edge label -- enables typed lookup without loading the edge record.
    pub label: IStr,
}

/// CSR adjacency for one direction (outgoing or incoming).
struct CsrData {
    /// offsets[node_id] = start index in neighbors. Length = max_node_id + 2.
    offsets: Vec<u32>,
    /// Flat array of (edge_id, target_node_id) pairs.
    neighbors: Vec<CsrNeighbor>,
}

/// Fully owned CSR adjacency. Both directions are built eagerly.
/// Provides O(1) neighbor slice access without allocation.
///
/// Because the struct is fully owned (`Send + Sync + 'static`), it can be
/// stored in a generation-gated cache and shared across concurrent queries.
pub struct CsrAdjacency {
    out: CsrData,
    inc: CsrData,
}

impl CsrAdjacency {
    /// Build CSR from the current graph state (both directions eagerly).
    pub fn build(graph: &SeleneGraph) -> Self {
        let max_node = graph.max_node_id();
        Self {
            out: build_csr(graph, max_node, true),
            inc: build_csr(graph, max_node, false),
        }
    }

    /// O(1) outgoing neighbor slice -- no allocation.
    pub fn outgoing(&self, node_id: NodeId) -> &[CsrNeighbor] {
        slice_neighbors_ref(&self.out, node_id)
    }

    /// O(1) incoming neighbor slice -- no allocation.
    pub fn incoming(&self, node_id: NodeId) -> &[CsrNeighbor] {
        slice_neighbors_ref(&self.inc, node_id)
    }

    /// O(log L) typed outgoing neighbor lookup where L = distinct edge labels for this node.
    /// Returns a contiguous sub-slice of neighbors with the given edge label.
    /// Neighbors are sorted by label within each node's segment during CSR build.
    pub fn outgoing_typed(&self, node_id: NodeId, label: IStr) -> &[CsrNeighbor] {
        typed_slice(&self.out, node_id, label)
    }

    /// O(log L) typed incoming neighbor lookup.
    pub fn incoming_typed(&self, node_id: NodeId, label: IStr) -> &[CsrNeighbor] {
        typed_slice(&self.inc, node_id, label)
    }
}

/// Find the contiguous sub-slice of neighbors with a given label.
/// Neighbors are sorted by label within each node's segment.
fn typed_slice(data: &CsrData, node_id: NodeId, label: IStr) -> &[CsrNeighbor] {
    let all = slice_neighbors_ref(data, node_id);
    if all.is_empty() {
        return &[];
    }
    // Binary search for the first neighbor with this label
    let start = all.partition_point(|n| n.label < label);
    if start >= all.len() || all[start].label != label {
        return &[];
    }
    let end = all[start..].partition_point(|n| n.label == label) + start;
    &all[start..end]
}

fn slice_neighbors_ref(data: &CsrData, node_id: NodeId) -> &[CsrNeighbor] {
    let idx = node_id.0 as usize;
    if idx >= data.offsets.len().saturating_sub(1) {
        return &[];
    }
    let start = data.offsets[idx] as usize;
    let end = data.offsets[idx + 1] as usize;
    &data.neighbors[start..end]
}

/// Build CSR for one direction from the graph's adjacency.
fn build_csr(graph: &SeleneGraph, max_node: u64, outgoing: bool) -> CsrData {
    let size = (max_node + 2) as usize;
    let mut offsets = vec![0u32; size];
    let mut neighbors = Vec::new();

    // Pass 1: count neighbors per node
    for nid_u32 in &graph.all_node_bitmap() {
        let nid = NodeId(u64::from(nid_u32));
        let edges = if outgoing {
            graph.outgoing(nid)
        } else {
            graph.incoming(nid)
        };
        let idx = nid.0 as usize;
        if idx < size {
            offsets[idx] = edges.len() as u32;
        }
    }

    // Convert counts to cumulative offsets
    let mut cumulative = 0u32;
    for offset in &mut offsets {
        let count = *offset;
        *offset = cumulative;
        cumulative += count;
    }

    // Pass 2: fill neighbor array and sort by label per node.
    neighbors.resize(
        cumulative as usize,
        CsrNeighbor {
            edge_id: EdgeId(0),
            node_id: NodeId(0),
            label: IStr::new(""), // sentinel, overwritten before any read
        },
    );

    let mut write_pos = offsets.clone();
    for nid_u32 in &graph.all_node_bitmap() {
        let nid = NodeId(u64::from(nid_u32));
        let edges = if outgoing {
            graph.outgoing(nid)
        } else {
            graph.incoming(nid)
        };
        let idx = nid.0 as usize;
        for &eid in edges {
            if let Some(edge) = graph.get_edge(eid) {
                let target = if outgoing { edge.target } else { edge.source };
                let pos = write_pos[idx] as usize;
                if pos < neighbors.len() {
                    neighbors[pos] = CsrNeighbor {
                        edge_id: eid,
                        node_id: target,
                        label: edge.label,
                    };
                    write_pos[idx] += 1;
                }
            }
        }
        // Sort this node's segment by label immediately after filling.
        // Enables O(log L) typed lookup via partition_point.
        let start = offsets[idx] as usize;
        let end = write_pos[idx] as usize;
        if end > start + 1 {
            neighbors[start..end].sort_unstable_by_key(|n| n.label);
        }
    }

    CsrData { offsets, neighbors }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, PropertyMap};

    fn test_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(LabelSet::from_strs(&["a"]), PropertyMap::new())
            .unwrap(); // 1
        m.create_node(LabelSet::from_strs(&["b"]), PropertyMap::new())
            .unwrap(); // 2
        m.create_node(LabelSet::from_strs(&["c"]), PropertyMap::new())
            .unwrap(); // 3
        m.create_edge(NodeId(1), IStr::new("knows"), NodeId(2), PropertyMap::new())
            .unwrap(); // 1
        m.create_edge(NodeId(1), IStr::new("knows"), NodeId(3), PropertyMap::new())
            .unwrap(); // 2
        m.create_edge(NodeId(2), IStr::new("knows"), NodeId(3), PropertyMap::new())
            .unwrap(); // 3
        m.commit(0).unwrap();
        g
    }

    #[test]
    fn adjacency_outgoing() {
        let g = test_graph();
        let adj = CsrAdjacency::build(&g);

        assert_eq!(adj.outgoing(NodeId(1)).len(), 2);
        assert_eq!(adj.outgoing(NodeId(2)).len(), 1);
        assert_eq!(adj.outgoing(NodeId(3)).len(), 0);
    }

    #[test]
    fn adjacency_incoming() {
        let g = test_graph();
        let adj = CsrAdjacency::build(&g);

        assert_eq!(adj.incoming(NodeId(3)).len(), 2);
        assert_eq!(adj.incoming(NodeId(1)).len(), 0);
    }
}
