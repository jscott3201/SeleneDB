//! Graph slice DTOs for wire transfer (federation sync, full graph export).

use serde::{Deserialize, Serialize};

use super::entity::{EdgeDto, NodeDto};

/// The type of graph slice to request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SliceType {
    /// Full graph snapshot.
    Full,
    /// Nodes matching any of the given labels + connecting edges.
    ByLabels { labels: Vec<String> },
    /// Containment subtree rooted at a node.
    Containment {
        root_id: u64,
        max_depth: Option<u32>,
    },
}

/// Request: get a graph slice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSliceRequest {
    pub slice_type: SliceType,
    /// Maximum number of nodes to return (default: all).
    #[serde(default)]
    pub limit: Option<usize>,
    /// Number of nodes to skip (for pagination).
    #[serde(default)]
    pub offset: Option<usize>,
}

/// Response: a graph slice (subgraph).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSlicePayload {
    pub nodes: Vec<NodeDto>,
    pub edges: Vec<EdgeDto>,
    /// Total number of nodes matching the slice (before pagination).
    /// Only set when limit/offset are used.
    #[serde(default)]
    pub total_nodes: Option<usize>,
    /// Total number of edges matching the slice (before pagination).
    #[serde(default)]
    pub total_edges: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};

    #[test]
    fn slice_request_round_trip() {
        let req = GraphSliceRequest {
            slice_type: SliceType::Containment {
                root_id: 42,
                max_depth: Some(3),
            },
            limit: None,
            offset: None,
        };
        let bytes = serialize_payload(&req, WireFlags::empty()).unwrap();
        let decoded: GraphSliceRequest = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        match decoded.slice_type {
            SliceType::Containment { root_id, max_depth } => {
                assert_eq!(root_id, 42);
                assert_eq!(max_depth, Some(3));
            }
            _ => panic!("expected Containment"),
        }
    }

    #[test]
    fn slice_payload_round_trip() {
        let payload = GraphSlicePayload {
            nodes: vec![NodeDto {
                id: 1,
                labels: vec!["test".into()],
                properties: Default::default(),
                created_at: 0,
                updated_at: 0,
                version: 1,
            }],
            edges: vec![],
            total_nodes: None,
            total_edges: None,
        };
        let bytes = serialize_payload(&payload, WireFlags::empty()).unwrap();
        let decoded: GraphSlicePayload = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.nodes.len(), 1);
        assert_eq!(decoded.nodes[0].id, 1);
    }
}
