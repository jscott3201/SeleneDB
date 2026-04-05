//! Federation DTOs: peer discovery and registration.

use serde::{Deserialize, Serialize};

/// Sent bidirectionally on peer connection to exchange node metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationRegisterPayload {
    /// Human-readable node name (e.g., "building_a", "hub").
    pub node_name: String,
    /// QUIC address for other peers to reach this node (e.g., "10.1.1.1:4510").
    pub address: String,
    /// Schema labels this node can serve (e.g., ["temperature_sensor", "ahu"]).
    pub schema_labels: Vec<String>,
    /// Role hint for topology awareness.
    pub role: String,
    /// Bloom filter summarizing labels and property keys (serialized bytes).
    /// Peers that lack this field are treated as matching everything.
    #[serde(default)]
    pub bloom_filter: Option<Vec<u8>>,
}

/// Request to list known federation peers.
///
/// The struct is intentionally empty: it exists as a typed request marker
/// consistent with other request/response DTO pairs. Future revisions may
/// add pagination or label-filter fields without breaking the wire format
/// (serde will ignore unknown fields by default).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationPeerListRequest {}

/// Response: list of known peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationPeerListResponse {
    pub peers: Vec<FederationRegisterPayload>,
}

/// Forward a GQL query to a remote peer for execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationGqlRequest {
    /// The GQL query string (already stripped of USE prefix).
    pub query: String,
    /// If true, return JSON instead of Arrow IPC (default: false = Arrow IPC).
    pub json_format: bool,
    /// Forwarded auth scope (RoaringBitmap serialized).
    /// Remote peer validates this is a subset of the connection's scope.
    pub forwarded_scope: Option<Vec<u8>>,
}

/// Response to a forwarded GQL query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationGqlResponse {
    /// Status code (5-digit GQLSTATUS).
    pub status_code: String,
    /// Human-readable message.
    pub message: String,
    /// Row count in result.
    pub row_count: u64,
    /// Arrow IPC serialized result (default).
    pub ipc_bytes: Option<Vec<u8>>,
    /// JSON result (opt-in via json_format flag).
    pub json_result: Option<String>,
    /// Error details if query failed on remote.
    pub error: Option<FederationError>,
}

/// Error from a remote peer query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationError {
    pub code: String,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};

    #[test]
    fn register_round_trip() {
        let payload = FederationRegisterPayload {
            node_name: "building_a".into(),
            address: "10.1.1.1:4510".into(),
            schema_labels: vec!["temperature_sensor".into(), "ahu".into()],
            role: "building".into(),
            bloom_filter: None,
        };

        let bytes = serialize_payload(&payload, WireFlags::empty()).unwrap();
        let decoded: FederationRegisterPayload =
            deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.node_name, "building_a");
        assert_eq!(decoded.schema_labels.len(), 2);
    }

    #[test]
    fn peer_list_round_trip() {
        let resp = FederationPeerListResponse {
            peers: vec![
                FederationRegisterPayload {
                    node_name: "building_a".into(),
                    address: "10.1.1.1:4510".into(),
                    schema_labels: vec!["sensor".into()],
                    role: "building".into(),
                    bloom_filter: None,
                },
                FederationRegisterPayload {
                    node_name: "building_b".into(),
                    address: "10.2.1.1:4510".into(),
                    schema_labels: vec!["ahu".into()],
                    role: "building".into(),
                    bloom_filter: None,
                },
            ],
        };

        let bytes = serialize_payload(&resp, WireFlags::empty()).unwrap();
        let decoded: FederationPeerListResponse =
            deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.peers.len(), 2);
        assert_eq!(decoded.peers[0].node_name, "building_a");
        assert_eq!(decoded.peers[1].node_name, "building_b");
    }

    #[test]
    fn gql_request_round_trip() {
        let req = FederationGqlRequest {
            query: "MATCH (n:sensor) RETURN n.name".into(),
            json_format: false,
            forwarded_scope: Some(vec![1, 2, 3]),
        };
        let bytes = serialize_payload(&req, WireFlags::empty()).unwrap();
        let decoded: FederationGqlRequest =
            deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.query, "MATCH (n:sensor) RETURN n.name");
        assert!(!decoded.json_format);
        assert_eq!(decoded.forwarded_scope, Some(vec![1, 2, 3]));
    }

    #[test]
    fn gql_response_round_trip() {
        let resp = FederationGqlResponse {
            status_code: "00000".into(),
            message: "success".into(),
            row_count: 5,
            ipc_bytes: Some(vec![0xFF, 0xFF]),
            json_result: None,
            error: None,
        };
        let bytes = serialize_payload(&resp, WireFlags::empty()).unwrap();
        let decoded: FederationGqlResponse =
            deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.status_code, "00000");
        assert_eq!(decoded.row_count, 5u64);
        assert!(decoded.ipc_bytes.is_some());
    }
}
