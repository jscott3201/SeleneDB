//! Service DTOs: health, server info, handshake.

use serde::{Deserialize, Serialize};

/// Response: health check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_count: u64,
    pub edge_count: u64,
    pub uptime_secs: u64,
    #[serde(default)]
    pub dev_mode: bool,
    /// Node role: "primary" or "replica".
    pub role: String,
    /// Address of the primary (only set on replicas).
    pub primary: Option<String>,
    /// Number of changelog sequences behind the primary (only set on replicas).
    pub lag_sequences: Option<u64>,
}

/// Request: authenticate a connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    /// Auth type: "token", "psk", "dev"
    pub auth_type: String,
    /// Principal identity (username) for lookup.
    #[serde(default)]
    pub identity: String,
    /// Secret credential for verification (token, key, or dev-mode string).
    pub credentials: String,
}

/// Response: successful authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    /// The principal node's ID.
    pub principal_id: u64,
    /// The principal's role.
    pub role: String,
    /// The containment root node IDs this principal is scoped to.
    pub scope_root_ids: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};

    #[test]
    fn health_response_round_trip() {
        let resp = HealthResponse {
            status: "ok".into(),
            node_count: 10_000,
            edge_count: 25_000,
            uptime_secs: 3600,
            dev_mode: true,
            role: "primary".into(),
            primary: None,
            lag_sequences: None,
        };

        let bytes = serialize_payload(&resp, WireFlags::empty()).unwrap();
        let decoded: HealthResponse = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.node_count, 10_000);
    }
}
