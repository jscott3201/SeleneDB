//! GQL wire DTOs: request/response types for GQL over SWP.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// GQL query request sent over QUIC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GqlQueryRequest {
    /// GQL query text.
    pub query: String,
    /// Optional query parameters ($param binding).
    #[serde(default)]
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    /// If true, return the execution plan without executing.
    #[serde(default)]
    pub explain: bool,
    /// If true, return execution plan with per-operator timing.
    #[serde(default)]
    pub profile: bool,
    /// Per-query timeout in milliseconds.
    #[serde(default)]
    pub timeout_ms: Option<u32>,
    /// Forwarded scope for federation (bitmap of node IDs).
    #[serde(default)]
    pub forwarded_scope: Option<Vec<u64>>,
}

/// GQL result response sent over QUIC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GqlResultResponse {
    /// 5-digit GQLSTATUS code (e.g. "00000", "42601").
    pub status_code: String,
    /// Human-readable status message.
    pub message: String,
    /// Number of result rows.
    pub row_count: u64,
    /// Mutation statistics (if any mutations executed).
    #[serde(default)]
    pub mutations: Option<MutationStatsDto>,
    /// Execution plan text (if explain/profile requested).
    #[serde(default)]
    pub plan: Option<String>,
    // Result data (Arrow IPC or JSON) is in the frame payload, not here.
}

/// Mutation statistics for GQL responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationStatsDto {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub edges_created: usize,
    pub edges_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
}

/// Status of a single managed service, returned with GQL service management responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub enabled: bool,
    pub running: bool,
    #[serde(default)]
    pub listen_addr: Option<String>,
    pub connections: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gql_request_round_trip_postcard() {
        let req = GqlQueryRequest {
            query: "MATCH (n) RETURN n".to_string(),
            parameters: None,
            explain: false,
            profile: false,
            timeout_ms: Some(5000),
            forwarded_scope: None,
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: GqlQueryRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.query, "MATCH (n) RETURN n");
        assert_eq!(decoded.timeout_ms, Some(5000));
    }

    #[test]
    fn gql_response_round_trip_postcard() {
        let resp = GqlResultResponse {
            status_code: "00000".to_string(),
            message: "success".to_string(),
            row_count: 42,
            mutations: Some(MutationStatsDto {
                nodes_created: 1,
                nodes_deleted: 0,
                edges_created: 2,
                edges_deleted: 0,
                properties_set: 3,
                properties_removed: 0,
            }),
            plan: None,
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: GqlResultResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.status_code, "00000");
        assert_eq!(decoded.row_count, 42);
        assert_eq!(decoded.mutations.unwrap().nodes_created, 1);
    }

    #[test]
    fn gql_request_round_trip_json() {
        let req = GqlQueryRequest {
            query: "MATCH (s:sensor) RETURN s".to_string(),
            parameters: None,
            explain: true,
            profile: false,
            timeout_ms: None,
            forwarded_scope: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: GqlQueryRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.query, req.query);
        assert!(decoded.explain);
    }
}
