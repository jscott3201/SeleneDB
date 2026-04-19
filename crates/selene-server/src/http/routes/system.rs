//! System and operational handlers: health, readiness, info, metrics, OpenAPI.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};

use crate::bootstrap::ServerState;
use crate::http::auth::OptionalHttpAuth;
use crate::ops;

// ── OpenAPI Spec ────────────────────────────────────────────────────

/// Serve the OpenAPI specification as YAML.
pub(in crate::http) async fn openapi_spec() -> impl IntoResponse {
    static SPEC: &str = include_str!("../../../openapi.yaml");
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/yaml; charset=utf-8",
        )],
        SPEC,
    )
}

// ── API Index ────────────────────────────────────────────────────────

pub(in crate::http) async fn api_index() -> impl IntoResponse {
    Json(serde_json::json!({
        "name": "Selene",
        "description": "Lightweight in-memory property graph runtime — domain-agnostic",
        "endpoints": {
            "health": "GET /health",
            "nodes": {
                "list": "GET /nodes?label=&limit=&offset=",
                "create": "POST /nodes {labels, properties, parent_id?}",
                "get": "GET /nodes/:id",
                "edges": "GET /nodes/:id/edges — all edges connected to this node",
                "modify": "PUT /nodes/:id {set_properties?, remove_properties?, add_labels?, remove_labels?}",
                "delete": "DELETE /nodes/:id — also deletes connected edges"
            },
            "edges": {
                "list": "GET /edges?label=&limit=&offset=",
                "create": "POST /edges {source, target, label, properties?}",
                "get": "GET /edges/:id",
                "modify": "PUT /edges/:id {set_properties?, remove_properties?}",
                "delete": "DELETE /edges/:id"
            },
            "time_series": {
                "write": "POST /ts/samples {samples: [{entity_id, property, timestamp_nanos, value}]}",
                "write_alias": "POST /ts/write (deprecated alias)",
                "query": "GET /ts/:entity_id/:property?start=&end=&limit=",
                "note": "timestamp_nanos is nanoseconds since Unix epoch."
            },
            "gql": {
                "endpoint": "POST /gql {query, parameters?, explain?, profile?}",
                "example": "MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name AS name",
                "note": "Primary query interface. Returns GQLSTATUS in response body."
            },
            "graph_slice": {
                "endpoint": "POST /graph/slice",
                "types": {
                    "full": "{\"slice_type\": \"full\", \"limit\": 100, \"offset\": 0}",
                    "labels": "{\"slice_type\": \"labels\", \"labels\": [\"sensor\", \"building\"]}",
                    "containment": "{\"slice_type\": \"containment\", \"root_id\": 1, \"max_depth\": 5}"
                }
            },
            "reactflow": {
                "export": "GET /graph/reactflow?label= — export graph as React Flow nodes/edges",
                "import": "POST /graph/reactflow {nodes, edges} — import React Flow graph",
                "docs": "https://reactflow.dev/api-reference"
            },
            "rdf": {
                "export": "GET /graph/rdf?format=turtle|ntriples|nquads&graphs=all",
                "import": "POST /graph/rdf?format=turtle&graph=ontology",
                "note": "Requires --features rdf. format defaults to turtle."
            },
            "schemas": {
                "list": "GET /schemas",
                "register_node": "POST /schemas/nodes",
                "register_edge": "POST /schemas/edges",
                "get_node": "GET /schemas/nodes/:label",
                "get_edge": "GET /schemas/edges/:label",
                "delete_node": "DELETE /schemas/nodes/:label",
                "delete_edge": "DELETE /schemas/edges/:label",
                "import_pack": "POST /schemas/import (TOML body)",
                "example_node_schema": {
                    "label": "my_sensor",
                    "description": "A custom sensor type",
                    "properties": [{"name": "unit", "value_type": "String", "required": false, "default": null, "description": "Unit of measure", "indexed": false}],
                    "valid_edge_labels": [],
                    "annotations": {}
                }
            }
        },
        "notes": {
            "properties": "Send plain JSON values: {\"name\": \"HQ\", \"floor\": 3}. Nested objects are stored as JSON strings — use flat key-value pairs.",
            "containment": "Set parent_id on node create to auto-create a 'contains' edge",
            "timestamps": "Time-series timestamps are nanoseconds since Unix epoch."
        }
    }))
}

// ── Health ───────────────────────────────────────────────────────────

pub(in crate::http) async fn health(
    State(state): State<Arc<ServerState>>,
    auth: OptionalHttpAuth,
) -> impl IntoResponse {
    let full = ops::health::health(&state);
    match auth.0 {
        Some(_) => {
            // Authenticated caller -- full operational details.
            let value = serde_json::to_value(&full).unwrap_or_else(|_| {
                serde_json::json!({"status": "error", "message": "failed to serialize health response"})
            });
            Json(value)
        }
        None => {
            // Unauthenticated caller (e.g., load balancer probe) -- minimal response.
            Json(serde_json::json!({
                "status": full.status,
                "uptime_secs": full.uptime_secs,
            }))
        }
    }
}

// ── Readiness Probe ────────────────────────────────────────────────

pub(in crate::http) async fn ready(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    if state.is_ready() {
        (StatusCode::OK, Json(serde_json::json!({"status": "ready"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status": "starting"})),
        )
            .into_response()
    }
}

// ── Server Info ─────────────────────────────────────────────────────

pub(in crate::http) async fn server_info(
    State(state): State<Arc<ServerState>>,
    auth: OptionalHttpAuth,
) -> impl IntoResponse {
    let full = ops::info::server_info(&state);
    match auth.0 {
        Some(_) => {
            // Authenticated caller -- full server info.
            Json(full)
        }
        None => {
            // Unauthenticated caller -- name and version only.
            Json(serde_json::json!({
                "name": full.get("name").cloned().unwrap_or_default(),
                "version": full.get("version").cloned().unwrap_or_default(),
            }))
        }
    }
}

// ── Fallback ─────────────────────────────────────────────────────────

pub(in crate::http) async fn fallback_handler(
    req: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": format!("{method} {path} not found"),
            "hint": "GET / for API index"
        })),
    )
}

// ── Prometheus Metrics ──────────────────────────────────────────────

pub(in crate::http) async fn prometheus_metrics(
    State(state): State<Arc<ServerState>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    // If a metrics_token is configured, require a matching Bearer token.
    if let Some(ref expected) = state.config.http.metrics_token {
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "));

        match provided {
            Some(token) if super::constant_time_eq(token.as_bytes(), expected.as_bytes()) => {}
            _ => {
                return (
                    StatusCode::UNAUTHORIZED,
                    [("content-type", "application/json")],
                    r#"{"error":"missing or invalid Bearer token for /metrics"}"#.to_string(),
                )
                    .into_response();
            }
        }
    }

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        crate::metrics::render(),
    )
        .into_response()
}
