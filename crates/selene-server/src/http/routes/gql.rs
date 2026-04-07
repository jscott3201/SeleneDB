//! GQL query, graph slice, and graph statistics handlers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use selene_core::Value;
use selene_gql::GqlStatement;
use selene_wire::dto::graph_slice::SliceType;
use serde::Deserialize;

use super::{edge_json, node_json};
use crate::bootstrap::ServerState;
use crate::http::auth::HttpAuth;
use crate::http::error::HttpError;
use crate::ops::{self, json_to_value};

// ── GQL ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(in crate::http) struct GqlBody {
    query: String,
    #[serde(default)]
    parameters: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    explain: bool,
    #[serde(default)]
    profile: bool,
    #[serde(default)]
    timeout_ms: Option<u32>,
}

/// Classify a GQL query string as needing the mutation batcher (writes/DDL)
/// or safe for direct read execution. Parses the statement; if parsing fails,
/// routes through the batcher to let the ops layer produce the proper error.
pub(crate) fn is_gql_write(query: &str) -> bool {
    // "USE <graph>" queries route through a separate path in ops::gql and
    // may involve federation -- let those go through the batcher to be safe.
    let trimmed = query.trim_start();
    if trimmed.len() >= 4
        && trimmed.as_bytes()[..3].eq_ignore_ascii_case(b"USE")
        && trimmed.as_bytes()[3] == b' '
    {
        return true;
    }

    match selene_gql::parse_statement(query) {
        Ok(stmt) => !matches!(
            stmt,
            GqlStatement::Query(_) | GqlStatement::Chained { .. } | GqlStatement::Composite { .. }
        ),
        // Parse error -- route through batcher so execute_gql produces proper error response
        Err(_) => true,
    }
}

pub(in crate::http) async fn gql_query(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    headers: axum::http::HeaderMap,
    Json(body): Json<GqlBody>,
) -> axum::response::Response {
    let auth = auth.0;
    let params = body.parameters.as_ref().map(|p| {
        p.iter()
            .map(|(k, v)| (k.clone(), json_to_value(v.clone())))
            .collect::<HashMap<String, Value>>()
    });
    let query = body.query.clone();
    let explain = body.explain;
    let profile = body.profile;
    let timeout_ms = body.timeout_ms;

    // Content negotiation: Arrow IPC for clients that request it.
    let use_arrow = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("application/vnd.apache.arrow.stream"));
    let format = if use_arrow {
        ops::gql::ResultFormat::ArrowIpc
    } else {
        ops::gql::ResultFormat::Json
    };

    // Classify the query: read-only queries bypass the mutation batcher for
    // lower latency (no serialization behind writes). Mutations and DDL
    // statements still go through the batcher to ensure write ordering.
    let needs_batcher = is_gql_write(&query);

    let result = if needs_batcher {
        let st = Arc::clone(&state);
        let batcher_result = state
            .mutation_batcher
            .submit(move || {
                ops::gql::execute_gql_with_timeout(
                    &st,
                    &auth,
                    &query,
                    params.as_ref(),
                    explain,
                    profile,
                    format,
                    timeout_ms,
                )
            })
            .await;

        match batcher_result {
            Ok(r) => r,
            Err(e) => {
                let resp = serde_json::json!({
                    "status": "XX000",
                    "message": e.to_string(),
                    "data": [],
                    "row_count": 0,
                });
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response();
            }
        }
    } else {
        // Read-only: execute directly without batcher serialization
        ops::gql::execute_gql_with_timeout(
            &state,
            &auth,
            &query,
            params.as_ref(),
            explain,
            profile,
            format,
            timeout_ms,
        )
    };

    match result {
        Ok(r) => {
            // Arrow IPC response: return raw bytes with appropriate content type.
            if let Some(arrow_bytes) = r.data_arrow {
                return (
                    StatusCode::OK,
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "application/vnd.apache.arrow.stream",
                    )],
                    arrow_bytes,
                )
                    .into_response();
            }

            let mut resp = serde_json::json!({
                "status": r.status_code,
                "message": r.message,
                "row_count": r.row_count,
            });

            if let Some(json_data) = &r.data_json {
                let data: serde_json::Value =
                    serde_json::from_str(json_data).unwrap_or(serde_json::Value::Array(vec![]));
                resp["data"] = data;
            }

            if let Some(ref mutations) = r.mutations {
                resp["mutations"] = serde_json::json!({
                    "nodes_created": mutations.nodes_created,
                    "nodes_deleted": mutations.nodes_deleted,
                    "edges_created": mutations.edges_created,
                    "edges_deleted": mutations.edges_deleted,
                    "properties_set": mutations.properties_set,
                    "properties_removed": mutations.properties_removed,
                });
            }

            if let Some(ref plan) = r.plan {
                resp["plan"] = serde_json::json!(plan);
            }

            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "GQL infrastructure error (not a query error)");
            let resp = serde_json::json!({
                "status": "XX000",
                "message": e.to_string(),
                "data": [],
                "row_count": 0,
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

// ── Graph Slice ─────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(in crate::http) struct GraphSliceBody {
    #[serde(default = "default_slice_type")]
    slice_type: String,
    labels: Option<Vec<String>>,
    root_id: Option<u64>,
    max_depth: Option<u32>,
    edge_labels: Option<Vec<String>>,
    direction: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

fn default_slice_type() -> String {
    "full".into()
}

pub(in crate::http) async fn graph_slice(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(body): Json<GraphSliceBody>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let slice_type = match body.slice_type.as_str() {
        "full" => SliceType::Full,
        "labels" => SliceType::ByLabels {
            labels: body.labels.unwrap_or_default(),
        },
        "containment" => SliceType::Containment {
            root_id: body.root_id.unwrap_or(1),
            max_depth: body.max_depth,
        },
        "traverse" => SliceType::Traverse {
            root_id: body.root_id.unwrap_or(1),
            edge_labels: body.edge_labels.unwrap_or_default(),
            direction: body.direction.unwrap_or_else(|| "outgoing".into()),
            max_depth: body.max_depth.unwrap_or(3),
        },
        other => {
            return Err(crate::ops::OpError::InvalidRequest(format!(
                "invalid slice_type: '{other}' -- use 'full', 'labels', 'containment', or 'traverse'"
            ))
            .into());
        }
    };
    let result = ops::graph_slice::graph_slice(&state, &auth, &slice_type, body.limit, body.offset);
    let nodes: Vec<serde_json::Value> = result.nodes.iter().map(node_json).collect();
    let edges: Vec<serde_json::Value> = result.edges.iter().map(edge_json).collect();
    let mut resp = serde_json::json!({
        "nodes": nodes,
        "edges": edges,
    });
    if let Some(total_nodes) = result.total_nodes {
        resp["total_nodes"] = serde_json::json!(total_nodes);
        resp["total_edges"] = serde_json::json!(result.total_edges);
    }
    Ok(Json(resp))
}

// ── Graph Statistics ─────────────────────────────────────────────────

pub(in crate::http) async fn graph_stats(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
) -> impl IntoResponse {
    let stats = ops::graph_stats::graph_stats(&state, &auth.0);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "node_count": stats.node_count,
            "edge_count": stats.edge_count,
            "node_labels": stats.node_labels,
            "edge_labels": stats.edge_labels,
        })),
    )
}
