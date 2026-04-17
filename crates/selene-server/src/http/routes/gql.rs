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
    let explain = body.explain;
    let profile = body.profile;
    let timeout_ms = body.timeout_ms;

    let query = body.query.clone();

    // Content negotiation: Arrow IPC for clients that request it.
    let use_arrow = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(accepts_arrow_ipc);
    let format = if use_arrow {
        ops::gql::ResultFormat::ArrowIpc
    } else {
        ops::gql::ResultFormat::Json
    };

    // Multi-statement support: split on semicolons, execute each in sequence.
    if query.contains(';') {
        let statements: Vec<&str> = query
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if statements.len() > 1 {
            return execute_batch(state, auth, &statements, params.as_ref(), format).await;
        }
    }

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
                    "error": true,
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
                    [(axum::http::header::CONTENT_TYPE, ARROW_IPC_MEDIA_TYPE)],
                    arrow_bytes,
                )
                    .into_response();
            }

            // The HTTP status stays 200 even on a GQL-level error so that
            // clients which want to inspect the GQLSTATUS / data envelope
            // never have to read the body for non-2xx responses. To make
            // that decision unambiguous, surface an explicit `error: bool`
            // flag at the envelope root keyed off the GQLSTATUS class so
            // callers do not have to know the SQLSTATE class table.
            let mut resp = serde_json::json!({
                "status": r.status_code,
                "message": r.message,
                "row_count": r.row_count,
                "error": is_gql_error_status(&r.status_code),
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
                "error": true,
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(resp)).into_response()
        }
    }
}

/// Classify a GQLSTATUS / SQLSTATE-style code as an error.
///
/// Following SQL precedent, the `00` class is "successful completion" and
/// `02` is "no data" (still success — just nothing to return). Any other
/// class is an error: `22` data exception, `40` transaction rollback,
/// `42` syntax error, etc.
///
/// Returned at the JSON envelope root so HTTP clients have a stable boolean
/// to dispatch on instead of replicating the SQLSTATE class table.
fn is_gql_error_status(code: &str) -> bool {
    if code.len() < 2 {
        return true; // malformed status — treat conservatively as error
    }
    let class = &code[..2];
    !(class == "00" || class == "02")
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

// ── Multi-statement batch execution ─────────────────────────────────

/// Execute semicolon-separated GQL statements in sequence.
/// Returns a JSON array of results (one per statement).
/// Aborts on first error and returns partial results + error.
async fn execute_batch(
    state: Arc<ServerState>,
    auth: crate::auth::handshake::AuthContext,
    statements: &[&str],
    params: Option<&HashMap<String, Value>>,
    format: ops::gql::ResultFormat,
) -> axum::response::Response {
    let mut results = Vec::with_capacity(statements.len());

    for (i, stmt) in statements.iter().enumerate() {
        let query = (*stmt).to_string();
        let needs_batcher = is_gql_write(&query);

        let result = if needs_batcher {
            let st = Arc::clone(&state);
            let auth2 = auth.clone();
            let params2 = params.cloned();
            let batcher_result = state
                .mutation_batcher
                .submit(move || {
                    ops::gql::execute_gql(
                        &st,
                        &auth2,
                        &query,
                        params2.as_ref(),
                        false,
                        false,
                        format,
                    )
                })
                .await;
            match batcher_result {
                Ok(r) => r,
                Err(e) => {
                    results.push(serde_json::json!({
                        "statement": i,
                        "status": "XX000",
                        "message": e.to_string(),
                        "error": true,
                    }));
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "batch": true,
                            "total": statements.len(),
                            "completed": results.len(),
                            "error": true,
                            "results": results,
                        })),
                    )
                        .into_response();
                }
            }
        } else {
            ops::gql::execute_gql(&state, &auth, &query, params, false, false, format)
        };

        match result {
            Ok(r) => {
                let stmt_error = is_gql_error_status(&r.status_code);
                results.push(serde_json::json!({
                    "statement": i,
                    "status": r.status_code,
                    "message": r.message,
                    "row_count": r.row_count,
                    "data": r.data_json.as_deref().and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()).unwrap_or(serde_json::Value::Array(vec![])),
                    "error": stmt_error,
                }));
                // Abort on error status. The `0A` (feature-not-supported)
                // class is continuable for legacy reasons; everything else
                // the shared classifier flags as an error stops the batch.
                // (Without this alignment, `02xxx` "no data" — which the
                // classifier correctly treats as success — would otherwise
                // abort, contradicting the new contract.)
                if stmt_error && !r.status_code.starts_with("0A") {
                    return (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "batch": true,
                            "total": statements.len(),
                            "completed": results.len(),
                            "error": true,
                            "results": results,
                        })),
                    )
                        .into_response();
                }
            }
            Err(e) => {
                results.push(serde_json::json!({
                    "statement": i,
                    "status": "XX000",
                    "message": e.to_string(),
                    "error": true,
                }));
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "batch": true,
                        "total": statements.len(),
                        "completed": results.len(),
                        "error": true,
                        "results": results,
                    })),
                )
                    .into_response();
            }
        }
    }

    // All statements ran without aborting. The batch as a whole is an
    // error if any individual statement failed (the abort branches above
    // already cover the early-exit cases, but defense-in-depth doesn't hurt).
    let any_error = results.iter().any(|r| {
        r.get("error")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    });
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "batch": true,
            "total": statements.len(),
            "completed": statements.len(),
            "error": any_error,
            "results": results,
        })),
    )
        .into_response()
}

/// Stable media type advertised by the Arrow IPC encoder.
const ARROW_IPC_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

/// Parse an `Accept` header value and report whether the client wants the
/// Arrow IPC stream format.
///
/// The previous implementation used `accept.contains(ARROW_IPC_MEDIA_TYPE)`,
/// which is unsafe in two ways:
///
/// 1. Substrings of malformed types — `text/application/vnd.apache.arrow.stream`
///    — would match.
/// 2. The token could appear inside a parameter value (`text/plain;
///    charset=application/vnd.apache.arrow.stream`) and falsely advertise
///    the format.
///
/// This implementation walks comma-separated entries, drops parameters
/// (everything after the first `;`), trims, and compares for case-insensitive
/// equality against the canonical media type. We intentionally do not honor
/// q-values: if the type is listed at all, we treat it as wanted. Real
/// clients that need q-weighting should request a stricter Accept anyway.
fn accepts_arrow_ipc(accept: &str) -> bool {
    accept.split(',').any(|entry| {
        let media = entry.split(';').next().unwrap_or("").trim();
        media.eq_ignore_ascii_case(ARROW_IPC_MEDIA_TYPE)
    })
}

#[cfg(test)]
mod tests {
    use super::{accepts_arrow_ipc, is_gql_error_status};

    #[test]
    fn success_classes_are_not_errors() {
        // Class 00 is "successful completion". Class 02 is "no data" —
        // still a success (just nothing to return).
        assert!(!is_gql_error_status("00000"));
        assert!(!is_gql_error_status("02000"));
    }

    #[test]
    fn data_and_syntax_errors_are_errors() {
        // 22xxx data exception, 42xxx syntax/access, 40xxx transaction —
        // all error classes per SQLSTATE precedent.
        assert!(is_gql_error_status("22001"));
        assert!(is_gql_error_status("42601"));
        assert!(is_gql_error_status("40001"));
        // The infrastructure-level fallback we use on Err paths.
        assert!(is_gql_error_status("XX000"));
    }

    #[test]
    fn malformed_status_treated_as_error() {
        // Defensive default: a missing or truncated status string must
        // not be silently classified as success.
        assert!(is_gql_error_status(""));
        assert!(is_gql_error_status("0"));
    }

    #[test]
    fn exact_match_is_accepted() {
        assert!(accepts_arrow_ipc("application/vnd.apache.arrow.stream"));
    }

    #[test]
    fn case_insensitive_match() {
        assert!(accepts_arrow_ipc("APPLICATION/VND.APACHE.ARROW.STREAM"));
    }

    #[test]
    fn one_of_many_listed() {
        assert!(accepts_arrow_ipc(
            "text/html, application/vnd.apache.arrow.stream, */*"
        ));
    }

    #[test]
    fn entry_with_q_value_still_matches() {
        // We don't honor q-values for accept/reject decisions, but a q
        // parameter must not break the type lookup.
        assert!(accepts_arrow_ipc(
            "application/vnd.apache.arrow.stream;q=0.9"
        ));
    }

    #[test]
    fn substring_in_invalid_type_is_rejected() {
        // The previous substring check would have matched this. Don't.
        assert!(!accepts_arrow_ipc(
            "text/application/vnd.apache.arrow.stream"
        ));
    }

    #[test]
    fn substring_inside_parameter_is_rejected() {
        // Same hazard, but hidden in a parameter rather than the type.
        assert!(!accepts_arrow_ipc(
            "text/plain; charset=application/vnd.apache.arrow.stream"
        ));
    }

    #[test]
    fn unrelated_types_are_rejected() {
        assert!(!accepts_arrow_ipc("application/json"));
        assert!(!accepts_arrow_ipc("text/html, image/png"));
        assert!(!accepts_arrow_ipc(""));
        assert!(!accepts_arrow_ipc("*/*"));
    }
}
