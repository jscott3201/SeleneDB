//! Axum route handlers — thin JSON ↔ ops adapters.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use selene_core::Value;
use selene_core::schema::{EdgeSchema, NodeSchema, SchemaPack};
use selene_wire::dto::graph_slice::SliceType;

// Re-use shared conversion functions from ops.
use crate::ops::{OpError, json_props_with_schema, json_to_value, value_to_json};

/// Constant-time byte comparison to prevent timing side-channels.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    a.ct_eq(b).into()
}

/// Reject mutations on read-only replicas with 405 Method Not Allowed.
fn reject_if_replica(state: &ServerState) -> Result<(), HttpError> {
    if state.replica.is_replica {
        Err(HttpError(OpError::ReadOnly))
    } else {
        Ok(())
    }
}

/// Convert a NodeDto to plain JSON (properties as plain values, not tagged enums).
fn node_json(node: &selene_wire::dto::entity::NodeDto) -> serde_json::Value {
    let props: serde_json::Map<String, serde_json::Value> = node
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), value_to_json(v)))
        .collect();
    serde_json::json!({
        "id": node.id,
        "labels": node.labels,
        "properties": props,
        "created_at": node.created_at,
        "updated_at": node.updated_at,
        "version": node.version,
    })
}

/// Convert an EdgeDto to plain JSON.
fn edge_json(edge: &selene_wire::dto::entity::EdgeDto) -> serde_json::Value {
    let props: serde_json::Map<String, serde_json::Value> = edge
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), value_to_json(v)))
        .collect();
    serde_json::json!({
        "id": edge.id,
        "source": edge.source,
        "target": edge.target,
        "label": edge.label,
        "properties": props,
        "created_at": edge.created_at,
    })
}

use selene_gql::GqlStatement;
use selene_wire::dto::ts::TsSampleDto;
use serde::Deserialize;

use super::auth::{HttpAuth, OptionalHttpAuth};
use super::error::HttpError;
use crate::bootstrap::ServerState;
use crate::ops;

// ── OpenAPI Spec ────────────────────────────────────────────────────

/// Serve the OpenAPI specification as YAML.
pub(super) async fn openapi_spec() -> impl IntoResponse {
    static SPEC: &str = include_str!("../../openapi.yaml");
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "application/yaml; charset=utf-8",
        )],
        SPEC,
    )
}

// ── API Index ────────────────────────────────────────────────────────

pub(super) async fn api_index() -> impl IntoResponse {
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

pub(super) async fn health(
    State(state): State<Arc<ServerState>>,
    auth: OptionalHttpAuth,
) -> impl IntoResponse {
    let full = ops::health::health(&state);
    match auth.0 {
        Some(_) => {
            // Authenticated caller -- full operational details.
            Json(serde_json::to_value(&full).unwrap_or_else(|_| {
                serde_json::json!({"status": "error", "message": "failed to serialize health response"})
            }))
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

pub(super) async fn ready(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
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

pub(super) async fn server_info(
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

pub(super) async fn fallback_handler(
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

// ── Node CRUD ────────────────────────────────────────────────────────

pub(super) async fn get_node(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let node = ops::nodes::get_node(&state, &auth, id)?;
    Ok(Json(node_json(&node)))
}

#[derive(Deserialize)]
pub(super) struct CreateNodeRequest {
    labels: Vec<String>,
    #[serde(default)]
    properties: HashMap<String, serde_json::Value>,
    #[serde(default)]
    parent_id: Option<u64>,
}

pub(super) async fn create_node(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(req): Json<CreateNodeRequest>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let label_strs: Vec<&str> = req.labels.iter().map(|s| s.as_str()).collect();
    let labels = selene_core::LabelSet::from_strs(&label_strs);
    let schema = {
        let snap = state.graph.load_snapshot();
        let label = req.labels.first().map_or("", |s| s.as_str());
        snap.schema().node_schema(label).cloned()
    };
    let props = json_props_with_schema(req.properties, schema.as_ref())?;
    let parent_id = req.parent_id;
    let st = Arc::clone(&state);
    let node = state
        .mutation_batcher
        .submit(move || ops::nodes::create_node(&st, &auth, labels, props, parent_id))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok((StatusCode::CREATED, Json(node_json(&node))))
}

#[derive(Deserialize)]
pub(super) struct ModifyNodeRequest {
    #[serde(default)]
    set_properties: HashMap<String, serde_json::Value>,
    #[serde(default)]
    remove_properties: Vec<String>,
    #[serde(default)]
    add_labels: Vec<String>,
    #[serde(default)]
    remove_labels: Vec<String>,
}

pub(super) async fn modify_node(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
    Json(req): Json<ModifyNodeRequest>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let mut set_props: Vec<(selene_core::IStr, Value)> = req
        .set_properties
        .into_iter()
        .map(|(k, v)| {
            let key = selene_core::try_intern(&k).ok_or_else(|| {
                OpError::InvalidRequest(
                    "interner at capacity: too many unique property keys".into(),
                )
            })?;
            Ok((key, json_to_value(v)))
        })
        .collect::<Result<Vec<_>, OpError>>()?;
    ops::nodes::prepare_modify_node_props(&state, id, &mut set_props);
    let add_labels: Vec<selene_core::IStr> = req
        .add_labels
        .iter()
        .map(|s| selene_core::IStr::new(s))
        .collect();
    let remove_properties = req.remove_properties;
    let remove_labels = req.remove_labels;
    let st = Arc::clone(&state);
    let node = state
        .mutation_batcher
        .submit(move || {
            ops::nodes::modify_node(
                &st,
                &auth,
                id,
                set_props,
                remove_properties,
                add_labels,
                remove_labels,
            )
        })
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok(Json(node_json(&node)))
}

pub(super) async fn delete_node(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let st = Arc::clone(&state);
    state
        .mutation_batcher
        .submit(move || ops::nodes::delete_node(&st, &auth, id))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub(super) struct NodeEdgesQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

pub(super) async fn node_edges(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
    Query(q): Query<NodeEdgesQuery>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let offset = q.offset.unwrap_or(0);
    let limit = q.limit.unwrap_or(1000).min(10_000);
    let result = ops::edges::node_edges(&state, &auth, id, offset, limit)?;
    let edges: Vec<serde_json::Value> = result.edges.iter().map(edge_json).collect();
    Ok(Json(serde_json::json!({
        "node_id": id,
        "edges": edges,
        "total": result.total,
    })))
}

#[derive(Deserialize)]
pub(super) struct ListQuery {
    label: Option<String>,
    limit: Option<u64>,
    offset: Option<u64>,
}

pub(super) async fn list_nodes(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let result = ops::nodes::list_nodes(
        &state,
        &auth,
        q.label.as_deref(),
        q.limit.unwrap_or(1000).min(10_000) as usize,
        q.offset.unwrap_or(0) as usize,
    )?;
    let nodes: Vec<serde_json::Value> = result.nodes.iter().map(node_json).collect();
    Ok(Json(serde_json::json!({
        "nodes": nodes,
        "total": result.total,
    })))
}

// ── Edge CRUD ────────────────────────────────────────────────────────

pub(super) async fn get_edge(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let edge = ops::edges::get_edge(&state, &auth, id)?;
    Ok(Json(edge_json(&edge)))
}

#[derive(Deserialize)]
pub(super) struct CreateEdgeRequest {
    source: u64,
    target: u64,
    label: String,
    #[serde(default)]
    properties: HashMap<String, serde_json::Value>,
}

pub(super) async fn create_edge(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(req): Json<CreateEdgeRequest>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let label = selene_core::IStr::new(&req.label);
    let props = ops::json_props_with_edge_schema(req.properties, &state, label)?;
    let source = req.source;
    let target = req.target;
    let st = Arc::clone(&state);
    let edge = state
        .mutation_batcher
        .submit(move || ops::edges::create_edge(&st, &auth, source, target, label, props))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok((StatusCode::CREATED, Json(edge_json(&edge))))
}

#[derive(Deserialize)]
pub(super) struct ModifyEdgeRequest {
    #[serde(default)]
    set_properties: HashMap<String, serde_json::Value>,
    #[serde(default)]
    remove_properties: Vec<String>,
}

pub(super) async fn modify_edge(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
    Json(req): Json<ModifyEdgeRequest>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let mut set_props: Vec<(selene_core::IStr, Value)> = req
        .set_properties
        .into_iter()
        .map(|(k, v)| {
            let key = selene_core::try_intern(&k).ok_or_else(|| {
                OpError::InvalidRequest(
                    "interner at capacity: too many unique property keys".into(),
                )
            })?;
            Ok((key, json_to_value(v)))
        })
        .collect::<Result<Vec<_>, OpError>>()?;
    ops::edges::prepare_modify_edge_props(&state, id, &mut set_props);
    let remove_properties = req.remove_properties;
    let st = Arc::clone(&state);
    let edge = state
        .mutation_batcher
        .submit(move || ops::edges::modify_edge(&st, &auth, id, set_props, remove_properties))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok(Json(edge_json(&edge)))
}

pub(super) async fn delete_edge(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let st = Arc::clone(&state);
    state
        .mutation_batcher
        .submit(move || ops::edges::delete_edge(&st, &auth, id))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn list_edges(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(q): Query<ListQuery>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let result = ops::edges::list_edges(
        &state,
        &auth,
        q.label.as_deref(),
        q.limit.unwrap_or(1000).min(10_000) as usize,
        q.offset.unwrap_or(0) as usize,
    )?;
    let edges: Vec<serde_json::Value> = result.edges.iter().map(edge_json).collect();
    Ok(Json(serde_json::json!({
        "edges": edges,
        "total": result.total,
    })))
}

// ── Time-Series ──────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(super) struct TsWriteBody {
    samples: Vec<TsSampleDto>,
}

pub(super) async fn ts_write(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(body): Json<TsWriteBody>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let st = Arc::clone(&state);
    let samples = body.samples;
    let count = state
        .mutation_batcher
        .submit(move || ops::ts::ts_write(&st, &auth, samples))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok(Json(serde_json::json!({ "written": count })))
}

#[derive(Deserialize)]
pub(super) struct TsQueryParams {
    #[serde(default)]
    start: Option<i64>,
    #[serde(default)]
    end: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
}

pub(super) async fn ts_query(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path((entity_id, property)): Path<(u64, String)>,
    Query(params): Query<TsQueryParams>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let start = params.start.unwrap_or(0);
    let end = params.end.unwrap_or(i64::MAX);
    let samples = ops::ts::ts_range(
        &state,
        &auth,
        entity_id,
        &property,
        start,
        end,
        params.limit,
    )?;
    Ok(Json(samples))
}

// ── GQL ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(super) struct GqlBody {
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
    // may involve federation — let those go through the batcher to be safe.
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
        // Parse error — route through batcher so execute_gql produces proper error response
        Err(_) => true,
    }
}

pub(super) async fn gql_query(
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

// ── React Flow ──────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(super) struct RFExportQuery {
    #[serde(default)]
    label: Option<String>,
}

pub(super) async fn reactflow_export(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<RFExportQuery>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let graph = ops::reactflow::export_reactflow(&state, &auth, params.label.as_deref());
    Ok(Json(serde_json::json!(graph)))
}

pub(super) async fn reactflow_import(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(graph): Json<ops::reactflow::RFGraph>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let st = Arc::clone(&state);
    let result = state
        .mutation_batcher
        .submit(move || ops::reactflow::import_reactflow(&st, &auth, graph))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "nodes_created": result.nodes_created,
            "edges_created": result.edges_created,
            "id_map": result.id_map,
        })),
    ))
}

// ── Graph Slice ─────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(super) struct GraphSliceBody {
    #[serde(default = "default_slice_type")]
    slice_type: String,
    labels: Option<Vec<String>>,
    root_id: Option<u64>,
    max_depth: Option<u32>,
    limit: Option<usize>,
    offset: Option<usize>,
}

fn default_slice_type() -> String {
    "full".into()
}

pub(super) async fn graph_slice(
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
        other => {
            return Err(crate::ops::OpError::InvalidRequest(format!(
                "invalid slice_type: '{other}' -- use 'full', 'labels', or 'containment'"
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

// ── Schemas ─────────────────────────────────────────────────────────

pub(super) async fn list_schemas(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let node_schemas = ops::schema::list_node_schemas(&state, &auth)?;
    let edge_schemas = ops::schema::list_edge_schemas(&state, &auth)?;
    Ok(Json(serde_json::json!({
        "node_schemas": node_schemas,
        "edge_schemas": edge_schemas,
    })))
}

pub(super) async fn get_node_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(label): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let schema = ops::schema::get_node_schema(&state, &auth, &label)?;
    Ok(Json(serde_json::json!(schema)))
}

pub(super) async fn get_edge_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(label): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let schema = ops::schema::get_edge_schema(&state, &auth, &label)?;
    Ok(Json(serde_json::json!(schema)))
}

pub(super) async fn register_node_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(schema): Json<NodeSchema>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let label = schema.label.to_string();
    let st = Arc::clone(&state);
    let auth2 = auth.clone();
    state
        .mutation_batcher
        .submit(move || ops::schema::register_node_schema(&st, &auth, schema))
        .await
        .map_err(HttpError::from_graph_error)??;
    let registered = ops::schema::get_node_schema(&state, &auth2, &label)?;
    Ok((StatusCode::CREATED, Json(serde_json::json!(registered))))
}

pub(super) async fn register_edge_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Json(schema): Json<EdgeSchema>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let label = schema.label.to_string();
    let st = Arc::clone(&state);
    let auth2 = auth.clone();
    state
        .mutation_batcher
        .submit(move || ops::schema::register_edge_schema(&st, &auth, schema))
        .await
        .map_err(HttpError::from_graph_error)??;
    let registered = ops::schema::get_edge_schema(&state, &auth2, &label)?;
    Ok((StatusCode::CREATED, Json(serde_json::json!(registered))))
}

pub(super) async fn update_node_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(label): Path<String>,
    Json(mut schema): Json<NodeSchema>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    // Ensure the path label matches the body label
    schema.label = Arc::from(label.as_str());
    let return_label = label.clone();
    let st = Arc::clone(&state);
    let auth2 = auth.clone();
    state
        .mutation_batcher
        .submit(move || ops::schema::register_node_schema_force(&st, &auth, schema))
        .await
        .map_err(HttpError::from_graph_error)??;
    let updated = ops::schema::get_node_schema(&state, &auth2, &return_label)?;
    Ok((StatusCode::OK, Json(serde_json::json!(updated))))
}

pub(super) async fn delete_node_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(label): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let st = Arc::clone(&state);
    state
        .mutation_batcher
        .submit(move || ops::schema::unregister_node_schema(&st, &auth, &label))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn delete_edge_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(label): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;
    let st = Arc::clone(&state);
    state
        .mutation_batcher
        .submit(move || ops::schema::unregister_edge_schema(&st, &auth, &label))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok(StatusCode::NO_CONTENT)
}

pub(super) async fn import_schema_pack(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    body: String,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let auth = auth.0;

    let pack: SchemaPack = selene_packs::load_from_str(&body)
        .map_err(|e| crate::ops::OpError::InvalidRequest(format!("invalid schema pack: {e}")))?;
    let st = Arc::clone(&state);
    let result = state
        .mutation_batcher
        .submit(move || ops::schema::import_pack(&st, &auth, pack))
        .await
        .map_err(HttpError::from_graph_error)??;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "pack": result.pack_name,
            "node_schemas_registered": result.node_schemas_registered, "node_schemas_skipped": result.node_schemas_skipped,
            "edge_schemas_registered": result.edge_schemas_registered, "edge_schemas_skipped": result.edge_schemas_skipped,
        })),
    ))
}

// ── Graph Statistics ─────────────────────────────────────────────────

pub(super) async fn graph_stats(
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

// ── CSV Import/Export ───────────────────────────────────────────────

#[derive(Deserialize)]
pub(super) struct CsvImportParams {
    pub(super) label: Option<String>,
    pub(super) delimiter: Option<String>,
    pub(super) parent_id_column: Option<String>,
    /// "nodes" or "edges"
    #[serde(default = "default_csv_type")]
    pub(super) r#type: String,
    /// For edges: source column name
    pub(super) source_column: Option<String>,
    /// For edges: target column name
    pub(super) target_column: Option<String>,
    /// For edges: label column name
    pub(super) label_column: Option<String>,
}

fn default_csv_type() -> String {
    "nodes".into()
}

pub(super) async fn csv_import(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<CsvImportParams>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let reader = std::io::Cursor::new(body);
    let delimiter = params
        .delimiter
        .as_deref()
        .unwrap_or(",")
        .as_bytes()
        .first()
        .copied()
        .unwrap_or(b',');

    let auth = auth.0;
    let st = Arc::clone(&state);
    let is_edges = params.r#type == "edges";

    let result = if is_edges {
        let config = ops::csv_io::CsvEdgeImportConfig {
            source_column: params.source_column.unwrap_or_else(|| "source_id".into()),
            target_column: params.target_column.unwrap_or_else(|| "target_id".into()),
            label_column: params.label_column.unwrap_or_else(|| "label".into()),
            delimiter,
        };
        state
            .mutation_batcher
            .submit(move || ops::csv_io::import_edges_csv(&st, &auth, reader, &config))
            .await
            .map_err(HttpError::from_graph_error)?
    } else {
        let label = params.label.ok_or_else(|| {
            HttpError::bad_request("label query parameter required for node import")
        })?;
        let config = ops::csv_io::CsvNodeImportConfig {
            label,
            delimiter,
            column_mappings: None,
            parent_id_column: params.parent_id_column,
        };
        state
            .mutation_batcher
            .submit(move || ops::csv_io::import_nodes_csv(&st, &auth, reader, &config))
            .await
            .map_err(HttpError::from_graph_error)?
    };

    match result {
        Ok(r) => Ok((StatusCode::CREATED, Json(serde_json::json!(r)))),
        Err(e) => Err(HttpError::from(e)),
    }
}

#[derive(Deserialize)]
pub(super) struct CsvExportParams {
    pub(super) label: Option<String>,
    /// "nodes" (default) or "edges".
    #[serde(default = "default_csv_type")]
    pub(super) r#type: String,
}

pub(super) async fn csv_export(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<CsvExportParams>,
) -> Result<impl IntoResponse, HttpError> {
    let csv_data = if params.r#type == "edges" {
        ops::csv_io::export_edges_csv(&state, &auth.0, params.label.as_deref())
            .map_err(HttpError::from)?
    } else {
        ops::csv_io::export_nodes_csv(&state, &auth.0, params.label.as_deref())
            .map_err(HttpError::from)?
    };

    Ok((
        StatusCode::OK,
        [("content-type", "text/csv; charset=utf-8")],
        csv_data,
    ))
}

// ── Prometheus Metrics ──────────────────────────────────────────────

pub(super) async fn prometheus_metrics(
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
            Some(token) if constant_time_eq(token.as_bytes(), expected.as_bytes()) => {}
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

// ── RDF Import/Export ──────────────────────────────────────────────

#[derive(Deserialize)]
pub(super) struct RdfExportParams {
    format: Option<String>,
    graphs: Option<String>,
}

pub(super) async fn export_rdf(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<RdfExportParams>,
) -> Result<impl IntoResponse, HttpError> {
    let _ = auth.0;
    let format_str = params.format.as_deref().unwrap_or("turtle");
    let format: selene_rdf::RdfFormat = format_str
        .parse()
        .map_err(|e: String| HttpError::bad_request(e))?;

    let ns = &state.rdf_namespace;
    let snap = state.graph.load_snapshot();
    let include_all = params.graphs.as_deref() == Some("all");

    let ontology_ref = state.rdf_ontology.as_ref().map(|o| o.read());

    let data =
        selene_rdf::export::export_graph(&snap, ns, format, ontology_ref.as_deref(), include_all)
            .map_err(|e| HttpError(OpError::Internal(e.to_string())))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, format.content_type())],
        data,
    ))
}

#[derive(Deserialize)]
pub(super) struct RdfImportParams {
    format: Option<String>,
    graph: Option<String>,
}

pub(super) async fn import_rdf(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<RdfImportParams>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, HttpError> {
    reject_if_replica(&state)?;
    let _ = auth.0;

    let format_str = params.format.as_deref().unwrap_or("turtle");
    let format: selene_rdf::RdfFormat = format_str
        .parse()
        .map_err(|e: String| HttpError::bad_request(e))?;

    let target_graph = params.graph.clone();
    let ns = state.rdf_namespace.clone();

    // Take write lock on ontology for the duration of the import.
    let ontology_arc = state
        .rdf_ontology
        .as_ref()
        .ok_or_else(|| {
            HttpError(OpError::Internal(
                "RDF ontology store not initialized".into(),
            ))
        })?
        .clone();

    let graph = state.graph.clone();
    let st = Arc::clone(&state);

    let result = st
        .mutation_batcher
        .submit(move || {
            let mut ontology = ontology_arc.write();
            selene_rdf::import::import_rdf(
                &body,
                format,
                target_graph.as_deref(),
                &graph,
                &ns,
                &mut ontology,
            )
            .map_err(|e| selene_graph::error::GraphError::Other(e.to_string()))
        })
        .await
        .map_err(HttpError::from_graph_error)?
        .map_err(|e| HttpError(crate::ops::graph_err(e)))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "nodes_created": result.nodes_created,
            "edges_created": result.edges_created,
            "labels_added": result.labels_added,
            "properties_set": result.properties_set,
            "ontology_triples_loaded": result.ontology_triples_loaded,
        })),
    ))
}

// -- SPARQL Protocol endpoint ------------------------------------------------

#[derive(Deserialize)]
pub(super) struct SparqlQueryParams {
    query: Option<String>,
    format: Option<String>,
}

/// `GET /sparql?query=SELECT...&format=json`
pub(super) async fn sparql_get(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<SparqlQueryParams>,
) -> Result<axum::response::Response, HttpError> {
    let _ = auth.0;
    let query_str = params
        .query
        .ok_or_else(|| HttpError::bad_request("missing required 'query' parameter"))?;
    let format_str = params.format.as_deref().unwrap_or("json").to_owned();
    execute_sparql_handler(&state, &query_str, &format_str)
}

/// `POST /sparql` (Content-Type: application/sparql-query, body = SPARQL query string)
pub(super) async fn sparql_post(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(params): Query<SparqlQueryParams>,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, HttpError> {
    let _ = auth.0;
    let query_str = std::str::from_utf8(&body)
        .map_err(|_| HttpError::bad_request("request body is not valid UTF-8"))?;
    if query_str.is_empty() {
        return Err(HttpError::bad_request("empty SPARQL query body"));
    }
    let format_str = params.format.as_deref().unwrap_or("json").to_owned();
    execute_sparql_handler(&state, query_str, &format_str)
}

fn execute_sparql_handler(
    state: &ServerState,
    query_str: &str,
    format_str: &str,
) -> Result<axum::response::Response, HttpError> {
    let format: selene_rdf::sparql::SparqlResultFormat = format_str
        .parse()
        .map_err(|e: String| HttpError::bad_request(e))?;

    let ns = &state.rdf_namespace;
    let snap = state.graph.load_snapshot();
    let csr = crate::bootstrap::get_or_build_csr(&state.csr_cache, &snap);

    let ontology_ref = state.rdf_ontology.as_ref().map(|o| o.read());

    let (bytes, content_type) = selene_rdf::sparql::execute_sparql(
        &snap,
        &csr,
        ns,
        ontology_ref.as_deref(),
        query_str,
        format,
    )
    .map_err(|e| HttpError(OpError::QueryError(e.to_string())))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response())
}
