//! Node CRUD handlers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use selene_core::Value;
use serde::Deserialize;

use super::{ListQuery, node_json, reject_if_replica};
use crate::bootstrap::ServerState;
use crate::http::auth::HttpAuth;
use crate::http::error::HttpError;
use crate::ops::{self, OpError, json_props_with_schema, json_to_value};

// ── Node CRUD ────────────────────────────────────────────────────────

pub(in crate::http) async fn get_node(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let node = ops::nodes::get_node(&state, &auth, id)?;
    Ok(Json(node_json(&node)))
}

#[derive(Deserialize)]
pub(in crate::http) struct CreateNodeRequest {
    labels: Vec<String>,
    #[serde(default)]
    properties: HashMap<String, serde_json::Value>,
    #[serde(default)]
    parent_id: Option<u64>,
}

pub(in crate::http) async fn create_node(
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
pub(in crate::http) struct ModifyNodeRequest {
    #[serde(default)]
    set_properties: HashMap<String, serde_json::Value>,
    #[serde(default)]
    remove_properties: Vec<String>,
    #[serde(default)]
    add_labels: Vec<String>,
    #[serde(default)]
    remove_labels: Vec<String>,
}

pub(in crate::http) async fn modify_node(
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

pub(in crate::http) async fn delete_node(
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
pub(in crate::http) struct NodeEdgesQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

pub(in crate::http) async fn node_edges(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
    Query(q): Query<NodeEdgesQuery>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let offset = q.offset.unwrap_or(0);
    let limit = q.limit.unwrap_or(1000).min(10_000);
    let result = ops::edges::node_edges(&state, &auth, id, offset, limit)?;
    let edges: Vec<serde_json::Value> = result.edges.iter().map(super::edge_json).collect();
    Ok(Json(serde_json::json!({
        "node_id": id,
        "edges": edges,
        "total": result.total,
    })))
}

pub(in crate::http) async fn list_nodes(
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
