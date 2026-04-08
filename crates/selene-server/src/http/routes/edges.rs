//! Edge CRUD handlers.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use selene_core::Value;
use serde::Deserialize;

use super::{ListQuery, edge_json, reject_if_replica};
use crate::bootstrap::ServerState;
use crate::http::auth::HttpAuth;
use crate::http::error::HttpError;
use crate::ops::{self, OpError, json_to_value};

// ── Edge CRUD ────────────────────────────────────────────────────────

pub(in crate::http) async fn get_edge(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(id): Path<u64>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let edge = ops::edges::get_edge(&state, &auth, id)?;
    Ok(Json(edge_json(&edge)))
}

#[derive(Deserialize)]
pub(in crate::http) struct CreateEdgeRequest {
    source: u64,
    target: u64,
    label: String,
    #[serde(default)]
    properties: HashMap<String, serde_json::Value>,
}

pub(in crate::http) async fn create_edge(
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
pub(in crate::http) struct ModifyEdgeRequest {
    #[serde(default)]
    set_properties: HashMap<String, serde_json::Value>,
    #[serde(default)]
    remove_properties: Vec<String>,
}

pub(in crate::http) async fn modify_edge(
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

pub(in crate::http) async fn delete_edge(
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

pub(in crate::http) async fn list_edges(
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
