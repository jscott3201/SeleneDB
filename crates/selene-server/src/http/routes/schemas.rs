//! Schema management handlers.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use selene_core::schema::{EdgeSchema, NodeSchema, SchemaPack};

use super::reject_if_replica;
use crate::bootstrap::ServerState;
use crate::http::auth::HttpAuth;
use crate::http::error::HttpError;
use crate::ops;

// ── Schemas ─────────────────────────────────────────────────────────

pub(in crate::http) async fn list_schemas(
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

pub(in crate::http) async fn get_node_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(label): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let schema = ops::schema::get_node_schema(&state, &auth, &label)?;
    Ok(Json(serde_json::json!(schema)))
}

pub(in crate::http) async fn get_edge_schema(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Path(label): Path<String>,
) -> Result<impl IntoResponse, HttpError> {
    let auth = auth.0;
    let schema = ops::schema::get_edge_schema(&state, &auth, &label)?;
    Ok(Json(serde_json::json!(schema)))
}

pub(in crate::http) async fn register_node_schema(
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

pub(in crate::http) async fn register_edge_schema(
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

pub(in crate::http) async fn update_node_schema(
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

pub(in crate::http) async fn delete_node_schema(
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

pub(in crate::http) async fn delete_edge_schema(
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

pub(in crate::http) async fn import_schema_pack(
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
