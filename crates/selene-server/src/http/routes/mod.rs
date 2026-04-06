//! Axum route handlers -- thin JSON/ops adapters.

mod data;
mod edges;
mod gql;
mod nodes;
mod schemas;
pub(super) mod subscribe;
mod system;

use crate::bootstrap::ServerState;
use crate::http::error::HttpError;
use crate::ops::{OpError, value_to_json};
use serde::Deserialize;

// ── Shared helpers ──────────────────────────────────────────────────

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

// ── Shared types ────────────────────────────────────────────────────

#[derive(Deserialize)]
pub(super) struct ListQuery {
    label: Option<String>,
    limit: Option<u64>,
    offset: Option<u64>,
}

// ── Re-exports ──────────────────────────────────────────────────────

// System / operational
pub(super) use system::{
    api_index, fallback_handler, health, openapi_spec, prometheus_metrics, ready, server_info,
};

// Node CRUD
pub(super) use nodes::{create_node, delete_node, get_node, list_nodes, modify_node, node_edges};

// Edge CRUD
pub(super) use edges::{create_edge, delete_edge, get_edge, list_edges, modify_edge};

// GQL, graph slice, graph stats
pub(super) use gql::{gql_query, graph_slice, graph_stats};

// Schema management
pub(super) use schemas::{
    delete_edge_schema, delete_node_schema, get_edge_schema, get_node_schema, import_schema_pack,
    list_schemas, register_edge_schema, register_node_schema, update_node_schema,
};

// Data import/export (CSV, RDF, SPARQL, ReactFlow, time-series)
pub(super) use data::{
    csv_export, csv_import, export_rdf, import_rdf, reactflow_export, reactflow_import, sparql_get,
    sparql_post, ts_query, ts_write,
};

// Cross-module: used by quic::handler and http::mcp::tools
pub(crate) use gql::is_gql_write;
