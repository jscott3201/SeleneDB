//! Shared business logic — transport-agnostic operations.
//!
//! Both QUIC (SWP) and HTTP (axum) transports call into these functions.
//! Each operation takes typed Rust inputs, checks authorization, performs
//! the graph/TS/query operation, and returns typed results.

pub mod api_keys;
pub mod csv_io;
pub mod edges;
pub mod gql;
pub mod gql_repair;
pub mod graph_resolver;
pub mod graph_slice;
pub mod graph_stats;
pub mod health;
pub mod info;
pub mod nodes;
pub mod principals;
pub mod reactflow;
pub mod schema;
pub mod ts;

use std::time::Instant;

use selene_core::changeset::Change;
use selene_core::{NodeId, Origin, Value};
use selene_graph::{EdgeRef, NodeRef};
use selene_wire::dto::entity::{EdgeDto, NodeDto};

use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

/// Server start time for uptime reporting.
pub(crate) static START_TIME: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Initialize the server start time. Call once at startup.
pub fn init_start_time() {
    START_TIME.get_or_init(Instant::now);
}

/// Transport-agnostic error type.
///
/// QUIC handler maps this to `ErrorResponse` wire codes.
/// HTTP handler maps this to HTTP status codes.
#[derive(Debug, thiserror::Error)]
pub enum OpError {
    #[error("{entity} {id} not found")]
    NotFound { entity: &'static str, id: u64 },

    #[error("access denied")]
    AuthDenied,

    #[error("schema violation: {0}")]
    SchemaViolation(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("query error: {0}")]
    QueryError(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("read-only replica")]
    ReadOnly,

    #[error("resources exhausted: {0}")]
    ResourcesExhausted(String),

    #[error("conflict: {0}")]
    Conflict(String),
}

/// Persist changes via the WAL coalescer, then archive old values for temporal queries.
///
/// Delegates to the coalescer (retry, batching, abort logic). When a version
/// store exists, also archives old property values for point-in-time queries.
pub(crate) fn persist_or_die(state: &ServerState, changes: &[Change]) {
    if state.replica.is_replica {
        return;
    }
    // Record in MergeTracker so LWW is aware of hub-local writes.
    // Generate HLC before submit so record_batch borrows changes first.
    let hlc = state.hlc().new_timestamp().get_time().as_u64();
    state.merge_tracker().lock().record_batch(changes, hlc);
    state
        .persistence
        .wal_coalescer
        .submit(changes, Origin::Local);

    if let Some(vs_svc) = state
        .services
        .get::<crate::version_store::VersionStoreService>()
    {
        let timestamp = selene_core::entity::now_nanos();
        archive_old_values(&vs_svc.store, changes, timestamp);
    }
}

/// Archive old property values into the VersionStore for temporal queries.
pub(crate) fn archive_old_values(
    version_store: &parking_lot::RwLock<crate::version_store::VersionStore>,
    changes: &[Change],
    timestamp_nanos: i64,
) {
    let mut vs = version_store.write();
    for change in changes {
        match change {
            Change::PropertySet {
                node_id,
                key,
                old_value: Some(old),
                ..
            } => {
                vs.archive(*node_id, *key, old.clone(), timestamp_nanos);
            }
            Change::PropertyRemoved {
                node_id,
                key,
                old_value: Some(old),
            } => {
                vs.archive(*node_id, *key, old.clone(), timestamp_nanos);
            }
            _ => {}
        }
    }
}

/// Refresh the auth scope if the containment hierarchy has changed.
///
/// Compares the graph's containment_generation against the auth context's
/// scope_generation. Recomputes scope only when containment edges changed.
pub(crate) fn refresh_scope_if_stale(state: &ServerState, auth: &AuthContext) -> AuthContext {
    if auth.is_admin() {
        return auth.clone();
    }

    let current_gen = state.graph.containment_generation();
    if auth.scope_generation == current_gen {
        return auth.clone();
    }

    tracing::debug!(
        principal = auth.principal_node_id.0,
        old_gen = auth.scope_generation,
        new_gen = current_gen,
        "refreshing stale auth scope"
    );

    state.graph.read(|g| {
        let roots = crate::auth::projection::scope_roots(g, auth.principal_node_id);
        let scope = crate::auth::projection::resolve_scope(g, &roots);
        AuthContext {
            principal_node_id: auth.principal_node_id,
            role: auth.role,
            scope,
            scope_generation: current_gen,
        }
    })
}

/// Check that the target node is within the principal's authorization scope.
pub(crate) fn require_in_scope(auth: &AuthContext, node_id: NodeId) -> Result<(), OpError> {
    if auth.in_scope(node_id) {
        Ok(())
    } else {
        Err(OpError::AuthDenied)
    }
}

/// Convert a `NodeRef` to a wire DTO.
pub(crate) fn node_to_dto(node: NodeRef<'_>) -> NodeDto {
    NodeDto {
        id: node.id.0,
        labels: node.labels.iter().map(|l| l.as_str().to_string()).collect(),
        properties: node
            .properties
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.clone()))
            .collect(),
        created_at: node.created_at,
        updated_at: node.updated_at,
        version: node.version,
    }
}

/// Convert an `EdgeRef` to a wire DTO.
pub(crate) fn edge_to_dto(edge: EdgeRef<'_>) -> EdgeDto {
    EdgeDto {
        id: edge.id.0,
        source: edge.source.0,
        target: edge.target.0,
        label: edge.label.as_str().to_string(),
        properties: edge
            .properties
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.clone()))
            .collect(),
        created_at: edge.created_at,
    }
}

/// Map a `GraphError` to an `OpError`.
pub(crate) fn graph_err(e: selene_graph::GraphError) -> OpError {
    use selene_graph::GraphError;
    match e {
        GraphError::NodeNotFound(id) => OpError::NotFound {
            entity: "node",
            id: id.0,
        },
        GraphError::EdgeNotFound(id) => OpError::NotFound {
            entity: "edge",
            id: id.0,
        },
        GraphError::SchemaViolation(msg) => OpError::SchemaViolation(msg),
        GraphError::AlreadyExists(msg) => OpError::Conflict(msg),
        GraphError::CapacityExceeded(msg) => OpError::ResourcesExhausted(msg),
        other => OpError::Internal(format!("graph error: {other}")),
    }
}

/// Convert a plain JSON value to Selene's Value enum.
/// Accepts: "string", 42, 3.14, true, null — no tagged-enum wrapping needed.
pub(crate) fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::str(&s),
        serde_json::Value::Array(arr) => Value::List(std::sync::Arc::from(
            arr.into_iter().map(json_to_value).collect::<Vec<_>>(),
        )),
        serde_json::Value::Object(_) => Value::str(&v.to_string()),
    }
}

/// Convert a JSON property map to Selene's PropertyMap.
///
/// Returns an error if any key cannot be interned (interner at capacity).
/// Build a PropertyMap from JSON, applying dictionary encoding for properties
/// with `dictionary: true` in the node schema.
pub(crate) fn json_props_with_schema(
    map: std::collections::HashMap<String, serde_json::Value>,
    schema: Option<&selene_core::NodeSchema>,
) -> Result<selene_core::PropertyMap, OpError> {
    let mut pairs = Vec::with_capacity(map.len());
    for (k, v) in map {
        let key = selene_core::try_intern(&k).ok_or_else(|| {
            OpError::InvalidRequest("interner at capacity: too many unique property keys".into())
        })?;
        let mut value = json_to_value(v);
        // Dictionary promotion
        if let selene_core::Value::String(ref s) = value
            && let Some(sch) = schema
            && let Some(prop_def) = sch.properties.iter().find(|p| *p.name == *k)
            && prop_def.dictionary
        {
            value = selene_core::Value::InternedStr(selene_core::IStr::new(s.as_str()));
        }
        pairs.push((key, value));
    }
    Ok(selene_core::PropertyMap::from_pairs(pairs))
}

/// Promote string values to InternedStr for dictionary-encoded properties.
///
/// Call this after building a set of (IStr, Value) pairs to apply dictionary
/// encoding based on the node or edge schema. Works for both node labels
/// (which look up NodeSchema) and edge labels (which look up EdgeSchema).
pub(crate) fn promote_dictionary_values(
    state: &ServerState,
    labels: &[selene_core::IStr],
    props: &mut [(selene_core::IStr, Value)],
) {
    state.graph.read(|g| {
        for (key, value) in props.iter_mut() {
            let s = match value {
                Value::String(s) => s.clone(),
                _ => continue,
            };
            for label in labels {
                if let Some(schema) = g.schema().node_schema(label.as_str())
                    && let Some(prop_def) =
                        schema.properties.iter().find(|p| *p.name == *key.as_str())
                    && prop_def.dictionary
                {
                    *value = Value::InternedStr(selene_core::IStr::new(s.as_str()));
                    break;
                }
                if let Some(schema) = g.schema().edge_schema(label.as_str())
                    && let Some(prop_def) =
                        schema.properties.iter().find(|p| *p.name == *key.as_str())
                    && prop_def.dictionary
                {
                    *value = Value::InternedStr(selene_core::IStr::new(s.as_str()));
                    break;
                }
            }
        }
    });
}

/// Build a PropertyMap from JSON, applying dictionary encoding using the
/// edge schema for the given label.
pub(crate) fn json_props_with_edge_schema(
    map: std::collections::HashMap<String, serde_json::Value>,
    state: &ServerState,
    edge_label: selene_core::IStr,
) -> Result<selene_core::PropertyMap, OpError> {
    let mut pairs = Vec::with_capacity(map.len());
    for (k, v) in map {
        let key = selene_core::try_intern(&k).ok_or_else(|| {
            OpError::InvalidRequest("interner at capacity: too many unique property keys".into())
        })?;
        let mut value = json_to_value(v);
        // Dictionary promotion via edge schema
        if let Value::String(ref s) = value {
            let should_intern = state.graph.read(|g| {
                g.schema()
                    .edge_schema(edge_label.as_str())
                    .and_then(|schema| schema.properties.iter().find(|p| *p.name == *k))
                    .is_some_and(|prop_def| prop_def.dictionary)
            });
            if should_intern {
                value = Value::InternedStr(selene_core::IStr::new(s.as_str()));
            }
        }
        pairs.push((key, value));
    }
    Ok(selene_core::PropertyMap::from_pairs(pairs))
}

/// Convert Selene Value to plain JSON (used by reactflow export and HTTP responses).
pub(crate) fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::UInt(u) => serde_json::json!(u),
        Value::Float(f) => serde_json::json!(f),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::InternedStr(s) => serde_json::Value::String(s.as_str().to_string()),
        Value::Timestamp(t) => serde_json::json!(t),
        Value::Date(d) => serde_json::Value::String(format!("{}", Value::Date(*d))),
        Value::LocalDateTime(n) => {
            serde_json::Value::String(format!("{}", Value::LocalDateTime(*n)))
        }
        Value::Duration(n) => serde_json::Value::String(format!("{}", Value::Duration(*n))),
        Value::Bytes(b) => {
            use std::fmt::Write;
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b.iter() {
                let _ = write!(hex, "{byte:02x}");
            }
            serde_json::Value::String(hex)
        }
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Vector(v) => {
            serde_json::Value::Array(v.iter().map(|f| serde_json::json!(f)).collect())
        }
    }
}
