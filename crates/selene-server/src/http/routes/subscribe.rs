//! SSE changelog subscription endpoint.
//!
//! Streams graph change events as Server-Sent Events. Supports filtering
//! by node labels, change types, and property keys.
//!
//! GET /subscribe?labels=alarm,fault&changes=NodeCreated,PropertySet&property=current_value

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use selene_core::changeset::Change;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::bootstrap::ServerState;
use crate::http::auth::HttpAuth;
use crate::http::changelog_event::lagged_payload;

/// Query parameters for SSE subscription filtering.
#[derive(serde::Deserialize, Default)]
pub(in crate::http) struct SubscribeQuery {
    /// Comma-separated node labels to filter by.
    #[serde(default)]
    labels: Option<String>,
    /// Comma-separated change types to filter.
    #[serde(default)]
    changes: Option<String>,
    /// Only include events for this property key.
    #[serde(default)]
    property: Option<String>,
}

/// SSE endpoint for streaming graph change events.
///
/// Since 1.3.0 this endpoint enforces the same authorization posture as
/// the WebSocket subscription path:
/// - The caller must have `Action::ChangelogSubscribe` authority (role
///   `service` or `admin` under the default Cedar policies).
/// - Non-admin principals see a scope-filtered stream — events whose
///   subject node is outside the principal's scope bitmap are dropped
///   before serialization so out-of-scope changes cannot leak via SSE.
///
/// Closes Selene_Bug_v1 finding #6 (11023). Pre-1.3.0 the handler took
/// `_auth` and discarded it, so any authenticated principal could observe
/// changelog events for the entire graph regardless of role or scope.
pub(in crate::http) async fn subscribe(
    State(state): State<Arc<ServerState>>,
    auth: HttpAuth,
    Query(q): Query<SubscribeQuery>,
) -> Result<
    Sse<impl futures::stream::Stream<Item = Result<Event, Infallible>>>,
    crate::http::error::HttpError,
> {
    let auth_ctx = auth.0;
    if !state
        .auth_engine
        .authorize_action(&auth_ctx, crate::auth::engine::Action::ChangelogSubscribe)
    {
        return Err(crate::http::error::HttpError(
            crate::ops::OpError::AuthDenied,
        ));
    }

    let scope_bitmap = if auth_ctx.is_admin() {
        None
    } else {
        Some(auth_ctx.scope.clone())
    };
    let rx = state.persistence.changelog_notify.subscribe();

    let label_filter: Vec<String> = q
        .labels
        .as_ref()
        .map(|s| s.split(',').map(|l| l.trim().to_string()).collect())
        .unwrap_or_default();
    let change_filter: Vec<String> = q
        .changes
        .as_ref()
        .map(|s| s.split(',').map(|c| c.trim().to_string()).collect())
        .unwrap_or_default();
    let property_filter = q.property.clone();

    let mut last_seq = state.persistence.changelog.lock().current_sequence();

    let ack_data = serde_json::json!({
        "type": "subscription_ack",
        "filters": {
            "labels": &label_filter,
            "changes": &change_filter,
            "property": &property_filter,
        }
    });
    let ack_event: Result<Event, Infallible> = Ok(Event::default()
        .event("subscription_ack")
        .data(serde_json::to_string(&ack_data).unwrap_or_default()));

    // Surface broadcast lag instead of silently dropping events. Without
    // this branch a slow subscriber would just stop receiving with no
    // signal that anything was missed; that breaks any consumer that
    // relies on changelog continuity for incremental sync.
    let change_stream = BroadcastStream::new(rx).filter_map(move |msg| {
        match msg {
            Ok(_seq) => {
                let entries = state
                    .persistence
                    .changelog
                    .lock()
                    .since(last_seq)
                    .unwrap_or_default();

                if let Some(last) = entries.last() {
                    last_seq = last.sequence;
                }

                let mut events = Vec::new();
                for entry in &entries {
                    for change in &entry.changes {
                        if !change_in_scope(change, scope_bitmap.as_ref()) {
                            continue;
                        }
                        if !matches_filter(
                            change,
                            &label_filter,
                            &change_filter,
                            property_filter.as_ref(),
                            &state,
                        ) {
                            continue;
                        }
                        events.push(change_to_json(change, entry.timestamp_nanos));
                    }
                }

                if events.is_empty() {
                    return None;
                }

                let data = serde_json::to_string(&events).unwrap_or_default();
                Some(Ok(Event::default().data(data)))
            }
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                // Broadcast queue overflowed — repositioned to the latest
                // message. Emit a typed event so the client can decide
                // whether to refetch state or carry on. The stream itself
                // continues; subsequent ticks will deliver the latest
                // changes from `last_seq` forward.
                tracing::warn!(
                    lagged = n,
                    "SSE subscriber lagged; emitting subscriber_lagged event"
                );
                let data = serde_json::to_string(&lagged_payload(n)).unwrap_or_default();
                Some(Ok(Event::default().event("subscriber_lagged").data(data)))
            }
        }
    });

    let stream = futures::StreamExt::chain(
        futures::stream::once(async move { ack_event }),
        change_stream,
    );

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(30))))
}

/// Return true when the change is in the caller's scope (or no scope
/// filter applies, i.e. admin).
///
/// Node events (`NodeCreated`, `NodeDeleted`, `Property*`, `Label*`) are
/// in-scope iff their subject node is in the bitmap.
///
/// Edge events (`EdgeCreated`, `EdgeDeleted`, `EdgeProperty*`) require
/// **both** endpoints in scope. This is the same policy the RDF
/// scope-filtered exporter applies (see
/// `selene_rdf::mapping::graph_to_quads_scoped`) and tightens the
/// WebSocket subscription path to match — pre-1.3.0 the WS variant
/// accepted edges where either endpoint was in scope, which leaked
/// out-of-scope node identifiers via relationship visibility. Aligning
/// the two transports removes the transport-dependent visibility
/// discrepancy.
fn change_in_scope(change: &Change, scope: Option<&roaring::RoaringBitmap>) -> bool {
    let Some(scope) = scope else {
        return true;
    };
    let in_scope = |nid: u64| scope.contains(nid as u32);
    match change {
        Change::NodeCreated { node_id }
        | Change::NodeDeleted { node_id, .. }
        | Change::PropertySet { node_id, .. }
        | Change::PropertyRemoved { node_id, .. }
        | Change::LabelAdded { node_id, .. }
        | Change::LabelRemoved { node_id, .. } => in_scope(node_id.0),
        Change::EdgeCreated { source, target, .. }
        | Change::EdgeDeleted { source, target, .. }
        | Change::EdgePropertySet { source, target, .. }
        | Change::EdgePropertyRemoved { source, target, .. } => {
            in_scope(source.0) && in_scope(target.0)
        }
    }
}

fn matches_filter(
    change: &Change,
    label_filter: &[String],
    change_filter: &[String],
    property_filter: Option<&String>,
    state: &ServerState,
) -> bool {
    let change_type = change_type_name(change);

    if !change_filter.is_empty() && !change_filter.iter().any(|f| f == change_type) {
        return false;
    }

    if let Some(prop) = property_filter {
        match change {
            Change::PropertySet { key, .. } | Change::PropertyRemoved { key, .. } => {
                if key.as_str() != prop.as_str() {
                    return false;
                }
            }
            _ => return false,
        }
    }

    if !label_filter.is_empty()
        && let Some(nid) = change.node_id()
    {
        let has_label = state.graph.read(|g| {
            g.get_node(selene_core::NodeId(nid)).is_some_and(|n| {
                n.labels
                    .iter()
                    .any(|l| label_filter.iter().any(|f| f == l.as_str()))
            })
        });
        if !has_label {
            return false;
        }
    }

    true
}

fn change_type_name(change: &Change) -> &'static str {
    match change {
        Change::NodeCreated { .. } => "NodeCreated",
        Change::NodeDeleted { .. } => "NodeDeleted",
        Change::PropertySet { .. } => "PropertySet",
        Change::PropertyRemoved { .. } => "PropertyRemoved",
        Change::LabelAdded { .. } => "LabelAdded",
        Change::LabelRemoved { .. } => "LabelRemoved",
        Change::EdgeCreated { .. } => "EdgeCreated",
        Change::EdgeDeleted { .. } => "EdgeDeleted",
        Change::EdgePropertySet { .. } => "EdgePropertySet",
        Change::EdgePropertyRemoved { .. } => "EdgePropertyRemoved",
    }
}

fn change_to_json(change: &Change, timestamp_nanos: i64) -> serde_json::Value {
    let ts_ms = timestamp_nanos / 1_000_000;
    match change {
        Change::NodeCreated { node_id } => serde_json::json!({
            "type": "NodeCreated",
            "node_id": node_id.0,
            "timestamp_ms": ts_ms,
        }),
        Change::NodeDeleted { node_id, .. } => serde_json::json!({
            "type": "NodeDeleted",
            "node_id": node_id.0,
            "timestamp_ms": ts_ms,
        }),
        Change::PropertySet {
            node_id,
            key,
            value,
            ..
        } => serde_json::json!({
            "type": "PropertySet",
            "node_id": node_id.0,
            "key": key.as_str(),
            "value": format!("{value:?}"),
            "timestamp_ms": ts_ms,
        }),
        Change::PropertyRemoved { node_id, key, .. } => serde_json::json!({
            "type": "PropertyRemoved",
            "node_id": node_id.0,
            "key": key.as_str(),
            "timestamp_ms": ts_ms,
        }),
        Change::LabelAdded { node_id, label, .. } => serde_json::json!({
            "type": "LabelAdded",
            "node_id": node_id.0,
            "label": label.as_str(),
            "timestamp_ms": ts_ms,
        }),
        Change::LabelRemoved { node_id, label, .. } => serde_json::json!({
            "type": "LabelRemoved",
            "node_id": node_id.0,
            "label": label.as_str(),
            "timestamp_ms": ts_ms,
        }),
        Change::EdgeCreated {
            edge_id,
            source,
            target,
            label,
            ..
        } => serde_json::json!({
            "type": "EdgeCreated",
            "edge_id": edge_id.0,
            "source": source.0,
            "target": target.0,
            "label": label.as_str(),
            "timestamp_ms": ts_ms,
        }),
        Change::EdgeDeleted { edge_id, .. } => serde_json::json!({
            "type": "EdgeDeleted",
            "edge_id": edge_id.0,
            "timestamp_ms": ts_ms,
        }),
        Change::EdgePropertySet {
            edge_id,
            key,
            value,
            ..
        } => serde_json::json!({
            "type": "EdgePropertySet",
            "edge_id": edge_id.0,
            "key": key.as_str(),
            "value": format!("{value:?}"),
            "timestamp_ms": ts_ms,
        }),
        Change::EdgePropertyRemoved { edge_id, key, .. } => serde_json::json!({
            "type": "EdgePropertyRemoved",
            "edge_id": edge_id.0,
            "key": key.as_str(),
            "timestamp_ms": ts_ms,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::changeset::Change;
    use selene_core::{IStr, NodeId, Value};

    fn property_set(node: u64) -> Change {
        Change::PropertySet {
            node_id: NodeId(node),
            key: IStr::new("x"),
            value: Value::Int(1),
            old_value: None,
        }
    }

    #[test]
    fn change_in_scope_admin_passes_everything() {
        assert!(change_in_scope(&property_set(1), None));
        assert!(change_in_scope(&property_set(999), None));
    }

    #[test]
    fn change_in_scope_filters_out_of_scope_nodes() {
        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(1);
        scope.insert(2);
        assert!(change_in_scope(&property_set(1), Some(&scope)));
        assert!(change_in_scope(&property_set(2), Some(&scope)));
        assert!(!change_in_scope(&property_set(3), Some(&scope)));
    }

    #[test]
    fn change_in_scope_node_created() {
        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(42);
        let inside = Change::NodeCreated {
            node_id: NodeId(42),
        };
        let outside = Change::NodeCreated { node_id: NodeId(7) };
        assert!(change_in_scope(&inside, Some(&scope)));
        assert!(!change_in_scope(&outside, Some(&scope)));
    }

    #[test]
    fn change_in_scope_edge_requires_both_endpoints() {
        // 1.3.0 policy: an edge event is in-scope iff BOTH endpoints
        // are in the bitmap. Pre-1.3.0 WS accepted "either endpoint"
        // and SSE accepted "source only"; both transports were
        // tightened and aligned to this stricter rule (matches the
        // RDF scope-filtered exporter).
        let mut scope = roaring::RoaringBitmap::new();
        scope.insert(10);
        scope.insert(20);
        let both_in = Change::EdgeCreated {
            edge_id: selene_core::EdgeId(1),
            source: NodeId(10),
            target: NodeId(20),
            label: IStr::new("rel"),
        };
        let source_in_only = Change::EdgeCreated {
            edge_id: selene_core::EdgeId(2),
            source: NodeId(10),
            target: NodeId(99),
            label: IStr::new("rel"),
        };
        let target_in_only = Change::EdgeCreated {
            edge_id: selene_core::EdgeId(3),
            source: NodeId(99),
            target: NodeId(10),
            label: IStr::new("rel"),
        };
        let neither = Change::EdgeCreated {
            edge_id: selene_core::EdgeId(4),
            source: NodeId(98),
            target: NodeId(99),
            label: IStr::new("rel"),
        };
        assert!(change_in_scope(&both_in, Some(&scope)));
        assert!(!change_in_scope(&source_in_only, Some(&scope)));
        assert!(!change_in_scope(&target_in_only, Some(&scope)));
        assert!(!change_in_scope(&neither, Some(&scope)));
    }
}
