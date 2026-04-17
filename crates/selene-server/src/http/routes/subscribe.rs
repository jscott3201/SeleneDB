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
pub(in crate::http) async fn subscribe(
    State(state): State<Arc<ServerState>>,
    _auth: HttpAuth,
    Query(q): Query<SubscribeQuery>,
) -> Sse<impl futures::stream::Stream<Item = Result<Event, Infallible>>> {
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

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(30)))
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

/// JSON body of the `subscriber_lagged` SSE event. Pulled out so the wire
/// shape is unit-testable independent of the full streaming pipeline.
fn lagged_payload(dropped_count: u64) -> serde_json::Value {
    serde_json::json!({
        "type": "subscriber_lagged",
        "dropped_count": dropped_count,
        "hint": "the changelog queue overflowed; fetch a fresh snapshot if you require strict continuity",
    })
}

#[cfg(test)]
mod tests {
    use super::lagged_payload;

    #[test]
    fn lagged_payload_carries_typed_kind_and_count() {
        // Clients dispatch on the `type` discriminator and report
        // `dropped_count` to the user; both must be present and stable.
        let p = lagged_payload(7);
        assert_eq!(p["type"], "subscriber_lagged");
        assert_eq!(p["dropped_count"], 7);
        assert!(
            p["hint"].as_str().unwrap().contains("snapshot"),
            "hint should suggest snapshot recovery"
        );
    }

    #[test]
    fn lagged_payload_handles_zero_drop_count() {
        // Edge case: BroadcastStreamRecvError::Lagged(0) is technically
        // possible if the channel underflows; payload should still be
        // well-formed.
        let p = lagged_payload(0);
        assert_eq!(p["dropped_count"], 0);
    }
}
