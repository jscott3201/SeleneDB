//! WebSocket subscription handler — real-time graph change notifications.
//!
//! Clients connect via `GET /ws/subscribe`, send a filter message, then
//! receive JSON change events as they occur. Uses the broadcast channel
//! from `ServerState::changelog_notify` — same mechanism as QUIC subscriptions.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

use super::auth::HttpAuth;

/// Global connection counter for enforcing max subscriptions.
static WS_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

/// Maximum concurrent WebSocket subscriptions.
const MAX_WS_SUBSCRIPTIONS: usize = 100;

/// How often to refresh the auth scope (seconds).
const SCOPE_REFRESH_INTERVAL_SECS: u64 = 60;

/// Maximum number of label or edge-type entries in a subscription filter.
const MAX_FILTER_ENTRIES: usize = 100;

/// Subscription filter sent by the client after connection.
#[derive(Debug, Deserialize, Default)]
pub struct SubscriptionFilter {
    /// Only receive changes for nodes with these labels.
    #[serde(default)]
    pub labels: Option<Vec<String>>,
    /// Only receive changes for edges with these types.
    #[serde(default)]
    pub edge_types: Option<Vec<String>>,
}

impl SubscriptionFilter {
    /// Validate filter sizes to prevent resource exhaustion.
    fn validate(&self) -> Result<(), &'static str> {
        if self
            .labels
            .as_ref()
            .is_some_and(|v| v.len() > MAX_FILTER_ENTRIES)
        {
            return Err("too many label filters (max 100)");
        }
        if self
            .edge_types
            .as_ref()
            .is_some_and(|v| v.len() > MAX_FILTER_ENTRIES)
        {
            return Err("too many edge_type filters (max 100)");
        }
        Ok(())
    }
}

/// A change event sent to the WebSocket client.
#[derive(Debug, Serialize)]
struct ChangeEvent {
    sequence: u64,
    changes: Vec<ChangeEntry>,
}

#[derive(Debug, Serialize)]
struct ChangeEntry {
    #[serde(rename = "type")]
    change_type: String,
    entity_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<Vec<String>>,
}

/// WebSocket upgrade handler.
pub async fn ws_subscribe(
    ws: WebSocketUpgrade,
    auth: HttpAuth,
    State(state): State<Arc<ServerState>>,
) -> impl IntoResponse {
    let current = WS_CONNECTIONS.load(Ordering::Relaxed);
    if current >= MAX_WS_SUBSCRIPTIONS {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    ws.on_upgrade(move |socket| handle_ws(socket, auth.0, state))
}

async fn handle_ws(mut socket: WebSocket, auth: AuthContext, state: Arc<ServerState>) {
    WS_CONNECTIONS.fetch_add(1, Ordering::Relaxed);

    // Wait for the client's filter message (with timeout)
    let filter = match tokio::time::timeout(std::time::Duration::from_secs(10), socket.recv()).await
    {
        Ok(Some(Ok(Message::Text(text)))) => {
            serde_json::from_str::<SubscriptionFilter>(&text).unwrap_or_default()
        }
        _ => SubscriptionFilter::default(),
    };

    // Reject oversized filters to prevent resource exhaustion.
    if let Err(msg) = filter.validate() {
        let _ = socket
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 1008, // Policy Violation
                reason: msg.into(),
            })))
            .await;
        WS_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
        return;
    }

    tracing::info!(
        principal = auth.principal_node_id.0,
        labels = ?filter.labels,
        edge_types = ?filter.edge_types,
        "WebSocket subscription started"
    );

    // Subscribe to the broadcast channel
    let mut broadcast_rx = state.persistence.changelog_notify.subscribe();

    // Mutable auth context for periodic scope refresh
    let mut auth = auth;
    let mut last_scope_refresh = Instant::now();

    loop {
        tokio::select! {
            // Wait for new changelog sequence notification
            result = broadcast_rx.recv() => {
                match result {
                    Ok(seq) => {
                        // Periodic scope refresh — recompute if stale
                        if last_scope_refresh.elapsed().as_secs() >= SCOPE_REFRESH_INTERVAL_SECS {
                            auth = crate::ops::refresh_scope_if_stale(&state, &auth);
                            last_scope_refresh = Instant::now();
                        }

                        // Read changes from the changelog buffer
                        let changes = {
                            let buf = state.persistence.changelog.lock();
                            buf.since(seq.saturating_sub(1))
                        };

                        if let Some(entries) = changes {
                            for entry in entries {
                                let filtered = filter_changes(&auth, &filter, &entry.changes);
                                if filtered.is_empty() { continue; }

                                let event = ChangeEvent {
                                    sequence: entry.sequence,
                                    changes: filtered,
                                };

                                let Ok(json) = serde_json::to_string(&event) else {
                                    continue;
                                };

                                if socket.send(Message::Text(json.into())).await.is_err() {
                                    // Client disconnected
                                    break;
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(lagged = n, "WebSocket subscriber lagged, skipping events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break; // Server shutting down
                    }
                }
            }
            // Check for client close
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {} // Ignore other client messages
                }
            }
        }
    }

    WS_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    tracing::info!(
        principal = auth.principal_node_id.0,
        "WebSocket subscription ended"
    );
}

/// Filter changelog changes by auth scope and subscription filter.
///
/// Label filter: applied to changes that carry label info (LabelAdded/Removed,
/// NodeDeleted, EdgeCreated/Deleted). Changes without label info (NodeCreated,
/// PropertySet/Removed, EdgePropertySet/Removed) pass through — the client
/// sees them regardless of label filter, since we cannot determine label
/// membership from the change alone without a graph lookup.
///
/// Edge type filter: applied to EdgeCreated/Deleted which carry the edge label.
/// EdgePropertySet/Removed pass through since the change does not carry the
/// edge label.
fn filter_changes(
    auth: &AuthContext,
    filter: &SubscriptionFilter,
    changes: &[selene_core::changeset::Change],
) -> Vec<ChangeEntry> {
    use selene_core::changeset::Change;

    let mut result = Vec::new();

    for change in changes {
        match change {
            Change::NodeCreated { node_id } => {
                if !auth.in_scope(*node_id) {
                    continue;
                }
                result.push(ChangeEntry {
                    change_type: "node_created".into(),
                    entity_id: node_id.0,
                    label: None,
                    labels: None,
                });
            }
            Change::NodeDeleted { node_id, labels } => {
                if !auth.in_scope(*node_id) {
                    continue;
                }
                if let Some(ref wanted) = filter.labels
                    && !labels
                        .iter()
                        .any(|l| wanted.iter().any(|w| w.as_str() == l.as_str()))
                {
                    continue;
                }
                result.push(ChangeEntry {
                    change_type: "node_deleted".into(),
                    entity_id: node_id.0,
                    label: None,
                    labels: Some(labels.iter().map(|l| l.to_string()).collect()),
                });
            }
            Change::PropertySet { node_id, .. } | Change::PropertyRemoved { node_id, .. } => {
                if !auth.in_scope(*node_id) {
                    continue;
                }
                result.push(ChangeEntry {
                    change_type: "node_updated".into(),
                    entity_id: node_id.0,
                    label: None,
                    labels: None,
                });
            }
            Change::LabelAdded { node_id, label } | Change::LabelRemoved { node_id, label } => {
                if !auth.in_scope(*node_id) {
                    continue;
                }
                if let Some(ref wanted) = filter.labels
                    && !wanted.iter().any(|w| w.as_str() == label.as_str())
                {
                    continue;
                }
                result.push(ChangeEntry {
                    change_type: "node_updated".into(),
                    entity_id: node_id.0,
                    label: Some(label.to_string()),
                    labels: None,
                });
            }
            Change::EdgeCreated {
                edge_id,
                label,
                source,
                target,
            } => {
                if !auth.in_scope(*source) && !auth.in_scope(*target) {
                    continue;
                }
                if let Some(ref wanted) = filter.edge_types
                    && !wanted.iter().any(|w| w.as_str() == label.as_str())
                {
                    continue;
                }
                result.push(ChangeEntry {
                    change_type: "edge_created".into(),
                    entity_id: edge_id.0,
                    label: Some(label.to_string()),
                    labels: None,
                });
            }
            Change::EdgeDeleted {
                edge_id,
                label,
                source,
                target,
            } => {
                if !auth.in_scope(*source) && !auth.in_scope(*target) {
                    continue;
                }
                if let Some(ref wanted) = filter.edge_types
                    && !wanted.iter().any(|w| w.as_str() == label.as_str())
                {
                    continue;
                }
                result.push(ChangeEntry {
                    change_type: "edge_deleted".into(),
                    entity_id: edge_id.0,
                    label: Some(label.to_string()),
                    labels: None,
                });
            }
            Change::EdgePropertySet {
                edge_id,
                source,
                target,
                ..
            }
            | Change::EdgePropertyRemoved {
                edge_id,
                source,
                target,
                ..
            } => {
                if !auth.in_scope(*source) && !auth.in_scope(*target) {
                    continue;
                }
                result.push(ChangeEntry {
                    change_type: "edge_updated".into(),
                    entity_id: edge_id.0,
                    label: None,
                    labels: None,
                });
            }
        }
    }

    result
}
