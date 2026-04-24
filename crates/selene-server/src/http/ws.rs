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
use super::changelog_event::lagged_payload;

/// Global connection counter for enforcing max subscriptions.
static WS_CONNECTIONS: AtomicUsize = AtomicUsize::new(0);

/// How often to refresh the auth scope (seconds). Long-lived WebSocket
/// connections poll `refresh_scope_if_stale` at this cadence; the call is
/// a no-op unless the graph's containment generation has changed, so this
/// only bounds the worst-case staleness for a sustained subscriber.
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
///
/// Atomically reserves a slot under `max_ws_subscriptions` before
/// accepting the upgrade. Closes Selene_Bug_v1 finding #10 (11027): the
/// pre-1.3.0 logic did a separate load → compare → fetch_add, so a burst
/// of concurrent upgrades all passed the load-check and then each
/// incremented past the cap.
pub async fn ws_subscribe(
    ws: WebSocketUpgrade,
    auth: HttpAuth,
    State(state): State<Arc<ServerState>>,
) -> impl IntoResponse {
    let limit = state.config.http.max_ws_subscriptions;

    // Atomic reservation: fetch_update returns the old value; we compute
    // the new value as `n + 1` only when `n < limit`. If every
    // observation of the counter already meets or exceeds the limit the
    // update returns Err and we reject — no other thread observed us
    // holding a slot.
    if WS_CONNECTIONS
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            if n < limit { Some(n + 1) } else { None }
        })
        .is_err()
    {
        tracing::warn!(
            current = WS_CONNECTIONS.load(Ordering::Relaxed),
            limit,
            principal = auth.0.principal_node_id.0,
            "WebSocket subscription rejected: at max_ws_subscriptions limit"
        );
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    // Slot is ours. `handle_ws` is responsible for releasing it on exit.
    ws.on_upgrade(move |socket| handle_ws(socket, auth.0, state))
}

async fn handle_ws(mut socket: WebSocket, auth: AuthContext, state: Arc<ServerState>) {
    // Slot already reserved in `ws_subscribe` under fetch_update — do
    // NOT increment again here. Every code path in this function must
    // release the slot on exit; the `fetch_sub` at the bottom + the
    // early-return `fetch_sub` in the invalid-filter branch together
    // cover that contract.

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
                code: axum::extract::ws::close_code::POLICY,
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
                        // Tell the client *before* we drop the connection. Otherwise
                        // they only see a silent socket close and have no way to know
                        // that events were dropped or to fall back to a snapshot reload.
                        tracing::warn!(
                            lagged = n,
                            principal = auth.principal_node_id.0,
                            "WebSocket subscriber lagged; sending notice and closing"
                        );
                        // Same JSON shape as the SSE `subscriber_lagged` event in
                        // routes/subscribe.rs::lagged_payload — clients should be
                        // able to dispatch on the same discriminator regardless
                        // of transport.
                        let notice = lagged_payload(n);
                        if let Ok(json) = serde_json::to_string(&notice) {
                            let _ = socket.send(Message::Text(json.into())).await;
                        }
                        let _ = socket
                            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                                // RFC 6455 §7.4: 1011 (ERROR) is the closest
                                // match for "the server is unable to fulfill
                                // the request" when the cause is internal
                                // backpressure rather than a protocol violation.
                                // The server itself is healthy; only this
                                // subscription is being dropped due to
                                // changelog overflow.
                                code: axum::extract::ws::close_code::ERROR,
                                reason: "subscriber lagged".into(),
                            })))
                            .await;
                        break;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Closes finding 11027: verify the atomic reservation pattern. Under
    /// concurrency, N threads racing to reserve a slot against a cap of K
    /// must see exactly K successes and (N - K) failures. The pre-fix
    /// `load → compare → fetch_add` pattern could let all N pass the
    /// load-check before any incremented, over-provisioning the cap.
    #[test]
    fn fetch_update_cap_is_strict_under_concurrency() {
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        let counter = Arc::new(AtomicUsize::new(0));
        let limit: usize = 4;
        let attempts: usize = 64;

        let mut handles = Vec::with_capacity(attempts);
        for _ in 0..attempts {
            let c = Arc::clone(&counter);
            handles.push(thread::spawn(move || -> bool {
                c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    if n < limit { Some(n + 1) } else { None }
                })
                .is_ok()
            }));
        }

        let successes: usize = handles
            .into_iter()
            .map(|h| usize::from(h.join().unwrap()))
            .sum();

        assert_eq!(
            successes, limit,
            "expected exactly {limit} reservations to succeed under \
             {attempts}-way contention; got {successes}"
        );
        assert_eq!(counter.load(Ordering::Relaxed), limit);
    }
}
