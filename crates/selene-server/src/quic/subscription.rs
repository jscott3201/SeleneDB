//! Changelog subscription handler — long-lived bidi stream for delta sync.

use std::sync::Arc;

use bytes::Bytes;
use quinn::{RecvStream, SendStream};
use selene_core::NodeId;
use selene_core::changeset::Change;
use selene_wire::WireFlags;
use selene_wire::dto::changelog::{
    ChangelogAckRequest, ChangelogEventDto, ChangelogSubscribeRequest,
};
use selene_wire::frame::Frame;
use selene_wire::io::read_frame;
use selene_wire::msg_type::MsgType;
use selene_wire::serialize::{deserialize_payload, serialize_payload};

use crate::auth::Role;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

/// Handle a changelog subscription on a dedicated bidi stream.
///
/// This function runs for the lifetime of the subscription — it does not
/// return until the client unsubscribes, the stream errors, or the
/// connection drops.
pub async fn handle_subscription(
    state: Arc<ServerState>,
    auth: Arc<AuthContext>,
    mut send: SendStream,
    mut recv: RecvStream,
    subscribe_frame: Frame,
) -> anyhow::Result<()> {
    let flags = subscribe_frame.flags;

    // Auth check: verify principal has changelog subscribe permission
    if !state
        .auth_engine
        .authorize_action(&auth, crate::auth::engine::Action::ChangelogSubscribe)
    {
        anyhow::bail!("authorization denied for changelog subscription");
    }

    // Parse subscribe request
    let req: ChangelogSubscribeRequest = deserialize_payload(&subscribe_frame.payload, flags)
        .map_err(|e| anyhow::anyhow!("subscribe deserialize: {e}"))?;

    // Check if this peer has a sync subscription filter.
    // Take ownership (remove from map) since this subscriber task owns it.
    // Use the peer_name from the request so multi-peer hubs retrieve the
    // correct filter rather than an arbitrary entry from the map.
    // Discard the filter for push-only subscriptions: pull replication to
    // those peers is unrestricted, so applying the scope filter would
    // incorrectly suppress hub-to-edge changes.
    let mut sync_filter: Option<crate::subscription::SubscriptionFilter> = {
        if let Some(ref peer_name) = req.peer_name {
            let mut filters = state.sync.peer_sync_filters.lock();
            filters.remove(peer_name).and_then(|f| {
                if f.direction() == crate::subscription::SyncDirection::PushOnly {
                    None // Don't filter pull for push-only subscriptions
                } else {
                    Some(f)
                }
            })
        } else {
            None
        }
    };

    // Check if we can serve from the requested sequence
    let catch_up = {
        let buf = state.persistence.changelog.lock();
        buf.since(req.since_sequence)
    };

    match catch_up {
        None => {
            // Sequence evicted — send sync_lost and close
            let event = ChangelogEventDto {
                sequence: 0,
                changes: vec![],
                sync_lost: true,
                hlc_timestamp: 0,
            };
            send_event(&mut send, &event, flags).await?;
            send.finish()?;
            tracing::info!(
                since = req.since_sequence,
                "subscriber sync lost, must re-sync"
            );
            return Ok(());
        }
        Some(entries) => {
            // Send catch-up events
            for entry in entries {
                let mut filtered = filter_changes_by_scope(&auth, &entry.changes);
                if let Some(ref mut sf) = sync_filter {
                    let graph = state.graph.load_snapshot();
                    filtered = sf.filter_changes(&filtered, &graph);
                }
                if !filtered.is_empty() {
                    let event = ChangelogEventDto {
                        sequence: entry.sequence,
                        changes: filtered,
                        sync_lost: false,
                        hlc_timestamp: entry.hlc_timestamp,
                    };
                    send_event(&mut send, &event, flags).await?;
                }
            }
        }
    }

    // Bounded channel for backpressure between broadcast receiver and QUIC sender.
    // If the subscriber is slow, events are dropped rather than accumulating unboundedly.
    const MAX_PENDING_EVENTS: usize = 1000;
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(MAX_PENDING_EVENTS);

    // Subscribe to broadcast for live events
    let mut broadcast_rx = state.persistence.changelog_notify.subscribe();
    let mut last_sent_seq = {
        let buf = state.persistence.changelog.lock();
        buf.current_sequence()
    };

    tracing::info!(
        principal = auth.principal_node_id.0,
        role = %auth.role,
        since = req.since_sequence,
        "changelog subscription started"
    );

    // Sender task — drains the bounded channel and writes to the QUIC stream.
    // This decouples the broadcast receiver (producer) from QUIC send rate (consumer).
    let mut send_handle = send;
    let sender = tokio::spawn(async move {
        while let Some(encoded) = event_rx.recv().await {
            if send_handle.write_all(&encoded).await.is_err() {
                break;
            }
        }
        let _ = send_handle.finish();
    });

    let result: anyhow::Result<()> = async {
        loop {
            tokio::select! {
                // Branch A: new changes available
                result = broadcast_rx.recv() => {
                    match result {
                        Ok(_new_seq) => {
                            let entries = {
                                let buf = state.persistence.changelog.lock();
                                buf.since(last_sent_seq)
                            };

                            if let Some(entries) = entries {
                                for entry in entries {
                                    let mut filtered =
                                        filter_changes_by_scope(&auth, &entry.changes);
                                    if let Some(ref mut sf) = sync_filter {
                                        let graph = state.graph.load_snapshot();
                                        filtered = sf.filter_changes(&filtered, &graph);
                                    }
                                    if !filtered.is_empty() {
                                        let event = ChangelogEventDto {
                                            sequence: entry.sequence,
                                            changes: filtered,
                                            sync_lost: false,
                                            hlc_timestamp: entry.hlc_timestamp,
                                        };
                                        let encoded = encode_event(&event, flags)?;
                                        if event_tx.try_send(encoded).is_err() {
                                            // Channel full — subscriber is too slow, force re-sync
                                            tracing::warn!("subscriber backpressure exceeded, forcing sync_lost");
                                            let sync_lost = ChangelogEventDto {
                                                sequence: 0,
                                                changes: vec![],
                                                sync_lost: true,
                                                hlc_timestamp: 0,
                                            };
                                            let _ = event_tx.send(encode_event(&sync_lost, flags)?).await;
                                            return Ok(());
                                        }
                                    }
                                    last_sent_seq = entry.sequence;
                                }
                            } else {
                                let event = ChangelogEventDto {
                                    sequence: 0,
                                    changes: vec![],
                                    sync_lost: true,
                                    hlc_timestamp: 0,
                                };
                                let _ = event_tx.send(encode_event(&event, flags)?).await;
                                return Ok(());
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::debug!(missed = n, "subscriber lagged, catching up from buffer");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::debug!("broadcast channel closed, ending subscription");
                            return Ok(());
                        }
                    }
                }

                // Branch B: client sends ack or unsubscribe
                result = read_frame(&mut recv) => {
                    if let Ok(frame) = result {
                        match frame.msg_type {
                            MsgType::ChangelogAck => {
                                if let Ok(ack) = deserialize_payload::<ChangelogAckRequest>(&frame.payload, frame.flags) {
                                    tracing::trace!(acked = ack.acked_sequence, "subscriber ack");
                                }
                            }
                            MsgType::ChangelogUnsubscribe => {
                                tracing::info!("subscriber unsubscribed");
                                return Ok(());
                            }
                            _ => {
                                tracing::debug!(msg_type = ?frame.msg_type, "unexpected message on subscription stream");
                            }
                        }
                    } else {
                        tracing::debug!("subscription stream closed");
                        return Ok(());
                    }
                }
            }
        }
    }
    .await;

    // Drop the sender channel to signal the sender task to finish
    drop(event_tx);
    let _ = sender.await;

    // Clean up peer subscription state on subscriber exit.
    if let Some(ref peer_name) = req.peer_name {
        state.sync.peer_subscription_hashes.lock().remove(peer_name);
        // Remove filter if it wasn't already consumed (e.g., sync_lost path).
        state.sync.peer_sync_filters.lock().remove(peer_name);
    }

    result
}

/// Filter a set of changes to only include those within the subscriber's scope.
fn filter_changes_by_scope(auth: &AuthContext, changes: &[Change]) -> Vec<Change> {
    if auth.role == Role::Admin {
        return changes.to_vec();
    }

    changes
        .iter()
        .filter(|change| {
            let node_ids = change_node_ids(change);
            node_ids.iter().any(|id| auth.scope.contains(id.0 as u32))
        })
        .cloned()
        .collect()
}

/// Extract the node IDs affected by a change (for scope filtering).
fn change_node_ids(change: &Change) -> Vec<NodeId> {
    match change {
        Change::NodeCreated { node_id }
        | Change::NodeDeleted { node_id, .. }
        | Change::PropertySet { node_id, .. }
        | Change::PropertyRemoved { node_id, .. }
        | Change::LabelAdded { node_id, .. }
        | Change::LabelRemoved { node_id, .. } => vec![*node_id],
        Change::EdgeCreated { source, target, .. }
        | Change::EdgeDeleted { source, target, .. }
        | Change::EdgePropertySet { source, target, .. }
        | Change::EdgePropertyRemoved { source, target, .. } => vec![*source, *target],
    }
}

fn encode_event(event: &ChangelogEventDto, flags: WireFlags) -> anyhow::Result<Vec<u8>> {
    let payload =
        serialize_payload(event, flags).map_err(|e| anyhow::anyhow!("event serialize: {e}"))?;
    let frame = Frame {
        msg_type: MsgType::ChangelogEvent,
        flags,
        payload: Bytes::from(payload),
    };
    Ok(frame.encode().to_vec())
}

async fn send_event(
    send: &mut SendStream,
    event: &ChangelogEventDto,
    flags: WireFlags,
) -> anyhow::Result<()> {
    let encoded = encode_event(event, flags)?;
    send.write_all(&encoded).await?;
    Ok(())
}

// read_frame imported from selene_wire::io
