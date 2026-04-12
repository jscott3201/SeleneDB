//! Sync state machine for bidirectional hub-spoke synchronization.
//!
//! Orchestrates the Offline/Syncing/Live state transitions for an edge
//! node. On startup:
//! 1. Connect to the upstream hub via QUIC.
//! 2. Push buffered local changes (WAL entries since last cursor).
//! 3. Subscribe to the hub's changelog for pull (hub changes applied locally).
//! 4. Enter live bidirectional streaming: push local changes as they
//!    occur, pull hub changes as they arrive.
//!
//! On connection failure the task backs off by `reconnect_delay_secs`
//! and retries from step 1.

use std::sync::Arc;
use std::time::Duration;

use selene_core::changeset::Change;
use selene_graph::change_applier::apply_changes;
use selene_graph::changelog::ChangelogEntry;
use selene_persist::sync_cursor::SyncCursor;
use tokio_util::sync::CancellationToken;

use crate::bootstrap::ServerState;
use crate::replica::apply_entry_to_replica;
use crate::sync_push::push_buffered_changes;

/// Run the sync loop for the lifetime of the server.
///
/// Returns only on cancellation. If sync is not enabled in the
/// configuration, returns immediately.
#[tracing::instrument(skip_all, fields(upstream = %state.config.sync.upstream, peer = %state.config.sync.peer_name))]
pub async fn run_sync_loop(state: Arc<ServerState>, cancel: CancellationToken) {
    if !state.config.sync.is_enabled() {
        return;
    }

    let upstream = state.config.sync.upstream.clone();
    let reconnect_delay = Duration::from_secs(state.config.sync.reconnect_delay_secs.max(1));

    tracing::info!(
        upstream = %upstream,
        peer = %state.config.sync.peer_name,
        "sync task started"
    );

    loop {
        match connect_and_sync(&state, &cancel).await {
            Ok(()) => {
                // Cancelled cleanly.
                tracing::info!("sync task stopped (cancelled)");
                return;
            }
            Err(e) => {
                if cancel.is_cancelled() {
                    tracing::info!("sync task stopped (cancelled during session)");
                    return;
                }
                tracing::warn!(error = %e, "sync session failed, reconnecting after backoff");
                tokio::select! {
                    _ = tokio::time::sleep(reconnect_delay) => {}
                    _ = cancel.cancelled() => {
                        tracing::info!("sync task stopped (cancelled during backoff)");
                        return;
                    }
                }
            }
        }
    }
}

/// Run a single sync session: connect, push buffered changes, then
/// enter the live bidirectional loop.
#[tracing::instrument(skip_all)]
async fn connect_and_sync(
    state: &Arc<ServerState>,
    cancel: &CancellationToken,
) -> Result<(), anyhow::Error> {
    // 1. Load or create the sync cursor.
    let mut cursor = SyncCursor::load(&state.config.data_dir)?
        .unwrap_or_else(|| SyncCursor::new(state.config.sync.peer_name.clone()));

    // 2. Build the QUIC client connection (same pattern as replica.rs).
    let client = build_client(state).await?;
    tracing::info!("connected to upstream hub");

    // 2b. Subscription handshake (if subscriptions configured).
    let mut subscription_filter = if state.config.sync.subscriptions.is_empty() {
        None
    } else {
        // Merge all configured subscriptions into a single wire config
        // by combining all rules (union). Direction from first subscription.
        let first = state.config.sync.subscriptions[0].to_wire_config();
        let mut merged_rules = first.rules;
        for sub in &state.config.sync.subscriptions[1..] {
            merged_rules.extend(sub.to_wire_config().rules);
        }
        let wire_config = selene_wire::dto::sync::SubscriptionConfig {
            name: first.name,
            rules: merged_rules,
            direction: first.direction,
        };

        let request = selene_wire::dto::sync::SyncSubscribeRequest {
            peer_name: state.config.sync.peer_name.clone(),
            subscription: wire_config.clone(),
            last_pulled_seq: cursor.last_pulled_seq,
        };

        let response = client
            .sync_subscribe(request)
            .await
            .map_err(|e| anyhow::anyhow!("sync subscribe: {e}"))?;

        // Deserialize scope bitmap.
        let bitmap = roaring::RoaringBitmap::deserialize_from(&response.scope_bitmap[..])
            .map_err(|e| anyhow::anyhow!("bitmap deserialize: {e}"))?;

        // Apply filtered snapshot if provided.
        if let Some(snapshot_changes) = response.snapshot {
            tracing::info!(
                changes = snapshot_changes.len(),
                scope_nodes = bitmap.len(),
                "applying filtered snapshot"
            );

            {
                let inner = state.graph.inner();
                let mut guard = inner.write();
                apply_changes(
                    &mut guard,
                    &snapshot_changes,
                    selene_core::Origin::Replicated,
                );
                let snapshot = Arc::new(guard.clone());
                drop(guard);
                state.graph.publish_snapshot_arc(snapshot);
            }

            // Persist to WAL with Replicated origin so the push task skips
            // these entries and avoids echo loops back to the hub.
            state
                .persistence
                .wal_coalescer
                .submit_wal_only(&snapshot_changes, selene_core::Origin::Replicated);
        }

        // Advance cursor to hub's changelog position to avoid re-pulling
        // changes already reflected in the snapshot.
        if response.changelog_seq > cursor.last_pulled_seq {
            cursor.last_pulled_seq = response.changelog_seq;
            cursor.save(&state.config.data_dir)?;
        }

        // Build the filter for push/pull filtering.
        let def = crate::subscription::SubscriptionDef::compile(&wire_config);
        let filter = crate::subscription::SubscriptionFilter::new(&def, bitmap);

        tracing::info!(
            subscription = %def.name,
            scope_nodes = filter.scope_bitmap().len(),
            "subscription handshake complete"
        );

        Some(filter)
    };

    // 3. Push phase: drain any buffered local changes.
    let pushed =
        push_buffered_changes(state, &client, &mut cursor, subscription_filter.as_mut()).await?;
    if pushed > 0 {
        tracing::info!(pushed, "push phase complete");
    }

    // 4. Subscribe to the hub's changelog for the pull phase.
    // Pass the peer name so the hub looks up the correct per-peer filter.
    let mut changelog_sub = client
        .subscribe_changelog(
            cursor.last_pulled_seq,
            Some(state.config.sync.peer_name.clone()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("changelog subscribe: {e}"))?;

    tracing::info!(
        since_seq = cursor.last_pulled_seq,
        "pull phase: subscribed to hub changelog"
    );

    // 5. Live bidirectional loop.
    let push_interval = Duration::from_millis(state.config.sync.push_interval_ms.max(50));
    let mut push_tick = tokio::time::interval(push_interval);
    push_tick.tick().await; // skip the immediate first tick

    let mut pull_count: u64 = 0;

    let result: Result<(), anyhow::Error> = loop {
        tokio::select! {
            // Pull: incoming hub changelog events.
            event = changelog_sub.next_event() => {
                let Some(event) = event else {
                    break Err(anyhow::anyhow!("hub changelog stream closed"));
                };

                if event.sync_lost {
                    break Err(anyhow::anyhow!("sync lost, must reconnect"));
                }

                // Apply the pulled change to the local graph.
                let entry = ChangelogEntry {
                    sequence: event.sequence,
                    timestamp_nanos: selene_core::entity::now_nanos(),
                    hlc_timestamp: event.hlc_timestamp,
                    changes: event.changes,
                };

                apply_entry_to_replica(state, &entry);

                // Update push filter bitmap with pulled changes so
                // locally-created edges to hub-originated nodes are included.
                if let Some(ref mut filter) = subscription_filter {
                    let graph = state.graph.load_snapshot();
                    for change in &entry.changes {
                        match change {
                            Change::LabelAdded { node_id, .. }
                            | Change::PropertySet { node_id, .. } => {
                                if !filter.scope_bitmap().contains(node_id.0 as u32)
                                    && filter.evaluate_node(&graph, *node_id)
                                {
                                    filter.add_to_scope(node_id.0 as u32);
                                }
                            }
                            Change::NodeDeleted { node_id, .. } => {
                                filter.remove_from_scope(node_id.0 as u32);
                            }
                            _ => {}
                        }
                    }
                }

                // Update cursor; persist every 100 events to reduce I/O.
                cursor.last_pulled_seq = event.sequence;
                pull_count += 1;
                if pull_count.is_multiple_of(100) {
                    cursor.save(&state.config.data_dir)?;
                }

                // Ack to the hub.
                let _ = changelog_sub.ack(event.sequence).await;

                tracing::debug!(seq = event.sequence, "pulled change from hub");
            }

            // Push: periodic flush of local changes to the hub.
            _ = push_tick.tick() => {
                match push_buffered_changes(state, &client, &mut cursor, subscription_filter.as_mut()).await {
                    Ok(0) => {} // nothing to push
                    Ok(n) => {
                        tracing::debug!(pushed = n, "live push batch sent");
                    }
                    Err(e) => {
                        break Err(anyhow::anyhow!("live push failed: {e}"));
                    }
                }
            }

            // Cancellation.
            _ = cancel.cancelled() => {
                break Ok(());
            }
        }
    };

    // Save cursor on loop exit to capture any un-flushed progress.
    let _ = cursor.save(&state.config.data_dir);

    result
}

/// Build a QUIC client connection to the upstream hub.
///
/// Follows the same TLS/auth pattern as `replica.rs`.
async fn build_client(state: &ServerState) -> Result<selene_client::SeleneClient, anyhow::Error> {
    let addr: std::net::SocketAddr = state.config.sync.upstream.parse().map_err(|e| {
        anyhow::anyhow!(
            "invalid upstream address '{}': {e}",
            state.config.sync.upstream
        )
    })?;

    let auth = if state.config.dev_mode {
        Some(selene_client::AuthCredentials {
            auth_type: "dev".into(),
            identity: state.config.sync.peer_name.clone(),
            credentials: "dev".into(),
        })
    } else {
        match (
            &state.config.sync.auth_identity,
            &state.config.sync.auth_credentials,
        ) {
            (Some(id), Some(cred)) => Some(selene_client::AuthCredentials {
                auth_type: "bearer".into(),
                identity: id.clone(),
                credentials: cred.clone(),
            }),
            _ => None,
        }
    };

    let server_name = state.config.sync.server_name.clone().unwrap_or_else(|| {
        state
            .config
            .sync
            .upstream
            .split(':')
            .next()
            .unwrap_or("localhost")
            .to_string()
    });

    let tls = if let (Some(ca), Some(cert), Some(key)) = (
        &state.config.node_tls.ca_cert,
        &state.config.node_tls.cert,
        &state.config.node_tls.key,
    ) {
        Some(selene_client::config::ClientTlsConfig {
            ca_cert_path: ca.clone(),
            cert_path: Some(cert.clone()),
            key_path: Some(key.clone()),
        })
    } else {
        None
    };

    let client_config = selene_client::ClientConfig {
        server_addr: addr,
        server_name,
        insecure: state.config.dev_mode && state.config.node_tls.cert.is_none(),
        tls,
        auth,
    };

    let client = selene_client::SeleneClient::connect(&client_config)
        .await
        .map_err(|e| anyhow::anyhow!("connect to upstream hub: {e}"))?;

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_sync_loop_returns_immediately_when_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        // Default config has empty upstream/peer_name, so sync is disabled.
        assert!(!state.config.sync.is_enabled());

        let cancel = CancellationToken::new();
        // Should return immediately without blocking.
        run_sync_loop(state, cancel).await;
    }

    #[tokio::test]
    async fn connect_and_sync_fails_on_bad_address() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServerState::for_testing(dir.path()).await;
        state.config.sync.upstream = "not-a-valid-addr".to_string();
        state.config.sync.peer_name = "test-peer".to_string();

        let state = Arc::new(state);
        let cancel = CancellationToken::new();

        let result = connect_and_sync(&state, &cancel).await;
        assert!(result.is_err(), "should fail with invalid address");
    }

    #[tokio::test]
    async fn sync_loop_respects_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServerState::for_testing(dir.path()).await;
        // Point at an unreachable address so connect_and_sync fails quickly.
        state.config.sync.upstream = "127.0.0.1:19999".to_string();
        state.config.sync.peer_name = "cancel-test".to_string();
        state.config.sync.reconnect_delay_secs = 60; // long delay
        let state = Arc::new(state);

        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();

        let handle = tokio::spawn(async move {
            run_sync_loop(state, cancel2).await;
        });

        // Give the loop time to attempt one connection and enter backoff.
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel.cancel();

        // The loop should exit promptly after cancellation.
        let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "sync loop should exit after cancellation");
    }
}
