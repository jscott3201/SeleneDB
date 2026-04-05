//! Replica mode orchestration — connects to primary, fetches snapshot, applies changelog.
//!
//! Startup sequence:
//! 1. Connect to primary via QUIC (selene-client)
//! 2. Subscribe to changelog stream (buffer incoming events)
//! 3. Request binary snapshot
//! 4. Load snapshot into graph
//! 5. Replay buffered changelog entries (sequence > snapshot sequence)
//! 6. Resume live changelog consumption

use std::sync::Arc;
use std::sync::atomic::Ordering;

use selene_graph::change_applier::apply_changes;
use selene_graph::changelog::ChangelogEntry;
use selene_graph::graph::SeleneGraph;
use tokio_util::sync::CancellationToken;

use crate::bootstrap::ServerState;

/// Run the replica replication loop.
///
/// This function runs for the lifetime of the replica. It connects to the
/// primary, fetches a snapshot, then continuously applies changelog entries.
/// Returns only on cancellation or unrecoverable error.
pub async fn run_replica(
    state: Arc<ServerState>,
    primary_addr: &str,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!(primary = primary_addr, "starting replica mode");

    loop {
        match replicate_once(&state, primary_addr, &cancel).await {
            Ok(()) => {
                tracing::info!("replica replication ended (cancelled)");
                return Ok(());
            }
            Err(e) => {
                if cancel.is_cancelled() {
                    return Ok(());
                }
                tracing::warn!(error = %e, "replication error, reconnecting in 5s");
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                    _ = cancel.cancelled() => return Ok(()),
                }
            }
        }
    }
}

/// Single replication attempt: connect, snapshot, consume changelog.
async fn replicate_once(
    state: &ServerState,
    primary_addr: &str,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    // 1. Connect to primary via QUIC
    let addr: std::net::SocketAddr = primary_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid primary address '{primary_addr}': {e}"))?;

    let auth = if state.config.dev_mode {
        Some(selene_client::AuthCredentials {
            auth_type: "dev".into(),
            identity: "replica".into(),
            credentials: "dev".into(),
        })
    } else {
        match (
            &state.config.replica.auth_identity,
            &state.config.replica.auth_credentials,
        ) {
            (Some(id), Some(cred)) => Some(selene_client::AuthCredentials {
                auth_type: "bearer".into(),
                identity: id.clone(),
                credentials: cred.clone(),
            }),
            _ => None,
        }
    };

    let server_name = state.config.replica.server_name.clone().unwrap_or_else(|| {
        primary_addr
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

    tracing::info!(primary = primary_addr, "connecting to primary");
    let client = selene_client::SeleneClient::connect(&client_config)
        .await
        .map_err(|e| anyhow::anyhow!("connect to primary: {e}"))?;

    // 2. Subscribe to changelog stream FIRST (buffer events during snapshot transfer)
    tracing::info!("subscribing to changelog stream");
    let mut changelog_sub = client
        .subscribe_changelog(0, None)
        .await
        .map_err(|e| anyhow::anyhow!("changelog subscribe: {e}"))?;

    // 3. Request binary snapshot
    tracing::info!("requesting snapshot from primary");
    let (snapshot_bytes, snapshot_sequence) = client
        .request_snapshot("")
        .await
        .map_err(|e| anyhow::anyhow!("snapshot request: {e}"))?;

    tracing::info!(
        bytes = snapshot_bytes.len(),
        snapshot_sequence,
        "snapshot received, loading into graph"
    );

    // 4. Load snapshot into graph
    load_snapshot_into_graph(state, &snapshot_bytes, snapshot_sequence)?;

    // Update lag counter
    if let Some(ref lag) = state.replica.lag {
        lag.store(0, Ordering::Relaxed);
    }

    tracing::info!(
        nodes = state.graph.read(|g| g.node_count()),
        edges = state.graph.read(|g| g.edge_count()),
        "snapshot loaded, starting changelog replay"
    );

    // 5. Replay buffered changelog entries (sequence > snapshot_sequence)
    //    and 6. Continue live consumption
    loop {
        tokio::select! {
            event = changelog_sub.next_event() => {
                if let Some(event) = event {
                    if event.sync_lost {
                        tracing::warn!("sync lost — must re-sync with full snapshot");
                        return Err(anyhow::anyhow!("sync lost, re-syncing"));
                    }

                    // Skip events already covered by the snapshot
                    if event.sequence <= snapshot_sequence {
                        continue;
                    }

                    let entry = ChangelogEntry {
                        sequence: event.sequence,
                        timestamp_nanos: selene_core::entity::now_nanos(),
                        hlc_timestamp: event.hlc_timestamp,
                        changes: event.changes,
                    };

                    apply_entry_to_replica(state, &entry);

                    // Update lag counter
                    if let Some(ref lag) = state.replica.lag {
                        // Lag = 0 when caught up (we don't know primary's current seq
                        // but will be close since we're consuming in real-time)
                        lag.store(0, Ordering::Relaxed);
                    }

                    // Ack to the primary
                    let _ = changelog_sub.ack(event.sequence).await;
                } else {
                    tracing::warn!("changelog stream closed by primary");
                    return Err(anyhow::anyhow!("changelog stream closed"));
                }
            }
            _ = cancel.cancelled() => {
                tracing::info!("replica replication cancelled");
                return Ok(());
            }
        }
    }
}

/// Load a binary snapshot into the replica's graph.
fn load_snapshot_into_graph(
    state: &ServerState,
    snapshot_bytes: &[u8],
    _snapshot_sequence: u64,
) -> anyhow::Result<()> {
    use selene_persist::snapshot::read_snapshot;

    // Write to temp file for read_snapshot() (it reads from a path)
    let tmp_path = state.config.data_dir.join("replica_incoming.tmp");
    std::fs::write(&tmp_path, snapshot_bytes)?;
    let snapshot =
        read_snapshot(&tmp_path).map_err(|e| anyhow::anyhow!("snapshot deserialize: {e}"))?;
    let _ = std::fs::remove_file(&tmp_path);

    // Convert snapshot types to core types
    let nodes: Vec<selene_core::Node> = snapshot
        .nodes
        .into_iter()
        .map(|sn| sn.into_node())
        .collect();
    let edges: Vec<selene_core::Edge> = snapshot
        .edges
        .into_iter()
        .map(|se| se.into_edge())
        .collect();

    // Load into graph under write lock
    {
        let inner = state.graph.inner();
        let mut guard = inner.write();

        // Clear existing graph state (in case of re-sync)
        *guard = SeleneGraph::new();

        // Load nodes and edges
        guard.load_nodes(nodes);
        guard.load_edges(edges);
        guard
            .set_next_ids(snapshot.next_node_id, snapshot.next_edge_id)
            .map_err(|e| anyhow::anyhow!("set_next_ids: {e}"))?;

        // Restore schemas
        if !snapshot.schemas.node_schemas.is_empty() || !snapshot.schemas.edge_schemas.is_empty() {
            guard
                .schema_mut()
                .import(snapshot.schemas.node_schemas, snapshot.schemas.edge_schemas);
            guard.build_property_indexes();
            guard.build_composite_indexes();
        }

        // Restore triggers
        if !snapshot.triggers.is_empty() {
            guard.trigger_registry_mut().load(snapshot.triggers);
        }

        // Publish snapshot for lock-free readers
        let new_snapshot = Arc::new(guard.clone());
        drop(guard);
        state.graph.publish_snapshot_arc(new_snapshot);
    }

    tracing::info!("snapshot loaded into graph");
    Ok(())
}

/// Apply a changelog entry to the replica's graph, persist to WAL, and notify subscribers.
///
/// Changes received from the primary/hub are tagged with `Origin::Replicated`
/// in both the graph and the WAL. The push task (Phase 5B) uses this tag to
/// filter out replicated entries and only push locally-originated changes back
/// to the hub, preventing echo loops.
pub fn apply_entry_to_replica(state: &ServerState, entry: &ChangelogEntry) {
    // Apply to graph via _raw methods (bypasses TrackedMutation)
    {
        let inner = state.graph.inner();
        let mut guard = inner.write();
        apply_changes(&mut guard, &entry.changes, selene_core::Origin::Replicated);

        // Publish snapshot for lock-free readers
        let snapshot = Arc::new(guard.clone());
        drop(guard);
        state.graph.publish_snapshot_arc(snapshot);
    }

    // Persist to local WAL with Replicated origin so the push task can
    // distinguish these from locally-produced changes. Uses
    // submit_wal_only because we manage the changelog append below
    // with the primary's original timestamps.
    state
        .persistence
        .wal_coalescer
        .submit_wal_only(&entry.changes, selene_core::Origin::Replicated);

    // Check containment generation
    state.graph.check_containment_generation(&entry.changes);

    // Populate local ChangelogBuffer (enables tantivy/vector subscribers)
    {
        let mut buf = state.persistence.changelog.lock();
        buf.append(
            entry.changes.clone(),
            entry.timestamp_nanos,
            entry.hlc_timestamp,
        );
    }

    // Notify subscribers (search index updater, vector store, etc.)
    let _ = state.persistence.changelog_notify.send(entry.sequence);
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::IStr;
    use selene_core::changeset::Change;
    use selene_core::entity::NodeId;

    #[tokio::test]
    async fn apply_entry_creates_node_and_notifies() {
        let dir = tempfile::tempdir().unwrap();
        let state = ServerState::for_testing(dir.path()).await;

        let entry = ChangelogEntry {
            sequence: 1,
            timestamp_nanos: 1_000_000_000,
            hlc_timestamp: 0,
            changes: vec![
                Change::NodeCreated { node_id: NodeId(1) },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: IStr::new("sensor"),
                },
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("name"),
                    value: selene_core::Value::str("Test"),
                    old_value: None,
                },
            ],
        };

        apply_entry_to_replica(&state, &entry);

        // Verify node exists in snapshot (lock-free read)
        let exists = state.graph.read(|g| g.contains_node(NodeId(1)));
        assert!(exists);

        // Verify changelog buffer was populated
        let seq = state.persistence.changelog.lock().current_sequence();
        assert!(seq > 0);
    }

    #[tokio::test]
    async fn apply_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let state = ServerState::for_testing(dir.path()).await;

        let entry1 = ChangelogEntry {
            sequence: 1,
            timestamp_nanos: 100,
            hlc_timestamp: 0,
            changes: vec![Change::NodeCreated { node_id: NodeId(1) }],
        };
        let entry2 = ChangelogEntry {
            sequence: 2,
            timestamp_nanos: 200,
            hlc_timestamp: 0,
            changes: vec![Change::NodeCreated { node_id: NodeId(2) }],
        };

        apply_entry_to_replica(&state, &entry1);
        apply_entry_to_replica(&state, &entry2);

        let count = state.graph.read(|g| g.node_count());
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn apply_entry_updates_containment_generation() {
        let dir = tempfile::tempdir().unwrap();
        let state = ServerState::for_testing(dir.path()).await;

        let gen_before = state.graph.containment_generation();

        let entry = ChangelogEntry {
            sequence: 1,
            timestamp_nanos: 100,
            hlc_timestamp: 0,
            changes: vec![
                Change::NodeCreated { node_id: NodeId(1) },
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: selene_core::entity::EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("contains"),
                },
            ],
        };

        apply_entry_to_replica(&state, &entry);

        assert!(state.graph.containment_generation() > gen_before);
    }

    #[tokio::test]
    async fn load_snapshot_populates_graph() {
        use selene_persist::snapshot::{
            GraphSnapshot, SnapshotEdge, SnapshotNode, SnapshotSchemas, write_snapshot_opts,
        };

        let dir = tempfile::tempdir().unwrap();
        let state = ServerState::for_testing(dir.path()).await;

        // Build a test snapshot
        let snapshot = GraphSnapshot {
            nodes: vec![
                SnapshotNode {
                    id: 1,
                    labels: vec!["sensor".into()],
                    properties: vec![("name".into(), selene_core::Value::str("Test"))],
                    created_at: 100,
                    updated_at: 100,
                    version: 1,
                },
                SnapshotNode {
                    id: 2,
                    labels: vec!["building".into()],
                    properties: vec![],
                    created_at: 100,
                    updated_at: 100,
                    version: 1,
                },
            ],
            edges: vec![SnapshotEdge {
                id: 1,
                source: 2,
                target: 1,
                label: "contains".into(),
                properties: vec![],
                created_at: 100,
            }],
            next_node_id: 3,
            next_edge_id: 2,
            changelog_sequence: 5,
            schemas: SnapshotSchemas {
                node_schemas: vec![],
                edge_schemas: vec![],
            },
            triggers: vec![],
            extra_sections: vec![],
        };

        // Serialize to bytes via temp file
        let snap_path = dir.path().join("test.snap");
        write_snapshot_opts(&snapshot, &snap_path, false).unwrap();
        let snapshot_bytes = std::fs::read(&snap_path).unwrap();

        // Load into replica
        load_snapshot_into_graph(&state, &snapshot_bytes, 5).unwrap();

        // Verify
        assert_eq!(state.graph.read(|g| g.node_count()), 2);
        assert_eq!(state.graph.read(|g| g.edge_count()), 1);
        assert!(state.graph.read(|g| g.contains_node(NodeId(1))));
        assert!(state.graph.read(|g| g.contains_node(NodeId(2))));
    }
}
