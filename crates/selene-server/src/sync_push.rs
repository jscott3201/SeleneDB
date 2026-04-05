//! Edge-side push task for buffered WAL entries.
//!
//! Reads un-pushed local WAL entries and sends them to the upstream hub
//! in batches. Called periodically or on reconnect to drain the local
//! change buffer.

use std::sync::Arc;

use selene_persist::Wal;
use selene_persist::sync_cursor::SyncCursor;
use selene_wire::dto::sync::{SyncEntry, SyncPushAckResponse, SyncPushRequest};

use crate::bootstrap::ServerState;

/// Read un-pushed WAL entries and send to upstream hub in batches.
/// Returns the total number of entries pushed.
///
/// Called by the sync background task on reconnect and at each push
/// interval during the live bidirectional loop.
///
/// When `subscription_filter` is `Some`, each WAL batch is filtered to only
/// include changes that match the subscription scope before serialization and
/// transmission. This reduces both CPU (no serialization of out-of-scope
/// changes) and bandwidth (smaller batches).
pub async fn push_buffered_changes(
    state: &Arc<ServerState>,
    client: &selene_client::SeleneClient,
    cursor: &mut SyncCursor,
    subscription_filter: Option<&mut crate::subscription::SubscriptionFilter>,
) -> Result<u64, anyhow::Error> {
    // Skip push when subscription is pull-only.
    if let Some(ref filter) = subscription_filter
        && filter.direction() == crate::subscription::SyncDirection::PullOnly
    {
        return Ok(0);
    }

    let wal_path = state.config.data_dir.join("wal.bin");

    // Read only Local-origin entries, skipping decompression and
    // deserialization for Replicated entries. The returned max_scanned_seq
    // tracks the highest WAL sequence seen (including skipped Replicated
    // entries) so the cursor can advance past replicated segments.
    let (local_entries, max_scanned_seq) =
        Wal::read_local_entries_after(&wal_path, cursor.last_pushed_seq)?;

    if local_entries.is_empty() {
        // Advance cursor past replicated segments to avoid re-scanning.
        if max_scanned_seq > cursor.last_pushed_seq {
            cursor.last_pushed_seq = max_scanned_seq;
            state
                .sync
                .last_pushed_seq
                .store(cursor.last_pushed_seq, std::sync::atomic::Ordering::Relaxed);
            cursor.save(&state.config.data_dir)?;
        }
        return Ok(0);
    }

    // Filter entries by subscription scope (if configured).
    let local_entries = if let Some(filter) = subscription_filter {
        let graph = state.graph.load_snapshot();
        local_entries
            .into_iter()
            .filter_map(|(seq, hlc_ts, changes)| {
                let filtered = filter.filter_changes(&changes, &graph);
                if filtered.is_empty() {
                    None
                } else {
                    Some((seq, hlc_ts, filtered))
                }
            })
            .collect::<Vec<_>>()
    } else {
        local_entries
    };

    if local_entries.is_empty() {
        return Ok(0);
    }

    let batch_size = state.config.sync.batch_size;
    let mut total_pushed: u64 = 0;

    for batch in local_entries.chunks(batch_size) {
        let entries: Vec<SyncEntry> = batch
            .iter()
            .map(|(seq, hlc_ts, changes)| SyncEntry {
                sequence: *seq,
                hlc_timestamp: *hlc_ts,
                changes: changes.clone(),
            })
            .collect();

        let request = SyncPushRequest {
            peer_name: cursor.peer_name.clone(),
            entries,
        };

        let ack: SyncPushAckResponse = client.sync_push(request).await?;

        cursor.last_pushed_seq = ack.acked_sequence;
        state
            .sync
            .last_pushed_seq
            .store(cursor.last_pushed_seq, std::sync::atomic::Ordering::Relaxed);
        cursor.save(&state.config.data_dir)?;

        if !ack.conflicts.is_empty() {
            tracing::warn!(
                count = ack.conflicts.len(),
                acked_seq = ack.acked_sequence,
                "sync push detected conflicts"
            );
            for c in &ack.conflicts {
                tracing::debug!(
                    entity_id = c.entity_id,
                    property = %c.property,
                    edge_hlc = c.edge_hlc,
                    hub_hlc = c.hub_hlc,
                    "conflict: hub wins (LWW)"
                );
            }
        }

        total_pushed += batch.len() as u64;
    }

    tracing::info!(total_pushed, "sync push complete");
    Ok(total_pushed)
}

#[cfg(test)]
mod tests {
    use selene_core::changeset::Change;
    use selene_core::{NodeId, Origin};
    use selene_persist::sync_cursor::SyncCursor;
    use selene_persist::{SyncPolicy, Wal};

    /// Verifies that `read_local_entries_after` returns only Local entries,
    /// skipping Replicated entries without decompressing them.
    #[test]
    fn filters_replicated_entries() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.bin");

        // Write a mix of Local and Replicated entries.
        let mut wal = Wal::open(&wal_path, SyncPolicy::OnSnapshot).unwrap();

        let local_changes = vec![Change::NodeCreated { node_id: NodeId(1) }];
        wal.append(&local_changes, 1000, Origin::Local).unwrap();

        let replicated_changes = vec![Change::NodeCreated { node_id: NodeId(2) }];
        wal.append(&replicated_changes, 2000, Origin::Replicated)
            .unwrap();

        let local_changes_2 = vec![Change::NodeCreated { node_id: NodeId(3) }];
        wal.append(&local_changes_2, 3000, Origin::Local).unwrap();
        drop(wal);

        // read_local_entries_after returns only Local entries directly.
        let (local_only, max_seq) = Wal::read_local_entries_after(&wal_path, 0).unwrap();

        assert_eq!(local_only.len(), 2, "only Local entries should remain");
        assert_eq!(max_seq, 3, "max_scanned_seq includes Replicated entries");

        // Verify the correct entries survived filtering.
        assert_eq!(local_only[0].0, 1, "first local entry has seq 1");
        assert_eq!(local_only[1].0, 3, "second local entry has seq 3");

        // Verify the changes match expectations.
        assert!(matches!(
            local_only[0].2[0],
            Change::NodeCreated { node_id: NodeId(1) }
        ));
        assert!(matches!(
            local_only[1].2[0],
            Change::NodeCreated { node_id: NodeId(3) }
        ));
    }

    /// Verifies that the cursor advances past replicated-only WAL segments
    /// even when no local entries remain to push. Without this, every push
    /// tick would re-scan the same replicated entries.
    #[test]
    fn cursor_advances_past_replicated_only_segments() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal.bin");

        // Write: Local(seq=1), Replicated(seq=2), Replicated(seq=3).
        let mut wal = Wal::open(&wal_path, SyncPolicy::OnSnapshot).unwrap();
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(1) }],
            1000,
            Origin::Local,
        )
        .unwrap();
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(2) }],
            2000,
            Origin::Replicated,
        )
        .unwrap();
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(3) }],
            3000,
            Origin::Replicated,
        )
        .unwrap();
        drop(wal);

        // Simulate a cursor that already pushed seq 1 (the only Local entry).
        let mut cursor = SyncCursor::new("hub-test".into());
        cursor.last_pushed_seq = 1;

        // Use read_local_entries_after which returns (local_entries, max_scanned_seq).
        let (local_entries, max_scanned_seq) =
            Wal::read_local_entries_after(&wal_path, cursor.last_pushed_seq).unwrap();

        assert!(
            local_entries.is_empty(),
            "no local entries should remain after seq 1"
        );

        // The cursor must advance past the replicated segment.
        if max_scanned_seq > cursor.last_pushed_seq {
            cursor.last_pushed_seq = max_scanned_seq;
            cursor.save(dir.path()).unwrap();
        }

        assert_eq!(
            cursor.last_pushed_seq, 3,
            "cursor should advance to seq 3 (highest scanned)"
        );

        // Verify persistence: reload and check.
        let reloaded = SyncCursor::load(dir.path())
            .unwrap()
            .expect("cursor should be persisted");
        assert_eq!(reloaded.last_pushed_seq, 3);
    }
}
