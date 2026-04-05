//! Hub-side handler for SyncPush requests from edge nodes.
//!
//! Receives batched changes, applies per-field LWW merge via
//! [`MergeTracker`](crate::merge_tracker::MergeTracker), writes
//! accepted changes to the graph, and returns a
//! [`SyncPushAckResponse`] with the highest acknowledged sequence and
//! any conflicts where the hub's value won.

use std::sync::Arc;

use selene_core::changeset::Change;
use selene_graph::change_applier::apply_changes;
use selene_wire::dto::sync::{SyncConflict, SyncPushAckResponse, SyncPushRequest};

use crate::ServerState;
use crate::auth::handshake::AuthContext;
use crate::merge_tracker::{MergeDecision, MergeTracker};

use super::SyncPushError;

/// Validate a `SyncPushRequest` against the server-side batch limits.
///
/// Returns `Ok(())` if the request is within limits, or
/// `Err(SyncPushError)` describing the violation.
pub fn validate_sync_push(
    state: &Arc<ServerState>,
    request: &SyncPushRequest,
) -> Result<(), SyncPushError> {
    let max_entries = state.config.sync.max_sync_entries;
    if request.entries.len() > max_entries {
        return Err(SyncPushError::TooManyEntries {
            count: request.entries.len(),
            limit: max_entries,
        });
    }

    let max_changes = state.config.sync.max_changes_per_entry;
    for (i, entry) in request.entries.iter().enumerate() {
        if entry.changes.len() > max_changes {
            return Err(SyncPushError::TooManyChanges {
                entry_index: i,
                count: entry.changes.len(),
                limit: max_changes,
            });
        }
    }

    Ok(())
}

/// Process a `SyncPushRequest` from an edge node.
///
/// For each entry the handler:
/// 1. Runs every change through the `MergeTracker` for LWW resolution.
/// 2. Applies accepted changes to the graph via `apply_changes` (raw
///    path, same as replica pull).
/// 3. Persists to WAL with `Origin::Replicated` and updates the
///    changelog buffer.
/// 4. Records accepted changes in the tracker so future pushes see the
///    latest HLC timestamps.
///
/// Returns the highest acknowledged sequence and a list of conflicts
/// (property-level only) where the hub's value was newer.
pub fn handle_sync_push(
    state: &Arc<ServerState>,
    request: SyncPushRequest,
    auth: &AuthContext,
) -> SyncPushAckResponse {
    if request.entries.is_empty() {
        return SyncPushAckResponse {
            acked_sequence: 0,
            conflicts: Vec::new(),
        };
    }

    // Reject entries with far-future HLC timestamps to prevent a
    // malicious or buggy peer from permanently freezing properties via
    // u64::MAX or similarly inflated values.
    let now_hlc = state.hlc().new_timestamp().get_time().as_u64();
    let max_skew_nanos = state.config.sync.max_hlc_skew_secs * 1_000_000_000;

    for entry in &request.entries {
        if entry.hlc_timestamp > now_hlc.saturating_add(max_skew_nanos) {
            tracing::warn!(
                peer = %request.peer_name,
                auth_principal = ?auth.principal_node_id,
                entry_hlc = entry.hlc_timestamp,
                "rejecting SyncPush with far-future HLC"
            );
            return SyncPushAckResponse {
                acked_sequence: 0,
                conflicts: vec![],
            };
        }
    }

    // Backfill the tracker on first push so it reflects existing hub
    // state. Never hold both locks simultaneously to avoid lock
    // ordering risks. Double-check after re-acquiring the tracker
    // lock in case another thread backfilled concurrently.
    let needs_backfill = state.sync.merge_tracker.lock().is_empty();
    if needs_backfill {
        let snapshot = state.persistence.changelog.lock().since(0);
        if let Some(entries) = snapshot {
            let mut tracker = state.sync.merge_tracker.lock();
            if tracker.is_empty() {
                tracker.backfill_from_changelog(&entries);
            }
        }
    }

    let mut acked_sequence: u64 = 0;
    let mut conflicts: Vec<SyncConflict> = Vec::new();

    for entry in &request.entries {
        // Phase 1: LWW filter under tracker lock.
        let (accepted, entry_conflicts) = {
            let tracker = state.sync.merge_tracker.lock();
            filter_changes(&tracker, &entry.changes, entry.hlc_timestamp)
        };

        conflicts.extend(entry_conflicts);

        // Phase 2: Apply accepted changes to the graph (if any).
        if !accepted.is_empty() {
            // Apply via raw graph mutation (same path as replica pull).
            {
                let inner = state.graph.inner();
                let mut guard = inner.write();
                apply_changes(&mut guard, &accepted, selene_core::Origin::Replicated);

                // Publish snapshot for lock-free readers.
                let snapshot = Arc::new(guard.clone());
                drop(guard);
                state.graph.publish_snapshot_arc(snapshot);
            }

            // Persist to WAL with Replicated origin.
            state
                .persistence
                .wal_coalescer
                .submit_wal_only(&accepted, selene_core::Origin::Replicated);

            // Update containment generation if needed.
            state.graph.check_containment_generation(&accepted);

            // Populate changelog buffer (enables search/vector subscribers).
            {
                let mut buf = state.persistence.changelog.lock();
                buf.append(
                    accepted.clone(),
                    selene_core::entity::now_nanos(),
                    entry.hlc_timestamp,
                );
            }

            // Notify subscribers (search index, vector store, etc.).
            let _ = state.persistence.changelog_notify.send(entry.sequence);

            // Record accepted changes in the tracker.
            {
                let mut tracker = state.sync.merge_tracker.lock();
                tracker.record_batch(&accepted, entry.hlc_timestamp);
            }
        }

        acked_sequence = entry.sequence;
    }

    tracing::debug!(
        peer = %request.peer_name,
        auth_principal = ?auth.principal_node_id,
        acked_sequence,
        accepted_entries = request.entries.len(),
        conflict_count = conflicts.len(),
        "sync push processed"
    );

    SyncPushAckResponse {
        acked_sequence,
        conflicts,
    }
}

/// Run each change through the tracker and split into accepted changes
/// and conflict reports.
fn filter_changes(
    tracker: &MergeTracker,
    changes: &[Change],
    edge_hlc: u64,
) -> (Vec<Change>, Vec<SyncConflict>) {
    let mut accepted = Vec::with_capacity(changes.len());
    let mut conflicts = Vec::new();

    for change in changes {
        match tracker.should_apply(change, edge_hlc) {
            MergeDecision::Apply => {
                accepted.push(change.clone());
            }
            MergeDecision::Skip => {
                // Only report property-level conflicts (not label/create/delete).
                if let Some(conflict) = make_conflict(change, edge_hlc, tracker) {
                    conflicts.push(conflict);
                }
            }
        }
    }

    (accepted, conflicts)
}

/// Build a `SyncConflict` for property-level changes that were skipped.
///
/// Returns `None` for non-property changes (labels, creates, deletes)
/// since those don't produce meaningful conflict reports.
fn make_conflict(change: &Change, edge_hlc: u64, tracker: &MergeTracker) -> Option<SyncConflict> {
    match change {
        Change::PropertySet { node_id, key, .. } | Change::PropertyRemoved { node_id, key, .. } => {
            let hub_hlc = tracker.property_hlc_for(node_id.0, *key).unwrap_or(0);
            Some(SyncConflict {
                entity_id: node_id.0,
                property: key.to_string(),
                edge_hlc,
                hub_hlc,
            })
        }
        Change::EdgePropertySet { edge_id, key, .. }
        | Change::EdgePropertyRemoved { edge_id, key, .. } => {
            let hub_hlc = tracker.property_hlc_for(edge_id.0, *key).unwrap_or(0);
            Some(SyncConflict {
                entity_id: edge_id.0,
                property: key.to_string(),
                edge_hlc,
                hub_hlc,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use selene_core::IStr;
    use selene_core::changeset::Change;
    use selene_core::entity::{EdgeId, NodeId};
    use selene_core::value::Value;
    use selene_wire::dto::sync::{SyncEntry, SyncPushRequest};

    use super::*;
    use crate::bootstrap::ServerState;

    // ── helpers ────────────────────────────────────────────────────────

    fn test_auth() -> AuthContext {
        AuthContext::dev_admin()
    }

    fn push_request(peer: &str, entries: Vec<SyncEntry>) -> SyncPushRequest {
        SyncPushRequest {
            peer_name: peer.to_string(),
            entries,
        }
    }

    fn entry(seq: u64, hlc: u64, changes: Vec<Change>) -> SyncEntry {
        SyncEntry {
            sequence: seq,
            hlc_timestamp: hlc,
            changes,
        }
    }

    // 1 ─────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn empty_push_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        let ack = handle_sync_push(&state, push_request("edge-01", vec![]), &auth);

        assert_eq!(ack.acked_sequence, 0);
        assert!(ack.conflicts.is_empty());
    }

    // 2 ─────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn creates_node_from_push() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        let ack = handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    5000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(100),
                        },
                        Change::LabelAdded {
                            node_id: NodeId(100),
                            label: IStr::new("Sensor"),
                        },
                        Change::PropertySet {
                            node_id: NodeId(100),
                            key: IStr::new("temp"),
                            value: Value::Float(22.0),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        assert_eq!(ack.acked_sequence, 1);
        assert!(ack.conflicts.is_empty());

        // Verify node was created.
        let exists = state.graph.read(|g| g.contains_node(NodeId(100)));
        assert!(exists, "node 100 should exist after sync push");

        // Verify property was set.
        let temp = state.graph.read(|g| {
            g.get_node(NodeId(100))
                .and_then(|n| n.properties.get(IStr::new("temp")).cloned())
        });
        assert_eq!(temp, Some(Value::Float(22.0)));
    }

    // 3 ─────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn lww_conflict_skips_stale_update() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // First push: create node and set property at HLC 5000.
        handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    5000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(100),
                        },
                        Change::PropertySet {
                            node_id: NodeId(100),
                            key: IStr::new("temp"),
                            value: Value::Float(22.0),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        // Second push: stale update at HLC 3000 (older) should be skipped.
        let ack = handle_sync_push(
            &state,
            push_request(
                "edge-02",
                vec![entry(
                    2,
                    3000,
                    vec![Change::PropertySet {
                        node_id: NodeId(100),
                        key: IStr::new("temp"),
                        value: Value::Float(99.0),
                        old_value: None,
                    }],
                )],
            ),
            &auth,
        );

        assert_eq!(ack.acked_sequence, 2);
        assert_eq!(ack.conflicts.len(), 1, "should report one conflict");
        assert_eq!(ack.conflicts[0].entity_id, 100);
        assert_eq!(ack.conflicts[0].property, "temp");
        assert_eq!(ack.conflicts[0].edge_hlc, 3000);
        assert_eq!(ack.conflicts[0].hub_hlc, 5000);

        // Verify value was NOT overwritten.
        let temp = state.graph.read(|g| {
            g.get_node(NodeId(100))
                .and_then(|n| n.properties.get(IStr::new("temp")).cloned())
        });
        assert_eq!(temp, Some(Value::Float(22.0)));
    }

    // 4 ─────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn lww_newer_update_wins() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // Create node at HLC 1000.
        handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    1000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(200),
                        },
                        Change::PropertySet {
                            node_id: NodeId(200),
                            key: IStr::new("status"),
                            value: Value::str("active"),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        // Newer update at HLC 2000.
        let ack = handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    2,
                    2000,
                    vec![Change::PropertySet {
                        node_id: NodeId(200),
                        key: IStr::new("status"),
                        value: Value::str("offline"),
                        old_value: None,
                    }],
                )],
            ),
            &auth,
        );

        assert_eq!(ack.acked_sequence, 2);
        assert!(ack.conflicts.is_empty());

        let status = state.graph.read(|g| {
            g.get_node(NodeId(200))
                .and_then(|n| n.properties.get(IStr::new("status")).cloned())
        });
        assert_eq!(status, Some(Value::str("offline")));
    }

    // 5 ─────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn multiple_entries_in_single_push() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        let ack = handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![
                    entry(
                        1,
                        1000,
                        vec![Change::NodeCreated {
                            node_id: NodeId(10),
                        }],
                    ),
                    entry(
                        2,
                        2000,
                        vec![Change::NodeCreated {
                            node_id: NodeId(20),
                        }],
                    ),
                    entry(
                        3,
                        3000,
                        vec![
                            Change::NodeCreated {
                                node_id: NodeId(30),
                            },
                            Change::EdgeCreated {
                                edge_id: EdgeId(1),
                                source: NodeId(10),
                                target: NodeId(20),
                                label: IStr::new("feeds"),
                            },
                        ],
                    ),
                ],
            ),
            &auth,
        );

        assert_eq!(ack.acked_sequence, 3);
        assert!(ack.conflicts.is_empty());

        let count = state.graph.read(|g| g.node_count());
        assert_eq!(count, 3);

        let edge_exists = state.graph.read(|g| g.get_edge(EdgeId(1)).is_some());
        assert!(edge_exists);
    }

    // 6 ─────────────────────────────────────────────────────────────────
    #[tokio::test]
    async fn edge_property_conflict_reported() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // Create edge and set property at HLC 5000.
        handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    5000,
                    vec![
                        Change::NodeCreated { node_id: NodeId(1) },
                        Change::NodeCreated { node_id: NodeId(2) },
                        Change::EdgeCreated {
                            edge_id: EdgeId(10),
                            source: NodeId(1),
                            target: NodeId(2),
                            label: IStr::new("feeds"),
                        },
                        Change::EdgePropertySet {
                            edge_id: EdgeId(10),
                            source: NodeId(1),
                            target: NodeId(2),
                            key: IStr::new("weight"),
                            value: Value::Float(1.0),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        // Stale edge property update at HLC 2000.
        let ack = handle_sync_push(
            &state,
            push_request(
                "edge-02",
                vec![entry(
                    2,
                    2000,
                    vec![Change::EdgePropertySet {
                        edge_id: EdgeId(10),
                        source: NodeId(1),
                        target: NodeId(2),
                        key: IStr::new("weight"),
                        value: Value::Float(99.0),
                        old_value: None,
                    }],
                )],
            ),
            &auth,
        );

        assert_eq!(ack.conflicts.len(), 1);
        assert_eq!(ack.conflicts[0].entity_id, 10);
        assert_eq!(ack.conflicts[0].property, "weight");
    }

    // ── Integration tests (Task 15) ───────────────────────────────────

    // 7 ─────────────────────────────────────────────────────────────────
    /// Push a delete for an existing node and verify it is removed.
    #[tokio::test]
    async fn delete_wins_over_update() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // Create a node on the hub via sync push.
        handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    1000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(50),
                        },
                        Change::LabelAdded {
                            node_id: NodeId(50),
                            label: IStr::new("Sensor"),
                        },
                        Change::PropertySet {
                            node_id: NodeId(50),
                            key: IStr::new("name"),
                            value: Value::str("temp-01"),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        // Confirm the node exists.
        let exists = state.graph.read(|g| g.contains_node(NodeId(50)));
        assert!(exists, "node 50 should exist before delete");

        // Push a deletion at a newer HLC.
        let ack = handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    2,
                    2000,
                    vec![Change::NodeDeleted {
                        node_id: NodeId(50),
                        labels: vec![IStr::new("Sensor")],
                    }],
                )],
            ),
            &auth,
        );

        assert_eq!(ack.acked_sequence, 2);
        assert!(ack.conflicts.is_empty());

        // Verify the node no longer exists.
        let gone = state.graph.read(|g| !g.contains_node(NodeId(50)));
        assert!(gone, "node 50 should be deleted after sync push");
    }

    // 8 ─────────────────────────────────────────────────────────────────
    /// Pushing the same node creation from two different sequence numbers
    /// must not produce duplicates. The second push uses a newer HLC to
    /// verify the property is updated rather than duplicated.
    #[tokio::test]
    async fn duplicate_push_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // First push: create node at HLC 1000.
        let ack1 = handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    1000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(300),
                        },
                        Change::LabelAdded {
                            node_id: NodeId(300),
                            label: IStr::new("Device"),
                        },
                        Change::PropertySet {
                            node_id: NodeId(300),
                            key: IStr::new("serial"),
                            value: Value::str("ABC-123"),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );
        assert_eq!(ack1.acked_sequence, 1);
        assert!(ack1.conflicts.is_empty());

        let count_after_first = state.graph.read(|g| g.node_count());
        assert_eq!(count_after_first, 1, "should have exactly one node");

        // Second push: same node at a newer HLC 2000 (simulates retry
        // from a different edge or a re-delivery with corrected data).
        let ack2 = handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    2,
                    2000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(300),
                        },
                        Change::LabelAdded {
                            node_id: NodeId(300),
                            label: IStr::new("Device"),
                        },
                        Change::PropertySet {
                            node_id: NodeId(300),
                            key: IStr::new("serial"),
                            value: Value::str("ABC-456"),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );
        assert_eq!(ack2.acked_sequence, 2);
        assert!(ack2.conflicts.is_empty());

        // Node count must remain 1 (no duplicates).
        let count_after_second = state.graph.read(|g| g.node_count());
        assert_eq!(
            count_after_second, 1,
            "duplicate push must not create extra nodes"
        );

        // Value should be updated to the newer push.
        let serial = state.graph.read(|g| {
            g.get_node(NodeId(300))
                .and_then(|n| n.properties.get(IStr::new("serial")).cloned())
        });
        assert_eq!(serial, Some(Value::str("ABC-456")));
    }

    // 9 ─────────────────────────────────────────────────────────────────
    /// Backfill the MergeTracker from an existing changelog, then verify
    /// that subsequent pushes respect the tracker state.
    #[tokio::test]
    async fn merge_tracker_backfill_from_changelog() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // Seed the changelog by pushing changes at a known HLC.
        handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    5000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(400),
                        },
                        Change::PropertySet {
                            node_id: NodeId(400),
                            key: IStr::new("firmware"),
                            value: Value::str("v1.0"),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        // Create a fresh MergeTracker and backfill from the changelog.
        let mut fresh_tracker = MergeTracker::new();
        assert!(fresh_tracker.is_empty());

        {
            let cl = state.persistence.changelog.lock();
            if let Some(entries) = cl.since(0) {
                fresh_tracker.backfill_from_changelog(&entries);
            }
        }

        assert!(
            !fresh_tracker.is_empty(),
            "backfilled tracker must not be empty"
        );

        // The tracker should know about the property at HLC 5000.
        let firmware_hlc = fresh_tracker.property_hlc_for(400, IStr::new("firmware"));
        assert_eq!(firmware_hlc, Some(5000), "backfilled HLC should be 5000");

        // A stale push (HLC 3000) should be skipped by the backfilled tracker.
        let stale_change = Change::PropertySet {
            node_id: NodeId(400),
            key: IStr::new("firmware"),
            value: Value::str("v0.9"),
            old_value: None,
        };
        assert_eq!(
            fresh_tracker.should_apply(&stale_change, 3000),
            crate::merge_tracker::MergeDecision::Skip
        );

        // A newer push (HLC 8000) should be accepted.
        assert_eq!(
            fresh_tracker.should_apply(&stale_change, 8000),
            crate::merge_tracker::MergeDecision::Apply
        );
    }

    // 10 ────────────────────────────────────────────────────────────────
    /// Full LWW round-trip: write at HLC 100, push newer at HLC 150
    /// (edge wins), then push older at HLC 50 (hub wins).
    #[tokio::test]
    async fn lww_resolves_property_conflict_full_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // Hub writes a property at HLC 100 via sync push.
        handle_sync_push(
            &state,
            push_request(
                "hub-local",
                vec![entry(
                    1,
                    100,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(500),
                        },
                        Change::PropertySet {
                            node_id: NodeId(500),
                            key: IStr::new("temperature"),
                            value: Value::Float(20.0),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        // Seed the tracker manually so it reflects HLC 100.
        {
            let mut tracker = state.sync.merge_tracker.lock();
            tracker.record_property(500, IStr::new("temperature"), 100);
        }

        // Edge pushes at HLC 150 (newer). Should win.
        let ack_150 = handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    2,
                    150,
                    vec![Change::PropertySet {
                        node_id: NodeId(500),
                        key: IStr::new("temperature"),
                        value: Value::Float(25.5),
                        old_value: None,
                    }],
                )],
            ),
            &auth,
        );

        assert!(ack_150.conflicts.is_empty(), "HLC 150 > 100, no conflict");
        let temp = state.graph.read(|g| {
            g.get_node(NodeId(500))
                .and_then(|n| n.properties.get(IStr::new("temperature")).cloned())
        });
        assert_eq!(
            temp,
            Some(Value::Float(25.5)),
            "edge value at HLC 150 should win"
        );

        // Edge pushes at HLC 50 (older). Should be skipped.
        let ack_50 = handle_sync_push(
            &state,
            push_request(
                "edge-02",
                vec![entry(
                    3,
                    50,
                    vec![Change::PropertySet {
                        node_id: NodeId(500),
                        key: IStr::new("temperature"),
                        value: Value::Float(10.0),
                        old_value: None,
                    }],
                )],
            ),
            &auth,
        );

        assert_eq!(ack_50.conflicts.len(), 1, "HLC 50 < 150, should conflict");
        assert_eq!(ack_50.conflicts[0].edge_hlc, 50);
        assert_eq!(ack_50.conflicts[0].hub_hlc, 150);

        // Value must still be the one from HLC 150.
        let temp_final = state.graph.read(|g| {
            g.get_node(NodeId(500))
                .and_then(|n| n.properties.get(IStr::new("temperature")).cloned())
        });
        assert_eq!(
            temp_final,
            Some(Value::Float(25.5)),
            "value must still be from HLC 150"
        );
    }

    // 12 ────────────────────────────────────────────────────────────────
    /// Verify that changes applied via sync push are persisted to WAL
    /// and populated in the changelog buffer.
    #[tokio::test]
    async fn push_persists_to_wal_and_changelog() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    4000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(600),
                        },
                        Change::PropertySet {
                            node_id: NodeId(600),
                            key: IStr::new("reading"),
                            value: Value::Float(42.0),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        // Verify the changelog buffer has the entry.
        let changelog_entries = {
            let cl = state.persistence.changelog.lock();
            cl.since(0)
        };

        assert!(changelog_entries.is_some(), "changelog should have entries");
        let entries = changelog_entries.unwrap();
        assert!(
            !entries.is_empty(),
            "changelog should not be empty after push"
        );

        // The last entry should contain changes for node 600.
        let last = entries.last().unwrap();
        assert_eq!(last.hlc_timestamp, 4000);
        assert!(last.changes.iter().any(|c| matches!(
            c,
            Change::NodeCreated {
                node_id: NodeId(600)
            }
        )));

        // Verify WAL has entries with Replicated origin.
        let wal_path = dir.path().join("wal.bin");
        let wal_entries = selene_persist::Wal::read_entries_after(&wal_path, 0).unwrap();
        assert!(
            !wal_entries.is_empty(),
            "WAL should have entries after push"
        );

        // All entries from sync push should be Origin::Replicated.
        for (_seq, _hlc, _changes, origin) in &wal_entries {
            assert_eq!(
                *origin,
                selene_core::Origin::Replicated,
                "sync-pushed entries must be marked Replicated in WAL"
            );
        }
    }

    // 13 ────────────────────────────────────────────────────────────────
    /// A SyncPush entry with a far-future HLC (e.g. u64::MAX) must be
    /// rejected so that a malicious peer cannot permanently freeze
    /// properties via an unreachable timestamp.
    #[tokio::test]
    async fn rejects_far_future_hlc_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // Push with u64::MAX HLC -- must be rejected.
        let ack = handle_sync_push(
            &state,
            push_request(
                "evil-edge",
                vec![entry(
                    1,
                    u64::MAX,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(900),
                        },
                        Change::PropertySet {
                            node_id: NodeId(900),
                            key: IStr::new("pwned"),
                            value: Value::str("yes"),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        assert_eq!(
            ack.acked_sequence, 0,
            "far-future HLC must be rejected (acked_sequence == 0)"
        );

        // Verify the node was NOT created.
        let exists = state.graph.read(|g| g.contains_node(NodeId(900)));
        assert!(!exists, "node 900 must not exist after rejected push");

        // Now push with a reasonable HLC derived from the server's clock.
        let reasonable_hlc = state.hlc().new_timestamp().get_time().as_u64();
        let ack_ok = handle_sync_push(
            &state,
            push_request(
                "good-edge",
                vec![entry(
                    2,
                    reasonable_hlc,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(901),
                        },
                        Change::PropertySet {
                            node_id: NodeId(901),
                            key: IStr::new("status"),
                            value: Value::str("ok"),
                            old_value: None,
                        },
                    ],
                )],
            ),
            &auth,
        );

        assert_eq!(ack_ok.acked_sequence, 2, "reasonable HLC must be accepted");

        let exists = state.graph.read(|g| g.contains_node(NodeId(901)));
        assert!(exists, "node 901 should exist after accepted push");
    }

    // 17 ────────────────────────────────────────────────────────────────
    /// Hub-local writes recorded in the MergeTracker via `persist_or_die`
    /// must prevent stale edge values from overwriting them.
    #[tokio::test]
    async fn hub_local_write_prevents_stale_edge_overwrite() {
        use crate::ops::nodes;

        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        // 1. Create a node on the hub via the ops layer (exercises persist_or_die).
        let labels = selene_core::LabelSet::from_strs(&["Sensor"]);
        let mut props = selene_core::PropertyMap::new();
        props.insert(IStr::new("temperature"), Value::Float(20.0));
        let auth = AuthContext::dev_admin();
        let dto = nodes::create_node(&state, &auth, labels, props, None).unwrap();
        let node_id = NodeId(dto.id);

        // 2. Update the property via the ops layer (another persist_or_die call).
        nodes::modify_node(
            &state,
            &auth,
            dto.id,
            vec![(IStr::new("temperature"), Value::Float(25.0))],
            vec![],
            vec![],
            vec![],
        )
        .unwrap();

        // The MergeTracker should now have an entry for this property
        // from the persist_or_die calls.
        let has_entry = {
            let tracker = state.merge_tracker().lock();
            tracker.property_hlc_for(node_id.0, IStr::new("temperature"))
        };
        assert!(
            has_entry.is_some(),
            "persist_or_die must record hub-local writes in MergeTracker"
        );
        let hub_hlc = has_entry.unwrap();

        // 3. Push a SyncPush from an edge with the SAME property but a
        //    LOWER HLC. The stale edge value must NOT overwrite the hub.
        let stale_hlc = 1; // far in the past
        let ack = handle_sync_push(
            &state,
            push_request(
                "edge-01",
                vec![entry(
                    1,
                    stale_hlc,
                    vec![Change::PropertySet {
                        node_id,
                        key: IStr::new("temperature"),
                        value: Value::Float(10.0),
                        old_value: None,
                    }],
                )],
            ),
            &auth,
        );

        // 4. The hub's value must still be 25.0 (the latest hub-local write).
        let temp = state.graph.read(|g| {
            g.get_node(node_id)
                .and_then(|n| n.properties.get(IStr::new("temperature")).cloned())
        });
        assert_eq!(
            temp,
            Some(Value::Float(25.0)),
            "hub-local value must survive stale edge push"
        );

        // 5. The ack must report a conflict.
        assert_eq!(
            ack.conflicts.len(),
            1,
            "stale edge push must report a conflict"
        );
        assert_eq!(ack.conflicts[0].edge_hlc, stale_hlc);
        assert_eq!(ack.conflicts[0].hub_hlc, hub_hlc);
    }

    // ── Batch limit tests ─────────────────────────────────────────────

    // 14 ────────────────────────────────────────────────────────────────
    /// A request with more entries than `max_sync_entries` is rejected.
    #[tokio::test]
    async fn rejects_too_many_entries() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        // Default limit is 1,000. Build a request with 1,001 entries.
        let limit = state.config.sync.max_sync_entries;
        let entries: Vec<SyncEntry> = (0..=limit as u64)
            .map(|i| entry(i + 1, 1000 + i, vec![]))
            .collect();

        let result = validate_sync_push(&state, &push_request("evil-edge", entries));

        assert_eq!(
            result,
            Err(SyncPushError::TooManyEntries {
                count: limit + 1,
                limit,
            })
        );
    }

    // 15 ────────────────────────────────────────────────────────────────
    /// An entry with more changes than `max_changes_per_entry` is rejected.
    #[tokio::test]
    async fn rejects_too_many_changes_per_entry() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        // Default limit is 10,000. Build an entry with 10,001 changes.
        let limit = state.config.sync.max_changes_per_entry;
        let changes: Vec<Change> = (0..=limit as u64)
            .map(|i| Change::NodeCreated { node_id: NodeId(i) })
            .collect();

        let req = push_request("evil-edge", vec![entry(1, 5000, changes)]);
        let result = validate_sync_push(&state, &req);

        assert_eq!(
            result,
            Err(SyncPushError::TooManyChanges {
                entry_index: 0,
                count: limit + 1,
                limit,
            })
        );
    }

    // 16 ────────────────────────────────────────────────────────────────
    /// Requests at exactly the limit pass validation (boundary test).
    #[tokio::test]
    async fn accepts_request_at_exact_limit() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        // Build an entry with exactly max_changes_per_entry changes.
        let limit = state.config.sync.max_changes_per_entry;
        let changes: Vec<Change> = (0..limit as u64)
            .map(|i| Change::NodeCreated { node_id: NodeId(i) })
            .collect();

        let req = push_request("good-edge", vec![entry(1, 5000, changes)]);
        let result = validate_sync_push(&state, &req);

        assert!(result.is_ok(), "request at exact limit should be accepted");
    }

    // ── SyncPushError Display tests ──────────────────────────────────

    // 23 ────────────────────────────────────────────────────────────────
    #[test]
    fn sync_push_error_too_many_entries_display() {
        let err = SyncPushError::TooManyEntries {
            count: 1500,
            limit: 1000,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("1500"),
            "Display must include the actual count"
        );
        assert!(
            msg.contains("1000"),
            "Display must include the configured limit"
        );
        assert!(msg.contains("entries"), "Display must mention entries");
    }

    // 24 ────────────────────────────────────────────────────────────────
    #[test]
    fn sync_push_error_too_many_changes_display() {
        let err = SyncPushError::TooManyChanges {
            entry_index: 3,
            count: 20000,
            limit: 10000,
        };
        let msg = err.to_string();
        assert!(msg.contains("3"), "Display must include the entry index");
        assert!(
            msg.contains("20000"),
            "Display must include the actual count"
        );
        assert!(msg.contains("10000"), "Display must include the limit");
    }

    // 25 ────────────────────────────────────────────────────────────────
    /// validate_sync_push accepts a request with zero entries.
    #[tokio::test]
    async fn validate_empty_request_passes() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        let req = push_request("edge-01", vec![]);
        assert!(validate_sync_push(&state, &req).is_ok());
    }

    // 26 ────────────────────────────────────────────────────────────────
    /// validate_sync_push: entry with zero changes passes.
    #[tokio::test]
    async fn validate_entry_with_zero_changes_passes() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        let req = push_request("edge-01", vec![entry(1, 1000, vec![])]);
        assert!(validate_sync_push(&state, &req).is_ok());
    }

    // 27 ────────────────────────────────────────────────────────────────
    /// validate_sync_push: second entry exceeds limit while first is OK.
    #[tokio::test]
    async fn validate_too_many_changes_in_second_entry() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        let limit = state.config.sync.max_changes_per_entry;
        let big_changes: Vec<Change> = (0..=limit as u64)
            .map(|i| Change::NodeCreated { node_id: NodeId(i) })
            .collect();

        let req = push_request(
            "edge-01",
            vec![
                entry(1, 1000, vec![Change::NodeCreated { node_id: NodeId(1) }]),
                entry(2, 2000, big_changes),
            ],
        );
        let result = validate_sync_push(&state, &req);
        assert_eq!(
            result,
            Err(SyncPushError::TooManyChanges {
                entry_index: 1,
                count: limit + 1,
                limit,
            }),
            "error must report the correct entry index"
        );
    }

    // ── filter_changes unit tests ────────────────────────────────────

    // 28 ────────────────────────────────────────────────────────────────
    /// filter_changes with empty tracker accepts all changes.
    #[test]
    fn filter_changes_empty_tracker_accepts_all() {
        let tracker = MergeTracker::new();
        let changes = vec![
            Change::NodeCreated { node_id: NodeId(1) },
            Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("temp"),
                value: Value::Float(22.0),
                old_value: None,
            },
        ];
        let (accepted, conflicts) = filter_changes(&tracker, &changes, 100);
        assert_eq!(accepted.len(), 2, "all changes accepted on empty tracker");
        assert!(conflicts.is_empty(), "no conflicts on empty tracker");
    }

    // 29 ────────────────────────────────────────────────────────────────
    /// filter_changes: label change skipped by tracker does not produce conflict.
    #[test]
    fn filter_changes_label_skip_no_conflict() {
        let mut tracker = MergeTracker::new();
        let label = IStr::new("Sensor");
        tracker.record_label(1, label, 200);

        let changes = vec![Change::LabelAdded {
            node_id: NodeId(1),
            label,
        }];

        let (accepted, conflicts) = filter_changes(&tracker, &changes, 100);
        // HLC 100 < 200 so the label change is skipped.
        assert!(accepted.is_empty(), "stale label must be skipped");
        // Label changes do not produce SyncConflict reports.
        assert!(
            conflicts.is_empty(),
            "label skip must not produce a conflict report"
        );
    }

    // 30 ────────────────────────────────────────────────────────────────
    /// make_conflict returns None for non-property changes (node creation, deletion).
    #[test]
    fn make_conflict_returns_none_for_non_property_changes() {
        let tracker = MergeTracker::new();

        let creation = Change::NodeCreated { node_id: NodeId(1) };
        assert!(make_conflict(&creation, 100, &tracker).is_none());

        let deletion = Change::NodeDeleted {
            node_id: NodeId(1),
            labels: vec![IStr::new("Sensor")],
        };
        assert!(make_conflict(&deletion, 100, &tracker).is_none());

        let edge_creation = Change::EdgeCreated {
            edge_id: EdgeId(1),
            source: NodeId(1),
            target: NodeId(2),
            label: IStr::new("feeds"),
        };
        assert!(make_conflict(&edge_creation, 100, &tracker).is_none());

        let label_add = Change::LabelAdded {
            node_id: NodeId(1),
            label: IStr::new("Sensor"),
        };
        assert!(make_conflict(&label_add, 100, &tracker).is_none());
    }

    // 31 ────────────────────────────────────────────────────────────────
    /// make_conflict returns Some with correct HLCs for property changes.
    #[test]
    fn make_conflict_returns_some_for_property_change() {
        let mut tracker = MergeTracker::new();
        tracker.record_property(1, IStr::new("temp"), 500);

        let change = Change::PropertySet {
            node_id: NodeId(1),
            key: IStr::new("temp"),
            value: Value::Float(10.0),
            old_value: None,
        };
        let conflict = make_conflict(&change, 200, &tracker);
        assert!(conflict.is_some());
        let c = conflict.unwrap();
        assert_eq!(c.entity_id, 1);
        assert_eq!(c.property, "temp");
        assert_eq!(c.edge_hlc, 200);
        assert_eq!(c.hub_hlc, 500);
    }

    // 32 ────────────────────────────────────────────────────────────────
    /// make_conflict for edge property removal includes correct entity ID.
    #[test]
    fn make_conflict_edge_property_removed() {
        let mut tracker = MergeTracker::new();
        tracker.record_property(42, IStr::new("weight"), 300);

        let change = Change::EdgePropertyRemoved {
            edge_id: EdgeId(42),
            source: NodeId(1),
            target: NodeId(2),
            key: IStr::new("weight"),
            old_value: None,
        };
        let conflict = make_conflict(&change, 100, &tracker);
        assert!(conflict.is_some());
        let c = conflict.unwrap();
        assert_eq!(c.entity_id, 42);
        assert_eq!(c.property, "weight");
    }
}
