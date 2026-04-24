//! Reconstruct graph state from a snapshot + WAL replay.
//!
//! Recovery steps:
//! 1. Find the latest snapshot file
//! 2. Deserialize into `GraphSnapshot`
//! 3. Read WAL entries after the snapshot sequence
//! 4. Replay changes with upsert semantics
//! 5. Return the recovered graph and current sequence

use std::collections::HashMap;
use std::path::Path;

use selene_core::Value;
use selene_core::changeset::Change;

use crate::error::PersistError;
use crate::snapshot::{GraphSnapshot, find_latest_snapshot, read_snapshot};
use crate::wal::Wal;

/// Result of recovery — the reconstructed graph and current WAL sequence.
pub struct RecoveryResult {
    /// Recovered nodes (id, labels, properties, timestamps, version).
    pub nodes: Vec<RecoveredNode>,
    /// Recovered edges.
    pub edges: Vec<RecoveredEdge>,
    /// Next node ID to allocate.
    pub next_node_id: u64,
    /// Next edge ID to allocate.
    pub next_edge_id: u64,
    /// Current changelog/WAL sequence.
    pub sequence: u64,
    /// Recovered schemas (from snapshot).
    pub schemas: crate::snapshot::SnapshotSchemas,
    /// Recovered triggers (from snapshot, empty if not present).
    pub triggers: Vec<selene_core::trigger::TriggerDef>,
    /// Extra snapshot sections (pre-deserialized bytes). Section 5 = version store.
    pub extra_sections: Vec<Vec<u8>>,
    /// Schema mutations drained from the WAL (post-snapshot). The caller
    /// applies these to the rebuilt graph's schema registry in WAL order
    /// (via `selene_graph::change_applier::apply_schema_mutation`) after
    /// loading the snapshot baseline. See `selene_server::bootstrap` for
    /// the replay site.
    pub schema_mutations: Vec<selene_core::changeset::SchemaMutation>,
}

/// A recovered node in owned form, ready for `SeleneGraph::load_nodes`.
pub struct RecoveredNode {
    pub id: u64,
    pub labels: selene_core::LabelSet,
    pub properties: selene_core::PropertyMap,
    pub created_at: i64,
    pub updated_at: i64,
    pub version: u64,
}

/// A recovered edge in owned form, ready for `SeleneGraph::load_edges`.
pub struct RecoveredEdge {
    pub id: u64,
    pub source: u64,
    pub target: u64,
    pub label: selene_core::IStr,
    pub properties: selene_core::PropertyMap,
    pub created_at: i64,
}

/// Recover the graph state from the data directory.
///
/// If no snapshot exists, returns an empty result.  WAL entries after
/// the snapshot sequence are replayed using upsert semantics.
pub fn recover(data_dir: &Path) -> Result<RecoveryResult, PersistError> {
    let snapshot_dir = data_dir.join("snapshots");
    let wal_path = data_dir.join("wal.bin");

    // Find latest snapshot
    let snapshot = find_latest_snapshot(&snapshot_dir)?;

    let (mut state, snapshot_seq, schemas, triggers, extra_sections) = if let Some(path) = snapshot
    {
        tracing::info!(?path, "loading snapshot");
        let mut snap = read_snapshot(&path)?;
        let seq = snap.changelog_sequence;
        let schemas = std::mem::take(&mut snap.schemas);
        let triggers = std::mem::take(&mut snap.triggers);
        let extra = std::mem::take(&mut snap.extra_sections);
        (
            snapshot_to_state_no_schemas(snap),
            seq,
            schemas,
            triggers,
            extra,
        )
    } else {
        tracing::info!("no snapshot found, starting fresh");
        (
            RecoveryState {
                nodes: HashMap::new(),
                edges: HashMap::new(),
                next_node_id: 1,
                next_edge_id: 1,
                schema_mutations: Vec::new(),
            },
            0,
            crate::snapshot::SnapshotSchemas::default(),
            Vec::new(),
            Vec::new(),
        )
    };

    // Replay WAL entries after snapshot sequence
    let mut current_seq = snapshot_seq;
    if wal_path.exists() {
        let entries = Wal::read_entries_after(&wal_path, snapshot_seq)?;
        tracing::info!(count = entries.len(), "replaying WAL entries");
        for (seq, timestamp, changes, _origin) in entries {
            replay_changes(&mut state, &changes, timestamp);
            current_seq = seq;
        }
    }

    // Build result
    let nodes: Vec<RecoveredNode> = state
        .nodes
        .into_values()
        .map(|n| {
            let label_strs: Vec<&str> = n.labels.iter().map(|s| s.as_str()).collect();
            RecoveredNode {
                id: n.id,
                labels: selene_core::LabelSet::from_strs(&label_strs),
                properties: selene_core::PropertyMap::from_pairs(
                    n.properties
                        .into_iter()
                        .map(|(k, v)| (selene_core::IStr::new(&k), v)),
                ),
                created_at: n.created_at,
                updated_at: n.updated_at,
                version: n.version,
            }
        })
        .collect();

    let edges: Vec<RecoveredEdge> = state
        .edges
        .into_values()
        .map(|e| RecoveredEdge {
            id: e.id,
            source: e.source,
            target: e.target,
            label: selene_core::IStr::new(&e.label),
            properties: selene_core::PropertyMap::from_pairs(
                e.properties
                    .into_iter()
                    .map(|(k, v)| (selene_core::IStr::new(&k), v)),
            ),
            created_at: e.created_at,
        })
        .collect();

    Ok(RecoveryResult {
        nodes,
        edges,
        next_node_id: state.next_node_id,
        next_edge_id: state.next_edge_id,
        sequence: current_seq,
        schemas,
        triggers,
        extra_sections,
        schema_mutations: state.schema_mutations,
    })
}

// ── Internal recovery state ──────────────────────────────────────────────

struct RecoveryState {
    nodes: HashMap<u64, RecoveryNode>,
    edges: HashMap<u64, RecoveryEdge>,
    next_node_id: u64,
    next_edge_id: u64,
    /// Schema mutations observed in the WAL after the snapshot seq. The
    /// caller (bootstrap) applies these to the rebuilt graph's schema
    /// registry in order so the final in-memory state matches what the
    /// running server saw before the crash.
    schema_mutations: Vec<selene_core::changeset::SchemaMutation>,
}

struct RecoveryNode {
    id: u64,
    labels: Vec<String>,
    /// Properties stored as a small Vec for linear-scan upsert.
    /// Typical node property counts (5-15) make O(n) scan cheaper
    /// than HashMap overhead.
    properties: Vec<(String, Value)>,
    created_at: i64,
    updated_at: i64,
    version: u64,
}

struct RecoveryEdge {
    id: u64,
    source: u64,
    target: u64,
    label: String,
    /// See `RecoveryNode::properties` for rationale.
    properties: Vec<(String, Value)>,
    created_at: i64,
}

fn snapshot_to_state_no_schemas(snap: GraphSnapshot) -> RecoveryState {
    let nodes: HashMap<u64, RecoveryNode> = snap
        .nodes
        .into_iter()
        .map(|n| {
            (
                n.id,
                RecoveryNode {
                    id: n.id,
                    labels: n.labels,
                    properties: n.properties,
                    created_at: n.created_at,
                    updated_at: n.updated_at,
                    version: n.version,
                },
            )
        })
        .collect();

    let edges: HashMap<u64, RecoveryEdge> = snap
        .edges
        .into_iter()
        .map(|e| {
            (
                e.id,
                RecoveryEdge {
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    label: e.label,
                    properties: e.properties,
                    created_at: e.created_at,
                },
            )
        })
        .collect();

    RecoveryState {
        nodes,
        edges,
        next_node_id: snap.next_node_id,
        next_edge_id: snap.next_edge_id,
        schema_mutations: Vec::new(),
    }
}

/// Replay changes with upsert semantics to handle the crash window
/// between snapshot write and WAL truncation.
fn replay_changes(state: &mut RecoveryState, changes: &[Change], entry_timestamp: u64) {
    let ts = entry_timestamp as i64;
    for change in changes {
        match change {
            Change::NodeCreated { node_id } => {
                let id = node_id.0;
                state.nodes.entry(id).or_insert_with(|| RecoveryNode {
                    id,
                    labels: Vec::new(),
                    properties: Vec::new(),
                    created_at: ts,
                    updated_at: ts,
                    version: 1,
                });
                if id >= state.next_node_id {
                    state.next_node_id = id + 1;
                }
            }
            Change::NodeDeleted { node_id, .. } => {
                state.nodes.remove(&node_id.0);
                // Also remove edges referencing this node
                state
                    .edges
                    .retain(|_, e| e.source != node_id.0 && e.target != node_id.0);
            }
            Change::PropertySet {
                node_id,
                key,
                value,
                ..
            } => {
                if let Some(node) = state.nodes.get_mut(&node_id.0) {
                    if let Some(existing) =
                        node.properties.iter_mut().find(|(k, _)| k == key.as_ref())
                    {
                        existing.1 = value.clone();
                    } else {
                        node.properties.push((key.to_string(), value.clone()));
                    }
                    node.version += 1;
                    node.updated_at = ts;
                }
            }
            Change::PropertyRemoved { node_id, key, .. } => {
                if let Some(node) = state.nodes.get_mut(&node_id.0) {
                    node.properties.retain(|(k, _)| k != key.as_ref());
                    node.updated_at = ts;
                }
            }
            Change::LabelAdded { node_id, label } => {
                if let Some(node) = state.nodes.get_mut(&node_id.0) {
                    let label_str = label.to_string();
                    if !node.labels.contains(&label_str) {
                        node.labels.push(label_str);
                    }
                    node.updated_at = ts;
                }
            }
            Change::LabelRemoved { node_id, label } => {
                if let Some(node) = state.nodes.get_mut(&node_id.0) {
                    node.labels.retain(|l| l != label.as_ref());
                    node.updated_at = ts;
                }
            }
            Change::EdgeCreated {
                edge_id,
                source,
                target,
                label,
            } => {
                let id = edge_id.0;
                state.edges.entry(id).or_insert_with(|| RecoveryEdge {
                    id,
                    source: source.0,
                    target: target.0,
                    label: label.to_string(),
                    properties: Vec::new(),
                    created_at: ts,
                });
                if id >= state.next_edge_id {
                    state.next_edge_id = id + 1;
                }
            }
            Change::EdgeDeleted { edge_id, .. } => {
                state.edges.remove(&edge_id.0);
            }
            Change::EdgePropertySet {
                edge_id,
                key,
                value,
                ..
            } => {
                if let Some(edge) = state.edges.get_mut(&edge_id.0) {
                    if let Some(existing) =
                        edge.properties.iter_mut().find(|(k, _)| k == key.as_ref())
                    {
                        existing.1 = value.clone();
                    } else {
                        edge.properties.push((key.to_string(), value.clone()));
                    }
                }
            }
            Change::EdgePropertyRemoved { edge_id, key, .. } => {
                if let Some(edge) = state.edges.get_mut(&edge_id.0) {
                    edge.properties.retain(|(k, _)| k != key.as_ref());
                }
            }
            Change::SchemaMutation(op) => {
                // Schema mutations are applied to the graph after recovery
                // rebuilds nodes/edges. We collect them here; the bootstrap
                // consumer (`bootstrap.rs`) drains the queue and applies
                // each op to the schema registry of the freshly-built
                // graph, preserving WAL order.
                state.schema_mutations.push(op.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SyncPolicy;
    use crate::snapshot::{GraphSnapshot, SnapshotEdge, SnapshotNode, write_snapshot};
    use selene_core::{EdgeId, IStr, NodeId, Origin, Value};
    use smol_str::SmolStr;

    fn setup_data_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("snapshots")).unwrap();
        dir
    }

    #[test]
    fn recover_empty_data_dir() {
        let dir = setup_data_dir();
        let result = recover(dir.path()).unwrap();
        assert!(result.nodes.is_empty());
        assert!(result.edges.is_empty());
        assert_eq!(result.next_node_id, 1);
        assert_eq!(result.sequence, 0);
    }

    #[test]
    fn recover_from_snapshot_only() {
        let dir = setup_data_dir();

        let snap = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["sensor".into()],
                properties: vec![("unit".into(), Value::String(SmolStr::new("°F")))],
                created_at: 100,
                updated_at: 200,
                version: 2,
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 5,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };

        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000005.snap")).unwrap();

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.nodes[0].id, 1);
        assert!(result.nodes[0].labels.contains_str("sensor"));
        assert_eq!(result.next_node_id, 2);
        assert_eq!(result.sequence, 5);
    }

    #[test]
    fn recover_snapshot_plus_wal() {
        let dir = setup_data_dir();

        // Write snapshot with 1 node at seq 5
        let snap = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["sensor".into()],
                properties: vec![],
                created_at: 100,
                updated_at: 100,
                version: 1,
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 5,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000005.snap")).unwrap();

        // Simulate post-snapshot WAL: truncate to seq 5, then append new entries.
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();
        wal.truncate(5).unwrap(); // simulate post-snapshot truncation

        // Add a property to node 1  (seq 6)
        wal.append(
            &[Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("value"),
                value: Value::Float(72.0),
                old_value: None,
            }],
            selene_core::entity::now_nanos() as u64,
            Origin::Local,
        )
        .unwrap();

        // Create node 2  (seq 7)
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(2) }],
            selene_core::entity::now_nanos() as u64,
            Origin::Local,
        )
        .unwrap();

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 2);

        let node1 = result.nodes.iter().find(|n| n.id == 1).unwrap();
        assert!(
            node1
                .properties
                .contains_key(selene_core::IStr::new("value"))
        );

        let node2 = result.nodes.iter().find(|n| n.id == 2).unwrap();
        assert_eq!(node2.id, 2);
    }

    #[test]
    fn recover_wal_only_no_snapshot() {
        let dir = setup_data_dir();

        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();

        wal.append(
            &[Change::NodeCreated { node_id: NodeId(1) }],
            selene_core::entity::now_nanos() as u64,
            Origin::Local,
        )
        .unwrap();
        wal.append(
            &[
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("contains"),
                },
            ],
            selene_core::entity::now_nanos() as u64,
            Origin::Local,
        )
        .unwrap();

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.sequence, 2);
    }

    #[test]
    fn recover_node_delete_in_wal() {
        let dir = setup_data_dir();

        let snap = GraphSnapshot {
            nodes: vec![
                SnapshotNode {
                    id: 1,
                    labels: vec!["a".into()],
                    properties: vec![],
                    created_at: 0,
                    updated_at: 0,
                    version: 1,
                },
                SnapshotNode {
                    id: 2,
                    labels: vec!["b".into()],
                    properties: vec![],
                    created_at: 0,
                    updated_at: 0,
                    version: 1,
                },
            ],
            edges: vec![SnapshotEdge {
                id: 1,
                source: 1,
                target: 2,
                label: "link".into(),
                properties: vec![],
                created_at: 0,
            }],
            next_node_id: 3,
            next_edge_id: 2,
            changelog_sequence: 3,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000003.snap")).unwrap();

        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();

        let ts = selene_core::entity::now_nanos() as u64;
        // Write 4 entries: seq 1-3 are skipped (<=snapshot_seq), seq 4 is replayed.
        wal.append(&[], ts, Origin::Local).unwrap(); // seq 1 (ignored)
        wal.append(&[], ts, Origin::Local).unwrap(); // seq 2 (ignored)
        wal.append(&[], ts, Origin::Local).unwrap(); // seq 3 (ignored)
        wal.append(
            &[Change::NodeDeleted {
                node_id: NodeId(1),
                labels: vec![IStr::new("sensor")],
            }],
            ts,
            Origin::Local,
        )
        .unwrap(); // seq 4 (replayed)

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 1); // node 1 deleted
        assert_eq!(result.nodes[0].id, 2);
        assert!(result.edges.is_empty()); // edge cascaded
    }

    #[test]
    fn recover_label_changes_in_wal() {
        let dir = setup_data_dir();

        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();

        let ts = selene_core::entity::now_nanos() as u64;
        wal.append(
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: IStr::new("sensor"),
                },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: IStr::new("temperature"),
                },
            ],
            ts,
            Origin::Local,
        )
        .unwrap();

        wal.append(
            &[Change::LabelRemoved {
                node_id: NodeId(1),
                label: IStr::new("temperature"),
            }],
            ts,
            Origin::Local,
        )
        .unwrap();

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 1);
        assert!(result.nodes[0].labels.contains_str("sensor"));
        assert!(!result.nodes[0].labels.contains_str("temperature"));
    }

    #[test]
    fn no_version_inflation_on_crash_window_replay() {
        // A crash between snapshot write and WAL truncation must not inflate
        // node versions: entries at seq <= snapshot_seq are skipped.
        let dir = setup_data_dir();

        let snap = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["sensor".into()],
                properties: vec![("name".into(), Value::String(SmolStr::new("HQ")))],
                created_at: 0,
                updated_at: 0,
                version: 3, // node was modified twice (version 1 + 2 property sets)
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 5,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000005.snap")).unwrap();

        // WAL has entries 1-5 (all skipped): simulates crash before truncation
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();
        let ts = selene_core::entity::now_nanos() as u64;
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(1) }],
            ts,
            Origin::Local,
        )
        .unwrap(); // seq 1
        wal.append(
            &[Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("name"),
                value: Value::String(SmolStr::new("HQ")),
                old_value: None,
            }],
            ts,
            Origin::Local,
        )
        .unwrap(); // seq 2
        wal.append(
            &[Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("unit"),
                value: Value::String(SmolStr::new("°F")),
                old_value: None,
            }],
            ts,
            Origin::Local,
        )
        .unwrap(); // seq 3
        wal.append(&[], ts, Origin::Local).unwrap(); // seq 4
        wal.append(&[], ts, Origin::Local).unwrap(); // seq 5

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 1);
        // Version stays at 3 (from snapshot), not inflated by skipped WAL entries
        assert_eq!(result.nodes[0].version, 3);
    }

    #[test]
    fn recover_snapshot_with_triggers() {
        use selene_core::trigger::{TriggerDef, TriggerEvent};

        let dir = setup_data_dir();
        let trigger = TriggerDef {
            name: std::sync::Arc::from("auto_status"),
            event: TriggerEvent::Insert,
            label: std::sync::Arc::from("sensor"),
            condition: Some("NEW.status IS NULL".into()),
            action: "SET NEW.status = 'active'".into(),
        };

        let snap = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["sensor".into()],
                properties: vec![],
                created_at: 100,
                updated_at: 200,
                version: 1,
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 5,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![trigger.clone()],
            extra_sections: vec![],
        };

        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000005.snap")).unwrap();

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.triggers.len(), 1);
        assert_eq!(result.triggers[0], trigger);
    }

    #[test]
    fn recover_triggers_with_no_nodes() {
        use selene_core::trigger::{TriggerDef, TriggerEvent};

        let dir = setup_data_dir();
        let trigger = TriggerDef {
            name: std::sync::Arc::from("auto_init"),
            event: TriggerEvent::Insert,
            label: std::sync::Arc::from("device"),
            condition: None,
            action: "SET NEW.status = 'new'".into(),
        };

        let snap = GraphSnapshot {
            nodes: vec![],
            edges: vec![],
            next_node_id: 1,
            next_edge_id: 1,
            changelog_sequence: 1,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![trigger.clone()],
            extra_sections: vec![],
        };

        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000001.snap")).unwrap();

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.triggers.len(), 1);
        assert_eq!(result.triggers[0], trigger);
    }

    #[test]
    fn upsert_semantics_on_replay() {
        let dir = setup_data_dir();

        // Snapshot has node 1
        let snap = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["sensor".into()],
                properties: vec![],
                created_at: 0,
                updated_at: 0,
                version: 1,
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 1,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000001.snap")).unwrap();

        // WAL also has NodeCreated for node 1 (crash between snapshot and truncate)
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();
        let ts = selene_core::entity::now_nanos() as u64;
        wal.append(&[], ts, Origin::Local).unwrap(); // seq 1 (skipped)
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(1) }],
            ts,
            Origin::Local,
        )
        .unwrap(); // seq 2 -- upsert, not duplicate

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 1); // still just 1 node, not duplicated
    }

    #[test]
    fn wal_recovery_preserves_timestamps() {
        let dir = setup_data_dir();
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();

        let ts_create: u64 = 1_000_000_000;
        let ts_modify: u64 = 2_000_000_000;

        // Entry 1: create node and edge
        wal.append(
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(1),
                    label: IStr::new("self"),
                },
            ],
            ts_create,
            Origin::Local,
        )
        .unwrap();

        // Entry 2: modify the node (property set + label add)
        wal.append(
            &[
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("temp"),
                    value: Value::Float(72.0),
                    old_value: None,
                },
                Change::LabelAdded {
                    node_id: NodeId(1),
                    label: IStr::new("sensor"),
                },
            ],
            ts_modify,
            Origin::Local,
        )
        .unwrap();

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 1);
        assert_eq!(result.edges.len(), 1);

        let node = &result.nodes[0];
        assert_eq!(
            node.created_at, ts_create as i64,
            "created_at should match first WAL entry timestamp"
        );
        assert_eq!(
            node.updated_at, ts_modify as i64,
            "updated_at should match last modifying WAL entry timestamp"
        );

        let edge = &result.edges[0];
        assert_eq!(
            edge.created_at, ts_create as i64,
            "edge created_at should match WAL entry timestamp"
        );
    }

    // ── Recovery hardening tests ────────────────────────────────────────

    #[test]
    fn recovery_with_corrupted_last_wal_entry() {
        let dir = setup_data_dir();

        // Snapshot with 1 node at seq 3
        let snap = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["sensor".into()],
                properties: vec![],
                created_at: 100,
                updated_at: 100,
                version: 1,
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 3,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000003.snap")).unwrap();

        // WAL with entries at seq 1-5. Entries 1-3 skipped (<=snapshot_seq).
        // Entry 4 is good, entry 5 is corrupted.
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();
        let ts = 1_000_000u64;
        for _ in 0..3 {
            wal.append(&[], ts, Origin::Local).unwrap(); // seq 1-3 (skipped)
        }
        wal.append(
            &[Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new("status"),
                value: Value::String(SmolStr::new("online")),
                old_value: None,
            }],
            ts,
            Origin::Local,
        )
        .unwrap(); // seq 4 (replayed)
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(2) }],
            ts,
            Origin::Local,
        )
        .unwrap(); // seq 5 (will be corrupted)
        drop(wal);

        // Corrupt last byte of the WAL file (in entry 5's payload).
        // open_existing truncates at the corrupt entry, so read_entries_after
        // sees only entries 1-4.
        let mut data = std::fs::read(&wal_path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        std::fs::write(&wal_path, &data).unwrap();

        // Force a re-open to trigger truncation of corrupt entry 5
        {
            let _wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();
        }

        let result = recover(dir.path()).unwrap();
        // Entry 4 (PropertySet on node 1) should be replayed; entry 5 discarded
        assert_eq!(result.nodes.len(), 1, "only original node from snapshot");
        assert!(
            result.nodes[0].properties.contains_key(IStr::new("status")),
            "property from entry 4 should be present"
        );
        assert_eq!(result.sequence, 4);
    }

    #[test]
    fn recovery_with_empty_snapshot() {
        let dir = setup_data_dir();

        let snap = GraphSnapshot {
            nodes: vec![],
            edges: vec![],
            next_node_id: 1,
            next_edge_id: 1,
            changelog_sequence: 0,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000000.snap")).unwrap();

        let result = recover(dir.path()).unwrap();
        assert!(result.nodes.is_empty());
        assert!(result.edges.is_empty());
        assert_eq!(result.next_node_id, 1);
        assert_eq!(result.next_edge_id, 1);
        assert_eq!(result.sequence, 0);
    }

    #[test]
    fn recovery_chain_snapshot_plus_many_wal_entries() {
        let dir = setup_data_dir();

        // Snapshot with 1 node at seq 5
        let snap = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["root".into()],
                properties: vec![],
                created_at: 0,
                updated_at: 0,
                version: 1,
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 5,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000005.snap")).unwrap();

        // WAL with 100 entries after snapshot
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::OnSnapshot).unwrap();
        wal.truncate(5).unwrap();

        for i in 0..100 {
            let node_id = NodeId(i + 2); // nodes 2-101
            wal.append(
                &[Change::NodeCreated { node_id }],
                1_000_000 + i,
                Origin::Local,
            )
            .unwrap();
        }
        wal.sync().unwrap();
        drop(wal);

        let result = recover(dir.path()).unwrap();
        // 1 from snapshot + 100 from WAL = 101 nodes
        assert_eq!(result.nodes.len(), 101);
        assert_eq!(result.next_node_id, 102);
        assert_eq!(result.sequence, 105); // snapshot_seq(5) + 100 entries
    }

    #[test]
    fn recovery_uses_latest_snapshot_when_multiple_exist() {
        let dir = setup_data_dir();

        // Older snapshot at seq 3 with node 1
        let snap_old = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["old".into()],
                properties: vec![],
                created_at: 0,
                updated_at: 0,
                version: 1,
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 3,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(
            &snap_old,
            &dir.path().join("snapshots/snap-000000000003.snap"),
        )
        .unwrap();

        // Newer snapshot at seq 10 with nodes 1 and 2
        let snap_new = GraphSnapshot {
            nodes: vec![
                SnapshotNode {
                    id: 1,
                    labels: vec!["new".into()],
                    properties: vec![("migrated".into(), Value::Bool(true))],
                    created_at: 0,
                    updated_at: 500,
                    version: 2,
                },
                SnapshotNode {
                    id: 2,
                    labels: vec!["new".into()],
                    properties: vec![],
                    created_at: 200,
                    updated_at: 200,
                    version: 1,
                },
            ],
            edges: vec![],
            next_node_id: 3,
            next_edge_id: 1,
            changelog_sequence: 10,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };
        write_snapshot(
            &snap_new,
            &dir.path().join("snapshots/snap-000000000010.snap"),
        )
        .unwrap();

        let result = recover(dir.path()).unwrap();
        // Should use the newer snapshot (seq 10)
        assert_eq!(
            result.nodes.len(),
            2,
            "should have 2 nodes from latest snapshot"
        );
        assert_eq!(result.sequence, 10);
        assert_eq!(result.next_node_id, 3);

        // Verify the newer snapshot's data is used
        let node1 = result.nodes.iter().find(|n| n.id == 1).unwrap();
        assert!(
            node1.labels.contains_str("new"),
            "should have label from newer snapshot"
        );
    }

    #[test]
    fn recovery_preserves_all_value_variants() {
        let dir = setup_data_dir();
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();

        let ts = 1_000_000u64;
        // Create node, then set properties with diverse Value types
        wal.append(
            &[Change::NodeCreated { node_id: NodeId(1) }],
            ts,
            Origin::Local,
        )
        .unwrap();

        let props = vec![
            ("int_val", Value::Int(-42)),
            ("float_val", Value::Float(3.15)),
            ("string_val", Value::String(SmolStr::new("hello world"))),
            ("bool_val", Value::Bool(false)),
            ("null_val", Value::Null),
            ("uint_val", Value::UInt(999)),
            ("timestamp_val", Value::Timestamp(1_700_000_000)),
            (
                "bytes_val",
                Value::Bytes(std::sync::Arc::from(vec![1, 2, 3])),
            ),
            (
                "list_val",
                Value::List(std::sync::Arc::from(vec![Value::Int(1), Value::Bool(true)])),
            ),
        ];

        let changes: Vec<Change> = props
            .iter()
            .map(|(key, value)| Change::PropertySet {
                node_id: NodeId(1),
                key: IStr::new(key),
                value: value.clone(),
                old_value: None,
            })
            .collect();

        wal.append(&changes, ts + 1, Origin::Local).unwrap();
        drop(wal);

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 1);

        let node = &result.nodes[0];
        for (key, expected_value) in &props {
            let actual = node.properties.get_by_str(key);
            assert_eq!(
                actual,
                Some(expected_value),
                "Value mismatch for property '{key}'"
            );
        }
    }

    #[test]
    fn recovery_edge_property_operations() {
        let dir = setup_data_dir();
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();
        let ts = 1_000_000u64;

        // Create two nodes and an edge, then set and remove edge properties
        wal.append(
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("connects"),
                },
            ],
            ts,
            Origin::Local,
        )
        .unwrap();

        wal.append(
            &[
                Change::EdgePropertySet {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    key: IStr::new("weight"),
                    value: Value::Float(1.5),
                    old_value: None,
                },
                Change::EdgePropertySet {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    key: IStr::new("color"),
                    value: Value::String(SmolStr::new("red")),
                    old_value: None,
                },
            ],
            ts + 1,
            Origin::Local,
        )
        .unwrap();

        wal.append(
            &[Change::EdgePropertyRemoved {
                edge_id: EdgeId(1),
                source: NodeId(1),
                target: NodeId(2),
                key: IStr::new("color"),
                old_value: Some(Value::String(SmolStr::new("red"))),
            }],
            ts + 2,
            Origin::Local,
        )
        .unwrap();
        drop(wal);

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.edges.len(), 1);

        let edge = &result.edges[0];
        assert_eq!(
            edge.properties.get_by_str("weight"),
            Some(&Value::Float(1.5))
        );
        assert_eq!(
            edge.properties.get_by_str("color"),
            None,
            "removed property should not be present"
        );
    }

    #[test]
    fn recovery_edge_deletion_in_wal() {
        let dir = setup_data_dir();
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();
        let ts = 1_000_000u64;

        wal.append(
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::NodeCreated { node_id: NodeId(2) },
                Change::EdgeCreated {
                    edge_id: EdgeId(1),
                    source: NodeId(1),
                    target: NodeId(2),
                    label: IStr::new("link"),
                },
                Change::EdgeCreated {
                    edge_id: EdgeId(2),
                    source: NodeId(2),
                    target: NodeId(1),
                    label: IStr::new("back"),
                },
            ],
            ts,
            Origin::Local,
        )
        .unwrap();

        wal.append(
            &[Change::EdgeDeleted {
                edge_id: EdgeId(1),
                source: NodeId(1),
                target: NodeId(2),
                label: IStr::new("link"),
            }],
            ts + 1,
            Origin::Local,
        )
        .unwrap();
        drop(wal);

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.nodes.len(), 2);
        assert_eq!(result.edges.len(), 1, "only edge 2 should survive");
        assert_eq!(result.edges[0].id, 2);
    }

    #[test]
    fn recovery_property_remove_in_wal() {
        let dir = setup_data_dir();
        let wal_path = dir.path().join("wal.bin");
        let mut wal = Wal::open(&wal_path, SyncPolicy::EveryEntry).unwrap();
        let ts = 1_000_000u64;

        wal.append(
            &[
                Change::NodeCreated { node_id: NodeId(1) },
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("a"),
                    value: Value::Int(1),
                    old_value: None,
                },
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: IStr::new("b"),
                    value: Value::Int(2),
                    old_value: None,
                },
            ],
            ts,
            Origin::Local,
        )
        .unwrap();

        wal.append(
            &[Change::PropertyRemoved {
                node_id: NodeId(1),
                key: IStr::new("a"),
                old_value: Some(Value::Int(1)),
            }],
            ts + 1,
            Origin::Local,
        )
        .unwrap();
        drop(wal);

        let result = recover(dir.path()).unwrap();
        let node = &result.nodes[0];
        assert_eq!(
            node.properties.get_by_str("a"),
            None,
            "removed property should not be present"
        );
        assert_eq!(
            node.properties.get_by_str("b"),
            Some(&Value::Int(2)),
            "remaining property should still be present"
        );
    }

    #[test]
    fn recovery_extra_sections_round_trip() {
        let dir = setup_data_dir();

        // Snapshot with extra sections (section 5 = version store bytes)
        let extra_data = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE];
        let snap = GraphSnapshot {
            nodes: vec![],
            edges: vec![],
            next_node_id: 1,
            next_edge_id: 1,
            changelog_sequence: 1,
            schemas: crate::snapshot::SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![extra_data.clone()],
        };
        write_snapshot(&snap, &dir.path().join("snapshots/snap-000000000001.snap")).unwrap();

        let result = recover(dir.path()).unwrap();
        assert_eq!(result.extra_sections.len(), 1);
        assert_eq!(result.extra_sections[0], extra_data);
    }
}
