//! Integration tests for Phase 5C partial graph sync subscriptions.
//!
//! Tests the `handle_sync_subscribe` handler directly without a QUIC
//! connection. Covers the subscription handshake, filtered snapshot
//! contents, edge boundary filtering, backward compatibility, and
//! subscription config change detection.

use std::sync::Arc;

use roaring::RoaringBitmap;
use selene_core::IStr;
use selene_core::changeset::Change;
use selene_core::entity::{EdgeId, NodeId};
use selene_wire::dto::sync::{
    SubscriptionConfig, SubscriptionRuleConfig, SyncDirectionConfig, SyncSubscribeRequest,
};

use selene_server::auth::handshake::AuthContext;
use selene_server::bootstrap::ServerState;
use selene_server::config::SyncConfig;
use selene_server::handle_sync_subscribe;

fn admin() -> AuthContext {
    AuthContext::dev_admin()
}

/// Build a simple sensor subscription that matches nodes labelled "Sensor"
/// with building = "HQ".
fn sensor_sub(name: &str) -> SubscriptionConfig {
    SubscriptionConfig {
        name: name.to_string(),
        rules: vec![SubscriptionRuleConfig {
            labels: vec!["Sensor".to_string()],
            predicates: vec![],
        }],
        direction: SyncDirectionConfig::Bidirectional,
    }
}

/// Build a subscription that matches nodes labelled "Equipment".
fn equipment_sub(name: &str) -> SubscriptionConfig {
    SubscriptionConfig {
        name: name.to_string(),
        rules: vec![SubscriptionRuleConfig {
            labels: vec!["Equipment".to_string()],
            predicates: vec![],
        }],
        direction: SyncDirectionConfig::Bidirectional,
    }
}

// ── Helper: insert a node directly into the graph via sync push ───────────────

fn insert_node_via_push(state: &Arc<ServerState>, node_id: u64, label: &str, auth: &AuthContext) {
    selene_server::handle_sync_push(
        state,
        selene_wire::dto::sync::SyncPushRequest {
            peer_name: "test-setup".to_string(),
            entries: vec![selene_wire::dto::sync::SyncEntry {
                sequence: node_id,
                hlc_timestamp: node_id * 1000,
                changes: vec![
                    Change::NodeCreated {
                        node_id: NodeId(node_id),
                    },
                    Change::LabelAdded {
                        node_id: NodeId(node_id),
                        label: IStr::new(label),
                    },
                ],
            }],
        },
        auth,
    );
}

fn insert_edge_via_push(
    state: &Arc<ServerState>,
    edge_id: u64,
    source: u64,
    target: u64,
    label: &str,
    hlc: u64,
    auth: &AuthContext,
) {
    selene_server::handle_sync_push(
        state,
        selene_wire::dto::sync::SyncPushRequest {
            peer_name: "test-setup".to_string(),
            entries: vec![selene_wire::dto::sync::SyncEntry {
                sequence: hlc,
                hlc_timestamp: hlc * 1000,
                changes: vec![Change::EdgeCreated {
                    edge_id: EdgeId(edge_id),
                    source: NodeId(source),
                    target: NodeId(target),
                    label: IStr::new(label),
                }],
            }],
        },
        auth,
    );
}

// ── Test 1: subscribe_handshake_fresh ────────────────────────────────────────

/// First subscription with `last_pulled_seq = 0` must return a filtered
/// snapshot that contains only nodes matching the subscription.
#[tokio::test]
async fn subscribe_handshake_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(ServerState::for_testing(dir.path()).await);
    let auth = admin();

    // Node 1: Sensor -- matches subscription
    insert_node_via_push(&state, 1, "Sensor", &auth);
    // Node 2: Sensor -- also matches
    insert_node_via_push(&state, 2, "Sensor", &auth);
    // Node 3: Building -- out of scope
    insert_node_via_push(&state, 3, "Building", &auth);

    let request = SyncSubscribeRequest {
        peer_name: "edge-01".to_string(),
        subscription: sensor_sub("sensors-only"),
        last_pulled_seq: 0,
    };

    let resp = handle_sync_subscribe(&state, request, &auth);

    // A fresh connection must receive a snapshot.
    assert!(
        resp.snapshot.is_some(),
        "fresh handshake must include a snapshot"
    );

    let snapshot = resp.snapshot.unwrap();

    // Both sensor nodes should be present.
    let node_ids_in_snapshot: Vec<u64> = snapshot
        .iter()
        .filter_map(|c| {
            if let Change::NodeCreated { node_id } = c {
                Some(node_id.0)
            } else {
                None
            }
        })
        .collect();

    assert!(
        node_ids_in_snapshot.contains(&1),
        "sensor node 1 must be in snapshot"
    );
    assert!(
        node_ids_in_snapshot.contains(&2),
        "sensor node 2 must be in snapshot"
    );

    // The Building node must NOT appear.
    assert!(
        !node_ids_in_snapshot.contains(&3),
        "Building node 3 must be excluded from snapshot"
    );

    // Bitmap must contain only the two sensor nodes.
    let bitmap =
        RoaringBitmap::deserialize_from(&resp.scope_bitmap[..]).expect("valid bitmap bytes");
    assert!(bitmap.contains(1), "bitmap must include node 1");
    assert!(bitmap.contains(2), "bitmap must include node 2");
    assert!(!bitmap.contains(3), "bitmap must exclude node 3");
    assert_eq!(bitmap.len(), 2, "bitmap should contain exactly two nodes");
}

// ── Test 2: subscribe_handshake_excludes_edges_across_boundary ───────────────

/// Edges that cross the subscription boundary (one endpoint out of scope)
/// must not appear in the snapshot. Edges with both endpoints in scope are
/// included.
#[tokio::test]
async fn subscribe_handshake_excludes_edges_across_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(ServerState::for_testing(dir.path()).await);
    let auth = admin();

    // Node 10: Sensor (in scope)
    insert_node_via_push(&state, 10, "Sensor", &auth);
    // Node 20: Sensor (in scope)
    insert_node_via_push(&state, 20, "Sensor", &auth);
    // Node 30: Building (out of scope)
    insert_node_via_push(&state, 30, "Building", &auth);

    // Edge 1: 10 -> 20 (both in scope -- should appear)
    insert_edge_via_push(&state, 1, 10, 20, "feeds", 100, &auth);
    // Edge 2: 10 -> 30 (one endpoint out of scope -- should be excluded)
    insert_edge_via_push(&state, 2, 10, 30, "located_in", 101, &auth);

    let request = SyncSubscribeRequest {
        peer_name: "edge-02".to_string(),
        subscription: sensor_sub("sensor-scope"),
        last_pulled_seq: 0,
    };

    let resp = handle_sync_subscribe(&state, request, &auth);
    assert!(resp.snapshot.is_some());
    let snapshot = resp.snapshot.unwrap();

    let edge_ids_in_snapshot: Vec<u64> = snapshot
        .iter()
        .filter_map(|c| {
            if let Change::EdgeCreated { edge_id, .. } = c {
                Some(edge_id.0)
            } else {
                None
            }
        })
        .collect();

    assert!(
        edge_ids_in_snapshot.contains(&1),
        "edge 1 (both endpoints in scope) must be in snapshot"
    );
    assert!(
        !edge_ids_in_snapshot.contains(&2),
        "edge 2 (crosses boundary) must be excluded from snapshot"
    );
}

// ── Test 3: no_subscription_means_full_sync ──────────────────────────────────

/// `SyncConfig::default()` must have an empty `subscriptions` list.
/// This confirms that Phase 5B nodes (no subscriptions configured) are
/// unaffected by Phase 5C.
#[test]
fn no_subscription_means_full_sync() {
    let sync = SyncConfig::default();
    assert!(
        sync.subscriptions.is_empty(),
        "default SyncConfig must have no subscriptions (Phase 5B backward compatibility)"
    );
}

// ── Test 4: subscribe_config_change_forces_snapshot ──────────────────────────

/// Three calls from the same peer:
/// 1. First call (seq=0) receives a snapshot.
/// 2. Second call with the same subscription and current seq -- no snapshot
///    (resume path).
/// 3. Third call with a *different* subscription -- forces a new snapshot
///    because the config hash changed.
#[tokio::test]
async fn subscribe_config_change_forces_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(ServerState::for_testing(dir.path()).await);
    let auth = admin();

    // Populate the graph with one Sensor and one Equipment node.
    insert_node_via_push(&state, 1, "Sensor", &auth);
    insert_node_via_push(&state, 2, "Equipment", &auth);

    // Call 1: fresh subscribe with sensor subscription.
    let resp1 = handle_sync_subscribe(
        &state,
        SyncSubscribeRequest {
            peer_name: "edge-03".to_string(),
            subscription: sensor_sub("sub-v1"),
            last_pulled_seq: 0,
        },
        &auth,
    );
    assert!(
        resp1.snapshot.is_some(),
        "call 1 (fresh) must return snapshot"
    );
    let seq_after_first = resp1.changelog_seq;

    // Call 2: resume with same subscription and current seq.
    let resp2 = handle_sync_subscribe(
        &state,
        SyncSubscribeRequest {
            peer_name: "edge-03".to_string(),
            subscription: sensor_sub("sub-v1"),
            last_pulled_seq: seq_after_first,
        },
        &auth,
    );
    assert!(
        resp2.snapshot.is_none(),
        "call 2 (resume, same config) must not return snapshot"
    );

    // Call 3: different subscription config forces a new snapshot.
    let resp3 = handle_sync_subscribe(
        &state,
        SyncSubscribeRequest {
            peer_name: "edge-03".to_string(),
            subscription: equipment_sub("sub-v2"), // different name and label
            last_pulled_seq: seq_after_first,
        },
        &auth,
    );
    assert!(
        resp3.snapshot.is_some(),
        "call 3 (config changed) must force a fresh snapshot"
    );

    // The new snapshot should contain the Equipment node, not the Sensor node.
    let snapshot3 = resp3.snapshot.unwrap();
    let node_ids: Vec<u64> = snapshot3
        .iter()
        .filter_map(|c| {
            if let Change::NodeCreated { node_id } = c {
                Some(node_id.0)
            } else {
                None
            }
        })
        .collect();

    assert!(
        node_ids.contains(&2),
        "equipment node 2 must be in new snapshot"
    );
    assert!(
        !node_ids.contains(&1),
        "sensor node 1 must be excluded from equipment snapshot"
    );
}

// ── Test 5: validate_rejects_oversized_subscription ──────────────────────────

/// Subscriptions with more than `max_subscription_rules` (default 50) rules
/// must be rejected by `validate_sync_subscribe` with a descriptive error.
#[tokio::test]
async fn validate_rejects_oversized_subscription() {
    let tmp = tempfile::tempdir().unwrap();
    let state = std::sync::Arc::new(ServerState::for_testing(tmp.path()).await);

    // Build a subscription with 51 rules (default limit is 50).
    let rules: Vec<_> = (0..51)
        .map(|i| SubscriptionRuleConfig {
            labels: vec![format!("Label{i}")],
            predicates: vec![],
        })
        .collect();

    let request = SyncSubscribeRequest {
        peer_name: "edge-test".to_string(),
        subscription: SubscriptionConfig {
            name: "oversized".to_string(),
            rules,
            direction: SyncDirectionConfig::Bidirectional,
        },
        last_pulled_seq: 0,
    };

    let result = selene_server::validate_sync_subscribe(&state, &request);
    assert!(result.is_err(), "51 rules must be rejected");
    assert!(
        result.unwrap_err().contains("51 rules"),
        "error message must mention the rule count"
    );
}

// ── Test 6: validate_rejects_oversized_in_list ────────────────────────────────

/// IN predicates with more than `max_in_list_size` (default 1000) values
/// must be rejected by `validate_sync_subscribe` with a descriptive error.
#[tokio::test]
async fn validate_rejects_oversized_in_list() {
    let tmp = tempfile::tempdir().unwrap();
    let state = std::sync::Arc::new(ServerState::for_testing(tmp.path()).await);

    let values: Vec<_> = (0..1001).map(selene_core::Value::Int).collect();

    let request = SyncSubscribeRequest {
        peer_name: "edge-test".to_string(),
        subscription: SubscriptionConfig {
            name: "big-in".to_string(),
            rules: vec![SubscriptionRuleConfig {
                labels: vec!["Sensor".to_string()],
                predicates: vec![selene_wire::dto::sync::PropertyPredicateConfig::In {
                    key: "id".to_string(),
                    values,
                }],
            }],
            direction: SyncDirectionConfig::Bidirectional,
        },
        last_pulled_seq: 0,
    };

    let result = selene_server::validate_sync_subscribe(&state, &request);
    assert!(result.is_err(), "1001-value IN list must be rejected");
    assert!(
        result.unwrap_err().contains("1001 values"),
        "error message must mention the value count"
    );
}
