//! Hub-side handler for SyncSubscribe requests from edge nodes.
//!
//! Compiles subscription rules, evaluates against the graph, optionally
//! builds a filtered snapshot, and returns the scope bitmap.

use std::sync::Arc;

use roaring::RoaringBitmap;
use selene_core::changeset::Change;
use selene_graph::SeleneGraph;

use crate::ServerState;

/// Validate a SyncSubscribe request against server-configured limits.
pub fn validate_sync_subscribe(
    state: &Arc<ServerState>,
    request: &selene_wire::dto::sync::SyncSubscribeRequest,
) -> Result<(), String> {
    let config = &state.config.sync;
    if request.subscription.rules.len() > config.max_subscription_rules {
        return Err(format!(
            "subscription has {} rules, max {}",
            request.subscription.rules.len(),
            config.max_subscription_rules
        ));
    }
    for rule in &request.subscription.rules {
        if rule.predicates.len() > config.max_predicates_per_rule {
            return Err(format!(
                "rule has {} predicates, max {}",
                rule.predicates.len(),
                config.max_predicates_per_rule
            ));
        }
        for pred in &rule.predicates {
            if let selene_wire::dto::sync::PropertyPredicateConfig::In { values, .. } = pred
                && values.len() > config.max_in_list_size
            {
                return Err(format!(
                    "IN predicate has {} values, max {}",
                    values.len(),
                    config.max_in_list_size
                ));
            }
        }
    }
    Ok(())
}

/// Build a filtered snapshot of the graph for a subscription.
///
/// Iterates all nodes, evaluates each against the subscription rules,
/// and emits Change variants that reconstruct the matching subgraph:
/// NodeCreated + LabelAdded + PropertySet for matched nodes,
/// EdgeCreated + EdgePropertySet for edges where both endpoints match.
///
/// Returns the changes and the scope bitmap of matched node IDs.
pub(crate) fn build_filtered_snapshot(
    graph: &SeleneGraph,
    def: &crate::subscription::SubscriptionDef,
) -> (Vec<Change>, RoaringBitmap) {
    use crate::subscription::SubscriptionFilter;

    let mut bitmap = RoaringBitmap::new();
    let mut changes: Vec<Change> = Vec::new();

    // Phase 1: Match nodes
    let filter = SubscriptionFilter::new(def, RoaringBitmap::new());
    for node_id in graph.all_node_ids() {
        if filter.evaluate_node(graph, node_id) {
            bitmap.insert(node_id.0 as u32);

            let node = graph.get_node(node_id).unwrap();

            // Emit NodeCreated
            changes.push(Change::NodeCreated { node_id });

            // Emit LabelAdded for each label
            for label in node.labels.iter() {
                changes.push(Change::LabelAdded { node_id, label });
            }

            // Emit PropertySet for each property
            for (key, value) in node.properties.iter() {
                changes.push(Change::PropertySet {
                    node_id,
                    key: *key,
                    value: value.clone(),
                    old_value: None,
                });
            }
        }
    }

    // Phase 2: Match edges (both endpoints in bitmap)
    for edge_id in graph.all_edge_ids() {
        let Some(edge) = graph.get_edge(edge_id) else {
            continue;
        };
        if bitmap.contains(edge.source.0 as u32) && bitmap.contains(edge.target.0 as u32) {
            changes.push(Change::EdgeCreated {
                edge_id,
                source: edge.source,
                target: edge.target,
                label: edge.label,
            });

            for (key, value) in edge.properties.iter() {
                changes.push(Change::EdgePropertySet {
                    edge_id,
                    source: edge.source,
                    target: edge.target,
                    key: *key,
                    value: value.clone(),
                    old_value: None,
                });
            }
        }
    }

    (changes, bitmap)
}

/// Build only the subscription scope bitmap without materializing Change objects.
///
/// Used on the resume path where a full snapshot is not needed. Avoids
/// allocating thousands of Change objects that would be immediately discarded.
fn build_scope_bitmap(
    graph: &SeleneGraph,
    def: &crate::subscription::SubscriptionDef,
) -> RoaringBitmap {
    use crate::subscription::SubscriptionFilter;

    let filter = SubscriptionFilter::new(def, RoaringBitmap::new());
    let mut bitmap = RoaringBitmap::new();
    for node_id in graph.all_node_ids() {
        if filter.evaluate_node(graph, node_id) {
            bitmap.insert(node_id.0 as u32);
        }
    }
    bitmap
}

/// Handle a SyncSubscribe request from an edge node.
///
/// Compiles the subscription, evaluates against the graph, optionally
/// builds a filtered snapshot, and returns the scope bitmap.
pub fn handle_sync_subscribe(
    state: &Arc<ServerState>,
    request: selene_wire::dto::sync::SyncSubscribeRequest,
    auth: &crate::auth::handshake::AuthContext,
) -> selene_wire::dto::sync::SyncSubscribeResponse {
    use crate::auth::Role;
    use crate::subscription::SubscriptionDef;

    let def = SubscriptionDef::compile(&request.subscription);

    // Capture changelog position FIRST, then load graph snapshot.
    // Changes applied between these two points will be delivered
    // again via the changelog stream (idempotent re-application).
    let changelog_seq = {
        let buf = state.persistence.changelog.lock();
        buf.current_sequence()
    };

    // Read the graph via ArcSwap (lock-free snapshot).
    let graph = state.graph.load_snapshot();

    // Check if we need a fresh snapshot
    let needs_snapshot = request.last_pulled_seq == 0 || {
        let buf = state.persistence.changelog.lock();
        buf.since(request.last_pulled_seq).is_none()
    };

    // Check subscription hash for config changes
    let sub_hash = match postcard::to_allocvec(&request.subscription) {
        Ok(bytes) => xxhash_rust::xxh3::xxh3_64(&bytes),
        Err(e) => {
            tracing::warn!(error = %e, "subscription serialization failed, forcing snapshot");
            0 // Sentinel that never matches a valid hash
        }
    };

    let needs_snapshot = needs_snapshot || {
        let hashes = state.sync.peer_subscription_hashes.lock();
        hashes
            .get(&request.peer_name)
            .is_some_and(|&prev| prev != sub_hash)
    };

    let (snapshot, bitmap) = if needs_snapshot {
        let (changes, bitmap) = build_filtered_snapshot(&graph, &def);
        (Some(changes), bitmap)
    } else {
        // Resume: rebuild bitmap without snapshot
        let bitmap = build_scope_bitmap(&graph, &def);
        (None, bitmap)
    };

    // Intersect subscription scope with Cedar auth scope. Non-admin
    // principals can only see nodes within their authorized scope.
    let bitmap = if auth.role == Role::Admin {
        bitmap
    } else {
        let mut scoped = bitmap;
        scoped &= &auth.scope;
        scoped
    };

    let snapshot = snapshot.map(|changes| {
        if auth.role == Role::Admin {
            changes
        } else {
            changes
                .into_iter()
                .filter(|change| match change {
                    Change::NodeCreated { node_id }
                    | Change::NodeDeleted { node_id, .. }
                    | Change::PropertySet { node_id, .. }
                    | Change::PropertyRemoved { node_id, .. }
                    | Change::LabelAdded { node_id, .. }
                    | Change::LabelRemoved { node_id, .. } => bitmap.contains(node_id.0 as u32),
                    Change::EdgeCreated { source, target, .. }
                    | Change::EdgeDeleted { source, target, .. }
                    | Change::EdgePropertySet { source, target, .. }
                    | Change::EdgePropertyRemoved { source, target, .. } => {
                        bitmap.contains(source.0 as u32) && bitmap.contains(target.0 as u32)
                    }
                    // This branch filters the initial snapshot for
                    // non-admin sync peers (role != Admin is the
                    // enclosing condition). Schema mutations are DDL
                    // events — never replicated to scoped peers — so
                    // they drop out here.
                    Change::SchemaMutation(_) => false,
                })
                .collect()
        }
    });

    // Store subscription hash and filter for this peer
    {
        let mut hashes = state.sync.peer_subscription_hashes.lock();
        hashes.insert(request.peer_name.clone(), sub_hash);
    }
    {
        let mut filters = state.sync.peer_sync_filters.lock();
        let filter = crate::subscription::SubscriptionFilter::new(&def, bitmap.clone());
        filters.insert(request.peer_name.clone(), filter);
    }

    // Serialize bitmap
    let mut bitmap_bytes = Vec::new();
    bitmap
        .serialize_into(&mut bitmap_bytes)
        .expect("RoaringBitmap serialize to Vec<u8> is infallible");

    tracing::info!(
        peer = %request.peer_name,
        subscription = %def.name,
        scope_nodes = bitmap.len(),
        has_snapshot = snapshot.is_some(),
        "sync subscribe processed"
    );

    selene_wire::dto::sync::SyncSubscribeResponse {
        snapshot,
        scope_bitmap: bitmap_bytes,
        changelog_seq,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use selene_core::IStr;
    use selene_core::changeset::Change;
    use selene_core::entity::NodeId;
    use selene_core::value::Value;
    use selene_core::{LabelSet, PropertyMap};
    use selene_graph::{SeleneGraph, SharedGraph};
    use selene_wire::dto::sync::{
        SubscriptionConfig, SubscriptionRuleConfig, SyncDirectionConfig, SyncSubscribeRequest,
    };

    use super::*;
    use crate::auth::handshake::AuthContext;
    use crate::bootstrap::ServerState;
    use crate::subscription::SubscriptionDef;
    use crate::sync::push::handle_sync_push;

    fn test_auth() -> AuthContext {
        AuthContext::dev_admin()
    }

    fn push_request(
        peer: &str,
        entries: Vec<selene_wire::dto::sync::SyncEntry>,
    ) -> selene_wire::dto::sync::SyncPushRequest {
        selene_wire::dto::sync::SyncPushRequest {
            peer_name: peer.to_string(),
            entries,
        }
    }

    fn entry(seq: u64, hlc: u64, changes: Vec<Change>) -> selene_wire::dto::sync::SyncEntry {
        selene_wire::dto::sync::SyncEntry {
            sequence: seq,
            hlc_timestamp: hlc,
            changes,
        }
    }

    fn sensor_sub_config() -> SubscriptionConfig {
        SubscriptionConfig {
            name: "sensors".to_string(),
            rules: vec![SubscriptionRuleConfig {
                labels: vec!["Sensor".to_string()],
                predicates: vec![],
            }],
            direction: SyncDirectionConfig::Bidirectional,
        }
    }

    // 18 ────────────────────────────────────────────────────────────────
    /// Matched nodes produce NodeCreated + LabelAdded + PropertySet changes.
    #[test]
    fn filtered_snapshot_matches_sensor_nodes() {
        let g = SeleneGraph::new();
        let shared = SharedGraph::new(g);

        // Create two Sensor nodes and one non-matching node.
        let (ids, _) = shared
            .write(|m| {
                let mut props1 = PropertyMap::new();
                props1.insert(IStr::new("temp"), Value::Float(22.0));
                let s1 = m.create_node(LabelSet::from_strs(&["Sensor"]), props1)?;

                let mut props2 = PropertyMap::new();
                props2.insert(IStr::new("temp"), Value::Float(18.5));
                let s2 = m.create_node(LabelSet::from_strs(&["Sensor"]), props2)?;

                // Non-matching node
                let _ = m.create_node(LabelSet::from_strs(&["Equipment"]), PropertyMap::new())?;

                Ok((s1, s2))
            })
            .unwrap();

        let config = sensor_sub_config();
        let def = SubscriptionDef::compile(&config);
        let graph = shared.load_snapshot();
        let (changes, bitmap) = build_filtered_snapshot(&graph, &def);

        // Bitmap should contain exactly the two sensor nodes.
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.contains(ids.0.0 as u32));
        assert!(bitmap.contains(ids.1.0 as u32));

        // Changes must include NodeCreated for each matched node.
        let created_ids: Vec<u64> = changes
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
            created_ids.contains(&ids.0.0),
            "sensor 1 NodeCreated missing"
        );
        assert!(
            created_ids.contains(&ids.1.0),
            "sensor 2 NodeCreated missing"
        );

        // Changes must include PropertySet for the temp property.
        let prop_changes: Vec<_> = changes
            .iter()
            .filter(|c| matches!(c, Change::PropertySet { key, .. } if key.as_str() == "temp"))
            .collect();
        assert_eq!(
            prop_changes.len(),
            2,
            "each sensor must have a temp PropertySet"
        );

        // Equipment node must NOT appear.
        let has_equipment = changes.iter().any(|c| match c {
            Change::LabelAdded { label, .. } => label.as_str() == "Equipment",
            _ => false,
        });
        assert!(!has_equipment, "Equipment node must be excluded");
    }

    // 19 ────────────────────────────────────────────────────────────────
    /// Edges are included only when both endpoints are in the bitmap.
    #[test]
    fn filtered_snapshot_includes_edges_between_matched_nodes() {
        let g = SeleneGraph::new();
        let shared = SharedGraph::new(g);

        let (ids, _) = shared
            .write(|m| {
                let s1 = m.create_node(LabelSet::from_strs(&["Sensor"]), PropertyMap::new())?;
                let s2 = m.create_node(LabelSet::from_strs(&["Sensor"]), PropertyMap::new())?;
                // Non-matching node
                let other =
                    m.create_node(LabelSet::from_strs(&["Equipment"]), PropertyMap::new())?;
                // Edge between the two sensors
                let e1 = m.create_edge(s1, IStr::new("feeds"), s2, PropertyMap::new())?;
                // Edge from sensor to equipment (should be excluded)
                let e2 = m.create_edge(s1, IStr::new("controls"), other, PropertyMap::new())?;
                Ok((s1, s2, other, e1, e2))
            })
            .unwrap();

        let config = sensor_sub_config();
        let def = SubscriptionDef::compile(&config);
        let graph = shared.load_snapshot();
        let (changes, bitmap) = build_filtered_snapshot(&graph, &def);

        // Only the two Sensor nodes in bitmap.
        assert_eq!(bitmap.len(), 2);

        // The sensor-to-sensor edge must appear.
        let has_feeds = changes.iter().any(|c| match c {
            Change::EdgeCreated { edge_id, .. } => *edge_id == ids.3,
            _ => false,
        });
        assert!(has_feeds, "sensor-to-sensor edge must be included");

        // The sensor-to-equipment edge must NOT appear.
        let has_controls = changes.iter().any(|c| match c {
            Change::EdgeCreated { edge_id, .. } => *edge_id == ids.4,
            _ => false,
        });
        assert!(!has_controls, "sensor-to-equipment edge must be excluded");

        // Suppress unused variable warning
        let _ = ids.2;
    }

    // 20 ────────────────────────────────────────────────────────────────
    /// An empty graph produces no changes and an empty bitmap.
    #[test]
    fn filtered_snapshot_empty_graph() {
        let g = SeleneGraph::new();
        let shared = SharedGraph::new(g);
        let config = sensor_sub_config();
        let def = SubscriptionDef::compile(&config);
        let graph = shared.load_snapshot();
        let (changes, bitmap) = build_filtered_snapshot(&graph, &def);
        assert!(changes.is_empty());
        assert!(bitmap.is_empty());
    }

    // 21 ────────────────────────────────────────────────────────────────
    /// handle_sync_subscribe: first call (seq=0) returns a snapshot.
    #[tokio::test]
    async fn subscribe_first_call_returns_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // Populate the graph with a sensor node via sync push.
        handle_sync_push(
            &state,
            push_request(
                "hub",
                vec![entry(
                    1,
                    1000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(10),
                        },
                        Change::LabelAdded {
                            node_id: NodeId(10),
                            label: IStr::new("Sensor"),
                        },
                    ],
                )],
            ),
            &auth,
        );

        let request = SyncSubscribeRequest {
            peer_name: "edge-01".to_string(),
            subscription: sensor_sub_config(),
            last_pulled_seq: 0, // first call
        };

        let resp = handle_sync_subscribe(&state, request, &auth);

        // First call must return a snapshot.
        assert!(resp.snapshot.is_some(), "first call must return a snapshot");
        let snapshot = resp.snapshot.unwrap();
        // The sensor node must appear in the snapshot.
        assert!(snapshot.iter().any(|c| matches!(c,
            Change::NodeCreated { node_id } if *node_id == NodeId(10)
        )));
        // Bitmap bytes must be non-empty (one node matched).
        assert!(!resp.scope_bitmap.is_empty());
    }

    // 22 ────────────────────────────────────────────────────────────────
    /// handle_sync_subscribe: second call with same seq returns no snapshot.
    #[tokio::test]
    async fn subscribe_resume_returns_no_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);
        let auth = test_auth();

        // Populate the graph.
        handle_sync_push(
            &state,
            push_request(
                "hub",
                vec![entry(
                    1,
                    1000,
                    vec![
                        Change::NodeCreated {
                            node_id: NodeId(20),
                        },
                        Change::LabelAdded {
                            node_id: NodeId(20),
                            label: IStr::new("Sensor"),
                        },
                    ],
                )],
            ),
            &auth,
        );

        // First subscribe to get the initial snapshot and learn the seq.
        let resp1 = handle_sync_subscribe(
            &state,
            SyncSubscribeRequest {
                peer_name: "edge-02".to_string(),
                subscription: sensor_sub_config(),
                last_pulled_seq: 0,
            },
            &auth,
        );
        assert!(resp1.snapshot.is_some());
        let seq = resp1.changelog_seq;

        // Second subscribe with the current seq (no new history dropped).
        // Since the changelog still has entries since `seq`, no snapshot needed.
        let resp2 = handle_sync_subscribe(
            &state,
            SyncSubscribeRequest {
                peer_name: "edge-02".to_string(),
                subscription: sensor_sub_config(),
                last_pulled_seq: seq,
            },
            &auth,
        );
        // Changelog has not been compacted, so since(seq) returns Some.
        // Subscription hash is unchanged, so no snapshot.
        assert!(
            resp2.snapshot.is_none(),
            "resume must not return a snapshot"
        );
    }

    // ── validate_sync_subscribe tests ────────────────────────────────

    // 33 ────────────────────────────────────────────────────────────────
    /// validate_sync_subscribe: valid request within all limits passes.
    #[tokio::test]
    async fn validate_sync_subscribe_valid_request() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        let request = SyncSubscribeRequest {
            peer_name: "edge-01".to_string(),
            subscription: sensor_sub_config(),
            last_pulled_seq: 0,
        };

        let result = validate_sync_subscribe(&state, &request);
        assert!(result.is_ok());
    }

    // 34 ────────────────────────────────────────────────────────────────
    /// validate_sync_subscribe: too many rules rejected.
    #[tokio::test]
    async fn validate_sync_subscribe_too_many_rules() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        let max_rules = state.config.sync.max_subscription_rules;
        let rules: Vec<SubscriptionRuleConfig> = (0..=max_rules)
            .map(|_| SubscriptionRuleConfig {
                labels: vec!["Sensor".to_string()],
                predicates: vec![],
            })
            .collect();

        let request = SyncSubscribeRequest {
            peer_name: "edge-01".to_string(),
            subscription: SubscriptionConfig {
                name: "too-many".to_string(),
                rules,
                direction: SyncDirectionConfig::Bidirectional,
            },
            last_pulled_seq: 0,
        };

        let result = validate_sync_subscribe(&state, &request);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("rules"), "error must mention rules: {err}");
    }

    // 35 ────────────────────────────────────────────────────────────────
    /// validate_sync_subscribe: too many predicates per rule rejected.
    #[tokio::test]
    async fn validate_sync_subscribe_too_many_predicates() {
        use selene_wire::dto::sync::PropertyPredicateConfig;

        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        let max_preds = state.config.sync.max_predicates_per_rule;
        let predicates: Vec<PropertyPredicateConfig> = (0..=max_preds)
            .map(|i| PropertyPredicateConfig::Eq {
                key: format!("key_{i}"),
                value: Value::Int(i as i64),
            })
            .collect();

        let request = SyncSubscribeRequest {
            peer_name: "edge-01".to_string(),
            subscription: SubscriptionConfig {
                name: "many-preds".to_string(),
                rules: vec![SubscriptionRuleConfig {
                    labels: vec!["Sensor".to_string()],
                    predicates,
                }],
                direction: SyncDirectionConfig::Bidirectional,
            },
            last_pulled_seq: 0,
        };

        let result = validate_sync_subscribe(&state, &request);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("predicates"),
            "error must mention predicates: {err}"
        );
    }

    // 36 ────────────────────────────────────────────────────────────────
    /// validate_sync_subscribe: IN predicate with too many values rejected.
    #[tokio::test]
    async fn validate_sync_subscribe_in_list_too_large() {
        use selene_wire::dto::sync::PropertyPredicateConfig;

        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(ServerState::for_testing(dir.path()).await);

        let max_in = state.config.sync.max_in_list_size;
        let values: Vec<Value> = (0..=max_in as i64).map(Value::Int).collect();

        let request = SyncSubscribeRequest {
            peer_name: "edge-01".to_string(),
            subscription: SubscriptionConfig {
                name: "big-in".to_string(),
                rules: vec![SubscriptionRuleConfig {
                    labels: vec!["Sensor".to_string()],
                    predicates: vec![PropertyPredicateConfig::In {
                        key: "zone".to_string(),
                        values,
                    }],
                }],
                direction: SyncDirectionConfig::Bidirectional,
            },
            last_pulled_seq: 0,
        };

        let result = validate_sync_subscribe(&state, &request);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("values"),
            "error must mention IN list values: {err}"
        );
    }
}
