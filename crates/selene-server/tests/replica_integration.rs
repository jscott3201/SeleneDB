//! Integration test: verify change applier replicates data correctly
//! and replicas reject mutations.

use selene_core::IStr;
use selene_core::Value;
use selene_core::changeset::Change;
use selene_core::entity::{EdgeId, NodeId};

#[tokio::test]
async fn replica_applies_primary_changes() {
    let primary_dir = tempfile::tempdir().unwrap();
    let primary = selene_server::ServerState::for_testing(primary_dir.path()).await;

    // Create data on primary via ops
    let auth = selene_server::auth::handshake::AuthContext::dev_admin();
    let _ = selene_server::ops::nodes::create_node(
        &primary,
        &auth,
        selene_core::LabelSet::from_strs(&["sensor"]),
        selene_core::PropertyMap::from_pairs(vec![(
            selene_core::IStr::new("name"),
            Value::str("Zone-A Temp"),
        )]),
        None,
    );
    let _ = selene_server::ops::nodes::create_node(
        &primary,
        &auth,
        selene_core::LabelSet::from_strs(&["building"]),
        selene_core::PropertyMap::from_pairs(vec![(
            selene_core::IStr::new("name"),
            Value::str("HQ"),
        )]),
        None,
    );
    let _ = selene_server::ops::edges::create_edge(
        &primary,
        &auth,
        2,
        1,
        selene_core::IStr::new("contains"),
        selene_core::PropertyMap::new(),
    );

    // Read changelog from primary
    let entries = primary.changelog().lock().since(0).unwrap();
    assert!(!entries.is_empty(), "primary should have changelog entries");

    // Create replica
    let replica_dir = tempfile::tempdir().unwrap();
    let mut replica = selene_server::ServerState::for_testing(replica_dir.path()).await;
    replica.set_replica(true);

    // Apply primary's changelog entries to replica
    for entry in &entries {
        selene_server::replica::apply_entry_to_replica(&replica, entry);
    }

    // Verify replica has same data
    let primary_nodes = primary.graph().read(|g| g.node_count());
    let replica_nodes = replica.graph().read(|g| g.node_count());
    assert_eq!(primary_nodes, replica_nodes, "node count mismatch");

    let primary_edges = primary.graph().read(|g| g.edge_count());
    let replica_edges = replica.graph().read(|g| g.edge_count());
    assert_eq!(primary_edges, replica_edges, "edge count mismatch");

    // Verify specific node properties
    let replica_name = replica.graph().read(|g| {
        g.get_node(NodeId(1))
            .unwrap()
            .properties
            .get(selene_core::IStr::new("name"))
            .cloned()
    });
    assert_eq!(replica_name, Some(Value::str("Zone-A Temp")));

    // Verify edge exists
    let edge_exists = replica.graph().read(|g| g.get_edge(EdgeId(1)).is_some());
    assert!(edge_exists, "edge should exist on replica");
}

#[tokio::test]
async fn replica_rejects_gql_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = selene_server::ServerState::for_testing(dir.path()).await;
    state.set_replica(true);

    let auth = selene_server::auth::handshake::AuthContext::dev_admin();
    let result = selene_server::ops::gql::execute_gql(
        &state,
        &auth,
        "INSERT (:sensor {name: 'test'})",
        None,
        false,
        false,
        selene_server::ops::gql::ResultFormat::Json,
    );

    // Should get an error result (not "00000" success)
    if let Ok(r) = result {
        assert_ne!(
            r.status_code, "00000",
            "mutation should fail on replica, got success"
        );
    } else {
        // Also acceptable — mutation rejected at ops layer
    }
}

#[tokio::test]
async fn replica_allows_gql_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = selene_server::ServerState::for_testing(dir.path()).await;
    state.set_replica(true);

    let auth = selene_server::auth::handshake::AuthContext::dev_admin();

    // Read queries should work fine on replicas
    let result = selene_server::ops::gql::execute_gql(
        &state,
        &auth,
        "MATCH (n) RETURN count(n) AS cnt",
        None,
        false,
        false,
        selene_server::ops::gql::ResultFormat::Json,
    );

    match result {
        Ok(r) => {
            assert_eq!(
                r.status_code, "00000",
                "read query should succeed on replica"
            );
        }
        Err(e) => {
            panic!("read query should not fail on replica: {e:?}");
        }
    }
}

#[tokio::test]
async fn replica_changelog_is_populated() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = selene_server::ServerState::for_testing(dir.path()).await;
    state.set_replica(true);

    // Apply some changes
    let entry = selene_graph::changelog::ChangelogEntry {
        sequence: 1,
        timestamp_nanos: 1_000_000_000,
        hlc_timestamp: 0,
        changes: vec![
            Change::NodeCreated { node_id: NodeId(1) },
            Change::LabelAdded {
                node_id: NodeId(1),
                label: IStr::new("sensor"),
            },
        ],
    };
    selene_server::replica::apply_entry_to_replica(&state, &entry);

    // Verify the replica's own changelog buffer was populated
    let entries = state.changelog().lock().since(0).unwrap();
    assert_eq!(entries.len(), 1, "replica changelog should have 1 entry");
    assert_eq!(entries[0].changes.len(), 2, "entry should have 2 changes");
}
