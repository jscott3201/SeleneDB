//! Unit tests for the ops layer — node/edge CRUD, TS, query, schema, health.
//!
//! Uses `ServerState::for_testing()` to create a minimal server state
//! without QUIC/TLS/Cedar policy files.

mod support;

use std::sync::Arc;

use selene_core::{IStr, PropertyMap, Value};
use selene_server::bootstrap::ServerState;
use selene_server::ops;
use smol_str::SmolStr;
use support::*;

// ── Node CRUD ────────────────────────────────────────────────────────

#[tokio::test]
async fn create_and_get_node() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let node = ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor", "temperature"]),
        props(&[("unit", Value::String(SmolStr::new("°F")))]),
        None,
    )
    .unwrap();

    assert_eq!(node.id, 1);
    assert!(node.labels.contains(&"sensor".to_string()));
    assert_eq!(
        node.properties.get("unit"),
        Some(&Value::String(SmolStr::new("°F")))
    );

    let fetched = ops::nodes::get_node(&state, &admin(), 1).unwrap();
    assert_eq!(fetched.id, node.id);
}

#[tokio::test]
async fn create_node_not_found_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let err = ops::nodes::get_node(&state, &admin(), 999);
    assert!(matches!(err, Err(ops::OpError::NotFound { .. })));
}

#[tokio::test]
async fn modify_node_properties_and_labels() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let modified = ops::nodes::modify_node(
        &state,
        &admin(),
        1,
        vec![(IStr::new("name"), Value::String(SmolStr::new("Sensor-1")))],
        vec![],
        vec![IStr::new("active")],
        vec![],
    )
    .unwrap();

    assert!(modified.labels.contains(&"active".to_string()));
    assert_eq!(
        modified.properties.get("name"),
        Some(&Value::String(SmolStr::new("Sensor-1")))
    );
}

#[tokio::test]
async fn delete_node_removes_it() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::nodes::delete_node(&state, &admin(), 1).unwrap();

    assert!(matches!(
        ops::nodes::get_node(&state, &admin(), 1),
        Err(ops::OpError::NotFound { .. })
    ));
}

#[tokio::test]
async fn list_nodes_with_label_filter() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["building"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let all = ops::nodes::list_nodes(&state, &admin(), None, 100, 0).unwrap();
    assert_eq!(all.total, 3);

    let sensors = ops::nodes::list_nodes(&state, &admin(), Some("sensor"), 100, 0).unwrap();
    assert_eq!(sensors.total, 2);

    let paged = ops::nodes::list_nodes(&state, &admin(), None, 1, 1).unwrap();
    assert_eq!(paged.nodes.len(), 1);
    assert_eq!(paged.total, 3);
}

#[tokio::test]
async fn create_node_with_parent_creates_containment_edge() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["building"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        Some(1),
    )
    .unwrap();

    let edges = ops::edges::list_edges(&state, &admin(), Some("contains"), 100, 0).unwrap();
    assert_eq!(edges.total, 1);
    assert_eq!(edges.edges[0].source, 1);
    assert_eq!(edges.edges[0].target, 2);
}

// ── Edge CRUD ────────────────────────────────────────────────────────

#[tokio::test]
async fn create_and_get_edge() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(&state, &admin(), labels(&["a"]), PropertyMap::new(), None).unwrap();
    ops::nodes::create_node(&state, &admin(), labels(&["b"]), PropertyMap::new(), None).unwrap();

    let edge = ops::edges::create_edge(
        &state,
        &admin(),
        1,
        2,
        IStr::new("feeds"),
        props(&[("medium", Value::String(SmolStr::new("air")))]),
        false,
    )
    .unwrap();

    assert_eq!(edge.source, 1);
    assert_eq!(edge.target, 2);
    assert_eq!(edge.label, "feeds");

    let fetched = ops::edges::get_edge(&state, &admin(), edge.id).unwrap();
    assert_eq!(fetched.label, "feeds");
}

#[tokio::test]
async fn modify_edge_properties() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(&state, &admin(), labels(&["a"]), PropertyMap::new(), None).unwrap();
    ops::nodes::create_node(&state, &admin(), labels(&["b"]), PropertyMap::new(), None).unwrap();
    ops::edges::create_edge(
        &state,
        &admin(),
        1,
        2,
        IStr::new("feeds"),
        PropertyMap::new(),
        false,
    )
    .unwrap();

    let modified = ops::edges::modify_edge(
        &state,
        &admin(),
        1,
        vec![(IStr::new("weight"), Value::Float(2.5))],
        vec![],
    )
    .unwrap();

    assert_eq!(modified.properties.get("weight"), Some(&Value::Float(2.5)));
}

#[tokio::test]
async fn delete_edge() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(&state, &admin(), labels(&["a"]), PropertyMap::new(), None).unwrap();
    ops::nodes::create_node(&state, &admin(), labels(&["b"]), PropertyMap::new(), None).unwrap();
    ops::edges::create_edge(
        &state,
        &admin(),
        1,
        2,
        IStr::new("feeds"),
        PropertyMap::new(),
        false,
    )
    .unwrap();

    ops::edges::delete_edge(&state, &admin(), 1).unwrap();
    assert!(matches!(
        ops::edges::get_edge(&state, &admin(), 1),
        Err(ops::OpError::NotFound { .. })
    ));
}

// ── Auth Scope Enforcement ───────────────────────────────────────────

#[tokio::test]
async fn scoped_user_can_read_in_scope() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let auth = scoped(&[1]);
    let node = ops::nodes::get_node(&state, &auth, 1).unwrap();
    assert_eq!(node.id, 1);
}

#[tokio::test]
async fn scoped_user_denied_out_of_scope() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let auth = scoped(&[999]); // node 1 not in scope
    assert!(matches!(
        ops::nodes::get_node(&state, &auth, 1),
        Err(ops::OpError::AuthDenied)
    ));
}

#[tokio::test]
async fn scoped_list_filters_by_scope() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let auth = scoped(&[1, 3]); // only nodes 1 and 3
    let result = ops::nodes::list_nodes(&state, &auth, None, 100, 0).unwrap();
    assert_eq!(result.total, 2);
}

// ── Time-Series ──────────────────────────────────────────────────────

#[tokio::test]
async fn ts_write_and_range() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let samples = vec![
        selene_wire::dto::ts::TsSampleDto {
            entity_id: 1,
            property: "temp".into(),
            timestamp_nanos: 1000,
            value: 72.5,
        },
        selene_wire::dto::ts::TsSampleDto {
            entity_id: 1,
            property: "temp".into(),
            timestamp_nanos: 2000,
            value: 73.0,
        },
    ];

    let count = ops::ts::ts_write(&state, &admin(), samples).unwrap();
    assert_eq!(count, 2);

    let readings = ops::ts::ts_range(&state, &admin(), 1, "temp", 0, i64::MAX, None).unwrap();
    assert_eq!(readings.len(), 2);
    assert_eq!(readings[0].value, 72.5);
    assert_eq!(readings[1].value, 73.0);
}

// ── Schema Ops ───────────────────────────────────────────────────────

#[tokio::test]
async fn register_and_list_schemas() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let schema = selene_core::schema::NodeSchema {
        label: Arc::from("test_sensor"),
        parent: None,
        properties: vec![{
            let mut p = selene_core::schema::PropertyDef::simple(
                "unit",
                selene_core::schema::ValueType::String,
                false,
            );
            p.default = Some(Value::String(SmolStr::new("°C")));
            p
        }],
        valid_edge_labels: vec![],
        description: "Test sensor".into(),
        annotations: std::collections::HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    ops::schema::register_node_schema(&state, &admin(), schema).unwrap();

    let schemas = ops::schema::list_node_schemas(&state, &admin()).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(&*schemas[0].label, "test_sensor");

    let fetched = ops::schema::get_node_schema(&state, &admin(), "test_sensor").unwrap();
    assert_eq!(fetched.properties.len(), 1);
}

#[tokio::test]
async fn schema_defaults_applied_on_create() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let schema = selene_core::schema::NodeSchema {
        label: Arc::from("sensor"),
        parent: None,
        properties: vec![{
            let mut p = selene_core::schema::PropertyDef::simple(
                "unit",
                selene_core::schema::ValueType::String,
                false,
            );
            p.default = Some(Value::String(SmolStr::new("°F")));
            p
        }],
        valid_edge_labels: vec![],
        description: String::new(),
        annotations: std::collections::HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    ops::schema::register_node_schema(&state, &admin(), schema).unwrap();

    // Create node without specifying "unit" — should get default
    let node = ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(), // no properties
        None,
    )
    .unwrap();

    assert_eq!(
        node.properties.get("unit"),
        Some(&Value::String(SmolStr::new("°F")))
    );
}

// ── Health ────────────────────────────────────────────────────────────

#[tokio::test]
async fn health_returns_counts() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["building"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let health = ops::health::health(&state);
    assert_eq!(health.node_count, 2);
    assert_eq!(health.edge_count, 0);
}

// ── Graph Slice ──────────────────────────────────────────────────────

#[tokio::test]
async fn graph_slice_full() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(&state, &admin(), labels(&["a"]), PropertyMap::new(), None).unwrap();
    ops::nodes::create_node(&state, &admin(), labels(&["b"]), PropertyMap::new(), None).unwrap();
    ops::edges::create_edge(
        &state,
        &admin(),
        1,
        2,
        IStr::new("link"),
        PropertyMap::new(),
        false,
    )
    .unwrap();

    let slice = ops::graph_slice::graph_slice(
        &state,
        &admin(),
        &selene_wire::dto::graph_slice::SliceType::Full,
        None,
        None,
    );

    assert_eq!(slice.nodes.len(), 2);
    assert_eq!(slice.edges.len(), 1);
}

#[tokio::test]
async fn graph_slice_with_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    for _ in 0..5 {
        ops::nodes::create_node(
            &state,
            &admin(),
            labels(&["sensor"]),
            PropertyMap::new(),
            None,
        )
        .unwrap();
    }

    let slice = ops::graph_slice::graph_slice(
        &state,
        &admin(),
        &selene_wire::dto::graph_slice::SliceType::Full,
        Some(2),
        Some(1),
    );

    assert_eq!(slice.nodes.len(), 2);
    assert_eq!(slice.total_nodes, Some(5));
}

// ── Import Schema Pack ───────────────────────────────────────────────

#[tokio::test]
async fn import_pack_registers_schemas() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let pack = selene_packs::builtin("common").unwrap();
    let result = ops::schema::import_pack(&state, &admin(), pack).unwrap();

    assert!(result.node_schemas_registered > 0);
    assert!(result.edge_schemas_registered > 0);

    let schemas = ops::schema::list_node_schemas(&state, &admin()).unwrap();
    let labels: Vec<&str> = schemas.iter().map(|s| s.label.as_ref()).collect();
    assert!(labels.contains(&"site"));
    assert!(labels.contains(&"building"));
    assert!(labels.contains(&"equipment"));
}

// ── TS Replica Guard ─────────────────────────────────────────────────

#[tokio::test]
async fn ts_write_rejects_on_replica() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = ServerState::for_testing(dir.path()).await;
    state.set_replica(true);

    // Create a node first so entity validation doesn't fail
    let node = ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        props(&[("name", Value::str("test"))]),
        None,
    )
    .unwrap();

    let samples = vec![selene_wire::dto::ts::TsSampleDto {
        entity_id: node.id,
        property: "temp".into(),
        timestamp_nanos: 1000,
        value: 72.0,
    }];

    let result = ops::ts::ts_write(&state, &admin(), samples);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ops::OpError::ReadOnly));
}
