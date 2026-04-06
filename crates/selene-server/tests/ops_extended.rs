//! Extended unit tests for the ops layer -- covers error types, CSV parsing,
//! schema ops, graph stats, graph slicing, GQL bridge, and edge cases.
//!
//! Complements `tests/ops.rs` (basic CRUD + auth scope) with deeper coverage
//! of error paths, boundary values, and data transformations.

use std::collections::HashMap;
use std::sync::Arc;

use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_server::auth::Role;
use selene_server::auth::handshake::AuthContext;
use selene_server::bootstrap::ServerState;
use selene_server::ops;
use smol_str::SmolStr;

// ── Test helpers ────────────────────────────────────────────────────

fn admin() -> AuthContext {
    AuthContext::dev_admin()
}

fn reader() -> AuthContext {
    AuthContext {
        principal_node_id: NodeId(999),
        role: Role::Reader,
        scope: roaring::RoaringBitmap::new(),
        scope_generation: 0,
    }
}

fn scoped(scope_ids: &[u64]) -> AuthContext {
    let mut scope = roaring::RoaringBitmap::new();
    for &id in scope_ids {
        scope.insert(id as u32);
    }
    AuthContext {
        principal_node_id: NodeId(999),
        role: Role::Operator,
        scope,
        scope_generation: 0,
    }
}

fn labels(names: &[&str]) -> LabelSet {
    LabelSet::from_strs(names)
}

fn props(pairs: &[(&str, Value)]) -> PropertyMap {
    PropertyMap::from_pairs(pairs.iter().map(|(k, v)| (IStr::new(k), v.clone())))
}

// ── OpError Display formatting ──────────────────────────────────────

#[test]
fn op_error_not_found_display() {
    let err = ops::OpError::NotFound {
        entity: "node",
        id: 42,
    };
    assert_eq!(err.to_string(), "node 42 not found");
}

#[test]
fn op_error_not_found_edge_display() {
    let err = ops::OpError::NotFound {
        entity: "edge",
        id: 7,
    };
    assert_eq!(err.to_string(), "edge 7 not found");
}

#[test]
fn op_error_auth_denied_display() {
    let err = ops::OpError::AuthDenied;
    assert_eq!(err.to_string(), "access denied");
}

#[test]
fn op_error_schema_violation_display() {
    let err = ops::OpError::SchemaViolation("missing required property 'name'".into());
    assert_eq!(
        err.to_string(),
        "schema violation: missing required property 'name'"
    );
}

#[test]
fn op_error_invalid_request_display() {
    let err = ops::OpError::InvalidRequest("label cannot be empty".into());
    assert_eq!(err.to_string(), "invalid request: label cannot be empty");
}

#[test]
fn op_error_query_error_display() {
    let err = ops::OpError::QueryError("syntax error at line 1".into());
    assert_eq!(err.to_string(), "query error: syntax error at line 1");
}

#[test]
fn op_error_internal_display() {
    let err = ops::OpError::Internal("WAL write failed".into());
    assert_eq!(err.to_string(), "internal error: WAL write failed");
}

#[test]
fn op_error_read_only_display() {
    let err = ops::OpError::ReadOnly;
    assert_eq!(err.to_string(), "read-only replica");
}

#[test]
fn op_error_resources_exhausted_display() {
    let err = ops::OpError::ResourcesExhausted("binding limit exceeded".into());
    assert_eq!(
        err.to_string(),
        "resources exhausted: binding limit exceeded"
    );
}

#[test]
fn op_error_conflict_display() {
    let err = ops::OpError::Conflict("node already exists".into());
    assert_eq!(err.to_string(), "conflict: node already exists");
}

// ── GQL execution ───────────────────────────────────────────────────

#[tokio::test]
async fn gql_read_query_empty_graph() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "MATCH (n) RETURN count(*) AS total",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    assert_eq!(result.status_code, "00000");
    assert!(result.data_json.is_some());
    assert!(result.mutations.is_none());
}

#[tokio::test]
async fn gql_mutation_creates_node_and_returns_stats() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "INSERT (:sensor {name: 'Temp-1', unit: 'C'})",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    assert_eq!(result.status_code, "00000");
    let ms = result.mutations.as_ref().expect("mutation stats present");
    assert_eq!(ms.nodes_created, 1);
}

#[tokio::test]
async fn gql_syntax_error_returns_42601() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "NOT A VALID QUERY",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    assert_eq!(result.status_code, "42601");
    assert!(result.message.contains("syntax error"));
}

#[tokio::test]
async fn gql_empty_result_returns_02000() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "MATCH (n:nonexistent_label_xyz) RETURN n.id AS id",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    assert_eq!(result.status_code, "02000");
}

#[tokio::test]
async fn gql_explain_returns_plan() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "MATCH (s:sensor) RETURN s.name AS name",
        None,
        true,  // explain
        false, // profile
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    assert_eq!(result.status_code, "00000");
    assert!(result.plan.is_some());
    // Plan should contain scan info
    let plan = result.plan.unwrap();
    assert!(plan.contains("LabelScan") || plan.contains("Scan"));
}

#[tokio::test]
async fn gql_mutation_on_replica_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = ServerState::for_testing(dir.path()).await;
    state.set_replica(true);

    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "INSERT (:sensor {name: 'Test'})",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    // Replica mutations produce an error-status result, not an OpError
    assert_ne!(result.status_code, "00000");
    assert!(
        result.message.contains("replica") || result.message.contains("read-only"),
        "message was: {}",
        result.message
    );
}

#[tokio::test]
async fn gql_with_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    // Create a node
    ops::gql::execute_gql(
        &state,
        &admin(),
        "INSERT (:sensor {name: 'ParamSensor', temp: 72.5})",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();

    // Query with parameter
    let mut params = HashMap::new();
    params.insert("threshold".into(), Value::Float(70.0));
    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "MATCH (s:sensor) FILTER s.temp > $threshold RETURN s.name AS name",
        Some(&params),
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    assert_eq!(result.status_code, "00000");
    assert_eq!(result.row_count, 1);
}

#[tokio::test]
async fn gql_arrow_format_returns_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::gql::execute_gql(
        &state,
        &admin(),
        "INSERT (:sensor {name: 'ArrowTest'})",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();

    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "MATCH (s:sensor) RETURN s.name AS name",
        None,
        false,
        false,
        ops::gql::ResultFormat::ArrowIpc,
    )
    .unwrap();
    assert_eq!(result.status_code, "00000");
    assert!(result.data_json.is_none());
    assert!(result.data_arrow.is_some());
    // Arrow IPC starts with the magic bytes "ARROW1"
    let arrow = result.data_arrow.unwrap();
    assert!(!arrow.is_empty());
}

#[tokio::test]
async fn gql_ddl_create_graph() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "CREATE GRAPH test_graph",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    // DDL returns "0A000" (feature not yet implemented)
    assert_eq!(result.status_code, "0A000");
    assert!(result.message.contains("test_graph"));
}

#[tokio::test]
async fn gql_ddl_non_admin_denied() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let result = ops::gql::execute_gql(
        &state,
        &reader(),
        "CREATE GRAPH test_graph",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    assert_eq!(result.status_code, "42501");
    assert!(result.message.contains("Admin"));
}

// ── CSV import/export ───────────────────────────────────────────────

#[tokio::test]
async fn csv_import_type_inference() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let csv = "name,value,active,ratio\nS1,42,true,3.15\nS2,-7,false,0.5\n";
    let config = ops::csv_io::CsvNodeImportConfig {
        label: "device".into(),
        ..Default::default()
    };
    let result = ops::csv_io::import_nodes_csv(&state, &admin(), csv.as_bytes(), &config).unwrap();
    assert_eq!(result.nodes_created, 2);

    state.graph().read(|g: &selene_graph::SeleneGraph| {
        let n1 = g.get_node(NodeId(1)).unwrap();
        assert!(matches!(
            n1.properties.get(IStr::new("value")),
            Some(Value::Int(42))
        ));
        assert!(matches!(
            n1.properties.get(IStr::new("active")),
            Some(Value::Bool(true))
        ));
        // 3.15 parses as float
        if let Some(Value::Float(f)) = n1.properties.get(IStr::new("ratio")) {
            assert!((f - 3.15).abs() < 0.001);
        } else {
            panic!("expected Float for ratio");
        }

        let n2 = g.get_node(NodeId(2)).unwrap();
        assert!(matches!(
            n2.properties.get(IStr::new("value")),
            Some(Value::Int(-7))
        ));
        assert!(matches!(
            n2.properties.get(IStr::new("active")),
            Some(Value::Bool(false))
        ));
    });
}

#[tokio::test]
async fn csv_import_empty_values_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let csv = "name,unit,accuracy\nTemp-1,,0.5\nTemp-2,F,\n";
    let config = ops::csv_io::CsvNodeImportConfig {
        label: "sensor".into(),
        ..Default::default()
    };
    let result = ops::csv_io::import_nodes_csv(&state, &admin(), csv.as_bytes(), &config).unwrap();
    assert_eq!(result.nodes_created, 2);

    state.graph().read(|g: &selene_graph::SeleneGraph| {
        let n1 = g.get_node(NodeId(1)).unwrap();
        // "unit" was empty so should not be set
        assert!(n1.properties.get(IStr::new("unit")).is_none());
        // "accuracy" was present
        assert!(n1.properties.get(IStr::new("accuracy")).is_some());

        let n2 = g.get_node(NodeId(2)).unwrap();
        // "unit" present
        assert!(n2.properties.get(IStr::new("unit")).is_some());
        // "accuracy" was empty
        assert!(n2.properties.get(IStr::new("accuracy")).is_none());
    });
}

#[tokio::test]
async fn csv_import_missing_label_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let config = ops::csv_io::CsvNodeImportConfig::default(); // empty label
    let result = ops::csv_io::import_nodes_csv(&state, &admin(), &b"name\nFoo\n"[..], &config);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ops::OpError::InvalidRequest(_)
    ));
}

#[tokio::test]
async fn csv_import_with_column_mappings() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let csv = "device_name,temp_reading\nS1,72.5\n";
    let mut mappings = HashMap::new();
    mappings.insert("device_name".into(), "name".into());
    mappings.insert("temp_reading".into(), "temperature".into());
    let config = ops::csv_io::CsvNodeImportConfig {
        label: "sensor".into(),
        column_mappings: Some(mappings),
        ..Default::default()
    };
    let result = ops::csv_io::import_nodes_csv(&state, &admin(), csv.as_bytes(), &config).unwrap();
    assert_eq!(result.nodes_created, 1);

    state.graph().read(|g: &selene_graph::SeleneGraph| {
        let n = g.get_node(NodeId(1)).unwrap();
        // Mapped "device_name" -> "name"
        assert!(n.properties.get(IStr::new("name")).is_some());
        // Mapped "temp_reading" -> "temperature"
        assert!(n.properties.get(IStr::new("temperature")).is_some());
    });
}

#[tokio::test]
async fn csv_import_with_parent_containment() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    // Create parent node
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["building"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let csv = "name,parent_id\nTemp-1,1\nTemp-2,1\n";
    let config = ops::csv_io::CsvNodeImportConfig {
        label: "sensor".into(),
        parent_id_column: Some("parent_id".into()),
        ..Default::default()
    };
    let result = ops::csv_io::import_nodes_csv(&state, &admin(), csv.as_bytes(), &config).unwrap();
    assert_eq!(result.nodes_created, 2);
    assert_eq!(result.edges_created, 2);

    // Verify containment edges
    let count = state
        .graph()
        .read(|g: &selene_graph::SeleneGraph| g.edges_by_label("contains").count());
    assert_eq!(count, 2);
}

#[tokio::test]
async fn csv_import_with_tab_delimiter() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    let tsv = "name\tvalue\nS1\t42\n";
    let config = ops::csv_io::CsvNodeImportConfig {
        label: "sensor".into(),
        delimiter: b'\t',
        ..Default::default()
    };
    let result = ops::csv_io::import_nodes_csv(&state, &admin(), tsv.as_bytes(), &config).unwrap();
    assert_eq!(result.nodes_created, 1);
}

#[tokio::test]
async fn csv_import_edges_valid() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    // Create nodes first
    for _ in 0..3 {
        ops::nodes::create_node(
            &state,
            &admin(),
            labels(&["node"]),
            PropertyMap::new(),
            None,
        )
        .unwrap();
    }

    let csv = "source_id,target_id,label,weight\n1,2,feeds,10\n2,3,feeds,5\n";
    let config = ops::csv_io::CsvEdgeImportConfig::default();
    let result = ops::csv_io::import_edges_csv(&state, &admin(), csv.as_bytes(), &config).unwrap();
    assert_eq!(result.edges_created, 2);
}

#[tokio::test]
async fn csv_import_edges_missing_column_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;
    // CSV missing the "label" column
    let csv = "source_id,target_id\n1,2\n";
    let config = ops::csv_io::CsvEdgeImportConfig::default();
    let result = ops::csv_io::import_edges_csv(&state, &admin(), csv.as_bytes(), &config);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, ops::OpError::InvalidRequest(_)));
}

#[tokio::test]
async fn csv_import_edges_invalid_ids_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["node"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["node"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    // First row has invalid source_id "abc"
    let csv = "source_id,target_id,label\nabc,2,feeds\n1,2,connects\n";
    let config = ops::csv_io::CsvEdgeImportConfig::default();
    let result = ops::csv_io::import_edges_csv(&state, &admin(), csv.as_bytes(), &config).unwrap();
    assert_eq!(result.edges_created, 1);
    assert_eq!(result.rows_skipped, 1);
    assert!(!result.errors.is_empty());
}

#[tokio::test]
async fn csv_export_nodes_with_label_filter() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        props(&[("name", Value::str("S1"))]),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["building"]),
        props(&[("name", Value::str("B1"))]),
        None,
    )
    .unwrap();

    // Export only sensors
    let csv = ops::csv_io::export_nodes_csv(&state, &admin(), Some("sensor")).unwrap();
    let lines: Vec<&str> = csv.trim().lines().collect();
    assert_eq!(lines.len(), 2); // header + 1 sensor
    assert!(csv.contains("S1"));
    assert!(!csv.contains("B1"));
}

#[tokio::test]
async fn csv_export_nodes_all() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        props(&[("name", Value::str("S1"))]),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["building"]),
        props(&[("name", Value::str("B1"))]),
        None,
    )
    .unwrap();

    let csv = ops::csv_io::export_nodes_csv(&state, &admin(), None).unwrap();
    let lines: Vec<&str> = csv.trim().lines().collect();
    assert_eq!(lines.len(), 3); // header + 2 nodes
    assert!(csv.contains("S1"));
    assert!(csv.contains("B1"));
}

#[tokio::test]
async fn csv_export_empty_graph() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let csv = ops::csv_io::export_nodes_csv(&state, &admin(), None).unwrap();
    let lines: Vec<&str> = csv.trim().lines().collect();
    // Only the header "id" row
    assert_eq!(lines.len(), 1);
}

#[tokio::test]
async fn csv_export_edges_with_properties() {
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
        props(&[("weight", Value::Float(2.5))]),
    )
    .unwrap();

    let csv = ops::csv_io::export_edges_csv(&state, &admin(), None).unwrap();
    assert!(csv.contains("id,source,target,label"));
    assert!(csv.contains("weight"));
    assert!(csv.contains("2.5"));
    let lines: Vec<&str> = csv.trim().lines().collect();
    assert_eq!(lines.len(), 2); // header + 1 edge
}

#[tokio::test]
async fn csv_export_scope_filters_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        props(&[("name", Value::str("Visible"))]),
        None,
    )
    .unwrap();
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        props(&[("name", Value::str("Hidden"))]),
        None,
    )
    .unwrap();

    // Scope only includes node 1
    let auth = scoped(&[1]);
    let csv = ops::csv_io::export_nodes_csv(&state, &auth, None).unwrap();
    assert!(csv.contains("Visible"));
    assert!(!csv.contains("Hidden"));
}

// ── Schema ops ──────────────────────────────────────────────────────

#[tokio::test]
async fn schema_register_and_list_node() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let schema = selene_core::schema::NodeSchema {
        label: Arc::from("test_device"),
        parent: None,
        properties: vec![selene_core::schema::PropertyDef::simple(
            "name",
            selene_core::schema::ValueType::String,
            false,
        )],
        valid_edge_labels: vec![],
        description: "A test device".into(),
        annotations: HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    ops::schema::register_node_schema(&state, &admin(), schema).unwrap();
    let schemas = ops::schema::list_node_schemas(&state, &admin()).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(&*schemas[0].label, "test_device");
}

#[tokio::test]
async fn schema_get_node_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let result = ops::schema::get_node_schema(&state, &admin(), "nonexistent");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ops::OpError::NotFound { .. }));
}

#[tokio::test]
async fn schema_duplicate_registration_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let schema = selene_core::schema::NodeSchema {
        label: Arc::from("sensor"),
        parent: None,
        properties: vec![],
        valid_edge_labels: vec![],
        description: String::new(),
        annotations: HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    ops::schema::register_node_schema(&state, &admin(), schema.clone()).unwrap();
    let result = ops::schema::register_node_schema(&state, &admin(), schema);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, ops::OpError::InvalidRequest(_)));
}

#[tokio::test]
async fn schema_force_registration_replaces() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let schema_v1 = selene_core::schema::NodeSchema {
        label: Arc::from("sensor"),
        parent: None,
        properties: vec![],
        valid_edge_labels: vec![],
        description: "v1".into(),
        annotations: HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    ops::schema::register_node_schema(&state, &admin(), schema_v1).unwrap();

    let schema_v2 = selene_core::schema::NodeSchema {
        label: Arc::from("sensor"),
        parent: None,
        properties: vec![selene_core::schema::PropertyDef::simple(
            "name",
            selene_core::schema::ValueType::String,
            false,
        )],
        valid_edge_labels: vec![],
        description: "v2".into(),
        annotations: HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    let replaced = ops::schema::register_node_schema_force(&state, &admin(), schema_v2).unwrap();
    assert!(replaced);

    let schema = ops::schema::get_node_schema(&state, &admin(), "sensor").unwrap();
    assert_eq!(schema.properties.len(), 1);
}

#[tokio::test]
async fn schema_register_and_list_edge() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let schema = selene_core::schema::EdgeSchema::builder("feeds")
        .property(selene_core::schema::PropertyDef::simple(
            "medium",
            selene_core::schema::ValueType::String,
            false,
        ))
        .description("Flow relationship")
        .build();

    ops::schema::register_edge_schema(&state, &admin(), schema).unwrap();
    let schemas = ops::schema::list_edge_schemas(&state, &admin()).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(&*schemas[0].label, "feeds");
}

#[tokio::test]
async fn schema_unregister_node() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let schema = selene_core::schema::NodeSchema {
        label: Arc::from("ephemeral"),
        parent: None,
        properties: vec![],
        valid_edge_labels: vec![],
        description: String::new(),
        annotations: HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    ops::schema::register_node_schema(&state, &admin(), schema).unwrap();
    ops::schema::unregister_node_schema(&state, &admin(), "ephemeral").unwrap();
    let schemas = ops::schema::list_node_schemas(&state, &admin()).unwrap();
    assert!(schemas.is_empty());
}

#[tokio::test]
async fn schema_unregister_nonexistent_returns_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let result = ops::schema::unregister_node_schema(&state, &admin(), "no_such");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ops::OpError::NotFound { .. }));
}

#[tokio::test]
async fn schema_unregister_edge() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let schema = selene_core::schema::EdgeSchema::builder("temp_edge").build();

    ops::schema::register_edge_schema(&state, &admin(), schema).unwrap();
    ops::schema::unregister_edge_schema(&state, &admin(), "temp_edge").unwrap();
    let schemas = ops::schema::list_edge_schemas(&state, &admin()).unwrap();
    assert!(schemas.is_empty());
}

#[tokio::test]
async fn schema_edge_get_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let result = ops::schema::get_edge_schema(&state, &admin(), "nonexistent");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ops::OpError::NotFound { .. }));
}

// ── Graph stats ─────────────────────────────────────────────────────

#[tokio::test]
async fn graph_stats_empty() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let stats = ops::graph_stats::graph_stats(&state, &admin());
    assert_eq!(stats.node_count, 0);
    assert_eq!(stats.edge_count, 0);
    assert!(stats.node_labels.is_empty());
    assert!(stats.edge_labels.is_empty());
}

#[tokio::test]
async fn graph_stats_with_nodes_and_edges() {
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
        labels(&["building"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::edges::create_edge(
        &state,
        &admin(),
        1,
        2,
        IStr::new("feeds"),
        PropertyMap::new(),
    )
    .unwrap();

    let stats = ops::graph_stats::graph_stats(&state, &admin());
    assert_eq!(stats.node_count, 3);
    assert_eq!(stats.edge_count, 1);
    assert_eq!(stats.node_labels.get("sensor"), Some(&2));
    assert_eq!(stats.node_labels.get("building"), Some(&1));
    assert_eq!(stats.edge_labels.get("feeds"), Some(&1));
}

#[tokio::test]
async fn graph_stats_scope_filtered() {
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
        labels(&["building"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    ops::edges::create_edge(
        &state,
        &admin(),
        1,
        2,
        IStr::new("feeds"),
        PropertyMap::new(),
    )
    .unwrap();

    // Only node 1 in scope
    let auth = scoped(&[1]);
    let stats = ops::graph_stats::graph_stats(&state, &auth);
    assert_eq!(stats.node_count, 1);
    // Edge 1->2 requires both source and target in scope; target (2) not in scope
    assert_eq!(stats.edge_count, 0);
}

// ── Graph slice ─────────────────────────────────────────────────────

#[tokio::test]
async fn graph_slice_empty() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let slice = ops::graph_slice::graph_slice(
        &state,
        &admin(),
        &selene_wire::dto::graph_slice::SliceType::Full,
        None,
        None,
    );
    assert!(slice.nodes.is_empty());
    assert!(slice.edges.is_empty());
    assert!(slice.total_nodes.is_none());
}

#[tokio::test]
async fn graph_slice_by_labels() {
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

    let slice = ops::graph_slice::graph_slice(
        &state,
        &admin(),
        &selene_wire::dto::graph_slice::SliceType::ByLabels {
            labels: vec!["sensor".into()],
        },
        None,
        None,
    );
    assert_eq!(slice.nodes.len(), 2);
}

#[tokio::test]
async fn graph_slice_containment() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    // Building -> Sensor hierarchy
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
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        Some(1),
    )
    .unwrap();
    // Unrelated node
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["other"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let slice = ops::graph_slice::graph_slice(
        &state,
        &admin(),
        &selene_wire::dto::graph_slice::SliceType::Containment {
            root_id: 1,
            max_depth: Some(1),
        },
        None,
        None,
    );
    // Building + 2 sensors
    assert_eq!(slice.nodes.len(), 3);
    // 2 containment edges
    assert_eq!(slice.edges.len(), 2);
}

#[tokio::test]
async fn graph_slice_pagination_offset_limit() {
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

    // Offset 1, limit 2
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

#[tokio::test]
async fn graph_slice_scope_filters_nodes() {
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

    let auth = scoped(&[1]); // only node 1
    let slice = ops::graph_slice::graph_slice(
        &state,
        &auth,
        &selene_wire::dto::graph_slice::SliceType::Full,
        None,
        None,
    );
    assert_eq!(slice.nodes.len(), 1);
}

// ── Edge CRUD extensions ────────────────────────────────────────────

#[tokio::test]
async fn node_edges_returns_both_directions() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(&state, &admin(), labels(&["a"]), PropertyMap::new(), None).unwrap();
    ops::nodes::create_node(&state, &admin(), labels(&["b"]), PropertyMap::new(), None).unwrap();
    ops::nodes::create_node(&state, &admin(), labels(&["c"]), PropertyMap::new(), None).unwrap();

    // Edge 1->2 and 3->1
    ops::edges::create_edge(&state, &admin(), 1, 2, IStr::new("out"), PropertyMap::new()).unwrap();
    ops::edges::create_edge(&state, &admin(), 3, 1, IStr::new("in"), PropertyMap::new()).unwrap();

    let result = ops::edges::node_edges(&state, &admin(), 1, 0, 100).unwrap();
    assert_eq!(result.total, 2);
    assert_eq!(result.edges.len(), 2);
}

#[tokio::test]
async fn node_edges_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let result = ops::edges::node_edges(&state, &admin(), 999, 0, 100);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(matches!(err, ops::OpError::NotFound { .. }));
}

#[tokio::test]
async fn node_edges_with_pagination() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(&state, &admin(), labels(&["hub"]), PropertyMap::new(), None).unwrap();
    for i in 2..=5 {
        ops::nodes::create_node(
            &state,
            &admin(),
            labels(&["spoke"]),
            PropertyMap::new(),
            None,
        )
        .unwrap();
        ops::edges::create_edge(
            &state,
            &admin(),
            1,
            i,
            IStr::new("link"),
            PropertyMap::new(),
        )
        .unwrap();
    }

    let result = ops::edges::node_edges(&state, &admin(), 1, 1, 2).unwrap();
    assert_eq!(result.total, 4);
    assert_eq!(result.edges.len(), 2);
}

#[tokio::test]
async fn list_edges_with_label_filter() {
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
    )
    .unwrap();
    ops::edges::create_edge(
        &state,
        &admin(),
        1,
        2,
        IStr::new("monitors"),
        PropertyMap::new(),
    )
    .unwrap();

    let all = ops::edges::list_edges(&state, &admin(), None, 100, 0).unwrap();
    assert_eq!(all.total, 2);

    let feeds_only = ops::edges::list_edges(&state, &admin(), Some("feeds"), 100, 0).unwrap();
    assert_eq!(feeds_only.total, 1);
    assert_eq!(feeds_only.edges[0].label, "feeds");
}

// ── Node CRUD extensions ────────────────────────────────────────────

#[tokio::test]
async fn create_node_non_admin_requires_parent() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    // Create a parent first
    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["building"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    // Non-admin without parent_id should fail
    let auth = scoped(&[1]);
    let result = ops::nodes::create_node(
        &state,
        &auth,
        labels(&["sensor"]),
        PropertyMap::new(),
        None, // no parent
    );
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ops::OpError::InvalidRequest(_)
    ));
}

#[tokio::test]
async fn modify_node_add_and_remove_labels() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor", "active"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let modified = ops::nodes::modify_node(
        &state,
        &admin(),
        1,
        vec![],
        vec![],
        vec![IStr::new("priority")], // add
        vec!["active".to_string()],  // remove
    )
    .unwrap();

    assert!(modified.labels.contains(&"priority".to_string()));
    assert!(!modified.labels.contains(&"active".to_string()));
    assert!(modified.labels.contains(&"sensor".to_string()));
}

#[tokio::test]
async fn modify_node_set_and_remove_properties() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        props(&[("name", Value::str("S1")), ("unit", Value::str("F"))]),
        None,
    )
    .unwrap();

    let modified = ops::nodes::modify_node(
        &state,
        &admin(),
        1,
        vec![(IStr::new("name"), Value::str("S1-updated"))],
        vec!["unit".to_string()],
        vec![],
        vec![],
    )
    .unwrap();

    assert_eq!(
        modified.properties.get("name"),
        Some(&Value::String(SmolStr::new("S1-updated")))
    );
    assert!(!modified.properties.contains_key("unit"));
}

#[tokio::test]
async fn delete_nonexistent_node_returns_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let result = ops::nodes::delete_node(&state, &admin(), 999);
    assert!(result.is_err());
}

#[tokio::test]
async fn list_nodes_empty_graph() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let result = ops::nodes::list_nodes(&state, &admin(), None, 100, 0).unwrap();
    assert_eq!(result.total, 0);
    assert!(result.nodes.is_empty());
}

#[tokio::test]
async fn list_nodes_pagination() {
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

    let page1 = ops::nodes::list_nodes(&state, &admin(), None, 2, 0).unwrap();
    assert_eq!(page1.nodes.len(), 2);
    assert_eq!(page1.total, 5);

    let page2 = ops::nodes::list_nodes(&state, &admin(), None, 2, 2).unwrap();
    assert_eq!(page2.nodes.len(), 2);

    let page3 = ops::nodes::list_nodes(&state, &admin(), None, 2, 4).unwrap();
    assert_eq!(page3.nodes.len(), 1);
}

// ── Edge scope enforcement ──────────────────────────────────────────

#[tokio::test]
async fn get_edge_denied_when_source_out_of_scope() {
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
    )
    .unwrap();

    // Scope includes target (2) but not source (1)
    let auth = scoped(&[2]);
    let result = ops::edges::get_edge(&state, &auth, 1);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ops::OpError::AuthDenied));
}

#[tokio::test]
async fn create_edge_denied_when_target_out_of_scope() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(&state, &admin(), labels(&["a"]), PropertyMap::new(), None).unwrap();
    ops::nodes::create_node(&state, &admin(), labels(&["b"]), PropertyMap::new(), None).unwrap();

    // Scope includes source (1) but not target (2)
    let auth = scoped(&[1]);
    let result =
        ops::edges::create_edge(&state, &auth, 1, 2, IStr::new("link"), PropertyMap::new());
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ops::OpError::AuthDenied));
}

// ── Replica guard for writes ────────────────────────────────────────

#[tokio::test]
async fn create_node_allowed_on_replica() {
    // Node creation goes through graph.write(), not ops-layer replica guard.
    // The persist_or_die skips WAL on replicas but the node itself is created.
    let dir = tempfile::tempdir().unwrap();
    let mut state = ServerState::for_testing(dir.path()).await;
    state.set_replica(true);

    // Node creation still works (WAL write is skipped for replicas)
    let node = ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();
    assert_eq!(node.id, 1);
}

// ── Import schema pack ──────────────────────────────────────────────

#[tokio::test]
async fn import_pack_skips_existing_schemas() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let pack = selene_packs::builtin("common").unwrap();
    let r1 = ops::schema::import_pack(&state, &admin(), pack).unwrap();
    assert!(r1.node_schemas_registered > 0);

    // Import same pack again
    let pack2 = selene_packs::builtin("common").unwrap();
    let r2 = ops::schema::import_pack(&state, &admin(), pack2).unwrap();
    assert_eq!(r2.node_schemas_registered, 0);
    assert!(r2.node_schemas_skipped > 0);
}

// ── GQL node type DDL ───────────────────────────────────────────────

#[tokio::test]
async fn gql_create_node_type_via_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "CREATE NODE TYPE :sensor (name :: STRING NOT NULL, unit :: STRING)",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    // DDL mutations return "00000" (success) or "02000" (no data rows)
    assert!(
        result.status_code == "00000" || result.status_code == "02000",
        "unexpected status: {}",
        result.status_code
    );

    // Verify schema was registered
    let schema = ops::schema::get_node_schema(&state, &admin(), "sensor").unwrap();
    assert!(!schema.properties.is_empty());
}

#[tokio::test]
async fn gql_create_edge_type_via_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    let result = ops::gql::execute_gql(
        &state,
        &admin(),
        "CREATE EDGE TYPE :feeds (medium :: STRING)",
        None,
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .unwrap();
    assert!(
        result.status_code == "00000" || result.status_code == "02000",
        "unexpected status: {}",
        result.status_code
    );

    let schema = ops::schema::get_edge_schema(&state, &admin(), "feeds").unwrap();
    assert_eq!(schema.properties.len(), 1);
}

// ── Multi-label node stats ──────────────────────────────────────────

#[tokio::test]
async fn graph_stats_multi_label_node_counted_per_label() {
    let dir = tempfile::tempdir().unwrap();
    let state = ServerState::for_testing(dir.path()).await;

    ops::nodes::create_node(
        &state,
        &admin(),
        labels(&["sensor", "active"]),
        PropertyMap::new(),
        None,
    )
    .unwrap();

    let stats = ops::graph_stats::graph_stats(&state, &admin());
    assert_eq!(stats.node_count, 1);
    // Node appears under both labels
    assert_eq!(stats.node_labels.get("sensor"), Some(&1));
    assert_eq!(stats.node_labels.get("active"), Some(&1));
}
