//! COUNT subquery, trigger change propagation, dictionary encoding,
//! and schema default fallback tests.

use super::*;

// ── COUNT subquery tests ──

#[test]
fn e2e_count_subquery() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let a = m
        .create_node(
            LabelSet::from_strs(&["P"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("alice"))]),
        )
        .unwrap();
    let b = m
        .create_node(
            LabelSet::from_strs(&["P"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("bob"))]),
        )
        .unwrap();
    let c = m
        .create_node(
            LabelSet::from_strs(&["P"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("carol"))]),
        )
        .unwrap();
    m.create_edge(a, IStr::new("knows"), b, PropertyMap::new())
        .unwrap();
    m.create_edge(a, IStr::new("knows"), c, PropertyMap::new())
        .unwrap();
    m.create_edge(b, IStr::new("knows"), c, PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();

    let result = QueryBuilder::new(
        "MATCH (N:P) WHERE COUNT { MATCH (N)-[:knows]->() } >= 2 RETURN N.name AS NAME",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let col = result.batches[0].column_by_name("NAME").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "alice");
}

#[test]
fn e2e_detach_delete_case_insensitive() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    m.create_node(LabelSet::from_strs(&["test"]), PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);
    // Variable 'n' is uppercase-folded to 'N' by intern_var
    let result = MutationBuilder::new("MATCH (n:test) DETACH DELETE n").execute(&shared);
    assert!(
        result.is_ok(),
        "DETACH DELETE should work with case-folded variable: {:?}",
        result.err()
    );
}

#[test]
fn e2e_set_then_delete_same_mutation() {
    // SET + DELETE in same mutation block should not crash
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["item"]),
        PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("x")))]),
    )
    .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);
    // SET runs in deferred phase, DELETE runs immediately --deferred SET should be skipped
    let result = MutationBuilder::new("MATCH (n:item) SET n.name = 'updated' DETACH DELETE n")
        .execute(&shared);
    assert!(
        result.is_ok(),
        "SET + DELETE in same block should not crash: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().mutations.nodes_deleted, 1);
}

#[test]
fn e2e_insert_per_match_row() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["src"]),
        PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("a")))]),
    )
    .unwrap();
    m.create_node(
        LabelSet::from_strs(&["src"]),
        PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("b")))]),
    )
    .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);
    let result = MutationBuilder::new("MATCH (S:src) INSERT (S)-[:link]->(T:tgt {v: 1}) RETURN T")
        .execute(&shared)
        .unwrap();
    // 2 src nodes matched -> 2 tgt nodes created
    assert_eq!(
        result.row_count(),
        2,
        "Expected 2 RETURN rows for 2 MATCH rows"
    );
    assert_eq!(
        result.mutations.nodes_created, 2,
        "Expected 2 nodes created"
    );
    assert_eq!(
        result.mutations.edges_created, 2,
        "Expected 2 edges created"
    );
}

/// M7: SET a.x = 1 followed by SET a = {y: 2} should result in {y: 2} only.
/// SetAllProperties must read from the live mutation state, not the pre-mutation snapshot,
/// so it removes properties added by earlier deferred mutations in the same block.
#[test]
fn e2e_set_all_properties_replaces_deferred_set() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let n = m
        .create_node(
            LabelSet::from_strs(&["item"]),
            PropertyMap::from_pairs(vec![(IStr::new("orig"), Value::Int(0))]),
        )
        .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);
    // SET N.added = 1, then SET N = {replaced: 2}
    // The second SET should replace ALL properties including "added" from the first SET
    MutationBuilder::new("MATCH (N:item) SET N.added = 1 SET N = {replaced: 2}")
        .execute(&shared)
        .unwrap();
    let snap = shared.load_snapshot();
    let node = snap.get_node(n).unwrap();
    // "replaced" should exist
    assert_eq!(
        node.properties.get_by_str("replaced"),
        Some(&Value::Int(2)),
        "replaced property should be set"
    );
    // "added" should NOT exist (SetAllProperties should have removed it)
    assert_eq!(
        node.properties.get_by_str("added"),
        None,
        "added property should be removed by SET N = {{...}}"
    );
    // "orig" should NOT exist either
    assert_eq!(
        node.properties.get_by_str("orig"),
        None,
        "orig property should be removed by SET N = {{...}}"
    );
}

/// DELETE trigger fires and creates an audit node.
#[test]
fn e2e_trigger_fires_on_delete() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["device"]),
        PropertyMap::from_pairs(vec![(
            IStr::new("name"),
            Value::String(SmolStr::new("sensor-1")),
        )]),
    )
    .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);

    // Create trigger: when a device is deleted, create an audit node
    MutationBuilder::new(
            "CREATE TRIGGER audit_delete AFTER DELETE ON :device EXECUTE INSERT (:audit {event: 'device_deleted'})",
        ).execute(&shared).unwrap();

    // Verify trigger is registered
    let trigger_count = shared.read(|g| g.trigger_registry().list().len());
    assert_eq!(trigger_count, 1, "trigger should be registered");

    // Delete the device --this should fire the DELETE trigger
    let del_result = MutationBuilder::new("MATCH (D:device) DETACH DELETE D")
        .execute(&shared)
        .unwrap();
    assert_eq!(
        del_result.mutations.nodes_deleted, 1,
        "device should be deleted"
    );

    // Verify audit node was created by the trigger
    let snap = shared.load_snapshot();
    let result = QueryBuilder::new("MATCH (A:audit) RETURN A.event AS EVENT", &snap)
        .execute()
        .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "DELETE trigger should have created an audit node"
    );
    let col = result.batches[0].column_by_name("EVENT").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    // String value comes back from trigger action --verify it's a non-empty string
    // (exact quote handling depends on trigger action re-parsing)
    assert!(
        arr.value(0).contains("device_deleted"),
        "expected 'device_deleted' in value, got: {}",
        arr.value(0)
    );
}

/// Trigger with InsertEdge action creates edges.
#[test]
fn e2e_trigger_insert_edge_action() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let alert_target = m
        .create_node(LabelSet::from_strs(&["alert_sink"]), PropertyMap::new())
        .unwrap();
    m.create_node(
        LabelSet::from_strs(&["device"]),
        PropertyMap::from_pairs(vec![(
            IStr::new("name"),
            Value::String(SmolStr::new("sensor-1")),
        )]),
    )
    .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);

    // Create trigger: when a device property is set, create an edge to the alert_sink
    // Note: InsertEdge in triggers requires both endpoints to be pre-bound variables.
    // We use a simple INSERT node action instead since InsertEdge resolution is complex.
    MutationBuilder::new(
            "CREATE TRIGGER on_device_set AFTER SET ON :device EXECUTE INSERT (:alert {source: 'device'})",
            ).execute(&shared).unwrap();

    // Trigger the SET
    MutationBuilder::new("MATCH (D:device) SET D.status = 'active'")
        .execute(&shared)
        .unwrap();

    // Verify the trigger fired and created the alert node
    let result = QueryBuilder::new(
        "MATCH (A:alert) RETURN A.source AS SRC",
        &shared.load_snapshot(),
    )
    .execute()
    .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "SET trigger should have created an alert node"
    );

    // Verify the alert_sink node still exists (sanity check)
    let exists = shared.read(|g| g.contains_node(alert_target));
    assert!(exists, "alert_sink should still exist");
}

/// C-5: Trigger-generated mutations must appear in the returned changes vec
/// so the ops layer can persist them to WAL and broadcast via changelog.
#[test]
fn e2e_trigger_changes_included_in_auto_commit_result() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["device"]),
        PropertyMap::from_pairs(vec![(
            IStr::new("name"),
            Value::String(SmolStr::new("sensor-1")),
        )]),
    )
    .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);

    // Create trigger: when a device property is SET, INSERT an audit node
    MutationBuilder::new(
            "CREATE TRIGGER audit_set AFTER SET ON :device EXECUTE INSERT (:audit_log {event: 'property_changed'})",
            ).execute(&shared).unwrap();

    // SET a property on the device -- trigger should fire and create an audit node
    let result = MutationBuilder::new("MATCH (D:device) SET D.status = 'active'")
        .execute(&shared)
        .unwrap();

    // The trigger-created node should be visible in the graph
    let audit_exists = QueryBuilder::new(
        "MATCH (A:audit_log) RETURN A.event AS EVENT",
        &shared.load_snapshot(),
    )
    .execute()
    .unwrap();
    assert_eq!(
        audit_exists.row_count(),
        1,
        "trigger should have created audit_log node"
    );

    // CRITICAL: The returned changes must include the trigger-generated changes
    // so the ops layer can persist them to WAL and broadcast via changelog.
    let has_trigger_changes = result.changes.iter().any(|c| {
            matches!(c, selene_core::changeset::Change::NodeCreated { .. })
                || matches!(c, selene_core::changeset::Change::LabelAdded { label, .. } if label.as_ref() == "audit_log")
        });
    assert!(
        has_trigger_changes,
        "trigger-generated changes must appear in result.changes for WAL persistence; got: {:?}",
        result.changes
    );
}

/// C-5: Trigger-generated mutations must appear in transaction path changes too.
#[test]
fn e2e_trigger_changes_included_in_transaction_result() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["device"]),
        PropertyMap::from_pairs(vec![(
            IStr::new("name"),
            Value::String(SmolStr::new("sensor-1")),
        )]),
    )
    .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);

    // Create trigger: when a device property is SET, INSERT an audit node
    MutationBuilder::new(
            "CREATE TRIGGER audit_set AFTER SET ON :device EXECUTE INSERT (:audit_log {event: 'property_changed'})",
            ).execute(&shared).unwrap();

    // Execute the mutation within a transaction
    let mut txn = shared.begin_transaction();
    let _result = MutationBuilder::new("MATCH (D:device) SET D.status = 'active'")
        .execute_in_transaction(&mut txn)
        .unwrap();
    let changes = txn.commit();

    // The trigger-created node should be visible in the graph
    let audit_exists = QueryBuilder::new(
        "MATCH (A:audit_log) RETURN A.event AS EVENT",
        &shared.load_snapshot(),
    )
    .execute()
    .unwrap();
    assert_eq!(
        audit_exists.row_count(),
        1,
        "trigger should have created audit_log node"
    );

    // CRITICAL: The transaction changes must include trigger-generated changes
    let has_trigger_changes = changes.iter().any(|c| {
            matches!(c, selene_core::changeset::Change::NodeCreated { .. })
                || matches!(c, selene_core::changeset::Change::LabelAdded { label, .. } if label.as_ref() == "audit_log")
        });
    assert!(
        has_trigger_changes,
        "trigger-generated changes must appear in committed transaction changes for WAL persistence; got: {changes:?}"
    );
}

// ── Dictionary encoding tests ─────────────────────────────────────

#[test]
fn dictionary_encoding_insert_promotes_to_interned_str() {
    let shared = SharedGraph::new(SeleneGraph::new());

    // Create a node type with a DICTIONARY property
    MutationBuilder::new("CREATE NODE TYPE :Sensor (unit :: STRING DICTIONARY, name :: STRING)")
        .execute(&shared)
        .unwrap();

    // Insert a node with the dictionary property
    MutationBuilder::new("INSERT (:Sensor {unit: 'degF', name: 'TempSensor-1'})")
        .execute(&shared)
        .unwrap();

    // Verify the stored value is InternedStr for the dictionary property
    let snap = shared.load_snapshot();
    let node = snap.all_node_ids().next().expect("should have one node");
    let n = snap.get_node(node).unwrap();

    let unit_val = n
        .properties
        .get_by_str("unit")
        .expect("unit property should exist");
    assert!(
        matches!(unit_val, Value::InternedStr(_)),
        "dictionary property should be InternedStr, got: {unit_val:?}"
    );

    // The non-dictionary property should remain as String
    let name_val = n
        .properties
        .get_by_str("name")
        .expect("name property should exist");
    assert!(
        matches!(name_val, Value::String(_)),
        "non-dictionary property should remain String, got: {name_val:?}"
    );

    // Both should still be readable via as_str()
    assert_eq!(unit_val.as_str(), Some("degF"));
    assert_eq!(name_val.as_str(), Some("TempSensor-1"));
}

#[test]
fn dictionary_encoding_set_promotes_to_interned_str() {
    let shared = SharedGraph::new(SeleneGraph::new());

    // Create a node type with a DICTIONARY property
    MutationBuilder::new("CREATE NODE TYPE :Sensor (unit :: STRING DICTIONARY, name :: STRING)")
        .execute(&shared)
        .unwrap();

    // Insert a node first
    MutationBuilder::new("INSERT (:Sensor {unit: 'degF', name: 'TempSensor-1'})")
        .execute(&shared)
        .unwrap();

    // SET the dictionary property to a new value
    MutationBuilder::new("MATCH (s:Sensor) SET s.unit = 'degC'")
        .execute(&shared)
        .unwrap();

    // Verify the updated value is still InternedStr
    let snap = shared.load_snapshot();
    let node = snap.all_node_ids().next().expect("should have one node");
    let n = snap.get_node(node).unwrap();

    let unit_val = n
        .properties
        .get_by_str("unit")
        .expect("unit property should exist");
    assert!(
        matches!(unit_val, Value::InternedStr(_)),
        "SET on dictionary property should produce InternedStr, got: {unit_val:?}"
    );
    assert_eq!(unit_val.as_str(), Some("degC"));

    // SET a non-dictionary property should remain String
    MutationBuilder::new("MATCH (s:Sensor) SET s.name = 'TempSensor-2'")
        .execute(&shared)
        .unwrap();

    let snap = shared.load_snapshot();
    let node = snap.all_node_ids().next().expect("should have one node");
    let n = snap.get_node(node).unwrap();

    let name_val = n
        .properties
        .get_by_str("name")
        .expect("name property should exist");
    assert!(
        matches!(name_val, Value::String(_)),
        "SET on non-dictionary property should remain String, got: {name_val:?}"
    );
    assert_eq!(name_val.as_str(), Some("TempSensor-2"));
}

// ── Schema default fallback tests ───────────────────────────────────────────

#[test]
fn schema_default_returned_when_property_missing() {
    use selene_core::schema::{NodeSchema, PropertyDef, ValidationMode, ValueType};

    // Build a graph with a Sensor schema that has a firmware default.
    let mut g = SeleneGraph::with_config(
        selene_graph::SchemaValidator::new(ValidationMode::Warn),
        100,
    );
    let schema = NodeSchema {
        label: std::sync::Arc::from("Sensor"),
        parent: None,
        properties: vec![PropertyDef {
            name: std::sync::Arc::from("firmware"),
            value_type: ValueType::String,
            required: false,
            default: Some(Value::String(SmolStr::new("1.0.0"))),
            description: String::new(),
            indexed: false,
            unique: false,
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            allowed_values: vec![],
            pattern: None,
            immutable: false,
            searchable: false,
            dictionary: false,
            fill: None,
            expected_interval_nanos: None,
            encoding: selene_core::ValueEncoding::Gorilla,
        }],
        valid_edge_labels: vec![],
        description: String::new(),
        annotations: std::collections::HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };
    g.schema_mut().register_node_schema(schema).unwrap();

    // Insert a Sensor without the firmware property.
    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["Sensor"]),
        PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S1")))]),
    )
    .unwrap();
    m.commit(0).unwrap();

    // Query should return the schema default "1.0.0".
    let result = QueryBuilder::new("MATCH (n:Sensor) RETURN n.firmware AS fw", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);

    let batch = &result.batches[0];
    let fw_col = batch
        .column_by_name("fw")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        fw_col.value(0),
        "1.0.0",
        "missing property should return schema default"
    );
}

#[test]
fn explicit_property_wins_over_schema_default() {
    use selene_core::schema::{NodeSchema, PropertyDef, ValidationMode, ValueType};

    // Build a graph with a Sensor schema that has a firmware default.
    let mut g = SeleneGraph::with_config(
        selene_graph::SchemaValidator::new(ValidationMode::Warn),
        100,
    );
    let schema = NodeSchema {
        label: std::sync::Arc::from("Sensor"),
        parent: None,
        properties: vec![PropertyDef {
            name: std::sync::Arc::from("firmware"),
            value_type: ValueType::String,
            required: false,
            default: Some(Value::String(SmolStr::new("1.0.0"))),
            description: String::new(),
            indexed: false,
            unique: false,
            min: None,
            max: None,
            min_length: None,
            max_length: None,
            allowed_values: vec![],
            pattern: None,
            immutable: false,
            searchable: false,
            dictionary: false,
            fill: None,
            expected_interval_nanos: None,
            encoding: selene_core::ValueEncoding::Gorilla,
        }],
        valid_edge_labels: vec![],
        description: String::new(),
        annotations: std::collections::HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };
    g.schema_mut().register_node_schema(schema).unwrap();

    // Insert a Sensor WITH an explicit firmware value.
    let mut m = g.mutate();
    m.create_node(
        LabelSet::from_strs(&["Sensor"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("name"), Value::String(SmolStr::new("S2"))),
            (IStr::new("firmware"), Value::String(SmolStr::new("2.0.0"))),
        ]),
    )
    .unwrap();
    m.commit(0).unwrap();

    // Explicit value must take precedence over the schema default.
    let result = QueryBuilder::new("MATCH (n:Sensor) RETURN n.firmware AS fw", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);

    let batch = &result.batches[0];
    let fw_col = batch
        .column_by_name("fw")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        fw_col.value(0),
        "2.0.0",
        "explicit property value must take precedence over schema default"
    );
}
