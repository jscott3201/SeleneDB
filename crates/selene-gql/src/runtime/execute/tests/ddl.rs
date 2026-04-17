//! Type DDL tests: CREATE/DROP/SHOW NODE TYPE, edge types, encoding,
//! inheritance, constraints, and schema roundtrips.

use super::*;

#[test]
fn type_ddl_create_node_type() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new(
        "CREATE NODE TYPE :sensor (temp :: FLOAT NOT NULL, unit :: STRING DEFAULT '°C' IMMUTABLE)",
    )
    .execute(&shared)
    .unwrap();
    assert_eq!(result.row_count(), 0);

    // Verify schema was registered
    let schema = shared.read(|g| g.schema().node_schema("sensor").cloned());
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.properties.len(), 2);
    assert_eq!(schema.properties[0].name.as_ref(), "temp");
    assert!(schema.properties[0].required);
    assert_eq!(schema.properties[1].name.as_ref(), "unit");
    assert!(schema.properties[1].immutable);
    assert_eq!(
        schema.properties[1].default,
        Some(Value::String(SmolStr::new("°C")))
    );
}

#[test]
fn type_ddl_create_node_type_or_replace() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :sensor (temp :: FLOAT)")
        .execute(&shared)
        .unwrap();

    // OR REPLACE overwrites
    MutationBuilder::new("CREATE OR REPLACE NODE TYPE :sensor (temp :: FLOAT, humidity :: FLOAT)")
        .execute(&shared)
        .unwrap();

    let schema = shared.read(|g| g.schema().node_schema("sensor").cloned());
    assert_eq!(schema.unwrap().properties.len(), 2);
}

#[test]
fn type_ddl_create_node_type_if_not_exists() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :sensor (temp :: FLOAT)")
        .execute(&shared)
        .unwrap();

    // IF NOT EXISTS silently skips
    let result = MutationBuilder::new(
        "CREATE NODE TYPE IF NOT EXISTS :sensor (temp :: FLOAT, humidity :: FLOAT)",
    )
    .execute(&shared);
    assert!(result.is_ok());

    // Original schema unchanged
    let schema = shared.read(|g| g.schema().node_schema("sensor").cloned());
    assert_eq!(schema.unwrap().properties.len(), 1);
}

#[test]
fn type_ddl_create_node_type_duplicate_errors() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :sensor (temp :: FLOAT)")
        .execute(&shared)
        .unwrap();

    // Without OR REPLACE or IF NOT EXISTS, duplicate errors
    let result = MutationBuilder::new("CREATE NODE TYPE :sensor (temp :: FLOAT)").execute(&shared);
    assert!(result.is_err());
}

// ── Per-type validation mode (STRICT / WARN) ────────────────────────

#[test]
fn type_ddl_strict_clause_persists_on_schema() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :workflow (title :: STRING NOT NULL) STRICT")
        .execute(&shared)
        .unwrap();
    let schema = shared
        .read(|g| g.schema().node_schema("workflow").cloned())
        .unwrap();
    assert_eq!(
        schema.validation_mode,
        Some(selene_core::ValidationMode::Strict)
    );
}

#[test]
fn type_ddl_warn_clause_persists_on_schema() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :telemetry (temp :: FLOAT) WARN")
        .execute(&shared)
        .unwrap();
    let schema = shared
        .read(|g| g.schema().node_schema("telemetry").cloned())
        .unwrap();
    assert_eq!(
        schema.validation_mode,
        Some(selene_core::ValidationMode::Warn)
    );
}

#[test]
fn type_ddl_no_mode_clause_leaves_validation_unset() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :generic (x :: INT)")
        .execute(&shared)
        .unwrap();
    let schema = shared
        .read(|g| g.schema().node_schema("generic").cloned())
        .unwrap();
    assert_eq!(schema.validation_mode, None);
}

#[test]
fn type_ddl_edge_strict_clause_persists() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new(
        "CREATE EDGE TYPE :delivers (FROM :workflow TO :milestone, weight :: FLOAT NOT NULL) STRICT",
    )
    .execute(&shared)
    .unwrap();
    let schema = shared
        .read(|g| g.schema().edge_schema("delivers").cloned())
        .unwrap();
    assert_eq!(
        schema.validation_mode,
        Some(selene_core::ValidationMode::Strict)
    );
}

#[test]
fn type_ddl_per_type_strict_rejects_missing_required_even_if_global_warn() {
    // Global mode is default (Warn). Per-type STRICT on :workflow should still
    // block a missing required property.
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :workflow (title :: STRING NOT NULL) STRICT")
        .execute(&shared)
        .unwrap();
    // Missing required 'title' -- strict mode must reject.
    let err = MutationBuilder::new("INSERT (:workflow {status: 'open'})")
        .execute(&shared)
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("title"),
        "error should mention the missing required property: {err:?}"
    );
}

#[test]
fn type_ddl_per_type_warn_allows_missing_required_globally_warn() {
    // Explicit WARN per-type matches default; ensure it doesn't accidentally
    // promote to strict.
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :telemetry (temp :: FLOAT NOT NULL) WARN")
        .execute(&shared)
        .unwrap();
    // Missing required 'temp' under WARN is accepted (logged as warning).
    MutationBuilder::new("INSERT (:telemetry {source: 'sensor'})")
        .execute(&shared)
        .unwrap();
}

#[test]
fn type_ddl_sibling_types_have_independent_modes() {
    // Two types, different modes: writes to WARN type succeed, writes to
    // STRICT type are rejected -- proves modes are resolved per-label.
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :strict_t (name :: STRING NOT NULL) STRICT")
        .execute(&shared)
        .unwrap();
    MutationBuilder::new("CREATE NODE TYPE :warn_t (name :: STRING NOT NULL) WARN")
        .execute(&shared)
        .unwrap();

    // Warn type accepts despite missing required property.
    MutationBuilder::new("INSERT (:warn_t {id: 1})")
        .execute(&shared)
        .unwrap();

    // Strict type rejects.
    let err = MutationBuilder::new("INSERT (:strict_t {id: 1})")
        .execute(&shared)
        .unwrap_err();
    assert!(format!("{err:?}").contains("name"));
}

#[test]
fn type_ddl_encoding_constraint() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new(
        "CREATE NODE TYPE :sensor (occupied :: FLOAT ENCODING RLE, mode :: FLOAT ENCODING DICTIONARY, temp :: FLOAT)",
    )
    .execute(&shared)
    .unwrap();

    let schema = shared.read(|g| g.schema().node_schema("sensor").cloned());
    let schema = schema.unwrap();
    let occupied = schema
        .properties
        .iter()
        .find(|p| &*p.name == "occupied")
        .unwrap();
    assert_eq!(occupied.encoding, selene_core::ValueEncoding::Rle);
    let mode = schema
        .properties
        .iter()
        .find(|p| &*p.name == "mode")
        .unwrap();
    assert_eq!(mode.encoding, selene_core::ValueEncoding::Dictionary);
    let temp = schema
        .properties
        .iter()
        .find(|p| &*p.name == "temp")
        .unwrap();
    assert_eq!(temp.encoding, selene_core::ValueEncoding::Gorilla);
}

#[test]
fn type_ddl_unknown_encoding_errors() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("CREATE NODE TYPE :sensor (temp :: FLOAT ENCODING SNAPPY)")
        .execute(&shared);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{err:?}").contains("SNAPPY"),
        "error should mention the unknown encoding"
    );
}

#[test]
fn type_ddl_drop_node_type() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :sensor (temp :: FLOAT)")
        .execute(&shared)
        .unwrap();

    MutationBuilder::new("DROP NODE TYPE :sensor")
        .execute(&shared)
        .unwrap();

    let schema = shared.read(|g| g.schema().node_schema("sensor").cloned());
    assert!(schema.is_none());
}

#[test]
fn type_ddl_drop_node_type_if_exists() {
    let shared = SharedGraph::new(SeleneGraph::new());
    // DROP IF EXISTS on nonexistent type is fine
    let result = MutationBuilder::new("DROP NODE TYPE IF EXISTS :sensor").execute(&shared);
    assert!(result.is_ok());
}

#[test]
fn type_ddl_drop_node_type_missing_errors() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("DROP NODE TYPE :sensor").execute(&shared);
    assert!(result.is_err());
}

#[test]
fn type_ddl_show_node_types() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :sensor (temp :: FLOAT, unit :: STRING)")
        .execute(&shared)
        .unwrap();
    MutationBuilder::new("CREATE NODE TYPE :building (name :: STRING NOT NULL)")
        .execute(&shared)
        .unwrap();

    let result = MutationBuilder::new("SHOW NODE TYPES")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.batches.len(), 1);
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.schema.fields().len(), 4); // label, parent, property_count, description
}

#[test]
fn type_ddl_extends_inherits_properties() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE NODE TYPE :sensor (unit :: STRING DEFAULT '°C' IMMUTABLE)")
        .execute(&shared)
        .unwrap();

    MutationBuilder::new(
            "CREATE NODE TYPE :temperature_sensor EXTENDS :sensor (quantity :: STRING DEFAULT 'temperature' IMMUTABLE)",
            ).execute(&shared)
        .unwrap();

    let schema = shared.read(|g| g.schema().node_schema("temperature_sensor").cloned());
    let schema = schema.unwrap();
    // Parent property (unit) + child property (quantity)
    assert_eq!(schema.properties.len(), 2);
    assert_eq!(schema.properties[0].name.as_ref(), "unit"); // inherited first
    assert_eq!(schema.properties[1].name.as_ref(), "quantity");
}

#[test]
fn type_ddl_extends_missing_parent_errors() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result =
        MutationBuilder::new("CREATE NODE TYPE :child EXTENDS :nonexistent (prop :: STRING)")
            .execute(&shared);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[test]
fn type_ddl_extends_cycle_errors() {
    let shared = SharedGraph::new(SeleneGraph::new());
    // Create a -> b
    MutationBuilder::new("CREATE NODE TYPE :a (x :: INT)")
        .execute(&shared)
        .unwrap();
    MutationBuilder::new("CREATE NODE TYPE :b EXTENDS :a (y :: INT)")
        .execute(&shared)
        .unwrap();

    // Try to create a self-cycle: c EXTENDS c --but c doesn't exist yet as parent
    // So test: try to replace a with a EXTENDS b (creating a->b->a cycle)
    let result = MutationBuilder::new("CREATE OR REPLACE NODE TYPE :a EXTENDS :b (x :: INT)")
        .execute(&shared);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cycle"));
}

#[test]
fn type_ddl_create_edge_type() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new(
        "CREATE EDGE TYPE :contains (FROM :building, :floor TO :floor, :room)",
    )
    .execute(&shared)
    .unwrap();
    assert_eq!(result.row_count(), 0);

    let schema = shared.read(|g| g.schema().edge_schema("contains").cloned());
    assert!(schema.is_some());
    let schema = schema.unwrap();
    assert_eq!(schema.source_labels.len(), 2);
    assert_eq!(schema.target_labels.len(), 2);
}

#[test]
fn type_ddl_create_edge_type_with_properties() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE EDGE TYPE :relates (FROM :a TO :b, weight :: FLOAT DEFAULT 1.0)")
        .execute(&shared)
        .unwrap();

    let schema = shared.read(|g| g.schema().edge_schema("relates").cloned());
    let schema = schema.unwrap();
    assert_eq!(schema.source_labels.len(), 1);
    assert_eq!(schema.target_labels.len(), 1);
    assert_eq!(schema.properties.len(), 1);
    assert_eq!(schema.properties[0].name.as_ref(), "weight");
}

#[test]
fn type_ddl_create_edge_type_props_only() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE EDGE TYPE :relates (weight :: FLOAT, label :: STRING)")
        .execute(&shared)
        .unwrap();

    let schema = shared.read(|g| g.schema().edge_schema("relates").cloned());
    let schema = schema.unwrap();
    assert!(schema.source_labels.is_empty());
    assert!(schema.target_labels.is_empty());
    assert_eq!(schema.properties.len(), 2);
}

#[test]
fn type_ddl_drop_edge_type() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE EDGE TYPE :contains (FROM :building TO :floor)")
        .execute(&shared)
        .unwrap();
    MutationBuilder::new("DROP EDGE TYPE :contains")
        .execute(&shared)
        .unwrap();

    let schema = shared.read(|g| g.schema().edge_schema("contains").cloned());
    assert!(schema.is_none());
}

#[test]
fn type_ddl_show_edge_types() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new("CREATE EDGE TYPE :contains (FROM :building TO :floor)")
        .execute(&shared)
        .unwrap();

    let result = MutationBuilder::new("SHOW EDGE TYPES")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.batches.len(), 1);
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.schema.fields().len(), 4); // label, source_labels, target_labels, property_count
}

#[test]
fn type_ddl_roundtrip_create_type_then_insert() {
    let shared = SharedGraph::new(SeleneGraph::new());
    // Create a node type with defaults
    MutationBuilder::new(
            "CREATE NODE TYPE :sensor (unit :: STRING DEFAULT '°C' IMMUTABLE, quality :: INT DEFAULT 3)",
            ).execute(&shared)
        .unwrap();

    // Insert a node of that type --defaults should apply
    MutationBuilder::new("INSERT (:sensor {name: 'T1'})")
        .execute(&shared)
        .unwrap();

    // Verify defaults were applied
    let unit = shared.read(|g| {
        g.get_node(NodeId(1))
            .and_then(|n| n.property("unit").cloned())
    });
    assert_eq!(unit, Some(Value::String(SmolStr::new("°C"))));

    let quality = shared.read(|g| {
        g.get_node(NodeId(1))
            .and_then(|n| n.property("quality").cloned())
    });
    assert_eq!(quality, Some(Value::Int(3)));
}

#[test]
fn type_ddl_all_constraint_keywords() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new(
            "CREATE NODE TYPE :full (a :: STRING NOT NULL IMMUTABLE UNIQUE INDEXED SEARCHABLE DEFAULT 'hello')",
            ).execute(&shared)
        .unwrap();

    let schema = shared
        .read(|g| g.schema().node_schema("full").cloned())
        .unwrap();
    let prop = &schema.properties[0];
    assert!(prop.required);
    assert!(prop.immutable);
    assert!(prop.unique);
    assert!(prop.indexed);
    assert!(prop.searchable);
    assert_eq!(prop.default, Some(Value::String(SmolStr::new("hello"))));
}

#[test]
fn type_ddl_value_type_mapping() {
    let shared = SharedGraph::new(SeleneGraph::new());
    MutationBuilder::new(
            "CREATE NODE TYPE :typed (a :: BOOL, b :: INT, c :: UINT, d :: FLOAT, e :: STRING, f :: BYTES, g :: VECTOR, h :: DATE, i :: DURATION)",
            ).execute(&shared)
        .unwrap();

    let schema = shared
        .read(|g| g.schema().node_schema("typed").cloned())
        .unwrap();
    assert_eq!(schema.properties.len(), 9);
    assert_eq!(
        schema.properties[0].value_type,
        selene_core::ValueType::Bool
    );
    assert_eq!(schema.properties[1].value_type, selene_core::ValueType::Int);
    assert_eq!(
        schema.properties[2].value_type,
        selene_core::ValueType::UInt
    );
    assert_eq!(
        schema.properties[3].value_type,
        selene_core::ValueType::Float
    );
    assert_eq!(
        schema.properties[4].value_type,
        selene_core::ValueType::String
    );
    assert_eq!(
        schema.properties[5].value_type,
        selene_core::ValueType::Bytes
    );
    assert_eq!(
        schema.properties[6].value_type,
        selene_core::ValueType::Vector
    );
    assert_eq!(
        schema.properties[7].value_type,
        selene_core::ValueType::Date
    );
    assert_eq!(
        schema.properties[8].value_type,
        selene_core::ValueType::Duration
    );
}
