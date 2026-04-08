//! Index-ordered scan, INSERT working table (RETURN after INSERT),
//! LIKE, BETWEEN, DIFFERENT EDGES, and misc predicate tests.

use super::*;

// ── Index-ordered scan tests ────────────────────────────────────

#[test]
fn index_ordered_scan_numeric_sort() {
    use selene_core::schema::{NodeSchema, PropertyDef, ValidationMode, ValueType};

    // Create a graph with an indexed FLOAT property
    let mut g = SeleneGraph::with_config(
        selene_graph::SchemaValidator::new(ValidationMode::Warn),
        100,
    );
    let schema = NodeSchema {
        label: std::sync::Arc::from("sensor"),
        parent: None,
        properties: vec![PropertyDef {
            name: std::sync::Arc::from("temp"),
            value_type: ValueType::Float,
            required: false,
            default: None,
            description: String::new(),
            indexed: true,
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

    // Insert nodes with temperatures that sort differently as strings vs numbers
    // String order: "100" < "2" < "50" < "9"
    // Numeric order: 2 < 9 < 50 < 100
    let mut m = g.mutate();
    for (i, temp) in [100.0, 2.0, 50.0, 9.0].iter().enumerate() {
        let id = m
            .create_node(
                selene_core::LabelSet::from_strs(&["sensor"]),
                selene_core::PropertyMap::from_pairs(vec![
                    (IStr::new("temp"), Value::Float(*temp)),
                    (
                        IStr::new("name"),
                        Value::String(SmolStr::new(format!("S{}", i + 1))),
                    ),
                ]),
            )
            .unwrap();
        let _ = id;
    }
    m.commit(0).unwrap();
    g.build_property_indexes();
    g.build_composite_indexes();

    // Query with ORDER BY temp ASC LIMIT 3 --should use index-ordered scan
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.temp AS temp ORDER BY s.temp LIMIT 3",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 3);

    // Extract the temp values from the result
    let batch = &result.batches[0];
    let col = batch.column_by_name("TEMP").expect("temp column");
    let values: Vec<f64> = col
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("float64 array")
        .values()
        .to_vec();

    // Should be in ascending numeric order: 2.0, 9.0, 50.0
    assert_eq!(values, vec![2.0, 9.0, 50.0]);
}

// ── INSERT working table (RETURN after INSERT) tests ─────────────

#[test]
fn e2e_insert_return_created_node() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new("INSERT (A:person {name: 'Alice'}) RETURN A.name AS NAME")
        .execute(&shared)
        .unwrap();
    assert_eq!(result.row_count(), 1);
    let col = result.batches[0].column_by_name("NAME").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    // String values include GQL-style quotes in Display-based Arrow serialization
    assert!(
        arr.value(0).contains("Alice"),
        "expected Alice in RETURN, got: {}",
        arr.value(0)
    );
}

#[test]
fn e2e_insert_path_return_both_nodes() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new(
            "INSERT (A:person {name: 'Alice'})-[:KNOWS]->(B:person {name: 'Bob'}) RETURN A.name AS ANAME, B.name AS BNAME",
            ).execute(&shared)
        .unwrap();
    assert_eq!(result.row_count(), 1);
    let col_a = result.batches[0].column_by_name("ANAME").unwrap();
    let arr_a = col_a
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(
        arr_a.value(0).contains("Alice"),
        "expected Alice in RETURN, got: {}",
        arr_a.value(0)
    );
    let col_b = result.batches[0].column_by_name("BNAME").unwrap();
    let arr_b = col_b
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(
        arr_b.value(0).contains("Bob"),
        "expected Bob in RETURN, got: {}",
        arr_b.value(0)
    );
}

#[test]
fn e2e_set_swap_values() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let a = m
        .create_node(
            LabelSet::from_strs(&["pair"]),
            PropertyMap::from_pairs(vec![(IStr::new("val"), Value::Int(1))]),
        )
        .unwrap();
    let b = m
        .create_node(
            LabelSet::from_strs(&["pair"]),
            PropertyMap::from_pairs(vec![(IStr::new("val"), Value::Int(2))]),
        )
        .unwrap();
    m.create_edge(a, IStr::new("link"), b, PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();
    let shared = SharedGraph::new(g);
    MutationBuilder::new("MATCH (A:pair)-[:link]->(B:pair) SET A.val = B.val, B.val = A.val")
        .execute(&shared)
        .unwrap();
    let snap = shared.load_snapshot();
    let a_val = snap
        .get_node(a)
        .unwrap()
        .properties
        .get_by_str("val")
        .unwrap();
    let b_val = snap
        .get_node(b)
        .unwrap()
        .properties
        .get_by_str("val")
        .unwrap();
    assert_eq!(*a_val, Value::Int(2), "a should have b's original value");
    assert_eq!(*b_val, Value::Int(1), "b should have a's original value");
}

#[test]
fn e2e_unknown_is_unknown() {
    let g = SeleneGraph::new();
    let result = QueryBuilder::new("RETURN UNKNOWN IS UNKNOWN AS R", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
    let col = result.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0), "UNKNOWN IS UNKNOWN should be TRUE");
}

#[test]
fn e2e_true_is_not_unknown() {
    let g = SeleneGraph::new();
    let result = QueryBuilder::new("RETURN TRUE IS UNKNOWN AS R", &g)
        .execute()
        .unwrap();
    let col = result.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(!arr.value(0), "TRUE IS UNKNOWN should be FALSE");
}

#[test]
fn e2e_all_different_null_error() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (N:sensor) RETURN ALL_DIFFERENT(N, NULL)", &g).execute();
    assert!(result.is_err(), "ALL_DIFFERENT with NULL should error");
}

#[test]
fn e2e_same_null_error() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (N:sensor) RETURN SAME(N, NULL)", &g).execute();
    assert!(result.is_err(), "SAME with NULL should error");
}

#[test]
fn e2e_strict_rejects_string_int_comparison() {
    let g = SeleneGraph::new();
    let opts = crate::GqlOptions {
        strict_coercion: true,
        ..Default::default()
    };
    let result = QueryBuilder::new("RETURN '72' = 72 AS R", &g)
        .with_options(&opts)
        .execute();
    assert!(result.is_err(), "strict mode should reject '72' = 72");
    assert!(
        result.unwrap_err().to_string().contains("CAST"),
        "error message should contain CAST hint"
    );
}

#[test]
fn e2e_permissive_allows_string_int_comparison() {
    let g = SeleneGraph::new();
    let result = QueryBuilder::new("RETURN '72' = 72 AS R", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

// ── LIKE predicate tests ──

#[test]
fn e2e_like_percent() {
    let g = SeleneGraph::new();
    let r = QueryBuilder::new("RETURN 'hello world' LIKE 'hello%' AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0));
}

#[test]
fn e2e_like_underscore() {
    let g = SeleneGraph::new();
    let r = QueryBuilder::new("RETURN 'hello' LIKE 'h_llo' AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0));
}

#[test]
fn e2e_like_case_sensitive() {
    let g = SeleneGraph::new();
    let r = QueryBuilder::new("RETURN 'Hello' LIKE 'hello' AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(!arr.value(0));
}

#[test]
fn e2e_not_like() {
    let g = SeleneGraph::new();
    let r = QueryBuilder::new("RETURN 'hello' NOT LIKE '%world%' AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0));
}

#[test]
fn e2e_like_pattern_containing_null() {
    // Regression: string 'NULL_THING' in LIKE pattern must not misroute to IS NULL
    let g = SeleneGraph::new();
    // Verify the parse is correct by checking AST
    let stmt = crate::parser::parse_statement("RETURN 'NULLX' LIKE 'NULL%' AS R").unwrap();
    let like_found = format!("{stmt:?}").contains("Like");
    assert!(like_found, "Expected LIKE expression in AST, got: {stmt:?}");
    // Verify LIKE evaluation with NULL-prefix pattern
    let r = QueryBuilder::new("RETURN 'NULL_THING' LIKE 'NULL%' AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0), "'NULL_THING' LIKE 'NULL%' should be true");
}

#[test]
fn e2e_like_percent_matches_newlines() {
    let g = SeleneGraph::new();
    // % should match any character sequence including newlines
    let r = QueryBuilder::new("RETURN 'line1\nline2' LIKE '%' AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0));
}

// ── BETWEEN predicate tests ──

#[test]
fn e2e_between_true() {
    let g = SeleneGraph::new();
    let r = QueryBuilder::new("RETURN 5 BETWEEN 1 AND 10 AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0));
}

#[test]
fn e2e_between_false() {
    let g = SeleneGraph::new();
    let r = QueryBuilder::new("RETURN 15 BETWEEN 1 AND 10 AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(!arr.value(0));
}

#[test]
fn e2e_between_inclusive() {
    let g = SeleneGraph::new();
    let r = QueryBuilder::new("RETURN 10 BETWEEN 1 AND 10 AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0));
}

#[test]
fn e2e_not_between() {
    let g = SeleneGraph::new();
    let r = QueryBuilder::new("RETURN 15 NOT BETWEEN 1 AND 10 AS R", &g)
        .execute()
        .unwrap();
    let col = r.batches[0].column_by_name("R").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(arr.value(0));
}

#[test]
fn e2e_between_type_mismatch_errors() {
    let g = SeleneGraph::new();
    // Comparing string with integers should produce a type error, not silently return false
    let r = QueryBuilder::new("RETURN 'hello' BETWEEN 1 AND 10 AS R", &g).execute();
    assert!(r.is_err(), "BETWEEN with incompatible types should error");
}

// ── DIFFERENT EDGES match mode tests ──

#[test]
fn e2e_different_edges_mode() {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let a = m
        .create_node(LabelSet::from_strs(&["N"]), PropertyMap::new())
        .unwrap();
    let b = m
        .create_node(LabelSet::from_strs(&["N"]), PropertyMap::new())
        .unwrap();
    let c = m
        .create_node(LabelSet::from_strs(&["N"]), PropertyMap::new())
        .unwrap();
    m.create_edge(a, IStr::new("R"), b, PropertyMap::new())
        .unwrap();
    m.create_edge(b, IStr::new("R"), c, PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();

    // Without DIFFERENT EDGES --normal behavior
    let r1 = QueryBuilder::new(
        "MATCH (X:N)-[E1:R]->(Y:N), (Y)-[E2:R]->(Z:N) RETURN X, Z",
        &g,
    )
    .execute()
    .unwrap();
    assert!(r1.row_count() > 0);

    // With DIFFERENT EDGES --e1 and e2 must be distinct
    let r2 = QueryBuilder::new(
        "MATCH DIFFERENT EDGES (X:N)-[E1:R]->(Y:N), (Y)-[E2:R]->(Z:N) RETURN X, Z",
        &g,
    )
    .execute()
    .unwrap();
    // Should still work since e1 (a->b) and e2 (b->c) are different edges
    assert!(r2.row_count() > 0);
}

#[test]
fn e2e_different_edges_filters_duplicate() {
    // Test that DIFFERENT EDGES actually filters when the same edge could match both vars.
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let a = m
        .create_node(LabelSet::from_strs(&["N"]), PropertyMap::new())
        .unwrap();
    let b = m
        .create_node(LabelSet::from_strs(&["N"]), PropertyMap::new())
        .unwrap();
    // Single edge a->b --the same edge can match both E1 and E2
    m.create_edge(a, IStr::new("R"), b, PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();

    // Without DIFFERENT EDGES: (X)-[E1:R]->(Y), (X)-[E2:R]->(Y) can both bind the same edge
    let r1 = QueryBuilder::new(
        "MATCH (X:N)-[E1:R]->(Y:N), (X)-[E2:R]->(Y) RETURN E1, E2",
        &g,
    )
    .execute()
    .unwrap();

    // With DIFFERENT EDGES: the same edge cannot match both E1 and E2
    let r2 = QueryBuilder::new(
        "MATCH DIFFERENT EDGES (X:N)-[E1:R]->(Y:N), (X)-[E2:R]->(Y) RETURN E1, E2",
        &g,
    )
    .execute()
    .unwrap();

    // r2 should have fewer (or zero) rows since the only edge can't be used for both vars
    assert!(
        r2.row_count() < r1.row_count() || r1.row_count() == 0,
        "DIFFERENT EDGES should filter out bindings where E1 == E2. r1={}, r2={}",
        r1.row_count(),
        r2.row_count()
    );
}

#[test]
fn e2e_insert_boolean_properties() {
    let shared = SharedGraph::new(SeleneGraph::new());
    let result = MutationBuilder::new(
        "INSERT (n:test {name: 'x', active: true, deleted: false}) RETURN id(n) AS id",
    )
    .execute(&shared)
    .unwrap();
    assert_eq!(result.row_count(), 1, "INSERT with booleans should create one node");

    shared.read(|graph| {
        let node = graph.get_node(selene_core::NodeId(1)).unwrap();
        assert_eq!(
            node.properties.get(IStr::new("active")),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            node.properties.get(IStr::new("deleted")),
            Some(&Value::Bool(false))
        );
    });
}
