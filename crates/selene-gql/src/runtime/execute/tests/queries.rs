//! End-to-end query tests and type inference tests.

use super::*;

// ── End-to-end query tests ──

#[test]
fn e2e_match_return_count() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (n) RETURN count(*) AS total", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result.status.code,
        crate::types::error::GqlStatusCode::Success
    );
}

#[test]
fn e2e_match_labeled_return() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN s.name AS name", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn e2e_filter() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) FILTER s.temp > 75 RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1); // Only TempSensor-2 (80.0)
}

#[test]
fn e2e_order_by_desc_limit() {
    let g = setup_graph();
    // ORDER BY DESC LIMIT 1 should return the sensor with the highest temp (80.0)
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.name AS name, s.temp AS temp ORDER BY s.temp DESC LIMIT 1",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);

    // Verify the actual value: should be TempSensor-2 with temp=80.0
    let batch = &result.batches[0];
    let temp_col = batch
        .column_by_name("TEMP")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(
        temp_col.value(0),
        80.0,
        "ORDER BY DESC should return highest temp first"
    );
}

#[test]
fn e2e_order_by_asc_returns_sorted() {
    let g = setup_graph();
    // ORDER BY ASC should return sensors in ascending temp order
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.name AS name, s.temp AS temp ORDER BY s.temp",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);

    let batch = &result.batches[0];
    let temp_col = batch
        .column_by_name("TEMP")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(temp_col.value(0), 72.5, "first row should be lowest temp");
    assert_eq!(temp_col.value(1), 80.0, "second row should be highest temp");
}

#[test]
fn e2e_order_by_hidden_column() {
    let g = setup_graph();
    // ORDER BY s.temp but only RETURN s.name --temp should not appear in output schema
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.name AS name ORDER BY s.temp DESC",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.column_count(), 1, "only 'name' should be in output");

    let batch = &result.batches[0];
    let name_col = batch
        .column_by_name("NAME")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    // String values may be serialized with quotes depending on the GqlValue->Arrow path
    assert!(
        name_col.value(0).contains("TempSensor-2"),
        "highest temp first in DESC order, got: {}",
        name_col.value(0)
    );
    assert!(
        name_col.value(1).contains("TempSensor-1"),
        "lowest temp second, got: {}",
        name_col.value(1)
    );
}

#[test]
fn e2e_edge_traversal() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (b:building)-[:contains]->(f:floor) RETURN b.name AS building, f.name AS floor",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1); // HQ -> Floor-1
}

#[test]
fn e2e_two_hop_traversal() {
    let g = setup_graph();
    let result = QueryBuilder::new(
            "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:sensor) RETURN b.name AS building, s.name AS sensor",
            &g,
            ).execute()
        .unwrap();
    assert_eq!(result.row_count(), 2); // HQ -> Floor-1 -> 2 sensors
}

#[test]
fn e2e_var_length_path_all() {
    let g = setup_graph();
    // No target label filter --returns all reachable nodes
    let result = QueryBuilder::new(
        "MATCH (b:building)-[:contains]->{1,3}(s) RETURN b.name AS building, s.id AS node_id",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 3); // floor + 2 sensors
}

#[test]
fn e2e_var_length_path_with_target_label() {
    let g = setup_graph();
    // Target label :sensor --only emit sensor nodes, traverse through floor
    let result = QueryBuilder::new(
            "MATCH (b:building)-[:contains]->{1,3}(s:sensor) RETURN b.name AS building, s.id AS sensor_id",
            &g,
            ).execute()
        .unwrap();
    assert_eq!(result.row_count(), 2); // only the 2 sensors
}

#[test]
fn e2e_left_directed_edge_with_quantifier() {
    let g = setup_graph();
    // Left-directed edge with + quantifier should parse and execute
    let result = QueryBuilder::new(
        "MATCH (s:sensor)<-[:contains]-+(b) RETURN s.name AS sensor, b.name AS parent",
        &g,
    )
    .execute();
    assert!(
        result.is_ok(),
        "Left-directed edge with quantifier should parse: {:?}",
        result.err()
    );
}

#[test]
fn e2e_let_filter() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) LET hot = s.temp FILTER hot > 75 RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_aggregation_avg() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN avg(s.temp) AS avg_temp", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_inline_properties() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor {name: 'TempSensor-1'}) RETURN s.temp AS temp",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_count_with_label() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN count(*) AS n", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_no_results() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:nonexistent_label) RETURN s.name AS name", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 0);
    assert_eq!(
        result.status.code,
        crate::types::error::GqlStatusCode::NoData
    );
}

#[test]
fn e2e_transaction_keywords_rejected() {
    let g = setup_graph();
    // Transaction keywords are not supported over the wire protocol.
    // They must return an error instead of silently succeeding as no-ops.
    assert!(
        QueryBuilder::new("START TRANSACTION", &g)
            .execute()
            .is_err()
    );
    assert!(QueryBuilder::new("COMMIT", &g).execute().is_err());
    assert!(QueryBuilder::new("ROLLBACK", &g).execute().is_err());
}

#[test]
fn e2e_insert_counts() {
    let g = setup_graph();
    let result = QueryBuilder::new("INSERT (:sensor {name: 'NewSensor'})", &g)
        .execute()
        .unwrap();
    assert_eq!(result.mutations.nodes_created, 1);
}

#[test]
fn e2e_set_property_counts() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) FILTER s.name = 'TempSensor-1' SET s.alert = TRUE RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.mutations.properties_set, 1);
}

#[test]
fn e2e_parse_error() {
    let g = setup_graph();
    let result = QueryBuilder::new("INVALID QUERY", &g).execute();
    assert!(result.is_err());
    match result.unwrap_err() {
        GqlError::Parse { position, .. } => {
            assert!(position.is_some());
        }
        _ => panic!("expected Parse error"),
    }
}

#[test]
fn e2e_arrow_output_has_schema() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.name AS sensor_name, s.temp AS temperature",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.column_count(), 2);
    assert_eq!(result.schema.field(0).name(), "SENSOR_NAME");
    assert_eq!(result.schema.field(1).name(), "TEMPERATURE");
    assert_eq!(result.batches.len(), 1);
    assert_eq!(result.batches[0].num_rows(), 2);
}

// ── Type inference tests ──

#[test]
fn e2e_type_inference_float() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN s.temp AS temperature", &g)
        .execute()
        .unwrap();
    assert_eq!(
        *result.schema.field(0).data_type(),
        arrow::datatypes::DataType::Float64
    );
}

#[test]
fn e2e_type_inference_string() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN s.name AS name", &g)
        .execute()
        .unwrap();
    assert_eq!(
        *result.schema.field(0).data_type(),
        arrow::datatypes::DataType::Utf8
    );
}

#[test]
fn e2e_type_inference_int_count() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN count(*) AS total", &g)
        .execute()
        .unwrap();
    assert_eq!(
        *result.schema.field(0).data_type(),
        arrow::datatypes::DataType::Int64
    );
}

#[test]
fn e2e_type_inference_bool() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN TRUE AS flag", &g)
        .execute()
        .unwrap();
    assert_eq!(
        *result.schema.field(0).data_type(),
        arrow::datatypes::DataType::Boolean
    );
}

#[test]
fn e2e_type_inference_empty_result_fallback() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:nonexistent) RETURN s.name AS name", &g)
        .execute()
        .unwrap();
    assert_eq!(
        *result.schema.field(0).data_type(),
        arrow::datatypes::DataType::Utf8
    );
}

// ── YIELD * wildcard tests ──

#[test]
fn yield_star_returns_all_columns() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    // Use YIELD * to get all columns, then project specific column
    let result = QueryBuilder::new("CALL graph.procedures() YIELD * RETURN NAME", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(result.row_count() > 0);
}

#[test]
fn yield_explicit_column_still_works() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    let result = QueryBuilder::new("CALL graph.procedures() YIELD name RETURN name", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(result.row_count() > 0);
}

// ── GROUP BY expression tests ──

#[test]
fn group_by_property_expression() {
    let g = setup_graph();
    // Group by sensor name property expression
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.name AS name, count(*) AS cnt GROUP BY s.name",
        &g,
    )
    .execute()
    .unwrap();
    // Each sensor has a unique name, so one group per sensor
    assert_eq!(result.row_count(), 2);
}

#[test]
fn group_by_variable_still_works() {
    let g = setup_graph();
    // Use a LET-bound variable so it exists before RETURN
    let result = QueryBuilder::new(
        "MATCH (n) LET t = n.name RETURN t, count(*) AS cnt GROUP BY t",
        &g,
    )
    .execute()
    .unwrap();
    // Each node has a unique name, so one group per node
    assert!(result.row_count() >= 3);
}

// GROUP BY projection alias: `GROUP BY alias` where alias is defined in the same
// RETURN or WITH clause.  Before the fix these returned "unbound variable".
#[test]
fn group_by_return_alias() {
    let g = setup_graph();
    // RETURN s.name AS nm, count(*) AS cnt GROUP BY nm
    // nm is an alias for s.name, not a pre-existing binding variable.
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.name AS nm, count(*) AS cnt GROUP BY nm",
        &g,
    )
    .execute()
    .unwrap();
    // Two sensors, each with a unique name -> two groups, each with count 1.
    assert_eq!(result.row_count(), 2);
}

#[test]
fn group_by_with_alias() {
    let g = setup_graph();
    // WITH s.name AS nm, count(s) AS cnt GROUP BY nm RETURN nm, cnt
    let result = QueryBuilder::new(
        "MATCH (s:sensor) WITH s.name AS nm, count(s) AS cnt GROUP BY nm RETURN nm, cnt",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn group_by_case_alias() {
    let g = setup_graph();
    // GROUP BY a CASE expression alias.
    // Sensor temps are 72.5 and 80.0 -> one 'hot' (>= 75) and one 'cool'.
    let result = QueryBuilder::new(
        "MATCH (s:sensor) \
         RETURN CASE WHEN s.temp >= 75.0 THEN 'hot' ELSE 'cool' END AS bucket, \
                count(*) AS cnt \
         GROUP BY bucket",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
}

// ── Standalone CALL/YIELD (no RETURN) tests ──

#[test]
fn call_yield_without_return_produces_rows() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    let result = QueryBuilder::new("CALL graph.procedures() YIELD name", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(
        result.row_count() > 0,
        "standalone CALL/YIELD should return rows without RETURN"
    );
}

#[test]
fn call_yield_with_return_still_works() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    let result = QueryBuilder::new("CALL graph.procedures() YIELD name RETURN name", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(
        result.row_count() > 0,
        "CALL/YIELD followed by RETURN should still work"
    );
}

#[test]
fn yield_star_return_star_returns_all_columns() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    let result = QueryBuilder::new("CALL graph.labels() YIELD * RETURN *", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(
        result.row_count() > 0,
        "YIELD * RETURN * should return rows"
    );
}

#[test]
fn yield_star_return_star_procedures() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    let result = QueryBuilder::new("CALL graph.procedures() YIELD * RETURN *", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(
        result.row_count() > 0,
        "YIELD * RETURN * on procedures should return rows"
    );
}

#[test]
fn labels_function_displays_as_array() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (n:sensor) RETURN labels(n) AS lbls", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
    let batch = &result.batches[0];
    let col = batch
        .column_by_name("LBLS")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let val = col.value(0);
    assert!(
        val.starts_with('[') && val.ends_with(']'),
        "labels() should display as array, got: {val}"
    );
}

#[test]
fn labels_as_alias_name() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (n:sensor) RETURN labels(n) AS labels", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
    let batch = &result.batches[0];
    let col = batch
        .column_by_name("LABELS")
        .expect("alias 'labels' should be usable as column name");
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let val = arr.value(0);
    assert!(
        val.starts_with('[') && val.ends_with(']'),
        "labels() aliased as labels should work, got: {val}"
    );
}

#[test]
fn call_yield_where_filters_rows() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    // graph.labels() yields one row per distinct label; WHERE filters to just 'sensor'
    let result = QueryBuilder::new(
        "CALL graph.labels() YIELD label WHERE label = 'sensor' RETURN label",
        &g,
    )
    .with_procedures(procs)
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let batch = &result.batches[0];
    let col = batch
        .column_by_name("LABEL")
        .expect("should have LABEL column");
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "sensor");
}

// ── Case-insensitive procedure lookup tests ──

#[test]
fn call_case_insensitive_procedure_name() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    // Mixed case: graph.Labels should resolve to graph.labels
    let result = QueryBuilder::new("CALL graph.Labels() YIELD label RETURN label", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(
        result.row_count() > 0,
        "case-insensitive procedure lookup should find graph.labels"
    );
}

#[test]
fn call_upper_case_procedure_name() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    // Full uppercase
    let result = QueryBuilder::new("CALL GRAPH.LABELS() YIELD label RETURN label", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(
        result.row_count() > 0,
        "uppercase procedure name should resolve"
    );
}

// ── Underscore-insensitive YIELD column matching tests ──

#[test]
fn yield_underscore_flexible_column_match() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    // graph.labels yields column "label"; YIELD la_bel (with underscore)
    // should match after normalization (both become LABEL).
    let result = QueryBuilder::new("CALL graph.labels() YIELD la_bel RETURN la_bel", &g)
        .with_procedures(procs)
        .execute()
        .unwrap();
    assert!(
        result.row_count() > 0,
        "underscore-flexible YIELD match should work"
    );
}

// ── Backtick-escaped reserved keyword in YIELD+RETURN ──

#[test]
fn yield_reserved_keyword_backtick_escaped_in_return() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    // graph.validate yields a column named "count" which is a reserved keyword.
    // The YIELD clause accepts it unescaped, but RETURN requires backtick
    // escaping. The case-insensitive slot_of fallback must bridge the gap
    // between YIELD binding (COUNT) and RETURN reference (`count` → count).
    let result = QueryBuilder::new(
        "CALL graph.validate() YIELD check, status, count, details RETURN check, status, `count`, details",
        &g,
    )
    .with_procedures(procs)
    .execute()
    .unwrap();
    assert!(
        result.row_count() > 0,
        "backtick-escaped reserved keyword in RETURN should resolve"
    );
}

// ── YIELD column validation tests ──

#[test]
fn yield_nonexistent_column_returns_error() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    let result = QueryBuilder::new(
        "CALL graph.labels() YIELD nonexistent_col RETURN nonexistent_col",
        &g,
    )
    .with_procedures(procs)
    .execute();
    assert!(result.is_err(), "invalid YIELD column should error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("does not yield column"),
        "error should mention 'does not yield column', got: {err_msg}"
    );
    assert!(
        err_msg.contains("label"),
        "error should list available columns, got: {err_msg}"
    );
}

#[test]
fn yield_nonexistent_column_error_message_lists_available() {
    let g = setup_graph();
    let procs = ProcedureRegistry::builtins();
    let result = QueryBuilder::new("CALL graph.procedures() YIELD bogus RETURN bogus", &g)
        .with_procedures(procs)
        .execute();
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("name") && err_msg.contains("params") && err_msg.contains("yields"),
        "error should list all available columns: {err_msg}"
    );
}

// ── IntermediateFilter tests (WHERE pushdown to pattern layer) ──
// These verify that id(), labels(), and property functions work correctly
// when WHERE is pushed into an IntermediateFilter (before Join) rather
// than staying as a pipeline Filter (after pattern execution).

#[test]
fn where_id_function_in_intermediate_filter() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (n) WHERE id(n) = 3 RETURN n.name AS name", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
    let batch = &result.batches[0];
    let name = batch
        .column_by_name("NAME")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(name, "TempSensor-1");
}

#[test]
fn where_id_with_multi_match_pushdown() {
    let g = setup_graph();
    // Both WHERE clauses should push down as IntermediateFilters
    let result = QueryBuilder::new(
        "MATCH (a) WHERE id(a) = 1 MATCH (b) WHERE id(b) = 2 RETURN a.name AS aname, b.name AS bname",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let batch = &result.batches[0];
    let aname = batch
        .column_by_name("ANAME")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(aname, "HQ");
}

#[test]
fn where_labels_function_in_intermediate_filter() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (n) WHERE labels(n) = ['sensor'] RETURN count(*) AS cnt",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let batch = &result.batches[0];
    let cnt = batch
        .column_by_name("CNT")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 2, "should find 2 sensor nodes");
}

#[test]
fn merge_then_set_creates_and_updates() {
    let shared = selene_graph::SharedGraph::new(SeleneGraph::new());
    let result =
        MutationBuilder::new("MERGE (c:Config {key: 'test'}) SET c.value = 42 RETURN id(c) AS id")
            .execute(&shared)
            .unwrap();
    assert_eq!(result.row_count(), 1);

    // Verify the property was set
    let snap = shared.load_snapshot();
    let node = snap.get_node(NodeId(1)).expect("node should exist");
    assert_eq!(
        node.properties.get(IStr::new("value")),
        Some(&Value::Int(42))
    );
}

// ── Record constructor / map literal ──

#[test]
fn e2e_record_constructor_with_keyword() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN RECORD {name: s.name, temp: s.temp}",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.column_count(), 1);
}

#[test]
fn e2e_bare_map_literal() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN {name: s.name, temp: s.temp}", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
    assert_eq!(result.column_count(), 1);
}

#[test]
fn e2e_bare_map_literal_with_alias() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN {name: s.name} AS info", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
    let col_names: Vec<_> = result
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert!(
        col_names.iter().any(|c| c.eq_ignore_ascii_case("info")),
        "expected 'info' column, got: {col_names:?}"
    );
}
