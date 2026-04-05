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
