//! UNION, OPTIONAL MATCH, CASE WHEN, WITH/HAVING/NULLS/UNWIND,
//! function library, and additional clause tests.

use super::*;

// ── UNION ──

#[test]
fn e2e_union_all() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (b:building) RETURN b.name AS name UNION ALL MATCH (f:floor) RETURN f.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2); // 1 building + 1 floor
}

#[test]
fn e2e_otherwise_fallback() {
    let g = setup_graph();
    // First query finds nothing, OTHERWISE uses second
    let result = QueryBuilder::new(
            "MATCH (x:nonexistent) RETURN x.name AS name OTHERWISE MATCH (b:building) RETURN b.name AS name",
            &g,
            ).execute().unwrap();
    assert_eq!(result.row_count(), 1); // building from second query
}

// ── OPTIONAL MATCH ──

#[test]
fn e2e_optional_match_with_results() {
    let g = setup_graph();
    // Building has floors --OPTIONAL MATCH should return them
    let result = QueryBuilder::new(
            "MATCH (b:building) OPTIONAL MATCH (b)-[:contains]->(f:floor) RETURN b.name AS building, f.name AS floor",
            &g,
            ).execute().unwrap();
    assert_eq!(result.row_count(), 1); // HQ -> Floor-1
}

#[test]
fn e2e_optional_match_no_results_gives_null() {
    let g = setup_graph();
    // Sensors don't have outgoing :manages edges --OPTIONAL should give NULL
    let result = QueryBuilder::new(
            "MATCH (s:sensor) OPTIONAL MATCH (s)-[:manages]->(x) RETURN s.name AS sensor, x.name AS managed",
            &g,
            ).execute().unwrap();
    assert_eq!(result.row_count(), 2); // 2 sensors, each with NULL x
}

// Bug 1 regression: inline property filters on OPTIONAL MATCH target node.
#[test]
fn e2e_optional_match_inline_prop_filter_matches() {
    let g = setup_graph();
    // Floor-1 exists -- inline prop filter should find it
    let result = QueryBuilder::new(
        "MATCH (b:building) OPTIONAL MATCH (b)-[:contains]->(f:floor {name: 'Floor-1'}) \
         RETURN b.name AS building, f.name AS floor",
        &g,
    )
    .execute()
    .unwrap();
    // Building matches, Floor-1 is found -- one row with real floor name
    assert_eq!(result.row_count(), 1);
    let batch = &result.batches[0];
    let floor_col = batch
        .column_by_name("floor")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(floor_col.value(0), "Floor-1");
}

#[test]
fn e2e_optional_match_inline_prop_filter_no_match_gives_null() {
    let g = setup_graph();
    // No floor named 'Level-99' -- inline prop filter should produce NULL
    let result = QueryBuilder::new(
        "MATCH (b:building) OPTIONAL MATCH (b)-[:contains]->(f:floor {name: 'Level-99'}) \
         RETURN b.name AS building, f.name AS floor",
        &g,
    )
    .execute()
    .unwrap();
    // Outer building row survives with NULL floor (left-join semantics)
    assert_eq!(result.row_count(), 1);
    let batch = &result.batches[0];
    let building_col = batch
        .column_by_name("building")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(building_col.value(0), "HQ");
    let floor_col = batch.column_by_name("floor").unwrap();
    assert!(
        floor_col.is_null(0),
        "floor column should be NULL when inline filter matches nothing"
    );
}

// Bug 2 regression: OPTIONAL MATCH WHERE clause drops outer row when no match exists.
#[test]
fn e2e_optional_match_where_filters_inner_match_passes() {
    let g = setup_graph();
    // Floor-1 exists and WHERE f.name = 'Floor-1' passes -- one row with real floor name
    let result = QueryBuilder::new(
        "MATCH (b:building) OPTIONAL MATCH (b)-[:contains]->(f:floor) WHERE f.name = 'Floor-1' \
         RETURN b.name AS building, f.name AS floor",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let batch = &result.batches[0];
    let floor_col = batch
        .column_by_name("floor")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(floor_col.value(0), "Floor-1");
}

#[test]
fn e2e_optional_match_where_no_inner_match_gives_null_not_drop() {
    let g = setup_graph();
    // WHERE f.name = 'Level-99' matches nothing -- outer building row should survive
    // with NULL floor (left-join semantics). Before the fix, this row was dropped.
    let result = QueryBuilder::new(
        "MATCH (b:building) OPTIONAL MATCH (b)-[:contains]->(f:floor) WHERE f.name = 'Level-99' \
         RETURN b.name AS building, f.name AS floor",
        &g,
    )
    .execute()
    .unwrap();
    // Outer row must survive with NULL floor
    assert_eq!(
        result.row_count(),
        1,
        "outer row must not be dropped when optional inner WHERE matches nothing"
    );
    let batch = &result.batches[0];
    let building_col = batch
        .column_by_name("building")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(building_col.value(0), "HQ");
    let floor_col = batch.column_by_name("floor").unwrap();
    assert!(
        floor_col.is_null(0),
        "floor should be NULL when optional WHERE matches nothing"
    );
}

// ── CASE WHEN ──

#[test]
fn e2e_case_when_simple() {
    let g = setup_graph();
    let result = QueryBuilder::new(
            "MATCH (s:sensor) RETURN s.name AS name, CASE WHEN s.temp > 75 THEN 'hot' ELSE 'normal' END AS status",
            &g,
            ).execute().unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn e2e_case_when_no_match_returns_null() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN CASE WHEN s.temp > 100 THEN 'extreme' END AS status",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn e2e_case_when_multi_branch() {
    let g = setup_graph();
    let result = QueryBuilder::new(
            "MATCH (s:sensor) RETURN CASE WHEN s.temp > 75 THEN 'hot' WHEN s.temp > 70 THEN 'warm' ELSE 'cold' END AS status",
            &g,
            ).execute().unwrap();
    assert_eq!(result.row_count(), 2);
}

// ── WITH, HAVING, NULLS FIRST/LAST, UNWIND ──────────────────────

#[test]
fn e2e_with_clause_basic() {
    let g = setup_graph();
    // WITH projects and resets scope --only 'name' survives into RETURN
    let result = QueryBuilder::new("MATCH (s:sensor) WITH s.name AS name RETURN name", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn e2e_with_clause_aggregation() {
    let g = setup_graph();
    // WITH + count aggregation
    let result = QueryBuilder::new("MATCH (s:sensor) WITH count(*) AS total RETURN total", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_with_clause_where_filter() {
    let g = setup_graph();
    // WITH + WHERE filter after projection
    let result = QueryBuilder::new(
        "MATCH (s:sensor) WITH s.name AS name, s.temp AS temp WHERE temp > 73 RETURN name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_with_scope_reset() {
    let g = setup_graph();
    // After WITH, the original variable 's' should not be accessible
    // Only 'name' was projected, so RETURN s.temp should fail or be null
    let result = QueryBuilder::new("MATCH (s:sensor) WITH s.name AS name RETURN name", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn e2e_having_clause() {
    let g = setup_graph();
    // Insert some nodes for grouping
    let mut g2 = g;
    {
        let mut m = g2.mutate();
        m.create_node(
            selene_core::LabelSet::from_strs(&["item"]),
            selene_core::PropertyMap::from_pairs(vec![
                (selene_core::IStr::new("cat"), selene_core::Value::str("A")),
                (selene_core::IStr::new("val"), selene_core::Value::Int(10)),
            ]),
        )
        .unwrap();
        m.create_node(
            selene_core::LabelSet::from_strs(&["item"]),
            selene_core::PropertyMap::from_pairs(vec![
                (selene_core::IStr::new("cat"), selene_core::Value::str("A")),
                (selene_core::IStr::new("val"), selene_core::Value::Int(20)),
            ]),
        )
        .unwrap();
        m.create_node(
            selene_core::LabelSet::from_strs(&["item"]),
            selene_core::PropertyMap::from_pairs(vec![
                (selene_core::IStr::new("cat"), selene_core::Value::str("B")),
                (selene_core::IStr::new("val"), selene_core::Value::Int(5)),
            ]),
        )
        .unwrap();
        m.commit(0).unwrap();
    }

    // GROUP BY i, HAVING sum > 10 -> only cat A (sum=30) passes
    let result = QueryBuilder::new(
        "MATCH (i:item) RETURN i.cat AS cat, sum(i.val) AS total GROUP BY i HAVING total > 10",
        &g2,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_nulls_first_last() {
    let g = setup_graph();
    // Insert a node without temp property
    let mut g2 = g;
    {
        let mut m = g2.mutate();
        m.create_node(
            selene_core::LabelSet::from_strs(&["sensor"]),
            selene_core::PropertyMap::from_pairs(vec![(
                selene_core::IStr::new("name"),
                selene_core::Value::str("S3"),
            )]),
        )
        .unwrap();
        m.commit(0).unwrap();
    }

    // NULLS FIRST: null temp sorts first
    let result = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.name AS name, s.temp AS temp ORDER BY temp ASC NULLS FIRST",
        &g2,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 3);
    // NULLS LAST: null temp sorts last
    let result2 = QueryBuilder::new(
        "MATCH (s:sensor) RETURN s.name AS name, s.temp AS temp ORDER BY temp ASC NULLS LAST",
        &g2,
    )
    .execute()
    .unwrap();
    assert_eq!(result2.row_count(), 3);
}

#[test]
fn e2e_unwind_keyword() {
    let g = setup_graph();
    let result = QueryBuilder::new("UNWIND [1, 2, 3] AS x RETURN x", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 3);
}

// UNWIND as a full pipeline clause: must compose with MATCH upstream and downstream,
// consume parameter lists, and handle empty/null/non-list edge cases.

#[test]
fn e2e_unwind_after_match_multiplies_rows() {
    let g = setup_graph();
    // 2 sensors × 3 tags = 6 rows
    let result = QueryBuilder::new(
        "MATCH (s:sensor) UNWIND ['a', 'b', 'c'] AS tag RETURN s.name AS name, tag",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 6);
}

#[test]
fn e2e_unwind_before_match_binds_downstream() {
    let g = setup_graph();
    // Unwind a list of names, then MATCH sensors whose name matches.
    // This proves UNWIND-produced bindings are visible to a later MATCH's WHERE.
    let result = QueryBuilder::new(
        "UNWIND ['TempSensor-1', 'TempSensor-2'] AS needle \
         MATCH (s:sensor) WHERE s.name = needle \
         RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn e2e_unwind_with_parameter_list() {
    let g = setup_graph();
    let mut params = ParameterMap::new();
    let list = crate::types::value::GqlList {
        element_type: crate::types::value::GqlType::Int,
        elements: std::sync::Arc::from(vec![
            crate::types::value::GqlValue::Int(1),
            crate::types::value::GqlValue::Int(2),
            crate::types::value::GqlValue::Int(3),
        ]),
    };
    params.insert(
        selene_core::IStr::new("items"),
        crate::types::value::GqlValue::List(list),
    );
    let result = QueryBuilder::new("UNWIND $items AS x RETURN x", &g)
        .with_parameters(&params)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 3);
}

#[test]
fn e2e_unwind_empty_list_yields_zero_rows() {
    let g = setup_graph();
    let result = QueryBuilder::new("UNWIND [] AS x RETURN x", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn e2e_unwind_null_yields_zero_rows() {
    let g = setup_graph();
    // UNWIND NULL is defined to produce no rows (matches ISO GQL / openCypher).
    let result = QueryBuilder::new("UNWIND null AS x RETURN x", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn e2e_unwind_non_list_errors() {
    let g = setup_graph();
    let err = QueryBuilder::new("UNWIND 42 AS x RETURN x", &g)
        .execute()
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("list") || err.contains("LIST"),
        "error should mention list typing: {err}"
    );
}

#[test]
fn e2e_unwind_chained() {
    let g = setup_graph();
    // 2 outer × 3 inner = 6 rows
    let result = QueryBuilder::new(
        "UNWIND [1, 2] AS a UNWIND ['x', 'y', 'z'] AS b RETURN a, b",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 6);
}

#[test]
fn e2e_unwind_cached_repeat_is_stable() {
    let g = setup_graph();
    // Execute the same UNWIND query twice: second call should hit the plan cache
    // and still produce the same binding shape. If cache accidentally stored
    // scope state or bindings, the second call would diverge.
    let q = "UNWIND [10, 20, 30] AS v RETURN v";
    let first = QueryBuilder::new(q, &g).execute().unwrap();
    let second = QueryBuilder::new(q, &g).execute().unwrap();
    assert_eq!(first.row_count(), 3);
    assert_eq!(second.row_count(), 3);
}

#[test]
fn e2e_unwind_with_filter_and_return_order() {
    let g = setup_graph();
    // Filter on the unwound variable, then return ordered by it.
    let result = QueryBuilder::new(
        "UNWIND [3, 1, 4, 1, 5, 9, 2, 6] AS n \
         FILTER n > 2 \
         RETURN n ORDER BY n",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 5); // 3, 4, 5, 9, 6 → 5 rows
}

#[test]
fn e2e_collect_alias() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN collect(s.name) AS names", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_average_alias() {
    let g = setup_graph();
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN average(s.temp) AS avg_temp", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

// ── Function library tests ──────────────────────────────────────

#[test]
fn e2e_power_function() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN power(2, 10) AS val", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_trig_functions() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN sin(0) AS s, cos(0) AS c, pi() AS p", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_log_exp() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN log(exp(1)) AS val", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_nullif_function() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN nullif(1, 1) AS n1, nullif(1, 2) AS n2", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_left_right_functions() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN left('hello', 3) AS l, right('hello', 3) AS r", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_now_function() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN now() AS ts", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_current_date_function() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN current_date() AS d", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_list_contains() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN list_contains([1, 2, 3], 2) AS found", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_list_reverse() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN list_reverse([1, 2, 3]) AS rev", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_mod_function() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN mod(10, 3) AS remainder", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_length_alias() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN length('hello') AS len", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

// ── Additional tests ────────────────────────────────────────

#[test]
fn e2e_with_group_by_having() {
    let g = setup_graph();
    // WITH + GROUP BY + HAVING: count sensors, filter groups
    let result = QueryBuilder::new(
            "MATCH (s:sensor) WITH s.name AS name, s.temp AS temp WHERE temp > 0 RETURN count(*) AS cnt",
            &g,
            ).execute().unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_implicit_coercion_string_vs_int() {
    let g = setup_graph();
    // String '72' should coerce to Int for comparison
    let result = QueryBuilder::new(
        "RETURN CASE WHEN 72 = '72' THEN 'coerced' ELSE 'not_coerced' END AS result",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_log_of_zero_returns_null() {
    let g = setup_graph();
    let result = QueryBuilder::new("RETURN log(0) AS val", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_ddl_create_graph_parses() {
    let _g = setup_graph();
    // DDL can be parsed but execution requires the server ops layer
    let stmt = crate::parser::parse_statement("CREATE GRAPH analytics").unwrap();
    assert!(matches!(
        stmt,
        crate::ast::statement::GqlStatement::CreateGraph { .. }
    ));
}

#[test]
fn e2e_ddl_create_index_parses() {
    let stmt = crate::parser::parse_statement("CREATE INDEX idx_temp ON :sensor(temp)").unwrap();
    assert!(matches!(
        stmt,
        crate::ast::statement::GqlStatement::CreateIndex { .. }
    ));
}

#[test]
fn e2e_keyword_graph_as_property() {
    let g = setup_graph();
    // 'graph' should NOT be reserved in expression context (L2 fix)
    let result = QueryBuilder::new("MATCH (s:sensor) RETURN s.name AS graph", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
}

// ── IN-list on non-indexed property ──

#[test]
fn e2e_in_list_no_index_returns_results() {
    // No property index on temp; IN-list must fall through to runtime eval.
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) FILTER s.temp IN [72.5, 80.0] RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(
        result.row_count(),
        2,
        "IN-list on non-indexed property must return matching nodes"
    );
}

#[test]
fn e2e_in_list_no_index_partial_match() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) FILTER s.temp IN [72.5] RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "IN-list with single match should return one node"
    );
}

#[test]
fn e2e_in_list_no_index_no_match() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) FILTER s.temp IN [999.0] RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(
        result.row_count(),
        0,
        "IN-list with no matches should return zero rows"
    );
}

// ── WITH followed by MATCH ──

// Regression test: WITH passes bindings to a subsequent MATCH.
// Before the fix, the MATCH after WITH would run against the full graph
// and then WITH would strip variables that RETURN needed, causing
// "unbound variable" errors.
#[test]
fn e2e_with_then_match_basic() {
    // Graph: building -[contains]-> floor -[contains]-> sensor
    // WITH should pass floor to the second MATCH.
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (b:building) WITH b MATCH (b)-[:contains]->(f:floor) RETURN b.name AS bname, f.name AS fname",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1, "one building-floor pair");
}

#[test]
fn e2e_with_filter_then_match() {
    // WITH with WHERE should filter before the second MATCH expands.
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (f:floor) WITH f WHERE f.name = 'Floor-1' MATCH (f)-[:contains]->(s:sensor) RETURN s.name AS sname",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2, "two sensors under Floor-1");
}

#[test]
fn e2e_with_no_match_drops_row() {
    // If the second MATCH finds no results, the row from WITH should be dropped.
    let g = setup_graph();
    // Sensors don't have outgoing :contains edges, so the MATCH returns nothing.
    let result = QueryBuilder::new(
        "MATCH (s:sensor) WITH s MATCH (s)-[:contains]->(x) RETURN s.name AS sname",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 0, "sensors have no :contains edges");
}

#[test]
fn e2e_with_then_match_with_count() {
    // Matches the originally failing pattern: WITH then MATCH with aggregation.
    // Building -[contains]-> floor, count per building.
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (b:building) WITH b MATCH (b)-[:contains]->(f:floor) RETURN b.name AS bname, count(f) AS cnt GROUP BY b.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1, "one building with floors");
    // Extract the count value
    use arrow::array::{Array, Int64Array};
    let batch = &result.batches[0];
    let cnt_col = batch
        .column_by_name("cnt")
        .expect("cnt column should be present");
    let cnt_arr = cnt_col.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(cnt_arr.value(0), 1, "building has 1 floor");
}

// ── Parameterized LIMIT/OFFSET ──

#[test]
fn e2e_parameterized_limit() {
    let g = setup_graph();
    let mut params = ParameterMap::new();
    params.insert(IStr::new("n"), GqlValue::Int(2));
    let result = QueryBuilder::new("MATCH (x) RETURN x LIMIT $n", &g)
        .with_parameters(&params)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn e2e_parameterized_offset() {
    let g = setup_graph();
    let mut params = ParameterMap::new();
    params.insert(IStr::new("skip"), GqlValue::Int(2));
    let all = QueryBuilder::new("MATCH (x) RETURN x", &g)
        .execute()
        .unwrap()
        .row_count();
    let result = QueryBuilder::new("MATCH (x) RETURN x OFFSET $skip", &g)
        .with_parameters(&params)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), all - 2);
}

#[test]
fn e2e_parameterized_limit_negative_error() {
    let g = setup_graph();
    let mut params = ParameterMap::new();
    params.insert(IStr::new("n"), GqlValue::Int(-1));
    let result = QueryBuilder::new("MATCH (x) RETURN x LIMIT $n", &g)
        .with_parameters(&params)
        .execute();
    assert!(result.is_err(), "negative LIMIT should error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("non-negative"),
        "error should mention non-negative: {err}"
    );
}

#[test]
fn e2e_parameterized_limit_type_error() {
    let g = setup_graph();
    let mut params = ParameterMap::new();
    params.insert(IStr::new("n"), GqlValue::String(SmolStr::new("five")));
    let result = QueryBuilder::new("MATCH (x) RETURN x LIMIT $n", &g)
        .with_parameters(&params)
        .execute();
    assert!(result.is_err(), "string LIMIT should error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("integer"),
        "error should mention integer: {err}"
    );
}

#[test]
fn e2e_parameterized_limit_unbound_error() {
    let g = setup_graph();
    let params = ParameterMap::new();
    let result = QueryBuilder::new("MATCH (x) RETURN x LIMIT $n", &g)
        .with_parameters(&params)
        .execute();
    assert!(result.is_err(), "unbound LIMIT param should error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not bound"),
        "error should mention not bound: {err}"
    );
}

// ── List iteration: comprehension, quantifiers (ANY/ALL/NONE/SINGLE), REDUCE ──
//
// Driver-row pattern: each query prefixes with `MATCH (b:building)` from
// setup_graph(), which yields exactly one row. That single row drives the
// RETURN expression evaluation so we can assert on exactly one result.

#[test]
fn e2e_list_comprehension_projection() {
    let g = setup_graph();
    // [x IN [1,2,3] | x*x] → [1, 4, 9]
    let result = QueryBuilder::new(
        "MATCH (b:building) RETURN [x IN [1, 2, 3] | x * x] AS squares",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.column_count(), 1);
}

#[test]
fn e2e_list_comprehension_with_where() {
    let g = setup_graph();
    // Full form: `[x IN xs WHERE p | f(x)]`. Projection (`| x`) is required
    // in the current grammar; ISO §20.10 shorthand `[x IN xs WHERE p]` (where
    // an omitted projection defaults to the iteration variable) is not yet
    // supported.
    let result = QueryBuilder::new(
        "MATCH (b:building) RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x] AS big",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_list_comprehension_empty_source() {
    let g = setup_graph();
    // Empty source list → empty result list, not error.
    let result = QueryBuilder::new("MATCH (b:building) RETURN [x IN [] | x * 2] AS out", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_list_comprehension_null_source() {
    let g = setup_graph();
    // Null source propagates to null (LIST-typed null), matching UNWIND null.
    let result = QueryBuilder::new("MATCH (b:building) RETURN [x IN null | x * 2] AS out", &g)
        .execute()
        .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_quantifier_any_true() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (b:building) WHERE ANY(x IN [1, 2, 3] WHERE x > 2) RETURN b.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_quantifier_any_false_filters_row_out() {
    let g = setup_graph();
    let result = QueryBuilder::new(
        "MATCH (b:building) WHERE ANY(x IN [1, 2, 3] WHERE x > 99) RETURN b.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn e2e_quantifier_all_true_on_empty() {
    let g = setup_graph();
    // Vacuous truth: ALL over empty list → true. Row should pass the filter.
    let result = QueryBuilder::new(
        "MATCH (b:building) WHERE ALL(x IN [] WHERE x > 0) RETURN b.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_quantifier_any_false_on_empty() {
    let g = setup_graph();
    // Vacuous falsity: ANY over empty list → false. Row should be filtered out.
    let result = QueryBuilder::new(
        "MATCH (b:building) WHERE ANY(x IN [] WHERE x > 0) RETURN b.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn e2e_quantifier_none() {
    let g = setup_graph();
    // NONE(x IN [1,2,3] WHERE x > 99) → true (no element > 99).
    let result = QueryBuilder::new(
        "MATCH (b:building) WHERE NONE(x IN [1, 2, 3] WHERE x > 99) RETURN b.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_quantifier_single_exactly_one() {
    let g = setup_graph();
    // SINGLE(x IN [1,2,3,4,5] WHERE x = 3) → true (exactly one match).
    let result = QueryBuilder::new(
        "MATCH (b:building) WHERE SINGLE(x IN [1, 2, 3, 4, 5] WHERE x = 3) RETURN b.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn e2e_quantifier_single_multiple_matches_is_false() {
    let g = setup_graph();
    // SINGLE(...) with >1 match → false, row filtered out.
    let result = QueryBuilder::new(
        "MATCH (b:building) WHERE SINGLE(x IN [1, 2, 3, 4, 5] WHERE x > 2) RETURN b.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn e2e_reduce_sum() {
    let g = setup_graph();
    // REDUCE(acc = 0, x IN [1,2,3,4] | acc + x) → 10
    let result = QueryBuilder::new(
        "MATCH (b:building) RETURN REDUCE(s = 0, x IN [1, 2, 3, 4] | s + x) AS total",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);

    let batch = &result.batches[0];
    let total = batch
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("total should be Int64");
    assert_eq!(total.value(0), 10);
}

#[test]
fn e2e_reduce_empty_list_returns_init() {
    let g = setup_graph();
    // REDUCE over empty list returns the init value unchanged.
    let result = QueryBuilder::new(
        "MATCH (b:building) RETURN REDUCE(s = 42, x IN [] | s + x) AS out",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);

    let batch = &result.batches[0];
    let out = batch
        .column_by_name("out")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("out should be Int64");
    assert_eq!(out.value(0), 42);
}

#[test]
fn e2e_list_iteration_var_shadows_outer_scope() {
    let g = setup_graph();
    // The comprehension's `x` must not leak into the outer RETURN. After the
    // comprehension closes, `b.name` is still visible and `x` is unbound —
    // the clone-based scoping ensures the outer binding is unchanged.
    let result = QueryBuilder::new(
        "MATCH (b:building) \
         RETURN b.name AS name, [x IN [1, 2, 3] | x + 1] AS incremented",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.column_count(), 2);
}

#[test]
fn e2e_comprehension_non_list_source_errors_in_return() {
    let g = setup_graph();
    // RETURN propagates eval errors up (unlike WHERE, which silently filters).
    // Using comprehension (not quantifier) in RETURN surfaces the type error.
    let err = QueryBuilder::new("MATCH (b:building) RETURN [x IN 42 | x * 2] AS out", &g)
        .execute()
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("LIST") || err.contains("list"),
        "error should mention list typing: {err}"
    );
}
