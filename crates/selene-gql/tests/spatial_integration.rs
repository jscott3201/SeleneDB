//! End-to-end GQL tests for the ST_* spatial function family.
//!
//! Exercises the full pipeline: property ingest, MATCH/FILTER over
//! GEOMETRY columns, and ORDER BY on spatial measurements.

use arrow::array::{Array, Float64Array, StringArray};
use selene_core::{GeometryValue, LabelSet, PropertyMap, Value};
use selene_gql::QueryBuilder;
use selene_graph::SeleneGraph;

fn fixture() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();

    // Building at NYC city hall
    m.create_node(
        LabelSet::from_strs(&["building"]),
        PropertyMap::from_pairs(vec![
            ("name".into(), Value::str("HQ")),
            (
                "location".into(),
                Value::geometry(GeometryValue::point_wgs84(-74.0060, 40.7128)),
            ),
        ]),
    )
    .unwrap();

    // Sensor 1: ~1.5 km east-southeast, inside the Manhattan zone.
    m.create_node(
        LabelSet::from_strs(&["sensor"]),
        PropertyMap::from_pairs(vec![
            ("name".into(), Value::str("Sensor-S")),
            (
                "location".into(),
                Value::geometry(GeometryValue::point_wgs84(-73.9936, 40.7050)),
            ),
        ]),
    )
    .unwrap();

    // Sensor 2: ~40 km north (Yonkers-ish), outside the Manhattan zone
    m.create_node(
        LabelSet::from_strs(&["sensor"]),
        PropertyMap::from_pairs(vec![
            ("name".into(), Value::str("Sensor-N")),
            (
                "location".into(),
                Value::geometry(GeometryValue::point_wgs84(-73.8988, 40.9312)),
            ),
        ]),
    )
    .unwrap();

    // Zone polygon covering Manhattan roughly
    let zone_poly = GeometryValue::from_geojson(
        r#"{"type":"Polygon","coordinates":[
            [[-74.02,40.70],[-73.97,40.70],[-73.97,40.80],[-74.02,40.80],[-74.02,40.70]]
        ]}"#,
    )
    .unwrap();
    m.create_node(
        LabelSet::from_strs(&["zone"]),
        PropertyMap::from_pairs(vec![
            ("name".into(), Value::str("Manhattan")),
            ("boundary".into(), Value::geometry(zone_poly)),
        ]),
    )
    .unwrap();

    m.commit(0).unwrap();
    g
}

#[test]
fn st_distance_between_building_and_sensors() {
    let g = fixture();
    let result = QueryBuilder::new(
        "MATCH (b:building), (s:sensor) \
         RETURN s.name AS name, ST_Distance(b.location, s.location) AS d \
         ORDER BY d ASC",
        &g,
    )
    .execute()
    .unwrap();

    assert_eq!(result.row_count(), 2);
    let batch = &result.batches[0];
    let names = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let dists = batch
        .column_by_name("d")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(names.value(0), "Sensor-S", "closer sensor sorts first");
    assert!(dists.value(0) < dists.value(1));
}

#[test]
fn st_dwithin_filters_close_sensors() {
    let g = fixture();
    // Cross-binding predicates use WHERE (evaluated after the cartesian
    // assembly); FILTER is scoped per-pattern during MATCH in Selene's GQL.
    let result = QueryBuilder::new(
        "MATCH (b:building), (s:sensor) \
         WHERE ST_DWithin(b.location, s.location, 10000.0) \
         RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1, "only Sensor-S within 10 km");
}

#[test]
fn st_contains_zone_contains_sensor() {
    let g = fixture();
    let result = QueryBuilder::new(
        "MATCH (z:zone), (s:sensor) \
         WHERE ST_Contains(z.boundary, s.location) \
         RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(
        result.row_count(),
        1,
        "Manhattan polygon contains only Sensor-S"
    );
}

#[test]
fn st_geometrytype_classifies_values() {
    let g = fixture();
    let result = QueryBuilder::new(
        "MATCH (z:zone) RETURN z.name AS name, ST_GeometryType(z.boundary) AS kind",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let kinds = result.batches[0]
        .column_by_name("kind")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(kinds.value(0), "Polygon");
}

#[test]
fn single_binding_filter_on_st_y() {
    // FILTER is per-pattern; single-binding geometry predicates work fine.
    let g = fixture();
    let result = QueryBuilder::new(
        "MATCH (s:sensor) FILTER ST_Y(s.location) > 40.80 RETURN s.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1, "only Sensor-N has lat > 40.80");
    let names = result.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Sensor-N");
}

#[test]
fn st_x_st_y_extract_coords_from_property() {
    let g = fixture();
    let result = QueryBuilder::new(
        "MATCH (b:building) RETURN ST_X(b.location) AS lng, ST_Y(b.location) AS lat",
        &g,
    )
    .execute()
    .unwrap();
    let batch = &result.batches[0];
    let lng = batch
        .column_by_name("lng")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let lat = batch
        .column_by_name("lat")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((lng.value(0) + 74.0060).abs() < 1e-9);
    assert!((lat.value(0) - 40.7128).abs() < 1e-9);
}
