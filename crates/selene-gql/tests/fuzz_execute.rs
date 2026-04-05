//! Property-based fuzzing for GQL execution.
//!
//! Generates random GQL queries and executes them against the reference building.
//! Verifies the executor never panics — always returns GqlResult or GqlError.
//! Run with: cargo test -p selene-gql --test fuzz_execute

use proptest::prelude::*;
use selene_gql::QueryBuilder;
use selene_testing::reference_building;

const LABELS: &[&str] = &[
    "sensor",
    "building",
    "floor",
    "zone",
    "ahu",
    "vav",
    "equipment",
    "campus",
    "alarm",
    "server",
    "temperature_sensor",
    "humidity_sensor",
    "co2_sensor",
    "breaker",
    "circuit",
    "electrical_panel",
];

const EDGE_TYPES: &[&str] = &[
    "contains",
    "feeds",
    "serves",
    "monitors",
    "returns_to",
    "isPointOf",
    "adjacent_to",
    "powers",
    "alarm_for",
];

const PROPS: &[&str] = &[
    "name",
    "unit",
    "accuracy",
    "value",
    "status",
    "supply_cfm",
    "max_cfm",
    "area_sqft",
    "level",
    "floors",
    "capacity",
];

/// Generate MATCH queries with valid structure but random labels/properties.
fn arb_valid_match() -> impl Strategy<Value = String> {
    (
        prop::sample::select(LABELS),
        prop::sample::select(PROPS),
        prop::option::of(0..20i64),
    )
        .prop_map(|(label, prop, limit)| {
            let limit_clause = limit.map_or(String::new(), |l| format!(" LIMIT {l}"));
            format!("MATCH (n:{label}) RETURN n.{prop} AS val{limit_clause}")
        })
}

/// Generate two-hop traversal queries.
fn arb_traversal() -> impl Strategy<Value = String> {
    (
        prop::sample::select(LABELS),
        prop::sample::select(EDGE_TYPES),
        prop::sample::select(LABELS),
    )
        .prop_map(|(l1, edge, l2)| {
            format!("MATCH (a:{l1})-[:{edge}]->(b:{l2}) RETURN a.name AS from, b.name AS to")
        })
}

/// Generate aggregation queries.
fn arb_aggregation() -> impl Strategy<Value = String> {
    (
        prop::sample::select(LABELS),
        prop::sample::select(PROPS),
        prop::sample::select(&["count", "sum", "avg", "min", "max"][..]),
    )
        .prop_map(|(label, prop, agg)| {
            if agg == "count" {
                format!("MATCH (n:{label}) RETURN count(*) AS total")
            } else {
                format!("MATCH (n:{label}) RETURN {agg}(n.{prop}) AS result")
            }
        })
}

/// Generate filter queries with comparison operators.
fn arb_filter() -> impl Strategy<Value = String> {
    (
        prop::sample::select(LABELS),
        prop::sample::select(PROPS),
        any::<i32>(),
        prop::sample::select(&["=", "<>", "<", ">", "<=", ">="][..]),
    )
        .prop_map(|(label, prop, val, op)| {
            format!("MATCH (n:{label}) FILTER n.{prop} {op} {val} RETURN n.name AS name")
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5000))]

    /// Valid MATCH queries execute without panicking.
    #[test]
    fn match_execute_never_panics(query in arb_valid_match()) {
        let g = reference_building::reference_building(1);
        let _ = QueryBuilder::new(&query, &g).execute();
    }

    /// Traversal queries execute without panicking.
    #[test]
    fn traversal_execute_never_panics(query in arb_traversal()) {
        let g = reference_building::reference_building(1);
        let _ = QueryBuilder::new(&query, &g).execute();
    }

    /// Aggregation queries execute without panicking.
    #[test]
    fn aggregation_execute_never_panics(query in arb_aggregation()) {
        let g = reference_building::reference_building(1);
        let _ = QueryBuilder::new(&query, &g).execute();
    }

    /// Filter queries execute without panicking.
    #[test]
    fn filter_execute_never_panics(query in arb_filter()) {
        let g = reference_building::reference_building(1);
        let _ = QueryBuilder::new(&query, &g).execute();
    }
}
