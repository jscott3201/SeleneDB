//! Property-based fuzzing for the GQL parser.
//!
//! Generates random strings mixing GQL keywords, identifiers, operators,
//! and literals. Verifies the parser never panics — always returns Ok or Err.
//! Run with: cargo test -p selene-gql --test fuzz_parser

use proptest::prelude::*;

/// GQL keywords for generating plausible queries.
const KEYWORDS: &[&str] = &[
    "MATCH", "RETURN", "WHERE", "FILTER", "ORDER", "BY", "LIMIT", "OFFSET", "INSERT", "DELETE",
    "SET", "REMOVE", "MERGE", "DETACH", "CREATE", "OPTIONAL", "CALL", "YIELD", "AS", "AND", "OR",
    "NOT", "XOR", "TRUE", "FALSE", "NULL", "IS", "IN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE",
    "END", "FOR", "UNWIND", "LET", "UNION", "DISTINCT", "ALL", "FINISH", "ASC", "DESC", "count",
    "sum", "avg", "min", "max", "SHORTEST", "ACYCLIC", "SIMPLE", "TRAIL",
];

const OPERATORS: &[&str] = &[
    "=", "<>", "<", ">", "<=", ">=", "+", "-", "*", "/", "%", "(", ")", "[", "]", "{", "}", ":",
    ",", ".", "->", "<-", "!", "&", "|", "$", "#", "@", "~",
];

const IDENTIFIERS: &[&str] = &[
    "n", "m", "e", "r", "s", "x", "y", "p", "node", "edge", "sensor", "building", "floor", "zone",
    "name", "temp", "value", "id", "label", "type", "count", "total",
];

fn arb_token() -> impl Strategy<Value = String> {
    prop_oneof![
        30 => prop::sample::select(KEYWORDS).prop_map(|s| s.to_string()),
        20 => prop::sample::select(OPERATORS).prop_map(|s| s.to_string()),
        20 => prop::sample::select(IDENTIFIERS).prop_map(|s| s.to_string()),
        10 => any::<i32>().prop_map(|n| n.to_string()),
        5 => "[a-z]{1,10}".prop_map(|s| format!("'{s}'")),
        5 => "\\$[a-z]{1,5}".prop_map(|s| s),
        5 => Just(" ".to_string()),
        5 => Just("\n".to_string()),
    ]
}

fn arb_query() -> impl Strategy<Value = String> {
    prop::collection::vec(arb_token(), 1..30).prop_map(|tokens| tokens.join(" "))
}

/// Generate somewhat-valid MATCH queries.
fn arb_match_query() -> impl Strategy<Value = String> {
    (
        prop::sample::select(IDENTIFIERS),
        prop::sample::select(IDENTIFIERS),
        prop::option::of(prop::sample::select(IDENTIFIERS)),
    )
        .prop_map(|(var, label, ret)| {
            let ret_clause = ret.map_or(format!("RETURN {var}"), |r| format!("RETURN {var}.{r}"));
            format!("MATCH ({var}:{label}) {ret_clause}")
        })
}

/// Generate INSERT queries.
fn arb_insert_query() -> impl Strategy<Value = String> {
    (prop::sample::select(IDENTIFIERS), any::<i32>())
        .prop_map(|(label, val)| format!("INSERT (:{label} {{value: {val}}})"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10000))]

    /// Random token sequences never cause a panic in the parser.
    #[test]
    fn parser_never_panics(query in arb_query()) {
        // The parser should return Ok or Err, never panic
        let _ = selene_gql::parse_statement(&query);
    }

    /// Semi-valid MATCH queries parse without panicking.
    #[test]
    fn match_queries_never_panic(query in arb_match_query()) {
        let _ = selene_gql::parse_statement(&query);
    }

    /// INSERT queries parse without panicking.
    #[test]
    fn insert_queries_never_panic(query in arb_insert_query()) {
        let _ = selene_gql::parse_statement(&query);
    }

    /// Empty and whitespace-only strings never panic.
    #[test]
    fn whitespace_never_panics(s in "\\s{0,100}") {
        let _ = selene_gql::parse_statement(&s);
    }

    /// Arbitrary UTF-8 strings never panic.
    #[test]
    fn arbitrary_utf8_never_panics(s in ".*") {
        let _ = selene_gql::parse_statement(&s);
    }
}
