//! Grammar coverage tests for the GQL parser.
//!
//! This suite exercises corners of the grammar that the audit identified as
//! under-tested: string escapes, number formats, CASE expressions, keyword
//! vs identifier edge cases, parameter references, YIELD variations, mutation
//! composition, and list/map literal edge cases.
//!
//! Each test category uses `accepts(&[...])` or `rejects(&[...])` to assert
//! whether the parser produces a typed AST (`parse_statement` returns Ok/Err).
//! When a test expectation differs from current parser behavior, the test
//! should fail loudly — that's the point.

use selene_gql::parse_statement;

fn accepts(label: &str, inputs: &[&str]) {
    let mut failures = Vec::new();
    for input in inputs {
        if let Err(e) = parse_statement(input) {
            failures.push(format!("  REJECTED  {input:?}\n    error: {e}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{label}: {} of {} inputs that should parse did not:\n{}",
        failures.len(),
        inputs.len(),
        failures.join("\n")
    );
}

fn rejects(label: &str, inputs: &[&str]) {
    let mut failures = Vec::new();
    for input in inputs {
        if parse_statement(input).is_ok() {
            failures.push(format!("  ACCEPTED  {input:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{label}: {} of {} inputs that should have been rejected were accepted:\n{}",
        failures.len(),
        inputs.len(),
        failures.join("\n")
    );
}

// ──────────────────────────────────────────────────────────────────────
// Phase A — String escape sequences
// Grammar supports \n \r \t \\ \' \" \` \b \f \uXXXX \UXXXXXXXX plus ''
// ──────────────────────────────────────────────────────────────────────

#[test]
fn string_escape_newline() {
    accepts("\\n escape", &[r"RETURN 'line1\nline2' AS s"]);
}

#[test]
fn string_escape_tab_and_return() {
    accepts(
        "\\t and \\r escapes",
        &[r"RETURN 'a\tb\rc' AS s"],
    );
}

#[test]
fn string_escape_backslash_and_quotes() {
    accepts(
        "escaped backslash and quotes",
        &[
            r"RETURN '\\' AS s",
            r"RETURN '\'' AS s",
            r#"RETURN '\"' AS s"#,
            r"RETURN '\`' AS s",
        ],
    );
}

#[test]
fn string_escape_double_single_quote() {
    // spec §21.1: `''` inside a single-quoted string is a literal `'`
    accepts(
        "doubled single-quote escape",
        &[r"RETURN 'it''s a test' AS s"],
    );
}

#[test]
fn string_escape_backspace_and_formfeed() {
    // \b is backspace (ASCII 0x08), \f is form feed (0x0C).
    accepts("\\b backspace and \\f form feed escapes", &[r"RETURN '\b\f' AS s"]);
}

#[test]
fn string_escape_unicode_4hex() {
    accepts(
        "\\uXXXX unicode escape",
        &[r"RETURN '\u00e9' AS e", r"RETURN '\u2603' AS snowman"],
    );
}

#[test]
fn string_escape_unicode_8hex() {
    accepts(
        "\\UXXXXXXXX unicode escape",
        &[r"RETURN '\U0001F600' AS grin"],
    );
}

#[test]
fn string_escape_invalid_sequences_rejected() {
    rejects(
        "invalid escape sequences",
        &[
            r"RETURN '\z' AS s",   // \z not defined
            r"RETURN '\u123' AS s", // \u needs exactly 4 hex
            r"RETURN '\U1234' AS s", // \U needs exactly 8 hex
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Phase A — Number formats
// Grammar supports int, uint (trailing u), float (x.x[eN][f|d]),
// hex (0x), oct (0o), bin (0b), all with optional +/- sign
// and underscore digit separators.
// ──────────────────────────────────────────────────────────────────────

#[test]
fn number_plain_int() {
    accepts(
        "plain ints",
        &["RETURN 0 AS n", "RETURN 42 AS n", "RETURN -17 AS n", "RETURN +5 AS n"],
    );
}

#[test]
fn number_int_with_underscores() {
    accepts(
        "int with underscore separators",
        &["RETURN 1_000_000 AS n", "RETURN -1_234_567 AS n"],
    );
}

#[test]
fn number_uint_suffix() {
    accepts(
        "unsigned int with u suffix",
        &["RETURN 42u AS n", "RETURN 1_000u AS n"],
    );
}

#[test]
fn number_float_basic() {
    accepts(
        "plain floats",
        &["RETURN 3.14 AS n", "RETURN -2.5 AS n", "RETURN 0.1 AS n"],
    );
}

#[test]
fn number_float_scientific() {
    accepts(
        "scientific notation",
        &[
            "RETURN 1.5e10 AS n",
            "RETURN 2.0E-3 AS n",
            "RETURN -1.23e+4 AS n",
        ],
    );
}

#[test]
fn number_float_f_or_d_suffix() {
    accepts(
        "float with f or d suffix",
        &["RETURN 1.5f AS n", "RETURN 2.5d AS n"],
    );
}

#[test]
fn number_hex_literal() {
    accepts(
        "hex literals",
        &[
            "RETURN 0xFF AS n",
            "RETURN 0X1A AS n",
            "RETURN -0xFF AS n",
            "RETURN 0xDEAD_BEEF AS n",
        ],
    );
}

#[test]
fn number_octal_literal() {
    accepts(
        "octal literals",
        &["RETURN 0o17 AS n", "RETURN 0O755 AS n", "RETURN 0o7_7 AS n"],
    );
}

#[test]
fn number_binary_literal() {
    accepts(
        "binary literals",
        &["RETURN 0b1010 AS n", "RETURN 0B1111_0000 AS n"],
    );
}

#[test]
fn number_bare_dot_rejected() {
    // Grammar requires digits on both sides of the decimal point.
    rejects(
        "floats with missing side of decimal",
        &["RETURN 5. AS n", "RETURN .5 AS n"],
    );
}

#[test]
fn number_leading_zero_ambiguity() {
    // 0123 — is this int 123 with leading zero? grammar does not require a hex/oct/bin prefix.
    // int_lit = @{ sign? ~ ASCII_DIGIT ~ (ASCII_DIGIT | "_")* } so 0123 parses as 123.
    accepts("leading-zero integer", &["RETURN 0123 AS n", "RETURN 0 AS n"]);
}

// ──────────────────────────────────────────────────────────────────────
// Phase B — CASE expressions
// ──────────────────────────────────────────────────────────────────────

#[test]
fn case_simple() {
    accepts(
        "simple CASE",
        &[
            "RETURN CASE 1 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END AS r",
            "RETURN CASE 1 WHEN 1 THEN 'a' ELSE 'other' END AS r",
        ],
    );
}

#[test]
fn case_searched() {
    accepts(
        "searched CASE",
        &[
            "MATCH (n:sensor) RETURN CASE WHEN n.temp > 70 THEN 'hot' WHEN n.temp < 30 THEN 'cold' ELSE 'ok' END AS cat",
        ],
    );
}

#[test]
fn case_nested() {
    accepts(
        "nested CASE in THEN branch",
        &[
            "MATCH (n:sensor) RETURN CASE WHEN n.temp > 70 THEN CASE WHEN n.temp > 90 THEN 'critical' ELSE 'warm' END ELSE 'cool' END AS cat",
        ],
    );
}

#[test]
fn case_in_aggregate() {
    accepts(
        "CASE inside an aggregate",
        &[
            "MATCH (n:sensor) RETURN count(CASE WHEN n.temp > 70 THEN 1 END) AS hot_count",
        ],
    );
}

#[test]
fn case_requires_when() {
    rejects(
        "CASE without any WHEN clause",
        &[
            "RETURN CASE END AS r",
            "RETURN CASE 1 END AS r",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Phase C — Keyword vs identifier edge cases
// ──────────────────────────────────────────────────────────────────────

#[test]
fn reserved_keywords_rejected_as_variables() {
    rejects(
        "reserved keywords as bare variable names",
        &[
            "MATCH (MATCH) RETURN MATCH",
            "MATCH (where) RETURN where",
            "MATCH (SELECT) RETURN SELECT",
        ],
    );
}

#[test]
fn keywords_ok_as_property_names() {
    // prop_ident is keyword-unrestricted.
    accepts(
        "keywords as property names",
        &[
            "MATCH (n:sensor) RETURN n.date AS d",
            "MATCH (n:sensor) RETURN n.type AS t",
            "INSERT (:sensor {date: 'today', type: 'temp', order: 1})",
        ],
    );
}

#[test]
fn delimited_identifiers_allow_keywords() {
    accepts(
        "double-quote and backtick delimited identifiers",
        &[
            "MATCH (`match`:sensor) RETURN `match`.name AS n",
            "MATCH (\"return\":sensor) RETURN \"return\".name AS n",
        ],
    );
}

#[test]
fn keyword_prefix_not_eager() {
    // The `ORDER` keyword must not greedily match `ORDERS` used as a label.
    accepts(
        "keywords do not prefix-match into identifiers",
        &[
            "MATCH (o:ORDERS) RETURN o.name AS n",
            "MATCH (n:matches) RETURN n.name AS m",
        ],
    );
}

#[test]
fn case_insensitive_keywords() {
    accepts(
        "keywords are case-insensitive",
        &[
            "match (n:sensor) return n.name AS name",
            "Match (n:sensor) Return n.name AS name",
            "mAtCh (n:sensor) ReTuRn n.name AS name",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Phase D — Parameter references
// ──────────────────────────────────────────────────────────────────────

#[test]
fn parameter_basic() {
    accepts(
        "basic parameter references",
        &[
            "MATCH (n:sensor) FILTER n.temp > $threshold RETURN n.name AS n",
            "MATCH (n:sensor) RETURN n LIMIT $k",
        ],
    );
}

#[test]
fn parameter_underscore_names() {
    accepts(
        "parameter names with underscores",
        &[
            "RETURN $_my_param AS x",
            "RETURN $my_param_1 AS x",
            "RETURN $_ AS x",
        ],
    );
}

#[test]
fn parameter_starting_digit_rejected() {
    rejects(
        "parameter names cannot start with a digit",
        &["RETURN $1 AS x", "RETURN $123 AS x"],
    );
}

#[test]
fn parameter_empty_rejected() {
    rejects(
        "empty parameter name",
        &["RETURN $ AS x"],
    );
}

#[test]
fn parameter_with_keyword_name() {
    // Parameter names are independent of the keyword list. `$match` / `$where`
    // should parse — they're bound names, not reserved words.
    accepts(
        "parameter names may coincide with keyword text",
        &["RETURN $match AS x", "RETURN $where AS x"],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Phase E — YIELD variations
// ──────────────────────────────────────────────────────────────────────

#[test]
fn yield_basic() {
    // YIELD names become variables and must not be reserved keywords.
    // Real procedure columns that would clash (e.g. `count`) must be aliased
    // with AS — matches the parser's hint.
    accepts(
        "simple YIELD with non-keyword names",
        &[
            "CALL graph.labels() YIELD label RETURN label",
            "CALL graph.vectorSearch('sensor', 'embedding', $qvec, 10) YIELD nodeId, score RETURN nodeId, score",
        ],
    );
}

#[test]
fn yield_reserved_keyword_name_rejected() {
    // The parser rejects reserved keywords as bare YIELD names and hints
    // that the caller should use backticks. This test locks in that behavior.
    rejects(
        "YIELD of a reserved keyword like `count` without escaping",
        &[
            "CALL graph.nodeCount() YIELD count RETURN count",
        ],
    );
}

#[test]
fn yield_keyword_with_backticks() {
    accepts(
        "YIELD of a reserved keyword name escaped with backticks",
        &[
            "CALL graph.nodeCount() YIELD `count` RETURN `count`",
        ],
    );
}

#[test]
fn yield_star() {
    accepts(
        "YIELD *",
        &["CALL graph.nodeCount() YIELD * RETURN *"],
    );
}

#[test]
fn yield_with_alias() {
    accepts(
        "YIELD with AS alias",
        &[
            "CALL graph.nodeCount() YIELD count AS total RETURN total",
        ],
    );
}

#[test]
fn call_with_args_and_yield() {
    accepts(
        "CALL with positional args + YIELD",
        &[
            "CALL graph.vectorSearch('sensor', 'embedding', $qvec, 10) YIELD nodeId, score RETURN nodeId, score",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Phase F — Mutation composition
// ──────────────────────────────────────────────────────────────────────

#[test]
fn merge_with_on_create_and_on_match() {
    accepts(
        "MERGE with both ON CREATE and ON MATCH",
        &[
            "MERGE (n:sensor {id: 1}) ON CREATE SET n.created = true ON MATCH SET n.updated = true",
            "MERGE (n:sensor {id: 1}) ON MATCH SET n.updated = true ON CREATE SET n.created = true",
        ],
    );
}

#[test]
fn merge_plain() {
    accepts(
        "MERGE without ON CREATE/ON MATCH",
        &["MERGE (n:sensor {id: 1})"],
    );
}

#[test]
fn merge_rejects_duplicate_clauses() {
    // The grammar accepts at most one ON CREATE and at most one ON MATCH.
    // Duplicating either clause must fail at parse time, not silently
    // drop the second. Locks in the tighter rule shape.
    rejects(
        "MERGE with duplicated ON CREATE or ON MATCH",
        &[
            "MERGE (n:sensor {id: 1}) ON CREATE SET n.a = 1 ON CREATE SET n.b = 2",
            "MERGE (n:sensor {id: 1}) ON MATCH SET n.a = 1 ON MATCH SET n.b = 2",
        ],
    );
}

#[test]
fn insert_with_labels_and_properties() {
    accepts(
        "INSERT of various shapes",
        &[
            "INSERT (:sensor {name: 'S1'})",
            // GQL-standard multi-label syntax uses `&` (not Cypher's `:`).
            "INSERT (:sensor&temperature {name: 'S1', unit: 'C'})",
            "INSERT (:building {name: 'HQ'})-[:contains]->(:floor {name: 'F1'})",
        ],
    );
}

#[test]
fn multi_label_requires_gql_ampersand_not_cypher_colon() {
    // Document that Cypher-style `:A:B` is NOT the accepted form.
    // If this test starts failing it means someone added Cypher-compat
    // semantics and the behavior change needs to be intentional.
    rejects(
        "Cypher-style `:A:B` multi-label is not supported",
        &[
            "MATCH (n:sensor:temperature) RETURN n.name AS x",
            "INSERT (:sensor:temperature {name: 'S1'})",
        ],
    );
}

#[test]
fn detach_delete() {
    accepts(
        "DETACH DELETE single and multi-target",
        &[
            "MATCH (n:sensor) DETACH DELETE n",
            "MATCH (a:sensor), (b:zone) DETACH DELETE a, b",
        ],
    );
}

#[test]
fn set_and_remove_multiple() {
    accepts(
        "SET and REMOVE multiple properties",
        &[
            "MATCH (n:sensor) SET n.temp = 72, n.updated = true",
            "MATCH (n:sensor) REMOVE n.temp, n.updated",
            "MATCH (n:sensor) SET n.temp = 72 REMOVE n.stale",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Phase G — List and map literal edge cases
// ──────────────────────────────────────────────────────────────────────

#[test]
fn list_literals_various() {
    accepts(
        "various list literals",
        &[
            "RETURN [] AS empty",
            "RETURN [1] AS single",
            "RETURN [1, 2, 3] AS nums",
            "RETURN [1, 'two', true, null] AS mixed",
            "RETURN [[1, 2], [3, 4]] AS nested",
        ],
    );
}

#[test]
fn list_trailing_comma_rejected() {
    rejects(
        "list literal cannot have a trailing comma",
        &["RETURN [1, 2, 3,] AS nums"],
    );
}

#[test]
fn map_literal_basic() {
    accepts(
        "record/map literals",
        &[
            "RETURN {name: 'foo', id: 1} AS r",
            "RETURN {a: {b: {c: 1}}} AS nested",
            "INSERT (:sensor {name: 'S1', loc: {lat: 0.0, lon: 0.0}})",
        ],
    );
}

#[test]
fn map_empty_literal() {
    // record_constructor requires at least one field per grammar:
    // record_constructor = { ^"RECORD"? ~ "{" ~ record_field ~ ("," ~ record_field)* ~ "}" }
    // So empty {} should NOT parse as a map.
    rejects(
        "empty map literal",
        &["RETURN {} AS empty"],
    );
}

#[test]
fn map_trailing_comma_rejected() {
    rejects(
        "map literal cannot have a trailing comma",
        &["RETURN {a: 1, b: 2,} AS r"],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Comments
// ──────────────────────────────────────────────────────────────────────

#[test]
fn comments_variants() {
    accepts(
        "line and block comments",
        &[
            "// leading line comment\nMATCH (n) RETURN n.name AS name",
            "-- SQL-style line comment\nMATCH (n) RETURN n.name AS name",
            "/* block comment */ MATCH (n) RETURN n.name AS name",
            "MATCH (n) /* inline */ RETURN n.name AS name",
            "MATCH (n) RETURN n.name AS name // trailing",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Quantifiers on variable-length paths
// ──────────────────────────────────────────────────────────────────────

#[test]
fn quantifier_bounded() {
    accepts(
        "bounded variable-length path",
        &[
            "MATCH (a)-[:contains]->{1,3}(b) RETURN a.name, b.name",
            "MATCH (a)-[:contains]->{2}(b) RETURN a.name, b.name",
        ],
    );
}

#[test]
fn quantifier_zero_is_allowed() {
    // {0,N} is a valid quantifier — matches zero hops (a === b).
    accepts(
        "quantifier with zero min",
        &["MATCH (a)-[:contains]->{0,3}(b) RETURN a.name, b.name"],
    );
}

#[test]
fn quantifier_inverted_rejected() {
    rejects(
        "quantifier with min > max is a parse/validation error",
        &["MATCH (a)-[:contains]->{5,2}(b) RETURN a.name, b.name"],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Expressions — IS suffix
// ──────────────────────────────────────────────────────────────────────

#[test]
fn is_null_variants() {
    accepts(
        "IS NULL and IS NOT NULL",
        &[
            "MATCH (n:sensor) FILTER n.alert IS NULL RETURN n.name AS n",
            "MATCH (n:sensor) FILTER n.alert IS NOT NULL RETURN n.name AS n",
        ],
    );
}

#[test]
fn is_typed_suffix() {
    // GQL spec §8.1.2: type predicates use `IS TYPED <type>` form, not
    // bare `IS <type>`. Document that the TYPED keyword is required.
    accepts(
        "IS TYPED <type> form",
        &[
            "RETURN 1 IS TYPED INTEGER AS b",
            "RETURN 'x' IS TYPED STRING AS b",
            "RETURN 1 IS NOT TYPED STRING AS b",
        ],
    );
    rejects(
        "bare IS <type> without TYPED keyword is not GQL",
        &[
            "RETURN 1 IS INTEGER AS b",
            "RETURN 'x' IS STRING AS b",
        ],
    );
}

#[test]
fn is_labeled_suffix() {
    accepts(
        "IS LABELED / IS NOT LABELED",
        &[
            "MATCH (n) FILTER n IS LABELED :sensor RETURN n.name AS x",
            "MATCH (n) FILTER n IS NOT LABELED :sensor RETURN n.name AS x",
        ],
    );
}

#[test]
fn is_source_destination_of() {
    accepts(
        "IS SOURCE OF / IS DESTINATION OF",
        &[
            "MATCH (n)-[e]->(m) FILTER n IS SOURCE OF e RETURN n.name AS x",
            "MATCH (n)-[e]->(m) FILTER m IS DESTINATION OF e RETURN m.name AS x",
        ],
    );
}

#[test]
fn string_matching_operators() {
    accepts(
        "STARTS WITH / ENDS WITH / CONTAINS / LIKE",
        &[
            "MATCH (n:sensor) FILTER n.name STARTS WITH 'Temp' RETURN n.name AS x",
            "MATCH (n:sensor) FILTER n.name ENDS WITH '1' RETURN n.name AS x",
            "MATCH (n:sensor) FILTER n.name CONTAINS 'Sensor' RETURN n.name AS x",
            "MATCH (n:sensor) FILTER n.name LIKE 'Temp%' RETURN n.name AS x",
            "MATCH (n:sensor) FILTER n.name NOT LIKE '%offline%' RETURN n.name AS x",
        ],
    );
}

#[test]
fn in_list_predicate() {
    accepts(
        "IN list / NOT IN list",
        &[
            "MATCH (n:sensor) FILTER n.type IN ['temp', 'humidity'] RETURN n.name AS x",
            "MATCH (n:sensor) FILTER n.type NOT IN ['offline'] RETURN n.name AS x",
        ],
    );
}

#[test]
fn is_unknown_variants() {
    accepts(
        "IS UNKNOWN and IS NOT UNKNOWN",
        &[
            "RETURN null IS UNKNOWN AS b",
            "RETURN true IS NOT UNKNOWN AS b",
        ],
    );
}

#[test]
fn between_and_not_between() {
    accepts(
        "BETWEEN and NOT BETWEEN",
        &[
            "MATCH (n:sensor) FILTER n.temp BETWEEN 60 AND 80 RETURN n.name AS n",
            "MATCH (n:sensor) FILTER n.temp NOT BETWEEN 60 AND 80 RETURN n.name AS n",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Expressions — operator precedence smoke
// ──────────────────────────────────────────────────────────────────────

#[test]
fn boolean_operator_smoke() {
    accepts(
        "AND / OR / XOR / NOT combinations",
        &[
            "MATCH (n:sensor) FILTER n.temp > 70 AND n.alert = true RETURN n.name AS n",
            "MATCH (n:sensor) FILTER n.temp > 70 OR n.alert = true RETURN n.name AS n",
            "MATCH (n:sensor) FILTER NOT (n.temp > 70) RETURN n.name AS n",
            "MATCH (n:sensor) FILTER n.a XOR n.b RETURN n.name AS n",
            "MATCH (n:sensor) FILTER (n.a OR n.b) AND NOT n.c RETURN n.name AS n",
        ],
    );
}

#[test]
fn arithmetic_precedence_smoke() {
    accepts(
        "mixed arithmetic with parentheses",
        &[
            "RETURN 1 + 2 * 3 AS r",
            "RETURN (1 + 2) * 3 AS r",
            "RETURN -1 + 2 * -3 AS r",
            "RETURN 10 % 3 + 2 AS r",
            "RETURN 'a' || 'b' || 'c' AS r",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Aggregates — smoke of the family
// ──────────────────────────────────────────────────────────────────────

#[test]
fn aggregate_family() {
    accepts(
        "count / sum / avg / min / max / collect / stddev",
        &[
            "MATCH (n:sensor) RETURN count(*) AS c",
            "MATCH (n:sensor) RETURN count(DISTINCT n.type) AS c",
            "MATCH (n:sensor) RETURN sum(n.temp) AS s",
            "MATCH (n:sensor) RETURN avg(n.temp) AS a",
            "MATCH (n:sensor) RETURN min(n.temp) AS mn, max(n.temp) AS mx",
            "MATCH (n:sensor) RETURN collect(n.name) AS names",
            "MATCH (n:sensor) RETURN stddev_samp(n.temp) AS sd",
            "MATCH (n:sensor) RETURN stddev_pop(n.temp) AS sd",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// List operations — indexing and slicing
// ──────────────────────────────────────────────────────────────────────

#[test]
fn list_index_access() {
    accepts(
        "list[i] indexing",
        &[
            "RETURN [1, 2, 3][0] AS first",
            "RETURN [1, 2, 3][-1] AS last",
            "MATCH (n:sensor) RETURN n.tags[0] AS first",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// List iteration — ANY / ALL / NONE / SINGLE / REDUCE / list comprehension
// ──────────────────────────────────────────────────────────────────────

#[test]
fn list_iter_quantifiers() {
    accepts(
        "ANY / ALL / NONE / SINGLE quantifiers",
        &[
            "RETURN ANY (x IN [1, 2, 3] WHERE x > 2) AS r",
            "RETURN ALL (x IN [1, 2, 3] WHERE x > 0) AS r",
            "RETURN NONE (x IN [1, 2, 3] WHERE x > 5) AS r",
            "RETURN SINGLE (x IN [1, 2, 3] WHERE x = 2) AS r",
        ],
    );
}

#[test]
fn list_comprehension_basic() {
    accepts(
        "list comprehension [x IN list WHERE p | proj]",
        &[
            "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS r",
            "RETURN [x IN [1, 2, 3] | x * 2] AS r",
        ],
    );
}

#[test]
fn list_reduce() {
    accepts(
        "REDUCE accumulator over list",
        &["RETURN REDUCE(acc = 0, x IN [1, 2, 3] | acc + x) AS sum"],
    );
}

// ──────────────────────────────────────────────────────────────────────
// CALL subquery form (vs CALL procedure)
// ──────────────────────────────────────────────────────────────────────

#[test]
fn call_procedure_requires_yield() {
    // Selene's call_procedure grammar requires YIELD — every registered
    // procedure declares yield columns. This test documents the behavior.
    rejects(
        "CALL procedure without YIELD is rejected",
        &["CALL graph.rebuildVectorIndex('embedding')"],
    );
    accepts(
        "same procedure with an explicit YIELD is accepted",
        &["CALL graph.rebuildVectorIndex('embedding') YIELD indexed RETURN indexed"],
    );
}

#[test]
fn call_subquery_form() {
    accepts(
        "CALL { query } subquery form",
        &[
            "CALL { MATCH (n:sensor) RETURN n.name AS name } RETURN name",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// UNWIND
// ──────────────────────────────────────────────────────────────────────

#[test]
fn unwind_basic() {
    accepts(
        "UNWIND over a list and an expression",
        &[
            "UNWIND [1, 2, 3] AS n RETURN n",
            "UNWIND $rows AS row RETURN row.id AS id",
            "MATCH (n:sensor) UNWIND n.tags AS t RETURN t",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// WITH re-projection
// ──────────────────────────────────────────────────────────────────────

#[test]
fn with_reproject() {
    accepts(
        "WITH introducing new bindings",
        &[
            "MATCH (n:sensor) WITH n.temp AS t RETURN t",
            "MATCH (n:sensor) WITH n, n.temp AS t WHERE t > 70 RETURN n.name AS name",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// ORDER BY / SKIP / LIMIT / OFFSET
// ──────────────────────────────────────────────────────────────────────

#[test]
fn pagination_and_ordering() {
    accepts(
        "ORDER BY, OFFSET, LIMIT combinations",
        &[
            "MATCH (n:sensor) RETURN n.name AS name ORDER BY n.temp DESC",
            "MATCH (n:sensor) RETURN n.name AS name ORDER BY n.temp ASC, n.name DESC",
            "MATCH (n:sensor) RETURN n.name AS name LIMIT 10",
            "MATCH (n:sensor) RETURN n.name AS name OFFSET 5 LIMIT 10",
            "MATCH (n:sensor) RETURN n.name AS name ORDER BY n.name OFFSET 5 LIMIT 10",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// DDL — silent-failure probes
// ──────────────────────────────────────────────────────────────────────
//
// The audit flagged `.unwrap_or_default()` on DDL body-text extraction.
// These tests confirm the grammar at least catches malformed variants so
// the silent default never kicks in.

#[test]
fn create_node_type_requires_colon_and_body() {
    rejects(
        "CREATE NODE TYPE requires : and a body",
        &[
            "CREATE NODE TYPE sensor (temp :: FLOAT)", // missing leading colon
            "CREATE NODE TYPE :sensor",                // missing property list
            "CREATE NODE TYPE :sensor temp :: FLOAT",  // missing parens
        ],
    );
}

#[test]
fn create_edge_type_requires_endpoints_or_body() {
    accepts(
        "CREATE EDGE TYPE with FROM/TO endpoints",
        &["CREATE EDGE TYPE :contains (FROM :building TO :floor)"],
    );
    rejects(
        "CREATE EDGE TYPE without braces or colon",
        &[
            "CREATE EDGE TYPE contains",
            "CREATE EDGE TYPE :contains",
        ],
    );
}

#[test]
fn create_procedure_requires_body() {
    rejects(
        "CREATE PROCEDURE requires { body }",
        &[
            "CREATE PROCEDURE foo()",
            "CREATE PROCEDURE foo() { }",  // empty body must still fail — grammar requires a query pipeline
        ],
    );
}

#[test]
fn create_trigger_requires_event_and_body() {
    accepts(
        "CREATE TRIGGER minimal valid form",
        &[
            "CREATE TRIGGER t AFTER INSERT ON :sensor EXECUTE SET n.updated = true",
        ],
    );
    rejects(
        "CREATE TRIGGER without event, label, or body",
        &[
            "CREATE TRIGGER t",
            "CREATE TRIGGER t AFTER ON :sensor EXECUTE SET n.x = 1",
            "CREATE TRIGGER t AFTER INSERT ON sensor EXECUTE SET n.x = 1", // missing :
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Transaction control
// ──────────────────────────────────────────────────────────────────────

#[test]
fn transaction_control_statements() {
    accepts(
        "START / COMMIT / ROLLBACK",
        &["START TRANSACTION", "COMMIT", "ROLLBACK"],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Set operations — UNION / INTERSECT / EXCEPT
// ──────────────────────────────────────────────────────────────────────

#[test]
fn set_operations() {
    accepts(
        "UNION / UNION ALL / INTERSECT / EXCEPT / OTHERWISE",
        &[
            "MATCH (n:sensor) RETURN n.name AS x UNION MATCH (n:zone) RETURN n.name AS x",
            "MATCH (n:sensor) RETURN n.name AS x UNION ALL MATCH (n:zone) RETURN n.name AS x",
            "MATCH (n:sensor) RETURN n.name AS x INTERSECT MATCH (n:zone) RETURN n.name AS x",
            "MATCH (n:sensor) RETURN n.name AS x EXCEPT MATCH (n:zone) RETURN n.name AS x",
        ],
    );
}

// ──────────────────────────────────────────────────────────────────────
// Regression: CREATE PROCEDURE body preserves nested braces
//
// The old body-extraction used `rfind('{')` / `rfind('}')` on the full
// statement text, which mis-split when the body contained record
// literals or map properties. Now extracted from the `query_pipeline`
// child span. Test locks that behavior.
// ──────────────────────────────────────────────────────────────────────

#[test]
fn create_procedure_with_nested_braces_in_body() {
    use selene_gql::GqlStatement;

    let src = "CREATE PROCEDURE foo() { MATCH (n:sensor) RETURN {name: n.name, id: n.id} AS row }";
    let stmt = parse_statement(src).expect("create procedure should parse");
    match stmt {
        GqlStatement::CreateProcedure { body, .. } => {
            // Body must include the record literal; must NOT start at the
            // last `{` (which is inside the record).
            assert!(
                body.contains("MATCH") && body.contains("RETURN {name"),
                "body was mis-extracted (did rfind('{{') pick up the record literal?):\n  {body:?}"
            );
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn create_materialized_view_captures_match_and_return_text() {
    use selene_gql::GqlStatement;

    // CREATE MATERIALIZED VIEW grammar accepts only match_stmt + return_stmt
    // (no pipeline stages); WHERE goes inside MATCH.
    let src = "CREATE MATERIALIZED VIEW high_temps AS MATCH (n:sensor) WHERE n.temp > 80 RETURN n.name AS name";
    let stmt = parse_statement(src).expect("create view should parse");
    match stmt {
        GqlStatement::CreateMaterializedView {
            definition_text, ..
        } => {
            assert!(
                definition_text.contains("MATCH") && definition_text.contains("RETURN"),
                "definition_text must contain both MATCH and RETURN:\n  {definition_text:?}"
            );
        }
        other => panic!("expected CreateMaterializedView, got {other:?}"),
    }
}
