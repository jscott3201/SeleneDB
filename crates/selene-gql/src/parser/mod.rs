//! GQL parser -- pest PEG grammar to AST.
//!
//! The grammar is defined in `grammar.pest`. This module provides the
//! pest-derived parser and the AST construction functions that convert
//! pest parse trees into typed AST nodes with IStr-interned identifiers.

use pest::Parser;
use pest_derive::Parser;

mod build;
mod build_clause;
mod build_expr;
mod build_match;

use crate::ast::statement::GqlStatement;
use crate::types::error::GqlError;

/// pest-derived GQL parser.
#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub struct GqlParser;

/// Parse a GQL string into a typed AST.
///
/// This is the primary entry point. Parses the input, constructs typed AST
/// nodes with IStr-interned identifiers, and returns a `GqlStatement`.
pub fn parse_statement(input: &str) -> Result<GqlStatement, GqlError> {
    let pairs = parse_raw(input)?;
    let pair = pairs
        .into_iter()
        .next()
        .ok_or_else(|| GqlError::parse_error("empty parse result"))?;
    build::build_statement(pair)
}

/// Parse a GQL statement string into a pest parse tree.
///
/// Low-level parse that returns pest Pairs. Use `parse_statement()` for
/// the full parse → AST pipeline.
pub(crate) fn parse_raw(input: &str) -> Result<pest::iterators::Pairs<'_, Rule>, GqlError> {
    GqlParser::parse(Rule::gql_statement, input).map_err(|e| {
        let (line, col) = match e.line_col {
            pest::error::LineColLocation::Pos((l, c)) => (l, c),
            pest::error::LineColLocation::Span((l, c), _) => (l, c),
        };
        let mut message = e.to_string();
        // Detect reserved keyword usage and add a helpful hint.
        // pest col is a 1-based character count; convert to byte offset
        // to avoid panicking on multi-byte UTF-8 input.
        if col > 0 {
            let byte_pos = input
                .char_indices()
                .nth(col.saturating_sub(1))
                .map_or(input.len(), |(i, _)| i);
            let rest = &input[byte_pos..];
            let word: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !word.is_empty() && is_reserved_keyword(&word) {
                message = format!(
                    "{message}\n\nHint: '{word}' is a reserved keyword. \
                     Use backticks to escape it: `{word}`"
                );
            }
        }
        GqlError::Parse {
            message,
            position: Some((line, col)),
        }
    })
}

/// Check if a word is a GQL reserved keyword (case-insensitive).
fn is_reserved_keyword(word: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "MATCH",
        "RETURN",
        "FILTER",
        "WHERE",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "GROUP",
        "LET",
        "SET",
        "DELETE",
        "INSERT",
        "REMOVE",
        "MERGE",
        "CALL",
        "YIELD",
        "AS",
        "FINISH",
        "DETACH",
        "FOR",
        "AND",
        "OR",
        "NOT",
        "XOR",
        "IN",
        "IS",
        "EXISTS",
        "NULL",
        "TRUE",
        "FALSE",
        "UNKNOWN",
        "WALK",
        "TRAIL",
        "ACYCLIC",
        "SIMPLE",
        "OPTIONAL",
        "SHORTEST",
        "DISTINCT",
        "ASC",
        "DESC",
        "START",
        "COMMIT",
        "ROLLBACK",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "OTHERWISE",
        "NEXT",
        "CAST",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "COUNT",
        "SUM",
        "AVERAGE",
        "AVG",
        "WITH",
        "HAVING",
        "UNWIND",
        "NULLS",
        "CREATE",
        "DROP",
        "GRANT",
        "REVOKE",
        "SELECT",
        "FROM",
        "DIRECTED",
        "LABELED",
        "RECORD",
        "NORMALIZED",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MONTH",
        "YEAR",
        "BETWEEN",
        "LIKE",
        "ARRAY",
        "BINDING",
        "CONNECTING",
        "DIFFERENT",
        "KEEP",
        "ONLY",
        "MATERIALIZED",
        "VIEW",
        "VIEWS",
    ];
    let upper = word.to_uppercase();
    KEYWORDS.iter().any(|k| *k == upper)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::expr::Expr;
    use crate::ast::mutation::InsertElement;

    /// Helper: parse and verify success.
    fn parse_ok(input: &str) -> pest::iterators::Pairs<'_, Rule> {
        parse_raw(input).unwrap_or_else(|e| panic!("parse failed for '{input}': {e}"))
    }

    /// Helper: parse and verify failure.
    fn parse_err(input: &str) {
        assert!(
            parse_raw(input).is_err(),
            "expected parse error for: {input}"
        );
    }

    // ── Literals ──

    #[test]
    fn parse_int_literal() {
        parse_ok("MATCH (n) RETURN 42");
    }

    #[test]
    fn parse_negative_int() {
        parse_ok("MATCH (n) RETURN -42");
    }

    #[test]
    fn parse_float_literal() {
        parse_ok("MATCH (n) RETURN 3.15");
    }

    #[test]
    fn parse_string_single_quote() {
        parse_ok("MATCH (n) RETURN 'hello'");
    }

    #[test]
    fn parse_string_double_quote() {
        parse_ok("MATCH (n) RETURN \"hello\"");
    }

    #[test]
    fn parse_bool_true() {
        parse_ok("MATCH (n) RETURN TRUE");
    }

    #[test]
    fn parse_null() {
        parse_ok("MATCH (n) RETURN NULL");
    }

    #[test]
    fn parse_list_literal() {
        parse_ok("MATCH (n) RETURN [1, 2, 3]");
    }

    #[test]
    fn parse_empty_list() {
        parse_ok("MATCH (n) RETURN []");
    }

    // ── Expressions ──

    #[test]
    fn parse_property_access() {
        parse_ok("MATCH (n) RETURN n.name");
    }

    #[test]
    fn parse_comparison() {
        parse_ok("MATCH (n) FILTER n.temp > 72 RETURN n");
    }

    #[test]
    fn parse_and_or() {
        parse_ok("MATCH (n) FILTER n.temp > 72 AND n.unit = 'F' RETURN n");
    }

    #[test]
    fn parse_arithmetic() {
        parse_ok("MATCH (n) LET x = n.a + n.b * 2 RETURN x");
    }

    // ── Node patterns ──

    #[test]
    fn parse_simple_node() {
        parse_ok("MATCH (n) RETURN n");
    }

    #[test]
    fn parse_labeled_node() {
        parse_ok("MATCH (n:sensor) RETURN n");
    }

    #[test]
    fn parse_node_with_properties() {
        parse_ok("MATCH (n:sensor {name: 'x'}) RETURN n");
    }

    // ── Edge patterns ──

    #[test]
    fn parse_outgoing_edge() {
        parse_ok("MATCH (a)-[:feeds]->(b) RETURN a, b");
    }

    #[test]
    fn parse_incoming_edge() {
        parse_ok("MATCH (a)<-[:feeds]-(b) RETURN a, b");
    }

    #[test]
    fn parse_undirected_edge() {
        parse_ok("MATCH (a)-[:knows]-(b) RETURN a, b");
    }

    // ── Variable-length ──

    #[test]
    fn parse_var_length_bounded() {
        parse_ok("MATCH (a)-[:contains]->{1,5}(b) RETURN a, b");
    }

    #[test]
    fn parse_var_length_exact() {
        parse_ok("MATCH (a)-[:contains]->{3}(b) RETURN a, b");
    }

    // ── Label expressions ──

    #[test]
    fn parse_label_or() {
        parse_ok("MATCH (n:sensor|equipment) RETURN n");
    }

    #[test]
    fn parse_label_and_not() {
        parse_ok("MATCH (n:sensor&!offline) RETURN n");
    }

    // ── TRAIL ──

    #[test]
    fn parse_trail() {
        parse_ok("MATCH TRAIL (a)-[:knows]->{1,4}(b) RETURN a, b");
    }

    // ── LET / FILTER / ORDER BY / LIMIT ──

    #[test]
    fn parse_let() {
        parse_ok("MATCH (n) LET x = n.temp + 10 RETURN x");
    }

    #[test]
    fn parse_filter() {
        parse_ok("MATCH (n) FILTER n.temp > 72 RETURN n");
    }

    #[test]
    fn parse_order_by_before_return() {
        parse_ok("MATCH (n) ORDER BY n.id RETURN n");
    }

    #[test]
    fn parse_order_by_after_return() {
        parse_ok("MATCH (n) RETURN n.name ORDER BY n.name DESC");
    }

    #[test]
    fn parse_limit() {
        parse_ok("MATCH (n) RETURN n LIMIT 10");
    }

    #[test]
    fn parse_limit_after_property() {
        parse_ok("MATCH (n) RETURN n.name LIMIT 10");
    }

    // ── RETURN ──

    #[test]
    fn parse_return_alias() {
        parse_ok("MATCH (n) RETURN n.name AS sensor_name");
    }

    #[test]
    fn parse_return_distinct() {
        parse_ok("MATCH (n) RETURN DISTINCT n.name");
    }

    #[test]
    fn parse_return_group_by() {
        // GROUP BY uses variable names, not property access (per GQL spec)
        parse_ok(
            "MATCH (n)-[:contains]->(s) LET name = n.name RETURN name, count(s) GROUP BY name",
        );
    }

    // ── Aggregation ──

    #[test]
    fn parse_count_star() {
        parse_ok("MATCH (n) RETURN count(*)");
    }

    #[test]
    fn parse_avg() {
        parse_ok("MATCH (n) RETURN avg(n.temp)");
    }

    // ── CALL ──

    #[test]
    fn parse_call_ts_latest() {
        parse_ok(
            "MATCH (s:sensor) CALL ts.latest(s.id, 'temperature') YIELD value AS temp RETURN s.name, temp",
        );
    }

    // ── WHERE on MATCH ──

    #[test]
    fn parse_match_where() {
        parse_ok("MATCH (n:sensor) WHERE n.temp > 72 RETURN n");
    }

    // ── Multi-pattern ──

    #[test]
    fn parse_multi_pattern() {
        parse_ok("MATCH (p)-[:workAt]->(c:company), (p)-[:livesIn]->(city:city) RETURN p, c, city");
    }

    // ── Path variable ──

    #[test]
    fn parse_path_variable() {
        parse_ok("MATCH p = (a)-[:feeds]->(b) RETURN p");
    }

    // ── Transactions ──

    #[test]
    fn parse_start_transaction() {
        parse_ok("START TRANSACTION");
    }

    #[test]
    fn parse_commit() {
        parse_ok("COMMIT");
    }

    #[test]
    fn parse_rollback() {
        parse_ok("ROLLBACK");
    }

    // ── Mutations ──

    #[test]
    fn parse_insert_node() {
        parse_ok("INSERT (:sensor {name: 'TempSensor'})");
    }

    #[test]
    fn parse_set_property() {
        parse_ok("MATCH (s:sensor) FILTER s.name = 'x' SET s.temp = 72.5 RETURN s");
    }

    #[test]
    fn parse_delete() {
        parse_ok("MATCH (s:sensor) FILTER s.name = 'old' DELETE s");
    }

    // ── Comments ──

    #[test]
    fn parse_line_comment() {
        parse_ok("// comment\nMATCH (n) RETURN n");
    }

    #[test]
    fn parse_dash_comment() {
        parse_ok("-- comment\nMATCH (n) RETURN n");
    }

    #[test]
    fn parse_block_comment() {
        parse_ok("/* block */ MATCH (n) RETURN n");
    }

    // ── Complex queries ──

    #[test]
    fn parse_full_pipeline() {
        parse_ok(
            "MATCH (b:building)-[:contains]->(s:sensor) \
             LET building_name = b.name \
             FILTER s.temp > 72 \
             RETURN building_name, count(s) AS sensor_count \
             GROUP BY building_name \
             ORDER BY sensor_count DESC \
             LIMIT 10",
        );
    }

    #[test]
    fn parse_trail_with_ts() {
        parse_ok(
            "MATCH TRAIL (b:building)-[:contains]->{1,5}(s:sensor) \
             CALL ts.latest(s.id, 'temperature') YIELD value AS temp \
             FILTER temp > 80 \
             RETURN b.name, s.name, temp \
             ORDER BY temp DESC",
        );
    }

    // ── Error cases ──

    #[test]
    fn parse_error_invalid() {
        parse_err("INVALID QUERY");
    }

    #[test]
    fn parse_error_position() {
        let err = parse_raw("MATCH (n) INVALID").unwrap_err();
        match err {
            GqlError::Parse { position, .. } => {
                assert!(position.is_some());
            }
            _ => panic!("expected Parse error"),
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // AST round-trip tests: parse GQL text, verify AST structure
    // ═══════════════════════════════════════════════════════════════

    use crate::ast::expr::*;
    use crate::ast::pattern::*;
    use crate::ast::statement::*;
    use crate::types::value::GqlValue;

    #[test]
    fn ast_simple_match_return() {
        let stmt = parse_statement("MATCH (n) RETURN n").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                assert_eq!(pipeline.statements.len(), 2);
                assert!(matches!(
                    pipeline.statements[0],
                    PipelineStatement::Match(_)
                ));
                assert!(matches!(
                    pipeline.statements[1],
                    PipelineStatement::Return(_)
                ));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_labeled_node() {
        let stmt = parse_statement("MATCH (s:sensor) RETURN s").unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                if let PipelineStatement::Match(m) = &p.statements[0] {
                    let PatternElement::Node(node) = &m.patterns[0].elements[0] else {
                        panic!("expected node");
                    };
                    assert_eq!(node.var.unwrap().as_str(), "S");
                    assert!(matches!(node.labels, Some(LabelExpr::Name(_))));
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_edge_pattern() {
        let stmt = parse_statement("MATCH (a)-[:feeds]->(b) RETURN a, b").unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                if let PipelineStatement::Match(m) = &p.statements[0] {
                    assert_eq!(m.patterns[0].elements.len(), 3); // node, edge, node
                    match &m.patterns[0].elements[1] {
                        PatternElement::Edge(e) => {
                            assert_eq!(e.direction, EdgeDirection::Out);
                            match &e.labels {
                                Some(LabelExpr::Name(n)) => assert_eq!(n.as_str(), "feeds"),
                                _ => panic!("expected label"),
                            }
                        }
                        PatternElement::Node(_) => panic!("expected edge"),
                    }
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_var_length_path() {
        let stmt = parse_statement("MATCH (a)-[:contains]->{1,5}(b) RETURN a, b").unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                if let PipelineStatement::Match(m) = &p.statements[0] {
                    match &m.patterns[0].elements[1] {
                        PatternElement::Edge(e) => {
                            let q = e.quantifier.unwrap();
                            assert_eq!(q.min, 1);
                            assert_eq!(q.max, Some(5));
                        }
                        PatternElement::Node(_) => panic!("expected edge"),
                    }
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_trail_modifier() {
        let stmt = parse_statement("MATCH TRAIL (a)-[:knows]->{1,4}(b) RETURN a").unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                if let PipelineStatement::Match(m) = &p.statements[0] {
                    assert!(m.path_mode == PathMode::Trail);
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_label_or() {
        let stmt = parse_statement("MATCH (n:sensor|equipment) RETURN n").unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                if let PipelineStatement::Match(m) = &p.statements[0] {
                    match &m.patterns[0].elements[0] {
                        PatternElement::Node(n) => {
                            assert!(matches!(n.labels, Some(LabelExpr::Or(_))));
                        }
                        PatternElement::Edge(_) => panic!("expected node"),
                    }
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_filter_comparison() {
        let stmt = parse_statement("MATCH (n) FILTER n.temp > 72 RETURN n").unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                assert_eq!(p.statements.len(), 3); // MATCH, FILTER, RETURN
                match &p.statements[1] {
                    PipelineStatement::Filter(expr) => {
                        assert!(matches!(expr, Expr::Compare(_, CompareOp::Gt, _)));
                    }
                    _ => panic!("expected Filter"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_let_binding() {
        let stmt = parse_statement("MATCH (n) LET x = n.temp + 10 RETURN x").unwrap();
        match &stmt {
            GqlStatement::Query(p) => match &p.statements[1] {
                PipelineStatement::Let(bindings) => {
                    assert_eq!(bindings.len(), 1);
                    assert_eq!(bindings[0].var.as_str(), "X");
                    assert!(matches!(
                        bindings[0].expr,
                        Expr::Arithmetic(_, ArithOp::Add, _)
                    ));
                }
                _ => panic!("expected Let"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_return_with_alias() {
        let stmt = parse_statement("MATCH (n) RETURN n.name AS sensor_name").unwrap();
        match &stmt {
            GqlStatement::Query(p) => match &p.statements[1] {
                PipelineStatement::Return(r) => {
                    assert_eq!(r.projections.len(), 1);
                    assert_eq!(r.projections[0].alias.unwrap().as_str(), "SENSOR_NAME");
                }
                _ => panic!("expected Return"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_return_group_by() {
        let stmt = parse_statement(
            "MATCH (n)-[:contains]->(s) LET name = n.name RETURN name, count(s) GROUP BY name",
        )
        .unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                // Find the RETURN statement
                let ret = p
                    .statements
                    .iter()
                    .find_map(|s| match s {
                        PipelineStatement::Return(r) => Some(r),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(ret.group_by.len(), 1);
                assert!(matches!(&ret.group_by[0], Expr::Var(n) if n.as_str() == "NAME"));
                assert_eq!(ret.projections.len(), 2);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_order_by_desc() {
        let stmt = parse_statement("MATCH (n) RETURN n.name ORDER BY n.name DESC").unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                // ORDER BY is a separate pipeline statement after RETURN
                let order = p
                    .statements
                    .iter()
                    .find_map(|s| match s {
                        PipelineStatement::OrderBy(terms) => Some(terms),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(order.len(), 1);
                assert!(order[0].descending);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_count_star() {
        let stmt = parse_statement("MATCH (n) RETURN count(*)").unwrap();
        match &stmt {
            GqlStatement::Query(p) => match &p.statements[1] {
                PipelineStatement::Return(r) => match &r.projections[0].expr {
                    Expr::Function(f) => assert!(f.count_star),
                    _ => panic!("expected Function"),
                },
                _ => panic!("expected Return"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_call_procedure() {
        let stmt = parse_statement(
            "MATCH (s:sensor) CALL ts.latest(s.id, 'temperature') YIELD value AS temp RETURN s.name, temp"
        ).unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                let call = p
                    .statements
                    .iter()
                    .find_map(|s| match s {
                        PipelineStatement::Call(c) => Some(c),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(call.name.as_str(), "ts.latest");
                assert_eq!(call.args.len(), 2);
                assert_eq!(call.yields.len(), 1);
                assert_eq!(call.yields[0].name.as_str(), "VALUE");
                assert_eq!(call.yields[0].alias.unwrap().as_str(), "TEMP");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_call_procedure_with_where_filter() {
        let stmt =
            parse_statement("CALL graph.labels() YIELD label WHERE label = 'sensor' RETURN label")
                .unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                let call = p
                    .statements
                    .iter()
                    .find_map(|s| match s {
                        PipelineStatement::Call(c) => Some(c),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(call.name.as_str(), "graph.labels");
                assert_eq!(call.yields.len(), 1);
                assert_eq!(call.yields[0].name.as_str(), "LABEL");
                assert!(
                    call.filter.is_some(),
                    "WHERE after YIELD should produce a filter"
                );
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_call_procedure_with_filter_where() {
        // FILTER WHERE variant should also work
        let stmt = parse_statement(
            "CALL graph.labels() YIELD label FILTER WHERE label = 'sensor' RETURN label",
        )
        .unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                let call = p
                    .statements
                    .iter()
                    .find_map(|s| match s {
                        PipelineStatement::Call(c) => Some(c),
                        _ => None,
                    })
                    .unwrap();
                assert!(
                    call.filter.is_some(),
                    "FILTER WHERE after YIELD should produce a filter"
                );
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_transaction_start() {
        let stmt = parse_statement("START TRANSACTION").unwrap();
        assert!(matches!(stmt, GqlStatement::StartTransaction));
    }

    #[test]
    fn ast_commit() {
        let stmt = parse_statement("COMMIT").unwrap();
        assert!(matches!(stmt, GqlStatement::Commit));
    }

    #[test]
    fn ast_insert_node() {
        let stmt = parse_statement("INSERT (:sensor {name: 'TempSensor'})").unwrap();
        match stmt {
            GqlStatement::Mutate(mp) => {
                assert_eq!(mp.mutations.len(), 1);
                match &mp.mutations[0] {
                    crate::ast::mutation::MutationOp::InsertPattern(pattern) => {
                        assert_eq!(pattern.paths.len(), 1);
                        assert_eq!(pattern.paths[0].elements.len(), 1);
                        match &pattern.paths[0].elements[0] {
                            crate::ast::mutation::InsertElement::Node {
                                labels,
                                properties,
                                ..
                            } => {
                                assert_eq!(labels.len(), 1);
                                assert_eq!(labels[0].as_str(), "sensor");
                                assert_eq!(properties.len(), 1);
                            }
                            InsertElement::Edge { .. } => panic!("expected InsertElement::Node"),
                        }
                    }
                    _ => panic!("expected InsertPattern"),
                }
            }
            _ => panic!("expected Mutate"),
        }
    }

    #[test]
    fn ast_set_property() {
        let stmt =
            parse_statement("MATCH (s:sensor) FILTER s.name = 'x' SET s.temp = 72.5 RETURN s")
                .unwrap();
        match stmt {
            GqlStatement::Mutate(mp) => {
                assert!(mp.query.is_some());
                assert_eq!(mp.mutations.len(), 1);
                assert!(mp.returning.is_some());
            }
            _ => panic!("expected Mutate"),
        }
    }

    #[test]
    fn ast_delete() {
        let stmt = parse_statement("MATCH (s:sensor) FILTER s.name = 'old' DELETE s").unwrap();
        match stmt {
            GqlStatement::Mutate(mp) => {
                assert!(mp.query.is_some());
                match &mp.mutations[0] {
                    crate::ast::mutation::MutationOp::Delete { target } => {
                        assert_eq!(target.as_str(), "S");
                    }
                    _ => panic!("expected Delete"),
                }
            }
            _ => panic!("expected Mutate"),
        }
    }

    #[test]
    fn ast_full_pipeline() {
        let stmt = parse_statement(
            "MATCH (b:building)-[:contains]->(s:sensor) \
             LET building_name = b.name \
             FILTER s.temp > 72 \
             RETURN building_name, count(s) AS sensor_count \
             GROUP BY building_name \
             ORDER BY sensor_count DESC \
             LIMIT 10",
        )
        .unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                // MATCH, LET, FILTER, RETURN, ORDER BY, LIMIT = 6 statements
                assert!(p.statements.len() >= 5);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_literal_int() {
        let stmt = parse_statement("MATCH (n) RETURN 42").unwrap();
        match &stmt {
            GqlStatement::Query(p) => match &p.statements[1] {
                PipelineStatement::Return(r) => match &r.projections[0].expr {
                    Expr::Literal(GqlValue::Int(42)) => {}
                    other => panic!("expected Int(42), got {other:?}"),
                },
                _ => panic!("expected Return"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_literal_string() {
        let stmt = parse_statement("MATCH (n) RETURN 'hello'").unwrap();
        match &stmt {
            GqlStatement::Query(p) => match &p.statements[1] {
                PipelineStatement::Return(r) => match &r.projections[0].expr {
                    Expr::Literal(GqlValue::String(s)) => assert_eq!(&**s, "hello"),
                    other => panic!("expected String, got {other:?}"),
                },
                _ => panic!("expected Return"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_boolean_and_or() {
        let stmt = parse_statement("MATCH (n) FILTER n.a > 1 AND n.b < 5 RETURN n").unwrap();
        match &stmt {
            GqlStatement::Query(p) => match &p.statements[1] {
                PipelineStatement::Filter(expr) => {
                    assert!(matches!(expr, Expr::Logic(_, LogicOp::And, _)));
                }
                _ => panic!("expected Filter"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn ast_node_with_inline_properties() {
        let stmt = parse_statement("MATCH (n:sensor {name: 'x'}) RETURN n").unwrap();
        match &stmt {
            GqlStatement::Query(p) => {
                if let PipelineStatement::Match(m) = &p.statements[0] {
                    match &m.patterns[0].elements[0] {
                        PatternElement::Node(n) => {
                            assert_eq!(n.properties.len(), 1);
                            assert_eq!(n.properties[0].0.as_str(), "name");
                        }
                        PatternElement::Edge(_) => panic!("expected node"),
                    }
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn parse_is_null_in_filter() {
        let q = "MATCH (n) FILTER n.x IS NULL RETURN n";
        let result = parse_raw(q);
        assert!(result.is_ok(), "failed to parse: {:?}", result.err());
    }

    #[test]
    fn parse_is_null_in_return() {
        let q = "RETURN NULL IS NULL AS R";
        let result = parse_raw(q);
        assert!(result.is_ok(), "failed to parse: {:?}", result.err());
    }

    #[test]
    fn parse_is_unknown_in_return() {
        let q = "RETURN TRUE IS UNKNOWN AS R";
        let result = parse_raw(q);
        assert!(result.is_ok(), "failed to parse: {:?}", result.err());
    }

    #[test]
    fn parse_between_in_return() {
        parse_ok("RETURN 5 BETWEEN 1 AND 10 AS R");
    }

    #[test]
    fn parse_between_in_where() {
        parse_ok("MATCH (n) WHERE n.x BETWEEN 1 AND 10 RETURN n");
    }

    #[test]
    fn parse_count_subquery() {
        parse_ok("MATCH (n:P) WHERE COUNT { MATCH (n)-[:knows]->() } >= 2 RETURN n");
    }

    // ── LIKE predicate ──

    #[test]
    fn parse_like() {
        parse_ok("MATCH (n) WHERE n.name LIKE 'A%' RETURN n");
    }

    #[test]
    fn parse_not_like() {
        parse_ok("MATCH (n) WHERE n.name NOT LIKE '%test%' RETURN n");
    }

    // ── DIFFERENT EDGES / REPEATABLE ELEMENTS match modes ──

    #[test]
    fn parse_different_edges() {
        parse_ok("MATCH DIFFERENT EDGES (a)-[e1]->(b), (b)-[e2]->(c) RETURN a, c");
    }

    #[test]
    fn parse_repeatable_elements() {
        parse_ok("MATCH REPEATABLE ELEMENTS (a)-[e]->(b) RETURN a");
    }

    // ── RETURN NO BINDINGS ──

    #[test]
    fn parse_return_no_bindings() {
        parse_ok("MATCH (n) RETURN NO BINDINGS");
    }

    // ── Quantifier validation ──

    #[test]
    fn quantifier_min_exceeds_max_is_error() {
        let result = parse_statement("MATCH (a)-[]->{5,1}(b) RETURN a");
        assert!(
            result.is_err(),
            "expected parse error for min > max quantifier"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("quantifier min (5) must not exceed max (1)"),
            "unexpected error message: {err}"
        );
    }

    // ── Expression depth limit ──

    #[test]
    fn expression_depth_limit() {
        // Each parenthesized expr traverses ~10 grammar levels in the AST builder
        // (or_expr -> xor_expr -> and_expr -> ... -> primary -> paren_expr).
        // 15 nested parens * ~10 levels = ~150, which exceeds the 128 limit.
        let mut query = String::from("MATCH (n) RETURN ");
        for _ in 0..15 {
            query.push('(');
        }
        query.push('1');
        for _ in 0..15 {
            query.push(')');
        }
        let result = parse_statement(&query);
        assert!(
            result.is_err(),
            "expected depth limit error for deeply nested expression"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expression nesting exceeds maximum depth of 128"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn parse_create_node_type_dictionary() {
        let stmt =
            parse_statement("CREATE NODE TYPE :Sensor (unit :: STRING DICTIONARY, name :: STRING)")
                .unwrap();
        match stmt {
            GqlStatement::CreateNodeType { properties, .. } => {
                assert!(properties[0].dictionary);
                assert!(!properties[1].dictionary);
            }
            _ => panic!("expected CreateNodeType"),
        }
    }

    // ── Materialized view DDL ───────────────────────────────────────

    #[test]
    fn parse_create_materialized_view() {
        let stmt = parse_statement(
            "CREATE MATERIALIZED VIEW stats AS MATCH (s:Sensor) RETURN count(*) AS cnt",
        )
        .unwrap();
        match stmt {
            GqlStatement::CreateMaterializedView {
                name,
                or_replace,
                if_not_exists,
                return_clause,
                ..
            } => {
                assert_eq!(name.as_str(), "stats");
                assert!(!or_replace);
                assert!(!if_not_exists);
                assert_eq!(return_clause.projections.len(), 1);
            }
            _ => panic!("expected CreateMaterializedView"),
        }
    }

    #[test]
    fn parse_create_or_replace_materialized_view() {
        let stmt = parse_statement(
            "CREATE OR REPLACE MATERIALIZED VIEW stats AS \
             MATCH (s:Sensor) WHERE s.temp > 0 \
             RETURN avg(s.temp) AS avg_temp, count(*) AS cnt",
        )
        .unwrap();
        match stmt {
            GqlStatement::CreateMaterializedView {
                name,
                or_replace,
                if_not_exists,
                return_clause,
                ..
            } => {
                assert_eq!(name.as_str(), "stats");
                assert!(or_replace);
                assert!(!if_not_exists);
                assert_eq!(return_clause.projections.len(), 2);
            }
            _ => panic!("expected CreateMaterializedView"),
        }
    }

    #[test]
    fn parse_create_materialized_view_if_not_exists() {
        let stmt = parse_statement(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS stats AS \
             MATCH (s:Sensor) RETURN count(*) AS cnt",
        )
        .unwrap();
        match stmt {
            GqlStatement::CreateMaterializedView { if_not_exists, .. } => {
                assert!(if_not_exists);
            }
            _ => panic!("expected CreateMaterializedView"),
        }
    }

    #[test]
    fn parse_drop_materialized_view() {
        let stmt = parse_statement("DROP MATERIALIZED VIEW stats").unwrap();
        match stmt {
            GqlStatement::DropMaterializedView { name, if_exists } => {
                assert_eq!(name.as_str(), "stats");
                assert!(!if_exists);
            }
            _ => panic!("expected DropMaterializedView"),
        }
    }

    #[test]
    fn parse_drop_materialized_view_if_exists() {
        let stmt = parse_statement("DROP MATERIALIZED VIEW IF EXISTS stats").unwrap();
        match stmt {
            GqlStatement::DropMaterializedView { name, if_exists } => {
                assert_eq!(name.as_str(), "stats");
                assert!(if_exists);
            }
            _ => panic!("expected DropMaterializedView"),
        }
    }

    #[test]
    fn parse_show_materialized_views() {
        let stmt = parse_statement("SHOW MATERIALIZED VIEWS").unwrap();
        assert!(matches!(stmt, GqlStatement::ShowMaterializedViews));
    }

    #[test]
    fn parse_match_view() {
        let stmt = parse_statement("MATCH VIEW stats YIELD avg_temp, cnt RETURN avg_temp").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => {
                assert!(pipeline.statements.len() >= 2);
                match &pipeline.statements[0] {
                    PipelineStatement::MatchView { name, yields, .. } => {
                        assert_eq!(name.as_str(), "stats");
                        assert_eq!(yields.len(), 2);
                        assert_eq!(yields[0].name.as_str(), "AVG_TEMP");
                        assert_eq!(yields[1].name.as_str(), "CNT");
                    }
                    _ => panic!("expected MatchView"),
                }
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn parse_match_view_with_alias() {
        let stmt =
            parse_statement("MATCH VIEW stats YIELD avg_temp AS temperature RETURN temperature")
                .unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => match &pipeline.statements[0] {
                PipelineStatement::MatchView { yields, .. } => {
                    assert_eq!(yields[0].name.as_str(), "AVG_TEMP");
                    assert_eq!(yields[0].alias.unwrap().as_str(), "TEMPERATURE");
                }
                _ => panic!("expected MatchView"),
            },
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn parse_match_view_yield_star() {
        let stmt = parse_statement("MATCH VIEW stats YIELD *").unwrap();
        match stmt {
            GqlStatement::Query(pipeline) => match &pipeline.statements[0] {
                PipelineStatement::MatchView {
                    name,
                    yields,
                    yield_star,
                } => {
                    assert_eq!(name.as_str(), "stats");
                    assert!(yields.is_empty());
                    assert!(*yield_star);
                }
                _ => panic!("expected MatchView"),
            },
            _ => panic!("expected Query"),
        }
    }

    // ── Record constructor / map literals ──

    #[test]
    fn record_constructor_with_keyword() {
        parse_ok("MATCH (n) RETURN RECORD {name: n.name, age: n.age}");
    }

    #[test]
    fn record_constructor_bare_map_literal() {
        parse_ok("MATCH (n) RETURN {name: n.name, age: n.age}");
    }

    #[test]
    fn record_constructor_both_forms_same_ast() {
        let with_kw = parse_statement("MATCH (n) RETURN RECORD {x: n.x}").unwrap();
        let bare = parse_statement("MATCH (n) RETURN {x: n.x}").unwrap();
        // Both produce RecordConstruct with the same field
        let extract = |stmt: crate::GqlStatement| -> String {
            if let crate::GqlStatement::Query(p) = stmt {
                format!("{:?}", p.statements.last())
            } else {
                panic!("expected Query")
            }
        };
        assert_eq!(extract(with_kw), extract(bare));
    }

    #[test]
    fn record_constructor_single_field() {
        parse_ok("MATCH (n) RETURN {id: id(n)}");
    }

    #[test]
    fn record_constructor_nested_expressions() {
        parse_ok("MATCH (n) RETURN {full: n.first || ' ' || n.last, count: count(*)}");
    }

    // ── Parameterized LIMIT/OFFSET ──

    #[test]
    fn parameterized_limit() {
        parse_ok("MATCH (n) RETURN n LIMIT $n");
    }

    #[test]
    fn parameterized_offset() {
        parse_ok("MATCH (n) RETURN n OFFSET $skip");
    }

    #[test]
    fn parameterized_skip_alias() {
        parse_ok("MATCH (n) RETURN n SKIP $page");
    }

    #[test]
    fn parameterized_limit_and_offset() {
        parse_ok("MATCH (n) RETURN n OFFSET $off LIMIT $lim");
    }

    #[test]
    fn literal_limit_still_works() {
        parse_ok("MATCH (n) RETURN n LIMIT 10");
    }

    // ── Reserved keyword error hints ──

    #[test]
    fn reserved_keyword_label_gives_hint() {
        let result = parse_statement("INSERT (:Commit {msg: 'test'})");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("reserved keyword"),
            "error should hint about reserved keyword: {err}"
        );
        assert!(
            err.contains("`Commit`"),
            "error should suggest backtick escaping: {err}"
        );
    }

    #[test]
    fn backtick_escaped_keyword_works() {
        parse_ok("INSERT (:`Commit` {msg: 'test'})");
    }
}
