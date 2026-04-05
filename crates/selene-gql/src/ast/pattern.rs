//! Graph pattern AST nodes -- MATCH clause patterns.
//!
//! Patterns describe graph structures to find: nodes, edges, paths,
//! variable-length traversals, and label expressions.

use selene_core::IStr;

use super::expr::Expr;

/// Path traversal mode for MATCH statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PathMode {
    /// Default: any walk, edges may repeat.
    #[default]
    Walk,
    /// TRAIL: no repeated edges.
    Trail,
    /// ACYCLIC: no repeated nodes (except start/end).
    Acyclic,
    /// SIMPLE: no repeated nodes at all.
    Simple,
}

/// Path selector for shortest-path queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathSelector {
    AnyShortest,
    AllShortest,
}

/// Match mode -- controls edge/element uniqueness across comma-separated patterns.
/// ISO GQL §16 SR 10: implementation-defined default. Selene defaults to RepeatableElements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchMode {
    /// No repeated edges across all patterns in this MATCH clause.
    DifferentEdges,
    /// Edges and elements may repeat freely (default).
    RepeatableElements,
}

/// MATCH clause -- one or more graph patterns with optional WHERE filter.
#[derive(Debug, Clone)]
pub struct MatchClause {
    /// Path selector: ANY SHORTEST, ALL SHORTEST.
    pub selector: Option<PathSelector>,
    /// Match mode: DIFFERENT EDGES, REPEATABLE ELEMENTS (default when not specified).
    pub match_mode: Option<MatchMode>,
    /// Path mode: WALK (default), TRAIL, ACYCLIC, SIMPLE.
    pub path_mode: PathMode,
    /// OPTIONAL modifier for left-join semantics.
    pub optional: bool,
    /// One or more comma-separated graph patterns.
    pub patterns: Vec<GraphPattern>,
    /// Statement-level WHERE predicate (post-filter on matched bindings).
    pub where_clause: Option<Expr>,
}

/// A single graph pattern -- chain of alternating node and edge patterns.
///
/// Examples:
///   `(a:sensor)` -- single node
///   `(a)-[:feeds]->(b)` -- node-edge-node
///   `(a)-[:contains]->{1,5}(b)` -- variable-length path
///   `p = (a)-[:feeds]->(b)` -- path variable binding
#[derive(Debug, Clone)]
pub struct GraphPattern {
    /// Pattern elements: alternating Node, Edge, Node, Edge, ..., Node.
    pub elements: Vec<PatternElement>,
    /// Optional path variable binding: `p = (a)-[]->(b)`.
    pub path_var: Option<IStr>,
}

/// An element in a graph pattern -- either a node or an edge.
#[derive(Debug, Clone)]
pub enum PatternElement {
    Node(NodePattern),
    Edge(EdgePattern),
}

/// Node pattern: `(var:LabelExpr {prop: value} WHERE predicate)`
#[derive(Debug, Clone)]
pub struct NodePattern {
    /// Variable name binding. None if anonymous: `(:sensor)`.
    pub var: Option<IStr>,
    /// Label expression: `:sensor`, `:sensor|equipment`, `:sensor&!offline`.
    pub labels: Option<LabelExpr>,
    /// Inline property filter: `{name: 'AHU-1', unit: '°F'}`.
    pub properties: Vec<(IStr, Expr)>,
    /// Inline WHERE predicate: `(s:sensor WHERE s.temp > 72)`.
    pub where_clause: Option<Expr>,
}

/// Edge pattern: `-[var:LabelExpr {props} WHERE pred]->`
#[derive(Debug, Clone)]
pub struct EdgePattern {
    /// Variable name binding. None if anonymous: `-[:feeds]->`.
    pub var: Option<IStr>,
    /// Label expression for edge type filtering.
    pub labels: Option<LabelExpr>,
    /// Edge direction.
    pub direction: EdgeDirection,
    /// Variable-length quantifier: `->{1,5}`.
    pub quantifier: Option<Quantifier>,
    /// Inline property filter: `[e:knows {since: 2020}]`.
    pub properties: Vec<(IStr, Expr)>,
    /// Inline WHERE predicate on edge properties.
    pub where_clause: Option<Expr>,
}

/// Edge direction in a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    /// `-[]->` -- outgoing (source to target).
    Out,
    /// `<-[]-` -- incoming (target to source).
    In,
    /// `-[]-` -- undirected (matches both directions).
    Any,
}

/// Variable-length path quantifier.
///
/// `{min, max}` -- match paths with edge count in [min, max].
/// `{min,}` -- min with no upper bound (max = None).
/// `{,max}` --0 to max.
/// `{n}` -- exactly n.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantifier {
    pub min: u32,
    /// None means unbounded (safety-capped at runtime).
    pub max: Option<u32>,
}

/// Label expression with AND/OR/NOT per GQL spec.
///
/// Compiled to RoaringBitmap operations at execution time:
/// - Name → bitmap lookup
/// - Or → bitmap union
/// - And → bitmap intersection
/// - Not → bitmap difference against all-nodes/edges bitmap
#[derive(Debug, Clone)]
pub enum LabelExpr {
    /// Single label: `:sensor`
    Name(IStr),
    /// OR: `:sensor|equipment` → bitmap union
    Or(Vec<LabelExpr>),
    /// AND: `:sensor&monitored` → bitmap intersection
    And(Vec<LabelExpr>),
    /// NOT: `!offline` → bitmap complement
    Not(Box<LabelExpr>),
    /// Wildcard: `:%` -- matches any label
    Wildcard,
    // ── RPQ (Regular Path Query) extensions ─────────────────────────
    /// Concatenation: `:feeds.serves` -- multi-hop sequence
    Concat(Vec<LabelExpr>),
    /// Kleene star: `(feeds|serves)*` -- zero or more repetitions
    Star(Box<LabelExpr>),
    /// Plus: `(feeds)+` -- one or more repetitions
    Plus(Box<LabelExpr>),
    /// Optional: `(feeds)?` -- zero or one
    Optional(Box<LabelExpr>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_node_pattern() {
        let pattern = NodePattern {
            var: Some(IStr::new("s")),
            labels: Some(LabelExpr::Name(IStr::new("sensor"))),
            properties: vec![],
            where_clause: None,
        };
        assert_eq!(pattern.var.unwrap().as_str(), "s");
    }

    #[test]
    fn edge_pattern_with_quantifier() {
        let pattern = EdgePattern {
            var: None,
            labels: Some(LabelExpr::Name(IStr::new("contains"))),
            direction: EdgeDirection::Out,
            quantifier: Some(Quantifier {
                min: 1,
                max: Some(5),
            }),
            properties: vec![],
            where_clause: None,
        };
        assert_eq!(pattern.quantifier.unwrap().min, 1);
        assert_eq!(pattern.quantifier.unwrap().max, Some(5));
    }

    #[test]
    fn label_expr_or() {
        let expr = LabelExpr::Or(vec![
            LabelExpr::Name(IStr::new("sensor")),
            LabelExpr::Name(IStr::new("equipment")),
        ]);
        match expr {
            LabelExpr::Or(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn label_expr_and_not() {
        // :sensor&!offline
        let expr = LabelExpr::And(vec![
            LabelExpr::Name(IStr::new("sensor")),
            LabelExpr::Not(Box::new(LabelExpr::Name(IStr::new("offline")))),
        ]);
        match expr {
            LabelExpr::And(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn graph_pattern_with_path_var() {
        let pattern = GraphPattern {
            elements: vec![
                PatternElement::Node(NodePattern {
                    var: Some(IStr::new("a")),
                    labels: None,
                    properties: vec![],
                    where_clause: None,
                }),
                PatternElement::Edge(EdgePattern {
                    var: None,
                    labels: Some(LabelExpr::Name(IStr::new("feeds"))),
                    direction: EdgeDirection::Out,
                    quantifier: None,
                    properties: vec![],
                    where_clause: None,
                }),
                PatternElement::Node(NodePattern {
                    var: Some(IStr::new("b")),
                    labels: None,
                    properties: vec![],
                    where_clause: None,
                }),
            ],
            path_var: Some(IStr::new("p")),
        };
        assert_eq!(pattern.elements.len(), 3);
        assert_eq!(pattern.path_var.unwrap().as_str(), "p");
    }
}
