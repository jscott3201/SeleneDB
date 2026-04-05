//! Expression AST nodes -- used in WHERE, FILTER, LET, RETURN, ORDER BY.
//!
//! All identifiers and property keys are `IStr` (interned at parse time).
//! No string comparisons occur during expression evaluation.

use selene_core::IStr;

use crate::types::value::{GqlType, GqlValue};

/// Expression AST node.
///
/// Represents any value-producing expression in GQL: literals, variable
/// references, property access, comparisons, arithmetic, boolean logic,
/// function calls, aggregation, CAST, and procedure calls.
///
/// This enum has ~30 variants, which makes `size_of::<Expr>()` larger
/// than ideal (~80-96 bytes depending on platform). This is a deliberate
/// trade-off: a flat enum avoids pointer-chasing during evaluation and
/// keeps pattern matching straightforward. Boxing heavy variants (Case,
/// Between, Trim) keeps the common-case size manageable.
#[derive(Debug, Clone)]
pub enum Expr {
    /// Literal value: 42, "hello", TRUE, NULL, [1,2,3], ZONED DATETIME '...'
    Literal(GqlValue),

    /// Variable reference: p, s, edge_var
    Var(IStr),

    /// Property access: p.name, e.creationDate
    Property(Box<Expr>, IStr),

    /// Temporal property access: s.temp AT TIME '2026-03-21T03:00:00Z'
    /// Resolves the property value at the given point in time via VersionStore.
    TemporalProperty(Box<Expr>, IStr, String),

    /// List construction: [expr1, expr2, ...] -- elements evaluated at runtime.
    ListConstruct(Vec<Expr>),

    /// List element access: interests[0]
    ListAccess(Box<Expr>, Box<Expr>),

    /// Binary comparison: =, <>, <, >, <=, >=
    Compare(Box<Expr>, CompareOp, Box<Expr>),

    /// Arithmetic: +, -, *, /
    Arithmetic(Box<Expr>, ArithOp, Box<Expr>),

    /// Boolean logic: AND, OR
    Logic(Box<Expr>, LogicOp, Box<Expr>),

    /// Unary NOT
    Not(Box<Expr>),

    /// Unary negation: -expr
    Negate(Box<Expr>),

    /// String concatenation: expr || expr
    Concat(Box<Expr>, Box<Expr>),

    /// String pattern matching: CONTAINS, STARTS WITH, ENDS WITH
    StringMatch(Box<Expr>, StringMatchOp, Box<Expr>),

    /// IS NULL / IS NOT NULL
    IsNull { expr: Box<Expr>, negated: bool },

    /// IN / NOT IN list
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    /// Function call: count(*), avg(x), coalesce(a, b), upper(s), size(list)
    Function(FunctionCall),

    /// CAST(expr AS type)
    Cast(Box<Expr>, GqlType),

    /// Aggregate expression: count(expr), sum(expr), avg(expr), etc.
    /// Separate from Function because aggregates have special evaluation
    /// semantics (vertical over groups, horizontal over group lists).
    Aggregate(AggregateExpr),

    /// labels(node_or_edge) -- returns list of label strings
    Labels(Box<Expr>),

    /// Query parameter: $name -- resolved from parameter map at runtime.
    Parameter(IStr),

    /// TRIM([LEADING|TRAILING|BOTH] [char] FROM source) --SQL TRIM syntax
    Trim {
        source: Box<Expr>,
        character: Option<Box<Expr>>,
        spec: TrimSpec,
    },

    /// RECORD {name: expr, ...} -- record constructor
    RecordConstruct(Vec<(IStr, Box<Expr>)>),

    /// IS [NOT] LABELED label_expression
    IsLabeled {
        expr: Box<Expr>,
        label: super::pattern::LabelExpr,
        negated: bool,
    },

    /// IS [NOT] DIRECTED -- tests if edge is directed (always true in Selene)
    IsDirected { expr: Box<Expr>, negated: bool },

    /// IS [NOT] TRUE / FALSE / UNKNOWN -- three-valued boolean test
    IsTruthValue {
        expr: Box<Expr>,
        value: TruthValue,
        negated: bool,
    },

    /// IS [NOT] TYPED type -- value type predicate (Feature GA06)
    IsTyped {
        expr: Box<Expr>,
        type_name: GqlType,
        negated: bool,
    },

    /// IS [NOT] [NFC|NFD|NFKC|NFKD] NORMALIZED --Unicode normalization test
    IsNormalized {
        expr: Box<Expr>,
        form: NormalForm,
        negated: bool,
    },

    /// IS [NOT] SOURCE OF edge_expr
    IsSourceOf {
        node: Box<Expr>,
        edge: Box<Expr>,
        negated: bool,
    },

    /// IS [NOT] DESTINATION OF edge_expr
    IsDestinationOf {
        node: Box<Expr>,
        edge: Box<Expr>,
        negated: bool,
    },

    /// [NOT] EXISTS { MATCH pattern }
    Exists {
        pattern: Box<super::pattern::MatchClause>,
        negated: bool,
    },

    /// ALL_DIFFERENT(a, b, c) -- all graph elements are distinct
    AllDifferent(Vec<Expr>),

    /// SAME(a, b) -- all graph elements are identical
    Same(Vec<Expr>),

    /// PROPERTY_EXISTS(node, 'key') -- property key exists on node
    PropertyExists(Box<Expr>, IStr),

    /// expr [NOT] LIKE pattern --SQL-style pattern matching (% = any, _ = one char)
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },

    /// expr [NOT] BETWEEN low AND high -- range test (inclusive)
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    /// COUNT { MATCH pattern } -- count matching bindings
    CountSubquery(Box<super::pattern::MatchClause>),

    /// CASE WHEN expr THEN expr [WHEN ...] [ELSE expr] END
    Case {
        branches: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },

    /// VALUE { subquery } -- executes subquery, returns scalar value.
    /// Errors if result has more than 1 row or more than 1 column.
    ValueSubquery(Box<super::statement::QueryPipeline>),

    /// COLLECT { subquery } -- executes subquery, collects all result values into a list.
    /// Supports DISTINCT, ORDER BY, LIMIT inside the subquery.
    CollectSubquery(Box<super::statement::QueryPipeline>),
}

impl Expr {
    /// Infer the output GqlType of this expression without executing it.
    /// Used for plan-time schema derivation (Arrow column types).
    /// Returns conservative estimates -- the actual type may be more specific.
    pub fn infer_type(&self) -> GqlType {
        match self {
            Expr::Literal(v) => v.gql_type(),
            Expr::Var(_) => GqlType::String, // unknown; refined at runtime
            Expr::Property(_, _) => GqlType::String, // unknown property type
            Expr::TemporalProperty(_, _, _) => GqlType::String, // unknown property type
            Expr::ListConstruct(_) => GqlType::List(Box::new(GqlType::Nothing)),
            Expr::ListAccess(_, _) => GqlType::String,
            Expr::Compare(_, _, _) => GqlType::Bool,
            Expr::Arithmetic(_, _, _) => GqlType::Float, // conservative
            Expr::Logic(_, _, _) => GqlType::Bool,
            Expr::Not(_) => GqlType::Bool,
            Expr::Negate(inner) => inner.infer_type(),
            Expr::Concat(_, _) => GqlType::String,
            Expr::StringMatch(_, _, _) => GqlType::Bool,
            Expr::IsNull { .. } => GqlType::Bool,
            Expr::InList { .. } => GqlType::Bool,
            Expr::Function(f) if f.count_star => GqlType::Int,
            Expr::Function(_) => GqlType::String, // unknown function return type
            Expr::Cast(_, target) => target.clone(),
            Expr::Aggregate(agg) => match agg.op {
                AggregateOp::Count => GqlType::Int,
                AggregateOp::Avg => GqlType::Float,
                AggregateOp::Sum | AggregateOp::Min | AggregateOp::Max => GqlType::Float,
                AggregateOp::CollectList => GqlType::List(Box::new(GqlType::Nothing)),
                AggregateOp::StddevSamp | AggregateOp::StddevPop => GqlType::Float,
            },
            Expr::Labels(_) => GqlType::List(Box::new(GqlType::String)),
            Expr::Parameter(_) => GqlType::String,
            Expr::IsLabeled { .. }
            | Expr::IsDirected { .. }
            | Expr::IsTruthValue { .. }
            | Expr::IsTyped { .. }
            | Expr::IsNormalized { .. }
            | Expr::IsSourceOf { .. }
            | Expr::IsDestinationOf { .. }
            | Expr::Exists { .. }
            | Expr::AllDifferent(_)
            | Expr::Same(_)
            | Expr::PropertyExists(_, _) => GqlType::Bool,
            Expr::Case {
                branches,
                else_expr,
            } => branches
                .first()
                .map(|(_, then)| then.infer_type())
                .or_else(|| else_expr.as_ref().map(|e| e.infer_type()))
                .unwrap_or(GqlType::String),
            Expr::ValueSubquery(_) => GqlType::String, // unknown subquery return type
            Expr::CollectSubquery(_) => GqlType::List(Box::new(GqlType::Nothing)),
            Expr::Trim { .. } => GqlType::String,
            Expr::RecordConstruct(_) => GqlType::Record,
            Expr::Like { .. } | Expr::Between { .. } => GqlType::Bool,
            Expr::CountSubquery(_) => GqlType::Int,
        }
    }

    /// Recursively visit all sub-expressions in pre-order (self first,
    /// then children). Subquery bodies (Exists, CountSubquery,
    /// ValueSubquery, CollectSubquery) are visited as leaf nodes: the
    /// callback fires on the subquery expression itself, but the walk
    /// does not descend into the inner `MatchClause` / `QueryPipeline`
    /// since those are not `Expr` trees.
    pub fn walk(&self, f: &mut impl FnMut(&Expr)) {
        f(self);
        match self {
            // Leaf nodes -- no children to visit
            Expr::Literal(_) | Expr::Var(_) | Expr::Parameter(_) => {}

            // Subquery leaves -- callback already fired above; inner
            // MatchClause / QueryPipeline are not Expr trees
            Expr::Exists { .. }
            | Expr::CountSubquery(_)
            | Expr::ValueSubquery(_)
            | Expr::CollectSubquery(_) => {}

            // Unary children
            Expr::Property(inner, _)
            | Expr::Not(inner)
            | Expr::Negate(inner)
            | Expr::Cast(inner, _)
            | Expr::Labels(inner)
            | Expr::TemporalProperty(inner, _, _) => inner.walk(f),

            Expr::IsNull { expr, .. }
            | Expr::IsLabeled { expr, .. }
            | Expr::IsDirected { expr, .. }
            | Expr::IsTruthValue { expr, .. }
            | Expr::IsTyped { expr, .. }
            | Expr::IsNormalized { expr, .. }
            | Expr::PropertyExists(expr, _) => expr.walk(f),

            // Binary children
            Expr::Compare(a, _, b)
            | Expr::Arithmetic(a, _, b)
            | Expr::Logic(a, _, b)
            | Expr::Concat(a, b)
            | Expr::StringMatch(a, _, b) => {
                a.walk(f);
                b.walk(f);
            }
            Expr::ListAccess(list, idx) => {
                list.walk(f);
                idx.walk(f);
            }
            Expr::Like { expr, pattern, .. } => {
                expr.walk(f);
                pattern.walk(f);
            }
            Expr::IsSourceOf { node, edge, .. } | Expr::IsDestinationOf { node, edge, .. } => {
                node.walk(f);
                edge.walk(f);
            }

            // Ternary children
            Expr::Between {
                expr, low, high, ..
            } => {
                expr.walk(f);
                low.walk(f);
                high.walk(f);
            }

            // Unary + optional child
            Expr::Trim {
                source, character, ..
            } => {
                source.walk(f);
                if let Some(c) = character {
                    c.walk(f);
                }
            }

            // List children
            Expr::InList { expr, list, .. } => {
                expr.walk(f);
                for item in list {
                    item.walk(f);
                }
            }
            Expr::ListConstruct(elems) | Expr::AllDifferent(elems) | Expr::Same(elems) => {
                for e in elems {
                    e.walk(f);
                }
            }
            Expr::RecordConstruct(fields) => {
                for (_, e) in fields {
                    e.walk(f);
                }
            }

            // Struct children
            Expr::Function(call) => {
                for arg in &call.args {
                    arg.walk(f);
                }
            }
            Expr::Aggregate(agg) => {
                if let Some(e) = &agg.expr {
                    e.walk(f);
                }
            }
            Expr::Case {
                branches,
                else_expr,
            } => {
                for (cond, val) in branches {
                    cond.walk(f);
                    val.walk(f);
                }
                if let Some(e) = else_expr {
                    e.walk(f);
                }
            }
        }
    }

    /// Returns true if this expression contains any aggregate operation.
    /// Used by the planner to determine GROUP BY vs simple projection.
    pub fn is_aggregate(&self) -> bool {
        let mut found = false;
        self.walk(&mut |e| {
            if !found {
                match e {
                    Expr::Aggregate(_) => found = true,
                    Expr::Function(f) if f.count_star => found = true,
                    _ => {}
                }
            }
        });
        found
    }
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,  // =
    Neq, // <>
    Lt,  // <
    Gt,  // >
    Lte, // <=
    Gte, // >=
}

/// Arithmetic operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add, // +
    Sub, // -
    Mul, // *
    Div, // /
    Mod, // %
}

/// Boolean logic operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicOp {
    And,
    Or,
    Xor,
}

/// TRIM specification: which end(s) to trim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimSpec {
    Leading,
    Trailing,
    Both,
}

/// Unicode normalization forms for IS NORMALIZED.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::upper_case_acronyms)]
pub enum NormalForm {
    NFC,
    NFD,
    NFKC,
    NFKD,
}

/// Boolean truth values for IS TRUE / IS FALSE / IS UNKNOWN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruthValue {
    True,
    False,
    Unknown,
}

/// String pattern matching operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringMatchOp {
    Contains,
    StartsWith,
    EndsWith,
}

/// Aggregate operation (vertical or horizontal).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CollectList,
    StddevSamp,
    StddevPop,
}

/// Aggregate expression -- wraps an operator and its argument expression.
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub op: AggregateOp,
    /// The expression to aggregate. For count(*), this is None.
    pub expr: Option<Box<Expr>>,
    /// DISTINCT modifier -- deduplicate values before aggregating.
    pub distinct: bool,
}

/// Function call -- named function with arguments.
#[derive(Debug, Clone)]
pub struct FunctionCall {
    /// Function name (e.g., "coalesce", "upper", "size", "char_length").
    pub name: IStr,
    /// Arguments.
    pub args: Vec<Expr>,
    /// True for count(*) -- special case with no argument expression.
    pub count_star: bool,
}

/// Procedure call --CALL qualified_name(args) YIELD columns.
#[derive(Debug, Clone)]
pub struct ProcedureCall {
    /// Qualified procedure name (e.g., "ts.range", "ts.latest").
    pub name: IStr,
    /// Arguments.
    pub args: Vec<Expr>,
    /// YIELD clause -- which columns to project from procedure results.
    pub yields: Vec<YieldItem>,
}

/// A single item in a YIELD clause.
#[derive(Debug, Clone)]
pub struct YieldItem {
    /// Column name from the procedure result.
    pub name: IStr,
    /// Optional alias: YIELD value AS temp.
    pub alias: Option<IStr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_is_not_aggregate() {
        let expr = Expr::Literal(GqlValue::Int(42));
        assert!(!expr.is_aggregate());
    }

    #[test]
    fn aggregate_expr_is_aggregate() {
        let expr = Expr::Aggregate(AggregateExpr {
            op: AggregateOp::Count,
            expr: Some(Box::new(Expr::Var(IStr::new("x")))),
            distinct: false,
        });
        assert!(expr.is_aggregate());
    }

    #[test]
    fn count_star_is_aggregate() {
        let expr = Expr::Function(FunctionCall {
            name: IStr::new("count"),
            args: vec![],
            count_star: true,
        });
        assert!(expr.is_aggregate());
    }

    #[test]
    fn nested_aggregate_detected() {
        // avg(x) + 1 -- the addition contains an aggregate
        let agg = Expr::Aggregate(AggregateExpr {
            op: AggregateOp::Avg,
            expr: Some(Box::new(Expr::Var(IStr::new("x")))),
            distinct: false,
        });
        let expr = Expr::Arithmetic(
            Box::new(agg),
            ArithOp::Add,
            Box::new(Expr::Literal(GqlValue::Int(1))),
        );
        assert!(expr.is_aggregate());
    }

    #[test]
    fn comparison_not_aggregate() {
        let expr = Expr::Compare(
            Box::new(Expr::Var(IStr::new("x"))),
            CompareOp::Gt,
            Box::new(Expr::Literal(GqlValue::Int(5))),
        );
        assert!(!expr.is_aggregate());
    }
}
