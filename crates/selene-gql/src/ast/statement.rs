//! GQL statement AST -- top-level query structure.
//!
//! A GQL submission is either a query pipeline, a mutation pipeline,
//! or a transaction control statement (START TRANSACTION / COMMIT / ROLLBACK).

use selene_core::IStr;

use super::expr::{Expr, ProcedureCall, YieldItem};
use super::mutation::MutationPipeline;
use super::pattern::MatchClause;

/// Top-level GQL statement.
#[derive(Debug, Clone)]
pub enum GqlStatement {
    /// Read-only query pipeline: MATCH → LET → FILTER → RETURN.
    Query(QueryPipeline),

    /// Chained query: first NEXT second -- output of first becomes input of second.
    Chained {
        blocks: Vec<QueryPipeline>,
    },

    /// Composite query: first UNION/INTERSECT/EXCEPT second [UNION ...].
    Composite {
        first: QueryPipeline,
        rest: Vec<(SetOp, QueryPipeline)>,
    },

    /// Transaction control.
    StartTransaction,
    Commit,
    Rollback,

    /// Mutation pipeline: optional MATCH + mutations + optional RETURN.
    Mutate(MutationPipeline),

    // ── DDL statements ────────────────────────────────────────────
    /// CREATE GRAPH name
    CreateGraph {
        name: String,
        if_not_exists: bool,
        or_replace: bool,
    },
    /// DROP GRAPH name
    DropGraph {
        name: String,
        if_exists: bool,
    },
    /// CREATE INDEX name ON :label(prop, ...)
    CreateIndex {
        name: String,
        label: String,
        properties: Vec<String>,
        if_not_exists: bool,
    },
    /// DROP INDEX name
    DropIndex {
        name: String,
        if_exists: bool,
    },
    /// CREATE USER name SET PASSWORD 'pass' [ROLE role]
    CreateUser {
        username: String,
        password: String,
        role: Option<String>,
        if_not_exists: bool,
    },
    /// DROP USER name
    DropUser {
        username: String,
        if_exists: bool,
    },
    /// CREATE ROLE name
    CreateRole {
        name: String,
        if_not_exists: bool,
    },
    /// DROP ROLE name
    DropRole {
        name: String,
        if_exists: bool,
    },
    /// GRANT ROLE role TO user
    GrantRole {
        role: String,
        username: String,
    },
    /// REVOKE ROLE role FROM user
    RevokeRole {
        role: String,
        username: String,
    },
    /// CREATE PROCEDURE name(params) { body }
    CreateProcedure {
        name: String,
        parameters: Vec<ProcedureParam>,
        body: String, // stored as GQL text
        or_replace: bool,
        if_not_exists: bool,
    },
    /// DROP PROCEDURE name
    DropProcedure {
        name: String,
        if_exists: bool,
    },

    // ── Trigger statements ──────────────────────────────────────────
    /// CREATE TRIGGER name AFTER event ON :label [WHEN cond] EXECUTE mutations
    CreateTrigger(CreateTriggerStmt),
    /// DROP TRIGGER name
    DropTrigger(String),
    /// SHOW TRIGGERS
    ShowTriggers,

    // ── Materialized view statements ────────────────────────────────
    /// CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] name AS MATCH ... RETURN ...
    CreateMaterializedView {
        name: IStr,
        or_replace: bool,
        if_not_exists: bool,
        /// Raw GQL text of the view definition (MATCH ... RETURN ...).
        definition_text: String,
        match_clause: MatchClause,
        return_clause: ReturnClause,
    },
    /// DROP MATERIALIZED VIEW [IF EXISTS] name
    DropMaterializedView {
        name: IStr,
        if_exists: bool,
    },
    /// SHOW MATERIALIZED VIEWS
    ShowMaterializedViews,

    // ── Type DDL statements ────────────────────────────────────────
    /// CREATE [OR REPLACE] NODE TYPE [IF NOT EXISTS] :label [EXTENDS :parent] (props)
    CreateNodeType {
        label: String,
        parent: Option<String>,
        properties: Vec<DdlPropertyDef>,
        or_replace: bool,
        if_not_exists: bool,
    },
    /// DROP NODE TYPE [IF EXISTS] :label
    DropNodeType {
        label: String,
        if_exists: bool,
    },
    /// SHOW NODE TYPES
    ShowNodeTypes,
    /// CREATE [OR REPLACE] EDGE TYPE [IF NOT EXISTS] :label (FROM ... TO ..., props)
    CreateEdgeType {
        label: String,
        source_labels: Vec<String>,
        target_labels: Vec<String>,
        properties: Vec<DdlPropertyDef>,
        or_replace: bool,
        if_not_exists: bool,
    },
    /// DROP EDGE TYPE [IF EXISTS] :label
    DropEdgeType {
        label: String,
        if_exists: bool,
    },
    /// SHOW EDGE TYPES
    ShowEdgeTypes,
}

/// CREATE TRIGGER statement.
#[derive(Debug, Clone)]
pub struct CreateTriggerStmt {
    pub name: String,
    pub event: selene_core::TriggerEvent,
    pub label: String,
    /// WHEN condition -- raw GQL expression text for storage. The Expr is for validation only.
    pub condition: Option<String>,
    /// EXECUTE action -- raw GQL mutation text for storage.
    pub action: String,
}

/// A property definition parsed from GQL type DDL.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct DdlPropertyDef {
    pub name: String,
    /// Type name as written in DDL (e.g. "INT", "STRING") -- mapped to ValueType at execution.
    pub value_type: String,
    /// NOT NULL constraint.
    pub required: bool,
    /// DEFAULT literal expression.
    pub default: Option<Expr>,
    /// IMMUTABLE constraint.
    pub immutable: bool,
    /// UNIQUE constraint.
    pub unique: bool,
    /// INDEXED constraint.
    pub indexed: bool,
    /// SEARCHABLE constraint (tantivy full-text index).
    pub searchable: bool,
    /// DICTIONARY constraint (intern string values).
    pub dictionary: bool,
    /// FILL strategy (LOCF, LINEAR).
    pub fill: Option<String>,
    /// INTERVAL duration string (e.g., "60s", "15m").
    pub expected_interval: Option<String>,
    /// ENCODING constraint (GORILLA, RLE, DICTIONARY).
    pub encoding: Option<String>,
}

/// Procedure parameter definition.
#[derive(Debug, Clone)]
pub struct ProcedureParam {
    pub name: IStr,
    pub type_name: String,
}

/// Set operation between two query pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    UnionAll,
    UnionDistinct,
    IntersectAll,
    IntersectDistinct,
    ExceptAll,
    ExceptDistinct,
    Otherwise,
}

/// A GQL query pipeline -- sequence of statements that each transform
/// the working table.
///
/// Mirrors the GQL spec's execution model:
///   MATCH → LET → FILTER → ORDER BY → LIMIT → RETURN
///
/// Each statement processes the output of the previous one.
#[derive(Debug, Clone)]
pub struct QueryPipeline {
    pub statements: Vec<PipelineStatement>,
}

/// A single statement in a query pipeline.
#[derive(Debug, Clone)]
pub enum PipelineStatement {
    /// MATCH [TRAIL] <patterns> [WHERE predicate]
    Match(MatchClause),

    /// LET var = expr, var2 = expr2
    Let(Vec<LetBinding>),

    /// FILTER [WHERE] <predicate>
    Filter(Expr),

    /// ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST], ...
    OrderBy(Vec<OrderTerm>),

    /// OFFSET n or OFFSET $param
    Offset(LimitValue),

    /// LIMIT n or LIMIT $param
    Limit(LimitValue),

    /// RETURN [DISTINCT] projections [GROUP BY vars] [HAVING expr]
    Return(ReturnClause),

    /// WITH [DISTINCT] projections [GROUP BY vars] [HAVING expr] [WHERE cond]
    /// Scope-resetting intermediate projection -- only projected variables survive.
    With(WithClause),

    /// CALL procedure(args) YIELD columns
    Call(ProcedureCall),

    /// CALL { subquery } -- inline subquery execution per input row
    Subquery(QueryPipeline),

    /// FOR var IN expr -- unwind list into rows (also parses UNWIND expr AS var)
    For { var: IStr, list_expr: Expr },

    /// MATCH VIEW name YIELD col1, col2 AS alias -- read from a materialized view.
    MatchView {
        name: IStr,
        yields: Vec<YieldItem>,
        yield_star: bool,
    },
}

/// Variable binding in a LET statement.
#[derive(Debug, Clone)]
pub struct LetBinding {
    pub var: IStr,
    pub expr: Expr,
}

/// LIMIT or OFFSET value: either a literal integer or a query parameter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LimitValue {
    /// Integer literal: `LIMIT 10`
    Literal(u64),
    /// Parameter reference: `LIMIT $n`
    Parameter(IStr),
}

impl LimitValue {
    /// Resolve to a concrete u64. For literals, returns the value directly.
    /// For parameters, looks up the value in the parameter map and casts to u64.
    pub fn resolve(
        &self,
        params: Option<&std::collections::HashMap<IStr, crate::types::value::GqlValue>>,
    ) -> Result<u64, crate::types::error::GqlError> {
        use crate::types::error::GqlError;
        use crate::types::value::GqlValue;
        match self {
            LimitValue::Literal(n) => Ok(*n),
            LimitValue::Parameter(name) => {
                let val = params
                    .and_then(|p| p.get(name))
                    .ok_or_else(|| {
                        GqlError::type_error(format!(
                            "LIMIT/OFFSET parameter ${} is not bound",
                            name.as_str()
                        ))
                    })?;
                match val {
                    GqlValue::Int(n) => {
                        if *n < 0 {
                            Err(GqlError::type_error(format!(
                                "LIMIT/OFFSET parameter ${} must be non-negative, got {n}",
                                name.as_str()
                            )))
                        } else {
                            Ok(*n as u64)
                        }
                    }
                    GqlValue::UInt(n) => Ok(*n),
                    other => Err(GqlError::type_error(format!(
                        "LIMIT/OFFSET parameter ${} must be an integer, got {}",
                        name.as_str(),
                        other.gql_type()
                    ))),
                }
            }
        }
    }
}

impl std::fmt::Display for LimitValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LimitValue::Literal(n) => write!(f, "{n}"),
            LimitValue::Parameter(name) => write!(f, "${}", name.as_str()),
        }
    }
}

/// Sort term for ORDER BY.
#[derive(Debug, Clone)]
pub struct OrderTerm {
    pub expr: Expr,
    pub descending: bool,
    /// NULLS FIRST/LAST override. None = default (NULLS LAST for ASC, NULLS FIRST for DESC).
    pub nulls_first: Option<bool>,
}

/// RETURN clause -- final projection with optional aggregation.
///
/// The `order_by`, `offset`, and `limit` fields are only populated by
/// `build_select` (SELECT desugaring), where ORDER BY / OFFSET / LIMIT
/// are syntactically part of the SELECT and get folded into the
/// ReturnClause. For native GQL `RETURN`, these are separate pipeline
/// statements and these fields remain empty/None.
#[derive(Debug, Clone)]
pub struct ReturnClause {
    /// RETURN NO BINDINGS -- returns empty result set.
    pub no_bindings: bool,
    /// DISTINCT modifier -- deduplicates using distinctness (not equality).
    pub distinct: bool,
    /// RETURN * -- project all bound variables.
    pub all: bool,
    /// Column projections (empty when all=true).
    pub projections: Vec<Projection>,
    /// GROUP BY expressions (variables, property access, etc.).
    pub group_by: Vec<Expr>,
    /// HAVING condition -- post-aggregation filter.
    pub having: Option<Expr>,
    /// RETURN-level ORDER BY -- only populated by SELECT desugaring.
    pub order_by: Vec<OrderTerm>,
    /// RETURN-level OFFSET -- only populated by SELECT desugaring.
    pub offset: Option<LimitValue>,
    /// RETURN-level LIMIT -- only populated by SELECT desugaring.
    pub limit: Option<LimitValue>,
}

/// WITH clause -- intermediate projection that resets binding scope.
/// Same structure as ReturnClause but does not terminate the pipeline.
#[derive(Debug, Clone)]
pub struct WithClause {
    pub distinct: bool,
    pub projections: Vec<Projection>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    /// Optional WHERE filter applied after projection.
    pub where_filter: Option<Expr>,
}

/// A single projection in RETURN/WITH.
#[derive(Debug, Clone)]
pub struct Projection {
    pub expr: Expr,
    /// Optional alias: `expr AS name`.
    pub alias: Option<IStr>,
}
