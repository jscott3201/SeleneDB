//! AST construction from pest parse trees.
//!
//! Converts pest `Pair<Rule>` nodes into typed AST nodes. All identifiers
//! are interned to `IStr` during construction. After this point, the
//! entire engine operates on integer comparisons.
//!
//! Split into sibling modules:
//! - `build_expr` -- expression/literal builders + shared helpers
//! - `build_match` -- MATCH clause, patterns, label expressions
//! - `build_clause` -- pipeline clauses (LET, FILTER, RETURN, etc.) + mutations

use pest::iterators::Pair;
use selene_core::IStr;

use super::Rule;
use crate::ast::statement::*;
use crate::types::error::GqlError;

use super::build_clause::{
    build_call, build_filter, build_group_by, build_let, build_mutation_pipeline, build_order_by,
    build_projection, build_return, build_with, build_yield_item,
};
use super::build_expr::{
    build_expr, build_limit_value, build_literal, first_inner, intern_name, intern_var,
    unescape_string, unexpected_rule,
};
use super::build_match::build_match;

/// Parse the common DDL pattern of a single name with optional
/// `IF EXISTS` / `IF NOT EXISTS` flags. Returns `(name, if_exists,
/// if_not_exists)`. The name token may be either `ident` or
/// `qualified_name`.
fn parse_name_with_flags(inner: Pair<'_, Rule>) -> (String, bool, bool) {
    let mut name = String::new();
    let mut if_exists = false;
    let mut if_not_exists = false;
    for p in inner.into_inner() {
        match p.as_rule() {
            Rule::ident | Rule::qualified_name => name = p.as_str().to_string(),
            Rule::if_exists => if_exists = true,
            Rule::if_not_exists => if_not_exists = true,
            _ => {}
        }
    }
    (name, if_exists, if_not_exists)
}

// ── Top-level ──────────────────────────────────────────────────────

/// Build a GqlStatement from a parsed gql_statement pair.
pub fn build_statement(pair: Pair<'_, Rule>) -> Result<GqlStatement, GqlError> {
    let inner = first_inner(pair)?;
    match inner.as_rule() {
        Rule::ddl_statement => build_ddl(inner),
        Rule::transaction_control => build_transaction(inner),
        Rule::chained_query => {
            let blocks: Vec<QueryPipeline> = inner
                .into_inner()
                .filter(|p: &Pair<'_, Rule>| p.as_rule() == Rule::query_pipeline)
                .map(build_query_pipeline)
                .collect::<Result<_, _>>()?;
            Ok(GqlStatement::Chained { blocks })
        }
        Rule::select_stmt => build_select(inner),
        Rule::composite_query => build_composite(inner),
        Rule::query_pipeline => Ok(GqlStatement::Query(build_query_pipeline(inner)?)),
        Rule::mutation_pipeline => Ok(GqlStatement::Mutate(build_mutation_pipeline(inner)?)),
        rule => Err(unexpected_rule("gql_statement", rule)),
    }
}

fn build_composite(pair: Pair<'_, Rule>) -> Result<GqlStatement, GqlError> {
    let mut pipelines = Vec::new();
    let mut ops = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::query_pipeline => pipelines.push(build_query_pipeline(inner)?),
            Rule::set_op => {
                let op_inner = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| GqlError::parse_error("expected set operation"))?;
                let op = match op_inner.as_rule() {
                    Rule::union_op => {
                        if op_inner.as_str().to_uppercase().contains("ALL") {
                            SetOp::UnionAll
                        } else {
                            SetOp::UnionDistinct
                        }
                    }
                    Rule::intersect_op => {
                        if op_inner.as_str().to_uppercase().contains("ALL") {
                            SetOp::IntersectAll
                        } else {
                            SetOp::IntersectDistinct
                        }
                    }
                    Rule::except_op => {
                        if op_inner.as_str().to_uppercase().contains("ALL") {
                            SetOp::ExceptAll
                        } else {
                            SetOp::ExceptDistinct
                        }
                    }
                    Rule::otherwise_op => SetOp::Otherwise,
                    _ => return Err(GqlError::parse_error("unknown set operation")),
                };
                ops.push(op);
            }
            _ => {}
        }
    }

    if pipelines.len() < 2 || ops.len() != pipelines.len() - 1 {
        return Err(GqlError::parse_error(
            "composite query requires at least 2 pipelines",
        ));
    }

    let first = pipelines.remove(0);
    let rest: Vec<(SetOp, QueryPipeline)> = ops.into_iter().zip(pipelines).collect();
    Ok(GqlStatement::Composite { first, rest })
}

/// Desugar SELECT to MATCH + RETURN pipeline
fn build_select(pair: Pair<'_, Rule>) -> Result<GqlStatement, GqlError> {
    let mut distinct = false;
    let mut all = false;
    let mut projections = Vec::new();
    let mut match_clause = None;
    let mut where_clause = None;
    let mut group_by = Vec::new();
    let mut having = None;
    let mut order_by = Vec::new();
    let mut offset = None;
    let mut limit = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::distinct_kw => distinct = true,
            Rule::return_star => all = true,
            Rule::projection_list => {
                for proj in inner.into_inner() {
                    if proj.as_rule() == Rule::projection {
                        projections.push(build_projection(proj)?);
                    }
                }
            }
            Rule::select_from => {
                let from_inner = first_inner(inner)?;
                if from_inner.as_rule() == Rule::match_stmt {
                    match_clause = Some(build_match(from_inner)?);
                }
                // FROM ident (graph name) -- not yet supported, ignored
            }
            Rule::where_clause => {
                where_clause = Some(build_expr(first_inner(inner)?)?);
            }
            Rule::group_by_clause => {
                group_by = build_group_by(inner)?;
            }
            Rule::having_clause => {
                having = Some(build_expr(first_inner(inner)?)?);
            }
            Rule::sorting_stmt => {
                order_by = build_order_by(inner)?;
            }
            Rule::offset_stmt => {
                offset = Some(build_limit_value(inner)?);
            }
            Rule::limit_stmt => {
                limit = Some(build_limit_value(inner)?);
            }
            _ => {}
        }
    }

    // Build pipeline: MATCH (if present) → FILTER WHERE (if present) → RETURN
    let mut stmts = Vec::new();

    if let Some(mc) = match_clause {
        stmts.push(PipelineStatement::Match(mc));
    }

    if let Some(wc) = where_clause {
        stmts.push(PipelineStatement::Filter(wc));
    }

    let ret = ReturnClause {
        no_bindings: false,
        distinct,
        all,
        projections,
        group_by,
        having,
        order_by,
        offset,
        limit,
    };
    stmts.push(PipelineStatement::Return(ret));

    Ok(GqlStatement::Query(QueryPipeline { statements: stmts }))
}

fn build_transaction(pair: Pair<'_, Rule>) -> Result<GqlStatement, GqlError> {
    let inner = first_inner(pair)?;
    match inner.as_rule() {
        Rule::start_transaction => Ok(GqlStatement::StartTransaction),
        Rule::commit => Ok(GqlStatement::Commit),
        Rule::rollback => Ok(GqlStatement::Rollback),
        rule => Err(unexpected_rule("transaction_control", rule)),
    }
}

// ── DDL statements ────────────────────────────────────────────────

fn build_ddl(pair: Pair<'_, Rule>) -> Result<GqlStatement, GqlError> {
    let inner = first_inner(pair)?;
    match inner.as_rule() {
        Rule::create_graph => {
            let mut or_replace = false;
            let mut if_not_exists = false;
            let mut name = String::new();
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => name = p.as_str().to_string(),
                    Rule::if_not_exists => if_not_exists = true,
                    Rule::or_replace => or_replace = true,
                    _ => {}
                }
            }
            Ok(GqlStatement::CreateGraph {
                name,
                if_not_exists,
                or_replace,
            })
        }
        Rule::drop_graph => {
            let (name, if_exists, _) = parse_name_with_flags(inner);
            Ok(GqlStatement::DropGraph { name, if_exists })
        }
        Rule::create_index => {
            let mut if_not_exists = false;
            let mut idents = Vec::new();
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => idents.push(p.as_str().to_string()),
                    Rule::if_not_exists => if_not_exists = true,
                    _ => {}
                }
            }
            // First ident is index name, second is label, rest are property columns
            if idents.len() < 3 {
                return Err(GqlError::parse_error(
                    "CREATE INDEX requires name, label, and at least one property",
                ));
            }
            let name = idents.remove(0);
            let label = idents.remove(0);
            Ok(GqlStatement::CreateIndex {
                name,
                label,
                properties: idents,
                if_not_exists,
            })
        }
        Rule::drop_index => {
            let (name, if_exists, _) = parse_name_with_flags(inner);
            Ok(GqlStatement::DropIndex { name, if_exists })
        }
        Rule::create_user => {
            let mut if_not_exists = false;
            let mut username = String::new();
            let mut password = String::new();
            let mut role = None;
            let mut idents = Vec::new();
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => idents.push(p.as_str().to_string()),
                    Rule::string_lit => {
                        let raw = p.as_str();
                        let s = &raw[1..raw.len() - 1]; // strip surrounding quotes
                        password = unescape_string(s);
                    }
                    Rule::if_not_exists => if_not_exists = true,
                    _ => {}
                }
            }
            if !idents.is_empty() {
                username = idents.remove(0);
            }
            if !idents.is_empty() {
                role = Some(idents.remove(0));
            }
            Ok(GqlStatement::CreateUser {
                username,
                password,
                role,
                if_not_exists,
            })
        }
        Rule::drop_user => {
            let (username, if_exists, _) = parse_name_with_flags(inner);
            Ok(GqlStatement::DropUser {
                username,
                if_exists,
            })
        }
        Rule::create_role => {
            let mut if_not_exists = false;
            let mut name = String::new();
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => name = p.as_str().to_string(),
                    Rule::if_not_exists => if_not_exists = true,
                    _ => {}
                }
            }
            Ok(GqlStatement::CreateRole {
                name,
                if_not_exists,
            })
        }
        Rule::drop_role => {
            let (name, if_exists, _) = parse_name_with_flags(inner);
            Ok(GqlStatement::DropRole { name, if_exists })
        }
        Rule::grant_role => {
            let mut idents = Vec::new();
            for p in inner.into_inner() {
                if p.as_rule() == Rule::ident {
                    idents.push(p.as_str().to_string());
                }
            }
            if idents.len() != 2 {
                return Err(GqlError::parse_error(
                    "GRANT ROLE requires role_name and username",
                ));
            }
            Ok(GqlStatement::GrantRole {
                role: idents.remove(0),
                username: idents.remove(0),
            })
        }
        Rule::revoke_role => {
            let mut idents = Vec::new();
            for p in inner.into_inner() {
                if p.as_rule() == Rule::ident {
                    idents.push(p.as_str().to_string());
                }
            }
            if idents.len() != 2 {
                return Err(GqlError::parse_error(
                    "REVOKE ROLE requires role_name and username",
                ));
            }
            Ok(GqlStatement::RevokeRole {
                role: idents.remove(0),
                username: idents.remove(0),
            })
        }
        Rule::create_procedure => {
            let mut or_replace = false;
            let mut if_not_exists = false;
            let mut name = String::new();
            let mut params = Vec::new();
            let mut body: Option<String> = None;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::qualified_name => name = p.as_str().to_string(),
                    Rule::if_not_exists => if_not_exists = true,
                    Rule::or_replace => or_replace = true,
                    Rule::param_list => {
                        for pd in p.into_inner() {
                            if pd.as_rule() == Rule::param_decl {
                                let mut parts = pd.into_inner();
                                let pname = parts.next().map_or_else(
                                    || {
                                        Err(GqlError::parse_error(
                                            "CREATE PROCEDURE parameter requires a name",
                                        ))
                                    },
                                    |x| Ok(intern_name(x)),
                                )?;
                                let ptype = parts.next().map_or_else(
                                    || {
                                        Err(GqlError::parse_error(
                                            "CREATE PROCEDURE parameter requires a type name",
                                        ))
                                    },
                                    |x| Ok(x.as_str().to_string()),
                                )?;
                                params.push(ProcedureParam {
                                    name: pname,
                                    type_name: ptype,
                                });
                            }
                        }
                    }
                    // Body is the query_pipeline child — use its span directly
                    // rather than rfind('{') heuristics, which would mis-split
                    // when the body contains nested braces (record literals,
                    // CASE blocks, map properties).
                    Rule::query_pipeline => {
                        body = Some(p.as_str().trim().to_string());
                    }
                    _ => {}
                }
            }
            let body = body.ok_or_else(|| {
                GqlError::parse_error(
                    "CREATE PROCEDURE body missing — expected a query pipeline between { and }",
                )
            })?;
            Ok(GqlStatement::CreateProcedure {
                name,
                parameters: params,
                body,
                or_replace,
                if_not_exists,
            })
        }
        Rule::drop_procedure => {
            let (name, if_exists, _) = parse_name_with_flags(inner);
            Ok(GqlStatement::DropProcedure { name, if_exists })
        }
        Rule::create_trigger => {
            let mut name = String::new();
            let mut event = selene_core::TriggerEvent::Set;
            let mut label = String::new();
            let mut condition: Option<String> = None;
            let mut mutation_ops_text = Vec::new();

            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => {
                        if name.is_empty() {
                            name = p.as_str().to_string();
                        } else if label.is_empty() {
                            label = p.as_str().to_string();
                        }
                    }
                    Rule::trigger_event => {
                        let ev_str = p.as_str().to_uppercase();
                        event = match ev_str.as_str() {
                            "INSERT" => selene_core::TriggerEvent::Insert,
                            "SET" => selene_core::TriggerEvent::Set,
                            "REMOVE" => selene_core::TriggerEvent::Remove,
                            "DELETE" => selene_core::TriggerEvent::Delete,
                            _ => {
                                return Err(GqlError::parse_error(format!(
                                    "unknown trigger event: {ev_str}"
                                )));
                            }
                        };
                    }
                    Rule::expr => {
                        condition = Some(p.as_str().to_string());
                    }
                    Rule::mutation_op => {
                        mutation_ops_text.push(p.as_str().to_string());
                    }
                    _ => {}
                }
            }

            let action = mutation_ops_text.join(" ");

            Ok(GqlStatement::CreateTrigger(CreateTriggerStmt {
                name,
                event,
                label,
                condition,
                action,
            }))
        }
        Rule::drop_trigger => {
            let name = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::ident)
                .map(|p| p.as_str().to_string())
                .ok_or_else(|| GqlError::parse_error("DROP TRIGGER requires a trigger name"))?;
            Ok(GqlStatement::DropTrigger(name))
        }
        Rule::show_triggers => Ok(GqlStatement::ShowTriggers),

        // ── Materialized view DDL ─────────────────────────────────────
        Rule::create_materialized_view => {
            let mut or_replace = false;
            let mut if_not_exists = false;
            let mut name = IStr::new("");
            let mut match_clause = None;
            let mut return_clause = None;
            // Capture the match_stmt start offset and return_stmt end offset
            // so we can slice the original input between them — this preserves
            // any whitespace / comments that sat between MATCH and RETURN.
            // Using `format!("{m} {r}")` of the two child strings would drop
            // that inter-clause text. Using `find(" AS ")` on the full input
            // would mis-split if the view name or a property alias contained
            // " AS ". Child spans are unambiguous.
            let outer_start = inner.as_span().start();
            let outer_str = inner.as_str();
            let mut match_start: Option<usize> = None;
            let mut return_end: Option<usize> = None;

            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::or_replace => or_replace = true,
                    Rule::if_not_exists => if_not_exists = true,
                    Rule::ident => name = intern_name(p),
                    Rule::match_stmt => {
                        match_start = Some(p.as_span().start() - outer_start);
                        match_clause = Some(build_match(p)?);
                    }
                    Rule::return_stmt => {
                        return_end = Some(p.as_span().end() - outer_start);
                        return_clause = Some(build_return(p)?);
                    }
                    _ => {}
                }
            }

            let definition_text = match (match_start, return_end) {
                (Some(s), Some(e)) => outer_str[s..e].to_string(),
                _ => {
                    return Err(GqlError::parse_error(
                        "CREATE MATERIALIZED VIEW requires both MATCH and RETURN clauses",
                    ));
                }
            };

            Ok(GqlStatement::CreateMaterializedView {
                name,
                or_replace,
                if_not_exists,
                definition_text,
                match_clause: match_clause.ok_or_else(|| {
                    GqlError::parse_error("CREATE MATERIALIZED VIEW requires a MATCH clause")
                })?,
                return_clause: return_clause.ok_or_else(|| {
                    GqlError::parse_error("CREATE MATERIALIZED VIEW requires a RETURN clause")
                })?,
            })
        }
        Rule::drop_materialized_view => {
            let mut if_exists = false;
            let mut name = IStr::new("");
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::if_exists => if_exists = true,
                    Rule::ident => name = intern_name(p),
                    _ => {}
                }
            }
            Ok(GqlStatement::DropMaterializedView { name, if_exists })
        }
        Rule::show_materialized_views => Ok(GqlStatement::ShowMaterializedViews),

        // ── Type DDL ──────────────────────────────────────────────────
        Rule::create_node_type => {
            let mut or_replace = false;
            let mut if_not_exists = false;
            let mut label = String::new();
            let mut parent = None;
            let mut properties = Vec::new();
            let mut idents = Vec::new();
            let mut validation_mode = None;

            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => idents.push(p.as_str().to_string()),
                    Rule::if_not_exists => if_not_exists = true,
                    Rule::or_replace => or_replace = true,
                    Rule::type_prop_def_list => {
                        properties = build_type_prop_def_list(p)?;
                    }
                    Rule::validation_mode_clause => {
                        validation_mode = Some(build_validation_mode(p));
                    }
                    _ => {}
                }
            }
            // First ident is the label, second (if present) is the EXTENDS parent
            if !idents.is_empty() {
                label = idents.remove(0);
            }
            if !idents.is_empty() {
                parent = Some(idents.remove(0));
            }
            Ok(GqlStatement::CreateNodeType {
                label,
                parent,
                properties,
                or_replace,
                if_not_exists,
                validation_mode,
            })
        }
        Rule::drop_node_type => {
            let mut if_exists = false;
            let mut label = String::new();
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => label = p.as_str().to_string(),
                    Rule::if_exists => if_exists = true,
                    _ => {}
                }
            }
            Ok(GqlStatement::DropNodeType { label, if_exists })
        }
        Rule::show_node_types => Ok(GqlStatement::ShowNodeTypes),

        Rule::create_edge_type => {
            let mut or_replace = false;
            let mut if_not_exists = false;
            let mut label = String::new();
            let mut source_labels = Vec::new();
            let mut target_labels = Vec::new();
            let mut properties = Vec::new();
            let mut validation_mode = None;

            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => label = p.as_str().to_string(),
                    Rule::if_not_exists => if_not_exists = true,
                    Rule::or_replace => or_replace = true,
                    Rule::edge_endpoint_clause => {
                        let (src, tgt) = build_edge_endpoint_clause(p);
                        source_labels = src;
                        target_labels = tgt;
                    }
                    Rule::type_prop_def_list => {
                        properties = build_type_prop_def_list(p)?;
                    }
                    Rule::validation_mode_clause => {
                        validation_mode = Some(build_validation_mode(p));
                    }
                    _ => {}
                }
            }
            Ok(GqlStatement::CreateEdgeType {
                label,
                source_labels,
                target_labels,
                properties,
                or_replace,
                if_not_exists,
                validation_mode,
            })
        }
        Rule::drop_edge_type => {
            let mut if_exists = false;
            let mut label = String::new();
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => label = p.as_str().to_string(),
                    Rule::if_exists => if_exists = true,
                    _ => {}
                }
            }
            Ok(GqlStatement::DropEdgeType { label, if_exists })
        }
        Rule::show_edge_types => Ok(GqlStatement::ShowEdgeTypes),

        rule => Err(unexpected_rule("ddl_statement", rule)),
    }
}

// ── Type DDL helpers ──────────────────────────────────────────────

fn build_type_prop_def_list(pair: Pair<'_, Rule>) -> Result<Vec<DdlPropertyDef>, GqlError> {
    let mut defs = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::type_prop_def {
            defs.push(build_type_prop_def(p)?);
        }
    }
    Ok(defs)
}

fn build_type_prop_def(pair: Pair<'_, Rule>) -> Result<DdlPropertyDef, GqlError> {
    let mut name = String::new();
    let mut value_type = String::new();
    let mut required = false;
    let mut default = None;
    let mut immutable = false;
    let mut unique = false;
    let mut indexed = false;
    let mut searchable = false;
    let mut dictionary = false;
    let mut fill: Option<String> = None;
    let mut expected_interval: Option<String> = None;
    let mut encoding: Option<String> = None;

    for p in pair.into_inner() {
        match p.as_rule() {
            Rule::ident => name = p.as_str().to_string(),
            Rule::type_name => value_type = p.as_str().to_string(),
            Rule::type_prop_constraint => {
                let text = p.as_str().to_uppercase();
                if text.contains("NOT") && text.contains("NULL") {
                    required = true;
                } else if text.starts_with("DEFAULT") || text.starts_with("default") {
                    // Extract the literal child from the constraint
                    for child in p.into_inner() {
                        if child.as_rule() == Rule::literal {
                            default = Some(build_literal(child)?);
                            break;
                        }
                    }
                } else if text == "IMMUTABLE" {
                    immutable = true;
                } else if text == "UNIQUE" {
                    unique = true;
                } else if text == "INDEXED" {
                    indexed = true;
                } else if text == "SEARCHABLE" {
                    searchable = true;
                } else if text == "DICTIONARY" {
                    dictionary = true;
                } else if text.starts_with("FILL") {
                    fill = p
                        .into_inner()
                        .find(|c| c.as_rule() == Rule::ident)
                        .map(|c| c.as_str().to_uppercase());
                } else if text.starts_with("INTERVAL") {
                    expected_interval = p
                        .into_inner()
                        .find(|c| c.as_rule() == Rule::string_lit)
                        .map(|c| c.as_str().trim_matches('\'').to_string());
                } else if text.starts_with("ENCODING") {
                    encoding = p
                        .into_inner()
                        .find(|c| c.as_rule() == Rule::ident)
                        .map(|c| c.as_str().to_uppercase());
                }
            }
            _ => {}
        }
    }

    Ok(DdlPropertyDef {
        name,
        value_type,
        required,
        default,
        immutable,
        unique,
        indexed,
        searchable,
        dictionary,
        fill,
        expected_interval,
        encoding,
    })
}

fn build_edge_endpoint_clause(pair: Pair<'_, Rule>) -> (Vec<String>, Vec<String>) {
    let mut lists: Vec<Vec<String>> = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::label_list {
            let labels: Vec<String> = p
                .into_inner()
                .filter(|c| c.as_rule() == Rule::ident)
                .map(|c| c.as_str().to_string())
                .collect();
            lists.push(labels);
        }
    }
    let target = if lists.len() > 1 {
        lists.remove(1)
    } else {
        Vec::new()
    };
    let source = if lists.is_empty() {
        Vec::new()
    } else {
        lists.remove(0)
    };
    (source, target)
}

/// Parse a `validation_mode_clause` (STRICT | WARN) to ValidationMode.
fn build_validation_mode(pair: Pair<'_, Rule>) -> selene_core::ValidationMode {
    // Case-insensitive: grammar uses ^"STRICT" / ^"WARN"; compare uppercased.
    if pair.as_str().trim().eq_ignore_ascii_case("strict") {
        selene_core::ValidationMode::Strict
    } else {
        selene_core::ValidationMode::Warn
    }
}

// ── Query pipeline ─────────────────────────────────────────────────

fn build_query_pipeline(pair: Pair<'_, Rule>) -> Result<QueryPipeline, GqlError> {
    let mut statements = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pipeline_statement {
            statements.push(build_pipeline_statement(inner)?);
        }
    }
    Ok(QueryPipeline { statements })
}

fn build_pipeline_statement(pair: Pair<'_, Rule>) -> Result<PipelineStatement, GqlError> {
    let inner = first_inner(pair)?;
    match inner.as_rule() {
        Rule::match_view_stmt => {
            let mut name = IStr::new("");
            let mut yields = Vec::new();
            let mut yield_star = false;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => name = intern_name(p),
                    Rule::yield_clause => {
                        for yi in p.into_inner() {
                            if yi.as_rule() == Rule::yield_item {
                                if yi.as_str().trim() == "*" {
                                    yield_star = true;
                                } else {
                                    yields.push(build_yield_item(yi)?);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(PipelineStatement::MatchView {
                name,
                yields,
                yield_star,
            })
        }
        Rule::match_stmt => Ok(PipelineStatement::Match(build_match(inner)?)),
        Rule::let_stmt => Ok(PipelineStatement::Let(build_let(inner)?)),
        Rule::for_stmt => {
            let mut parts = inner.into_inner();
            let var = intern_var(
                parts
                    .next()
                    .ok_or_else(|| GqlError::parse_error("FOR missing var"))?,
            );
            let expr = build_expr(
                parts
                    .next()
                    .ok_or_else(|| GqlError::parse_error("FOR missing IN expr"))?,
            )?;
            Ok(PipelineStatement::For {
                var,
                list_expr: expr,
            })
        }
        Rule::unwind_stmt => {
            let mut parts = inner.into_inner();
            let expr = build_expr(
                parts
                    .next()
                    .ok_or_else(|| GqlError::parse_error("UNWIND missing expr"))?,
            )?;
            let var = intern_var(
                parts
                    .next()
                    .ok_or_else(|| GqlError::parse_error("UNWIND missing AS var"))?,
            );
            Ok(PipelineStatement::For {
                var,
                list_expr: expr,
            })
        }
        Rule::with_stmt => Ok(PipelineStatement::With(build_with(inner)?)),
        Rule::filter_stmt => Ok(PipelineStatement::Filter(build_filter(inner)?)),
        Rule::sorting_stmt => Ok(PipelineStatement::OrderBy(build_order_by(inner)?)),
        Rule::offset_stmt => Ok(PipelineStatement::Offset(build_limit_value(inner)?)),
        Rule::limit_stmt => Ok(PipelineStatement::Limit(build_limit_value(inner)?)),
        Rule::return_stmt => Ok(PipelineStatement::Return(build_return(inner)?)),
        Rule::call_stmt => {
            let call_inner = first_inner(inner)?;
            match call_inner.as_rule() {
                Rule::call_subquery => {
                    let pipeline_pair = first_inner(call_inner)?;
                    Ok(PipelineStatement::Subquery(build_query_pipeline(
                        pipeline_pair,
                    )?))
                }
                Rule::call_procedure => Ok(PipelineStatement::Call(build_call(call_inner)?)),
                rule => Err(unexpected_rule("call_stmt", rule)),
            }
        }
        rule => Err(unexpected_rule("pipeline_statement", rule)),
    }
}
