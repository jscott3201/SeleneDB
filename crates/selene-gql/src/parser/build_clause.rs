//! Pipeline clause AST builders: LET, FILTER, ORDER BY, RETURN, WITH,
//! CALL, mutations (INSERT/SET/REMOVE/DELETE), and projection helpers.

use pest::iterators::Pair;
use selene_core::IStr;

use super::Rule;
use crate::ast::expr::*;
use crate::ast::mutation::*;
use crate::ast::pattern::{EdgeDirection, LabelExpr};
use crate::ast::statement::*;
use crate::types::error::GqlError;

use super::build_expr::{
    build_expr, first_inner, intern_name, intern_prop, intern_var, unexpected_rule,
};
use super::build_match::{build_label_expr, build_match, build_property_map};

// ── LET ────────────────────────────────────────────────────────────

pub(in crate::parser) fn build_let(pair: Pair<'_, Rule>) -> Result<Vec<LetBinding>, GqlError> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::let_binding)
        .map(|lb| {
            let mut inner = lb.into_inner();
            let var = intern_var(inner.next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: missing variable in LET binding")
            })?);
            let expr = build_expr(inner.next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: missing expression in LET binding")
            })?)?;
            Ok(LetBinding { var, expr })
        })
        .collect()
}

// ── FILTER ─────────────────────────────────────────────────────────

pub(in crate::parser) fn build_filter(pair: Pair<'_, Rule>) -> Result<Expr, GqlError> {
    // filter_stmt = { ^"FILTER" ~ ^"WHERE"? ~ expr }
    let expr_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::expr || p.as_rule() == Rule::or_expr)
        .ok_or_else(|| GqlError::parse_error("expected expression in FILTER"))?;
    build_expr(expr_pair)
}

// ── ORDER BY ───────────────────────────────────────────────────────

pub(in crate::parser) fn build_order_by(pair: Pair<'_, Rule>) -> Result<Vec<OrderTerm>, GqlError> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::order_term)
        .map(build_order_term)
        .collect()
}

fn build_order_term(pair: Pair<'_, Rule>) -> Result<OrderTerm, GqlError> {
    let mut expr = None;
    let mut descending = false;
    let mut nulls_first = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::sort_dir => {
                descending = inner.as_str().eq_ignore_ascii_case("DESC");
            }
            Rule::nulls_order => {
                nulls_first = Some(inner.as_str().to_uppercase().contains("FIRST"));
            }
            _ => {
                if expr.is_none() {
                    expr = Some(build_expr(inner)?);
                }
            }
        }
    }

    Ok(OrderTerm {
        expr: expr.ok_or_else(|| GqlError::parse_error("expected expression in ORDER BY"))?,
        descending,
        nulls_first,
    })
}

// ── GROUP BY ──────────────────────────────────────────────────────

/// Parse a GROUP BY clause into a list of variable names.
///
/// Shared by `build_return`, `build_with`, and `build_select`.
pub(in crate::parser) fn build_group_by(pair: Pair<'_, Rule>) -> Result<Vec<IStr>, GqlError> {
    let mut keys = Vec::new();
    for gb_inner in pair.into_inner() {
        match gb_inner.as_rule() {
            Rule::ident => keys.push(intern_var(gb_inner)),
            Rule::group_by_item => {
                let expr = build_expr(first_inner(gb_inner)?)?;
                match &expr {
                    Expr::Var(name) => keys.push(*name),
                    _ => {
                        return Err(GqlError::parse_error(
                            "GROUP BY currently supports variable names only, not expressions",
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(keys)
}

// ── RETURN ─────────────────────────────────────────────────────────

pub(in crate::parser) fn build_return(pair: Pair<'_, Rule>) -> Result<ReturnClause, GqlError> {
    let mut no_bindings = false;
    let mut distinct = false;
    let mut all = false;
    let mut projections = Vec::new();
    let mut group_by = Vec::new();
    let mut having = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::no_bindings => no_bindings = true,
            Rule::distinct_kw => distinct = true,
            Rule::return_star => all = true,
            Rule::projection_list => {
                for proj in inner.into_inner() {
                    if proj.as_rule() == Rule::projection {
                        projections.push(build_projection(proj)?);
                    }
                }
            }
            Rule::group_by_clause => {
                group_by = build_group_by(inner)?;
            }
            Rule::having_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| GqlError::parse_error("HAVING missing expression"))?;
                having = Some(build_expr(expr_pair)?);
            }
            _ => {}
        }
    }

    Ok(ReturnClause {
        no_bindings,
        distinct,
        all,
        projections,
        group_by,
        having,
        // ORDER BY, OFFSET, LIMIT are separate pipeline statements
        order_by: vec![],
        offset: None,
        limit: None,
    })
}

pub(in crate::parser) fn build_with(pair: Pair<'_, Rule>) -> Result<WithClause, GqlError> {
    let mut distinct = false;
    let mut projections = Vec::new();
    let mut group_by = Vec::new();
    let mut having = None;
    let mut where_filter = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::distinct_kw => distinct = true,
            Rule::projection_list => {
                for proj in inner.into_inner() {
                    if proj.as_rule() == Rule::projection {
                        projections.push(build_projection(proj)?);
                    }
                }
            }
            Rule::group_by_clause => {
                group_by = build_group_by(inner)?;
            }
            Rule::having_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| GqlError::parse_error("HAVING missing expression"))?;
                having = Some(build_expr(expr_pair)?);
            }
            Rule::where_clause => {
                let expr_pair = inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| GqlError::parse_error("WITH WHERE missing expression"))?;
                where_filter = Some(build_expr(expr_pair)?);
            }
            _ => {}
        }
    }

    Ok(WithClause {
        distinct,
        projections,
        group_by,
        having,
        where_filter,
    })
}

pub(in crate::parser) fn build_projection(pair: Pair<'_, Rule>) -> Result<Projection, GqlError> {
    let mut expr = None;
    let mut alias = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::alias => {
                let ident_pair = inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::ident)
                    .ok_or_else(|| GqlError::parse_error("expected ident in alias"))?;
                alias = Some(intern_var(ident_pair));
            }
            _ => {
                if expr.is_none() {
                    expr = Some(build_expr(inner)?);
                }
            }
        }
    }

    Ok(Projection {
        expr: expr.ok_or_else(|| GqlError::parse_error("expected expression in projection"))?,
        alias,
    })
}

// ── CALL ───────────────────────────────────────────────────────────

pub(in crate::parser) fn build_call(pair: Pair<'_, Rule>) -> Result<ProcedureCall, GqlError> {
    let mut name = None;
    let mut args = Vec::new();
    let mut yields = Vec::new();
    let mut yield_star = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::qualified_name => {
                let parts: Vec<&str> = inner.into_inner().map(|p| p.as_str()).collect();
                name = Some(IStr::new(&parts.join(".")));
            }
            Rule::arg_list => {
                for arg in inner.into_inner() {
                    args.push(build_expr(arg)?);
                }
            }
            Rule::yield_clause => {
                for yi in inner.into_inner() {
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

    Ok(ProcedureCall {
        name: name.ok_or_else(|| GqlError::parse_error("expected procedure name"))?,
        args,
        yields,
        yield_star,
    })
}

pub(in crate::parser) fn build_yield_item(pair: Pair<'_, Rule>) -> Result<YieldItem, GqlError> {
    let mut name = None;
    let mut alias = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::ident if name.is_none() => name = Some(intern_var(inner)),
            Rule::alias => {
                let ident_pair = inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::ident)
                    .ok_or_else(|| {
                        GqlError::parse_error(
                            "unexpected parser state: missing ident in YIELD alias",
                        )
                    })?;
                alias = Some(intern_var(ident_pair));
            }
            _ => {}
        }
    }

    Ok(YieldItem {
        name: name.ok_or_else(|| GqlError::parse_error("expected name in YIELD item"))?,
        alias,
    })
}

// ── Mutations ──────────────────────────────────────────────────────

pub(in crate::parser) fn build_mutation_pipeline(
    pair: Pair<'_, Rule>,
) -> Result<MutationPipeline, GqlError> {
    let mut query_stmts = Vec::new();
    let mut mutations = Vec::new();
    let mut returning = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_stmt => {
                query_stmts.push(PipelineStatement::Match(build_match(inner)?));
            }
            Rule::filter_stmt => {
                query_stmts.push(PipelineStatement::Filter(build_filter(inner)?));
            }
            Rule::mutation_op => {
                let op = first_inner(inner)?;
                // Multi-item SET/REMOVE expand into multiple mutation ops
                match op.as_rule() {
                    Rule::set_stmt => {
                        for set_item in op.into_inner().filter(|p| p.as_rule() == Rule::set_item) {
                            mutations.push(build_set_item(set_item)?);
                        }
                    }
                    Rule::remove_stmt => {
                        for rm_item in op.into_inner().filter(|p| p.as_rule() == Rule::remove_item)
                        {
                            mutations.push(build_remove_item(rm_item)?);
                        }
                    }
                    Rule::insert_op => {
                        mutations.push(build_insert_pattern(op)?);
                    }
                    Rule::delete_op => {
                        // Multi-target DELETE: expand each ident into a separate Delete op
                        for ident_pair in op.into_inner().filter(|p| p.as_rule() == Rule::ident) {
                            mutations.push(MutationOp::Delete {
                                target: intern_var(ident_pair),
                            });
                        }
                    }
                    Rule::detach_delete_op => {
                        // Multi-target DETACH DELETE: expand each ident
                        for ident_pair in op.into_inner().filter(|p| p.as_rule() == Rule::ident) {
                            mutations.push(MutationOp::DetachDelete {
                                target: intern_var(ident_pair),
                            });
                        }
                    }
                    _ => {
                        mutations.push(build_mutation_op(op)?);
                    }
                }
            }
            Rule::return_stmt => {
                returning = Some(build_return(inner)?);
            }
            Rule::finish_stmt => {
                // FINISH = no RETURN. returning stays None → empty output schema.
            }
            _ => {}
        }
    }

    let query = if query_stmts.is_empty() {
        None
    } else {
        Some(QueryPipeline {
            statements: query_stmts,
        })
    };

    Ok(MutationPipeline {
        query,
        mutations,
        returning,
    })
}

fn build_mutation_op(pair: Pair<'_, Rule>) -> Result<MutationOp, GqlError> {
    match pair.as_rule() {
        // Note: insert_op, set_stmt, remove_stmt, delete_op, and detach_delete_op
        // are all handled directly in build_mutation_pipeline (multi-item expansion).
        // Only merge_op and fallback cases reach this function.
        Rule::merge_op => {
            let mut merge_var: Option<IStr> = None;
            let mut labels = Vec::new();
            let mut properties = Vec::new();
            let mut on_create = Vec::new();
            let mut on_match = Vec::new();

            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::node_pattern => {
                        for np in inner.into_inner() {
                            match np.as_rule() {
                                Rule::node_var => {
                                    merge_var = Some(intern_var(first_inner(np)?));
                                }
                                Rule::label_expr => {
                                    if let Ok(le) = build_label_expr(np) {
                                        collect_label_names(&le, &mut labels);
                                    }
                                }
                                Rule::property_map => {
                                    for pp in np
                                        .into_inner()
                                        .filter(|p| p.as_rule() == Rule::property_pair)
                                    {
                                        let mut parts = pp.into_inner();
                                        let key = IStr::new(
                                            parts
                                                .next()
                                                .ok_or_else(|| {
                                                    GqlError::parse_error("unexpected parser state: missing key in MERGE property")
                                                })?
                                                .as_str(),
                                        );
                                        let val = build_expr(parts.next().ok_or_else(|| {
                                            GqlError::parse_error("unexpected parser state: missing value in MERGE property")
                                        })?)?;
                                        properties.push((key, val));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    Rule::on_create_clause | Rule::on_match_clause => {
                        let is_create = inner.as_rule() == Rule::on_create_clause;
                        for sp in inner.into_inner().filter(|p| p.as_rule() == Rule::set_pair) {
                            let mut parts = sp.into_inner();
                            let target = intern_var(parts.next().ok_or_else(|| {
                                GqlError::parse_error(
                                    "unexpected parser state: missing target in MERGE ON SET",
                                )
                            })?);
                            let prop = intern_prop(parts.next().ok_or_else(|| {
                                GqlError::parse_error(
                                    "unexpected parser state: missing property in MERGE ON SET",
                                )
                            })?);
                            let val = build_expr(parts.next().ok_or_else(|| {
                                GqlError::parse_error(
                                    "unexpected parser state: missing value in MERGE ON SET",
                                )
                            })?)?;
                            if is_create {
                                on_create.push((target, prop, val));
                            } else {
                                on_match.push((target, prop, val));
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(MutationOp::Merge {
                var: merge_var,
                labels,
                properties,
                on_create,
                on_match,
            })
        }
        Rule::detach_delete_op => {
            let ident = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::ident)
                .ok_or_else(|| {
                    GqlError::parse_error("unexpected parser state: missing ident in DETACH DELETE")
                })?;
            Ok(MutationOp::DetachDelete {
                target: intern_var(ident),
            })
        }
        Rule::delete_op => {
            let ident = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::ident)
                .ok_or_else(|| {
                    GqlError::parse_error("unexpected parser state: missing ident in DELETE")
                })?;
            Ok(MutationOp::Delete {
                target: intern_var(ident),
            })
        }
        rule => Err(unexpected_rule("mutation_op", rule)),
    }
}

/// Collect flat label names from a LabelExpr tree (for INSERT/MERGE node labels).
///
/// Only Name, And, Or, and Concat contribute concrete labels. Not, Star,
/// Plus, Optional, and Wildcard are semantically invalid for INSERT (you
/// cannot insert a negated or quantified label), so they produce no names.
/// The grammar restricts INSERT labels to AND-conjunction, but MERGE
/// reuses the general label_expr rule. Returning empty for unsupported
/// variants is safe -- the mutation executor will reject a node with no labels.
fn collect_label_names(expr: &LabelExpr, names: &mut Vec<IStr>) {
    match expr {
        LabelExpr::Name(n) => names.push(*n),
        LabelExpr::Or(items) | LabelExpr::And(items) | LabelExpr::Concat(items) => {
            for item in items {
                collect_label_names(item, names);
            }
        }
        // Not/Star/Plus/Optional are pattern-matching constructs, not valid for
        // label creation. Return no names rather than recursing into them.
        LabelExpr::Not(_)
        | LabelExpr::Star(_)
        | LabelExpr::Plus(_)
        | LabelExpr::Optional(_)
        | LabelExpr::Wildcard => {}
    }
}

// ── INSERT graph pattern builder ──────────────────────────────────

fn build_insert_pattern(pair: Pair<'_, Rule>) -> Result<MutationOp, GqlError> {
    // insert_op = { INSERT ~ insert_graph_pattern }
    let graph_pattern = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::insert_graph_pattern)
        .ok_or_else(|| GqlError::parse_error("expected insert graph pattern"))?;

    let mut paths = Vec::new();
    for path_pair in graph_pattern
        .into_inner()
        .filter(|p| p.as_rule() == Rule::insert_path_pattern)
    {
        let mut elements = Vec::new();
        for elem in path_pair.into_inner() {
            match elem.as_rule() {
                Rule::insert_node_pattern => {
                    let mut var = None;
                    let mut labels = Vec::new();
                    let mut properties = Vec::new();
                    for p in elem.into_inner() {
                        match p.as_rule() {
                            Rule::ident => var = Some(intern_var(p)),
                            Rule::insert_label_set | Rule::label_expr => {
                                // insert_label_set wraps label_expr; unwrap if needed
                                let le_pair = if p.as_rule() == Rule::insert_label_set {
                                    p.into_inner().find(|c| c.as_rule() == Rule::label_expr)
                                } else {
                                    Some(p)
                                };
                                if let Some(lp) = le_pair
                                    && let Ok(le) = build_label_expr(lp)
                                {
                                    collect_label_names(&le, &mut labels);
                                }
                            }
                            Rule::property_map => {
                                properties = build_property_map(p)?;
                            }
                            _ => {}
                        }
                    }
                    elements.push(InsertElement::Node {
                        var,
                        labels,
                        properties,
                    });
                }
                Rule::insert_edge_pattern => {
                    let inner = first_inner(elem)?;
                    let direction = match inner.as_rule() {
                        Rule::insert_edge_right => EdgeDirection::Out,
                        Rule::insert_edge_left => EdgeDirection::In,
                        _ => EdgeDirection::Out,
                    };
                    let mut var = None;
                    let mut label = None;
                    let mut properties = Vec::new();
                    for p in inner.into_inner() {
                        match p.as_rule() {
                            Rule::edge_var => var = Some(intern_var(first_inner(p)?)),
                            Rule::label_expr => {
                                if let Ok(le) = build_label_expr(p)
                                    && let LabelExpr::Name(n) = &le
                                {
                                    label = Some(*n);
                                }
                            }
                            Rule::property_map => {
                                properties = build_property_map(p)?;
                            }
                            _ => {}
                        }
                    }
                    elements.push(InsertElement::Edge {
                        var,
                        label,
                        direction,
                        properties,
                    });
                }
                _ => {}
            }
        }
        paths.push(InsertPathPattern { elements });
    }

    Ok(MutationOp::InsertPattern(InsertGraphPattern { paths }))
}

// ── SET/REMOVE item builders ──────────────────────────────────────

fn build_set_item(pair: Pair<'_, Rule>) -> Result<MutationOp, GqlError> {
    let inner = first_inner(pair)?;
    match inner.as_rule() {
        Rule::set_property_item => {
            let mut target = None;
            let mut property = None;
            let mut value = None;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident if target.is_none() => target = Some(intern_var(p)),
                    Rule::prop_ident => property = Some(intern_prop(p)),
                    _ => {
                        if value.is_none() {
                            value = Some(build_expr(p)?);
                        }
                    }
                }
            }
            Ok(MutationOp::SetProperty {
                target: target.ok_or_else(|| GqlError::parse_error("expected target in SET"))?,
                property: property
                    .ok_or_else(|| GqlError::parse_error("expected property in SET"))?,
                value: value.ok_or_else(|| GqlError::parse_error("expected value in SET"))?,
            })
        }
        Rule::set_all_properties_item => {
            let mut target = None;
            let mut properties = Vec::new();
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => target = Some(intern_var(p)),
                    Rule::property_map => {
                        for pp in p.into_inner() {
                            if pp.as_rule() == Rule::property_pair {
                                let mut parts = pp.into_inner();
                                let key = intern_prop(parts.next().ok_or_else(|| {
                                    GqlError::parse_error(
                                        "unexpected parser state: missing key in SET = property",
                                    )
                                })?);
                                let val = build_expr(parts.next().ok_or_else(|| {
                                    GqlError::parse_error(
                                        "unexpected parser state: missing value in SET = property",
                                    )
                                })?)?;
                                properties.push((key, val));
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(MutationOp::SetAllProperties {
                target: target.ok_or_else(|| GqlError::parse_error("expected target in SET ="))?,
                properties,
            })
        }
        Rule::set_label_item => {
            let ident_pairs: Vec<Pair<'_, Rule>> = inner
                .into_inner()
                .filter(|p| p.as_rule() == Rule::ident)
                .collect();
            let target = intern_var(
                ident_pairs
                    .first()
                    .cloned()
                    .ok_or_else(|| GqlError::parse_error("SET label: missing target"))?,
            );
            let label = intern_name(
                ident_pairs
                    .get(1)
                    .cloned()
                    .ok_or_else(|| GqlError::parse_error("SET label: missing label"))?,
            );
            Ok(MutationOp::SetLabel { target, label })
        }
        _ => Err(GqlError::parse_error("unexpected set item")),
    }
}

fn build_remove_item(pair: Pair<'_, Rule>) -> Result<MutationOp, GqlError> {
    let inner = first_inner(pair)?;
    match inner.as_rule() {
        Rule::remove_property_item => {
            let mut target = None;
            let mut property = None;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::ident => target = Some(intern_var(p)),
                    Rule::prop_ident => property = Some(intern_prop(p)),
                    _ => {}
                }
            }
            Ok(MutationOp::RemoveProperty {
                target: target.ok_or_else(|| GqlError::parse_error("expected target in REMOVE"))?,
                property: property
                    .ok_or_else(|| GqlError::parse_error("expected property in REMOVE"))?,
            })
        }
        Rule::remove_label_item => {
            let ident_pairs: Vec<Pair<'_, Rule>> = inner
                .into_inner()
                .filter(|p| p.as_rule() == Rule::ident)
                .collect();
            let target = intern_var(
                ident_pairs
                    .first()
                    .cloned()
                    .ok_or_else(|| GqlError::parse_error("REMOVE label: missing target"))?,
            );
            let label = intern_name(
                ident_pairs
                    .get(1)
                    .cloned()
                    .ok_or_else(|| GqlError::parse_error("REMOVE label: missing label"))?,
            );
            Ok(MutationOp::RemoveLabel { target, label })
        }
        _ => Err(GqlError::parse_error("unexpected remove item")),
    }
}
