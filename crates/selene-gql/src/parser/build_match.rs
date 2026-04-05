//! MATCH clause, pattern, and label expression AST builders.

use pest::iterators::Pair;
use selene_core::IStr;

use super::Rule;
use crate::ast::expr::Expr;
use crate::ast::pattern::*;
use crate::types::error::GqlError;

use super::build_expr::{
    build_expr, first_inner, intern_name, intern_prop, intern_var, parse_uint, unexpected_rule,
};

// ── MATCH ──────────────────────────────────────────────────────────

pub(in crate::parser) fn build_match(pair: Pair<'_, Rule>) -> Result<MatchClause, GqlError> {
    let mut selector = None;
    let mut match_mode = None;
    let mut path_mode = PathMode::Walk;
    let mut optional = false;
    let mut patterns = Vec::new();
    let mut where_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::optional_modifier => optional = true,
            Rule::path_selector => {
                let s = inner.as_str().to_uppercase();
                selector = Some(if s.contains("ALL") {
                    PathSelector::AllShortest
                } else {
                    PathSelector::AnyShortest
                });
            }
            Rule::match_mode => {
                let s = inner.as_str().to_uppercase();
                match_mode = Some(if s.contains("DIFFERENT") {
                    MatchMode::DifferentEdges
                } else {
                    MatchMode::RepeatableElements
                });
            }
            Rule::path_modifier => {
                let mode_str = inner.as_str().to_uppercase();
                path_mode = match mode_str.as_str() {
                    "WALK" => PathMode::Walk,
                    "TRAIL" => PathMode::Trail,
                    "ACYCLIC" => PathMode::Acyclic,
                    "SIMPLE" => PathMode::Simple,
                    _ => PathMode::Walk,
                };
            }
            Rule::graph_pattern_list => {
                for gp in inner.into_inner() {
                    if gp.as_rule() == Rule::graph_pattern {
                        patterns.push(build_graph_pattern(gp)?);
                    }
                }
            }
            Rule::where_clause => {
                let expr_pair = first_inner(inner)?;
                where_clause = Some(build_expr(expr_pair)?);
            }
            _ => {}
        }
    }

    Ok(MatchClause {
        selector,
        match_mode,
        path_mode,
        optional,
        patterns,
        where_clause,
    })
}

fn build_graph_pattern(pair: Pair<'_, Rule>) -> Result<GraphPattern, GqlError> {
    let mut path_var = None;
    let mut elements = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::path_var_binding => {
                let ident_pair = first_inner(inner)?;
                path_var = Some(intern_var(ident_pair));
            }
            Rule::pattern_chain => {
                for chain_inner in inner.into_inner() {
                    match chain_inner.as_rule() {
                        Rule::node_pattern => {
                            elements.push(PatternElement::Node(build_node_pattern(chain_inner)?));
                        }
                        Rule::edge_pattern => {
                            elements.push(PatternElement::Edge(build_edge_pattern(chain_inner)?));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(GraphPattern { elements, path_var })
}

fn build_node_pattern(pair: Pair<'_, Rule>) -> Result<NodePattern, GqlError> {
    let mut var = None;
    let mut labels = None;
    let mut properties = Vec::new();
    let mut where_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::node_var => {
                let ident_pair = first_inner(inner)?;
                var = Some(intern_var(ident_pair));
            }
            Rule::label_expr => labels = Some(build_label_expr(inner)?),
            Rule::property_map => properties = build_property_map(inner)?,
            Rule::inline_where => {
                let expr_pair = first_inner(inner)?;
                where_clause = Some(build_expr(expr_pair)?);
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        var,
        labels,
        properties,
        where_clause,
    })
}

fn build_edge_pattern(pair: Pair<'_, Rule>) -> Result<EdgePattern, GqlError> {
    let inner = first_inner(pair)?;
    let (direction, rule_inner) = match inner.as_rule() {
        Rule::edge_right => (EdgeDirection::Out, inner),
        Rule::edge_left => (EdgeDirection::In, inner),
        Rule::edge_any => (EdgeDirection::Any, inner),
        // Abbreviated edge patterns -- no brackets, no interior
        Rule::abbrev_right => {
            return Ok(EdgePattern {
                var: None,
                labels: None,
                direction: EdgeDirection::Out,
                quantifier: None,
                properties: Vec::new(),
                where_clause: None,
            });
        }
        Rule::abbrev_left => {
            return Ok(EdgePattern {
                var: None,
                labels: None,
                direction: EdgeDirection::In,
                quantifier: None,
                properties: Vec::new(),
                where_clause: None,
            });
        }
        Rule::abbrev_any => {
            return Ok(EdgePattern {
                var: None,
                labels: None,
                direction: EdgeDirection::Any,
                quantifier: None,
                properties: Vec::new(),
                where_clause: None,
            });
        }
        rule => return Err(unexpected_rule("edge_pattern", rule)),
    };

    let mut var = None;
    let mut labels = None;
    let mut quantifier = None;
    let mut properties = Vec::new();
    let mut where_clause = None;

    for part in rule_inner.into_inner() {
        match part.as_rule() {
            Rule::edge_interior => {
                for ei in part.into_inner() {
                    match ei.as_rule() {
                        Rule::edge_var => {
                            let ident_pair = first_inner(ei)?;
                            var = Some(intern_var(ident_pair));
                        }
                        Rule::label_expr => labels = Some(build_label_expr(ei)?),
                        Rule::property_map => properties = build_property_map(ei)?,
                        Rule::inline_where => {
                            let expr_pair = first_inner(ei)?;
                            where_clause = Some(build_expr(expr_pair)?);
                        }
                        _ => {}
                    }
                }
            }
            Rule::quantifier => quantifier = Some(build_quantifier(part)?),
            _ => {}
        }
    }

    Ok(EdgePattern {
        var,
        labels,
        direction,
        quantifier,
        properties,
        where_clause,
    })
}

// ── Label expressions ──────────────────────────────────────────────

pub(in crate::parser) fn build_label_expr(pair: Pair<'_, Rule>) -> Result<LabelExpr, GqlError> {
    // label_expr = { ":" ~ label_or }
    let or_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::label_or)
        .ok_or_else(|| GqlError::parse_error("expected label_or in label_expr"))?;
    build_label_or(or_pair)
}

pub(in crate::parser) fn build_label_or(pair: Pair<'_, Rule>) -> Result<LabelExpr, GqlError> {
    let items: Vec<LabelExpr> = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::label_and)
        .map(build_label_and)
        .collect::<Result<_, _>>()?;
    if items.len() == 1 {
        Ok(items.into_iter().next().unwrap())
    } else {
        Ok(LabelExpr::Or(items))
    }
}

fn build_label_and(pair: Pair<'_, Rule>) -> Result<LabelExpr, GqlError> {
    let items: Vec<LabelExpr> = pair
        .into_inner()
        .filter(|p| p.as_rule() == Rule::label_not)
        .map(build_label_not)
        .collect::<Result<_, _>>()?;
    if items.len() == 1 {
        Ok(items.into_iter().next().unwrap())
    } else {
        Ok(LabelExpr::And(items))
    }
}

fn build_label_not(pair: Pair<'_, Rule>) -> Result<LabelExpr, GqlError> {
    let parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    if parts.len() == 1 {
        // label_atom
        build_label_atom(parts[0].clone())
    } else {
        // "!" ~ label_atom
        let atom = parts
            .last()
            .ok_or_else(|| GqlError::parse_error("empty label_not"))?;
        Ok(LabelExpr::Not(Box::new(build_label_atom(atom.clone())?)))
    }
}

fn build_label_atom(pair: Pair<'_, Rule>) -> Result<LabelExpr, GqlError> {
    // label_atom = { label_wildcard | ident }
    let inner = pair.into_inner().next();
    match inner {
        Some(p) if p.as_rule() == Rule::label_wildcard => {
            // % wildcard = match all nodes (spec §16.8)
            Ok(LabelExpr::Wildcard)
        }
        Some(p) => Ok(LabelExpr::Name(intern_name(p))),
        None => Err(GqlError::parse_error("empty label_atom")),
    }
}

// ── Quantifier ─────────────────────────────────────────────────────

fn build_quantifier(pair: Pair<'_, Rule>) -> Result<Quantifier, GqlError> {
    // Check for shortcut quantifiers: *, +, ?
    let first = pair
        .into_inner()
        .next()
        .ok_or_else(|| GqlError::parse_error("empty quantifier"))?;
    match first.as_rule() {
        Rule::quant_star => return Ok(Quantifier { min: 0, max: None }),
        Rule::quant_plus => return Ok(Quantifier { min: 1, max: None }),
        Rule::quant_question => {
            return Ok(Quantifier {
                min: 0,
                max: Some(1),
            });
        }
        _ => {}
    }
    // Fall through to brace-delimited quantifier: { quant_body }
    let body = if first.as_rule() == Rule::quant_body {
        first
    } else {
        return Err(GqlError::parse_error("expected quant_body"));
    };
    let inner = first_inner(body)?;
    match inner.as_rule() {
        Rule::quant_exact => {
            let n = parse_uint(inner)?;
            Ok(Quantifier {
                min: n,
                max: Some(n),
            })
        }
        Rule::quant_range_full => {
            let mut uints = inner.into_inner().filter(|p| p.as_rule() == Rule::uint);
            let min = parse_uint(uints.next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: missing min in quantifier range")
            })?)?;
            let max = parse_uint(uints.next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: missing max in quantifier range")
            })?)?;
            if min > max {
                return Err(GqlError::parse_error(format!(
                    "quantifier min ({min}) must not exceed max ({max})"
                )));
            }
            Ok(Quantifier {
                min,
                max: Some(max),
            })
        }
        Rule::quant_range_min => {
            let uint_pair = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::uint)
                .ok_or_else(|| {
                    GqlError::parse_error(
                        "unexpected parser state: missing uint in quantifier min range",
                    )
                })?;
            Ok(Quantifier {
                min: parse_uint(uint_pair)?,
                max: None,
            })
        }
        Rule::quant_range_max => {
            let uint_pair = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::uint)
                .ok_or_else(|| {
                    GqlError::parse_error(
                        "unexpected parser state: missing uint in quantifier max range",
                    )
                })?;
            Ok(Quantifier {
                min: 0,
                max: Some(parse_uint(uint_pair)?),
            })
        }
        rule => Err(unexpected_rule("quant_body", rule)),
    }
}

// ── Property map ───────────────────────────────────────────────────

pub(in crate::parser) fn build_property_map(
    pair: Pair<'_, Rule>,
) -> Result<Vec<(IStr, Expr)>, GqlError> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::property_pair)
        .map(|pp| {
            let mut inner = pp.into_inner();
            let key_pair = inner.next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: missing key in property pair")
            })?;
            // property_pair now uses prop_ident (keywords allowed as property names)
            let key = if key_pair.as_rule() == Rule::prop_ident {
                intern_prop(key_pair)
            } else {
                intern_name(key_pair)
            };
            let value = build_expr(inner.next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: missing value in property pair")
            })?)?;
            Ok((key, value))
        })
        .collect()
}
