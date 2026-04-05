//! Expression and literal AST builders, plus shared helpers.

use pest::iterators::Pair;
use selene_core::IStr;
use smol_str::SmolStr;

use super::Rule;
use crate::ast::expr::*;
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── Shared helpers ─────────────────────────────────────────────────

/// Get the first inner pair of a rule.
pub(in crate::parser) fn first_inner(pair: Pair<'_, Rule>) -> Result<Pair<'_, Rule>, GqlError> {
    pair.into_inner()
        .next()
        .ok_or_else(|| GqlError::parse_error("expected inner rule"))
}

/// Strip backtick or double-quote delimiters from an identifier string.
/// Escaped double-quotes (`""`) inside double-quoted identifiers are unescaped.
/// Non-delimited identifiers are returned as-is.
pub(in crate::parser) fn strip_delimiters(s: &str) -> std::borrow::Cow<'_, str> {
    if s.starts_with('`') && s.ends_with('`') {
        std::borrow::Cow::Borrowed(&s[1..s.len() - 1])
    } else if s.starts_with('"') && s.ends_with('"') {
        std::borrow::Cow::Owned(s[1..s.len() - 1].replace("\"\"", "\""))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// Intern as a binding variable. Case-fold non-delimited to uppercase (ISO §21.3 SR 17-18).
/// Delimited identifiers (backtick or double-quote) preserve case exactly.
pub(in crate::parser) fn intern_var(pair: Pair<'_, Rule>) -> IStr {
    let s = pair.as_str();
    let stripped = strip_delimiters(s);
    if s.starts_with('`') || s.starts_with('"') {
        IStr::new(&stripped)
    } else {
        IStr::new(&stripped.to_uppercase())
    }
}

/// Intern as a name (label, alias, procedure name). Preserve case.
/// Delimited identifiers strip quotes; undelimited identifiers are kept as-is.
pub(in crate::parser) fn intern_name(pair: Pair<'_, Rule>) -> IStr {
    IStr::new(&strip_delimiters(pair.as_str()))
}

/// Intern a property key from prop_ident rule. Delegates to `intern_name` (preserve case).
pub(in crate::parser) fn intern_prop(pair: Pair<'_, Rule>) -> IStr {
    intern_name(pair)
}

/// Parse a uint rule to u32.
pub(in crate::parser) fn parse_uint(pair: Pair<'_, Rule>) -> Result<u32, GqlError> {
    let s = pair.as_str();
    s.parse::<u32>()
        .map_err(|_| GqlError::parse_error(format!("invalid unsigned integer: {s}")))
}

/// Extract uint from offset_stmt or limit_stmt.
pub(in crate::parser) fn build_uint(pair: Pair<'_, Rule>) -> Result<u64, GqlError> {
    let uint_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::uint)
        .ok_or_else(|| GqlError::parse_error("expected uint"))?;
    uint_pair
        .as_str()
        .parse::<u64>()
        .map_err(|_| GqlError::parse_error("invalid uint"))
}

/// Create an error for an unexpected rule.
pub(in crate::parser) fn unexpected_rule(context: &str, rule: Rule) -> GqlError {
    GqlError::parse_error(format!("unexpected rule {rule:?} in {context}"))
}

// ── Expression builder ─────────────────────────────────────────────

/// Maximum allowed expression nesting depth.
const MAX_EXPR_DEPTH: u16 = 128;

/// Public entry point for expression parsing (depth starts at 0).
pub(in crate::parser) fn build_expr(pair: Pair<'_, Rule>) -> Result<Expr, GqlError> {
    build_expr_depth(pair, 0)
}

/// Depth-tracked expression builder. All recursive calls within the
/// expression chain go through this function so that deeply nested
/// expressions are rejected before they overflow the call stack.
fn build_expr_depth(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(GqlError::parse_error(
            "expression nesting exceeds maximum depth of 128",
        ));
    }
    match pair.as_rule() {
        Rule::expr | Rule::or_expr => build_or_expr(pair, depth),
        Rule::xor_expr => build_xor_expr(pair, depth),
        Rule::and_expr => build_and_expr(pair, depth),
        Rule::not_expr => build_not_expr(pair, depth),
        Rule::is_expr => build_is_expr(pair, depth),
        Rule::comparison => build_comparison(pair, depth),
        Rule::concat => build_concat(pair, depth),
        Rule::addition => build_addition(pair, depth),
        Rule::multiplication => build_multiplication(pair, depth),
        Rule::unary => build_unary(pair, depth),
        Rule::postfix => build_postfix(pair, depth),
        Rule::primary => build_primary(pair, depth),
        // Literals and direct values
        Rule::literal => build_literal(pair),
        Rule::var_ref => Ok(Expr::Var(intern_var(first_inner(pair)?))),
        Rule::int_lit
        | Rule::float_lit
        | Rule::uint_lit
        | Rule::string_lit
        | Rule::bool_lit
        | Rule::null_lit
        | Rule::unknown_lit
        | Rule::hex_lit
        | Rule::oct_lit
        | Rule::bin_lit
        | Rule::list_lit
        | Rule::duration_lit
        | Rule::date_lit
        | Rule::time_lit
        | Rule::zoned_datetime_lit
        | Rule::local_datetime_lit
        | Rule::zoned_time_lit
        | Rule::local_time_lit
        | Rule::datetime_bare_lit => build_literal_value(pair),
        Rule::aggregate_expr => build_aggregate(pair, depth),
        Rule::function_call => build_function_call(pair, depth),
        Rule::all_different_expr => {
            let d = depth + 1;
            let exprs: Vec<Expr> = pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::expr)
                .map(|p| build_expr_depth(p, d))
                .collect::<Result<_, _>>()?;
            Ok(Expr::AllDifferent(exprs))
        }
        Rule::same_expr => {
            let d = depth + 1;
            let exprs: Vec<Expr> = pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::expr)
                .map(|p| build_expr_depth(p, d))
                .collect::<Result<_, _>>()?;
            Ok(Expr::Same(exprs))
        }
        Rule::property_exists_expr => {
            let mut parts = pair.into_inner();
            let node_expr = build_expr_depth(
                parts.next().ok_or_else(|| {
                    GqlError::parse_error(
                        "unexpected parser state: missing node expression in PROPERTY_EXISTS",
                    )
                })?,
                depth + 1,
            )?;
            let key_lit = parts.next().ok_or_else(|| {
                GqlError::parse_error(
                    "unexpected parser state: missing key literal in PROPERTY_EXISTS",
                )
            })?;
            let key_str = key_lit.as_str();
            // Strip quotes from string literal
            let key = key_str.trim_matches('\'').trim_matches('"');
            Ok(Expr::PropertyExists(Box::new(node_expr), IStr::new(key)))
        }
        Rule::count_subquery_expr => {
            let match_pair = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::match_stmt)
                .ok_or_else(|| GqlError::parse_error("COUNT subquery requires a MATCH clause"))?;
            let pattern = super::build_match::build_match(match_pair)?;
            Ok(Expr::CountSubquery(Box::new(pattern)))
        }
        Rule::exists_expr => {
            let mut negated = false;
            let mut match_clause = None;
            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::not_kw => negated = true,
                    Rule::match_stmt => {
                        match_clause = Some(super::build_match::build_match(inner)?);
                    }
                    _ => {}
                }
            }
            let pattern = match_clause
                .ok_or_else(|| GqlError::parse_error("EXISTS requires a MATCH clause"))?;
            Ok(Expr::Exists {
                pattern: Box::new(pattern),
                negated,
            })
        }
        Rule::param_ref => {
            // $name -- strip the leading '$'
            let name = &pair.as_str()[1..];
            Ok(Expr::Parameter(IStr::new(name)))
        }
        Rule::trim_expr => {
            // TRIM([LEADING|TRAILING|BOTH] [char] FROM source)
            let mut spec = TrimSpec::Both;
            let mut character = None;
            let mut source = None;
            let mut found_from = false;
            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::trim_spec => {
                        spec = match inner.as_str().to_uppercase().as_str() {
                            s if s.contains("LEADING") => TrimSpec::Leading,
                            s if s.contains("TRAILING") => TrimSpec::Trailing,
                            _ => TrimSpec::Both,
                        };
                    }
                    Rule::trim_char if !found_from => {
                        character =
                            Some(Box::new(build_expr_depth(first_inner(inner)?, depth + 1)?));
                    }
                    _ => {
                        if inner.as_str().eq_ignore_ascii_case("FROM") {
                            found_from = true;
                        } else if source.is_none() {
                            source = Some(Box::new(build_expr_depth(inner, depth + 1)?));
                        }
                    }
                }
            }
            Ok(Expr::Trim {
                source: source
                    .ok_or_else(|| GqlError::parse_error("TRIM: missing source expression"))?,
                character,
                spec,
            })
        }
        Rule::record_constructor => {
            let mut fields = Vec::new();
            for field in pair
                .into_inner()
                .filter(|p| p.as_rule() == Rule::record_field)
            {
                let mut parts = field.into_inner();
                let key = intern_prop(parts.next().ok_or_else(|| {
                    GqlError::parse_error("unexpected parser state: missing key in record field")
                })?);
                let val = build_expr_depth(
                    parts.next().ok_or_else(|| {
                        GqlError::parse_error(
                            "unexpected parser state: missing value in record field",
                        )
                    })?,
                    depth + 1,
                )?;
                fields.push((key, Box::new(val)));
            }
            Ok(Expr::RecordConstruct(fields))
        }
        Rule::case_expr => build_case(pair, depth),
        Rule::cast_expr => build_cast(pair, depth),
        Rule::labels_expr => build_labels(pair, depth),
        Rule::paren_expr => {
            let inner = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::expr)
                .ok_or_else(|| {
                    GqlError::parse_error("expected expr in parenthesized expression")
                })?;
            build_expr_depth(inner, depth + 1)
        }
        rule => Err(unexpected_rule("expr", rule)),
    }
}

fn build_or_expr(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let mut parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    // Filter out or_kw tokens
    parts.retain(|p| p.as_rule() != Rule::or_kw);

    if parts.len() == 1 {
        return build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty OR expression")
            })?,
            depth + 1,
        );
    }

    let mut iter = parts.into_iter();
    let mut result = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: missing first operand in OR")
        })?,
        depth + 1,
    )?;
    for operand in iter {
        let right = build_expr_depth(operand, depth + 1)?;
        result = Expr::Logic(Box::new(result), LogicOp::Or, Box::new(right));
    }
    Ok(result)
}

fn build_xor_expr(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let mut parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    parts.retain(|p| p.as_rule() != Rule::xor_kw);

    if parts.len() == 1 {
        return build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty XOR expression")
            })?,
            depth + 1,
        );
    }

    let mut iter = parts.into_iter();
    let mut result = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: missing first operand in XOR")
        })?,
        depth + 1,
    )?;
    for operand in iter {
        let right = build_expr_depth(operand, depth + 1)?;
        result = Expr::Logic(Box::new(result), LogicOp::Xor, Box::new(right));
    }
    Ok(result)
}

fn build_and_expr(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let mut parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    parts.retain(|p| p.as_rule() != Rule::and_kw);

    if parts.len() == 1 {
        return build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty AND expression")
            })?,
            depth + 1,
        );
    }

    let mut iter = parts.into_iter();
    let mut result = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: missing first operand in AND")
        })?,
        depth + 1,
    )?;
    for operand in iter {
        let right = build_expr_depth(operand, depth + 1)?;
        result = Expr::Logic(Box::new(result), LogicOp::And, Box::new(right));
    }
    Ok(result)
}

fn build_not_expr(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    if parts.len() == 1 {
        build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty NOT expression")
            })?,
            depth + 1,
        )
    } else {
        // NOT ~ not_expr -- find the inner expression (skip not_kw)
        let inner = parts
            .into_iter()
            .find(|p| p.as_rule() != Rule::not_kw)
            .ok_or_else(|| GqlError::parse_error("expected expression after NOT"))?;
        Ok(Expr::Not(Box::new(build_expr_depth(inner, depth + 1)?)))
    }
}

fn build_is_expr(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let mut inner_iter = pair.into_inner();
    let comparison = build_expr_depth(
        inner_iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: missing comparison in IS expression")
        })?,
        depth + 1,
    )?;

    if let Some(suffix) = inner_iter.next()
        && suffix.as_rule() == Rule::is_suffix
    {
        // Dispatch on structural pest rules, not string matching.
        // This avoids misrouting when string literals contain keywords
        // (e.g., `x LIKE 'NULL_THING'` previously matched the NULL branch).
        let children: Vec<Pair<'_, Rule>> = suffix.clone().into_inner().collect();
        let first_rule = children.first().map(|c| c.as_rule());

        match first_rule {
            // ── LIKE (without NOT) ──
            Some(Rule::like_kw) => {
                let pattern_expr = children
                    .into_iter()
                    .find(|p| p.as_rule() != Rule::like_kw)
                    .ok_or_else(|| GqlError::parse_error("LIKE: missing pattern expression"))?;
                return Ok(Expr::Like {
                    expr: Box::new(comparison),
                    pattern: Box::new(build_expr_depth(pattern_expr, depth + 1)?),
                    negated: false,
                });
            }
            // ── BETWEEN (without NOT) ──
            Some(Rule::between_kw) => {
                let mut additions = children
                    .into_iter()
                    .filter(|p| p.as_rule() != Rule::between_kw && p.as_rule() != Rule::and_kw);
                let low_pair = additions
                    .next()
                    .ok_or_else(|| GqlError::parse_error("BETWEEN: missing lower bound"))?;
                let high_pair = additions
                    .next()
                    .ok_or_else(|| GqlError::parse_error("BETWEEN: missing upper bound"))?;
                return Ok(Expr::Between {
                    expr: Box::new(comparison),
                    low: Box::new(build_expr_depth(low_pair, depth + 1)?),
                    high: Box::new(build_expr_depth(high_pair, depth + 1)?),
                    negated: false,
                });
            }
            // ── string_match_op (STARTS WITH, ENDS WITH, CONTAINS) ──
            Some(Rule::string_match_op) => {
                let op_pair = &children[0];
                let op = match op_pair.as_str().to_uppercase().as_str() {
                    s if s.contains("STARTS") => StringMatchOp::StartsWith,
                    s if s.contains("ENDS") => StringMatchOp::EndsWith,
                    _ => StringMatchOp::Contains,
                };
                let right = build_expr_depth(
                    children.into_iter().nth(1).ok_or_else(|| {
                        GqlError::parse_error(
                            "unexpected parser state: missing operand in string match",
                        )
                    })?,
                    depth + 1,
                )?;
                return Ok(Expr::StringMatch(Box::new(comparison), op, Box::new(right)));
            }
            // ── NOT LIKE / NOT BETWEEN / NOT IN ──
            Some(Rule::not_kw) => {
                let second_rule = children.get(1).map(|c| c.as_rule());
                match second_rule {
                    Some(Rule::like_kw) => {
                        let pattern_expr = children
                            .into_iter()
                            .find(|p| p.as_rule() != Rule::not_kw && p.as_rule() != Rule::like_kw)
                            .ok_or_else(|| {
                                GqlError::parse_error("NOT LIKE: missing pattern expression")
                            })?;
                        return Ok(Expr::Like {
                            expr: Box::new(comparison),
                            pattern: Box::new(build_expr_depth(pattern_expr, depth + 1)?),
                            negated: true,
                        });
                    }
                    Some(Rule::between_kw) => {
                        let mut additions = children.into_iter().filter(|p| {
                            p.as_rule() != Rule::not_kw
                                && p.as_rule() != Rule::between_kw
                                && p.as_rule() != Rule::and_kw
                        });
                        let low_pair = additions.next().ok_or_else(|| {
                            GqlError::parse_error("NOT BETWEEN: missing lower bound")
                        })?;
                        let high_pair = additions.next().ok_or_else(|| {
                            GqlError::parse_error("NOT BETWEEN: missing upper bound")
                        })?;
                        return Ok(Expr::Between {
                            expr: Box::new(comparison),
                            low: Box::new(build_expr_depth(low_pair, depth + 1)?),
                            high: Box::new(build_expr_depth(high_pair, depth + 1)?),
                            negated: true,
                        });
                    }
                    Some(Rule::list_lit) => {
                        // not_kw ~ ^"IN" ~ list_lit -- anonymous IN keyword
                        let list = children
                            .into_iter()
                            .find(|p| p.as_rule() == Rule::list_lit)
                            .ok_or_else(|| {
                                GqlError::parse_error(
                                    "unexpected parser state: missing list in NOT IN",
                                )
                            })?;
                        let d = depth + 1;
                        let list_exprs: Vec<Expr> = list
                            .into_inner()
                            .map(|p| build_expr_depth(p, d))
                            .collect::<Result<_, _>>()?;
                        return Ok(Expr::InList {
                            expr: Box::new(comparison),
                            list: list_exprs,
                            negated: true,
                        });
                    }
                    _ => {
                        return Err(GqlError::parse_error(
                            "unexpected token after NOT in is_suffix",
                        ));
                    }
                }
            }
            // ── IS ... branches ──
            Some(Rule::is_kw) => {
                return build_is_kw_suffix(comparison, &children, depth);
            }
            // ── IN list_lit (without NOT) ──
            Some(Rule::list_lit) => {
                // ^"IN" is anonymous, first named child is list_lit
                let list = children.into_iter().next().ok_or_else(|| {
                    GqlError::parse_error("unexpected parser state: missing list in IN")
                })?;
                let d = depth + 1;
                let list_exprs: Vec<Expr> = list
                    .into_inner()
                    .map(|p| build_expr_depth(p, d))
                    .collect::<Result<_, _>>()?;
                return Ok(Expr::InList {
                    expr: Box::new(comparison),
                    list: list_exprs,
                    negated: false,
                });
            }
            _ => {
                return Err(GqlError::parse_error("unrecognized is_suffix form"));
            }
        }
    }

    Ok(comparison)
}

/// Sub-dispatch for IS-prefixed is_suffix alternatives.
/// Children start with is_kw, followed by optional not_kw, then distinguishing rules.
fn build_is_kw_suffix(
    comparison: Expr,
    children: &[Pair<'_, Rule>],
    depth: u16,
) -> Result<Expr, GqlError> {
    let has_not = children.iter().any(|c| c.as_rule() == Rule::not_kw);

    // Find the first "payload" child after is_kw and optional not_kw
    let payload = children
        .iter()
        .find(|c| !matches!(c.as_rule(), Rule::is_kw | Rule::not_kw));

    match payload.map(|p| p.as_rule()) {
        Some(Rule::null_kw) => Ok(Expr::IsNull {
            expr: Box::new(comparison),
            negated: has_not,
        }),
        Some(Rule::truth_value) => {
            let tv_str = payload.unwrap().as_str().to_uppercase();
            let value = if tv_str.contains("TRUE") {
                TruthValue::True
            } else if tv_str.contains("FALSE") {
                TruthValue::False
            } else {
                TruthValue::Unknown
            };
            Ok(Expr::IsTruthValue {
                expr: Box::new(comparison),
                value,
                negated: has_not,
            })
        }
        Some(Rule::normal_form) => {
            let nf_str = payload.unwrap().as_str().to_uppercase();
            let form = if nf_str.contains("NFKD") {
                NormalForm::NFKD
            } else if nf_str.contains("NFKC") {
                NormalForm::NFKC
            } else if nf_str.contains("NFD") {
                NormalForm::NFD
            } else {
                NormalForm::NFC
            };
            Ok(Expr::IsNormalized {
                expr: Box::new(comparison),
                form,
                negated: has_not,
            })
        }
        Some(Rule::type_name) => {
            let gql_type = parse_type_name(payload.unwrap().as_str())?;
            Ok(Expr::IsTyped {
                expr: Box::new(comparison),
                type_name: gql_type,
                negated: has_not,
            })
        }
        Some(Rule::label_or) => {
            let label = super::build_match::build_label_or(payload.unwrap().clone())?;
            Ok(Expr::IsLabeled {
                expr: Box::new(comparison),
                label,
                negated: has_not,
            })
        }
        Some(Rule::comparison) => {
            // IS [NOT] SOURCE OF or IS [NOT] DESTINATION OF
            // String dispatch is safe here: the pest grammar (grammar.pest:295-296)
            // guarantees the only two alternatives that reach this branch are
            // `is_kw not_kw? "SOURCE" "OF" comparison` and
            // `is_kw not_kw? "DESTINATION" "OF" comparison`.
            // Both parse as Rule::comparison payload, so we disambiguate by keyword text.
            let suffix_text = children
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(" ")
                .to_uppercase();
            let edge = build_expr_depth(payload.unwrap().clone(), depth + 1)?;
            if suffix_text.contains("SOURCE") {
                Ok(Expr::IsSourceOf {
                    node: Box::new(comparison),
                    edge: Box::new(edge),
                    negated: has_not,
                })
            } else {
                Ok(Expr::IsDestinationOf {
                    node: Box::new(comparison),
                    edge: Box::new(edge),
                    negated: has_not,
                })
            }
        }
        None => {
            // No payload child -- IS [NOT] DIRECTED or IS [NOT] NORMALIZED (default NFC)
            let suffix_text = children
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(" ")
                .to_uppercase();
            if suffix_text.contains("DIRECTED") {
                Ok(Expr::IsDirected {
                    expr: Box::new(comparison),
                    negated: has_not,
                })
            } else {
                // IS [NOT] NORMALIZED (default NFC, no explicit normal_form)
                Ok(Expr::IsNormalized {
                    expr: Box::new(comparison),
                    form: NormalForm::NFC,
                    negated: has_not,
                })
            }
        }
        _ => Err(GqlError::parse_error("unrecognized IS suffix")),
    }
}

fn build_comparison(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    if parts.len() == 1 {
        return build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty comparison")
            })?,
            depth + 1,
        );
    }

    // left comp_op right
    let mut iter = parts.into_iter();
    let left = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: missing left operand in comparison")
        })?,
        depth + 1,
    )?;
    let op_pair = iter.next().ok_or_else(|| {
        GqlError::parse_error("unexpected parser state: missing operator in comparison")
    })?;
    let right = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: missing right operand in comparison")
        })?,
        depth + 1,
    )?;

    let op = match op_pair.as_str() {
        "=" => CompareOp::Eq,
        "<>" => CompareOp::Neq,
        "<=" => CompareOp::Lte,
        ">=" => CompareOp::Gte,
        "<" => CompareOp::Lt,
        ">" => CompareOp::Gt,
        _ => return Err(GqlError::parse_error("unknown comparison operator")),
    };

    Ok(Expr::Compare(Box::new(left), op, Box::new(right)))
}

fn build_concat(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    if parts.len() == 1 {
        return build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty concat expression")
            })?,
            depth + 1,
        );
    }

    let mut iter = parts.into_iter();
    let mut result = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: missing first operand in concat")
        })?,
        depth + 1,
    )?;
    for operand in iter {
        let right = build_expr_depth(operand, depth + 1)?;
        result = Expr::Concat(Box::new(result), Box::new(right));
    }
    Ok(result)
}

fn build_addition(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    if parts.len() == 1 {
        return build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty addition expression")
            })?,
            depth + 1,
        );
    }

    let mut iter = parts.into_iter();
    let mut result = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: missing first operand in addition")
        })?,
        depth + 1,
    )?;
    while let Some(op_pair) = iter.next() {
        if op_pair.as_rule() == Rule::add_op {
            let right = build_expr_depth(
                iter.next().ok_or_else(|| {
                    GqlError::parse_error(
                        "unexpected parser state: missing right operand in addition",
                    )
                })?,
                depth + 1,
            )?;
            let op = match op_pair.as_str() {
                "+" => ArithOp::Add,
                "-" => ArithOp::Sub,
                _ => return Err(GqlError::parse_error("unknown add operator")),
            };
            result = Expr::Arithmetic(Box::new(result), op, Box::new(right));
        }
    }
    Ok(result)
}

fn build_multiplication(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    if parts.len() == 1 {
        return build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty multiplication expression")
            })?,
            depth + 1,
        );
    }

    let mut iter = parts.into_iter();
    let mut result = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error(
                "unexpected parser state: missing first operand in multiplication",
            )
        })?,
        depth + 1,
    )?;
    while let Some(op_pair) = iter.next() {
        if op_pair.as_rule() == Rule::mul_op {
            let right = build_expr_depth(
                iter.next().ok_or_else(|| {
                    GqlError::parse_error(
                        "unexpected parser state: missing right operand in multiplication",
                    )
                })?,
                depth + 1,
            )?;
            let op = match op_pair.as_str() {
                "*" => ArithOp::Mul,
                "/" => ArithOp::Div,
                "%" => ArithOp::Mod,
                _ => return Err(GqlError::parse_error("unknown mul operator")),
            };
            result = Expr::Arithmetic(Box::new(result), op, Box::new(right));
        }
    }
    Ok(result)
}

fn build_unary(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let parts: Vec<Pair<'_, Rule>> = pair.into_inner().collect();
    if parts.len() == 1 {
        return build_expr_depth(
            parts.into_iter().next().ok_or_else(|| {
                GqlError::parse_error("unexpected parser state: empty unary expression")
            })?,
            depth + 1,
        );
    }
    // sign_op ~ unary
    let sign = parts[0].as_str();
    let inner = parts.into_iter().last().ok_or_else(|| {
        GqlError::parse_error("unexpected parser state: missing operand in unary expression")
    })?;
    let expr = build_expr_depth(inner, depth + 1)?;
    match sign {
        "+" => Ok(expr), // unary plus is identity
        "-" => Ok(Expr::Negate(Box::new(expr))),
        _ => Err(GqlError::parse_error("unknown unary operator")),
    }
}

fn build_postfix(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let mut iter = pair.into_inner();
    let mut result = build_expr_depth(
        iter.next().ok_or_else(|| {
            GqlError::parse_error("unexpected parser state: empty postfix expression")
        })?,
        depth + 1,
    )?;

    for op in iter {
        if op.as_rule() == Rule::postfix_op {
            let inner = first_inner(op)?;
            match inner.as_rule() {
                Rule::temporal_prop_access => {
                    let mut prop_name = IStr::new("");
                    let mut timestamp = String::new();
                    for child in inner.into_inner() {
                        match child.as_rule() {
                            Rule::prop_ident => prop_name = intern_prop(child),
                            Rule::string_lit => {
                                let raw = child.as_str();
                                let inner_str = &raw[1..raw.len() - 1];
                                timestamp = unescape_string(inner_str);
                            }
                            _ => {}
                        }
                    }
                    result = Expr::TemporalProperty(Box::new(result), prop_name, timestamp);
                }
                Rule::prop_access => {
                    let prop = inner
                        .into_inner()
                        .find(|p| p.as_rule() == Rule::prop_ident)
                        .ok_or_else(|| {
                            GqlError::parse_error(
                                "unexpected parser state: missing property name in property access",
                            )
                        })?;
                    result = Expr::Property(Box::new(result), intern_prop(prop));
                }
                Rule::list_access_op => {
                    let index_expr = inner
                        .into_inner()
                        .find(|p| p.as_rule() == Rule::expr)
                        .ok_or_else(|| {
                            GqlError::parse_error(
                                "unexpected parser state: missing index expression in list access",
                            )
                        })?;
                    result = Expr::ListAccess(
                        Box::new(result),
                        Box::new(build_expr_depth(index_expr, depth + 1)?),
                    );
                }
                _ => {}
            }
        }
    }
    Ok(result)
}

fn build_primary(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let inner = first_inner(pair)?;
    build_expr_depth(inner, depth + 1)
}

// ── Aggregate ──────────────────────────────────────────────────────

fn build_aggregate(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let mut op = None;
    let mut expr = None;
    let mut count_star = false;
    let mut distinct = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::aggregate_op => {
                op = Some(match inner.as_str().to_uppercase().as_str() {
                    "COUNT" => AggregateOp::Count,
                    "SUM" => AggregateOp::Sum,
                    "AVG" | "AVERAGE" => AggregateOp::Avg,
                    "MIN" => AggregateOp::Min,
                    "MAX" => AggregateOp::Max,
                    "COLLECT_LIST" | "COLLECT" => AggregateOp::CollectList,
                    "STDDEV_SAMP" => AggregateOp::StddevSamp,
                    "STDDEV_POP" => AggregateOp::StddevPop,
                    _ => return Err(GqlError::parse_error("unknown aggregate function")),
                });
            }
            Rule::distinct_kw => distinct = true,
            Rule::star => count_star = true,
            _ => {
                if expr.is_none() {
                    expr = Some(build_expr_depth(inner, depth + 1)?);
                }
            }
        }
    }

    let agg_op = op.ok_or_else(|| GqlError::parse_error("expected aggregate operator"))?;

    if count_star {
        Ok(Expr::Function(FunctionCall {
            name: IStr::new("count"),
            args: vec![],
            count_star: true,
        }))
    } else {
        Ok(Expr::Aggregate(AggregateExpr {
            op: agg_op,
            expr: expr.map(Box::new),
            distinct,
        }))
    }
}

// ── Function call ──────────────────────────────────────────────────

fn build_function_call(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let mut name = None;
    let mut args = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::qualified_name => {
                let parts: Vec<&str> = inner.into_inner().map(|p| p.as_str()).collect();
                name = Some(IStr::new(&parts.join(".")));
            }
            Rule::arg_list => {
                for arg in inner.into_inner() {
                    args.push(build_expr_depth(arg, depth + 1)?);
                }
            }
            _ => {}
        }
    }

    Ok(Expr::Function(FunctionCall {
        name: name.ok_or_else(|| GqlError::parse_error("expected function name"))?,
        args,
        count_star: false,
    }))
}

// ── CASE ───────────────────────────────────────────────────────────

fn build_case(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    // case_expr = { simple_case | searched_case }
    let inner = first_inner(pair)?;
    match inner.as_rule() {
        Rule::simple_case => {
            // simple_case = { CASE ~ expr ~ simple_when+ ~ else_clause? ~ END }
            // Desugar: CASE x WHEN v THEN r → CASE WHEN x = v THEN r
            let mut parts = inner.into_inner();
            let operand = build_expr_depth(
                parts
                    .next()
                    .ok_or_else(|| GqlError::parse_error("expected CASE operand"))?,
                depth + 1,
            )?;
            let mut branches = Vec::new();
            let mut else_expr = None;
            for p in parts {
                match p.as_rule() {
                    Rule::simple_when => {
                        let mut wp = p.into_inner();
                        let when_val = build_expr_depth(
                            wp.next()
                                .ok_or_else(|| GqlError::parse_error("expected WHEN value"))?,
                            depth + 1,
                        )?;
                        let then_val = build_expr_depth(
                            wp.next()
                                .ok_or_else(|| GqlError::parse_error("expected THEN value"))?,
                            depth + 1,
                        )?;
                        // Desugar to: WHEN operand = when_val THEN then_val
                        let condition = Expr::Compare(
                            Box::new(operand.clone()),
                            CompareOp::Eq,
                            Box::new(when_val),
                        );
                        branches.push((condition, then_val));
                    }
                    Rule::else_clause => {
                        let e = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| GqlError::parse_error("expected ELSE expr"))?;
                        else_expr = Some(Box::new(build_expr_depth(e, depth + 1)?));
                    }
                    _ => {}
                }
            }
            Ok(Expr::Case {
                branches,
                else_expr,
            })
        }
        Rule::searched_case => {
            // searched_case = { CASE ~ when_clause+ ~ else_clause? ~ END }
            let mut branches = Vec::new();
            let mut else_expr = None;
            for p in inner.into_inner() {
                match p.as_rule() {
                    Rule::when_clause => {
                        let mut parts = p.into_inner();
                        let when_expr = build_expr_depth(
                            parts
                                .next()
                                .ok_or_else(|| GqlError::parse_error("expected WHEN expr"))?,
                            depth + 1,
                        )?;
                        let then_expr = build_expr_depth(
                            parts
                                .next()
                                .ok_or_else(|| GqlError::parse_error("expected THEN expr"))?,
                            depth + 1,
                        )?;
                        branches.push((when_expr, then_expr));
                    }
                    Rule::else_clause => {
                        let e = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| GqlError::parse_error("expected ELSE expr"))?;
                        else_expr = Some(Box::new(build_expr_depth(e, depth + 1)?));
                    }
                    _ => {}
                }
            }
            Ok(Expr::Case {
                branches,
                else_expr,
            })
        }
        _ => Err(GqlError::parse_error("expected simple or searched CASE")),
    }
}

fn build_cast(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let mut expr = None;
    let mut target_type = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::type_name => {
                target_type = Some(parse_type_name(inner.as_str())?);
            }
            _ => {
                if expr.is_none() && inner.as_rule() == Rule::expr {
                    expr = Some(build_expr_depth(inner, depth + 1)?);
                }
            }
        }
    }

    Ok(Expr::Cast(
        Box::new(expr.ok_or_else(|| GqlError::parse_error("expected expression in CAST"))?),
        target_type.ok_or_else(|| GqlError::parse_error("expected type in CAST"))?,
    ))
}

// ── LABELS ─────────────────────────────────────────────────────────

fn build_labels(pair: Pair<'_, Rule>, depth: u16) -> Result<Expr, GqlError> {
    let expr = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::expr)
        .ok_or_else(|| GqlError::parse_error("expected expression in LABELS"))?;
    Ok(Expr::Labels(Box::new(build_expr_depth(expr, depth + 1)?)))
}

// ── Literals ───────────────────────────────────────────────────────

pub(in crate::parser) fn build_literal(pair: Pair<'_, Rule>) -> Result<Expr, GqlError> {
    let inner = first_inner(pair)?;
    build_literal_value(inner)
}

fn build_literal_value(pair: Pair<'_, Rule>) -> Result<Expr, GqlError> {
    match pair.as_rule() {
        Rule::null_lit | Rule::unknown_lit => Ok(Expr::Literal(GqlValue::Null)),
        Rule::bool_lit => {
            let val = pair.as_str().eq_ignore_ascii_case("TRUE");
            Ok(Expr::Literal(GqlValue::Bool(val)))
        }
        Rule::int_lit => {
            let s = pair.as_str().replace('_', "");
            let val: i64 = s
                .parse()
                .map_err(|_| GqlError::parse_error(format!("invalid integer: {s}")))?;
            Ok(Expr::Literal(GqlValue::Int(val)))
        }
        Rule::uint_lit => {
            let s = pair
                .as_str()
                .replace('_', "")
                .trim_end_matches('u')
                .to_string();
            let val: u64 = s
                .parse()
                .map_err(|_| GqlError::parse_error(format!("invalid unsigned integer: {s}")))?;
            Ok(Expr::Literal(GqlValue::UInt(val)))
        }
        Rule::float_lit => {
            let s = pair
                .as_str()
                .replace('_', "")
                .trim_end_matches(['f', 'd'])
                .to_string();
            let val: f64 = s
                .parse()
                .map_err(|_| GqlError::parse_error(format!("invalid float: {s}")))?;
            Ok(Expr::Literal(GqlValue::Float(val)))
        }
        Rule::hex_lit => {
            let s = pair.as_str().replace('_', "");
            // Strip optional sign and 0x prefix
            let (neg, digits) = if s.starts_with('-') {
                (true, &s[3..]) // skip -0x
            } else if s.starts_with('+') {
                (false, &s[3..]) // skip +0x
            } else {
                (false, &s[2..]) // skip 0x
            };
            let val = i64::from_str_radix(digits, 16)
                .map_err(|_| GqlError::parse_error(format!("invalid hex integer: {s}")))?;
            Ok(Expr::Literal(GqlValue::Int(if neg { -val } else { val })))
        }
        Rule::oct_lit => {
            let s = pair.as_str().replace('_', "");
            let (neg, digits) = if s.starts_with('-') {
                (true, &s[3..])
            } else if s.starts_with('+') {
                (false, &s[3..])
            } else {
                (false, &s[2..])
            };
            let val = i64::from_str_radix(digits, 8)
                .map_err(|_| GqlError::parse_error(format!("invalid octal integer: {s}")))?;
            Ok(Expr::Literal(GqlValue::Int(if neg { -val } else { val })))
        }
        Rule::bin_lit => {
            let s = pair.as_str().replace('_', "");
            let (neg, digits) = if s.starts_with('-') {
                (true, &s[3..])
            } else if s.starts_with('+') {
                (false, &s[3..])
            } else {
                (false, &s[2..])
            };
            let val = i64::from_str_radix(digits, 2)
                .map_err(|_| GqlError::parse_error(format!("invalid binary integer: {s}")))?;
            Ok(Expr::Literal(GqlValue::Int(if neg { -val } else { val })))
        }
        Rule::string_lit => {
            let raw = pair.as_str();
            // Remove surrounding quotes (single-quote only after spec alignment)
            let inner = &raw[1..raw.len() - 1];
            let unescaped = unescape_string(inner);
            Ok(Expr::Literal(GqlValue::String(SmolStr::new(
                unescaped.as_str(),
            ))))
        }
        Rule::list_lit => {
            let elements: Vec<Expr> = pair
                .into_inner()
                .map(build_expr)
                .collect::<Result<_, _>>()?;
            if elements.is_empty() {
                Ok(Expr::Literal(GqlValue::List(
                    crate::types::value::GqlList::empty(),
                )))
            } else {
                Ok(Expr::ListConstruct(elements))
            }
        }
        Rule::duration_lit => {
            let string_pair = pair
                .into_inner()
                .find(|p| p.as_rule() == Rule::string_lit)
                .ok_or_else(|| GqlError::parse_error("expected string in DURATION"))?;
            let raw = string_pair.as_str();
            let inner_str = &raw[1..raw.len() - 1];
            Ok(Expr::Function(FunctionCall {
                name: IStr::new("duration"),
                args: vec![Expr::Literal(GqlValue::String(SmolStr::new(inner_str)))],
                count_star: false,
            }))
        }
        // Temporal keyword literals (spec §21.2)
        Rule::date_lit => {
            let s = extract_string_from_temporal_lit(pair)?;
            Ok(Expr::Function(FunctionCall {
                name: IStr::new("date"),
                args: vec![Expr::Literal(GqlValue::String(SmolStr::new(&s)))],
                count_star: false,
            }))
        }
        Rule::zoned_datetime_lit => {
            let s = extract_string_from_temporal_lit(pair)?;
            Ok(Expr::Function(FunctionCall {
                name: IStr::new("zoned_datetime"),
                args: vec![Expr::Literal(GqlValue::String(SmolStr::new(&s)))],
                count_star: false,
            }))
        }
        Rule::local_datetime_lit => {
            let s = extract_string_from_temporal_lit(pair)?;
            Ok(Expr::Function(FunctionCall {
                name: IStr::new("local_datetime"),
                args: vec![Expr::Literal(GqlValue::String(SmolStr::new(&s)))],
                count_star: false,
            }))
        }
        Rule::time_lit | Rule::local_time_lit => {
            let s = extract_string_from_temporal_lit(pair)?;
            Ok(Expr::Function(FunctionCall {
                name: IStr::new("time"),
                args: vec![Expr::Literal(GqlValue::String(SmolStr::new(&s)))],
                count_star: false,
            }))
        }
        Rule::zoned_time_lit => {
            let s = extract_string_from_temporal_lit(pair)?;
            Ok(Expr::Function(FunctionCall {
                name: IStr::new("zoned_time"),
                args: vec![Expr::Literal(GqlValue::String(SmolStr::new(&s)))],
                count_star: false,
            }))
        }
        Rule::datetime_bare_lit => {
            let s = extract_string_from_temporal_lit(pair)?;
            // Infer zoned vs local from content: if contains 'Z', '+', or '-' offset → zoned
            let is_zoned = s.contains('Z') || s.contains('+') || (s.matches('-').count() > 2); // date has 2 dashes; a 3rd means tz offset
            let func = if is_zoned {
                "zoned_datetime"
            } else {
                "local_datetime"
            };
            Ok(Expr::Function(FunctionCall {
                name: IStr::new(func),
                args: vec![Expr::Literal(GqlValue::String(SmolStr::new(&s)))],
                count_star: false,
            }))
        }
        rule => Err(unexpected_rule("literal", rule)),
    }
}

/// Extract the string content from a temporal literal (keyword + string_lit).
fn extract_string_from_temporal_lit(pair: Pair<'_, Rule>) -> Result<std::string::String, GqlError> {
    let string_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::string_lit)
        .ok_or_else(|| GqlError::parse_error("expected string in temporal literal"))?;
    let raw = string_pair.as_str();
    Ok(unescape_string(&raw[1..raw.len() - 1]))
}

// ── String unescaping ──────────────────────────────────────────────

pub(in crate::parser) fn unescape_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('\'') => result.push('\''),
                Some('"') => result.push('"'),
                Some('`') => result.push('`'),
                Some('b') => result.push('\u{0008}'),
                Some('f') => result.push('\u{000C}'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    if let Ok(cp) = u32::from_str_radix(&hex, 16)
                        && let Some(ch) = char::from_u32(cp)
                    {
                        result.push(ch);
                    }
                }
                Some('U') => {
                    let hex: String = chars.by_ref().take(8).collect();
                    if let Ok(cp) = u32::from_str_radix(&hex, 16)
                        && let Some(ch) = char::from_u32(cp)
                    {
                        result.push(ch);
                    }
                }
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else if c == '\'' {
            // '' escape (two single-quotes = literal single-quote)
            if chars.clone().next() == Some('\'') {
                chars.next(); // consume the second '
                result.push('\'');
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }
    result
}

// ── Type name parsing ─────────────────────────────────────────────

pub(in crate::parser) fn parse_type_name(s: &str) -> Result<GqlType, GqlError> {
    // Normalize whitespace for multi-word types (pest may produce variable spacing)
    let normalized: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    let upper = normalized.to_uppercase();

    // Parser-only types not in ValueType
    match upper.as_str() {
        "PATH" => return Ok(GqlType::Path),
        "NULL" => return Ok(GqlType::Null),
        "NOTHING" => return Ok(GqlType::Nothing),
        "RECORD" => return Ok(GqlType::Record),
        "NODE" => return Ok(GqlType::Node),
        "EDGE" | "RELATIONSHIP" => return Ok(GqlType::Edge),
        _ => {}
    }
    // Contains-based for multi-word parser-only types
    if upper.contains("ZONED") && upper.contains("TIME") && !upper.contains("DATETIME") {
        return Ok(GqlType::ZonedTime);
    }
    if upper.contains("LOCAL") && upper.contains("TIME") && !upper.contains("DATETIME") {
        return Ok(GqlType::LocalTime);
    }
    // Reject unsupported widths with specific error
    if upper.contains("128") || upper.contains("256") {
        return Err(GqlError::parse_error(format!(
            "unsupported type width: {s}"
        )));
    }
    // Delegate to shared ValueType::from_str for storage types
    let vt: selene_core::ValueType = s.parse().map_err(|e: String| GqlError::parse_error(e))?;
    Ok(GqlType::from(vt))
}
