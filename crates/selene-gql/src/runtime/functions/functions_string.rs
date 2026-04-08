//! String, collection, type-checking, and list scalar functions.

use std::sync::Arc;

use smol_str::SmolStr;

use super::{EvalContext, GqlError, GqlValue, ScalarFunction};
use crate::types::value::{GqlList, GqlType};

// ── String functions ──────────────────────────────────────────────

pub(crate) struct ReplaceFunction;
impl ScalarFunction for ReplaceFunction {
    fn name(&self) -> &'static str {
        "replace"
    }
    fn description(&self) -> &'static str {
        "Replace all occurrences: replace(str, search, replacement)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match (args.first(), args.get(1), args.get(2)) {
            (
                Some(GqlValue::String(s)),
                Some(GqlValue::String(search)),
                Some(GqlValue::String(repl)),
            ) => Ok(GqlValue::String(SmolStr::new(
                s.replace(search.as_str(), repl.as_str()),
            ))),
            _ if args.iter().any(|a| a.is_null()) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error(
                "replace() requires (string, string, string)",
            )),
        }
    }
}

pub(crate) struct ReverseFunction;
impl ScalarFunction for ReverseFunction {
    fn name(&self) -> &'static str {
        "reverse"
    }
    fn description(&self) -> &'static str {
        "Reverse a string or list"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => Ok(GqlValue::String(SmolStr::new(
                s.chars().rev().collect::<String>(),
            ))),
            Some(GqlValue::List(list)) => {
                let reversed: Vec<GqlValue> = list.elements.iter().rev().cloned().collect();
                Ok(GqlValue::List(GqlList {
                    element_type: list.element_type.clone(),
                    elements: Arc::from(reversed),
                }))
            }
            Some(GqlValue::Null) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("reverse() requires a string or list")),
        }
    }
}

pub(crate) struct SubstringFunction;
impl ScalarFunction for SubstringFunction {
    fn name(&self) -> &'static str {
        "substring"
    }
    fn description(&self) -> &'static str {
        "Extract substring: substring(str, start, length?)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match (args.first(), args.get(1)) {
            (Some(GqlValue::String(s)), Some(GqlValue::Int(start))) => {
                let start = (*start).max(0) as usize;
                let len = args.get(2).and_then(|v| {
                    if let GqlValue::Int(n) = v {
                        Some(*n as usize)
                    } else {
                        None
                    }
                });
                let chars: Vec<char> = s.chars().collect();
                let start = start.min(chars.len());
                let end = match len {
                    Some(l) => (start + l).min(chars.len()),
                    None => chars.len(),
                };
                Ok(GqlValue::String(SmolStr::new(
                    chars[start..end].iter().collect::<String>(),
                )))
            }
            (Some(GqlValue::Null), _) | (_, Some(GqlValue::Null)) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error(
                "substring() requires (string, int, int?)",
            )),
        }
    }
}

pub(crate) struct ToStringFunction;
impl ScalarFunction for ToStringFunction {
    fn name(&self) -> &'static str {
        "to_string"
    }
    fn description(&self) -> &'static str {
        "Convert any value to string"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => Ok(GqlValue::String(s.clone())),
            Some(GqlValue::Int(i)) => Ok(GqlValue::String(SmolStr::new(i.to_string()))),
            Some(GqlValue::UInt(u)) => Ok(GqlValue::String(SmolStr::new(u.to_string()))),
            Some(GqlValue::Float(f)) => Ok(GqlValue::String(SmolStr::new(f.to_string()))),
            Some(GqlValue::Bool(b)) => Ok(GqlValue::String(SmolStr::new(if *b {
                "true"
            } else {
                "false"
            }))),
            Some(GqlValue::Null) => Ok(GqlValue::String(SmolStr::new("null"))),
            _ => Ok(GqlValue::String(SmolStr::new("<complex>"))),
        }
    }
}

// ── Type conversion functions (Cypher-compatible camelCase names) ────

pub(crate) struct ToStringCypherFunction;
impl ScalarFunction for ToStringCypherFunction {
    fn name(&self) -> &'static str {
        "tostring"
    }
    fn description(&self) -> &'static str {
        "Convert any value to its string representation; Null returns Null"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => Ok(GqlValue::String(s.clone())),
            Some(GqlValue::Int(i)) => Ok(GqlValue::String(SmolStr::new(i.to_string()))),
            Some(GqlValue::UInt(u)) => Ok(GqlValue::String(SmolStr::new(u.to_string()))),
            Some(GqlValue::Float(f)) => Ok(GqlValue::String(SmolStr::new(f.to_string()))),
            Some(GqlValue::Bool(b)) => Ok(GqlValue::String(SmolStr::new(if *b {
                "true"
            } else {
                "false"
            }))),
            _ => Ok(GqlValue::String(SmolStr::new("<complex>"))),
        }
    }
}

pub(crate) struct ToIntegerFunction;
impl ScalarFunction for ToIntegerFunction {
    fn name(&self) -> &'static str {
        "tointeger"
    }
    fn description(&self) -> &'static str {
        "Convert a value to integer; returns Null on parse failure or Null input"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::Int(i)) => Ok(GqlValue::Int(*i)),
            Some(GqlValue::UInt(u)) => Ok(GqlValue::Int(*u as i64)),
            Some(GqlValue::Float(f)) => Ok(GqlValue::Int(*f as i64)),
            Some(GqlValue::Bool(b)) => Ok(GqlValue::Int(i64::from(*b))),
            Some(GqlValue::String(s)) => match s.trim().parse::<i64>() {
                Ok(n) => Ok(GqlValue::Int(n)),
                Err(_) => Ok(GqlValue::Null),
            },
            _ => Ok(GqlValue::Null),
        }
    }
}

pub(crate) struct ToFloatFunction;
impl ScalarFunction for ToFloatFunction {
    fn name(&self) -> &'static str {
        "tofloat"
    }
    fn description(&self) -> &'static str {
        "Convert a value to float; returns Null on parse failure or Null input"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::Float(f)) => Ok(GqlValue::Float(*f)),
            Some(GqlValue::Int(i)) => Ok(GqlValue::Float(*i as f64)),
            Some(GqlValue::UInt(u)) => Ok(GqlValue::Float(*u as f64)),
            Some(GqlValue::Bool(b)) => Ok(GqlValue::Float(if *b { 1.0 } else { 0.0 })),
            Some(GqlValue::String(s)) => match s.trim().parse::<f64>() {
                Ok(n) => Ok(GqlValue::Float(n)),
                Err(_) => Ok(GqlValue::Null),
            },
            _ => Ok(GqlValue::Null),
        }
    }
}

// ── Type checking functions ───────────────────────────────────────

pub(crate) struct ValueTypeFunction;
impl ScalarFunction for ValueTypeFunction {
    fn name(&self) -> &'static str {
        "value_type"
    }
    fn description(&self) -> &'static str {
        "Return the type name of a value"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let name = match args.first() {
            Some(GqlValue::Null) | None => "NULL",
            Some(GqlValue::Bool(_)) => "BOOL",
            Some(GqlValue::Int(_)) => "INT",
            Some(GqlValue::UInt(_)) => "UINT",
            Some(GqlValue::Float(_)) => "FLOAT",
            Some(GqlValue::String(_)) => "STRING",
            Some(GqlValue::Node(_)) => "NODE",
            Some(GqlValue::Edge(_)) => "EDGE",
            Some(GqlValue::Path(_)) => "PATH",
            Some(GqlValue::List(_)) => "LIST",
            Some(GqlValue::ZonedDateTime(_)) => "ZONED_DATETIME",
            Some(GqlValue::Date(_)) => "DATE",
            Some(GqlValue::LocalDateTime(_)) => "LOCAL_DATETIME",
            Some(GqlValue::ZonedTime(_)) => "ZONED_TIME",
            Some(GqlValue::LocalTime(_)) => "LOCAL_TIME",
            Some(GqlValue::Duration(_)) => "DURATION",
            Some(GqlValue::Bytes(_)) => "BYTES",
            Some(GqlValue::Vector(_)) => "VECTOR",
            Some(GqlValue::Record(_)) => "RECORD",
        };
        Ok(GqlValue::String(SmolStr::new(name)))
    }
}

// ── Collection functions ──────────────────────────────────────────

pub(crate) struct HeadFunction;
impl ScalarFunction for HeadFunction {
    fn name(&self) -> &'static str {
        "head"
    }
    fn description(&self) -> &'static str {
        "First element of a list"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::List(list)) => {
                Ok(list.elements.first().cloned().unwrap_or(GqlValue::Null))
            }
            Some(GqlValue::Null) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("head() requires a list")),
        }
    }
}

pub(crate) struct TailFunction;
impl ScalarFunction for TailFunction {
    fn name(&self) -> &'static str {
        "tail"
    }
    fn description(&self) -> &'static str {
        "All elements except the first"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::List(list)) => {
                let tail: Vec<GqlValue> = if list.elements.is_empty() {
                    vec![]
                } else {
                    list.elements[1..].to_vec()
                };
                Ok(GqlValue::List(GqlList {
                    element_type: list.element_type.clone(),
                    elements: Arc::from(tail),
                }))
            }
            Some(GqlValue::Null) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("tail() requires a list")),
        }
    }
}

pub(crate) struct LastFunction;
impl ScalarFunction for LastFunction {
    fn name(&self) -> &'static str {
        "last"
    }
    fn description(&self) -> &'static str {
        "Last element of a list"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::List(list)) => {
                Ok(list.elements.last().cloned().unwrap_or(GqlValue::Null))
            }
            Some(GqlValue::Null) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("last() requires a list")),
        }
    }
}

pub(crate) struct RangeFunction;
impl ScalarFunction for RangeFunction {
    fn name(&self) -> &'static str {
        "range"
    }
    fn description(&self) -> &'static str {
        "Generate integer list: range(start, end, step?)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let start = match args.first() {
            Some(GqlValue::Int(i)) => *i,
            _ => return Err(GqlError::type_error("range() requires integers")),
        };
        let end = match args.get(1) {
            Some(GqlValue::Int(i)) => *i,
            _ => return Err(GqlError::type_error("range() requires integers")),
        };
        let step = match args.get(2) {
            Some(GqlValue::Int(0)) => {
                return Err(GqlError::InvalidArgument {
                    message: "step cannot be 0".into(),
                });
            }
            Some(GqlValue::Int(i)) => *i,
            None => 1,
            _ => return Err(GqlError::type_error("step must be integer")),
        };
        let mut values = Vec::new();
        let mut i = start;
        while (step > 0 && i <= end) || (step < 0 && i >= end) {
            values.push(GqlValue::Int(i));
            if values.len() >= 100_000 {
                break;
            }
            i += step;
        }
        Ok(GqlValue::List(GqlList {
            element_type: GqlType::Int,
            elements: Arc::from(values),
        }))
    }
}

pub(crate) struct KeysFunction;
impl ScalarFunction for KeysFunction {
    fn name(&self) -> &'static str {
        "keys"
    }
    fn description(&self) -> &'static str {
        "Property keys of a node or edge"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Node(id)) => {
                let names: Vec<GqlValue> = ctx
                    .graph
                    .get_node(*id)
                    .map(|n| {
                        n.properties
                            .iter()
                            .map(|(k, _)| GqlValue::String(SmolStr::new(k.as_str())))
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(GqlValue::List(GqlList {
                    element_type: GqlType::String,
                    elements: Arc::from(names),
                }))
            }
            Some(GqlValue::Edge(id)) => {
                let names: Vec<GqlValue> = ctx
                    .graph
                    .get_edge(*id)
                    .map(|e| {
                        e.properties
                            .iter()
                            .map(|(k, _)| GqlValue::String(SmolStr::new(k.as_str())))
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(GqlValue::List(GqlList {
                    element_type: GqlType::String,
                    elements: Arc::from(names),
                }))
            }
            Some(GqlValue::Null) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("keys() requires a node or edge")),
        }
    }
}

// ── String/null functions ────────────────────────────────────────

pub(crate) struct NullIfFunction;
impl ScalarFunction for NullIfFunction {
    fn name(&self) -> &'static str {
        "nullif"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let a = args.first().ok_or_else(|| GqlError::InvalidArgument {
            message: "nullif() requires 2 args".into(),
        })?;
        let b = args.get(1).ok_or_else(|| GqlError::InvalidArgument {
            message: "nullif() requires 2 args".into(),
        })?;
        if a.gql_eq(b).is_true() {
            Ok(GqlValue::Null)
        } else {
            Ok(a.clone())
        }
    }
}

pub(crate) struct LeftFunction;
impl ScalarFunction for LeftFunction {
    fn name(&self) -> &'static str {
        "left"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match (args.first(), args.get(1)) {
            (Some(GqlValue::String(s)), Some(GqlValue::Int(n))) => {
                let n = (*n).max(0) as usize;
                Ok(GqlValue::String(SmolStr::new(
                    s.chars().take(n).collect::<String>(),
                )))
            }
            (Some(GqlValue::Null), _) | (_, Some(GqlValue::Null)) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("left(string, int)")),
        }
    }
}

pub(crate) struct RightFunction;
impl ScalarFunction for RightFunction {
    fn name(&self) -> &'static str {
        "right"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match (args.first(), args.get(1)) {
            (Some(GqlValue::String(s)), Some(GqlValue::Int(n))) => {
                let n = (*n).max(0) as usize;
                let chars: Vec<char> = s.chars().collect();
                let start = chars.len().saturating_sub(n);
                Ok(GqlValue::String(SmolStr::new(
                    chars[start..].iter().collect::<String>(),
                )))
            }
            (Some(GqlValue::Null), _) | (_, Some(GqlValue::Null)) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("right(string, int)")),
        }
    }
}

pub(crate) struct LtrimFunction;
impl ScalarFunction for LtrimFunction {
    fn name(&self) -> &'static str {
        "ltrim"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => Ok(GqlValue::String(SmolStr::new(s.trim_start()))),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("ltrim requires a string")),
        }
    }
}

pub(crate) struct RtrimFunction;
impl ScalarFunction for RtrimFunction {
    fn name(&self) -> &'static str {
        "rtrim"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => Ok(GqlValue::String(SmolStr::new(s.trim_end()))),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("rtrim requires a string")),
        }
    }
}

pub(crate) struct StartsWithFunction;
impl ScalarFunction for StartsWithFunction {
    fn name(&self) -> &'static str {
        "starts_with"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match (args.first(), args.get(1)) {
            (Some(GqlValue::String(s)), Some(GqlValue::String(prefix))) => {
                Ok(GqlValue::Bool(s.starts_with(prefix.as_str())))
            }
            (Some(GqlValue::Null), _) | (_, Some(GqlValue::Null)) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("starts_with(string, string)")),
        }
    }
}

pub(crate) struct EndsWithFunction;
impl ScalarFunction for EndsWithFunction {
    fn name(&self) -> &'static str {
        "ends_with"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match (args.first(), args.get(1)) {
            (Some(GqlValue::String(s)), Some(GqlValue::String(suffix))) => {
                Ok(GqlValue::Bool(s.ends_with(suffix.as_str())))
            }
            (Some(GqlValue::Null), _) | (_, Some(GqlValue::Null)) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("ends_with(string, string)")),
        }
    }
}

pub(crate) struct ContainsFn;
impl ScalarFunction for ContainsFn {
    fn name(&self) -> &'static str {
        "contains"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match (args.first(), args.get(1)) {
            (Some(GqlValue::String(s)), Some(GqlValue::String(sub))) => {
                Ok(GqlValue::Bool(s.contains(sub.as_str())))
            }
            (Some(GqlValue::Null), _) | (_, Some(GqlValue::Null)) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("contains(string, string)")),
        }
    }
}

// ── List functions ──────────────────────────────────────────────

pub(crate) struct ListContainsFunction;
impl ScalarFunction for ListContainsFunction {
    fn name(&self) -> &'static str {
        "list_contains"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match (args.first(), args.get(1)) {
            (Some(GqlValue::List(l)), Some(elem)) => Ok(GqlValue::Bool(
                l.elements.iter().any(|e| e.gql_eq(elem).is_true()),
            )),
            (Some(GqlValue::Null), _) => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("list_contains(list, element)")),
        }
    }
}

pub(crate) struct ListSliceFunction;
impl ScalarFunction for ListSliceFunction {
    fn name(&self) -> &'static str {
        "list_slice"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let list = match args.first() {
            Some(GqlValue::List(l)) => l,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("list_slice(list, from, to)")),
        };
        let from = match args.get(1) {
            Some(GqlValue::Int(n)) => (*n).max(0) as usize,
            _ => 0,
        };
        let to = match args.get(2) {
            Some(GqlValue::Int(n)) => (*n).max(0) as usize,
            _ => list.len(),
        };
        let to = to.min(list.len());
        let from = from.min(to);
        let sliced: Vec<GqlValue> = list.elements[from..to].to_vec();
        Ok(GqlValue::List(GqlList {
            element_type: list.element_type.clone(),
            elements: Arc::from(sliced),
        }))
    }
}

pub(crate) struct ListAppendFunction;
impl ScalarFunction for ListAppendFunction {
    fn name(&self) -> &'static str {
        "list_append"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let list = match args.first() {
            Some(GqlValue::List(l)) => l,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("list_append(list, element)")),
        };
        let elem = args.get(1).cloned().unwrap_or(GqlValue::Null);
        let mut elems: Vec<GqlValue> = list.elements.to_vec();
        elems.push(elem);
        Ok(GqlValue::List(GqlList {
            element_type: list.element_type.clone(),
            elements: Arc::from(elems),
        }))
    }
}

pub(crate) struct ListPrependFunction;
impl ScalarFunction for ListPrependFunction {
    fn name(&self) -> &'static str {
        "list_prepend"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let list = match args.first() {
            Some(GqlValue::List(l)) => l,
            Some(GqlValue::Null) => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("list_prepend(list, element)")),
        };
        let elem = args.get(1).cloned().unwrap_or(GqlValue::Null);
        let mut elems = vec![elem];
        elems.extend(list.elements.iter().cloned());
        Ok(GqlValue::List(GqlList {
            element_type: list.element_type.clone(),
            elements: Arc::from(elems),
        }))
    }
}

pub(crate) struct ListLengthFunction;
impl ScalarFunction for ListLengthFunction {
    fn name(&self) -> &'static str {
        "list_length"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::List(l)) => Ok(GqlValue::Int(l.len() as i64)),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("list_length requires a list")),
        }
    }
}

pub(crate) struct ListReverseFunction;
impl ScalarFunction for ListReverseFunction {
    fn name(&self) -> &'static str {
        "list_reverse"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::List(l)) => {
                let mut elems: Vec<GqlValue> = l.elements.to_vec();
                elems.reverse();
                Ok(GqlValue::List(GqlList {
                    element_type: l.element_type.clone(),
                    elements: Arc::from(elems),
                }))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("list_reverse requires a list")),
        }
    }
}

pub(crate) struct ListSortFunction;
impl ScalarFunction for ListSortFunction {
    fn name(&self) -> &'static str {
        "list_sort"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::List(l)) => {
                let mut elems: Vec<GqlValue> = l.elements.to_vec();
                elems.sort_by(|a, b| a.sort_order(b));
                Ok(GqlValue::List(GqlList {
                    element_type: l.element_type.clone(),
                    elements: Arc::from(elems),
                }))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("list_sort requires a list")),
        }
    }
}

// ── Length alias ──────────────────────────────────────────────────

pub(crate) struct LengthFunction;
impl ScalarFunction for LengthFunction {
    fn name(&self) -> &'static str {
        "length"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::List(l)) => Ok(GqlValue::Int(l.len() as i64)),
            Some(GqlValue::String(s)) => Ok(GqlValue::Int(s.chars().count() as i64)),
            Some(GqlValue::Path(p)) => Ok(GqlValue::Int(p.edge_count() as i64)),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error(
                "length() requires a list, string, or path",
            )),
        }
    }
}

// ── Normalize function ─────────────────────────────────────────────

pub(crate) struct NormalizeFunction;
impl ScalarFunction for NormalizeFunction {
    fn name(&self) -> &'static str {
        "normalize"
    }
    fn description(&self) -> &'static str {
        "Unicode normalization (NFC/NFD/NFKC/NFKD)"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        use unicode_normalization::UnicodeNormalization;
        let s = match args.first() {
            Some(GqlValue::String(s)) => {
                if s.len() > 1_000_000 {
                    return Err(GqlError::ResourcesExhausted {
                        message: "normalize: input exceeds 1MB limit".into(),
                    });
                }
                s.as_str()
            }
            Some(GqlValue::Null) | None => return Ok(GqlValue::Null),
            _ => return Err(GqlError::type_error("normalize() requires a string")),
        };
        let form = args
            .get(1)
            .and_then(|v| match v {
                GqlValue::String(s) => Some(s.as_str()),
                _ => None,
            })
            .unwrap_or("NFC");
        let result: String = match form.to_uppercase().as_str() {
            "NFC" => s.nfc().collect(),
            "NFD" => s.nfd().collect(),
            "NFKC" => s.nfkc().collect(),
            "NFKD" => s.nfkd().collect(),
            _ => {
                return Err(GqlError::type_error(format!(
                    "unknown normalization form: {form}"
                )));
            }
        };
        Ok(GqlValue::String(SmolStr::new(&result)))
    }
}

// ── Double conversion ──────────────────────────────────────────────

pub(crate) struct DoubleFunction;
impl ScalarFunction for DoubleFunction {
    fn name(&self) -> &'static str {
        "double"
    }
    fn description(&self) -> &'static str {
        "Convert to DOUBLE"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(v) => Ok(GqlValue::Float(v.as_float()?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::functions::FunctionRegistry;
    use crate::types::value::{GqlList, GqlType};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;
    use std::sync::Arc;

    fn ctx() -> (SeleneGraph, FunctionRegistry) {
        (SeleneGraph::new(), FunctionRegistry::with_builtins())
    }

    fn s(val: &str) -> GqlValue {
        GqlValue::String(SmolStr::new(val))
    }

    fn int_list(vals: &[i64]) -> GqlValue {
        GqlValue::List(GqlList {
            element_type: GqlType::Int,
            elements: Arc::from(vals.iter().map(|v| GqlValue::Int(*v)).collect::<Vec<_>>()),
        })
    }

    // ── SubstringFunction ──

    #[test]
    fn substring_with_start_and_length() {
        let f = SubstringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // 0-indexed: substring("hello", 1, 3) = "ell"
        let r = f
            .invoke(&[s("hello"), GqlValue::Int(1), GqlValue::Int(3)], &c)
            .unwrap();
        assert_eq!(r, s("ell"));
    }

    #[test]
    fn substring_start_zero() {
        let f = SubstringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[s("hello"), GqlValue::Int(0), GqlValue::Int(2)], &c)
            .unwrap();
        assert_eq!(r, s("he"));
    }

    #[test]
    fn substring_no_length_returns_rest() {
        let f = SubstringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[s("hello"), GqlValue::Int(2)], &c).unwrap();
        assert_eq!(r, s("llo"));
    }

    #[test]
    fn substring_negative_start_clamps_to_zero() {
        let f = SubstringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[s("hello"), GqlValue::Int(-5), GqlValue::Int(3)], &c)
            .unwrap();
        assert_eq!(r, s("hel"));
    }

    #[test]
    fn substring_length_exceeds_string() {
        let f = SubstringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[s("hi"), GqlValue::Int(0), GqlValue::Int(100)], &c)
            .unwrap();
        assert_eq!(r, s("hi"));
    }

    #[test]
    fn substring_null_returns_null() {
        let f = SubstringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Null, GqlValue::Int(0)], &c).unwrap(),
            GqlValue::Null
        );
    }

    // ── LeftFunction / RightFunction ──

    #[test]
    fn left_basic() {
        let f = LeftFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), GqlValue::Int(3)], &c).unwrap(),
            s("hel")
        );
    }

    #[test]
    fn left_zero_returns_empty() {
        let f = LeftFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), GqlValue::Int(0)], &c).unwrap(),
            s("")
        );
    }

    #[test]
    fn left_exceeds_length() {
        let f = LeftFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hi"), GqlValue::Int(100)], &c).unwrap(),
            s("hi")
        );
    }

    #[test]
    fn right_basic() {
        let f = RightFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), GqlValue::Int(3)], &c).unwrap(),
            s("llo")
        );
    }

    #[test]
    fn right_zero_returns_empty() {
        let f = RightFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), GqlValue::Int(0)], &c).unwrap(),
            s("")
        );
    }

    #[test]
    fn right_exceeds_length() {
        let f = RightFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hi"), GqlValue::Int(100)], &c).unwrap(),
            s("hi")
        );
    }

    // ── ReplaceFunction ──

    #[test]
    fn replace_basic() {
        let f = ReplaceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[s("hello world"), s("world"), s("rust")], &c)
            .unwrap();
        assert_eq!(r, s("hello rust"));
    }

    #[test]
    fn replace_empty_search() {
        let f = ReplaceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // Rust str::replace with "" inserts replacement between every char
        let r = f.invoke(&[s("ab"), s(""), s("X")], &c).unwrap();
        assert_eq!(r, s("XaXbX"));
    }

    #[test]
    fn replace_no_match() {
        let f = ReplaceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[s("hello"), s("xyz"), s("!")], &c).unwrap();
        assert_eq!(r, s("hello"));
    }

    #[test]
    fn replace_null_arg_returns_null() {
        let f = ReplaceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Null, s("a"), s("b")], &c).unwrap(),
            GqlValue::Null
        );
    }

    // ── ReverseFunction ──

    #[test]
    fn reverse_string() {
        let f = ReverseFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[s("abc")], &c).unwrap(), s("cba"));
    }

    #[test]
    fn reverse_empty_string() {
        let f = ReverseFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[s("")], &c).unwrap(), s(""));
    }

    #[test]
    fn reverse_list() {
        let f = ReverseFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[int_list(&[1, 2, 3])], &c).unwrap();
        assert_eq!(r, int_list(&[3, 2, 1]));
    }

    // ── LtrimFunction / RtrimFunction ──

    #[test]
    fn ltrim_leading_whitespace() {
        let f = LtrimFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[s("  hi  ")], &c).unwrap(), s("hi  "));
    }

    #[test]
    fn rtrim_trailing_whitespace() {
        let f = RtrimFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[s("  hi  ")], &c).unwrap(), s("  hi"));
    }

    // ── StartsWithFunction / EndsWithFunction / ContainsFn ──

    #[test]
    fn starts_with_true() {
        let f = StartsWithFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), s("hel")], &c).unwrap(),
            GqlValue::Bool(true)
        );
    }

    #[test]
    fn starts_with_empty_prefix() {
        let f = StartsWithFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), s("")], &c).unwrap(),
            GqlValue::Bool(true)
        );
    }

    #[test]
    fn starts_with_false() {
        let f = StartsWithFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), s("world")], &c).unwrap(),
            GqlValue::Bool(false)
        );
    }

    #[test]
    fn ends_with_true() {
        let f = EndsWithFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), s("llo")], &c).unwrap(),
            GqlValue::Bool(true)
        );
    }

    #[test]
    fn ends_with_empty_suffix() {
        let f = EndsWithFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), s("")], &c).unwrap(),
            GqlValue::Bool(true)
        );
    }

    #[test]
    fn contains_true() {
        let f = ContainsFn;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello world"), s("lo wo")], &c).unwrap(),
            GqlValue::Bool(true)
        );
    }

    #[test]
    fn contains_false() {
        let f = ContainsFn;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[s("hello"), s("xyz")], &c).unwrap(),
            GqlValue::Bool(false)
        );
    }

    // ── NullIfFunction ──

    #[test]
    fn nullif_equal_returns_null() {
        let f = NullIfFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(5), GqlValue::Int(5)], &c).unwrap(),
            GqlValue::Null
        );
    }

    #[test]
    fn nullif_not_equal_returns_first() {
        let f = NullIfFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(5), GqlValue::Int(3)], &c).unwrap(),
            GqlValue::Int(5)
        );
    }

    // ── ToStringFunction ──

    #[test]
    fn to_string_int() {
        let f = ToStringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Int(42)], &c).unwrap(), s("42"));
    }

    #[test]
    fn to_string_bool() {
        let f = ToStringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Bool(true)], &c).unwrap(), s("true"));
    }

    #[test]
    fn to_string_null() {
        let f = ToStringFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), s("null"));
    }

    // ── ValueTypeFunction ──

    #[test]
    fn value_type_int() {
        let f = ValueTypeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Int(1)], &c).unwrap(), s("INT"));
    }

    #[test]
    fn value_type_null() {
        let f = ValueTypeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), s("NULL"));
    }

    #[test]
    fn value_type_string() {
        let f = ValueTypeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[s("hi")], &c).unwrap(), s("STRING"));
    }

    // ── HeadFunction / TailFunction / LastFunction ──

    #[test]
    fn head_of_list() {
        let f = HeadFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[10, 20, 30])], &c).unwrap(),
            GqlValue::Int(10)
        );
    }

    #[test]
    fn head_of_empty_list() {
        let f = HeadFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[int_list(&[])], &c).unwrap(), GqlValue::Null);
    }

    #[test]
    fn tail_of_list() {
        let f = TailFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[10, 20, 30])], &c).unwrap(),
            int_list(&[20, 30])
        );
    }

    #[test]
    fn tail_of_empty_list() {
        let f = TailFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[int_list(&[])], &c).unwrap(), int_list(&[]));
    }

    #[test]
    fn last_of_list() {
        let f = LastFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[10, 20, 30])], &c).unwrap(),
            GqlValue::Int(30)
        );
    }

    #[test]
    fn last_of_empty_list() {
        let f = LastFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[int_list(&[])], &c).unwrap(), GqlValue::Null);
    }

    // ── RangeFunction ──

    #[test]
    fn range_basic() {
        let f = RangeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::Int(1), GqlValue::Int(5)], &c).unwrap();
        assert_eq!(r, int_list(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn range_with_step() {
        let f = RangeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::Int(0), GqlValue::Int(10), GqlValue::Int(3)], &c)
            .unwrap();
        assert_eq!(r, int_list(&[0, 3, 6, 9]));
    }

    #[test]
    fn range_negative_step() {
        let f = RangeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::Int(5), GqlValue::Int(1), GqlValue::Int(-1)], &c)
            .unwrap();
        assert_eq!(r, int_list(&[5, 4, 3, 2, 1]));
    }

    #[test]
    fn range_step_zero_errors() {
        let f = RangeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert!(
            f.invoke(&[GqlValue::Int(1), GqlValue::Int(5), GqlValue::Int(0)], &c)
                .is_err()
        );
    }

    // ── List functions ──

    #[test]
    fn list_contains_present() {
        let f = ListContainsFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[1, 2, 3]), GqlValue::Int(2)], &c)
                .unwrap(),
            GqlValue::Bool(true)
        );
    }

    #[test]
    fn list_contains_absent() {
        let f = ListContainsFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[1, 2, 3]), GqlValue::Int(99)], &c)
                .unwrap(),
            GqlValue::Bool(false)
        );
    }

    #[test]
    fn list_slice_basic() {
        let f = ListSliceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(
                &[
                    int_list(&[10, 20, 30, 40]),
                    GqlValue::Int(1),
                    GqlValue::Int(3),
                ],
                &c,
            )
            .unwrap();
        assert_eq!(r, int_list(&[20, 30]));
    }

    #[test]
    fn list_append_adds_to_end() {
        let f = ListAppendFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[int_list(&[1, 2]), GqlValue::Int(3)], &c)
            .unwrap();
        assert_eq!(r, int_list(&[1, 2, 3]));
    }

    #[test]
    fn list_prepend_adds_to_front() {
        let f = ListPrependFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[int_list(&[2, 3]), GqlValue::Int(1)], &c)
            .unwrap();
        assert_eq!(r, int_list(&[1, 2, 3]));
    }

    #[test]
    fn list_length_returns_count() {
        let f = ListLengthFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[1, 2, 3])], &c).unwrap(),
            GqlValue::Int(3)
        );
    }

    #[test]
    fn list_reverse_reverses() {
        let f = ListReverseFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[3, 1, 2])], &c).unwrap(),
            int_list(&[2, 1, 3])
        );
    }

    #[test]
    fn list_sort_ascending() {
        let f = ListSortFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[3, 1, 2])], &c).unwrap(),
            int_list(&[1, 2, 3])
        );
    }

    // ── LengthFunction (overloaded alias) ──

    #[test]
    fn length_of_string() {
        let f = LengthFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[s("hello")], &c).unwrap(), GqlValue::Int(5));
    }

    #[test]
    fn length_of_list() {
        let f = LengthFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[int_list(&[1, 2])], &c).unwrap(),
            GqlValue::Int(2)
        );
    }

    // ── NormalizeFunction ──

    #[test]
    fn normalize_nfc_default() {
        let f = NormalizeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // Already NFC, should pass through
        let r = f.invoke(&[s("hello")], &c).unwrap();
        assert_eq!(r, s("hello"));
    }

    #[test]
    fn normalize_null_returns_null() {
        let f = NormalizeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    #[test]
    fn normalize_unknown_form_errors() {
        let f = NormalizeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert!(f.invoke(&[s("hello"), s("INVALID")], &c).is_err());
    }

    // ── DoubleFunction ──

    #[test]
    fn double_from_int() {
        let f = DoubleFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[GqlValue::Int(42)], &c).unwrap(),
            GqlValue::Float(42.0)
        );
    }

    #[test]
    fn double_null_returns_null() {
        let f = DoubleFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }
}
