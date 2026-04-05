//! Core built-in functions: coalesce, string basics, graph element,
//! degree, path, and property introspection functions.

use std::sync::Arc;

use smol_str::SmolStr;

use super::{EvalContext, GqlError, GqlValue, ScalarFunction, ZonedDateTime};
use crate::types::value::{GqlList, GqlType, PathElement};

// ── Basic functions ─────────────────────────────────────────────────

pub(crate) struct CoalesceFunction;
impl ScalarFunction for CoalesceFunction {
    fn name(&self) -> &'static str {
        "coalesce"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        for arg in args {
            if !arg.is_null() {
                return Ok(arg.clone());
            }
        }
        Ok(GqlValue::Null)
    }
}

pub(crate) struct CharLengthFunction;
impl ScalarFunction for CharLengthFunction {
    fn name(&self) -> &'static str {
        "char_length"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => Ok(GqlValue::Int(s.chars().count() as i64)),
            _ => Err(GqlError::type_error("char_length requires a string")),
        }
    }
}

pub(crate) struct UpperFunction;
impl ScalarFunction for UpperFunction {
    fn name(&self) -> &'static str {
        "upper"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => {
                Ok(GqlValue::String(SmolStr::new(s.to_uppercase().as_str())))
            }
            _ => Err(GqlError::type_error("upper requires a string")),
        }
    }
}

pub(crate) struct LowerFunction;
impl ScalarFunction for LowerFunction {
    fn name(&self) -> &'static str {
        "lower"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => {
                Ok(GqlValue::String(SmolStr::new(s.to_lowercase().as_str())))
            }
            _ => Err(GqlError::type_error("lower requires a string")),
        }
    }
}

pub(crate) struct TrimFunction;
impl ScalarFunction for TrimFunction {
    fn name(&self) -> &'static str {
        "trim"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => Ok(GqlValue::String(SmolStr::new(s.trim()))),
            _ => Err(GqlError::type_error("trim requires a string")),
        }
    }
}

pub(crate) struct SizeFunction;
impl ScalarFunction for SizeFunction {
    fn name(&self) -> &'static str {
        "size"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            Some(GqlValue::List(l)) => Ok(GqlValue::Int(l.len() as i64)),
            Some(GqlValue::Path(p)) => Ok(GqlValue::Int(p.edge_count() as i64)),
            _ => Err(GqlError::type_error("size requires a list or path")),
        }
    }
}

pub(crate) struct DurationFunction;
impl ScalarFunction for DurationFunction {
    fn name(&self) -> &'static str {
        "duration"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => {
                let nanos = crate::runtime::eval::parse_duration(s)?;
                Ok(GqlValue::Duration(
                    crate::types::value::GqlDuration::day_time(nanos),
                ))
            }
            _ => Err(GqlError::type_error("duration requires a string")),
        }
    }
}

pub(crate) struct ZonedDatetimeFunction;
impl ScalarFunction for ZonedDatetimeFunction {
    fn name(&self) -> &'static str {
        "zoned_datetime"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::String(s)) => parse_iso8601(s),
            None => Ok(GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(
                selene_core::now_nanos(),
            ))),
            _ => Err(GqlError::type_error(
                "zoned_datetime requires a string or no arguments",
            )),
        }
    }
}

// ── Graph element functions ──────────────────────────────────────────

pub(crate) struct IdFunction;
impl ScalarFunction for IdFunction {
    fn name(&self) -> &'static str {
        "id"
    }
    fn description(&self) -> &'static str {
        "Return the numeric ID of a node or edge"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Node(id)) => Ok(GqlValue::UInt(id.0)),
            Some(GqlValue::Edge(id)) => Ok(GqlValue::UInt(id.0)),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("id() requires a node or edge")),
        }
    }
}

pub(crate) struct ElementIdFunction;
impl ScalarFunction for ElementIdFunction {
    fn name(&self) -> &'static str {
        "element_id"
    }
    fn description(&self) -> &'static str {
        "ISO alias for id()"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        IdFunction.invoke(args, ctx)
    }
}

pub(crate) struct TypeFunction;
impl ScalarFunction for TypeFunction {
    fn name(&self) -> &'static str {
        "type"
    }
    fn description(&self) -> &'static str {
        "Return the label/type of an edge"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Edge(id)) => match ctx.graph.get_edge(*id) {
                Some(edge) => Ok(GqlValue::String(SmolStr::new(edge.label.as_str()))),
                None => Ok(GqlValue::Null),
            },
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("type() requires an edge")),
        }
    }
}

pub(crate) struct StartNodeFunction;
impl ScalarFunction for StartNodeFunction {
    fn name(&self) -> &'static str {
        "start_node"
    }
    fn description(&self) -> &'static str {
        "Return the source node of an edge"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Edge(id)) => match ctx.graph.get_edge(*id) {
                Some(edge) => Ok(GqlValue::Node(edge.source)),
                None => Ok(GqlValue::Null),
            },
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("start_node() requires an edge")),
        }
    }
}

pub(crate) struct EndNodeFunction;
impl ScalarFunction for EndNodeFunction {
    fn name(&self) -> &'static str {
        "end_node"
    }
    fn description(&self) -> &'static str {
        "Return the target node of an edge"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Edge(id)) => match ctx.graph.get_edge(*id) {
                Some(edge) => Ok(GqlValue::Node(edge.target)),
                None => Ok(GqlValue::Null),
            },
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("end_node() requires an edge")),
        }
    }
}

// ── Degree functions ────────────────────────────────────────────────

pub(crate) struct DegreeFunction;
impl ScalarFunction for DegreeFunction {
    fn name(&self) -> &'static str {
        "degree"
    }
    fn description(&self) -> &'static str {
        "Total degree (in + out) of a node"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Node(id)) => {
                let d = ctx.graph.outgoing(*id).len() + ctx.graph.incoming(*id).len();
                Ok(GqlValue::Int(d as i64))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("degree() requires a node")),
        }
    }
}

pub(crate) struct InDegreeFunction;
impl ScalarFunction for InDegreeFunction {
    fn name(&self) -> &'static str {
        "in_degree"
    }
    fn description(&self) -> &'static str {
        "Number of incoming edges to a node"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Node(id)) => Ok(GqlValue::Int(ctx.graph.incoming(*id).len() as i64)),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("in_degree() requires a node")),
        }
    }
}

pub(crate) struct OutDegreeFunction;
impl ScalarFunction for OutDegreeFunction {
    fn name(&self) -> &'static str {
        "out_degree"
    }
    fn description(&self) -> &'static str {
        "Number of outgoing edges from a node"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Node(id)) => Ok(GqlValue::Int(ctx.graph.outgoing(*id).len() as i64)),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("out_degree() requires a node")),
        }
    }
}

// ── Path functions ──────────────────────────────────────────────────

pub(crate) struct PathLengthFunction;
impl ScalarFunction for PathLengthFunction {
    fn name(&self) -> &'static str {
        "path_length"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Path(p)) => Ok(GqlValue::Int(p.edge_count() as i64)),
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("path_length() requires a path")),
        }
    }
}

pub(crate) struct NodesFunction;
impl ScalarFunction for NodesFunction {
    fn name(&self) -> &'static str {
        "nodes"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Path(p)) => {
                let nodes: Vec<GqlValue> = p
                    .elements
                    .iter()
                    .filter_map(|e| match e {
                        PathElement::Node(id) => Some(GqlValue::Node(*id)),
                        PathElement::Edge(_) => None,
                    })
                    .collect();
                Ok(GqlValue::List(GqlList {
                    element_type: GqlType::Node,
                    elements: Arc::from(nodes),
                }))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("nodes() requires a path")),
        }
    }
}

pub(crate) struct EdgesFunction;
impl ScalarFunction for EdgesFunction {
    fn name(&self) -> &'static str {
        "edges"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Path(p)) => {
                let edges: Vec<GqlValue> = p
                    .elements
                    .iter()
                    .filter_map(|e| match e {
                        PathElement::Edge(id) => Some(GqlValue::Edge(*id)),
                        PathElement::Node(_) => None,
                    })
                    .collect();
                Ok(GqlValue::List(GqlList {
                    element_type: GqlType::Edge,
                    elements: Arc::from(edges),
                }))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("edges() requires a path")),
        }
    }
}

pub(crate) struct IsAcyclicFunction;
impl ScalarFunction for IsAcyclicFunction {
    fn name(&self) -> &'static str {
        "is_acyclic"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Path(p)) => {
                let mut seen = std::collections::HashSet::new();
                let acyclic = p.elements.iter().all(|e| match e {
                    PathElement::Node(id) => seen.insert(*id),
                    PathElement::Edge(_) => true,
                });
                Ok(GqlValue::Bool(acyclic))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("is_acyclic() requires a path")),
        }
    }
}

pub(crate) struct IsTrailFunction;
impl ScalarFunction for IsTrailFunction {
    fn name(&self) -> &'static str {
        "is_trail"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Path(p)) => {
                let mut seen = std::collections::HashSet::new();
                let trail = p.elements.iter().all(|e| match e {
                    PathElement::Edge(id) => seen.insert(*id),
                    PathElement::Node(_) => true,
                });
                Ok(GqlValue::Bool(trail))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("is_trail() requires a path")),
        }
    }
}

pub(crate) struct IsSimpleFunction;
impl ScalarFunction for IsSimpleFunction {
    fn name(&self) -> &'static str {
        "is_simple"
    }
    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Path(p)) => {
                let nodes: Vec<selene_core::NodeId> = p
                    .elements
                    .iter()
                    .filter_map(|e| match e {
                        PathElement::Node(id) => Some(*id),
                        PathElement::Edge(_) => None,
                    })
                    .collect();
                if nodes.len() <= 1 {
                    return Ok(GqlValue::Bool(true));
                }
                // Simple: no repeated nodes, except start == end is allowed
                let mut seen = std::collections::HashSet::new();
                for (i, id) in nodes.iter().enumerate() {
                    if i == nodes.len() - 1 && *id == nodes[0] {
                        continue;
                    }
                    if !seen.insert(*id) {
                        return Ok(GqlValue::Bool(false));
                    }
                }
                Ok(GqlValue::Bool(true))
            }
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("is_simple() requires a path")),
        }
    }
}

// ── Property introspection ──────────────────────────────────────────

pub(crate) struct PropertyNamesFunction;
impl ScalarFunction for PropertyNamesFunction {
    fn name(&self) -> &'static str {
        "property_names"
    }
    fn description(&self) -> &'static str {
        "Return list of property keys on a node"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Node(id)) => match ctx.graph.get_node(*id) {
                Some(node) => {
                    let names: Vec<GqlValue> = node
                        .properties
                        .iter()
                        .map(|(k, _)| GqlValue::String(SmolStr::new(k.as_str())))
                        .collect();
                    Ok(GqlValue::List(GqlList {
                        element_type: GqlType::String,
                        elements: Arc::from(names),
                    }))
                }
                None => Ok(GqlValue::Null),
            },
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("property_names() requires a node")),
        }
    }
}

pub(crate) struct PropertiesFunction;
impl ScalarFunction for PropertiesFunction {
    fn name(&self) -> &'static str {
        "properties"
    }
    fn invoke(&self, args: &[GqlValue], ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        match args.first() {
            Some(GqlValue::Node(id)) => match ctx.graph.get_node(*id) {
                Some(node) => {
                    let pairs: Vec<String> = node
                        .properties
                        .iter()
                        .map(|(k, v)| format!("{}: {v}", k.as_str()))
                        .collect();
                    Ok(GqlValue::String(SmolStr::new(format!(
                        "{{{}}}",
                        pairs.join(", ")
                    ))))
                }
                None => Ok(GqlValue::Null),
            },
            Some(GqlValue::Edge(id)) => match ctx.graph.get_edge(*id) {
                Some(edge) => {
                    let pairs: Vec<String> = edge
                        .properties
                        .iter()
                        .map(|(k, v)| format!("{}: {v}", k.as_str()))
                        .collect();
                    Ok(GqlValue::String(SmolStr::new(format!(
                        "{{{}}}",
                        pairs.join(", ")
                    ))))
                }
                None => Ok(GqlValue::Null),
            },
            Some(GqlValue::Null) | None => Ok(GqlValue::Null),
            _ => Err(GqlError::type_error("properties() requires a node or edge")),
        }
    }
}

// ── ISO 8601 helpers ────────────────────────────────────────────────

/// Parse an ISO 8601 datetime string into a ZonedDateTime.
///
/// Supported formats:
///   2024-08-15T14:30:00Z
///   2024-08-15T14:30:00+02:00
///   2024-08-15T14:30:00-05:00
///   2024-08-15T12:30:00.123Z        (fractional seconds)
///   2024-12-31T23:59:59.999-08:00
pub fn parse_iso8601(s: &str) -> Result<GqlValue, GqlError> {
    let err = || GqlError::InvalidArgument {
        message: format!("invalid ISO 8601 datetime: '{s}'"),
    };

    // Split at 'T' to get date and time parts
    let (date_str, time_offset) = s.split_once('T').ok_or_else(err)?;

    // Parse date: YYYY-MM-DD
    let date_parts: Vec<&str> = date_str.split('-').collect();
    if date_parts.len() != 3 {
        return Err(err());
    }
    let year: i32 = date_parts[0].parse().map_err(|_| err())?;
    let month: u32 = date_parts[1].parse().map_err(|_| err())?;
    let day: u32 = date_parts[2].parse().map_err(|_| err())?;

    // Split time from timezone offset
    let (time_str, offset_seconds) = if let Some(stripped) = time_offset.strip_suffix('Z') {
        (stripped, 0i32)
    } else if let Some(pos) = time_offset.rfind('+') {
        let offset = parse_tz_offset(&time_offset[pos..])?;
        (&time_offset[..pos], offset)
    } else if let Some(pos) = time_offset[1..].rfind('-') {
        // Skip first char (might be part of time), find last '-'
        let actual_pos = pos + 1;
        let offset = parse_tz_offset(&time_offset[actual_pos..])?;
        (&time_offset[..actual_pos], offset)
    } else {
        return Err(err());
    };

    // Parse time: HH:MM:SS or HH:MM:SS.fff
    let (time_base, frac_nanos) = if let Some((base, frac)) = time_str.split_once('.') {
        let frac_str = frac;
        let nanos = match frac_str.len() {
            1 => frac_str.parse::<i64>().map_err(|_| err())? * 100_000_000,
            2 => frac_str.parse::<i64>().map_err(|_| err())? * 10_000_000,
            3 => frac_str.parse::<i64>().map_err(|_| err())? * 1_000_000,
            6 => frac_str.parse::<i64>().map_err(|_| err())? * 1_000,
            9 => frac_str.parse::<i64>().map_err(|_| err())?,
            _ => {
                let padded = format!("{:0<9}", &frac_str[..frac_str.len().min(9)]);
                padded.parse::<i64>().map_err(|_| err())?
            }
        };
        (base, nanos)
    } else {
        (time_str, 0i64)
    };

    let time_parts: Vec<&str> = time_base.split(':').collect();
    if time_parts.len() < 2 {
        return Err(err());
    }
    let hour: u32 = time_parts[0].parse().map_err(|_| err())?;
    let minute: u32 = time_parts[1].parse().map_err(|_| err())?;
    let second: u32 = if time_parts.len() > 2 {
        time_parts[2].parse().map_err(|_| err())?
    } else {
        0
    };

    // Convert to Unix timestamp (nanos since epoch)
    // Simplified: doesn't handle leap seconds, pre-epoch dates are approximate
    let days = days_from_civil(year, month, day);
    let total_seconds =
        days * 86400 + i64::from(hour) * 3600 + i64::from(minute) * 60 + i64::from(second)
            - i64::from(offset_seconds); // Convert to UTC
    let nanos = total_seconds * 1_000_000_000 + frac_nanos;

    Ok(GqlValue::ZonedDateTime(ZonedDateTime {
        nanos,
        offset_seconds,
    }))
}

/// Parse timezone offset string like "+02:00" or "-05:00" to seconds.
pub(crate) fn parse_tz_offset(s: &str) -> Result<i32, GqlError> {
    let err = || GqlError::InvalidArgument {
        message: format!("invalid timezone offset: '{s}'"),
    };

    let sign: i32 = if s.starts_with('+') { 1 } else { -1 };
    let offset_str = &s[1..]; // skip +/-

    let parts: Vec<&str> = offset_str.split(':').collect();
    if parts.len() != 2 {
        return Err(err());
    }
    let hours: i32 = parts[0].parse().map_err(|_| err())?;
    let minutes: i32 = parts[1].parse().map_err(|_| err())?;

    Ok(sign * (hours * 3600 + minutes * 60))
}

/// Convert a civil date to days since Unix epoch (1970-01-01).
/// Algorithm from Howard Hinnant's date algorithms.
pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let y = i64::from(if month <= 2 { year - 1 } else { year });
    let m = i64::from(if month <= 2 { month + 9 } else { month - 3 });
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * m as u64 + 2) / 5 + u64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::functions::{FunctionRegistry, ScalarFunction};
    use crate::types::value::{GqlList, GqlPath, GqlType, PathElement};
    use selene_core::{EdgeId, NodeId};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;
    use std::sync::Arc;

    fn ctx() -> (SeleneGraph, FunctionRegistry) {
        (SeleneGraph::new(), FunctionRegistry::with_builtins())
    }

    // ── CoalesceFunction ──

    #[test]
    fn coalesce_returns_first_non_null() {
        let f = CoalesceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::Null, GqlValue::Int(7)], &c).unwrap();
        assert_eq!(r, GqlValue::Int(7));
    }

    #[test]
    fn coalesce_all_null_returns_null() {
        let f = CoalesceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::Null, GqlValue::Null], &c).unwrap();
        assert_eq!(r, GqlValue::Null);
    }

    #[test]
    fn coalesce_no_args_returns_null() {
        let f = CoalesceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[], &c).unwrap(), GqlValue::Null);
    }

    #[test]
    fn coalesce_first_arg_non_null() {
        let f = CoalesceFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::Int(1), GqlValue::Int(2)], &c).unwrap();
        assert_eq!(r, GqlValue::Int(1));
    }

    // ── CharLengthFunction ──

    #[test]
    fn char_length_ascii() {
        let f = CharLengthFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::String(SmolStr::new("hello"))], &c)
            .unwrap();
        assert_eq!(r, GqlValue::Int(5));
    }

    #[test]
    fn char_length_unicode() {
        let f = CharLengthFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // 4 Unicode codepoints
        let r = f
            .invoke(&[GqlValue::String(SmolStr::new("\u{1F600}abc"))], &c)
            .unwrap();
        assert_eq!(r, GqlValue::Int(4));
    }

    #[test]
    fn char_length_empty() {
        let f = CharLengthFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::String(SmolStr::new(""))], &c).unwrap();
        assert_eq!(r, GqlValue::Int(0));
    }

    #[test]
    fn char_length_null() {
        let f = CharLengthFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── UpperFunction / LowerFunction ──

    #[test]
    fn upper_mixed_case() {
        let f = UpperFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::String(SmolStr::new("hElLo"))], &c)
            .unwrap();
        assert_eq!(r, GqlValue::String(SmolStr::new("HELLO")));
    }

    #[test]
    fn lower_mixed_case() {
        let f = LowerFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::String(SmolStr::new("HeLLO"))], &c)
            .unwrap();
        assert_eq!(r, GqlValue::String(SmolStr::new("hello")));
    }

    #[test]
    fn upper_null_returns_null() {
        let f = UpperFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── TrimFunction ──

    #[test]
    fn trim_whitespace() {
        let f = TrimFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::String(SmolStr::new("  hi  "))], &c)
            .unwrap();
        assert_eq!(r, GqlValue::String(SmolStr::new("hi")));
    }

    #[test]
    fn trim_no_whitespace() {
        let f = TrimFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::String(SmolStr::new("abc"))], &c)
            .unwrap();
        assert_eq!(r, GqlValue::String(SmolStr::new("abc")));
    }

    // ── SizeFunction ──

    #[test]
    fn size_of_list() {
        let f = SizeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let list = GqlValue::List(GqlList {
            element_type: GqlType::Int,
            elements: Arc::from(vec![GqlValue::Int(1), GqlValue::Int(2), GqlValue::Int(3)]),
        });
        assert_eq!(f.invoke(&[list], &c).unwrap(), GqlValue::Int(3));
    }

    #[test]
    fn size_of_empty_list() {
        let f = SizeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let list = GqlValue::List(GqlList {
            element_type: GqlType::Nothing,
            elements: Arc::from(vec![]),
        });
        assert_eq!(f.invoke(&[list], &c).unwrap(), GqlValue::Int(0));
    }

    #[test]
    fn size_null_returns_null() {
        let f = SizeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    #[test]
    fn size_wrong_type_errors() {
        let f = SizeFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert!(f.invoke(&[GqlValue::Int(42)], &c).is_err());
    }

    // ── IdFunction ──

    #[test]
    fn id_of_node() {
        let f = IdFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::Node(NodeId(5))], &c).unwrap();
        assert_eq!(r, GqlValue::UInt(5));
    }

    #[test]
    fn id_of_edge() {
        let f = IdFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[GqlValue::Edge(EdgeId(10))], &c).unwrap();
        assert_eq!(r, GqlValue::UInt(10));
    }

    #[test]
    fn id_null_returns_null() {
        let f = IdFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[GqlValue::Null], &c).unwrap(), GqlValue::Null);
    }

    // ── Path functions ──

    fn sample_path() -> GqlValue {
        GqlValue::Path(GqlPath {
            elements: vec![
                PathElement::Node(NodeId(1)),
                PathElement::Edge(EdgeId(10)),
                PathElement::Node(NodeId(2)),
                PathElement::Edge(EdgeId(11)),
                PathElement::Node(NodeId(3)),
            ],
        })
    }

    #[test]
    fn path_length_two_edges() {
        let f = PathLengthFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(f.invoke(&[sample_path()], &c).unwrap(), GqlValue::Int(2));
    }

    #[test]
    fn nodes_extracts_node_ids() {
        let f = NodesFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[sample_path()], &c).unwrap();
        if let GqlValue::List(list) = r {
            assert_eq!(list.elements.len(), 3);
            assert_eq!(list.elements[0], GqlValue::Node(NodeId(1)));
            assert_eq!(list.elements[2], GqlValue::Node(NodeId(3)));
        } else {
            panic!("expected List");
        }
    }

    #[test]
    fn edges_extracts_edge_ids() {
        let f = EdgesFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f.invoke(&[sample_path()], &c).unwrap();
        if let GqlValue::List(list) = r {
            assert_eq!(list.elements.len(), 2);
            assert_eq!(list.elements[0], GqlValue::Edge(EdgeId(10)));
        } else {
            panic!("expected List");
        }
    }

    // ── is_acyclic / is_trail / is_simple ──

    #[test]
    fn is_acyclic_true_for_no_repeated_nodes() {
        let f = IsAcyclicFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[sample_path()], &c).unwrap(),
            GqlValue::Bool(true)
        );
    }

    #[test]
    fn is_acyclic_false_when_node_repeats() {
        let f = IsAcyclicFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let cyclic = GqlValue::Path(GqlPath {
            elements: vec![
                PathElement::Node(NodeId(1)),
                PathElement::Edge(EdgeId(10)),
                PathElement::Node(NodeId(2)),
                PathElement::Edge(EdgeId(11)),
                PathElement::Node(NodeId(1)), // repeat
            ],
        });
        assert_eq!(f.invoke(&[cyclic], &c).unwrap(), GqlValue::Bool(false));
    }

    #[test]
    fn is_trail_true_no_repeated_edges() {
        let f = IsTrailFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert_eq!(
            f.invoke(&[sample_path()], &c).unwrap(),
            GqlValue::Bool(true)
        );
    }

    #[test]
    fn is_trail_false_when_edge_repeats() {
        let f = IsTrailFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let not_trail = GqlValue::Path(GqlPath {
            elements: vec![
                PathElement::Node(NodeId(1)),
                PathElement::Edge(EdgeId(10)),
                PathElement::Node(NodeId(2)),
                PathElement::Edge(EdgeId(10)), // same edge
                PathElement::Node(NodeId(3)),
            ],
        });
        assert_eq!(f.invoke(&[not_trail], &c).unwrap(), GqlValue::Bool(false));
    }

    #[test]
    fn is_simple_allows_start_eq_end() {
        let f = IsSimpleFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        // Closed path where start == end is still simple
        let closed = GqlValue::Path(GqlPath {
            elements: vec![
                PathElement::Node(NodeId(1)),
                PathElement::Edge(EdgeId(10)),
                PathElement::Node(NodeId(2)),
                PathElement::Edge(EdgeId(11)),
                PathElement::Node(NodeId(1)), // start == end
            ],
        });
        assert_eq!(f.invoke(&[closed], &c).unwrap(), GqlValue::Bool(true));
    }

    #[test]
    fn is_simple_false_middle_repeat() {
        let f = IsSimpleFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let not_simple = GqlValue::Path(GqlPath {
            elements: vec![
                PathElement::Node(NodeId(1)),
                PathElement::Edge(EdgeId(10)),
                PathElement::Node(NodeId(2)),
                PathElement::Edge(EdgeId(11)),
                PathElement::Node(NodeId(2)), // middle repeat (not start==end)
                PathElement::Edge(EdgeId(12)),
                PathElement::Node(NodeId(3)),
            ],
        });
        assert_eq!(f.invoke(&[not_simple], &c).unwrap(), GqlValue::Bool(false));
    }

    // ── days_from_civil ──

    #[test]
    fn days_from_civil_epoch() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
    }

    #[test]
    fn days_from_civil_known_date() {
        // 2024-01-15 = 19737 days since epoch (verified externally)
        assert_eq!(days_from_civil(2024, 1, 15), 19737);
    }

    // ── DurationFunction ──

    #[test]
    fn duration_parses_string() {
        let f = DurationFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        let r = f
            .invoke(&[GqlValue::String(SmolStr::new("1h"))], &c)
            .unwrap();
        if let GqlValue::Duration(d) = r {
            assert_eq!(d.nanos, 3_600_000_000_000);
        } else {
            panic!("expected Duration");
        }
    }

    #[test]
    fn duration_rejects_non_string() {
        let f = DurationFunction;
        let (g, reg) = ctx();
        let c = EvalContext::new(&g, &reg);
        assert!(f.invoke(&[GqlValue::Int(42)], &c).is_err());
    }
}
