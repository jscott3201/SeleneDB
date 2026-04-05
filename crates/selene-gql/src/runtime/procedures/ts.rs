//! Built-in time-series procedures: ts.range, ts.latest, ts.aggregate, ts.window.
//!
//! Each procedure maps directly to HotTier methods. Timestamps are
//! converted from i64 nanos (storage) to ZonedDateTime (GQL runtime)
//! with UTC offset assumed.
//!
//! Procedures are organized into submodules by domain:
//! - `ts_range`: point queries (ts.range, ts.latest, ts.valueAt)
//! - `ts_aggregate`: aggregation (ts.aggregate, ts.window)
//! - `ts_tiers`: multi-tier (ts.downsample, ts.history, ts.fullRange, ts.trends)

use selene_core::NodeId;
use selene_graph::SeleneGraph;

use crate::runtime::eval::parse_duration;
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

// Re-export all procedure structs so registration in mod.rs remains unchanged.
pub use super::ts_aggregate::{TsAggregate, TsWindow};
pub use super::ts_range::{TsLatest, TsRange, TsValueAt};
pub use super::ts_tiers::{TsDownsample, TsFullRange, TsHistory, TsTrends};

// ── Helpers ────────────────────────────────────────────────────────

/// Extract an absolute timestamp in nanoseconds from a GqlValue.
/// Accepts ZonedDateTime (uses .nanos), Int (raw nanos), or String (ISO 8601 parse).
pub(crate) fn extract_timestamp(val: &GqlValue) -> Result<i64, GqlError> {
    match val {
        GqlValue::ZonedDateTime(zdt) => Ok(zdt.nanos),
        GqlValue::Int(n) => Ok(*n),
        GqlValue::String(s) => match crate::runtime::functions::parse_iso8601(s)? {
            GqlValue::ZonedDateTime(zdt) => Ok(zdt.nanos),
            _ => Err(GqlError::type_error(format!(
                "cannot parse '{s}' as timestamp"
            ))),
        },
        _ => Err(GqlError::type_error(format!(
            "expected timestamp, got {}",
            val.gql_type()
        ))),
    }
}

/// Extract duration nanos from a GqlValue.
/// Handles both ZonedDateTime (from duration() function) and String (raw parse).
pub fn extract_duration(val: &GqlValue) -> Result<i64, GqlError> {
    match val {
        GqlValue::ZonedDateTime(zdt) => Ok(zdt.nanos.abs()),
        GqlValue::String(s) => parse_duration(s),
        GqlValue::Int(n) => Ok(n.abs()),
        _ => Err(GqlError::type_error(format!(
            "expected duration, got {}",
            val.gql_type()
        ))),
    }
}

/// Look up the schema-defined expected interval for a property on a node.
pub(crate) fn resolve_schema_interval(
    graph: &SeleneGraph,
    node_id: NodeId,
    property: &str,
) -> Option<i64> {
    let node = graph.get_node(node_id)?;
    for label in node.labels.iter() {
        if let Some(schema) = graph.schema().node_schema(label.as_str())
            && let Some(prop_def) = schema.properties.iter().find(|p| *p.name == *property)
        {
            return prop_def.expected_interval_nanos;
        }
    }
    None
}
