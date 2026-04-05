//! Type-safe Arrow array builder wrappers for constructing `Column` values.
//!
//! `ColumnBuilder` wraps Arrow's native builders, adding GqlValue coercion
//! and BoundValue routing so callers do not need to match on column kinds
//! at every append site.

use std::sync::Arc;

use arrow::array::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder};
use selene_core::{EdgeId, NodeId};

use super::binding::BoundValue;
use super::chunk::{Column, ColumnKind};
use super::value::{GqlList, GqlType, GqlValue};

// ---------------------------------------------------------------------------
// ColumnBuilder
// ---------------------------------------------------------------------------

/// Type-safe wrapper around Arrow array builders.
///
/// Each variant wraps the corresponding Arrow builder. Builders pre-allocate
/// capacity for the expected row count to minimize reallocations.
#[derive(Debug)]
pub(crate) enum ColumnBuilder {
    NodeIds(UInt64Builder),
    EdgeIds(UInt64Builder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float64(Float64Builder),
    Bool(BooleanBuilder),
    Utf8(StringBuilder),
    /// Heterogeneous value accumulator for complex/mixed types.
    Values(Vec<GqlValue>),
}

#[allow(dead_code)]
impl ColumnBuilder {
    /// Create a builder for node ID columns.
    pub fn new_node_ids(capacity: usize) -> Self {
        Self::NodeIds(UInt64Builder::with_capacity(capacity))
    }

    /// Create a builder for edge ID columns.
    pub fn new_edge_ids(capacity: usize) -> Self {
        Self::EdgeIds(UInt64Builder::with_capacity(capacity))
    }

    /// Create a builder for signed 64-bit integer columns.
    pub fn new_int64(capacity: usize) -> Self {
        Self::Int64(Int64Builder::with_capacity(capacity))
    }

    /// Create a builder for unsigned 64-bit integer columns.
    pub fn new_uint64(capacity: usize) -> Self {
        Self::UInt64(UInt64Builder::with_capacity(capacity))
    }

    /// Create a builder for 64-bit float columns.
    pub fn new_float64(capacity: usize) -> Self {
        Self::Float64(Float64Builder::with_capacity(capacity))
    }

    /// Create a builder for boolean columns.
    pub fn new_bool(capacity: usize) -> Self {
        Self::Bool(BooleanBuilder::with_capacity(capacity))
    }

    /// Create a builder for UTF-8 string columns.
    pub fn new_utf8() -> Self {
        Self::Utf8(StringBuilder::new())
    }

    /// Create a builder for heterogeneous GqlValue columns.
    pub fn new_values(capacity: usize) -> Self {
        Self::Values(Vec::with_capacity(capacity))
    }

    /// Create a builder matching the given `ColumnKind`.
    pub fn for_kind(kind: ColumnKind, capacity: usize) -> Self {
        match kind {
            ColumnKind::NodeId => Self::new_node_ids(capacity),
            ColumnKind::EdgeId => Self::new_edge_ids(capacity),
            ColumnKind::Int64 => Self::new_int64(capacity),
            ColumnKind::UInt64 => Self::new_uint64(capacity),
            ColumnKind::Float64 => Self::new_float64(capacity),
            ColumnKind::Bool => Self::new_bool(capacity),
            ColumnKind::Utf8 | ColumnKind::Null => Self::new_utf8(),
            ColumnKind::Values => Self::new_values(capacity),
        }
    }

    /// Append a node ID value.
    pub fn append_node_id(&mut self, id: NodeId) {
        match self {
            Self::NodeIds(b) => b.append_value(id.0),
            _ => self.append_null(),
        }
    }

    /// Append an edge ID value.
    pub fn append_edge_id(&mut self, id: EdgeId) {
        match self {
            Self::EdgeIds(b) => b.append_value(id.0),
            _ => self.append_null(),
        }
    }

    /// Append a null value.
    pub fn append_null(&mut self) {
        match self {
            Self::NodeIds(b) => b.append_null(),
            Self::EdgeIds(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt64(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Bool(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::Values(v) => v.push(GqlValue::Null),
        }
    }

    /// Append a GqlValue, coercing to the builder's type.
    pub fn append_gql_value(&mut self, val: &GqlValue) {
        match self {
            Self::NodeIds(b) => match val {
                GqlValue::Node(id) => b.append_value(id.0),
                GqlValue::UInt(u) => b.append_value(*u),
                _ => b.append_null(),
            },
            Self::EdgeIds(b) => match val {
                GqlValue::Edge(id) => b.append_value(id.0),
                GqlValue::UInt(u) => b.append_value(*u),
                _ => b.append_null(),
            },
            Self::Int64(b) => match val {
                GqlValue::Int(i) => b.append_value(*i),
                GqlValue::UInt(u) => b.append_value(*u as i64),
                _ => b.append_null(),
            },
            Self::UInt64(b) => match val {
                GqlValue::UInt(u) => b.append_value(*u),
                GqlValue::Int(i) => b.append_value(*i as u64),
                _ => b.append_null(),
            },
            Self::Float64(b) => match val {
                GqlValue::Float(f) => b.append_value(*f),
                GqlValue::Int(i) => b.append_value(*i as f64),
                GqlValue::UInt(u) => b.append_value(*u as f64),
                _ => b.append_null(),
            },
            Self::Bool(b) => match val {
                GqlValue::Bool(v) => b.append_value(*v),
                _ => b.append_null(),
            },
            Self::Utf8(b) => match val {
                GqlValue::Null => b.append_null(),
                GqlValue::String(s) => b.append_value(s.as_str()),
                other => b.append_value(format!("{other}")),
            },
            Self::Values(v) => v.push(val.clone()),
        }
    }

    /// Consume the builder, producing a finished `Column`.
    pub fn finish(self) -> Column {
        match self {
            Self::NodeIds(mut b) => Column::NodeIds(Arc::new(b.finish())),
            Self::EdgeIds(mut b) => Column::EdgeIds(Arc::new(b.finish())),
            Self::Int64(mut b) => Column::Int64(Arc::new(b.finish())),
            Self::UInt64(mut b) => Column::UInt64(Arc::new(b.finish())),
            Self::Float64(mut b) => Column::Float64(Arc::new(b.finish())),
            Self::Bool(mut b) => Column::Bool(Arc::new(b.finish())),
            Self::Utf8(mut b) => Column::Utf8(Arc::new(b.finish())),
            Self::Values(v) => Column::Values(Arc::from(v)),
        }
    }

    /// The `ColumnKind` this builder produces.
    pub fn kind(&self) -> ColumnKind {
        match self {
            Self::NodeIds(_) => ColumnKind::NodeId,
            Self::EdgeIds(_) => ColumnKind::EdgeId,
            Self::Int64(_) => ColumnKind::Int64,
            Self::UInt64(_) => ColumnKind::UInt64,
            Self::Float64(_) => ColumnKind::Float64,
            Self::Bool(_) => ColumnKind::Bool,
            Self::Utf8(_) => ColumnKind::Utf8,
            Self::Values(_) => ColumnKind::Values,
        }
    }

    /// Append a `BoundValue`, routing to the correct builder method.
    ///
    /// For Values builders, converts the BoundValue to GqlValue first.
    pub fn append_bound_value(&mut self, val: &BoundValue) {
        match val {
            BoundValue::Node(id) => match self {
                Self::NodeIds(b) => b.append_value(id.0),
                Self::Values(v) => v.push(GqlValue::Node(*id)),
                _ => self.append_null(),
            },
            BoundValue::Edge(id) => match self {
                Self::EdgeIds(b) => b.append_value(id.0),
                Self::Values(v) => v.push(GqlValue::Edge(*id)),
                _ => self.append_null(),
            },
            BoundValue::Scalar(gv) => self.append_gql_value(gv),
            BoundValue::Path(p) => match self {
                Self::Values(v) => v.push(GqlValue::Path(p.clone())),
                Self::Utf8(b) => b.append_value(format!("{p:?}")),
                _ => self.append_null(),
            },
            BoundValue::Group(edges) => match self {
                Self::Values(v) => {
                    // Store as List<Edge> for fidelity (matches eval.rs resolve_var)
                    let elements: Vec<GqlValue> =
                        edges.iter().map(|e| GqlValue::Edge(*e)).collect();
                    v.push(GqlValue::List(GqlList {
                        element_type: GqlType::Edge,
                        elements: Arc::from(elements),
                    }));
                }
                Self::Utf8(b) => {
                    let s = format!(
                        "[{}]",
                        edges
                            .iter()
                            .map(|e| e.0.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    b.append_value(s);
                }
                _ => self.append_null(),
            },
        }
    }
}
