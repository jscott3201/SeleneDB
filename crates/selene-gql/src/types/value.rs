//! GQL runtime value type with full ISO GQL type parity.
//!
//! `GqlValue` is the runtime type used during query execution. It is a
//! superset of `selene_core::Value` (the storage type), adding graph-native
//! types (Node, Edge, Path) and richer temporal handling (ZonedDateTime).
//!
//! Conversion:
//! - `Value → GqlValue`: lossless, used on every property read.
//! - `GqlValue → Value`: used on INSERT/SET, fails for graph-native types.

use std::sync::Arc;

use selene_core::{EdgeId, NodeId, Value};

use super::error::GqlError;

/// GQL type descriptor for schema, plan-time type checking,
/// list element typing, CAST validation, and RETURN column schemas.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GqlType {
    Bool,
    Int,
    UInt,
    Float,
    String,
    ZonedDateTime,
    Date,
    LocalDateTime,
    ZonedTime,
    LocalTime,
    Duration,
    Bytes,
    List(Box<GqlType>),
    Path,
    Node,
    Edge,
    Null,
    /// Dense float vector (embeddings).
    Vector,
    /// Record (ordered named fields). ISO GQL RECORD type.
    Record,
    /// Empty list element type, subtype of all types.
    Nothing,
}

impl From<selene_core::ValueType> for GqlType {
    fn from(vt: selene_core::ValueType) -> Self {
        match vt {
            selene_core::ValueType::Bool => GqlType::Bool,
            selene_core::ValueType::Int => GqlType::Int,
            selene_core::ValueType::UInt => GqlType::UInt,
            selene_core::ValueType::Float => GqlType::Float,
            selene_core::ValueType::String => GqlType::String,
            selene_core::ValueType::ZonedDateTime => GqlType::ZonedDateTime,
            selene_core::ValueType::Date => GqlType::Date,
            selene_core::ValueType::LocalDateTime => GqlType::LocalDateTime,
            selene_core::ValueType::Duration => GqlType::Duration,
            selene_core::ValueType::Bytes => GqlType::Bytes,
            selene_core::ValueType::Vector => GqlType::Vector,
            selene_core::ValueType::List => GqlType::List(Box::new(GqlType::Nothing)),
            selene_core::ValueType::Any => GqlType::String,
        }
    }
}

impl std::fmt::Display for GqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool => f.write_str("BOOL"),
            Self::Int => f.write_str("INT64"),
            Self::UInt => f.write_str("UINT64"),
            Self::Float => f.write_str("DOUBLE"),
            Self::String => f.write_str("STRING"),
            Self::ZonedDateTime => f.write_str("ZONED DATETIME"),
            Self::Date => f.write_str("DATE"),
            Self::LocalDateTime => f.write_str("LOCAL DATETIME"),
            Self::ZonedTime => f.write_str("ZONED TIME"),
            Self::LocalTime => f.write_str("LOCAL TIME"),
            Self::Duration => f.write_str("DURATION"),
            Self::Bytes => f.write_str("BYTES"),
            Self::Vector => f.write_str("VECTOR"),
            Self::List(inner) => write!(f, "LIST<{inner}>"),
            Self::Path => f.write_str("PATH"),
            Self::Node => f.write_str("NODE"),
            Self::Edge => f.write_str("EDGE"),
            Self::Null => f.write_str("NULL"),
            Self::Record => f.write_str("RECORD"),
            Self::Nothing => f.write_str("NOTHING"),
        }
    }
}

/// ISO 8601 zoned datetime.
///
/// Storage: nanos-since-epoch (i64), identical to `selene_core::Value::Timestamp`.
/// Runtime: adds timezone offset for display, comparison, and arithmetic.
///
/// Conversion from storage is zero-cost: `ZonedDateTime { nanos: ts, offset_seconds: 0 }`.
/// TS hot tier stores i64 nanos with no timezone overhead on billions of samples.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ZonedDateTime {
    /// Nanoseconds since Unix epoch (UTC).
    pub nanos: i64,
    /// UTC offset in seconds. UTC = 0, EST = -18000, CET = 3600, JST = 32400.
    pub offset_seconds: i32,
}

impl ZonedDateTime {
    /// Create a UTC datetime from nanos.
    pub fn from_nanos_utc(nanos: i64) -> Self {
        Self {
            nanos,
            offset_seconds: 0,
        }
    }

    /// Compare by UTC nanos regardless of offset.
    pub fn cmp_absolute(&self, other: &Self) -> std::cmp::Ordering {
        self.nanos.cmp(&other.nanos)
    }
}

impl Eq for ZonedDateTime {}

impl std::fmt::Display for ZonedDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Full ISO 8601 calendar formatting
        let epoch_secs = self.nanos.div_euclid(1_000_000_000);
        let nanos_rem = self.nanos.rem_euclid(1_000_000_000) as u32;
        // Apply timezone offset for local display
        let local_secs = epoch_secs + i64::from(self.offset_seconds);
        let days = local_secs.div_euclid(86400) as i32;
        let day_secs = local_secs.rem_euclid(86400) as u32;
        let (y, mo, d) = selene_core::value::epoch_days_to_ymd(days);
        let h = day_secs / 3600;
        let mi = (day_secs % 3600) / 60;
        let s = day_secs % 60;
        write!(f, "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}")?;
        if nanos_rem > 0 {
            let ms = nanos_rem / 1_000_000;
            if nanos_rem.is_multiple_of(1_000_000) {
                write!(f, ".{ms:03}")?;
            } else if nanos_rem.is_multiple_of(1000) {
                write!(f, ".{:06}", nanos_rem / 1000)?;
            } else {
                write!(f, ".{nanos_rem:09}")?;
            }
        }
        if self.offset_seconds == 0 {
            write!(f, "Z")
        } else {
            let sign = if self.offset_seconds >= 0 { '+' } else { '-' };
            let abs_off = self.offset_seconds.unsigned_abs();
            write!(
                f,
                "{sign}{:02}:{:02}",
                abs_off / 3600,
                (abs_off % 3600) / 60
            )
        }
    }
}

/// Calendar date (no time, no timezone). ISO GQL DATE type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GqlDate {
    /// Days since 1970-01-01.
    pub days: i32,
}

impl std::fmt::Display for GqlDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (y, m, d) = selene_core::value::epoch_days_to_ymd(self.days);
        write!(f, "{y:04}-{m:02}-{d:02}")
    }
}

/// Local datetime without timezone. ISO GQL LOCAL DATETIME type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GqlLocalDateTime {
    /// Nanoseconds since epoch (timezone-unaware).
    pub nanos: i64,
}

/// Zoned time-of-day with timezone offset. ISO GQL ZONED TIME type.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GqlZonedTime {
    /// Nanoseconds since midnight.
    pub nanos: u64,
    /// UTC offset in seconds.
    pub offset_seconds: i32,
}

/// Local time-of-day without timezone. ISO GQL LOCAL TIME type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GqlLocalTime {
    /// Nanoseconds since midnight.
    pub nanos: u64,
}

/// Temporal duration. ISO GQL DURATION type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GqlDuration {
    /// Day-and-time based duration in nanoseconds. Positive or negative.
    pub nanos: i64,
}

impl GqlDuration {
    pub fn day_time(nanos: i64) -> Self {
        Self { nanos }
    }
}

impl std::fmt::Display for GqlDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_secs = self.nanos.unsigned_abs() / 1_000_000_000;
        let h = total_secs / 3600;
        let m = (total_secs % 3600) / 60;
        let s = total_secs % 60;
        let sign = if self.nanos < 0 { "-" } else { "" };
        write!(f, "{sign}PT{h}H{m}M{s}S")
    }
}

/// Ordered set of named fields. ISO GQL RECORD type.
/// Records appear in query results and expressions but cannot be stored as properties.
#[derive(Debug, Clone, PartialEq)]
pub struct GqlRecord {
    pub fields: Vec<(selene_core::IStr, GqlValue)>,
}

impl GqlRecord {
    pub fn get(&self, name: &str) -> Option<&GqlValue> {
        self.fields
            .iter()
            .find(|(k, _)| k.as_str() == name)
            .map(|(_, v)| v)
    }
}

impl std::fmt::Display for GqlRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        for (i, (k, v)) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {v}", k.as_str())?;
        }
        write!(f, "}}")
    }
}

/// A path through the graph: alternating sequence of nodes and edges.
///
/// Always starts and ends with a Node. Length is always 2n+1 where n is
/// the number of edges. A zero-length path has exactly one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GqlPath {
    pub elements: Vec<PathElement>,
}

impl GqlPath {
    /// Create a zero-length path (single node, no edges).
    pub fn single_node(node_id: NodeId) -> Self {
        Self {
            elements: vec![PathElement::Node(node_id)],
        }
    }

    /// Number of edges in the path.
    pub fn edge_count(&self) -> usize {
        self.elements.len() / 2
    }

    /// Number of nodes in the path.
    pub fn node_count(&self) -> usize {
        self.elements.len().div_ceil(2)
    }

    /// Build from parallel node/edge vectors.
    /// nodes: [n0, n1, n2], edges: [e0, e1] → [n0, e0, n1, e1, n2]
    pub fn from_nodes_and_edges(nodes: &[NodeId], edges: &[EdgeId]) -> Self {
        let mut elements = Vec::with_capacity(nodes.len() + edges.len());
        for (i, &node) in nodes.iter().enumerate() {
            elements.push(PathElement::Node(node));
            if i < edges.len() {
                elements.push(PathElement::Edge(edges[i]));
            }
        }
        Self { elements }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathElement {
    Node(NodeId),
    Edge(EdgeId),
}

/// Typed homogeneous list per ISO GQL.
///
/// "Lists in graph can't contain elements of mixed types."
/// Empty lists have type `LIST<NOTHING>` and are assignable to any `LIST<T>`.
#[derive(Debug, Clone, PartialEq)]
pub struct GqlList {
    pub element_type: GqlType,
    pub elements: Arc<[GqlValue]>,
}

impl GqlList {
    /// Create an empty list with NOTHING element type.
    pub fn empty() -> Self {
        Self {
            element_type: GqlType::Nothing,
            elements: Arc::from(Vec::<GqlValue>::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

/// GQL runtime value with full ISO 39075 type parity.
#[derive(Debug, Clone, PartialEq)]
pub enum GqlValue {
    // === Immaterial ===
    Null,

    // === Predefined scalars ===
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(smol_str::SmolStr),

    // === Temporal instants ===
    ZonedDateTime(ZonedDateTime),
    Date(GqlDate),
    LocalDateTime(GqlLocalDateTime),
    ZonedTime(GqlZonedTime),
    LocalTime(GqlLocalTime),

    // === Temporal durations ===
    Duration(GqlDuration),

    // === Selene extension ===
    Bytes(Arc<[u8]>),
    /// Dense float vector (embeddings, feature vectors).
    Vector(Arc<[f32]>),

    // === Constructed ===
    List(GqlList),

    // === Constructed ===
    Record(GqlRecord),

    // === Graph-native (runtime only, never stored as properties) ===
    Node(NodeId),
    Edge(EdgeId),
    Path(GqlPath),
}

impl GqlValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Infer the GqlType of this value.
    pub fn gql_type(&self) -> GqlType {
        match self {
            Self::Null => GqlType::Null,
            Self::Bool(_) => GqlType::Bool,
            Self::Int(_) => GqlType::Int,
            Self::UInt(_) => GqlType::UInt,
            Self::Float(_) => GqlType::Float,
            Self::String(_) => GqlType::String,
            Self::ZonedDateTime(_) => GqlType::ZonedDateTime,
            Self::Date(_) => GqlType::Date,
            Self::LocalDateTime(_) => GqlType::LocalDateTime,
            Self::ZonedTime(_) => GqlType::ZonedTime,
            Self::LocalTime(_) => GqlType::LocalTime,
            Self::Duration(_) => GqlType::Duration,
            Self::Bytes(_) => GqlType::Bytes,
            Self::Vector(_) => GqlType::Vector,
            Self::Record(_) => GqlType::Record,
            Self::List(l) => GqlType::List(Box::new(l.element_type.clone())),
            Self::Node(_) => GqlType::Node,
            Self::Edge(_) => GqlType::Edge,
            Self::Path(_) => GqlType::Path,
        }
    }

    /// Convenience: extract i64 or error.
    pub fn as_int(&self) -> Result<i64, GqlError> {
        match self {
            Self::Int(i) => Ok(*i),
            Self::UInt(u) => i64::try_from(*u)
                .map_err(|_| GqlError::type_error("UINT64 value exceeds INT64 range")),
            other => Err(GqlError::type_error(format!(
                "expected INT64, got {}",
                other.gql_type()
            ))),
        }
    }

    /// Convenience: extract &str or error.
    pub fn as_str(&self) -> Result<&str, GqlError> {
        match self {
            Self::String(s) => Ok(s),
            other => Err(GqlError::type_error(format!(
                "expected STRING, got {}",
                other.gql_type()
            ))),
        }
    }

    /// Convenience: extract f64 or error (coerces Int/UInt to Float).
    pub fn as_float(&self) -> Result<f64, GqlError> {
        match self {
            Self::Float(f) => Ok(*f),
            Self::Int(i) => Ok(*i as f64),
            Self::UInt(u) => Ok(*u as f64),
            other => Err(GqlError::type_error(format!(
                "expected DOUBLE, got {}",
                other.gql_type()
            ))),
        }
    }

    /// Convenience: extract ZonedDateTime or error.
    pub fn as_zoned_datetime(&self) -> Result<&ZonedDateTime, GqlError> {
        match self {
            Self::ZonedDateTime(zdt) => Ok(zdt),
            other => Err(GqlError::type_error(format!(
                "expected ZONED DATETIME, got {}",
                other.gql_type()
            ))),
        }
    }

    /// Convenience: extract NodeId or error.
    pub fn as_node_id(&self) -> Result<NodeId, GqlError> {
        match self {
            Self::Node(id) => Ok(*id),
            other => Err(GqlError::type_error(format!(
                "expected NODE, got {}",
                other.gql_type()
            ))),
        }
    }

    /// Convenience: extract bool or error.
    pub fn as_bool(&self) -> Result<bool, GqlError> {
        match self {
            Self::Bool(b) => Ok(*b),
            other => Err(GqlError::type_error(format!(
                "expected BOOL, got {}",
                other.gql_type()
            ))),
        }
    }
}

impl std::fmt::Display for GqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Bool(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
            Self::Int(i) => write!(f, "{i}"),
            Self::UInt(u) => write!(f, "{u}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::String(s) => write!(f, "'{s}'"),
            Self::ZonedDateTime(zdt) => write!(f, "{zdt}"),
            Self::Date(d) => write!(f, "{d}"),
            Self::LocalDateTime(dt) => {
                let secs = dt.nanos.div_euclid(1_000_000_000);
                let (y, mo, d) =
                    selene_core::value::epoch_days_to_ymd(secs.div_euclid(86400) as i32);
                let ds = secs.rem_euclid(86400) as u32;
                write!(
                    f,
                    "{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}",
                    ds / 3600,
                    (ds % 3600) / 60,
                    ds % 60
                )
            }
            Self::ZonedTime(t) => {
                let total_secs = (t.nanos / 1_000_000_000) as u32;
                let h = total_secs / 3600;
                let m = (total_secs % 3600) / 60;
                let s = total_secs % 60;
                let oh = t.offset_seconds / 3600;
                let om = (t.offset_seconds.abs() % 3600) / 60;
                if t.offset_seconds == 0 {
                    write!(f, "{h:02}:{m:02}:{s:02}Z")
                } else {
                    write!(f, "{h:02}:{m:02}:{s:02}{oh:+03}:{om:02}")
                }
            }
            Self::LocalTime(t) => {
                let total_secs = (t.nanos / 1_000_000_000) as u32;
                write!(
                    f,
                    "{:02}:{:02}:{:02}",
                    total_secs / 3600,
                    (total_secs % 3600) / 60,
                    total_secs % 60
                )
            }
            Self::Duration(d) => write!(f, "{d}"),
            Self::Bytes(b) => write!(f, "bytes[{}]", b.len()),
            Self::Vector(v) => write!(f, "vector[{}]", v.len()),
            Self::Record(r) => write!(f, "{r}"),
            Self::List(l) => write!(f, "list[{}]", l.len()),
            Self::Node(id) => write!(f, "node({id})", id = id.0),
            Self::Edge(id) => write!(f, "edge({id})", id = id.0),
            Self::Path(p) => write!(f, "path[{} edges]", p.edge_count()),
        }
    }
}

// ── Value ↔ GqlValue conversion ──

impl From<&Value> for GqlValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => GqlValue::Null,
            Value::Bool(b) => GqlValue::Bool(*b),
            Value::Int(i) => GqlValue::Int(*i),
            Value::UInt(u) => GqlValue::UInt(*u),
            Value::Float(f) => GqlValue::Float(*f),
            Value::String(s) => GqlValue::String(s.clone()),
            Value::InternedStr(s) => GqlValue::String(smol_str::SmolStr::new(s.as_str())),
            Value::Timestamp(t) => GqlValue::ZonedDateTime(ZonedDateTime::from_nanos_utc(*t)),
            Value::Bytes(b) => GqlValue::Bytes(Arc::clone(b)),
            Value::Vector(v) => GqlValue::Vector(Arc::clone(v)),
            Value::Date(d) => GqlValue::Date(GqlDate { days: *d }),
            Value::LocalDateTime(n) => GqlValue::LocalDateTime(GqlLocalDateTime { nanos: *n }),
            Value::Duration(n) => GqlValue::Duration(GqlDuration { nanos: *n }),
            Value::List(l) => {
                let elements: Arc<[GqlValue]> = l.iter().map(GqlValue::from).collect();
                let element_type = infer_list_element_type(&elements);
                GqlValue::List(GqlList {
                    element_type,
                    elements,
                })
            }
        }
    }
}

impl TryFrom<&GqlValue> for Value {
    type Error = GqlError;

    fn try_from(v: &GqlValue) -> Result<Self, GqlError> {
        match v {
            GqlValue::Null => Ok(Value::Null),
            GqlValue::Bool(b) => Ok(Value::Bool(*b)),
            GqlValue::Int(i) => Ok(Value::Int(*i)),
            GqlValue::UInt(u) => Ok(Value::UInt(*u)),
            GqlValue::Float(f) => Ok(Value::Float(*f)),
            GqlValue::String(s) => Ok(Value::String(s.clone())),
            GqlValue::ZonedDateTime(zdt) => Ok(Value::Timestamp(zdt.nanos)),
            GqlValue::Bytes(b) => Ok(Value::Bytes(Arc::clone(b))),
            GqlValue::Vector(v) => Ok(Value::Vector(Arc::clone(v))),
            GqlValue::Date(d) => Ok(Value::Date(d.days)),
            GqlValue::LocalDateTime(dt) => Ok(Value::LocalDateTime(dt.nanos)),
            GqlValue::Duration(d) => Ok(Value::Duration(d.nanos)),
            GqlValue::ZonedTime(_) | GqlValue::LocalTime(_) => Err(GqlError::type_error(
                "TIME types cannot be stored as properties (GQL expression-only)",
            )),
            GqlValue::List(l) => {
                let values: Result<Vec<Value>, _> =
                    l.elements.iter().map(Value::try_from).collect();
                Ok(Value::List(Arc::from(values?)))
            }
            GqlValue::Record(_) | GqlValue::Node(_) | GqlValue::Edge(_) | GqlValue::Path(_) => Err(
                GqlError::type_error("cannot store graph reference/record value as a property"),
            ),
        }
    }
}

/// Infer the element type of a list from its contents.
pub(crate) fn infer_list_element_type(elements: &[GqlValue]) -> GqlType {
    for elem in elements {
        if !elem.is_null() {
            return elem.gql_type();
        }
    }
    GqlType::Nothing
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_str::SmolStr;

    // ── GqlType ──

    #[test]
    fn gql_type_display() {
        assert_eq!(format!("{}", GqlType::Bool), "BOOL");
        assert_eq!(format!("{}", GqlType::Int), "INT64");
        assert_eq!(format!("{}", GqlType::UInt), "UINT64");
        assert_eq!(format!("{}", GqlType::Float), "DOUBLE");
        assert_eq!(format!("{}", GqlType::String), "STRING");
        assert_eq!(format!("{}", GqlType::ZonedDateTime), "ZONED DATETIME");
        assert_eq!(
            format!("{}", GqlType::List(Box::new(GqlType::Int))),
            "LIST<INT64>"
        );
        assert_eq!(format!("{}", GqlType::Nothing), "NOTHING");
    }

    // ── GqlValue construction ──

    #[test]
    fn value_is_null() {
        assert!(GqlValue::Null.is_null());
        assert!(!GqlValue::Int(0).is_null());
        assert!(!GqlValue::Bool(false).is_null());
    }

    #[test]
    fn value_gql_type() {
        assert_eq!(GqlValue::Null.gql_type(), GqlType::Null);
        assert_eq!(GqlValue::Bool(true).gql_type(), GqlType::Bool);
        assert_eq!(GqlValue::Int(42).gql_type(), GqlType::Int);
        assert_eq!(GqlValue::UInt(42).gql_type(), GqlType::UInt);
        assert_eq!(GqlValue::Float(3.14).gql_type(), GqlType::Float);
        assert_eq!(
            GqlValue::String(SmolStr::new("hi")).gql_type(),
            GqlType::String
        );
        assert_eq!(GqlValue::Node(NodeId(1)).gql_type(), GqlType::Node);
    }

    #[test]
    fn value_display() {
        assert_eq!(format!("{}", GqlValue::Null), "NULL");
        assert_eq!(format!("{}", GqlValue::Bool(true)), "TRUE");
        assert_eq!(format!("{}", GqlValue::Int(42)), "42");
        assert_eq!(format!("{}", GqlValue::UInt(42)), "42");
        assert_eq!(
            format!("{}", GqlValue::String(SmolStr::new("hello"))),
            "'hello'"
        );
        assert_eq!(format!("{}", GqlValue::Node(NodeId(5))), "node(5)");
    }

    // ── ZonedDateTime ──

    #[test]
    fn zoned_datetime_utc() {
        let zdt = ZonedDateTime::from_nanos_utc(1_000_000_000);
        assert_eq!(zdt.offset_seconds, 0);
        assert_eq!(format!("{zdt}"), "1970-01-01T00:00:01Z");
    }

    #[test]
    fn zoned_datetime_with_offset() {
        let zdt = ZonedDateTime {
            nanos: 1_000_000_000,
            offset_seconds: 3600,
        };
        // 1 second UTC = 01:00:01 in +01:00
        assert_eq!(format!("{zdt}"), "1970-01-01T01:00:01+01:00");
    }

    #[test]
    fn zoned_datetime_absolute_comparison() {
        let utc = ZonedDateTime {
            nanos: 100,
            offset_seconds: 0,
        };
        let est = ZonedDateTime {
            nanos: 100,
            offset_seconds: -18000,
        };
        // Same absolute time regardless of offset
        assert_eq!(utc.cmp_absolute(&est), std::cmp::Ordering::Equal);
    }

    // ── GqlPath ──

    #[test]
    fn path_single_node() {
        let path = GqlPath::single_node(NodeId(1));
        assert_eq!(path.edge_count(), 0);
        assert_eq!(path.node_count(), 1);
    }

    #[test]
    fn path_from_nodes_and_edges() {
        let path = GqlPath::from_nodes_and_edges(
            &[NodeId(1), NodeId(2), NodeId(3)],
            &[EdgeId(10), EdgeId(20)],
        );
        assert_eq!(path.edge_count(), 2);
        assert_eq!(path.node_count(), 3);
        assert_eq!(path.elements.len(), 5); // n, e, n, e, n
    }

    // ── GqlList ──

    #[test]
    fn list_empty() {
        let list = GqlList::empty();
        assert!(list.is_empty());
        assert_eq!(list.element_type, GqlType::Nothing);
    }

    #[test]
    fn list_with_elements() {
        let list = GqlList {
            element_type: GqlType::Int,
            elements: Arc::from(vec![GqlValue::Int(1), GqlValue::Int(2)]),
        };
        assert_eq!(list.len(), 2);
    }

    // ── Value → GqlValue conversion ──

    #[test]
    fn from_value_null() {
        assert_eq!(GqlValue::from(&Value::Null), GqlValue::Null);
    }

    #[test]
    fn from_value_bool() {
        assert_eq!(GqlValue::from(&Value::Bool(true)), GqlValue::Bool(true));
    }

    #[test]
    fn from_value_int() {
        assert_eq!(GqlValue::from(&Value::Int(42)), GqlValue::Int(42));
    }

    #[test]
    fn from_value_uint() {
        assert_eq!(GqlValue::from(&Value::UInt(42)), GqlValue::UInt(42));
    }

    #[test]
    fn from_value_float() {
        assert_eq!(GqlValue::from(&Value::Float(3.14)), GqlValue::Float(3.14));
    }

    #[test]
    fn from_value_timestamp_becomes_zoned_datetime() {
        let ts = 1_700_000_000_000_000_000i64;
        let gql = GqlValue::from(&Value::Timestamp(ts));
        match gql {
            GqlValue::ZonedDateTime(zdt) => {
                assert_eq!(zdt.nanos, ts);
                assert_eq!(zdt.offset_seconds, 0); // UTC assumed
            }
            _ => panic!("expected ZonedDateTime"),
        }
    }

    #[test]
    fn from_value_list() {
        let v = Value::List(Arc::from(vec![Value::Int(1), Value::Int(2)]));
        let gql = GqlValue::from(&v);
        match gql {
            GqlValue::List(l) => {
                assert_eq!(l.element_type, GqlType::Int);
                assert_eq!(l.len(), 2);
            }
            _ => panic!("expected List"),
        }
    }

    // ── GqlValue → Value conversion ──

    #[test]
    fn to_value_scalars() {
        assert_eq!(Value::try_from(&GqlValue::Null).unwrap(), Value::Null);
        assert_eq!(
            Value::try_from(&GqlValue::Bool(true)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(Value::try_from(&GqlValue::Int(42)).unwrap(), Value::Int(42));
        assert_eq!(
            Value::try_from(&GqlValue::UInt(42)).unwrap(),
            Value::UInt(42)
        );
    }

    #[test]
    fn to_value_zoned_datetime_drops_offset() {
        let zdt = ZonedDateTime {
            nanos: 1000,
            offset_seconds: 3600,
        };
        let v = Value::try_from(&GqlValue::ZonedDateTime(zdt)).unwrap();
        assert_eq!(v, Value::Timestamp(1000));
    }

    #[test]
    fn to_value_node_fails() {
        let result = Value::try_from(&GqlValue::Node(NodeId(1)));
        assert!(result.is_err());
    }

    #[test]
    fn to_value_edge_fails() {
        let result = Value::try_from(&GqlValue::Edge(EdgeId(1)));
        assert!(result.is_err());
    }

    #[test]
    fn to_value_path_fails() {
        let path = GqlPath::single_node(NodeId(1));
        let result = Value::try_from(&GqlValue::Path(path));
        assert!(result.is_err());
    }

    // ── Accessor methods ──

    #[test]
    fn as_int_from_int() {
        assert_eq!(GqlValue::Int(42).as_int().unwrap(), 42);
    }

    #[test]
    fn as_int_from_uint() {
        assert_eq!(GqlValue::UInt(42).as_int().unwrap(), 42);
    }

    #[test]
    fn as_int_from_uint_overflow() {
        let result = GqlValue::UInt(u64::MAX).as_int();
        assert!(result.is_err());
    }

    #[test]
    fn as_str_success() {
        let v = GqlValue::String(SmolStr::new("hello"));
        assert_eq!(v.as_str().unwrap(), "hello");
    }

    #[test]
    fn as_float_coerces_int() {
        assert_eq!(GqlValue::Int(42).as_float().unwrap(), 42.0);
    }

    // ── List element type inference ──

    #[test]
    fn infer_type_from_elements() {
        let elems: Vec<GqlValue> = vec![GqlValue::Int(1), GqlValue::Int(2)];
        assert_eq!(infer_list_element_type(&elems), GqlType::Int);
    }

    #[test]
    fn infer_type_skips_nulls() {
        let elems: Vec<GqlValue> = vec![GqlValue::Null, GqlValue::Float(1.0)];
        assert_eq!(infer_list_element_type(&elems), GqlType::Float);
    }

    #[test]
    fn infer_type_all_nulls() {
        let elems: Vec<GqlValue> = vec![GqlValue::Null, GqlValue::Null];
        assert_eq!(infer_list_element_type(&elems), GqlType::Nothing);
    }
}
