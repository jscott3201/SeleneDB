//! Property value types -- Arrow-native, domain-agnostic.
//!
//! Uses `SmolStr` for string values: strings <= 22 bytes are stored inline
//! (no heap allocation). IoT property values ("degF", "psi", "active",
//! "zone_1a", "normal") overwhelmingly fit inline.

use std::sync::Arc;

use smol_str::SmolStr;

use crate::geometry::GeometryValue;
use crate::interner::IStr;

/// A property value. Every variant maps cleanly to an Arrow data type.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(SmolStr),
    Timestamp(i64),
    Bytes(Arc<[u8]>),
    List(Arc<[Value]>),
    /// Unsigned 64-bit integer (GQL UINT64).
    UInt(u64),
    /// Calendar date (days since 1970-01-01, ISO GQL DATE).
    Date(i32),
    /// Local datetime without timezone (nanos since epoch, ISO GQL LOCAL DATETIME).
    LocalDateTime(i64),
    /// Day-and-time duration in nanoseconds (ISO GQL DURATION).
    Duration(i64),
    /// Dense float vector for embeddings and feature vectors. Zero-copy via Arc.
    Vector(Arc<[f32]>),
    /// Interned string value -- shared across all nodes with the same value.
    /// Uses IStr for zero-cost comparison and deduplication of repeated
    /// string property values longer than 22 bytes (SmolStr inlines shorter ones).
    InternedStr(IStr),
    /// Spatial geometry (point, polygon, line, multi-variants, collection).
    /// Wrapped in `Arc` so polygons with large coordinate rings clone cheaply
    /// through the mutation batcher and plan cache.
    Geometry(Arc<GeometryValue>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::UInt(a), Self::UInt(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::Timestamp(a), Self::Timestamp(b)) => a == b,
            (Self::Date(a), Self::Date(b)) => a == b,
            (Self::LocalDateTime(a), Self::LocalDateTime(b)) => a == b,
            (Self::Duration(a), Self::Duration(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::List(a), Self::List(b)) => a == b,
            (Self::Vector(a), Self::Vector(b)) => a == b,
            (Self::Geometry(a), Self::Geometry(b)) => **a == **b,

            // Cross-variant string equality: String and InternedStr represent
            // the same logical type. Dictionary encoding promotes String to
            // InternedStr on write, so both variants can coexist for the same
            // logical value.
            (Self::String(a), Self::String(b)) => a == b,
            (Self::InternedStr(a), Self::InternedStr(b)) => a == b,
            (Self::String(a), Self::InternedStr(b)) => a.as_str() == b.as_str(),
            (Self::InternedStr(a), Self::String(b)) => a.as_str() == b.as_str(),

            _ => false,
        }
    }
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::UInt(_) => "uint",
            Self::Float(_) => "float",
            Self::String(_) => "string",
            Self::Timestamp(_) => "timestamp",
            Self::Bytes(_) => "bytes",
            Self::List(_) => "list",
            Self::Date(_) => "date",
            Self::LocalDateTime(_) => "local_datetime",
            Self::Duration(_) => "duration",
            Self::Vector(_) => "vector",
            Self::InternedStr(_) => "string",
            Self::Geometry(_) => "geometry",
        }
    }

    /// Get the string content, whether stored as String or InternedStr.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s.as_str()),
            Self::InternedStr(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Convenience constructor for string values.
    pub fn str(s: &str) -> Self {
        Self::String(SmolStr::new(s))
    }

    /// Convenience constructor for vector values.
    pub fn vector(data: Vec<f32>) -> Self {
        Self::Vector(Arc::from(data))
    }

    /// Convenience constructor for geometry values.
    pub fn geometry(g: GeometryValue) -> Self {
        Self::Geometry(Arc::new(g))
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "null"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Int(i) => write!(f, "{i}"),
            Self::UInt(u) => write!(f, "{u}u"),
            Self::Float(v) => write!(f, "{v}"),
            Self::String(s) => write!(f, "{s}"),
            Self::Timestamp(t) => write!(f, "ts:{t}"),
            Self::Bytes(b) => write!(f, "bytes[{}]", b.len()),
            Self::List(l) => write!(f, "list[{}]", l.len()),
            Self::Date(days) => {
                // Convert epoch days to calendar date
                let (y, m, d) = epoch_days_to_ymd(*days);
                write!(f, "{y:04}-{m:02}-{d:02}")
            }
            Self::LocalDateTime(nanos) => {
                let secs = nanos.div_euclid(1_000_000_000);
                let ns = nanos.rem_euclid(1_000_000_000) as u32;
                let epoch_days = secs.div_euclid(86400);
                let (y, mo, d) =
                    epoch_days_to_ymd(i32::try_from(epoch_days).unwrap_or(if epoch_days >= 0 {
                        i32::MAX
                    } else {
                        i32::MIN
                    }));
                let day_secs = secs.rem_euclid(86400) as u32;
                let h = day_secs / 3600;
                let mi = (day_secs % 3600) / 60;
                let s = day_secs % 60;
                write!(f, "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}")?;
                if ns > 0 {
                    let ms = ns / 1_000_000;
                    if ns.is_multiple_of(1_000_000) {
                        write!(f, ".{ms:03}")
                    } else {
                        write!(f, ".{ns:09}")
                    }
                } else {
                    Ok(())
                }
            }
            Self::Duration(nanos) => {
                let total_secs = nanos.unsigned_abs() / 1_000_000_000;
                let h = total_secs / 3600;
                let m = (total_secs % 3600) / 60;
                let s = total_secs % 60;
                let sign = if *nanos < 0 { "-" } else { "" };
                write!(f, "{sign}PT{h}H{m}M{s}S")
            }
            Self::Vector(v) => write!(f, "vector[{}]", v.len()),
            Self::InternedStr(s) => write!(f, "{s}"),
            // Lossless — the GeoJSON string is stable for equal geometries and
            // is used by identity-sensitive callers (composite index keys in
            // selene-graph::typed_index). CRS is appended when set so that two
            // geometries with the same shape but different CRS produce
            // distinct string representations.
            Self::Geometry(g) => match &g.crs {
                Some(crs) => write!(f, "{}@{}", g.to_geojson(), crs.as_str()),
                None => write!(f, "{}", g.to_geojson()),
            },
        }
    }
}

/// Convert epoch days (since 1970-01-01) to (year, month, day).
/// Algorithm from Howard Hinnant's `chrono`-compatible civil calendar.
pub fn epoch_days_to_ymd(days: i32) -> (i32, u32, u32) {
    let z = i64::from(days) + 719_468; // shift epoch to 0000-03-01
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

/// Convenience conversions for Value::String.
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::String(SmolStr::new(s))
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::String(SmolStr::from(s))
    }
}

impl From<Arc<str>> for Value {
    fn from(s: Arc<str>) -> Self {
        Self::String(SmolStr::new(&*s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names() {
        assert_eq!(Value::Null.type_name(), "null");
        assert_eq!(Value::Bool(true).type_name(), "bool");
        assert_eq!(Value::Int(42).type_name(), "int");
        assert_eq!(Value::UInt(42).type_name(), "uint");
        assert_eq!(Value::Float(3.15).type_name(), "float");
        assert_eq!(Value::str("hi").type_name(), "string");
        assert_eq!(Value::Timestamp(0).type_name(), "timestamp");
        assert_eq!(Value::Bytes(Arc::from(vec![1u8])).type_name(), "bytes");
        assert_eq!(Value::List(Arc::from(vec![])).type_name(), "list");
    }

    #[test]
    fn display_values() {
        assert_eq!(format!("{}", Value::Int(42)), "42");
        assert_eq!(format!("{}", Value::UInt(42)), "42u");
        assert_eq!(format!("{}", Value::str("hello")), "hello");
        assert_eq!(format!("{}", Value::Bool(true)), "true");
        assert_eq!(format!("{}", Value::Null), "null");
    }

    #[test]
    fn is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int(0).is_null());
        assert!(!Value::UInt(0).is_null());
        assert!(!Value::Bool(false).is_null());
    }

    #[test]
    fn list_value() {
        let list = Value::List(Arc::from(vec![Value::Int(1), Value::Int(2)]));
        assert_eq!(list.type_name(), "list");
        assert_eq!(format!("{list}"), "list[2]");
    }

    #[test]
    fn from_str() {
        let v: Value = "hello".into();
        assert!(matches!(v, Value::String(_)));
    }

    #[test]
    fn str_convenience() {
        let v = Value::str("degF");
        assert_eq!(format!("{v}"), "degF");
    }

    #[test]
    fn vector_value() {
        let v = Value::vector(vec![0.1, 0.2, 0.3]);
        assert_eq!(v.type_name(), "vector");
        assert_eq!(format!("{v}"), "vector[3]");
    }

    #[test]
    fn geometry_value() {
        let g = Value::geometry(crate::GeometryValue::point_wgs84(-74.0, 40.7));
        assert_eq!(g.type_name(), "geometry");
        // Display is lossless: GeoJSON + CRS hint so identity-sensitive
        // callers (composite index keys) can't collide two distinct geometries.
        let s = format!("{g}");
        assert!(s.contains("\"Point\""));
        assert!(s.contains("-74"));
        assert!(s.contains("@EPSG:4326"));
    }

    #[test]
    fn geometry_display_distinguishes_crs() {
        let a = Value::geometry(crate::GeometryValue::point_wgs84(1.0, 2.0));
        let b = Value::geometry(crate::GeometryValue::point_planar(1.0, 2.0));
        assert_ne!(format!("{a}"), format!("{b}"));
    }

    #[test]
    fn geometry_value_round_trips_postcard() {
        let original = Value::geometry(crate::GeometryValue::point_wgs84(-74.0, 40.7));
        let bytes = postcard::to_allocvec(&original).expect("serialize");
        let decoded: Value = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    #[test]
    fn geometry_equality_ignores_arc_identity() {
        let a = Value::geometry(crate::GeometryValue::point_wgs84(1.0, 2.0));
        let b = Value::geometry(crate::GeometryValue::point_wgs84(1.0, 2.0));
        assert_eq!(a, b);
    }

    #[test]
    fn vector_equality() {
        let a = Value::vector(vec![1.0, 2.0, 3.0]);
        let b = Value::vector(vec![1.0, 2.0, 3.0]);
        let c = Value::vector(vec![1.0, 2.0, 4.0]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn vector_clone_is_cheap() {
        let v = Value::vector(vec![1.0; 384]);
        let v2 = v.clone();
        // Arc clone — same underlying data
        if let (Value::Vector(a), Value::Vector(b)) = (&v, &v2) {
            assert!(Arc::ptr_eq(a, b));
        } else {
            panic!("expected Vector");
        }
    }

    #[test]
    fn vector_serde_round_trip() {
        let v = Value::vector(vec![0.5, -1.0, 3.15]);
        let json = serde_json::to_string(&v).unwrap();
        let v2: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v, v2);
    }

    #[test]
    fn interned_str_type_name() {
        let v = Value::InternedStr(IStr::new("building_name"));
        assert_eq!(v.type_name(), "string");
    }

    #[test]
    fn interned_str_display() {
        let v = Value::InternedStr(IStr::new("zone_description"));
        assert_eq!(format!("{v}"), "zone_description");
    }

    #[test]
    fn interned_str_as_str() {
        let interned = Value::InternedStr(IStr::new("equipment_model"));
        assert_eq!(interned.as_str(), Some("equipment_model"));

        let smol = Value::str("equipment_model");
        assert_eq!(smol.as_str(), Some("equipment_model"));

        assert_eq!(Value::Int(42).as_str(), None);
    }

    #[test]
    fn interned_str_serde_round_trip() {
        let v = Value::InternedStr(IStr::new("round_trip_test"));
        let json = serde_json::to_string(&v).unwrap();
        let v2: Value = serde_json::from_str(&json).unwrap();
        // Deserializes as InternedStr (IStr's Deserialize interns the string)
        assert_eq!(v2.as_str(), Some("round_trip_test"));
    }

    #[test]
    fn string_interned_str_cross_variant_equality() {
        let smol = Value::String(SmolStr::new("test"));
        let interned = Value::InternedStr(IStr::new("test"));

        // Cross-variant equality in both directions
        assert_eq!(smol, interned);
        assert_eq!(interned, smol);

        // Same-variant equality still works
        assert_eq!(smol, Value::String(SmolStr::new("test")));
        assert_eq!(interned, Value::InternedStr(IStr::new("test")));

        // Different content is not equal
        let other = Value::InternedStr(IStr::new("other"));
        assert_ne!(smol, other);
        assert_ne!(interned, other);

        // Non-string variants unaffected
        assert_ne!(Value::Int(42), Value::String(SmolStr::new("42")));
    }
}
