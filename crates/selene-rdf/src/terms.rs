//! RDF term types and bidirectional Value/Literal conversion.
//!
//! Converts between Selene's `Value` enum and `oxrdf::Literal` with
//! appropriate XSD datatype annotations. Also defines `SeleneRdfTerm`
//! and `RdfPredicate` for internal RDF term representation.

use std::sync::Arc;

use oxrdf::{Literal, NamedNode, vocab::xsd};
use selene_core::interner::IStr;
use selene_core::value::{Value, epoch_days_to_ymd};
use selene_core::{EdgeId, NodeId};

// ---------------------------------------------------------------------------
// Custom datatype URIs
// ---------------------------------------------------------------------------

/// Custom Selene vector datatype URI for dense float vectors.
const SELENE_VECTOR_DATATYPE: &str = "urn:selene:vector";

/// GeoSPARQL wktLiteral datatype URI — primary geometry encoding on export.
/// Broadest SPARQL-engine support (Jena, RDF4J, Stardog, GraphDB).
const WKT_LITERAL_DATATYPE: &str = "http://www.opengis.net/ont/geosparql#wktLiteral";

/// GeoSPARQL geoJSONLiteral datatype URI. Accepted on import for
/// round-tripping and interop with web-native producers.
const GEOJSON_LITERAL_DATATYPE: &str = "http://www.opengis.net/ont/geosparql#geoJSONLiteral";

/// EPSG CRS URI prefix, used for non-WGS84 codes.
const GEO_EPSG_CRS_URI_PREFIX: &str = "http://www.opengis.net/def/crs/EPSG/0/";

/// OGC CRS84 URI — WGS84 with explicit (longitude, latitude) axis order.
/// This is the CRS we want to emit for Selene's `EPSG:4326` geometries,
/// since our coordinate order is (lng, lat). The EPSG:4326 code itself is
/// formally (lat, lon) per the EPSG database, so axis-order-aware engines
/// (Jena ARQ in strict mode) would otherwise flip our coordinates.
const OGC_CRS84_URI: &str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84";

/// Return a `NamedNode` for the custom `selene:vector` datatype.
fn selene_vector_datatype() -> NamedNode {
    NamedNode::new_unchecked(SELENE_VECTOR_DATATYPE)
}

/// Return a `NamedNode` for the GeoSPARQL wktLiteral datatype.
fn wkt_literal_datatype() -> NamedNode {
    NamedNode::new_unchecked(WKT_LITERAL_DATATYPE)
}

/// Return a `NamedNode` for the GeoSPARQL geoJSONLiteral datatype.
fn geojson_literal_datatype() -> NamedNode {
    NamedNode::new_unchecked(GEOJSON_LITERAL_DATATYPE)
}

/// Build the GeoSPARQL wktLiteral lexical form: an optional `<crs-uri>`
/// prefix followed by the WKT text.
///
/// - `EPSG:4326` → `<.../OGC/1.3/CRS84>` so axis-order-aware engines
///   interpret our (lng, lat) coordinates correctly. Selene stores WGS84
///   points as (lng, lat); the formal EPSG:4326 definition is (lat, lon).
///   CRS84 is WGS84 with explicit longitude-first axis order.
/// - Other `EPSG:<code>` values pass through as `<.../EPSG/0/<code>>`.
/// - Unset or non-EPSG CRS emits no prefix (spec default is CRS84).
fn format_wkt_literal(geom: &selene_core::geometry::GeometryValue) -> String {
    let wkt = geom.to_wkt();
    match geom.crs.as_ref().map(|c| c.as_str()) {
        Some("EPSG:4326") => format!("<{OGC_CRS84_URI}> {wkt}"),
        Some(crs) => match crs.strip_prefix("EPSG:") {
            Some(code) => format!("<{GEO_EPSG_CRS_URI_PREFIX}{code}> {wkt}"),
            None => wkt,
        },
        None => wkt,
    }
}

// ---------------------------------------------------------------------------
// Value -> Literal
// ---------------------------------------------------------------------------

/// Convert a Selene `Value` to an RDF `Literal` with appropriate XSD datatype.
///
/// Returns `None` for `Null` and `List` values. `Null` produces no triple;
/// `List` values are expanded into multiple triples by the mapping layer.
pub fn value_to_literal(value: &Value) -> Option<Literal> {
    match value {
        Value::Null => None,
        Value::Bool(b) => {
            let lexical = if *b { "true" } else { "false" };
            Some(Literal::new_typed_literal(lexical, xsd::BOOLEAN))
        }
        Value::Int(i) => Some(Literal::new_typed_literal(i.to_string(), xsd::LONG)),
        Value::UInt(u) => Some(Literal::new_typed_literal(
            u.to_string(),
            xsd::UNSIGNED_LONG,
        )),
        Value::Float(f) => Some(Literal::new_typed_literal(format_double(*f), xsd::DOUBLE)),
        Value::String(s) => Some(Literal::new_simple_literal(s.as_str())),
        Value::InternedStr(s) => Some(Literal::new_simple_literal(s.as_str())),
        Value::Timestamp(nanos) => {
            let iso = nanos_to_iso8601_utc(*nanos);
            Some(Literal::new_typed_literal(iso, xsd::DATE_TIME))
        }
        Value::Date(days) => {
            let (y, m, d) = epoch_days_to_ymd(*days);
            let lexical = format!("{y:04}-{m:02}-{d:02}");
            Some(Literal::new_typed_literal(lexical, xsd::DATE))
        }
        Value::LocalDateTime(nanos) => {
            let iso = nanos_to_iso8601_local(*nanos);
            Some(Literal::new_typed_literal(iso, xsd::DATE_TIME))
        }
        Value::Duration(nanos) => {
            let lexical = nanos_to_xsd_duration(*nanos);
            Some(Literal::new_typed_literal(lexical, xsd::DURATION))
        }
        Value::Bytes(b) => {
            use std::fmt::Write;
            let mut hex = String::with_capacity(b.len() * 2);
            for byte in b.iter() {
                let _ = write!(hex, "{byte:02X}");
            }
            Some(Literal::new_typed_literal(hex, xsd::HEX_BINARY))
        }
        Value::Vector(v) => {
            let csv: String = v.iter().enumerate().fold(String::new(), |mut acc, (i, f)| {
                if i > 0 {
                    acc.push(',');
                }
                use std::fmt::Write;
                let _ = write!(acc, "{f}");
                acc
            });
            Some(Literal::new_typed_literal(csv, selene_vector_datatype()))
        }
        Value::Geometry(g) => {
            // Points export as geo:wktLiteral (broadest engine support). Other
            // geometries export as geo:geoJSONLiteral for now, because our WKT
            // import path only parses POINT — round-tripping a polygon through
            // wktLiteral would silently lose data on reimport. When the WKT
            // parser grows to cover the full 2D set, switch this branch so
            // every geometry emits wktLiteral uniformly.
            if g.geometry_type() == "Point" {
                Some(Literal::new_typed_literal(
                    format_wkt_literal(g),
                    wkt_literal_datatype(),
                ))
            } else {
                Some(Literal::new_typed_literal(
                    g.to_geojson(),
                    geojson_literal_datatype(),
                ))
            }
        }
        Value::List(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Literal -> Value
// ---------------------------------------------------------------------------

/// Convert an RDF `Literal` back to a Selene `Value`.
///
/// Dispatches on the literal's datatype IRI. Unknown datatypes are stored
/// as `Value::String` with the lexical form preserved.
pub fn literal_to_value(literal: &Literal) -> Value {
    let dt = literal.datatype();
    let lex = literal.value();

    if dt == xsd::BOOLEAN {
        return match lex {
            "true" | "1" => Value::Bool(true),
            "false" | "0" => Value::Bool(false),
            _ => Value::Bool(false),
        };
    }

    if dt == xsd::LONG
        || dt == xsd::INTEGER
        || dt == xsd::INT
        || dt == xsd::SHORT
        || dt == xsd::BYTE
    {
        return lex
            .parse::<i64>()
            .map_or_else(|_| Value::str(lex), Value::Int);
    }

    if dt == xsd::UNSIGNED_LONG
        || dt == xsd::UNSIGNED_INT
        || dt == xsd::UNSIGNED_SHORT
        || dt == xsd::UNSIGNED_BYTE
    {
        return lex
            .parse::<u64>()
            .map_or_else(|_| Value::str(lex), Value::UInt);
    }

    if dt == xsd::DOUBLE || dt == xsd::FLOAT || dt == xsd::DECIMAL {
        return lex
            .parse::<f64>()
            .map_or_else(|_| Value::str(lex), Value::Float);
    }

    if dt == xsd::STRING {
        return Value::str(lex);
    }

    if dt == xsd::DATE_TIME || dt == xsd::DATE_TIME_STAMP {
        return parse_iso8601_to_nanos(lex).map_or_else(|| Value::str(lex), Value::Timestamp);
    }

    if dt == xsd::DATE {
        return parse_xsd_date_to_days(lex).map_or_else(|| Value::str(lex), Value::Date);
    }

    if dt == xsd::DURATION || dt == xsd::DAY_TIME_DURATION {
        return parse_xsd_duration_to_nanos(lex).map_or_else(|| Value::str(lex), Value::Duration);
    }

    if dt == xsd::HEX_BINARY {
        return parse_hex_to_bytes(lex)
            .map_or_else(|| Value::str(lex), |b| Value::Bytes(Arc::from(b)));
    }

    if dt == xsd::BASE_64_BINARY {
        use base64::Engine;
        return base64::engine::general_purpose::STANDARD
            .decode(lex)
            .map_or_else(|_| Value::str(lex), |bytes| Value::Bytes(Arc::from(bytes)));
    }

    // Custom selene:vector datatype.
    if dt.as_str() == SELENE_VECTOR_DATATYPE {
        return parse_vector(lex).map_or_else(|| Value::str(lex), Value::Vector);
    }

    // GeoSPARQL literals: WKT is the primary encoding on export, but accept
    // geoJSONLiteral too for round-trips and interop with web-native producers.
    if dt.as_str() == WKT_LITERAL_DATATYPE {
        return parse_wkt_literal(lex).map_or_else(|| Value::str(lex), Value::geometry);
    }
    if dt.as_str() == GEOJSON_LITERAL_DATATYPE {
        return selene_core::geometry::GeometryValue::from_geojson(lex)
            .map_or_else(|_| Value::str(lex), Value::geometry);
    }

    // Unknown datatype: preserve lexical form as string.
    Value::str(lex)
}

/// Parse the wktLiteral lexical form: an optional `<crs-uri>` prefix followed
/// by the WKT text.
///
/// Currently limited to the exporter-produced `POINT (x y)` shape. If the lex
/// starts with `<...>`, we extract and normalize the CRS IRI (OGC CRS84 and
/// EPSG:4326 both map to Selene's `EPSG:4326` tag). Unsupported WKT forms
/// return `None`, letting callers preserve the original lexical form as
/// `Value::str` until a fuller WKT parser is added.
fn parse_wkt_literal(lex: &str) -> Option<selene_core::geometry::GeometryValue> {
    let (crs_tag, body) = if let Some(rest) = lex.strip_prefix('<') {
        let end = rest.find('>')?;
        let uri = &rest[..end];
        let body = rest[end + 1..].trim_start();
        let tag = normalize_crs_uri(uri)?;
        (Some(tag), body)
    } else {
        (None, lex.trim_start())
    };

    let (x, y) = parse_wkt_point(body)?;
    let mut g = selene_core::geometry::GeometryValue::point_planar(x, y);
    g.crs = crs_tag.map(|t| selene_core::interner::IStr::new(&t));
    Some(g)
}

/// Map a GeoSPARQL CRS IRI back to Selene's short CRS tag. Both CRS84 and
/// EPSG:4326 normalize to `EPSG:4326` since Selene treats WGS84 as a single
/// (lng, lat) system. Returns `None` for URIs we don't recognize.
fn normalize_crs_uri(uri: &str) -> Option<String> {
    if uri == OGC_CRS84_URI {
        return Some("EPSG:4326".to_string());
    }
    uri.strip_prefix(GEO_EPSG_CRS_URI_PREFIX)
        .map(|code| format!("EPSG:{code}"))
}

/// Parse `POINT (x y)` — exporter's shape. Whitespace-tolerant.
fn parse_wkt_point(s: &str) -> Option<(f64, f64)> {
    let rest = s.trim().strip_prefix("POINT")?.trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?.trim();
    let mut parts = inner.split_ascii_whitespace();
    let x: f64 = parts.next()?.parse().ok()?;
    let y: f64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((x, y))
}

// ---------------------------------------------------------------------------
// SeleneRdfTerm (sparql feature only)
// ---------------------------------------------------------------------------

/// Internal RDF term representation for the Selene RDF layer.
///
/// Bridges Selene's typed entity model with RDF's term model. Used by
/// the mapping layer (Task 4) for triple generation and by the SPARQL
/// adapter (Task 14) for query evaluation.
#[derive(Clone, Debug)]
pub enum SeleneRdfTerm {
    /// A graph node, identified by Selene `NodeId`.
    Node(NodeId),
    /// An edge reified as an RDF resource, identified by Selene `EdgeId`.
    EdgeReifier(EdgeId),
    /// A label used as an RDF class (rdf:type target).
    Type(IStr),
    /// A predicate (property key, edge label, or well-known URI).
    Predicate(RdfPredicate),
    /// A literal value converted from Selene `Value`.
    Literal(Value),
    /// A SOSA Observation node (sensor node + observed property).
    Observation(NodeId, IStr),
    /// An external ontology term not mapped to Selene entities.
    OntologyTerm(oxrdf::Term),
}

impl PartialEq for SeleneRdfTerm {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Node(a), Self::Node(b)) => a == b,
            (Self::EdgeReifier(a), Self::EdgeReifier(b)) => a == b,
            (Self::Type(a), Self::Type(b)) => a == b,
            (Self::Predicate(a), Self::Predicate(b)) => a == b,
            (Self::Literal(a), Self::Literal(b)) => value_eq(a, b),
            (Self::Observation(a1, a2), Self::Observation(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::OntologyTerm(a), Self::OntologyTerm(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for SeleneRdfTerm {}

impl std::hash::Hash for SeleneRdfTerm {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Node(id) => id.hash(state),
            Self::EdgeReifier(id) => id.hash(state),
            Self::Type(s) => s.hash(state),
            Self::Predicate(p) => p.hash(state),
            Self::Literal(v) => value_hash(v, state),
            Self::Observation(id, s) => {
                id.hash(state);
                s.hash(state);
            }
            Self::OntologyTerm(t) => t.hash(state),
        }
    }
}

impl SeleneRdfTerm {
    /// Convert an `oxrdf::Term` (e.g. from a SPARQL query) into a
    /// `SeleneRdfTerm` that references Selene-native structures.
    ///
    /// URI matching priority:
    /// 1. Well-known predicates: `rdf:type`, SOSA terms.
    /// 2. Namespace-prefixed URIs: `node/`, `type/`, `prop/`, `rel/`,
    ///    `edge/`, `obs/`.
    /// 3. External named nodes fall through to `OntologyTerm`.
    /// 4. Literals are converted via `literal_to_value`.
    /// 5. Blank nodes are wrapped as `OntologyTerm`.
    pub fn internalize(term: oxrdf::Term, ns: &crate::namespace::RdfNamespace) -> Self {
        use crate::namespace::{ParsedUri, RDF_TYPE, SOSA_NS};

        match term {
            oxrdf::Term::NamedNode(nn) => {
                let uri = nn.as_str();

                // Well-known predicates.
                if uri == RDF_TYPE {
                    return Self::Predicate(RdfPredicate::RdfType);
                }

                // SOSA predicates.
                if let Some(local) = uri.strip_prefix(SOSA_NS) {
                    return match local {
                        "hasSimpleResult" => Self::Predicate(RdfPredicate::SosaResult),
                        "resultTime" => Self::Predicate(RdfPredicate::SosaTime),
                        "madeBySensor" => Self::Predicate(RdfPredicate::SosaSensor),
                        "observedProperty" => Self::Predicate(RdfPredicate::SosaProperty),
                        _ => Self::Predicate(RdfPredicate::External(nn)),
                    };
                }

                // Namespace-prefixed URIs. ParsedUri variants already carry
                // IStr, so we can use the interned handles directly.
                if let Some(parsed) = ns.parse(uri) {
                    return match parsed {
                        ParsedUri::Node(id) => Self::Node(id),
                        ParsedUri::Type(label) => Self::Type(label),
                        ParsedUri::Property(key) => Self::Predicate(RdfPredicate::PropertyKey(key)),
                        ParsedUri::Relationship(label) => {
                            Self::Predicate(RdfPredicate::EdgeLabel(label))
                        }
                        ParsedUri::Edge(id) => Self::EdgeReifier(id),
                        ParsedUri::Observation(nid, prop) => Self::Observation(nid, prop),
                    };
                }

                // Unrecognized named node -- external ontology term.
                Self::OntologyTerm(oxrdf::Term::NamedNode(nn))
            }
            oxrdf::Term::Literal(lit) => Self::Literal(literal_to_value(&lit)),
            oxrdf::Term::BlankNode(bn) => Self::OntologyTerm(oxrdf::Term::BlankNode(bn)),
        }
    }

    /// Convert a `SeleneRdfTerm` back to an `oxrdf::Term` for SPARQL result
    /// serialization.
    pub fn externalize(&self, ns: &crate::namespace::RdfNamespace) -> oxrdf::Term {
        match self {
            Self::Node(id) => oxrdf::Term::NamedNode(ns.node_uri(*id)),
            Self::EdgeReifier(id) => oxrdf::Term::NamedNode(ns.edge_uri(*id)),
            Self::Type(label) => oxrdf::Term::NamedNode(ns.type_uri(label.as_str())),
            Self::Predicate(pred) => oxrdf::Term::NamedNode(match pred {
                RdfPredicate::RdfType => crate::namespace::RdfNamespace::rdf_type().clone(),
                RdfPredicate::PropertyKey(k) => ns.prop_uri(k.as_str()),
                RdfPredicate::EdgeLabel(l) => ns.rel_uri(l.as_str()),
                RdfPredicate::SosaResult => {
                    crate::namespace::RdfNamespace::sosa_uri("hasSimpleResult")
                }
                RdfPredicate::SosaTime => crate::namespace::RdfNamespace::sosa_uri("resultTime"),
                RdfPredicate::SosaSensor => {
                    crate::namespace::RdfNamespace::sosa_uri("madeBySensor")
                }
                RdfPredicate::SosaProperty => {
                    crate::namespace::RdfNamespace::sosa_uri("observedProperty")
                }
                RdfPredicate::External(nn) => nn.clone(),
            }),
            Self::Literal(v) => {
                // Null maps to empty string literal.
                let lit =
                    value_to_literal(v).unwrap_or_else(|| oxrdf::Literal::new_simple_literal(""));
                oxrdf::Term::Literal(lit)
            }
            Self::Observation(nid, prop) => oxrdf::Term::NamedNode(ns.obs_uri(*nid, prop.as_str())),
            Self::OntologyTerm(t) => t.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// RdfPredicate (sparql feature only)
// ---------------------------------------------------------------------------

/// Predicate types used in Selene RDF triple generation.
///
/// Separates well-known predicates (rdf:type, SOSA terms) from
/// Selene-minted predicates (property keys, edge labels) and
/// arbitrary external URIs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RdfPredicate {
    /// `rdf:type`
    RdfType,
    /// `sosa:hasResult`
    SosaResult,
    /// `sosa:resultTime`
    SosaTime,
    /// `sosa:madeBySensor`
    SosaSensor,
    /// `sosa:observedProperty`
    SosaProperty,
    /// An edge label used as a predicate: `<ns>rel/<label>`.
    EdgeLabel(IStr),
    /// A property key used as a predicate: `<ns>prop/<key>`.
    PropertyKey(IStr),
    /// An arbitrary external URI used as a predicate.
    External(NamedNode),
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

/// Decompose nanoseconds since epoch into date/time components.
///
/// Returns `(year, month, day, hour, minute, second, nanosecond)`.
/// Uses Howard Hinnant's civil date algorithm (via `epoch_days_to_ymd`)
/// to avoid pulling in chrono.
fn decompose_nanos(total_ns: i64) -> (i64, u8, u8, u8, u8, u8, u32) {
    let secs = total_ns.div_euclid(1_000_000_000);
    let ns = total_ns.rem_euclid(1_000_000_000) as u32;
    let epoch_days = secs.div_euclid(86400);
    let (y, mo, d) = epoch_days_to_ymd(i32::try_from(epoch_days).unwrap_or(if epoch_days >= 0 {
        i32::MAX
    } else {
        i32::MIN
    }));
    let day_secs = secs.rem_euclid(86400) as u32;
    let h = (day_secs / 3600) as u8;
    let mi = ((day_secs % 3600) / 60) as u8;
    let s = (day_secs % 60) as u8;
    (i64::from(y), mo as u8, d as u8, h, mi, s, ns)
}

/// Format decomposed date/time components with optional fractional seconds.
///
/// The `suffix` is appended after the time portion (e.g. "Z" for UTC, "" for local).
#[allow(clippy::too_many_arguments)]
fn format_datetime(y: i64, mo: u8, d: u8, h: u8, mi: u8, s: u8, ns: u32, suffix: &str) -> String {
    if ns > 0 {
        if ns.is_multiple_of(1_000_000) {
            let ms = ns / 1_000_000;
            format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{ms:03}{suffix}")
        } else {
            format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{ns:09}{suffix}")
        }
    } else {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}{suffix}")
    }
}

/// Convert nanoseconds since Unix epoch to ISO 8601 UTC string.
fn nanos_to_iso8601_utc(nanos: i64) -> String {
    let (y, mo, d, h, mi, s, ns) = decompose_nanos(nanos);
    format_datetime(y, mo, d, h, mi, s, ns, "Z")
}

/// Convert nanoseconds since epoch to ISO 8601 local datetime (no timezone).
fn nanos_to_iso8601_local(nanos: i64) -> String {
    let (y, mo, d, h, mi, s, ns) = decompose_nanos(nanos);
    format_datetime(y, mo, d, h, mi, s, ns, "")
}

/// Convert nanosecond duration to XSD duration string (`PT...S`).
fn nanos_to_xsd_duration(nanos: i64) -> String {
    let negative = nanos < 0;
    let abs_nanos = nanos.unsigned_abs();
    let total_secs = abs_nanos / 1_000_000_000;
    let frac_nanos = abs_nanos % 1_000_000_000;

    let prefix = if negative { "-" } else { "" };

    if frac_nanos == 0 {
        format!("{prefix}PT{total_secs}S")
    } else if frac_nanos.is_multiple_of(1_000_000) {
        let ms = frac_nanos / 1_000_000;
        format!("{prefix}PT{total_secs}.{ms:03}S")
    } else {
        format!("{prefix}PT{total_secs}.{frac_nanos:09}S")
    }
}

// ---------------------------------------------------------------------------
// Parsing helpers (Literal -> Value)
// ---------------------------------------------------------------------------

/// Parse an ISO 8601 datetime string to nanoseconds since epoch.
///
/// Supports formats:
/// - `YYYY-MM-DDThh:mm:ssZ`
/// - `YYYY-MM-DDThh:mm:ss.fracZ`
/// - `YYYY-MM-DDThh:mm:ss` (no timezone, treated as UTC)
/// - `YYYY-MM-DDThh:mm:ss+HH:MM` / `-HH:MM`
fn parse_iso8601_to_nanos(s: &str) -> Option<i64> {
    // Minimum: YYYY-MM-DDThh:mm:ss (19 chars)
    if s.len() < 19 {
        return None;
    }

    let y: i32 = s.get(0..4)?.parse().ok()?;
    if s.as_bytes().get(4)? != &b'-' {
        return None;
    }
    let mo: u32 = s.get(5..7)?.parse().ok()?;
    if s.as_bytes().get(7)? != &b'-' {
        return None;
    }
    let d: u32 = s.get(8..10)?.parse().ok()?;
    if s.as_bytes().get(10)? != &b'T' {
        return None;
    }
    let h: u32 = s.get(11..13)?.parse().ok()?;
    if s.as_bytes().get(13)? != &b':' {
        return None;
    }
    let mi: u32 = s.get(14..16)?.parse().ok()?;
    if s.as_bytes().get(16)? != &b':' {
        return None;
    }
    let sec: u32 = s.get(17..19)?.parse().ok()?;

    let rest = &s[19..];

    // Parse fractional seconds and timezone.
    let (frac_nanos, tz_offset_secs) = parse_frac_and_tz(rest)?;

    let epoch_days = ymd_to_epoch_days(y, mo, d)?;
    let day_secs = i64::from(h * 3600 + mi * 60 + sec);
    let total_secs = i64::from(epoch_days) * 86400 + day_secs - tz_offset_secs;
    Some(total_secs * 1_000_000_000 + frac_nanos as i64)
}

/// Parse fractional seconds and timezone from the remainder of an ISO 8601 string.
/// Returns (fractional_nanos, tz_offset_seconds).
fn parse_frac_and_tz(rest: &str) -> Option<(u64, i64)> {
    if rest.is_empty() {
        return Some((0, 0));
    }

    let (frac_nanos, tz_part) = if let Some(after_dot) = rest.strip_prefix('.') {
        // Find where the fractional part ends.
        let frac_end = after_dot
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after_dot.len());
        let frac_str = &after_dot[..frac_end];
        // Pad or truncate to 9 digits for nanosecond precision.
        let padded = format!("{frac_str:0<9}");
        let nanos: u64 = padded[..9].parse().ok()?;
        (nanos, &after_dot[frac_end..])
    } else {
        (0, rest)
    };

    let tz_offset = parse_tz_offset(tz_part)?;
    Some((frac_nanos, tz_offset))
}

/// Parse a timezone suffix: `Z`, `+HH:MM`, `-HH:MM`, or empty (= UTC).
fn parse_tz_offset(s: &str) -> Option<i64> {
    if s.is_empty() || s == "Z" {
        return Some(0);
    }

    let sign: i64 = match s.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };

    let body = &s[1..];
    if body.len() < 5 || body.as_bytes()[2] != b':' {
        return None;
    }
    let h: i64 = body[0..2].parse().ok()?;
    let m: i64 = body[3..5].parse().ok()?;
    Some(sign * (h * 3600 + m * 60))
}

/// Convert (year, month, day) to epoch days since 1970-01-01.
/// Inverse of `epoch_days_to_ymd`. Howard Hinnant's algorithm.
fn ymd_to_epoch_days(y: i32, m: u32, d: u32) -> Option<i32> {
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = i64::from(y);
    let m = u64::from(m);
    let d = u64::from(d);

    let (y, m) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe as i64 - 719_468;
    Some(days as i32)
}

/// Parse an XSD date (YYYY-MM-DD) to epoch days.
fn parse_xsd_date_to_days(s: &str) -> Option<i32> {
    if s.len() < 10 {
        return None;
    }
    let y: i32 = s.get(0..4)?.parse().ok()?;
    let mo: u32 = s.get(5..7)?.parse().ok()?;
    let d: u32 = s.get(8..10)?.parse().ok()?;
    ymd_to_epoch_days(y, mo, d)
}

/// Parse an XSD duration to nanoseconds.
///
/// Handles time-only (`PT1H30M`), date-only (`P30D`), and full ISO 8601
/// (`P1Y2M3DT4H5M6S`). Date components use fixed integer approximations:
/// 1Y = 365 days + 6 hours (365.25 days), 1M = 30 days + 10h30m (30.4375 days).
/// Days are exact. The time portion preserves the existing integer-precision
/// approach for H/M/S to avoid f64 rounding.
///
/// Returns `None` if the string cannot be parsed.
fn parse_xsd_duration_to_nanos(s: &str) -> Option<i64> {
    let (negative, rest) = if let Some(stripped) = s.strip_prefix('-') {
        (true, stripped)
    } else {
        (false, s)
    };

    let rest = rest.strip_prefix('P')?;

    let mut total_nanos: i64 = 0;

    // Split into date part (before T) and time part (after T).
    let (date_part, time_part) = if let Some(t_pos) = rest.find('T') {
        (&rest[..t_pos], Some(&rest[t_pos + 1..]))
    } else {
        (rest, None)
    };

    // Parse date components: Y, M, D (integer values only).
    if !date_part.is_empty() {
        let mut dp = date_part;
        if let Some(idx) = dp.find('Y') {
            let years: i64 = dp[..idx].parse().ok()?;
            // 365.25 days = 365 days + 6 hours
            total_nanos += years * (365 * 86_400_000_000_000 + 6 * 3_600_000_000_000);
            dp = &dp[idx + 1..];
        }
        if let Some(idx) = dp.find('M') {
            let months: i64 = dp[..idx].parse().ok()?;
            // 30.4375 days = 30 days + 10h 30m
            total_nanos +=
                months * (30 * 86_400_000_000_000 + 10 * 3_600_000_000_000 + 30 * 60_000_000_000);
            dp = &dp[idx + 1..];
        }
        if let Some(idx) = dp.find('D') {
            let days: i64 = dp[..idx].parse().ok()?;
            total_nanos += days * 86_400_000_000_000;
            dp = &dp[idx + 1..];
        }
        if !dp.is_empty() {
            return None; // Unparsed date content.
        }
    }

    // Parse time components: H, M, S (preserves integer precision for seconds).
    if let Some(tp) = time_part {
        let mut buf = String::new();
        for ch in tp.chars() {
            match ch {
                'H' => {
                    let h: i64 = buf.parse().ok()?;
                    total_nanos += h * 3_600_000_000_000;
                    buf.clear();
                }
                'M' => {
                    let m: i64 = buf.parse().ok()?;
                    total_nanos += m * 60_000_000_000;
                    buf.clear();
                }
                'S' => {
                    // Split on '.' to handle fractional seconds without f64 loss.
                    if let Some((whole, frac)) = buf.split_once('.') {
                        let whole_secs: i64 = if whole.is_empty() {
                            0
                        } else {
                            whole.parse().ok()?
                        };
                        total_nanos += whole_secs * 1_000_000_000;
                        if frac.len() >= 9 {
                            let frac_nanos: i64 = frac[..9].parse().ok()?;
                            total_nanos += frac_nanos;
                        } else {
                            let frac_val: i64 = frac.parse().ok()?;
                            let scale = 10i64.pow(9 - frac.len() as u32);
                            total_nanos += frac_val * scale;
                        }
                    } else {
                        let secs: i64 = buf.parse().ok()?;
                        total_nanos += secs * 1_000_000_000;
                    }
                    buf.clear();
                }
                _ => buf.push(ch),
            }
        }
        if !buf.is_empty() {
            return None; // Unparsed time content.
        }
    }

    Some(if negative { -total_nanos } else { total_nanos })
}

/// Parse hex-encoded bytes.
fn parse_hex_to_bytes(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16).ok()?;
        bytes.push(byte);
    }
    Some(bytes)
}

/// Parse comma-separated floats into a vector.
fn parse_vector(s: &str) -> Option<Arc<[f32]>> {
    if s.is_empty() {
        return Some(Arc::from(Vec::<f32>::new()));
    }
    let floats: Result<Vec<f32>, _> = s.split(',').map(|tok| tok.trim().parse::<f32>()).collect();
    floats.ok().map(Arc::from)
}

/// Format a double value for XSD serialization.
///
/// Ensures that the lexical form always contains a decimal point or
/// uses scientific notation, so it parses unambiguously as xsd:double.
fn format_double(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            "INF".to_string()
        } else {
            "-INF".to_string()
        }
    } else {
        // Use Rust's default float formatting, which produces a decimal point.
        let s = f.to_string();
        // Rust's Display for f64 always includes a decimal point for non-integer
        // values. For integer-valued floats like `3.0`, it still prints `3`.
        // We need to ensure the lexical form always has a decimal point.
        if s.contains('.') || s.contains('e') || s.contains('E') {
            s
        } else {
            format!("{s}.0")
        }
    }
}

// ---------------------------------------------------------------------------
// Value equality and hashing helpers (for SeleneRdfTerm)
// ---------------------------------------------------------------------------

/// Bitwise equality for Value, handling f64 and f32 NaN correctly.
///
/// Uses `to_bits()` for float comparison so NaN == NaN (same bit pattern).
fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float(x), Value::Float(y)) => x.to_bits() == y.to_bits(),
        (Value::Vector(x), Value::Vector(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|(a, b)| a.to_bits() == b.to_bits())
        }
        _ => a == b,
    }
}

/// Hash a Value for use in SeleneRdfTerm, handling f64/f32 via to_bits().
fn value_hash<H: std::hash::Hasher>(v: &Value, state: &mut H) {
    use std::hash::Hash;
    std::mem::discriminant(v).hash(state);
    match v {
        Value::Null => {}
        Value::Bool(b) => b.hash(state),
        Value::Int(i) => i.hash(state),
        Value::UInt(u) => u.hash(state),
        Value::Float(f) => f.to_bits().hash(state),
        Value::String(s) => s.hash(state),
        Value::InternedStr(s) => s.hash(state),
        Value::Timestamp(t) => t.hash(state),
        Value::Date(d) => d.hash(state),
        Value::LocalDateTime(n) => n.hash(state),
        Value::Duration(n) => n.hash(state),
        Value::Bytes(b) => b.hash(state),
        Value::Vector(v) => {
            v.len().hash(state);
            for f in v.iter() {
                f.to_bits().hash(state);
            }
        }
        Value::Geometry(g) => {
            // GeoJSON serialization is stable for equal geometries.
            g.to_geojson().hash(state);
        }
        Value::List(l) => {
            l.len().hash(state);
            for item in l.iter() {
                value_hash(item, state);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn selene_vector_uri() {
        assert_eq!(selene_vector_datatype().as_str(), "urn:selene:vector");
    }

    // --- value_to_literal ---

    #[test]
    fn null_produces_none() {
        assert!(value_to_literal(&Value::Null).is_none());
    }

    #[test]
    fn list_produces_none() {
        let list = Value::List(Arc::from(vec![Value::Int(1)]));
        assert!(value_to_literal(&list).is_none());
    }

    #[test]
    fn bool_to_literal() {
        let lit = value_to_literal(&Value::Bool(true)).unwrap();
        assert_eq!(lit.value(), "true");
        assert_eq!(lit.datatype(), xsd::BOOLEAN);

        let lit = value_to_literal(&Value::Bool(false)).unwrap();
        assert_eq!(lit.value(), "false");
    }

    #[test]
    fn int_to_literal() {
        let lit = value_to_literal(&Value::Int(-42)).unwrap();
        assert_eq!(lit.value(), "-42");
        assert_eq!(lit.datatype(), xsd::LONG);
    }

    #[test]
    fn uint_to_literal() {
        let lit = value_to_literal(&Value::UInt(999)).unwrap();
        assert_eq!(lit.value(), "999");
        assert_eq!(lit.datatype(), xsd::UNSIGNED_LONG);
    }

    #[test]
    fn float_to_literal() {
        let lit = value_to_literal(&Value::Float(3.15)).unwrap();
        assert_eq!(lit.datatype(), xsd::DOUBLE);
        // Should parse back to the same value.
        let parsed: f64 = lit.value().parse().unwrap();
        assert!((parsed - 3.15).abs() < 1e-10);
    }

    #[test]
    fn float_special_values() {
        let nan_lit = value_to_literal(&Value::Float(f64::NAN)).unwrap();
        assert_eq!(nan_lit.value(), "NaN");

        let inf_lit = value_to_literal(&Value::Float(f64::INFINITY)).unwrap();
        assert_eq!(inf_lit.value(), "INF");

        let neg_inf_lit = value_to_literal(&Value::Float(f64::NEG_INFINITY)).unwrap();
        assert_eq!(neg_inf_lit.value(), "-INF");
    }

    #[test]
    fn string_to_literal() {
        let lit = value_to_literal(&Value::str("hello")).unwrap();
        assert_eq!(lit.value(), "hello");
        assert_eq!(lit.datatype(), xsd::STRING);
    }

    #[test]
    fn interned_str_to_literal() {
        let lit = value_to_literal(&Value::InternedStr(selene_core::interner::IStr::new(
            "world",
        )))
        .unwrap();
        assert_eq!(lit.value(), "world");
        assert_eq!(lit.datatype(), xsd::STRING);
    }

    #[test]
    fn timestamp_to_literal() {
        // 2026-03-28T12:00:00Z = 1774699200 seconds since epoch
        let nanos = 1_774_699_200i64 * 1_000_000_000;
        let lit = value_to_literal(&Value::Timestamp(nanos)).unwrap();
        assert_eq!(lit.datatype(), xsd::DATE_TIME);
        assert!(lit.value().ends_with('Z'));
        assert!(lit.value().starts_with("2026-03-28T12:00:00"));
    }

    #[test]
    fn timestamp_with_fractional() {
        // Exact milliseconds.
        let nanos = 1_774_699_200i64 * 1_000_000_000 + 500_000_000;
        let lit = value_to_literal(&Value::Timestamp(nanos)).unwrap();
        assert!(lit.value().contains(".500Z"));
    }

    #[test]
    fn date_to_literal() {
        // 2026-03-28 = days since epoch
        let days = ymd_to_epoch_days(2026, 3, 28).unwrap();
        let lit = value_to_literal(&Value::Date(days)).unwrap();
        assert_eq!(lit.value(), "2026-03-28");
        assert_eq!(lit.datatype(), xsd::DATE);
    }

    #[test]
    fn local_datetime_to_literal() {
        let nanos = 1_774_699_200i64 * 1_000_000_000;
        let lit = value_to_literal(&Value::LocalDateTime(nanos)).unwrap();
        assert_eq!(lit.datatype(), xsd::DATE_TIME);
        // No timezone suffix.
        assert!(!lit.value().ends_with('Z'));
        assert!(!lit.value().contains('+'));
    }

    #[test]
    fn duration_to_literal() {
        // 1 hour, 30 minutes = 5400 seconds
        let nanos = 5400i64 * 1_000_000_000;
        let lit = value_to_literal(&Value::Duration(nanos)).unwrap();
        assert_eq!(lit.value(), "PT5400S");
        assert_eq!(lit.datatype(), xsd::DURATION);
    }

    #[test]
    fn negative_duration_to_literal() {
        let nanos = -3_000_000_000i64;
        let lit = value_to_literal(&Value::Duration(nanos)).unwrap();
        assert!(lit.value().starts_with('-'));
        assert_eq!(lit.value(), "-PT3S");
    }

    #[test]
    fn bytes_to_literal() {
        let bytes = Value::Bytes(Arc::from(vec![0xde, 0xad, 0xbe, 0xef]));
        let lit = value_to_literal(&bytes).unwrap();
        assert_eq!(lit.value(), "DEADBEEF");
        assert_eq!(lit.datatype(), xsd::HEX_BINARY);
    }

    #[test]
    fn vector_to_literal() {
        let v = Value::vector(vec![0.1, 0.2, 0.3]);
        let lit = value_to_literal(&v).unwrap();
        assert_eq!(lit.datatype().as_str(), "urn:selene:vector");
        // Should contain comma-separated floats.
        let parts: Vec<&str> = lit.value().split(',').collect();
        assert_eq!(parts.len(), 3);
    }

    // --- literal_to_value ---

    #[test]
    fn round_trip_bool() {
        let orig = Value::Bool(true);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        assert_eq!(back, orig);
    }

    #[test]
    fn round_trip_int() {
        let orig = Value::Int(-42);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        assert_eq!(back, orig);
    }

    #[test]
    fn round_trip_uint() {
        let orig = Value::UInt(999);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        assert_eq!(back, orig);
    }

    #[test]
    fn round_trip_float() {
        let orig = Value::Float(3.15);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        if let Value::Float(f) = back {
            assert!((f - 3.15).abs() < 1e-10);
        } else {
            panic!("expected Float, got {back:?}");
        }
    }

    #[test]
    fn round_trip_string() {
        let orig = Value::str("hello world");
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        assert_eq!(back.as_str(), Some("hello world"));
    }

    #[test]
    fn round_trip_date() {
        let days = ymd_to_epoch_days(2026, 3, 28).unwrap();
        let orig = Value::Date(days);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        assert_eq!(back, orig);
    }

    #[test]
    fn round_trip_timestamp() {
        let nanos = 1_774_699_200i64 * 1_000_000_000;
        let orig = Value::Timestamp(nanos);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        assert_eq!(back, orig);
    }

    #[test]
    fn round_trip_duration() {
        // Whole seconds round-trip exactly.
        let nanos = 5400i64 * 1_000_000_000;
        let orig = Value::Duration(nanos);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        assert_eq!(back, orig);
    }

    #[test]
    fn round_trip_bytes() {
        let orig = Value::Bytes(Arc::from(vec![0xca, 0xfe]));
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        assert_eq!(back, orig);
    }

    #[test]
    fn round_trip_vector() {
        let orig = Value::vector(vec![1.0, 2.5, -3.0]);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        if let Value::Vector(v) = back {
            assert_eq!(v.len(), 3);
            assert!((v[0] - 1.0).abs() < 1e-6);
            assert!((v[1] - 2.5).abs() < 1e-6);
            assert!((v[2] - (-3.0)).abs() < 1e-6);
        } else {
            panic!("expected Vector, got {back:?}");
        }
    }

    #[test]
    fn unknown_datatype_falls_back_to_string() {
        let lit = Literal::new_typed_literal(
            "some-value",
            NamedNode::new_unchecked("http://example.com/custom"),
        );
        let val = literal_to_value(&lit);
        assert_eq!(val.as_str(), Some("some-value"));
    }

    #[test]
    fn xsd_integer_maps_to_int() {
        let lit = Literal::new_typed_literal("42", xsd::INTEGER);
        let val = literal_to_value(&lit);
        assert_eq!(val, Value::Int(42));
    }

    #[test]
    fn xsd_decimal_maps_to_float() {
        let lit = Literal::new_typed_literal("3.15", xsd::DECIMAL);
        let val = literal_to_value(&lit);
        if let Value::Float(f) = val {
            assert!((f - 3.15).abs() < 1e-10);
        } else {
            panic!("expected Float, got {val:?}");
        }
    }

    // --- SeleneRdfTerm (sparql feature only) ---

    #[test]
    fn selene_rdf_term_eq_node() {
        let a = SeleneRdfTerm::Node(NodeId(1));
        let b = SeleneRdfTerm::Node(NodeId(1));
        let c = SeleneRdfTerm::Node(NodeId(2));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn selene_rdf_term_eq_literal_float() {
        // NaN == NaN should work for SeleneRdfTerm.
        let a = SeleneRdfTerm::Literal(Value::Float(f64::NAN));
        let b = SeleneRdfTerm::Literal(Value::Float(f64::NAN));
        assert_eq!(a, b);
    }

    #[test]
    fn selene_rdf_term_hash_in_set() {
        let mut set = HashSet::new();
        set.insert(SeleneRdfTerm::Node(NodeId(1)));
        set.insert(SeleneRdfTerm::Node(NodeId(1)));
        set.insert(SeleneRdfTerm::Node(NodeId(2)));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn selene_rdf_term_hash_literal_float() {
        let mut set = HashSet::new();
        set.insert(SeleneRdfTerm::Literal(Value::Float(1.0)));
        set.insert(SeleneRdfTerm::Literal(Value::Float(1.0)));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn selene_rdf_term_different_variants_not_equal() {
        let a = SeleneRdfTerm::Node(NodeId(1));
        let b = SeleneRdfTerm::EdgeReifier(EdgeId(1));
        assert_ne!(a, b);
    }

    // --- RdfPredicate (sparql feature only) ---

    #[test]
    fn rdf_predicate_eq() {
        assert_eq!(RdfPredicate::RdfType, RdfPredicate::RdfType);
        assert_ne!(RdfPredicate::RdfType, RdfPredicate::SosaResult);

        let a = RdfPredicate::PropertyKey(IStr::new("temp"));
        let b = RdfPredicate::PropertyKey(IStr::new("temp"));
        assert_eq!(a, b);
    }

    #[test]
    fn rdf_predicate_hash_in_set() {
        let mut set = HashSet::new();
        set.insert(RdfPredicate::RdfType);
        set.insert(RdfPredicate::RdfType);
        set.insert(RdfPredicate::SosaResult);
        assert_eq!(set.len(), 2);
    }

    // --- ymd_to_epoch_days ---

    #[test]
    fn ymd_epoch_round_trip() {
        // 1970-01-01 = day 0
        assert_eq!(ymd_to_epoch_days(1970, 1, 1), Some(0));

        // Round-trip various dates.
        for days in [-365, 0, 1, 365, 10000, 20000] {
            let (y, m, d) = epoch_days_to_ymd(days);
            let back = ymd_to_epoch_days(y, m, d).unwrap();
            assert_eq!(
                back, days,
                "round-trip failed for days={days} ({y}-{m}-{d})"
            );
        }
    }

    #[test]
    fn ymd_invalid_month() {
        assert_eq!(ymd_to_epoch_days(2026, 0, 1), None);
        assert_eq!(ymd_to_epoch_days(2026, 13, 1), None);
    }

    // --- parse_iso8601_to_nanos ---

    #[test]
    fn parse_iso8601_basic() {
        let nanos = parse_iso8601_to_nanos("2026-03-28T12:00:00Z").unwrap();
        assert_eq!(nanos, 1_774_699_200i64 * 1_000_000_000);
    }

    #[test]
    fn parse_iso8601_with_tz_offset() {
        // +01:00 means the local time is 1 hour ahead of UTC,
        // so UTC time is 1 hour earlier.
        let nanos = parse_iso8601_to_nanos("2026-03-28T13:00:00+01:00").unwrap();
        assert_eq!(nanos, 1_774_699_200i64 * 1_000_000_000);
    }

    #[test]
    fn parse_iso8601_no_tz() {
        // No timezone = treated as UTC.
        let nanos = parse_iso8601_to_nanos("2026-03-28T12:00:00").unwrap();
        assert_eq!(nanos, 1_774_699_200i64 * 1_000_000_000);
    }

    #[test]
    fn parse_iso8601_with_millis() {
        let nanos = parse_iso8601_to_nanos("2026-03-28T12:00:00.500Z").unwrap();
        assert_eq!(nanos, 1_774_699_200i64 * 1_000_000_000 + 500_000_000);
    }

    #[test]
    fn empty_vector_round_trip() {
        let orig = Value::vector(vec![]);
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        if let Value::Vector(v) = back {
            assert!(v.is_empty());
        } else {
            panic!("expected empty Vector, got {back:?}");
        }
    }

    #[test]
    fn hex_binary_uppercase() {
        let val = Value::Bytes(Arc::from(vec![0xAB, 0xCD, 0xEF]));
        let lit = value_to_literal(&val).unwrap();
        assert_eq!(lit.value(), "ABCDEF");
    }

    #[test]
    fn base64_binary_decodes_to_bytes() {
        let lit = Literal::new_typed_literal("SGVsbG8=", xsd::BASE_64_BINARY);
        let val = literal_to_value(&lit);
        match val {
            Value::Bytes(b) => assert_eq!(&*b, b"Hello"),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    #[test]
    fn base64_binary_invalid_falls_back_to_string() {
        let lit = Literal::new_typed_literal("!!!invalid!!!", xsd::BASE_64_BINARY);
        let val = literal_to_value(&lit);
        assert!(matches!(val, Value::String(_)));
    }

    // --- GeoSPARQL geometry literals ---

    #[test]
    fn geometry_wgs84_point_exports_as_wkt_with_crs84() {
        let g = selene_core::geometry::GeometryValue::point_wgs84(-74.006, 40.7128);
        let lit = value_to_literal(&Value::geometry(g)).unwrap();
        assert_eq!(lit.datatype().as_str(), WKT_LITERAL_DATATYPE);
        // CRS84 (lng, lat) rather than EPSG:4326 (lat, lon) so axis-order-aware
        // engines read our coordinates correctly.
        assert_eq!(
            lit.value(),
            "<http://www.opengis.net/def/crs/OGC/1.3/CRS84> POINT (-74.006 40.7128)"
        );
    }

    #[test]
    fn non_wgs84_epsg_still_emits_epsg_uri() {
        let mut g = selene_core::geometry::GeometryValue::point_planar(100.0, 200.0);
        g.crs = Some(selene_core::interner::IStr::new("EPSG:3857"));
        let lit = value_to_literal(&Value::geometry(g)).unwrap();
        assert_eq!(
            lit.value(),
            "<http://www.opengis.net/def/crs/EPSG/0/3857> POINT (100 200)"
        );
    }

    #[test]
    fn epsg_4326_uri_still_accepted_on_import() {
        // Producers that emit the EPSG:4326 IRI should still round-trip,
        // even though our exporter prefers CRS84 now.
        let lit = Literal::new_typed_literal(
            "<http://www.opengis.net/def/crs/EPSG/0/4326> POINT (1 2)",
            NamedNode::new_unchecked(WKT_LITERAL_DATATYPE),
        );
        match literal_to_value(&lit) {
            Value::Geometry(g) => {
                assert_eq!(g.crs.as_ref().map(|c| c.as_str()), Some("EPSG:4326"));
            }
            other => panic!("expected Geometry, got {other:?}"),
        }
    }

    #[test]
    fn geometry_planar_point_exports_without_crs_prefix() {
        let g = selene_core::geometry::GeometryValue::point_planar(3.0, 4.0);
        let lit = value_to_literal(&Value::geometry(g)).unwrap();
        assert_eq!(lit.datatype().as_str(), WKT_LITERAL_DATATYPE);
        assert_eq!(lit.value(), "POINT (3 4)");
    }

    #[test]
    fn wkt_point_round_trips_through_rdf() {
        let orig = Value::geometry(selene_core::geometry::GeometryValue::point_wgs84(
            -74.006, 40.7128,
        ));
        let lit = value_to_literal(&orig).unwrap();
        let back = literal_to_value(&lit);
        match back {
            Value::Geometry(g) => {
                assert_eq!(g.geometry_type(), "Point");
                // Export is CRS84, import normalizes both CRS84 and EPSG:4326
                // back to Selene's `EPSG:4326` short tag.
                assert_eq!(g.crs.as_ref().map(|c| c.as_str()), Some("EPSG:4326"));
            }
            other => panic!("expected Geometry, got {other:?}"),
        }
    }

    #[test]
    fn geojson_literal_still_accepted_on_import() {
        // Producers that only speak geoJSONLiteral should round-trip.
        let lit = Literal::new_typed_literal(
            r#"{"type":"Point","coordinates":[1.0,2.0]}"#,
            NamedNode::new_unchecked(GEOJSON_LITERAL_DATATYPE),
        );
        let val = literal_to_value(&lit);
        assert!(matches!(val, Value::Geometry(_)));
    }

    #[test]
    fn polygon_exports_as_geojson_literal_for_lossless_round_trip() {
        // Until the WKT import parser covers the full 2D set, non-Point
        // geometries export as geoJSONLiteral so round-trips don't lose
        // shape. Points use wktLiteral; this test pins the split.
        let g = selene_core::geometry::GeometryValue::from_geojson(
            r#"{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}"#,
        )
        .unwrap();
        let lit = value_to_literal(&Value::geometry(g.clone())).unwrap();
        assert_eq!(lit.datatype().as_str(), GEOJSON_LITERAL_DATATYPE);

        let back = literal_to_value(&lit);
        match back {
            Value::Geometry(rt) => {
                assert_eq!(rt.geometry_type(), "Polygon");
                assert_eq!(rt.coord_count(), g.coord_count());
            }
            other => panic!("expected Geometry, got {other:?}"),
        }
    }

    #[test]
    fn malformed_wkt_literal_falls_back_to_string() {
        let lit = Literal::new_typed_literal(
            "POINT (not a number)",
            NamedNode::new_unchecked(WKT_LITERAL_DATATYPE),
        );
        let val = literal_to_value(&lit);
        assert!(matches!(val, Value::String(_)));
    }

    #[test]
    fn wkt_literal_without_crs_parses_as_planar() {
        let lit = Literal::new_typed_literal(
            "POINT (10 20)",
            NamedNode::new_unchecked(WKT_LITERAL_DATATYPE),
        );
        match literal_to_value(&lit) {
            Value::Geometry(g) => assert!(g.crs.is_none()),
            other => panic!("expected Geometry, got {other:?}"),
        }
    }

    // --- parse_xsd_duration_to_nanos (full ISO 8601) ---

    #[test]
    fn duration_full_iso8601() {
        // P1Y2M3DT4H5M6S
        let nanos = parse_xsd_duration_to_nanos("P1Y2M3DT4H5M6S").unwrap();
        // 1Y = 365.25 * 86400 * 1e9
        // 2M = 2 * 30.4375 * 86400 * 1e9
        // 3D = 3 * 86400 * 1e9
        // 4H = 4 * 3600 * 1e9
        // 5M = 5 * 60 * 1e9
        // 6S = 6 * 1e9
        assert!(nanos > 0);
        // Rough sanity: ~1 year + 2 months + 3 days should be > 365 days in nanos.
        let min_expected = 365 * 86_400_000_000_000_i64;
        assert!(nanos > min_expected, "got {nanos}");
    }

    #[test]
    fn duration_date_only() {
        let nanos = parse_xsd_duration_to_nanos("P30D").unwrap();
        let expected = 30 * 86_400_000_000_000_i64;
        assert_eq!(nanos, expected);
    }

    #[test]
    fn duration_time_only_still_works() {
        let nanos = parse_xsd_duration_to_nanos("PT1H30M").unwrap();
        let expected = (90 * 60) * 1_000_000_000_i64;
        assert_eq!(nanos, expected);
    }

    #[test]
    fn local_datetime_round_trip() {
        let nanos: i64 = 1_700_000_000_000_000_000; // ~2023-11-14
        let val = Value::LocalDateTime(nanos);
        let lit = value_to_literal(&val).unwrap();
        let round_tripped = literal_to_value(&lit);
        // LocalDateTime exports as xsd:dateTime without timezone, which imports
        // as Timestamp. This is a known asymmetry (dateTime import always
        // produces Timestamp).
        assert!(matches!(round_tripped, Value::Timestamp(_)));
    }
}
