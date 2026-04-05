//! URI minting and resolution.

use std::sync::OnceLock;

use oxrdf::NamedNode;
use selene_core::interner::IStr;
use selene_core::{EdgeId, NodeId};

// URI path segment constants.
pub const SEG_NODE: &str = "node/";
pub const SEG_TYPE: &str = "type/";
pub const SEG_PROP: &str = "prop/";
pub const SEG_REL: &str = "rel/";
pub const SEG_EDGE: &str = "edge/";
pub const SEG_OBS: &str = "obs/";

// Well-known external namespace constants.
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub const SOSA_NS: &str = "http://www.w3.org/ns/sosa/";
pub const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema#";

// Edge reification predicate URIs used by export and import layers.
pub(crate) const SELENE_SOURCE: &str = "urn:selene:source";
pub(crate) const SELENE_TARGET: &str = "urn:selene:target";
pub(crate) const SELENE_LABEL: &str = "urn:selene:label";

// Cached well-known NamedNodes (initialized on first use).
static RDF_TYPE_NODE: OnceLock<NamedNode> = OnceLock::new();
static SOSA_HAS_SIMPLE_RESULT: OnceLock<NamedNode> = OnceLock::new();
static SOSA_RESULT_TIME: OnceLock<NamedNode> = OnceLock::new();
static SOSA_MADE_BY_SENSOR: OnceLock<NamedNode> = OnceLock::new();
static SOSA_OBSERVED_PROPERTY: OnceLock<NamedNode> = OnceLock::new();
static SOSA_OBSERVATION: OnceLock<NamedNode> = OnceLock::new();

/// Parsed form of a Selene namespace URI.
///
/// String-valued variants carry `IStr` directly so that downstream consumers
/// (the SPARQL adapter, the import layer) can use the interned handle without
/// an extra allocation from `IStr::new(&s)`.
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedUri {
    Node(NodeId),
    Type(IStr),
    Property(IStr),
    Relationship(IStr),
    Edge(EdgeId),
    Observation(NodeId, IStr),
}

/// Bidirectional conversion between Selene entity IDs and RDF URIs.
///
/// All URIs share a common namespace prefix, e.g. `https://example.com/building/`.
/// The prefix always ends with `/` or `#` after normalization.
#[derive(Debug, Clone)]
pub struct RdfNamespace {
    prefix: String,
}

impl RdfNamespace {
    /// Create a new namespace with the given prefix. The prefix is normalized
    /// to end with `/` if it does not already end with `/` or `#`.
    pub fn new(prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.ends_with('/') && !prefix.ends_with('#') {
            prefix.push('/');
        }
        Self { prefix }
    }

    /// Return the namespace prefix (always ends with `/` or `#`).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    // -------------------------------------------------------------------------
    // URI minting (Selene -> RDF)
    // -------------------------------------------------------------------------

    /// Mint a URI for a node: `<prefix>node/<id>`.
    pub fn node_uri(&self, id: NodeId) -> NamedNode {
        NamedNode::new_unchecked(format!("{}{}{}", self.prefix, SEG_NODE, id.0))
    }

    /// Mint a URI for a node label used as an RDF class: `<prefix>type/<label>`.
    pub fn type_uri(&self, label: &str) -> NamedNode {
        NamedNode::new_unchecked(format!("{}{}{}", self.prefix, SEG_TYPE, label))
    }

    /// Mint a URI for a property key used as an RDF predicate: `<prefix>prop/<key>`.
    pub fn prop_uri(&self, key: &str) -> NamedNode {
        NamedNode::new_unchecked(format!("{}{}{}", self.prefix, SEG_PROP, key))
    }

    /// Mint a URI for an edge label used as an RDF predicate: `<prefix>rel/<label>`.
    pub fn rel_uri(&self, label: &str) -> NamedNode {
        NamedNode::new_unchecked(format!("{}{}{}", self.prefix, SEG_REL, label))
    }

    /// Mint a URI for an edge reifier: `<prefix>edge/<id>`.
    pub fn edge_uri(&self, id: EdgeId) -> NamedNode {
        NamedNode::new_unchecked(format!("{}{}{}", self.prefix, SEG_EDGE, id.0))
    }

    /// Mint a URI for an observation (node + property): `<prefix>obs/<node_id>/<prop>`.
    pub fn obs_uri(&self, node_id: NodeId, prop: &str) -> NamedNode {
        NamedNode::new_unchecked(format!("{}{}{}/{}", self.prefix, SEG_OBS, node_id.0, prop))
    }

    /// Return the well-known `rdf:type` predicate (cached after first call).
    pub fn rdf_type() -> &'static NamedNode {
        RDF_TYPE_NODE.get_or_init(|| NamedNode::new_unchecked(RDF_TYPE))
    }

    /// Return a SOSA `NamedNode`. Common SOSA terms are cached; others allocate.
    pub fn sosa_uri(local: &str) -> NamedNode {
        match local {
            "hasSimpleResult" => SOSA_HAS_SIMPLE_RESULT
                .get_or_init(|| NamedNode::new_unchecked(format!("{SOSA_NS}hasSimpleResult")))
                .clone(),
            "resultTime" => SOSA_RESULT_TIME
                .get_or_init(|| NamedNode::new_unchecked(format!("{SOSA_NS}resultTime")))
                .clone(),
            "madeBySensor" => SOSA_MADE_BY_SENSOR
                .get_or_init(|| NamedNode::new_unchecked(format!("{SOSA_NS}madeBySensor")))
                .clone(),
            "observedProperty" => SOSA_OBSERVED_PROPERTY
                .get_or_init(|| NamedNode::new_unchecked(format!("{SOSA_NS}observedProperty")))
                .clone(),
            "Observation" => SOSA_OBSERVATION
                .get_or_init(|| NamedNode::new_unchecked(format!("{SOSA_NS}Observation")))
                .clone(),
            _ => NamedNode::new_unchecked(format!("{SOSA_NS}{local}")),
        }
    }

    // -------------------------------------------------------------------------
    // URI parsing (RDF -> Selene)
    // -------------------------------------------------------------------------

    /// Parse a URI against this namespace prefix. Returns `None` if the URI
    /// does not start with the prefix or does not match a known segment pattern.
    pub fn parse(&self, uri: &str) -> Option<ParsedUri> {
        let rest = uri.strip_prefix(self.prefix.as_str())?;

        if let Some(id_str) = rest.strip_prefix(SEG_NODE) {
            let id: u64 = id_str.parse().ok()?;
            return Some(ParsedUri::Node(NodeId(id)));
        }

        if let Some(label) = rest.strip_prefix(SEG_TYPE) {
            return Some(ParsedUri::Type(IStr::new(label)));
        }

        if let Some(key) = rest.strip_prefix(SEG_PROP) {
            return Some(ParsedUri::Property(IStr::new(key)));
        }

        if let Some(label) = rest.strip_prefix(SEG_REL) {
            return Some(ParsedUri::Relationship(IStr::new(label)));
        }

        if let Some(id_str) = rest.strip_prefix(SEG_EDGE) {
            let id: u64 = id_str.parse().ok()?;
            return Some(ParsedUri::Edge(EdgeId(id)));
        }

        if let Some(obs_rest) = rest.strip_prefix(SEG_OBS) {
            // Format: <node_id>/<prop>
            let slash = obs_rest.find('/')?;
            let id: u64 = obs_rest[..slash].parse().ok()?;
            let prop = IStr::new(&obs_rest[slash + 1..]);
            return Some(ParsedUri::Observation(NodeId(id), prop));
        }

        None
    }

    /// Extract the local name from an external URI by splitting on `#` or `/`.
    ///
    /// For example:
    /// - `"https://brickschema.org/schema/Brick#Temperature_Sensor"` -> `"Temperature_Sensor"`
    /// - `"https://example.com/ns/feeds"` -> `"feeds"`
    pub fn extract_local_name(uri: &str) -> &str {
        // Try splitting on '#' first (common in OWL/RDFS ontologies).
        if let Some(pos) = uri.rfind('#') {
            return &uri[pos + 1..];
        }
        // Fall back to splitting on the last '/'.
        if let Some(pos) = uri.rfind('/') {
            return &uri[pos + 1..];
        }
        uri
    }
}

#[cfg(test)]
mod tests {
    use selene_core::interner::IStr;

    use super::*;

    fn ns() -> RdfNamespace {
        RdfNamespace::new("https://example.com/building/")
    }

    // --- normalization ---

    #[test]
    fn prefix_already_slash() {
        let ns = RdfNamespace::new("https://example.com/building/");
        assert_eq!(ns.prefix(), "https://example.com/building/");
    }

    #[test]
    fn prefix_no_trailing_slash_is_normalized() {
        let ns = RdfNamespace::new("https://example.com/building");
        assert_eq!(ns.prefix(), "https://example.com/building/");
    }

    #[test]
    fn prefix_hash_is_preserved() {
        let ns = RdfNamespace::new("https://example.com/building#");
        assert_eq!(ns.prefix(), "https://example.com/building#");
    }

    // --- URI minting ---

    #[test]
    fn node_uri() {
        let uri = ns().node_uri(NodeId(42));
        assert_eq!(uri.as_str(), "https://example.com/building/node/42");
    }

    #[test]
    fn type_uri() {
        let uri = ns().type_uri("Sensor");
        assert_eq!(uri.as_str(), "https://example.com/building/type/Sensor");
    }

    #[test]
    fn prop_uri() {
        let uri = ns().prop_uri("unit");
        assert_eq!(uri.as_str(), "https://example.com/building/prop/unit");
    }

    #[test]
    fn rel_uri() {
        let uri = ns().rel_uri("feeds");
        assert_eq!(uri.as_str(), "https://example.com/building/rel/feeds");
    }

    #[test]
    fn edge_uri() {
        let uri = ns().edge_uri(EdgeId(99));
        assert_eq!(uri.as_str(), "https://example.com/building/edge/99");
    }

    #[test]
    fn obs_uri() {
        let uri = ns().obs_uri(NodeId(42), "temperature");
        assert_eq!(
            uri.as_str(),
            "https://example.com/building/obs/42/temperature"
        );
    }

    #[test]
    fn rdf_type_uri() {
        let uri = RdfNamespace::rdf_type();
        assert_eq!(
            uri.as_str(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        );
    }

    #[test]
    fn sosa_uri() {
        let uri = RdfNamespace::sosa_uri("Observation");
        assert_eq!(uri.as_str(), "http://www.w3.org/ns/sosa/Observation");
    }

    // --- URI parsing ---

    #[test]
    fn parse_node() {
        assert_eq!(
            ns().parse("https://example.com/building/node/42"),
            Some(ParsedUri::Node(NodeId(42)))
        );
    }

    #[test]
    fn parse_type() {
        assert_eq!(
            ns().parse("https://example.com/building/type/Sensor"),
            Some(ParsedUri::Type(IStr::new("Sensor")))
        );
    }

    #[test]
    fn parse_property() {
        assert_eq!(
            ns().parse("https://example.com/building/prop/unit"),
            Some(ParsedUri::Property(IStr::new("unit")))
        );
    }

    #[test]
    fn parse_relationship() {
        assert_eq!(
            ns().parse("https://example.com/building/rel/feeds"),
            Some(ParsedUri::Relationship(IStr::new("feeds")))
        );
    }

    #[test]
    fn parse_edge() {
        assert_eq!(
            ns().parse("https://example.com/building/edge/99"),
            Some(ParsedUri::Edge(EdgeId(99)))
        );
    }

    #[test]
    fn parse_observation() {
        assert_eq!(
            ns().parse("https://example.com/building/obs/42/temperature"),
            Some(ParsedUri::Observation(NodeId(42), IStr::new("temperature")))
        );
    }

    #[test]
    fn parse_foreign_prefix_returns_none() {
        assert_eq!(ns().parse("https://other.com/foo"), None);
    }

    #[test]
    fn parse_unknown_segment_returns_none() {
        assert_eq!(ns().parse("https://example.com/building/unknown/42"), None);
    }

    // --- extract_local_name ---

    #[test]
    fn extract_local_name_hash() {
        assert_eq!(
            RdfNamespace::extract_local_name(
                "https://brickschema.org/schema/Brick#Temperature_Sensor"
            ),
            "Temperature_Sensor"
        );
    }

    #[test]
    fn extract_local_name_slash() {
        assert_eq!(
            RdfNamespace::extract_local_name("https://example.com/ns/feeds"),
            "feeds"
        );
    }

    #[test]
    fn extract_local_name_no_separator_returns_whole() {
        assert_eq!(RdfNamespace::extract_local_name("opaque"), "opaque");
    }

    #[test]
    fn rdf_type_is_cached() {
        let a = RdfNamespace::rdf_type();
        let b = RdfNamespace::rdf_type();
        // Same static reference.
        assert!(std::ptr::eq(a, b));
    }
}
