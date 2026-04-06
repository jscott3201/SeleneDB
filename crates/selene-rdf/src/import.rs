//! RDF import -- parse RDF data and route TBox vs ABox triples.
//!
//! The entry point is [`import_rdf`], which parses incoming RDF bytes in
//! Turtle, N-Triples, or N-Quads format, classifies each triple as TBox
//! (ontology definition) or ABox (instance data), and routes accordingly:
//!
//! - **TBox triples** go into the [`OntologyStore`] for class hierarchy and
//!   property definitions (e.g., `rdfs:subClassOf`, `owl:Class`).
//! - **ABox triples** are translated into Selene property graph mutations:
//!   `rdf:type` -> node labels, `ns/prop/key` -> properties, `ns/rel/label`
//!   -> edges.
//!
//! This is the reverse of [`crate::export`]. A typical workflow:
//! 1. Export a Brick model as Turtle from an external tool
//! 2. POST it to Selene's `/graph/rdf?format=turtle` endpoint
//! 3. The importer classifies class definitions as TBox and instance data
//!    as ABox, creating property graph entities for the latter.

use std::collections::HashMap;

use oxrdf::{GraphName, NamedOrBlankNode, Quad, Term};
use oxttl::{NQuadsParser, NTriplesParser, TurtleParser};
use selene_core::NodeId;
use selene_core::interner::IStr;
use selene_core::label_set::LabelSet;
use selene_core::property_map::PropertyMap;
use selene_core::value::Value;
use selene_graph::SharedGraph;
use tracing::debug;

use crate::namespace::{
    ParsedUri, RDF_TYPE, RdfNamespace, SEG_PROP, SELENE_LABEL, SELENE_SOURCE, SELENE_TARGET,
};
use crate::ontology::{ONTOLOGY_GRAPH_NAME, OntologyStore};
use crate::terms::literal_to_value;
use crate::{DEFAULT_MAX_QUADS, RdfError, RdfFormat, RdfImportResult};

// ---------------------------------------------------------------------------
// Well-known TBox predicate URIs
// ---------------------------------------------------------------------------

const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";

const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
const OWL_DATATYPE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#DatatypeProperty";
const OWL_RESTRICTION: &str = "http://www.w3.org/2002/07/owl#Restriction";
const OWL_EQUIVALENT_CLASS: &str = "http://www.w3.org/2002/07/owl#equivalentClass";
const OWL_UNION_OF: &str = "http://www.w3.org/2002/07/owl#unionOf";
const OWL_INTERSECTION_OF: &str = "http://www.w3.org/2002/07/owl#intersectionOf";

const OWL_NS: &str = "http://www.w3.org/2002/07/owl#";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Import RDF data into the property graph and/or ontology store.
///
/// - `data` -- raw RDF bytes in the format indicated by `format`.
/// - `format` -- Turtle, N-Triples, or N-Quads.
/// - `target_graph` -- if `Some("ontology")`, ALL triples are routed to the
///   ontology store regardless of their content.
/// - `shared` -- the shared property graph for ABox mutations.
/// - `ns` -- namespace for URI minting/parsing.
/// - `ontology` -- the ontology store for TBox triples.
///
/// Returns an [`RdfImportResult`] with counts of created/modified entities.
///
/// The import is bounded to [`DEFAULT_MAX_QUADS`] quads. Inputs exceeding
/// this limit produce [`RdfError::TooManyQuads`].
pub fn import_rdf(
    data: &[u8],
    format: RdfFormat,
    target_graph: Option<&str>,
    shared: &SharedGraph,
    ns: &RdfNamespace,
    ontology: &mut OntologyStore,
) -> Result<RdfImportResult, RdfError> {
    let quads = parse_quads(data, format, DEFAULT_MAX_QUADS)?;

    // If target is explicitly "ontology" or matches the ontology graph URI,
    // store ALL triples in the ontology store and return early.
    let all_to_ontology =
        target_graph.is_some_and(|tg| tg == "ontology" || tg == ONTOLOGY_GRAPH_NAME);

    if all_to_ontology {
        let count = quads.len();
        for quad in &quads {
            ontology.insert_triple(
                quad.subject.clone(),
                quad.predicate.clone(),
                quad.object.clone(),
            );
        }
        debug!(count, "imported all triples to ontology store");
        return Ok(RdfImportResult {
            ontology_triples_loaded: count,
            ..Default::default()
        });
    }

    // Classify each quad as TBox or ABox and collect mutations.
    let mut result = RdfImportResult::default();
    let mut collector = ABoxCollector::new();

    for quad in &quads {
        if is_named_graph(quad) || is_tbox_triple(quad) {
            ontology.insert_triple(
                quad.subject.clone(),
                quad.predicate.clone(),
                quad.object.clone(),
            );
            result.ontology_triples_loaded += 1;
        } else {
            collector.process_abox_quad(quad, ns);
        }
    }

    // Apply collected ABox mutations to the property graph.
    apply_abox_mutations(shared, &collector, &mut result)?;

    debug!(
        nodes_created = result.nodes_created,
        edges_created = result.edges_created,
        labels_added = result.labels_added,
        properties_set = result.properties_set,
        ontology_triples = result.ontology_triples_loaded,
        "RDF import complete"
    );

    Ok(result)
}

// ---------------------------------------------------------------------------
// Quad parsing
// ---------------------------------------------------------------------------

/// Parse raw RDF bytes into a vector of quads. For Turtle and N-Triples
/// (triple-only formats), each triple is wrapped in a Quad with
/// `GraphName::DefaultGraph`.
///
/// `max_quads` bounds the number of quads collected. If the input contains
/// more quads than the limit, parsing stops early and returns
/// [`RdfError::TooManyQuads`].
fn parse_quads(data: &[u8], format: RdfFormat, max_quads: usize) -> Result<Vec<Quad>, RdfError> {
    let mut quads = Vec::new();

    match format {
        RdfFormat::Turtle => {
            for result in TurtleParser::new().for_reader(data) {
                let triple = result.map_err(|e| RdfError::Parse(e.to_string()))?;
                quads.push(Quad::new(
                    triple.subject,
                    triple.predicate,
                    triple.object,
                    GraphName::DefaultGraph,
                ));
                if quads.len() > max_quads {
                    return Err(RdfError::TooManyQuads(max_quads));
                }
            }
        }
        RdfFormat::NTriples => {
            for result in NTriplesParser::new().for_reader(data) {
                let triple = result.map_err(|e| RdfError::Parse(e.to_string()))?;
                quads.push(Quad::new(
                    triple.subject,
                    triple.predicate,
                    triple.object,
                    GraphName::DefaultGraph,
                ));
                if quads.len() > max_quads {
                    return Err(RdfError::TooManyQuads(max_quads));
                }
            }
        }
        RdfFormat::NQuads => {
            for result in NQuadsParser::new().for_reader(data) {
                let quad = result.map_err(|e| RdfError::Parse(e.to_string()))?;
                quads.push(quad);
                if quads.len() > max_quads {
                    return Err(RdfError::TooManyQuads(max_quads));
                }
            }
        }
    }

    Ok(quads)
}

// ---------------------------------------------------------------------------
// TBox classification
// ---------------------------------------------------------------------------

/// Returns true if the quad has a non-default named graph, indicating it
/// belongs to an ontology or external dataset.
fn is_named_graph(quad: &Quad) -> bool {
    !matches!(quad.graph_name, GraphName::DefaultGraph)
}

/// Returns true if the triple is a TBox (schema/ontology) definition.
///
/// A triple is TBox if:
/// - Its predicate is a well-known ontology predicate (rdfs:subClassOf,
///   owl:equivalentClass, etc.)
/// - Its predicate is `rdf:type` and the object is `rdfs:Class` or `owl:Class`
///   (class declaration)
/// - Its predicate is `rdf:type` and the object is `owl:ObjectProperty`,
///   `owl:DatatypeProperty`, or `owl:Restriction` (property/restriction
///   declaration)
/// - Its subject URI is in the `owl:` namespace (owl ontology elements)
fn is_tbox_triple(quad: &Quad) -> bool {
    let pred = quad.predicate.as_str();

    // Direct TBox predicates.
    if matches!(
        pred,
        RDFS_SUBCLASS_OF
            | RDFS_DOMAIN
            | RDFS_RANGE
            | OWL_EQUIVALENT_CLASS
            | OWL_UNION_OF
            | OWL_INTERSECTION_OF
    ) {
        return true;
    }

    // rdf:type declarations for classes, properties, and restrictions.
    if pred == RDF_TYPE
        && let Term::NamedNode(obj) = &quad.object
    {
        let obj_str = obj.as_str();
        if matches!(
            obj_str,
            RDFS_CLASS | OWL_CLASS | OWL_OBJECT_PROPERTY | OWL_DATATYPE_PROPERTY | OWL_RESTRICTION
        ) {
            return true;
        }
    }

    // Subjects in the owl: namespace are ontology-level definitions.
    if let NamedOrBlankNode::NamedNode(subj) = &quad.subject
        && subj.as_str().starts_with(OWL_NS)
    {
        return true;
    }

    false
}

// ---------------------------------------------------------------------------
// ABox collection
// ---------------------------------------------------------------------------

/// Accumulated data for a single node being imported.
#[derive(Debug, Default)]
struct NodeData {
    labels: Vec<IStr>,
    properties: Vec<(IStr, Value)>,
}

/// Accumulated data for a single edge being imported.
#[derive(Debug)]
struct EdgeData {
    /// Subject URI string of the source node.
    source_uri: String,
    label: IStr,
    /// Object URI string of the target node.
    target_uri: String,
}

/// Accumulated edge reification data (from the selene:source/target/label
/// pattern used by the export layer). Keyed by the edge reifier URI.
#[derive(Debug, Default)]
struct EdgeReifierData {
    source_uri: Option<String>,
    target_uri: Option<String>,
    label: Option<IStr>,
    properties: Vec<(IStr, Value)>,
}

/// Collects ABox triples into node/edge/property data structures before
/// applying them as property graph mutations.
#[derive(Debug, Default)]
struct ABoxCollector {
    /// Nodes keyed by their subject URI string. Each entry collects labels
    /// and properties for that node.
    nodes: HashMap<String, NodeData>,

    /// Edges collected from `ns/rel/<label>` predicates. Each entry is a
    /// (source_uri, label, target_uri) tuple.
    edges: Vec<EdgeData>,

    /// Edge reification data keyed by the edge URI (e.g., `ns/edge/42`).
    edge_reifiers: HashMap<String, EdgeReifierData>,

    /// External URI subjects that could not be parsed as Selene entities.
    /// These are stored as nodes with a label derived from the local name
    /// of the type, and properties derived from any literal-object triples.
    external_nodes: HashMap<String, NodeData>,
}

impl ABoxCollector {
    fn new() -> Self {
        Self::default()
    }

    /// Process a single ABox quad. The quad has already been classified as
    /// not-TBox and not-named-graph.
    fn process_abox_quad(&mut self, quad: &Quad, ns: &RdfNamespace) {
        let pred_str = quad.predicate.as_str();

        // Handle edge reification predicates (urn:selene:source/target/label).
        if let NamedOrBlankNode::NamedNode(subj) = &quad.subject {
            let subj_str = subj.as_str();

            if pred_str == SELENE_SOURCE || pred_str == SELENE_TARGET || pred_str == SELENE_LABEL {
                self.process_reifier_quad(subj_str, pred_str, &quad.object);
                return;
            }

            // Check if the subject is a known edge reifier (ns/edge/<id>).
            if let Some(ParsedUri::Edge(_)) = ns.parse(subj_str) {
                // Property on an edge reifier.
                if let Some(key) = extract_prop_key(pred_str, ns)
                    && let Some(value) = term_to_value(&quad.object)
                {
                    let reifier = self.edge_reifiers.entry(subj_str.to_owned()).or_default();
                    reifier.properties.push((key, value));
                }
                return;
            }
        }

        // Try to parse the subject URI against the Selene namespace.
        let subj_str = match &quad.subject {
            NamedOrBlankNode::NamedNode(nn) => nn.as_str(),
            NamedOrBlankNode::BlankNode(_) => {
                // Blank node subjects cannot be mapped to Selene entities.
                return;
            }
        };

        match ns.parse(subj_str) {
            Some(ParsedUri::Node(_)) => {
                self.process_node_quad(subj_str, quad, ns);
            }
            Some(ParsedUri::Type(_) | ParsedUri::Property(_) | ParsedUri::Relationship(_)) => {
                // These are schema-level URIs used as subjects; skip in ABox.
            }
            Some(ParsedUri::Observation(_, _)) => {
                // Observation triples are handled by the observation module.
            }
            Some(ParsedUri::Edge(_)) => {
                // Already handled above (edge reifier properties).
            }
            None => {
                // External URI: collect as an external node.
                self.process_external_quad(subj_str, quad, ns);
            }
        }
    }

    /// Process a quad whose subject is a Selene node URI.
    fn process_node_quad(&mut self, subj_str: &str, quad: &Quad, ns: &RdfNamespace) {
        let pred_str = quad.predicate.as_str();
        let node = self.nodes.entry(subj_str.to_owned()).or_default();

        if pred_str == RDF_TYPE {
            // rdf:type -> collect as a label.
            if let Some(label) = type_object_to_label(&quad.object, ns) {
                node.labels.push(label);
            }
        } else if let Some(key) = extract_prop_key(pred_str, ns) {
            // ns/prop/<key> -> collect as a property.
            if let Some(value) = term_to_value(&quad.object) {
                node.properties.push((key, value));
            }
        } else if let Some(rel_label) = extract_rel_label(pred_str, ns) {
            // ns/rel/<label> -> collect as an edge.
            if let Some(target_uri) = term_to_uri(&quad.object) {
                self.edges.push(EdgeData {
                    source_uri: subj_str.to_owned(),
                    label: rel_label,
                    target_uri,
                });
            }
        }
    }

    /// Process edge reification quads (urn:selene:source/target/label).
    fn process_reifier_quad(&mut self, subj_str: &str, pred_str: &str, object: &Term) {
        let reifier = self.edge_reifiers.entry(subj_str.to_owned()).or_default();

        match pred_str {
            SELENE_SOURCE => {
                if let Some(uri) = term_to_uri(object) {
                    reifier.source_uri = Some(uri);
                }
            }
            SELENE_TARGET => {
                if let Some(uri) = term_to_uri(object) {
                    reifier.target_uri = Some(uri);
                }
            }
            SELENE_LABEL => {
                if let Some(uri) = term_to_uri(object) {
                    // The label URI is ns/rel/<label>; extract the label.
                    let local = RdfNamespace::extract_local_name(&uri);
                    reifier.label = Some(IStr::new(local));
                }
            }
            _ => {}
        }
    }

    /// Process a quad whose subject is an external (non-Selene-namespace) URI.
    fn process_external_quad(&mut self, subj_str: &str, quad: &Quad, ns: &RdfNamespace) {
        let pred_str = quad.predicate.as_str();
        let node = self.external_nodes.entry(subj_str.to_owned()).or_default();

        if pred_str == RDF_TYPE {
            if let Some(label) = type_object_to_label(&quad.object, ns) {
                node.labels.push(label);
            }
        } else if let Some(value) = term_to_value(&quad.object) {
            // Use the predicate local name as the property key.
            let key = IStr::new(RdfNamespace::extract_local_name(pred_str));
            node.properties.push((key, value));
        }
    }
}

// ---------------------------------------------------------------------------
// ABox mutation application
// ---------------------------------------------------------------------------

/// Apply the collected ABox data as property graph mutations.
fn apply_abox_mutations(
    shared: &SharedGraph,
    collector: &ABoxCollector,
    result: &mut RdfImportResult,
) -> Result<(), RdfError> {
    // Skip the write lock entirely if there is nothing to mutate.
    if collector.nodes.is_empty()
        && collector.edges.is_empty()
        && collector.edge_reifiers.is_empty()
        && collector.external_nodes.is_empty()
    {
        return Ok(());
    }

    let ((), _changes) = shared.write(|m| {
        // Maps from subject URI to the NodeId assigned by Selene. We need
        // this to wire up edges after nodes are created.
        let mut uri_to_node: HashMap<String, NodeId> = HashMap::new();

        // --- Create nodes from Selene-namespace subjects ---
        for (uri, data) in &collector.nodes {
            let label_strs: Vec<&str> = data.labels.iter().map(|s| s.as_str()).collect();
            let labels = LabelSet::from_strs(&label_strs);

            let props =
                PropertyMap::from_pairs(data.properties.iter().map(|(k, v)| (*k, v.clone())));

            let node_id = m.create_node(labels, props)?;
            result.nodes_created += 1;
            result.labels_added += data.labels.len();
            result.properties_set += data.properties.len();
            uri_to_node.insert(uri.clone(), node_id);
        }

        // --- Create nodes from external-URI subjects ---
        for (uri, data) in &collector.external_nodes {
            let label_strs: Vec<&str> = data.labels.iter().map(|s| s.as_str()).collect();
            let labels = LabelSet::from_strs(&label_strs);

            let mut pairs: Vec<(IStr, Value)> = data
                .properties
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect();

            // Store the original URI as a property so the external identity
            // is preserved for later reference or re-export.
            pairs.push((IStr::new("_uri"), Value::String(uri.clone().into())));

            let props = PropertyMap::from_pairs(pairs);

            let node_id = m.create_node(labels, props)?;
            result.nodes_created += 1;
            result.labels_added += data.labels.len();
            // +1 for the _uri property.
            result.properties_set += data.properties.len() + 1;
            uri_to_node.insert(uri.clone(), node_id);
        }

        // --- Create edges from rel/ predicates ---
        for edge in &collector.edges {
            let source_id = match uri_to_node.get(&edge.source_uri) {
                Some(id) => *id,
                None => continue, // Source not found; skip edge.
            };
            let target_id = match uri_to_node.get(&edge.target_uri) {
                Some(id) => *id,
                None => continue, // Target not found; skip edge.
            };

            m.create_edge(source_id, edge.label, target_id, PropertyMap::new())?;
            result.edges_created += 1;
        }

        // --- Create edges from edge reifiers ---
        // Edge reifiers carry the source/target/label from the export layer's
        // reification pattern. They may also carry properties.
        for reifier in collector.edge_reifiers.values() {
            let (Some(source_uri), Some(target_uri), Some(label)) =
                (&reifier.source_uri, &reifier.target_uri, &reifier.label)
            else {
                continue; // Incomplete reifier; skip.
            };

            let source_id = match uri_to_node.get(source_uri.as_str()) {
                Some(id) => *id,
                None => continue,
            };
            let target_id = match uri_to_node.get(target_uri.as_str()) {
                Some(id) => *id,
                None => continue,
            };

            let props =
                PropertyMap::from_pairs(reifier.properties.iter().map(|(k, v)| (*k, v.clone())));

            m.create_edge(source_id, *label, target_id, props)?;
            result.edges_created += 1;
            result.properties_set += reifier.properties.len();
        }

        Ok(())
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a property key from a predicate URI. Returns `Some(key)` if the
/// predicate is in the Selene `prop/` segment (e.g., `ns/prop/temperature`
/// -> `IStr("temperature")`).
fn extract_prop_key(pred_uri: &str, ns: &RdfNamespace) -> Option<IStr> {
    // Check if the predicate is in the Selene namespace prop/ segment.
    let rest = pred_uri.strip_prefix(ns.prefix())?;
    let key = rest.strip_prefix(SEG_PROP)?;
    Some(IStr::new(key))
}

/// Extract a relationship label from a predicate URI. Returns `Some(label)`
/// if the predicate is in the Selene `rel/` segment (e.g., `ns/rel/feeds`
/// -> `IStr("feeds")`).
fn extract_rel_label(pred_uri: &str, ns: &RdfNamespace) -> Option<IStr> {
    let rest = pred_uri.strip_prefix(ns.prefix())?;
    let label = rest.strip_prefix(crate::namespace::SEG_REL)?;
    Some(IStr::new(label))
}

/// Convert an `rdf:type` object term to an interned label.
///
/// - If the object is a Selene type URI (`ns/type/<label>`), returns the `IStr`.
/// - If the object is an external named node, returns the local name as `IStr`.
fn type_object_to_label(object: &Term, ns: &RdfNamespace) -> Option<IStr> {
    match object {
        Term::NamedNode(nn) => {
            let uri = nn.as_str();
            // Try to parse as a Selene type URI first. ParsedUri::Type
            // already carries an IStr.
            if let Some(ParsedUri::Type(label)) = ns.parse(uri) {
                return Some(label);
            }
            // Fall back to extracting the local name.
            let local = RdfNamespace::extract_local_name(uri);
            if local.is_empty() {
                return None;
            }
            Some(IStr::new(local))
        }
        _ => None,
    }
}

/// Convert an RDF Term to a Selene Value.
///
/// - `Term::Literal` -> delegates to `literal_to_value`
/// - `Term::NamedNode` -> `Value::String` with the full URI
/// - `Term::BlankNode` -> `None` (cannot represent as a property value)
fn term_to_value(term: &Term) -> Option<Value> {
    match term {
        Term::Literal(lit) => Some(literal_to_value(lit)),
        Term::NamedNode(nn) => Some(Value::String(nn.as_str().to_owned().into())),
        Term::BlankNode(_) => None,
    }
}

/// Extract a URI string from a Term if it is a NamedNode.
fn term_to_uri(term: &Term) -> Option<String> {
    match term {
        Term::NamedNode(nn) => Some(nn.as_str().to_owned()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{Literal, NamedNode};
    use selene_core::interner::IStr;
    use selene_graph::SeleneGraph;

    fn test_ns() -> RdfNamespace {
        RdfNamespace::new("https://example.com/building/")
    }

    fn shared() -> SharedGraph {
        SharedGraph::new(SeleneGraph::new())
    }

    // --- parse_quads ---

    #[test]
    fn parse_turtle_basic() {
        // Turtle does not allow `/` in prefixed local names, so use full URIs.
        let data = br#"
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            <https://example.com/building/node/1> rdf:type <https://example.com/building/type/Sensor> .
            <https://example.com/building/node/1> <https://example.com/building/prop/unit> "degC" .
        "#;

        let quads = parse_quads(data, RdfFormat::Turtle, DEFAULT_MAX_QUADS).unwrap();
        assert_eq!(quads.len(), 2);

        // All should be default graph.
        for q in &quads {
            assert!(matches!(q.graph_name, GraphName::DefaultGraph));
        }
    }

    #[test]
    fn parse_ntriples_basic() {
        let data = b"<https://example.com/building/node/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/building/type/Sensor> .\n";

        let quads = parse_quads(data, RdfFormat::NTriples, DEFAULT_MAX_QUADS).unwrap();
        assert_eq!(quads.len(), 1);
    }

    #[test]
    fn parse_nquads_with_named_graph() {
        let data = b"<https://example.com/s> <https://example.com/p> <https://example.com/o> <urn:selene:ontology> .\n";

        let quads = parse_quads(data, RdfFormat::NQuads, DEFAULT_MAX_QUADS).unwrap();
        assert_eq!(quads.len(), 1);
        assert!(!matches!(quads[0].graph_name, GraphName::DefaultGraph));
    }

    #[test]
    fn parse_invalid_turtle_returns_error() {
        let data = b"this is not valid turtle!!!";
        let result = parse_quads(data, RdfFormat::Turtle, DEFAULT_MAX_QUADS);
        assert!(result.is_err());
    }

    #[test]
    fn parse_quads_rejects_exceeding_limit() {
        // 3 triples, but limit is 2.
        let data = b"\
            <urn:a> <urn:p> <urn:b> .\n\
            <urn:a> <urn:p> <urn:c> .\n\
            <urn:a> <urn:p> <urn:d> .\n";
        let result = parse_quads(data, RdfFormat::NTriples, 2);
        assert!(matches!(result, Err(RdfError::TooManyQuads(2))));
    }

    // --- TBox classification ---

    #[test]
    fn tbox_rdfs_subclass_of() {
        let quad = Quad::new(
            NamedNode::new_unchecked("urn:test:A"),
            NamedNode::new_unchecked(RDFS_SUBCLASS_OF),
            NamedNode::new_unchecked("urn:test:B"),
            GraphName::DefaultGraph,
        );
        assert!(is_tbox_triple(&quad));
    }

    #[test]
    fn tbox_rdf_type_owl_class() {
        let quad = Quad::new(
            NamedNode::new_unchecked("urn:test:Sensor"),
            NamedNode::new_unchecked(RDF_TYPE),
            NamedNode::new_unchecked(OWL_CLASS),
            GraphName::DefaultGraph,
        );
        assert!(is_tbox_triple(&quad));
    }

    #[test]
    fn tbox_rdf_type_rdfs_class() {
        let quad = Quad::new(
            NamedNode::new_unchecked("urn:test:Sensor"),
            NamedNode::new_unchecked(RDF_TYPE),
            NamedNode::new_unchecked(RDFS_CLASS),
            GraphName::DefaultGraph,
        );
        assert!(is_tbox_triple(&quad));
    }

    #[test]
    fn tbox_owl_subject() {
        let quad = Quad::new(
            NamedNode::new_unchecked("http://www.w3.org/2002/07/owl#Thing"),
            NamedNode::new_unchecked(RDF_TYPE),
            NamedNode::new_unchecked("urn:test:Something"),
            GraphName::DefaultGraph,
        );
        assert!(is_tbox_triple(&quad));
    }

    #[test]
    fn abox_instance_triple_is_not_tbox() {
        let quad = Quad::new(
            NamedNode::new_unchecked("https://example.com/building/node/1"),
            NamedNode::new_unchecked(RDF_TYPE),
            NamedNode::new_unchecked("https://example.com/building/type/Sensor"),
            GraphName::DefaultGraph,
        );
        assert!(!is_tbox_triple(&quad));
    }

    #[test]
    fn abox_property_triple_is_not_tbox() {
        let quad = Quad::new(
            NamedNode::new_unchecked("https://example.com/building/node/1"),
            NamedNode::new_unchecked("https://example.com/building/prop/unit"),
            Literal::new_simple_literal("degC"),
            GraphName::DefaultGraph,
        );
        assert!(!is_tbox_triple(&quad));
    }

    #[test]
    fn named_graph_quad_routes_to_ontology() {
        let quad = Quad::new(
            NamedNode::new_unchecked("urn:test:A"),
            NamedNode::new_unchecked("urn:test:p"),
            NamedNode::new_unchecked("urn:test:B"),
            GraphName::NamedNode(NamedNode::new_unchecked(ONTOLOGY_GRAPH_NAME)),
        );
        assert!(is_named_graph(&quad));
    }

    // --- Full import ---

    #[test]
    fn import_all_to_ontology() {
        let data = b"<urn:test:A> <urn:test:p> <urn:test:B> .\n";
        let ns = test_ns();
        let shared = shared();
        let mut ontology = OntologyStore::new();

        let result = import_rdf(
            data,
            RdfFormat::NTriples,
            Some("ontology"),
            &shared,
            &ns,
            &mut ontology,
        )
        .unwrap();

        assert_eq!(result.ontology_triples_loaded, 1);
        assert_eq!(result.nodes_created, 0);
        assert_eq!(ontology.len(), 1);
    }

    #[test]
    fn import_abox_creates_nodes_with_labels_and_props() {
        let data = br#"
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            <https://example.com/building/node/1> rdf:type <https://example.com/building/type/Sensor> .
            <https://example.com/building/node/1> <https://example.com/building/prop/unit> "degC" .
            <https://example.com/building/node/1> <https://example.com/building/prop/floor> "3"^^xsd:long .
        "#;

        let ns = test_ns();
        let sg = shared();
        let mut ontology = OntologyStore::new();

        let result = import_rdf(data, RdfFormat::Turtle, None, &sg, &ns, &mut ontology).unwrap();

        assert_eq!(result.nodes_created, 1);
        assert_eq!(result.labels_added, 1);
        assert_eq!(result.properties_set, 2);
        assert_eq!(result.ontology_triples_loaded, 0);

        // Verify the node was created in the graph.
        sg.read(|g| {
            // The first created node gets NodeId(1).
            let node = g.get_node(NodeId(1)).expect("node should exist");
            assert!(node.labels.contains(IStr::new("Sensor")));
            assert_eq!(
                node.properties.get(IStr::new("unit")),
                Some(&Value::String("degC".into()))
            );
            assert_eq!(
                node.properties.get(IStr::new("floor")),
                Some(&Value::Int(3))
            );
        });
    }

    #[test]
    fn import_abox_creates_edges() {
        let data = br"
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            <https://example.com/building/node/1> rdf:type <https://example.com/building/type/Sensor> .
            <https://example.com/building/node/2> rdf:type <https://example.com/building/type/Room> .
            <https://example.com/building/node/1> <https://example.com/building/rel/locatedIn> <https://example.com/building/node/2> .
        ";

        let ns = test_ns();
        let sg = shared();
        let mut ontology = OntologyStore::new();

        let result = import_rdf(data, RdfFormat::Turtle, None, &sg, &ns, &mut ontology).unwrap();

        assert_eq!(result.nodes_created, 2);
        assert_eq!(result.edges_created, 1);

        sg.read(|g| {
            let edges: Vec<_> = g.all_edge_ids().collect();
            assert_eq!(edges.len(), 1);
            let edge = g.get_edge(edges[0]).unwrap();
            assert_eq!(edge.label.as_str(), "locatedIn");
        });
    }

    #[test]
    fn import_mixed_tbox_and_abox() {
        let data = br#"
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix owl: <http://www.w3.org/2002/07/owl#> .

            <https://example.com/building/type/Sensor> rdf:type owl:Class .
            <https://example.com/building/type/Sensor> rdfs:subClassOf <https://example.com/building/type/Device> .
            <https://example.com/building/node/1> rdf:type <https://example.com/building/type/Sensor> .
            <https://example.com/building/node/1> <https://example.com/building/prop/unit> "degC" .
        "#;

        let ns = test_ns();
        let sg = shared();
        let mut ontology = OntologyStore::new();

        let result = import_rdf(data, RdfFormat::Turtle, None, &sg, &ns, &mut ontology).unwrap();

        // 2 TBox triples (owl:Class, rdfs:subClassOf).
        assert_eq!(result.ontology_triples_loaded, 2);
        // 1 ABox node with 1 label and 1 property.
        assert_eq!(result.nodes_created, 1);
        assert_eq!(result.labels_added, 1);
        assert_eq!(result.properties_set, 1);
    }

    #[test]
    fn import_edge_reifier_with_properties() {
        // Simulate the export pattern: base triple + reifier quads.
        let data = br#"
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

            <https://example.com/building/node/1> rdf:type <https://example.com/building/type/Sensor> .
            <https://example.com/building/node/2> rdf:type <https://example.com/building/type/Room> .
            <https://example.com/building/node/1> <https://example.com/building/rel/locatedIn> <https://example.com/building/node/2> .
            <https://example.com/building/edge/1> <urn:selene:source> <https://example.com/building/node/1> .
            <https://example.com/building/edge/1> <urn:selene:target> <https://example.com/building/node/2> .
            <https://example.com/building/edge/1> <urn:selene:label> <https://example.com/building/rel/locatedIn> .
            <https://example.com/building/edge/1> <https://example.com/building/prop/since> "2024"^^xsd:long .
        "#;

        let ns = test_ns();
        let sg = shared();
        let mut ontology = OntologyStore::new();

        let result = import_rdf(data, RdfFormat::Turtle, None, &sg, &ns, &mut ontology).unwrap();

        assert_eq!(result.nodes_created, 2);
        // 1 edge from rel/ predicate + 1 edge from reifier.
        assert_eq!(result.edges_created, 2);
        // The reifier edge should have the "since" property.
        assert_eq!(result.properties_set, 1);
    }

    #[test]
    fn import_nquads_routes_named_graph_to_ontology() {
        let data = b"<urn:test:A> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <urn:test:B> <urn:selene:ontology> .\n\
                     <https://example.com/building/node/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://example.com/building/type/Sensor> .\n";

        let ns = test_ns();
        let sg = shared();
        let mut ontology = OntologyStore::new();

        let result = import_rdf(data, RdfFormat::NQuads, None, &sg, &ns, &mut ontology).unwrap();

        // The named graph quad + the subClassOf predicate both route to ontology.
        // But the first quad matches both named-graph AND TBox predicate -- counted once.
        assert_eq!(result.ontology_triples_loaded, 1);
        assert_eq!(result.nodes_created, 1);
    }

    #[test]
    fn import_empty_data() {
        let ns = test_ns();
        let sg = shared();
        let mut ontology = OntologyStore::new();

        let result = import_rdf(b"", RdfFormat::NTriples, None, &sg, &ns, &mut ontology).unwrap();

        assert_eq!(result.nodes_created, 0);
        assert_eq!(result.edges_created, 0);
        assert_eq!(result.ontology_triples_loaded, 0);
    }

    #[test]
    fn import_external_uri_creates_node_with_uri_property() {
        let data = br#"
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix brick: <https://brickschema.org/schema/Brick#> .

            <https://external.example.com/sensor/42> rdf:type brick:Temperature_Sensor .
            <https://external.example.com/sensor/42> brick:hasUnit "degC" .
        "#;

        let ns = test_ns();
        let sg = shared();
        let mut ontology = OntologyStore::new();

        let result = import_rdf(data, RdfFormat::Turtle, None, &sg, &ns, &mut ontology).unwrap();

        assert_eq!(result.nodes_created, 1);

        sg.read(|g| {
            let node = g.get_node(NodeId(1)).expect("node should exist");
            assert!(node.labels.contains(IStr::new("Temperature_Sensor")));
            // External URI preserved.
            assert_eq!(
                node.properties.get(IStr::new("_uri")),
                Some(&Value::String(
                    "https://external.example.com/sensor/42".into()
                ))
            );
            // Property from external predicate (local name extraction).
            assert_eq!(
                node.properties.get(IStr::new("hasUnit")),
                Some(&Value::String("degC".into()))
            );
        });
    }

    // --- Helper tests ---

    #[test]
    fn extract_prop_key_works() {
        let ns = test_ns();
        assert_eq!(
            extract_prop_key("https://example.com/building/prop/unit", &ns),
            Some(IStr::new("unit"))
        );
        assert_eq!(extract_prop_key("https://other.com/prop/unit", &ns), None);
    }

    #[test]
    fn extract_rel_label_works() {
        let ns = test_ns();
        assert_eq!(
            extract_rel_label("https://example.com/building/rel/feeds", &ns),
            Some(IStr::new("feeds"))
        );
        assert_eq!(extract_rel_label("https://other.com/rel/feeds", &ns), None);
    }

    #[test]
    fn type_object_to_label_selene_type() {
        let ns = test_ns();
        let obj = Term::NamedNode(NamedNode::new_unchecked(
            "https://example.com/building/type/Sensor",
        ));
        assert_eq!(type_object_to_label(&obj, &ns), Some(IStr::new("Sensor")));
    }

    #[test]
    fn type_object_to_label_external_uri() {
        let ns = test_ns();
        let obj = Term::NamedNode(NamedNode::new_unchecked(
            "https://brickschema.org/schema/Brick#Temperature_Sensor",
        ));
        assert_eq!(
            type_object_to_label(&obj, &ns),
            Some(IStr::new("Temperature_Sensor"))
        );
    }

    #[test]
    fn term_to_value_literal() {
        let term = Term::Literal(Literal::new_simple_literal("hello"));
        assert_eq!(term_to_value(&term), Some(Value::String("hello".into())));
    }

    #[test]
    fn term_to_value_named_node() {
        let term = Term::NamedNode(NamedNode::new_unchecked("urn:test:foo"));
        assert_eq!(
            term_to_value(&term),
            Some(Value::String("urn:test:foo".into()))
        );
    }

    #[test]
    fn term_to_value_blank_node_returns_none() {
        let term = Term::BlankNode(oxrdf::BlankNode::new("b0").unwrap());
        assert_eq!(term_to_value(&term), None);
    }

    // --- Dictionary encoding through import path ---

    #[test]
    fn import_promotes_dictionary_flagged_strings() {
        // TrackedMutation::create_node applies dictionary encoding for
        // properties whose schema has `dictionary: true`. The RDF import
        // path passes through TrackedMutation, so dictionary-flagged
        // properties are promoted to Value::InternedStr automatically.
        use selene_core::{NodeSchema, PropertyDef, ValueType};

        let ns = test_ns();

        // Build a graph with a DICTIONARY schema registered before import.
        let mut graph = SeleneGraph::new();
        let schema = NodeSchema::builder("Sensor")
            .property(
                PropertyDef::builder("unit", ValueType::String)
                    .dictionary()
                    .build(),
            )
            .property(PropertyDef::simple("name", ValueType::String, false))
            .build();
        graph.schema_mut().register_node_schema(schema).unwrap();
        let sg = SharedGraph::new(graph);

        let data = br#"
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            <https://example.com/building/node/1> rdf:type <https://example.com/building/type/Sensor> .
            <https://example.com/building/node/1> <https://example.com/building/prop/unit> "degC" .
            <https://example.com/building/node/1> <https://example.com/building/prop/name> "TempSensor-1" .
        "#;

        let mut ontology = OntologyStore::new();
        let result = import_rdf(data, RdfFormat::Turtle, None, &sg, &ns, &mut ontology).unwrap();

        assert_eq!(result.nodes_created, 1);
        assert_eq!(result.properties_set, 2);

        sg.read(|g| {
            let node = g.get_node(NodeId(1)).expect("node should exist");

            // Dictionary-flagged property is promoted to InternedStr.
            let unit_val = node.properties.get(IStr::new("unit")).unwrap();
            assert!(
                matches!(unit_val, Value::InternedStr(_)),
                "dictionary property should be Value::InternedStr, got: {unit_val:?}"
            );
            assert_eq!(unit_val.as_str(), Some("degC"));

            // Non-dictionary property remains Value::String.
            let name_val = node.properties.get(IStr::new("name")).unwrap();
            assert!(
                matches!(name_val, Value::String(_)),
                "non-dictionary prop should remain Value::String, got: {name_val:?}"
            );
            assert_eq!(name_val.as_str(), Some("TempSensor-1"));
        });
    }

    #[test]
    fn export_import_round_trip_preserves_graph_structure() {
        use crate::export::export_turtle;
        use selene_core::label_set::LabelSet;
        use selene_core::property_map::PropertyMap;
        use selene_core::value::Value;

        // Build original graph: 2 nodes, 1 edge with a property so reifier
        // quads are warranted and the round-trip stays at exactly 1 edge.
        let mut g1 = SeleneGraph::new();
        let mut m = g1.mutate();
        let n1 = m
            .create_node(
                LabelSet::from_strs(&["Sensor"]),
                PropertyMap::from_pairs([(IStr::new("unit"), Value::String("degC".into()))]),
            )
            .unwrap();
        let n2 = m
            .create_node(
                LabelSet::from_strs(&["Room"]),
                PropertyMap::from_pairs([(IStr::new("name"), Value::String("Lab".into()))]),
            )
            .unwrap();
        m.create_edge(
            n1,
            IStr::new("locatedIn"),
            n2,
            PropertyMap::from_pairs([(IStr::new("weight"), Value::Int(1))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let ns = RdfNamespace::new("https://example.com/building/");

        // Export to Turtle.
        let turtle_bytes = export_turtle(&g1, &ns).unwrap();

        // Import into a fresh graph.
        let shared = SharedGraph::new(SeleneGraph::new());
        let mut ontology = crate::ontology::OntologyStore::new();
        let result = import_rdf(
            &turtle_bytes,
            crate::RdfFormat::Turtle,
            None,
            &shared,
            &ns,
            &mut ontology,
        )
        .unwrap();

        // Verify: same number of nodes and edges.
        assert_eq!(result.nodes_created, 2);
        assert!(
            result.edges_created >= 1,
            "expected at least 1 edge, got {}",
            result.edges_created
        );
        assert_eq!(result.labels_added, 2);
        // 2 node properties + 1 edge property
        assert!(
            result.properties_set >= 3,
            "expected at least 3 properties, got {}",
            result.properties_set
        );
    }

    #[test]
    fn parse_quads_respects_limit() {
        // 3 triples, limit of 2.
        let nt = b"<urn:a> <urn:b> <urn:c> .\n\
                    <urn:d> <urn:e> <urn:f> .\n\
                    <urn:g> <urn:h> <urn:i> .\n";
        let result = parse_quads(nt, crate::RdfFormat::NTriples, 2);
        assert!(matches!(result, Err(crate::RdfError::TooManyQuads(2))));
    }
}
