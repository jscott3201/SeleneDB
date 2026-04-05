//! Ontology store -- in-memory TBox graph for RDF class/property definitions.
//!
//! The [`OntologyStore`] holds RDF class hierarchies, property definitions, and
//! domain/range constraints imported from external ontologies (e.g., Brick
//! Schema, ASHRAE 223P, RealEstateCore). It wraps an [`oxrdf::Dataset`] and
//! places all triples in a single named graph for isolation from the ABox
//! (instance data) graph.
//!
//! Persistence is via N-Quads bytes, which are stored as an extra section in
//! Selene snapshots. The named graph carries through the round-trip so ontology
//! quads and instance quads are always distinguishable.

use oxrdf::{Dataset, GraphName, NamedNode, NamedOrBlankNode, Quad, QuadRef, Term};
use oxttl::{NQuadsParser, NQuadsSerializer};

use crate::RdfError;

/// Named graph URI used for all TBox (ontology) quads.
pub const ONTOLOGY_GRAPH_NAME: &str = "urn:selene:ontology";

/// In-memory store for ontology (TBox) quads.
///
/// All triples are stored in the named graph [`ONTOLOGY_GRAPH_NAME`] so they
/// can be distinguished from ABox (instance) quads during SPARQL evaluation
/// and N-Quads export. The underlying store is [`oxrdf::Dataset`], which
/// provides O(1) insertion and efficient iteration.
#[derive(Debug)]
pub struct OntologyStore {
    dataset: Dataset,
    graph_name: NamedNode,
}

impl OntologyStore {
    /// Create an empty ontology store.
    pub fn new() -> Self {
        Self {
            dataset: Dataset::new(),
            graph_name: NamedNode::new(ONTOLOGY_GRAPH_NAME)
                .expect("ONTOLOGY_GRAPH_NAME is a valid IRI"),
        }
    }

    /// Insert a quad directly into the store.
    ///
    /// The quad's graph name is preserved as-is. Use `insert_triple` to
    /// always route into the ontology named graph.
    pub fn insert(&mut self, quad: &Quad) {
        self.dataset.insert(quad.as_ref());
    }

    /// Insert a triple into the ontology named graph.
    ///
    /// The triple is placed in [`ONTOLOGY_GRAPH_NAME`] regardless of any
    /// graph context the caller may have. This is the primary insertion method
    /// for ontology loading.
    pub fn insert_triple(&mut self, subject: NamedOrBlankNode, predicate: NamedNode, object: Term) {
        let quad = Quad::new(
            subject,
            predicate,
            object,
            GraphName::NamedNode(self.graph_name.clone()),
        );
        self.dataset.insert(quad.as_ref());
    }

    /// Return the number of quads in the store.
    pub fn len(&self) -> usize {
        self.dataset.len()
    }

    /// Return true if the store contains no quads.
    pub fn is_empty(&self) -> bool {
        self.dataset.is_empty()
    }

    /// Iterate all quads in the store.
    pub fn quads(&self) -> impl Iterator<Item = QuadRef<'_>> {
        self.dataset.iter()
    }

    /// Return a reference to the underlying oxrdf `Dataset`.
    ///
    /// Used by the SPARQL adapter to delegate ontology graph pattern matching.
    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// Return the named graph URI used for ontology quads.
    pub fn graph_name(&self) -> &NamedNode {
        &self.graph_name
    }

    /// Remove all quads from the store.
    pub fn clear(&mut self) {
        self.dataset.clear();
    }

    /// Serialize the store to N-Quads bytes for snapshot persistence.
    ///
    /// The graph name is preserved in the output so `from_nquads` can
    /// reconstruct the store exactly.
    pub fn to_nquads(&self) -> Result<Vec<u8>, RdfError> {
        let mut buf = Vec::new();
        let mut writer = NQuadsSerializer::new().for_writer(&mut buf);
        for quad in &self.dataset {
            writer
                .serialize_quad(quad)
                .map_err(|e| RdfError::Serialize(e.to_string()))?;
        }
        writer.finish();
        Ok(buf)
    }

    /// Deserialize an ontology store from N-Quads bytes.
    ///
    /// All quads in `data` are inserted into the store; their graph names are
    /// preserved as written in the N-Quads stream. Returns an error if any
    /// line cannot be parsed.
    pub fn from_nquads(data: &[u8]) -> Result<Self, RdfError> {
        let mut store = Self::new();
        let parser = NQuadsParser::new().for_reader(data);
        for result in parser {
            let quad = result.map_err(|e| RdfError::Parse(e.to_string()))?;
            store.dataset.insert(quad.as_ref());
        }
        Ok(store)
    }
}

impl Default for OntologyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term};

    use super::*;

    fn sensor_class() -> NamedNode {
        NamedNode::new("https://brickschema.org/schema/Brick#Sensor").unwrap()
    }

    fn rdf_type() -> NamedNode {
        NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap()
    }

    fn rdfs_class() -> NamedNode {
        NamedNode::new("http://www.w3.org/2000/01/rdf-schema#Class").unwrap()
    }

    #[test]
    fn new_store_is_empty() {
        let store = OntologyStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn insert_triple_adds_quad_in_ontology_graph() {
        let mut store = OntologyStore::new();
        store.insert_triple(
            NamedOrBlankNode::NamedNode(sensor_class()),
            rdf_type(),
            Term::NamedNode(rdfs_class()),
        );
        assert_eq!(store.len(), 1);

        // The inserted quad should have the ontology graph name.
        let quad = store.quads().next().unwrap();
        match quad.graph_name {
            oxrdf::GraphNameRef::NamedNode(nn) => {
                assert_eq!(nn.as_str(), ONTOLOGY_GRAPH_NAME);
            }
            other => panic!("expected named graph, got {other:?}"),
        }
    }

    #[test]
    fn insert_raw_quad_preserves_graph_name() {
        let mut store = OntologyStore::new();
        let custom_graph = NamedNode::new("urn:custom:graph").unwrap();
        let quad = Quad::new(
            NamedOrBlankNode::NamedNode(sensor_class()),
            rdf_type(),
            Term::NamedNode(rdfs_class()),
            GraphName::NamedNode(custom_graph.clone()),
        );
        store.insert(&quad);
        assert_eq!(store.len(), 1);
        let stored = store.quads().next().unwrap();
        match stored.graph_name {
            oxrdf::GraphNameRef::NamedNode(nn) => assert_eq!(nn.as_str(), "urn:custom:graph"),
            other => panic!("expected named graph, got {other:?}"),
        }
    }

    #[test]
    fn clear_removes_all_quads() {
        let mut store = OntologyStore::new();
        store.insert_triple(
            NamedOrBlankNode::NamedNode(sensor_class()),
            rdf_type(),
            Term::NamedNode(rdfs_class()),
        );
        assert!(!store.is_empty());
        store.clear();
        assert!(store.is_empty());
    }

    #[test]
    fn nquads_round_trip() {
        let mut store = OntologyStore::new();
        store.insert_triple(
            NamedOrBlankNode::NamedNode(sensor_class()),
            rdf_type(),
            Term::NamedNode(rdfs_class()),
        );
        store.insert_triple(
            NamedOrBlankNode::NamedNode(sensor_class()),
            NamedNode::new("http://www.w3.org/2000/01/rdf-schema#label").unwrap(),
            Term::Literal(Literal::new_simple_literal("Sensor")),
        );

        let bytes = store.to_nquads().unwrap();
        assert!(!bytes.is_empty());

        let recovered = OntologyStore::from_nquads(&bytes).unwrap();
        assert_eq!(recovered.len(), store.len());
    }

    #[test]
    fn from_nquads_empty_bytes_returns_empty_store() {
        let store = OntologyStore::from_nquads(b"").unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn from_nquads_rejects_malformed_input() {
        let result = OntologyStore::from_nquads(b"this is not valid nquads!!!");
        assert!(result.is_err());
    }

    #[test]
    fn default_impl_produces_empty_store() {
        let store = OntologyStore::default();
        assert!(store.is_empty());
    }

    #[test]
    fn quads_iterator_yields_all_inserted() {
        let mut store = OntologyStore::new();
        for i in 0..5u32 {
            let node = NamedNode::new(format!("urn:test:node{i}")).unwrap();
            store.insert_triple(
                NamedOrBlankNode::NamedNode(node),
                rdf_type(),
                Term::NamedNode(rdfs_class()),
            );
        }
        assert_eq!(store.quads().count(), 5);
    }

    #[test]
    fn blank_node_subject_is_accepted() {
        let mut store = OntologyStore::new();
        store.insert_triple(
            NamedOrBlankNode::BlankNode(BlankNode::new("b0").unwrap()),
            rdf_type(),
            Term::NamedNode(rdfs_class()),
        );
        assert_eq!(store.len(), 1);
    }
}
