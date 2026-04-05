//! SPARQL QueryableDataset adapter.
//!
//! Implements `spareval::QueryableDataset` on Selene's property graph for
//! zero-duplication SPARQL evaluation. The SPARQL engine (spareval) handles
//! joins, filters, and result construction; this adapter provides pattern-
//! matched quads using Selene's native indexes (label bitmaps, CSR adjacency,
//! property lookups).
//!
//! All instance data quads live in the default graph. Ontology quads live in
//! the `urn:selene:ontology` named graph and are delegated to the
//! [`OntologyStore`] when present.

use std::convert::Infallible;

use oxrdf::{NamedNode, Term};
use selene_core::interner::IStr;
use selene_graph::SeleneGraph;
use selene_graph::csr::CsrAdjacency;
use spareval::{InternalQuad, QueryableDataset};

use crate::namespace::RdfNamespace;
use crate::ontology::{ONTOLOGY_GRAPH_NAME, OntologyStore};
use crate::terms::{RdfPredicate, SeleneRdfTerm};

// ---------------------------------------------------------------------------
// SeleneDataset
// ---------------------------------------------------------------------------

/// A queryable dataset view over a Selene property graph.
///
/// Holds shared references to the graph, a pre-built CSR for edge traversal,
/// the RDF namespace configuration, and an optional ontology store.
///
/// Lifetime `'a` is tied to the graph snapshot. Build one per query.
pub struct SeleneDataset<'a> {
    graph: &'a SeleneGraph,
    csr: &'a CsrAdjacency,
    namespace: &'a RdfNamespace,
    ontology: Option<&'a OntologyStore>,
}

impl<'a> SeleneDataset<'a> {
    /// Create a new queryable dataset.
    ///
    /// The CSR must have been built from the same graph snapshot. The ontology
    /// store is optional; when present, quads in the `urn:selene:ontology`
    /// named graph are served from it.
    pub fn new(
        graph: &'a SeleneGraph,
        csr: &'a CsrAdjacency,
        namespace: &'a RdfNamespace,
        ontology: Option<&'a OntologyStore>,
    ) -> Self {
        Self {
            graph,
            csr,
            namespace,
            ontology,
        }
    }
}

// ---------------------------------------------------------------------------
// QueryableDataset implementation
// ---------------------------------------------------------------------------

impl<'a> QueryableDataset<'a> for SeleneDataset<'a> {
    type InternalTerm = SeleneRdfTerm;
    type Error = Infallible;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&SeleneRdfTerm>,
        predicate: Option<&SeleneRdfTerm>,
        object: Option<&SeleneRdfTerm>,
        graph_name: Option<Option<&SeleneRdfTerm>>,
    ) -> impl Iterator<Item = Result<InternalQuad<SeleneRdfTerm>, Infallible>> + use<'a> {
        // Route to the appropriate graph(s) and return a boxed iterator.
        // Each branch collects into a Vec internally (the underlying graph
        // accessors are snapshot-based), but the Box<dyn Iterator> dispatch
        // avoids an extra top-level Vec when we can short-circuit early.
        let iter: Box<dyn Iterator<Item = InternalQuad<SeleneRdfTerm>>> = match graph_name {
            Some(None) => {
                // Default graph: instance data from the property graph.
                self.collect_default_graph(subject, predicate, object)
            }
            Some(Some(g)) => {
                // Specific named graph: check if it matches the ontology graph.
                if self.is_ontology_graph(g) {
                    Box::new(
                        self.collect_ontology_graph(subject, predicate, object)
                            .into_iter(),
                    )
                } else {
                    Box::new(std::iter::empty())
                }
            }
            None => {
                // Any named graph (but NOT default). Only the ontology graph.
                Box::new(
                    self.collect_ontology_graph(subject, predicate, object)
                        .into_iter(),
                )
            }
        };
        iter.map(Ok)
    }

    fn internal_named_graphs(
        &self,
    ) -> impl Iterator<Item = Result<SeleneRdfTerm, Infallible>> + use<'a> {
        let term = self.ontology.filter(|onto| !onto.is_empty()).map(|_| {
            let nn = NamedNode::new_unchecked(ONTOLOGY_GRAPH_NAME);
            Ok(SeleneRdfTerm::OntologyTerm(Term::NamedNode(nn)))
        });
        term.into_iter()
    }

    fn internalize_term(&self, term: Term) -> Result<SeleneRdfTerm, Infallible> {
        Ok(SeleneRdfTerm::internalize(term, self.namespace))
    }

    fn externalize_term(&self, term: SeleneRdfTerm) -> Result<Term, Infallible> {
        Ok(term.externalize(self.namespace))
    }
}

// ---------------------------------------------------------------------------
// Pattern routing
// ---------------------------------------------------------------------------

impl SeleneDataset<'_> {
    /// Check whether a term matches the ontology named graph URI.
    #[allow(clippy::unused_self)]
    fn is_ontology_graph(&self, term: &SeleneRdfTerm) -> bool {
        match term {
            SeleneRdfTerm::OntologyTerm(Term::NamedNode(nn)) => nn.as_str() == ONTOLOGY_GRAPH_NAME,
            _ => false,
        }
    }

    // -----------------------------------------------------------------------
    // Ontology graph delegation
    // -----------------------------------------------------------------------

    /// Collect quads from the ontology store.
    ///
    /// When bound positions are provided, the corresponding SeleneRdfTerms are
    /// externalized back to oxrdf terms and passed to `Dataset::quads_for_pattern`
    /// so the underlying index pre-filters instead of scanning all quads.
    fn collect_ontology_graph(
        &self,
        subject: Option<&SeleneRdfTerm>,
        predicate: Option<&SeleneRdfTerm>,
        object: Option<&SeleneRdfTerm>,
    ) -> Vec<InternalQuad<SeleneRdfTerm>> {
        let Some(onto) = self.ontology else {
            return Vec::new();
        };

        let ontology_graph_term =
            SeleneRdfTerm::OntologyTerm(Term::NamedNode(onto.graph_name().clone()));

        // Externalize bound positions to oxrdf terms for index-based filtering.
        let ext_s = subject.map(|t| t.externalize(self.namespace));
        let ext_p = predicate.map(|t| t.externalize(self.namespace));
        let ext_o = object.map(|t| t.externalize(self.namespace));

        // Extract oxrdf reference types for quads_for_pattern.
        let s_ref = ext_s.as_ref().and_then(|t| match t {
            Term::NamedNode(nn) => Some(oxrdf::NamedOrBlankNodeRef::from(nn.as_ref())),
            Term::BlankNode(bn) => Some(oxrdf::NamedOrBlankNodeRef::from(bn.as_ref())),
            Term::Literal(_) => None,
        });
        let p_ref = ext_p.as_ref().and_then(|t| match t {
            Term::NamedNode(nn) => Some(nn.as_ref()),
            _ => None,
        });
        let o_ref = ext_o.as_ref().map(|t| t.as_ref());
        let gn = oxrdf::GraphNameRef::NamedNode(onto.graph_name().as_ref());

        let mut quads = Vec::new();
        for quad_ref in onto
            .dataset()
            .quads_for_pattern(s_ref, p_ref, o_ref, Some(gn))
        {
            let s =
                SeleneRdfTerm::internalize(quad_ref.subject.into_owned().into(), self.namespace);
            let p =
                SeleneRdfTerm::internalize(quad_ref.predicate.into_owned().into(), self.namespace);
            let o = SeleneRdfTerm::internalize(quad_ref.object.into_owned(), self.namespace);

            quads.push(InternalQuad {
                subject: s,
                predicate: p,
                object: o,
                graph_name: Some(ontology_graph_term.clone()),
            });
        }
        quads
    }

    // -----------------------------------------------------------------------
    // Default graph -- instance data
    // -----------------------------------------------------------------------

    /// Route pattern matching based on which positions are bound.
    ///
    /// Returns a boxed iterator instead of a collected `Vec` so that the
    /// caller can chain results without an extra intermediate allocation.
    fn collect_default_graph(
        &self,
        subject: Option<&SeleneRdfTerm>,
        predicate: Option<&SeleneRdfTerm>,
        object: Option<&SeleneRdfTerm>,
    ) -> Box<dyn Iterator<Item = InternalQuad<SeleneRdfTerm>>> {
        match (subject, predicate, object) {
            // Subject + Predicate bound (object may also be bound).
            (Some(s), Some(p), obj) => Box::new(self.quads_sp(s, p, obj).into_iter()),

            // Subject bound, predicate unbound.
            (Some(s), None, obj) => Box::new(self.quads_s(s, obj).into_iter()),

            // Predicate bound, subject unbound.
            (None, Some(p), obj) => Box::new(self.quads_p(p, obj).into_iter()),

            // Object only bound (or nothing bound).
            (None, None, Some(o)) => Box::new(self.quads_o(o).into_iter()),

            // Nothing bound -- full graph scan.
            (None, None, None) => Box::new(self.quads_full_scan().into_iter()),
        }
    }

    // -----------------------------------------------------------------------
    // Subject + Predicate bound
    // -----------------------------------------------------------------------

    fn quads_sp(
        &self,
        subject: &SeleneRdfTerm,
        predicate: &SeleneRdfTerm,
        object: Option<&SeleneRdfTerm>,
    ) -> Vec<InternalQuad<SeleneRdfTerm>> {
        let mut quads = Vec::with_capacity(4);

        let node_id = match subject {
            SeleneRdfTerm::Node(id) => *id,
            _ => return quads,
        };

        let Some(node) = self.graph.get_node(node_id) else {
            return quads;
        };

        match predicate {
            SeleneRdfTerm::Predicate(RdfPredicate::RdfType) => {
                // Emit rdf:type quads for each label.
                for label in node.labels.iter() {
                    let obj = SeleneRdfTerm::Type(label);
                    if let Some(expected) = object
                        && &obj != expected
                    {
                        continue;
                    }
                    quads.push(self.default_quad(subject.clone(), predicate.clone(), obj));
                }
            }
            SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(key)) => {
                // Direct property lookup.
                if let Some(val) = node.properties.get(*key) {
                    let obj = SeleneRdfTerm::Literal(val.clone());
                    if object.is_none() || object == Some(&obj) {
                        quads.push(self.default_quad(subject.clone(), predicate.clone(), obj));
                    }
                }
            }
            SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(label)) => {
                // Typed outgoing edge traversal via CSR.
                for nbr in self.csr.outgoing_typed(node_id, *label) {
                    let obj = SeleneRdfTerm::Node(nbr.node_id);
                    if let Some(expected) = object
                        && &obj != expected
                    {
                        continue;
                    }
                    quads.push(self.default_quad(subject.clone(), predicate.clone(), obj));
                }
            }
            SeleneRdfTerm::Predicate(RdfPredicate::SosaSensor) => {
                // madeBySensor is an edge, not a property. Handle via CSR.
                let label = IStr::new("madeBySensor");
                for nbr in self.csr.outgoing_typed(node_id, label) {
                    let obj = SeleneRdfTerm::Node(nbr.node_id);
                    if let Some(expected) = object
                        && &obj != expected
                    {
                        continue;
                    }
                    quads.push(self.default_quad(subject.clone(), predicate.clone(), obj));
                }
            }
            SeleneRdfTerm::Predicate(
                RdfPredicate::SosaResult | RdfPredicate::SosaTime | RdfPredicate::SosaProperty,
            ) => {
                // SOSA property predicates map to property keys on Observation nodes.
                if let Some(key) = sosa_predicate_to_key(predicate)
                    && let Some(val) = node.properties.get(IStr::new(key))
                {
                    let obj = SeleneRdfTerm::Literal(val.clone());
                    if object.is_none() || object == Some(&obj) {
                        quads.push(self.default_quad(subject.clone(), predicate.clone(), obj));
                    }
                }
            }
            _ => {
                // Unknown predicate -- no matches in instance data.
            }
        }

        quads
    }

    // -----------------------------------------------------------------------
    // Subject only bound
    // -----------------------------------------------------------------------

    fn quads_s(
        &self,
        subject: &SeleneRdfTerm,
        object: Option<&SeleneRdfTerm>,
    ) -> Vec<InternalQuad<SeleneRdfTerm>> {
        let node_id = match subject {
            SeleneRdfTerm::Node(id) => *id,
            _ => return Vec::new(),
        };

        let Some(node) = self.graph.get_node(node_id) else {
            return Vec::new();
        };

        // Estimate: labels + properties + outgoing edges.
        let cap = node.labels.len() + node.properties.len() + self.csr.outgoing(node_id).len();
        let mut quads = Vec::with_capacity(cap);

        let rdf_type = SeleneRdfTerm::Predicate(RdfPredicate::RdfType);

        // Emit rdf:type quads for each label.
        for label in node.labels.iter() {
            let obj = SeleneRdfTerm::Type(label);
            if let Some(expected) = object
                && &obj != expected
            {
                continue;
            }
            quads.push(self.default_quad(subject.clone(), rdf_type.clone(), obj));
        }

        // Emit property quads.
        for (key, val) in node.properties.iter() {
            let pred = SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(*key));
            let obj = SeleneRdfTerm::Literal(val.clone());
            if let Some(expected) = object
                && &obj != expected
            {
                continue;
            }
            quads.push(self.default_quad(subject.clone(), pred, obj));
        }

        // Emit outgoing edge quads via CSR.
        for nbr in self.csr.outgoing(node_id) {
            let pred = SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(nbr.label));
            let obj = SeleneRdfTerm::Node(nbr.node_id);
            if let Some(expected) = object
                && &obj != expected
            {
                continue;
            }
            quads.push(self.default_quad(subject.clone(), pred, obj));
        }

        quads
    }

    // -----------------------------------------------------------------------
    // Predicate only bound
    // -----------------------------------------------------------------------

    fn quads_p(
        &self,
        predicate: &SeleneRdfTerm,
        object: Option<&SeleneRdfTerm>,
    ) -> Vec<InternalQuad<SeleneRdfTerm>> {
        let mut quads = Vec::with_capacity(self.graph.node_count());

        match predicate {
            SeleneRdfTerm::Predicate(RdfPredicate::RdfType) => {
                // If object is bound and is a Type, use label bitmap scan.
                if let Some(SeleneRdfTerm::Type(label)) = object {
                    for nid in self.graph.nodes_by_label(label.as_str()) {
                        quads.push(self.default_quad(
                            SeleneRdfTerm::Node(nid),
                            predicate.clone(),
                            SeleneRdfTerm::Type(*label),
                        ));
                    }
                } else {
                    // Scan all nodes and emit rdf:type for each label.
                    for nid in self.graph.all_node_ids() {
                        if let Some(node) = self.graph.get_node(nid) {
                            for label in node.labels.iter() {
                                let obj = SeleneRdfTerm::Type(label);
                                if let Some(expected) = object
                                    && &obj != expected
                                {
                                    continue;
                                }
                                quads.push(self.default_quad(
                                    SeleneRdfTerm::Node(nid),
                                    predicate.clone(),
                                    obj,
                                ));
                            }
                        }
                    }
                }
            }
            SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(key)) => {
                // Scan all nodes for this property.
                for nid in self.graph.all_node_ids() {
                    if let Some(node) = self.graph.get_node(nid)
                        && let Some(val) = node.properties.get(*key)
                    {
                        let obj = SeleneRdfTerm::Literal(val.clone());
                        if let Some(expected) = object
                            && &obj != expected
                        {
                            continue;
                        }
                        quads.push(self.default_quad(
                            SeleneRdfTerm::Node(nid),
                            predicate.clone(),
                            obj,
                        ));
                    }
                }
            }
            SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(label)) => {
                // Scan all edges with this label.
                for eid in self.graph.edges_by_label(label.as_str()) {
                    if let Some(edge) = self.graph.get_edge(eid) {
                        let s = SeleneRdfTerm::Node(edge.source);
                        let obj = SeleneRdfTerm::Node(edge.target);
                        if let Some(expected) = object
                            && &obj != expected
                        {
                            continue;
                        }
                        quads.push(self.default_quad(s, predicate.clone(), obj));
                    }
                }
            }
            SeleneRdfTerm::Predicate(RdfPredicate::SosaSensor) => {
                // madeBySensor edges.
                for eid in self.graph.edges_by_label("madeBySensor") {
                    if let Some(edge) = self.graph.get_edge(eid) {
                        let s = SeleneRdfTerm::Node(edge.source);
                        let obj = SeleneRdfTerm::Node(edge.target);
                        if let Some(expected) = object
                            && &obj != expected
                        {
                            continue;
                        }
                        quads.push(self.default_quad(s, predicate.clone(), obj));
                    }
                }
            }
            SeleneRdfTerm::Predicate(
                RdfPredicate::SosaResult | RdfPredicate::SosaTime | RdfPredicate::SosaProperty,
            ) => {
                // SOSA property predicates -- scan Observation nodes.
                if let Some(key) = sosa_predicate_to_key(predicate) {
                    for nid in self.graph.nodes_by_label("Observation") {
                        if let Some(node) = self.graph.get_node(nid)
                            && let Some(val) = node.properties.get(IStr::new(key))
                        {
                            let obj = SeleneRdfTerm::Literal(val.clone());
                            if let Some(expected) = object
                                && &obj != expected
                            {
                                continue;
                            }
                            quads.push(self.default_quad(
                                SeleneRdfTerm::Node(nid),
                                predicate.clone(),
                                obj,
                            ));
                        }
                    }
                }
            }
            _ => {
                // Unknown predicate -- no matches.
            }
        }

        quads
    }

    // -----------------------------------------------------------------------
    // Object only bound
    // -----------------------------------------------------------------------

    fn quads_o(&self, object: &SeleneRdfTerm) -> Vec<InternalQuad<SeleneRdfTerm>> {
        let mut quads = Vec::with_capacity(16);

        match object {
            SeleneRdfTerm::Node(target_id) => {
                // Incoming edges to this node.
                for nbr in self.csr.incoming(*target_id) {
                    let s = SeleneRdfTerm::Node(nbr.node_id);
                    let p = SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(nbr.label));
                    quads.push(self.default_quad(s, p, object.clone()));
                }
            }
            SeleneRdfTerm::Type(label) => {
                // rdf:type with this class as object.
                let rdf_type = SeleneRdfTerm::Predicate(RdfPredicate::RdfType);
                for nid in self.graph.nodes_by_label(label.as_str()) {
                    quads.push(self.default_quad(
                        SeleneRdfTerm::Node(nid),
                        rdf_type.clone(),
                        object.clone(),
                    ));
                }
            }
            SeleneRdfTerm::Literal(val) => {
                // Scan all nodes for matching property values.
                for nid in self.graph.all_node_ids() {
                    if let Some(node) = self.graph.get_node(nid) {
                        for (key, v) in node.properties.iter() {
                            if v == val {
                                let pred =
                                    SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(*key));
                                quads.push(self.default_quad(
                                    SeleneRdfTerm::Node(nid),
                                    pred,
                                    object.clone(),
                                ));
                            }
                        }
                    }
                }
            }
            _ => {
                // OntologyTerm, EdgeReifier, etc. -- no instance data matches
                // when only the object is bound.
            }
        }

        quads
    }

    // -----------------------------------------------------------------------
    // Full graph scan
    // -----------------------------------------------------------------------

    fn quads_full_scan(&self) -> Vec<InternalQuad<SeleneRdfTerm>> {
        // Estimate: ~3 quads per node (type + props) + 1 per edge.
        let mut quads = Vec::with_capacity(self.graph.node_count() * 3 + self.graph.edge_count());
        let rdf_type = SeleneRdfTerm::Predicate(RdfPredicate::RdfType);

        // All nodes: labels + properties.
        for nid in self.graph.all_node_ids() {
            let subject = SeleneRdfTerm::Node(nid);
            if let Some(node) = self.graph.get_node(nid) {
                for label in node.labels.iter() {
                    quads.push(self.default_quad(
                        subject.clone(),
                        rdf_type.clone(),
                        SeleneRdfTerm::Type(label),
                    ));
                }
                for (key, val) in node.properties.iter() {
                    quads.push(self.default_quad(
                        subject.clone(),
                        SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(*key)),
                        SeleneRdfTerm::Literal(val.clone()),
                    ));
                }
            }
        }

        // All edges: relationship triples.
        for eid in self.graph.all_edge_ids() {
            if let Some(edge) = self.graph.get_edge(eid) {
                quads.push(self.default_quad(
                    SeleneRdfTerm::Node(edge.source),
                    SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(edge.label)),
                    SeleneRdfTerm::Node(edge.target),
                ));
            }
        }

        quads
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Build an `InternalQuad` in the default graph (graph_name = None).
    #[allow(clippy::unused_self)]
    fn default_quad(
        &self,
        subject: SeleneRdfTerm,
        predicate: SeleneRdfTerm,
        object: SeleneRdfTerm,
    ) -> InternalQuad<SeleneRdfTerm> {
        InternalQuad {
            subject,
            predicate,
            object,
            graph_name: None,
        }
    }
}

// ---------------------------------------------------------------------------
// SOSA predicate mapping
// ---------------------------------------------------------------------------

/// Map a SOSA predicate term to the Selene property key name.
///
/// `SosaSensor` ("madeBySensor") is an edge, not a property. Returns `None`
/// for it because the caller must handle it via CSR edge traversal.
fn sosa_predicate_to_key(pred: &SeleneRdfTerm) -> Option<&'static str> {
    match pred {
        SeleneRdfTerm::Predicate(RdfPredicate::SosaResult) => Some("simpleResult"),
        SeleneRdfTerm::Predicate(RdfPredicate::SosaTime) => Some("resultTime"),
        SeleneRdfTerm::Predicate(RdfPredicate::SosaProperty) => Some("observedProperty"),
        // SosaSensor is an edge relationship, not a property. Callers handle it
        // via CSR edge traversal in the SosaSensor match arm.
        SeleneRdfTerm::Predicate(RdfPredicate::SosaSensor) => None,
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::interner::IStr;
    use selene_core::label_set::LabelSet;
    use selene_core::property_map::PropertyMap;
    use selene_core::value::Value;
    use selene_core::{EdgeId, NodeId};
    use selene_graph::SeleneGraph;

    fn test_ns() -> RdfNamespace {
        RdfNamespace::new("https://example.com/building/")
    }

    /// Build a small test graph:
    /// Node 1: labels {Sensor}, props {unit: "degC", floor: 3}
    /// Node 2: labels {Room}, props {name: "Lobby"}
    /// Edge 1: Node 1 -[locatedIn]-> Node 2
    fn build_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();

        let n1 = m
            .create_node(
                LabelSet::from_strs(&["Sensor"]),
                PropertyMap::from_pairs([
                    (IStr::new("unit"), Value::str("degC")),
                    (IStr::new("floor"), Value::Int(3)),
                ]),
            )
            .unwrap();

        let n2 = m
            .create_node(
                LabelSet::from_strs(&["Room"]),
                PropertyMap::from_pairs([(IStr::new("name"), Value::str("Lobby"))]),
            )
            .unwrap();

        m.create_edge(n1, IStr::new("locatedIn"), n2, PropertyMap::new())
            .unwrap();

        m.commit(0).unwrap();
        g
    }

    fn make_dataset<'a>(
        graph: &'a SeleneGraph,
        csr: &'a CsrAdjacency,
        ns: &'a RdfNamespace,
    ) -> SeleneDataset<'a> {
        SeleneDataset::new(graph, csr, ns, None)
    }

    /// Helper: collect all quads from a pattern query.
    #[allow(clippy::option_option)]
    fn collect_quads(
        ds: &SeleneDataset<'_>,
        subject: Option<&SeleneRdfTerm>,
        predicate: Option<&SeleneRdfTerm>,
        object: Option<&SeleneRdfTerm>,
        graph_name: Option<Option<&SeleneRdfTerm>>,
    ) -> Vec<InternalQuad<SeleneRdfTerm>> {
        ds.internal_quads_for_pattern(subject, predicate, object, graph_name)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    // --- internalize / externalize round-trip ---

    #[test]
    fn internalize_node_uri() {
        let ns = test_ns();
        let term = Term::NamedNode(ns.node_uri(NodeId(42)));
        let internal = SeleneRdfTerm::internalize(term, &ns);
        assert_eq!(internal, SeleneRdfTerm::Node(NodeId(42)));
    }

    #[test]
    fn internalize_type_uri() {
        let ns = test_ns();
        let term = Term::NamedNode(ns.type_uri("Sensor"));
        let internal = SeleneRdfTerm::internalize(term, &ns);
        assert_eq!(internal, SeleneRdfTerm::Type(IStr::new("Sensor")));
    }

    #[test]
    fn internalize_prop_uri() {
        let ns = test_ns();
        let term = Term::NamedNode(ns.prop_uri("temperature"));
        let internal = SeleneRdfTerm::internalize(term, &ns);
        assert_eq!(
            internal,
            SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(IStr::new("temperature")))
        );
    }

    #[test]
    fn internalize_rel_uri() {
        let ns = test_ns();
        let term = Term::NamedNode(ns.rel_uri("feeds"));
        let internal = SeleneRdfTerm::internalize(term, &ns);
        assert_eq!(
            internal,
            SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(IStr::new("feeds")))
        );
    }

    #[test]
    fn internalize_edge_uri() {
        let ns = test_ns();
        let term = Term::NamedNode(ns.edge_uri(EdgeId(99)));
        let internal = SeleneRdfTerm::internalize(term, &ns);
        assert_eq!(internal, SeleneRdfTerm::EdgeReifier(EdgeId(99)));
    }

    #[test]
    fn internalize_obs_uri() {
        let ns = test_ns();
        let term = Term::NamedNode(ns.obs_uri(NodeId(5), "temperature"));
        let internal = SeleneRdfTerm::internalize(term, &ns);
        assert_eq!(
            internal,
            SeleneRdfTerm::Observation(NodeId(5), IStr::new("temperature"))
        );
    }

    #[test]
    fn internalize_rdf_type() {
        let ns = test_ns();
        let term = Term::NamedNode(RdfNamespace::rdf_type().clone());
        let internal = SeleneRdfTerm::internalize(term, &ns);
        assert_eq!(internal, SeleneRdfTerm::Predicate(RdfPredicate::RdfType));
    }

    #[test]
    fn internalize_sosa_predicates() {
        let ns = test_ns();

        let result_term = Term::NamedNode(RdfNamespace::sosa_uri("hasSimpleResult"));
        assert_eq!(
            SeleneRdfTerm::internalize(result_term, &ns),
            SeleneRdfTerm::Predicate(RdfPredicate::SosaResult)
        );

        let time_term = Term::NamedNode(RdfNamespace::sosa_uri("resultTime"));
        assert_eq!(
            SeleneRdfTerm::internalize(time_term, &ns),
            SeleneRdfTerm::Predicate(RdfPredicate::SosaTime)
        );

        let sensor_term = Term::NamedNode(RdfNamespace::sosa_uri("madeBySensor"));
        assert_eq!(
            SeleneRdfTerm::internalize(sensor_term, &ns),
            SeleneRdfTerm::Predicate(RdfPredicate::SosaSensor)
        );

        let prop_term = Term::NamedNode(RdfNamespace::sosa_uri("observedProperty"));
        assert_eq!(
            SeleneRdfTerm::internalize(prop_term, &ns),
            SeleneRdfTerm::Predicate(RdfPredicate::SosaProperty)
        );
    }

    #[test]
    fn internalize_unknown_sosa_becomes_external() {
        let ns = test_ns();
        let term = Term::NamedNode(RdfNamespace::sosa_uri("UnknownThing"));
        let internal = SeleneRdfTerm::internalize(term, &ns);
        match &internal {
            SeleneRdfTerm::Predicate(RdfPredicate::External(nn)) => {
                assert!(nn.as_str().contains("UnknownThing"));
            }
            other => panic!("expected External predicate, got {other:?}"),
        }
    }

    #[test]
    fn internalize_literal() {
        let ns = test_ns();
        let lit = oxrdf::Literal::new_typed_literal("42", oxrdf::vocab::xsd::LONG);
        let term = Term::Literal(lit);
        let internal = SeleneRdfTerm::internalize(term, &ns);
        assert_eq!(internal, SeleneRdfTerm::Literal(Value::Int(42)));
    }

    #[test]
    fn internalize_blank_node() {
        let ns = test_ns();
        let bn = oxrdf::BlankNode::new("b0").unwrap();
        let term = Term::BlankNode(bn.clone());
        let internal = SeleneRdfTerm::internalize(term, &ns);
        match &internal {
            SeleneRdfTerm::OntologyTerm(Term::BlankNode(b)) => {
                assert_eq!(b.as_str(), "b0");
            }
            other => panic!("expected OntologyTerm(BlankNode), got {other:?}"),
        }
    }

    #[test]
    fn internalize_external_named_node() {
        let ns = test_ns();
        let nn = NamedNode::new_unchecked("https://brickschema.org/schema/Brick#Sensor");
        let term = Term::NamedNode(nn.clone());
        let internal = SeleneRdfTerm::internalize(term, &ns);
        match &internal {
            SeleneRdfTerm::OntologyTerm(Term::NamedNode(n)) => {
                assert_eq!(n.as_str(), nn.as_str());
            }
            other => panic!("expected OntologyTerm(NamedNode), got {other:?}"),
        }
    }

    #[test]
    fn externalize_round_trip() {
        let ns = test_ns();

        let cases = vec![
            SeleneRdfTerm::Node(NodeId(42)),
            SeleneRdfTerm::EdgeReifier(EdgeId(99)),
            SeleneRdfTerm::Type(IStr::new("Sensor")),
            SeleneRdfTerm::Predicate(RdfPredicate::RdfType),
            SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(IStr::new("temp"))),
            SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(IStr::new("feeds"))),
            SeleneRdfTerm::Predicate(RdfPredicate::SosaResult),
            SeleneRdfTerm::Predicate(RdfPredicate::SosaTime),
            SeleneRdfTerm::Predicate(RdfPredicate::SosaSensor),
            SeleneRdfTerm::Predicate(RdfPredicate::SosaProperty),
            SeleneRdfTerm::Literal(Value::Int(42)),
            SeleneRdfTerm::Observation(NodeId(5), IStr::new("temperature")),
        ];

        for original in &cases {
            let ext = original.externalize(&ns);
            let back = SeleneRdfTerm::internalize(ext, &ns);
            assert_eq!(&back, original, "round-trip failed for {original:?}");
        }
    }

    // --- QueryableDataset: full scan ---

    #[test]
    fn full_scan_returns_all_quads() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let quads = collect_quads(&ds, None, None, None, Some(None));

        // Node 1: 1 label + 2 props = 3
        // Node 2: 1 label + 1 prop  = 2
        // Edge 1: 1 relationship     = 1
        // Total = 6
        assert_eq!(quads.len(), 6, "expected 6 quads from full scan");
    }

    // --- Subject bound ---

    #[test]
    fn subject_bound_returns_node_quads() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let subject = SeleneRdfTerm::Node(NodeId(1));
        let quads = collect_quads(&ds, Some(&subject), None, None, Some(None));

        // Node 1: 1 label + 2 props + 1 outgoing edge = 4
        assert_eq!(quads.len(), 4, "expected 4 quads for node 1");
    }

    // --- Subject + Predicate bound: rdf:type ---

    #[test]
    fn sp_rdf_type() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let subject = SeleneRdfTerm::Node(NodeId(1));
        let predicate = SeleneRdfTerm::Predicate(RdfPredicate::RdfType);
        let quads = collect_quads(&ds, Some(&subject), Some(&predicate), None, Some(None));

        assert_eq!(quads.len(), 1, "node 1 has 1 label");
        assert_eq!(quads[0].object, SeleneRdfTerm::Type(IStr::new("Sensor")));
    }

    // --- Subject + Predicate bound: property key ---

    #[test]
    fn sp_property_key() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let subject = SeleneRdfTerm::Node(NodeId(1));
        let predicate = SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(IStr::new("unit")));
        let quads = collect_quads(&ds, Some(&subject), Some(&predicate), None, Some(None));

        assert_eq!(quads.len(), 1, "node 1 has 'unit' property");
        assert_eq!(quads[0].object, SeleneRdfTerm::Literal(Value::str("degC")));
    }

    // --- Subject + Predicate bound: edge label ---

    #[test]
    fn sp_edge_label() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let subject = SeleneRdfTerm::Node(NodeId(1));
        let predicate = SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(IStr::new("locatedIn")));
        let quads = collect_quads(&ds, Some(&subject), Some(&predicate), None, Some(None));

        assert_eq!(quads.len(), 1, "node 1 has 1 locatedIn edge");
        assert_eq!(quads[0].object, SeleneRdfTerm::Node(NodeId(2)));
    }

    // --- Predicate bound: rdf:type with object ---

    #[test]
    fn p_rdf_type_with_object() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let predicate = SeleneRdfTerm::Predicate(RdfPredicate::RdfType);
        let object = SeleneRdfTerm::Type(IStr::new("Sensor"));
        let quads = collect_quads(&ds, None, Some(&predicate), Some(&object), Some(None));

        assert_eq!(quads.len(), 1, "exactly 1 node with label Sensor");
        assert_eq!(quads[0].subject, SeleneRdfTerm::Node(NodeId(1)));
    }

    // --- Object only bound: incoming edges ---

    #[test]
    fn object_bound_incoming_edges() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let object = SeleneRdfTerm::Node(NodeId(2));
        let quads = collect_quads(&ds, None, None, Some(&object), Some(None));

        // Node 2 has 1 incoming edge: node 1 -[locatedIn]-> node 2
        assert_eq!(quads.len(), 1, "node 2 has 1 incoming edge");
        assert_eq!(quads[0].subject, SeleneRdfTerm::Node(NodeId(1)));
    }

    // --- Object bound: rdf:type label ---

    #[test]
    fn object_bound_type_label() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let object = SeleneRdfTerm::Type(IStr::new("Room"));
        let quads = collect_quads(&ds, None, None, Some(&object), Some(None));

        assert_eq!(quads.len(), 1, "1 node with Room label");
        assert_eq!(quads[0].subject, SeleneRdfTerm::Node(NodeId(2)));
    }

    // --- Predicate bound: edge label scan ---

    #[test]
    fn predicate_bound_edge_label() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let predicate = SeleneRdfTerm::Predicate(RdfPredicate::EdgeLabel(IStr::new("locatedIn")));
        let quads = collect_quads(&ds, None, Some(&predicate), None, Some(None));

        assert_eq!(quads.len(), 1, "1 locatedIn edge");
    }

    // --- Predicate bound: property scan ---

    #[test]
    fn predicate_bound_property_scan() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let predicate = SeleneRdfTerm::Predicate(RdfPredicate::PropertyKey(IStr::new("name")));
        let quads = collect_quads(&ds, None, Some(&predicate), None, Some(None));

        // Only node 2 has a "name" property.
        assert_eq!(quads.len(), 1, "1 node has 'name' property");
        assert_eq!(quads[0].subject, SeleneRdfTerm::Node(NodeId(2)));
    }

    // --- Subject + Predicate + Object bound ---

    #[test]
    fn spo_bound_match() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let s = SeleneRdfTerm::Node(NodeId(1));
        let p = SeleneRdfTerm::Predicate(RdfPredicate::RdfType);
        let o = SeleneRdfTerm::Type(IStr::new("Sensor"));
        let quads = collect_quads(&ds, Some(&s), Some(&p), Some(&o), Some(None));

        assert_eq!(quads.len(), 1);
    }

    #[test]
    fn spo_bound_no_match() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let s = SeleneRdfTerm::Node(NodeId(1));
        let p = SeleneRdfTerm::Predicate(RdfPredicate::RdfType);
        let o = SeleneRdfTerm::Type(IStr::new("Room")); // Node 1 is not a Room
        let quads = collect_quads(&ds, Some(&s), Some(&p), Some(&o), Some(None));

        assert_eq!(quads.len(), 0);
    }

    // --- Named graph: no ontology -> empty ---

    #[test]
    fn named_graph_without_ontology() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        // Query for any named graph -- should be empty (no ontology).
        let quads = collect_quads(&ds, None, None, None, None);
        assert_eq!(quads.len(), 0);
    }

    // --- Named graph: with ontology ---

    #[test]
    fn ontology_graph_returns_quads() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();

        let mut onto = OntologyStore::new();
        let sensor_class = NamedNode::new_unchecked("https://brick.org/Sensor");
        let rdfs_class = NamedNode::new_unchecked("http://www.w3.org/2000/01/rdf-schema#Class");
        let rdf_type_nn =
            NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        onto.insert_triple(
            oxrdf::NamedOrBlankNode::NamedNode(sensor_class.clone()),
            rdf_type_nn.clone(),
            Term::NamedNode(rdfs_class.clone()),
        );

        let ds = SeleneDataset::new(&graph, &csr, &ns, Some(&onto));

        // Query the ontology named graph.
        let onto_term = SeleneRdfTerm::OntologyTerm(Term::NamedNode(NamedNode::new_unchecked(
            ONTOLOGY_GRAPH_NAME,
        )));
        let quads = collect_quads(&ds, None, None, None, Some(Some(&onto_term)));

        assert_eq!(quads.len(), 1, "ontology has 1 triple");
        // Verify graph_name is set.
        assert!(quads[0].graph_name.is_some());
    }

    // --- internalize_term / externalize_term via QueryableDataset ---

    #[test]
    fn dataset_internalize_externalize() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let original = Term::NamedNode(ns.node_uri(NodeId(1)));
        let internal = ds.internalize_term(original.clone()).unwrap();
        let back = ds.externalize_term(internal).unwrap();
        assert_eq!(back, original);
    }

    // --- internal_named_graphs ---

    #[test]
    fn named_graphs_empty_without_ontology() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let graphs: Vec<SeleneRdfTerm> = ds
            .internal_named_graphs()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(graphs.is_empty());
    }

    #[test]
    fn named_graphs_with_ontology() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();

        let mut onto = OntologyStore::new();
        onto.insert_triple(
            oxrdf::NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("urn:test:A")),
            NamedNode::new_unchecked("urn:test:pred"),
            Term::NamedNode(NamedNode::new_unchecked("urn:test:B")),
        );

        let ds = SeleneDataset::new(&graph, &csr, &ns, Some(&onto));
        let graphs: Vec<SeleneRdfTerm> = ds
            .internal_named_graphs()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(graphs.len(), 1);
    }

    // --- Nonexistent node ---

    #[test]
    fn subject_nonexistent_node_returns_empty() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let subject = SeleneRdfTerm::Node(NodeId(999));
        let quads = collect_quads(&ds, Some(&subject), None, None, Some(None));
        assert!(quads.is_empty());
    }

    // --- Object literal scan ---

    #[test]
    fn object_literal_scan() {
        let graph = build_graph();
        let csr = CsrAdjacency::build(&graph);
        let ns = test_ns();
        let ds = make_dataset(&graph, &csr, &ns);

        let object = SeleneRdfTerm::Literal(Value::Int(3));
        let quads = collect_quads(&ds, None, None, Some(&object), Some(None));

        // Node 1 has floor=3.
        assert_eq!(quads.len(), 1);
        assert_eq!(quads[0].subject, SeleneRdfTerm::Node(NodeId(1)));
    }
}
