//! RDF export -- serialize a SeleneGraph to Turtle, N-Triples, or N-Quads.
//!
//! The main entry point is [`export_graph`], which dispatches to the
//! format-specific functions. Each serializer converts the graph to quads via
//! [`crate::mapping::graph_to_quads`] and then writes them using the
//! appropriate oxttl writer.

use oxrdf::TripleRef;
use oxttl::{NQuadsSerializer, NTriplesSerializer, TurtleSerializer};
use selene_graph::SeleneGraph;

use crate::mapping::graph_to_quads_scoped;
use crate::namespace::RdfNamespace;
use crate::ontology::OntologyStore;
use crate::{RdfError, RdfFormat};

/// Serialize a [`SeleneGraph`] to RDF bytes in the requested format.
///
/// - `ontology` and `include_all_graphs` are only used by the N-Quads path
///   to optionally include ontology quads in a named graph.
/// - For Turtle and N-Triples, ontology quads are ignored (these formats do
///   not support named graphs).
pub fn export_graph(
    graph: &SeleneGraph,
    ns: &RdfNamespace,
    format: RdfFormat,
    ontology: Option<&OntologyStore>,
    include_all_graphs: bool,
) -> Result<Vec<u8>, RdfError> {
    export_graph_scoped(graph, ns, format, ontology, include_all_graphs, None)
}

/// Serialize a [`SeleneGraph`] to RDF bytes, optionally filtering the output
/// to a principal's scope bitmap.
///
/// When `scope` is `None`, the output is identical to [`export_graph`]. When
/// `Some`, only in-scope nodes and their fully-in-scope edges are emitted.
/// Ontology quads — schema-level triples loaded at import time — are shared
/// and not scope-filtered: they describe types, not instance data, so they
/// do not leak per-tenant information. Scoped SPARQL query enforcement
/// follows the same rule (see `sparql::execute_sparql_scoped`).
pub fn export_graph_scoped(
    graph: &SeleneGraph,
    ns: &RdfNamespace,
    format: RdfFormat,
    ontology: Option<&OntologyStore>,
    include_all_graphs: bool,
    scope: Option<&roaring::RoaringBitmap>,
) -> Result<Vec<u8>, RdfError> {
    match format {
        RdfFormat::Turtle => export_turtle_scoped(graph, ns, scope),
        RdfFormat::NTriples => export_ntriples_scoped(graph, ns, scope),
        RdfFormat::NQuads => export_nquads_scoped(graph, ns, ontology, include_all_graphs, scope),
    }
}

/// Serialize a [`SeleneGraph`] to Turtle format (full graph).
pub fn export_turtle(graph: &SeleneGraph, ns: &RdfNamespace) -> Result<Vec<u8>, RdfError> {
    export_turtle_scoped(graph, ns, None)
}

/// Serialize a [`SeleneGraph`] to Turtle format, optionally scope-filtered.
///
/// Turtle does not support named graphs, so only default-graph quads are
/// written. Each quad is converted to a triple (subject, predicate, object)
/// before serialization. Namespace prefixes are declared for compact output.
pub fn export_turtle_scoped(
    graph: &SeleneGraph,
    ns: &RdfNamespace,
    scope: Option<&roaring::RoaringBitmap>,
) -> Result<Vec<u8>, RdfError> {
    let quads = graph_to_quads_scoped(graph, ns, scope);

    let mut buf = Vec::new();

    // Build serializer with prefix declarations for compact output.
    let serializer = TurtleSerializer::new()
        .with_prefix("bldg", ns.prefix())
        .and_then(|s| s.with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
        .and_then(|s| s.with_prefix("xsd", "http://www.w3.org/2001/XMLSchema#"));

    let mut writer = match serializer {
        Ok(s) => s.for_writer(&mut buf),
        // If prefix registration fails (malformed IRI), fall back to no prefixes.
        Err(_) => TurtleSerializer::new().for_writer(&mut buf),
    };

    for quad in &quads {
        let triple: TripleRef<'_> = quad.as_ref().into();
        writer
            .serialize_triple(triple)
            .map_err(|e| RdfError::Serialize(e.to_string()))?;
    }

    writer
        .finish()
        .map_err(|e| RdfError::Serialize(e.to_string()))?;

    Ok(buf)
}

/// Serialize a [`SeleneGraph`] to N-Triples format (full graph).
pub fn export_ntriples(graph: &SeleneGraph, ns: &RdfNamespace) -> Result<Vec<u8>, RdfError> {
    export_ntriples_scoped(graph, ns, None)
}

/// Serialize a [`SeleneGraph`] to N-Triples format, optionally scope-filtered.
///
/// N-Triples is a line-based triple format with no abbreviations or named
/// graphs. Each quad is written as a triple (subject, predicate, object).
pub fn export_ntriples_scoped(
    graph: &SeleneGraph,
    ns: &RdfNamespace,
    scope: Option<&roaring::RoaringBitmap>,
) -> Result<Vec<u8>, RdfError> {
    let quads = graph_to_quads_scoped(graph, ns, scope);

    let mut buf = Vec::new();
    let mut writer = NTriplesSerializer::new().for_writer(&mut buf);

    for quad in &quads {
        let triple: TripleRef<'_> = quad.as_ref().into();
        writer
            .serialize_triple(triple)
            .map_err(|e| RdfError::Serialize(e.to_string()))?;
    }

    writer.finish();

    Ok(buf)
}

/// Serialize a [`SeleneGraph`] to N-Quads format (full graph).
pub fn export_nquads(
    graph: &SeleneGraph,
    ns: &RdfNamespace,
    ontology: Option<&OntologyStore>,
    include_all_graphs: bool,
) -> Result<Vec<u8>, RdfError> {
    export_nquads_scoped(graph, ns, ontology, include_all_graphs, None)
}

/// Serialize a [`SeleneGraph`] to N-Quads format, optionally scope-filtered.
///
/// N-Quads extends N-Triples with an optional graph name per statement.
/// Default-graph quads from the property graph are scope-filtered when
/// `scope` is `Some`. Ontology quads (a named graph) are schema-level
/// metadata and are included unfiltered when `include_all_graphs` is set.
pub fn export_nquads_scoped(
    graph: &SeleneGraph,
    ns: &RdfNamespace,
    ontology: Option<&OntologyStore>,
    include_all_graphs: bool,
    scope: Option<&roaring::RoaringBitmap>,
) -> Result<Vec<u8>, RdfError> {
    let quads = graph_to_quads_scoped(graph, ns, scope);

    let mut buf = Vec::new();
    let mut writer = NQuadsSerializer::new().for_writer(&mut buf);

    // Write default-graph quads from the property graph.
    for quad in &quads {
        writer
            .serialize_quad(quad.as_ref())
            .map_err(|e| RdfError::Serialize(e.to_string()))?;
    }

    // Optionally write ontology quads (named graph).
    if include_all_graphs && let Some(store) = ontology {
        for quad in store.quads() {
            writer
                .serialize_quad(quad)
                .map_err(|e| RdfError::Serialize(e.to_string()))?;
        }
    }

    writer.finish();

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::interner::IStr;
    use selene_core::label_set::LabelSet;
    use selene_core::property_map::PropertyMap;
    use selene_core::value::Value;
    use selene_graph::SeleneGraph;

    fn test_ns() -> RdfNamespace {
        RdfNamespace::new("https://example.com/building/")
    }

    /// Build a small test graph: one Sensor node and one Room node linked by
    /// a locatedIn edge.
    fn build_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();

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

        m.create_edge(n1, IStr::new("locatedIn"), n2, PropertyMap::new())
            .unwrap();

        m.commit(0).unwrap();
        g
    }

    #[test]
    fn export_turtle_produces_valid_output() {
        let g = build_graph();
        let ns = test_ns();
        let bytes = export_turtle(&g, &ns).unwrap();
        let output = String::from_utf8(bytes).unwrap();

        // Turtle uses prefix abbreviation (bldg:node\/1) so check for the
        // abbreviated forms. The serializer escapes `/` in local names.
        assert!(output.contains("bldg:node\\/1"), "missing node/1: {output}");
        assert!(output.contains("bldg:node\\/2"), "missing node/2: {output}");
        assert!(
            output.contains("type\\/Sensor") || output.contains("type/Sensor"),
            "missing type/Sensor: {output}"
        );
        assert!(
            output.contains("type\\/Room") || output.contains("type/Room"),
            "missing type/Room: {output}"
        );
        assert!(
            output.contains("prop\\/unit") || output.contains("prop/unit"),
            "missing prop/unit: {output}"
        );
        assert!(
            output.contains("prop\\/name") || output.contains("prop/name"),
            "missing prop/name: {output}"
        );
        assert!(
            output.contains("rel\\/locatedIn") || output.contains("rel/locatedIn"),
            "missing rel/locatedIn: {output}"
        );
    }

    #[test]
    fn export_ntriples_produces_line_per_triple() {
        let g = build_graph();
        let ns = test_ns();
        let bytes = export_ntriples(&g, &ns).unwrap();
        let output = String::from_utf8(bytes).unwrap();

        // N-Triples: each non-empty line ends with " .\n"
        let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
        assert!(!lines.is_empty());
        for line in &lines {
            assert!(line.ends_with(" ."), "line does not end with ' .': {line}");
        }

        // Should contain full URIs (no prefixes in N-Triples).
        assert!(output.contains("<https://example.com/building/node/1>"));
        assert!(output.contains("<https://example.com/building/node/2>"));
    }

    #[test]
    fn export_nquads_produces_valid_output() {
        let g = build_graph();
        let ns = test_ns();
        let bytes = export_nquads(&g, &ns, None, false).unwrap();
        let output = String::from_utf8(bytes).unwrap();

        // N-Quads: each non-empty line ends with " .\n"
        let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
        assert!(!lines.is_empty());
        for line in &lines {
            assert!(line.ends_with(" ."), "line does not end with ' .': {line}");
        }

        assert!(output.contains("<https://example.com/building/node/1>"));
    }

    #[test]
    fn export_graph_dispatches_correctly() {
        let g = build_graph();
        let ns = test_ns();

        let turtle = export_graph(&g, &ns, RdfFormat::Turtle, None, false).unwrap();
        let nt = export_graph(&g, &ns, RdfFormat::NTriples, None, false).unwrap();
        let nq = export_graph(&g, &ns, RdfFormat::NQuads, None, false).unwrap();

        // All three should produce non-empty output for the same graph.
        assert!(!turtle.is_empty());
        assert!(!nt.is_empty());
        assert!(!nq.is_empty());

        // N-Triples and N-Quads should be byte-identical for default-graph
        // quads (N-Quads omits graph name for default graph).
        assert_eq!(
            nt, nq,
            "N-Triples and N-Quads should match for default-graph-only data"
        );
    }

    #[test]
    fn export_empty_graph_returns_empty_or_minimal() {
        let g = SeleneGraph::new();
        let ns = test_ns();

        let turtle = export_turtle(&g, &ns).unwrap();
        let nt = export_ntriples(&g, &ns).unwrap();
        let nq = export_nquads(&g, &ns, None, false).unwrap();

        // N-Triples and N-Quads should be completely empty for an empty graph.
        assert!(nt.is_empty());
        assert!(nq.is_empty());

        // Turtle may include prefix declarations even with no triples; that is
        // acceptable. It should not contain any triple statements.
        let turtle_str = String::from_utf8(turtle).unwrap();
        assert!(
            !turtle_str.contains("node/"),
            "empty graph should produce no node URIs"
        );
    }
}
