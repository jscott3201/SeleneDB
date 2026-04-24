//! PG to RDF mapping.
//!
//! Converts Selene property graph entities (nodes and edges) to RDF quads.
//! Each node produces `rdf:type` quads for its labels and property quads for
//! its properties. Each edge produces a base relationship triple, edge identity
//! quads linking the edge URI to source/target/label, and property quads for
//! edge properties. `Value::List` items are expanded into separate quads;
//! `Value::Null` properties are skipped.

use oxrdf::{GraphName, NamedNode, Quad};
use selene_core::value::Value;
use selene_core::{EdgeId, NodeId};
use selene_graph::SeleneGraph;

use crate::namespace::{RdfNamespace, SELENE_LABEL, SELENE_SOURCE, SELENE_TARGET};
use crate::terms::value_to_literal;

// ---------------------------------------------------------------------------
// Selene-specific predicates for edge reification
// ---------------------------------------------------------------------------

/// `selene:source` -- links an edge URI to its source node URI.
fn selene_source() -> NamedNode {
    NamedNode::new_unchecked(SELENE_SOURCE)
}

/// `selene:target` -- links an edge URI to its target node URI.
fn selene_target() -> NamedNode {
    NamedNode::new_unchecked(SELENE_TARGET)
}

/// `selene:label` -- links an edge URI to the relationship type URI.
fn selene_label() -> NamedNode {
    NamedNode::new_unchecked(SELENE_LABEL)
}

// ---------------------------------------------------------------------------
// Node mapping
// ---------------------------------------------------------------------------

/// Convert a single node to RDF quads.
///
/// Generates:
/// - One `rdf:type` quad per label
/// - One property quad per property (lists expand to multiple quads)
/// - `Value::Null` properties are skipped
///
/// Returns an empty `Vec` if the node does not exist.
pub fn node_to_quads(graph: &SeleneGraph, node_id: NodeId, ns: &RdfNamespace) -> Vec<Quad> {
    let Some(node) = graph.get_node(node_id) else {
        return Vec::new();
    };

    let subject = ns.node_uri(node_id);
    let rdf_type = RdfNamespace::rdf_type();
    let mut quads = Vec::new();

    // rdf:type quad per label
    for label in node.labels.iter() {
        let type_uri = ns.type_uri(label.as_str());
        quads.push(Quad::new(
            subject.clone(),
            rdf_type.clone(),
            type_uri,
            GraphName::DefaultGraph,
        ));
    }

    // Property quads
    for (key, value) in node.properties.iter() {
        emit_property_quads(&mut quads, &subject, &ns.prop_uri(key.as_str()), value);
    }

    quads
}

// ---------------------------------------------------------------------------
// Edge mapping
// ---------------------------------------------------------------------------

/// Convert a single edge to RDF quads.
///
/// Generates:
/// - Base relationship triple: `(source_uri, rel/label, target_uri)`
/// - Edge identity quads (only when the edge has properties):
///   `(edge_uri, selene:source, source_uri)`,
///   `(edge_uri, selene:target, target_uri)`, `(edge_uri, selene:label, rel_uri)`
/// - One property quad per edge property (lists expand to multiple quads)
///
/// Edges without properties are fully represented by the base triple alone.
/// Emitting reifier triples unconditionally causes duplicate edges on
/// round-trip import because the importer creates one edge from the
/// `rel/` triple and another from the reifier.
///
/// Returns an empty `Vec` if the edge does not exist.
pub fn edge_to_quads(graph: &SeleneGraph, edge_id: EdgeId, ns: &RdfNamespace) -> Vec<Quad> {
    let Some(edge) = graph.get_edge(edge_id) else {
        return Vec::new();
    };
    let mut quads = Vec::new();
    emit_edge_quads(&mut quads, edge, ns);
    quads
}

/// Emit a single edge's quads into an existing buffer using an already-
/// fetched [`selene_graph::EdgeRef`]. Factored out so callers that
/// pre-fetch the edge (scope filtering, bulk iteration) avoid the double
/// `get_edge` lookup the old `edge_to_quads` path incurred.
fn emit_edge_quads(quads: &mut Vec<Quad>, edge: selene_graph::EdgeRef<'_>, ns: &RdfNamespace) {
    let source_uri = ns.node_uri(edge.source);
    let target_uri = ns.node_uri(edge.target);
    let label_str = edge.label.as_str();
    let rel_uri = ns.rel_uri(label_str);
    let edge_uri = ns.edge_uri(edge.id);

    // Base relationship triple (always emitted).
    quads.push(Quad::new(
        source_uri.clone(),
        rel_uri.clone(),
        target_uri.clone(),
        GraphName::DefaultGraph,
    ));

    // Edge identity + property quads only when the edge carries properties.
    if !edge.properties.is_empty() {
        quads.push(Quad::new(
            edge_uri.clone(),
            selene_source(),
            source_uri,
            GraphName::DefaultGraph,
        ));
        quads.push(Quad::new(
            edge_uri.clone(),
            selene_target(),
            target_uri,
            GraphName::DefaultGraph,
        ));
        quads.push(Quad::new(
            edge_uri.clone(),
            selene_label(),
            rel_uri,
            GraphName::DefaultGraph,
        ));

        for (key, value) in edge.properties.iter() {
            emit_property_quads(quads, &edge_uri, &ns.prop_uri(key.as_str()), value);
        }
    }
}

// ---------------------------------------------------------------------------
// Full graph mapping
// ---------------------------------------------------------------------------

/// Convert every node and edge in the graph to RDF quads.
///
/// The returned `Vec` is pre-allocated with a capacity estimate based on
/// the graph size: roughly 4 quads per node (labels + properties) and
/// 2 quads per edge (most edges lack properties and emit only the base
/// triple; the estimate is conservative).
pub fn graph_to_quads(graph: &SeleneGraph, ns: &RdfNamespace) -> Vec<Quad> {
    graph_to_quads_scoped(graph, ns, None)
}

/// Convert nodes and edges in the graph to RDF quads, optionally filtered
/// by an authorization scope bitmap.
///
/// When `scope` is `None`, the output is identical to [`graph_to_quads`]
/// (admin/global view). When `Some`, only nodes whose `NodeId.0` is set in
/// the bitmap contribute quads; an edge contributes its base triple + any
/// reifier/property quads only if **both** of its endpoints are in scope.
/// This matches the semantics of `CRUD` scope enforcement elsewhere in the
/// server: a principal cannot observe a relationship outside its subtree
/// by virtue of one endpoint happening to be inside.
pub fn graph_to_quads_scoped(
    graph: &SeleneGraph,
    ns: &RdfNamespace,
    scope: Option<&roaring::RoaringBitmap>,
) -> Vec<Quad> {
    let estimated = graph.node_count() * 4 + graph.edge_count() * 2;
    let mut quads = Vec::with_capacity(estimated);

    let in_scope = |node_id: NodeId| -> bool { scope.is_none_or(|s| s.contains(node_id.0 as u32)) };

    for node_id in graph.all_node_ids() {
        if !in_scope(node_id) {
            continue;
        }
        quads.extend(node_to_quads(graph, node_id, ns));
    }

    // Fetch each edge once via `get_edge`, check scope, then emit quads
    // from the borrowed ref — avoids the pre-fix pattern where we called
    // `get_edge` for the scope check and then `edge_to_quads` looked the
    // same edge up again.
    for edge_id in graph.all_edge_ids() {
        let Some(edge) = graph.get_edge(edge_id) else {
            continue;
        };
        if in_scope(edge.source) && in_scope(edge.target) {
            emit_edge_quads(&mut quads, edge, ns);
        }
    }

    quads
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Emit property quads for a single (key, value) pair. `Value::List` items are
/// expanded into one quad per element. `Value::Null` is skipped.
fn emit_property_quads(
    quads: &mut Vec<Quad>,
    subject: &NamedNode,
    predicate: &NamedNode,
    value: &Value,
) {
    match value {
        Value::Null => {}
        Value::List(items) => {
            for item in items.iter() {
                if let Some(lit) = value_to_literal(item) {
                    quads.push(Quad::new(
                        subject.clone(),
                        predicate.clone(),
                        lit,
                        GraphName::DefaultGraph,
                    ));
                }
            }
        }
        _ => {
            if let Some(lit) = value_to_literal(value) {
                quads.push(Quad::new(
                    subject.clone(),
                    predicate.clone(),
                    lit,
                    GraphName::DefaultGraph,
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use selene_core::interner::IStr;
    use selene_core::label_set::LabelSet;
    use selene_core::property_map::PropertyMap;
    use selene_graph::SeleneGraph;

    fn test_ns() -> RdfNamespace {
        RdfNamespace::new("https://example.com/building/")
    }

    /// Build a small test graph via the public TrackedMutation API.
    /// Node 1: labels {Device, Sensor}, props {floor: 3, unit: "°F"}
    /// Node 2: labels {Room}, props {name: "Conference A"}
    /// Edge 1: Node 1 -[locatedIn]-> Node 2, props {since: 2024}
    fn build_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();

        let n1 = m
            .create_node(
                LabelSet::from_strs(&["Sensor", "Device"]),
                PropertyMap::from_pairs([
                    (IStr::new("unit"), Value::String("°F".into())),
                    (IStr::new("floor"), Value::Int(3)),
                ]),
            )
            .unwrap();

        let n2 = m
            .create_node(
                LabelSet::from_strs(&["Room"]),
                PropertyMap::from_pairs([(
                    IStr::new("name"),
                    Value::String("Conference A".into()),
                )]),
            )
            .unwrap();

        m.create_edge(
            n1,
            IStr::new("locatedIn"),
            n2,
            PropertyMap::from_pairs([(IStr::new("since"), Value::Int(2024))]),
        )
        .unwrap();

        m.commit(0).unwrap();
        g
    }

    #[test]
    fn node_to_quads_labels_and_props() {
        let g = build_graph();
        let ns = test_ns();
        let quads = node_to_quads(&g, NodeId(1), &ns);

        // 2 labels + 2 properties = 4 quads
        assert_eq!(quads.len(), 4);

        // Check rdf:type quads
        let type_quads: Vec<_> = quads
            .iter()
            .filter(|q| q.predicate.as_str().contains("rdf-syntax-ns#type"))
            .collect();
        assert_eq!(type_quads.len(), 2);

        // Check property quads
        let prop_quads: Vec<_> = quads
            .iter()
            .filter(|q| q.predicate.as_str().contains("/prop/"))
            .collect();
        assert_eq!(prop_quads.len(), 2);
    }

    #[test]
    fn node_to_quads_nonexistent_returns_empty() {
        let g = SeleneGraph::new();
        let ns = test_ns();
        let quads = node_to_quads(&g, NodeId(999), &ns);
        assert!(quads.is_empty());
    }

    #[test]
    fn node_to_quads_skips_null() {
        let mut g = SeleneGraph::new();
        {
            let mut m = g.mutate();
            m.create_node(
                LabelSet::from_strs(&["Thing"]),
                PropertyMap::from_pairs([(IStr::new("empty"), Value::Null)]),
            )
            .unwrap();
            m.commit(0).unwrap();
        }

        let ns = test_ns();
        let quads = node_to_quads(&g, NodeId(1), &ns);

        // 1 rdf:type, 0 properties (null skipped)
        assert_eq!(quads.len(), 1);
    }

    #[test]
    fn node_to_quads_expands_list() {
        let mut g = SeleneGraph::new();
        {
            let mut m = g.mutate();
            m.create_node(
                LabelSet::new(),
                PropertyMap::from_pairs([(
                    IStr::new("tags"),
                    Value::List(Arc::from(vec![
                        Value::String("a".into()),
                        Value::String("b".into()),
                        Value::String("c".into()),
                    ])),
                )]),
            )
            .unwrap();
            m.commit(0).unwrap();
        }

        let ns = test_ns();
        let quads = node_to_quads(&g, NodeId(1), &ns);

        // 0 labels + 3 list items = 3 quads
        assert_eq!(quads.len(), 3);
        for q in &quads {
            assert!(q.predicate.as_str().ends_with("/prop/tags"));
        }
    }

    #[test]
    fn edge_to_quads_base_and_identity() {
        let g = build_graph();
        let ns = test_ns();
        let quads = edge_to_quads(&g, EdgeId(1), &ns);

        // 1 base triple + 3 identity + 1 property = 5
        assert_eq!(quads.len(), 5);

        // Base triple: source rel/locatedIn target
        let base = &quads[0];
        assert!(base.subject.to_string().contains("node/1"));
        assert!(base.predicate.as_str().contains("rel/locatedIn"));
        assert!(base.object.to_string().contains("node/2"));

        // Identity quads
        let source_q = &quads[1];
        assert_eq!(source_q.predicate.as_str(), SELENE_SOURCE);

        let target_q = &quads[2];
        assert_eq!(target_q.predicate.as_str(), SELENE_TARGET);

        let label_q = &quads[3];
        assert_eq!(label_q.predicate.as_str(), SELENE_LABEL);

        // Edge property
        let prop_q = &quads[4];
        assert!(prop_q.subject.to_string().contains("edge/1"));
        assert!(prop_q.predicate.as_str().contains("prop/since"));
    }

    #[test]
    fn edge_to_quads_nonexistent_returns_empty() {
        let g = SeleneGraph::new();
        let ns = test_ns();
        let quads = edge_to_quads(&g, EdgeId(999), &ns);
        assert!(quads.is_empty());
    }

    #[test]
    fn graph_to_quads_all_entities() {
        let g = build_graph();
        let ns = test_ns();
        let quads = graph_to_quads(&g, &ns);

        // Node 1: 2 labels + 2 props = 4
        // Node 2: 1 label  + 1 prop  = 2
        // Edge 1: 1 base + 3 identity + 1 prop = 5
        // Total = 11
        assert_eq!(quads.len(), 11);
    }

    #[test]
    fn graph_to_quads_empty_graph() {
        let g = SeleneGraph::new();
        let ns = test_ns();
        let quads = graph_to_quads(&g, &ns);
        assert!(quads.is_empty());
    }

    #[test]
    fn edge_without_properties_emits_one_quad() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        let n1 = m
            .create_node(LabelSet::from_strs(&["A"]), PropertyMap::new())
            .unwrap();
        let n2 = m
            .create_node(LabelSet::from_strs(&["B"]), PropertyMap::new())
            .unwrap();
        let eid = m
            .create_edge(n1, IStr::new("connects"), n2, PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();

        let ns = test_ns();
        let quads = edge_to_quads(&g, eid, &ns);
        // Only the base rel/ triple, no reifier triples.
        assert_eq!(quads.len(), 1);
        assert!(quads[0].predicate.as_str().ends_with("rel/connects"));
    }

    #[test]
    fn edge_with_properties_emits_reifier_quads() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        let n1 = m
            .create_node(LabelSet::from_strs(&["A"]), PropertyMap::new())
            .unwrap();
        let n2 = m
            .create_node(LabelSet::from_strs(&["B"]), PropertyMap::new())
            .unwrap();
        let eid = m
            .create_edge(
                n1,
                IStr::new("connects"),
                n2,
                PropertyMap::from_pairs([(IStr::new("weight"), Value::Int(5))]),
            )
            .unwrap();
        m.commit(0).unwrap();

        let ns = test_ns();
        let quads = edge_to_quads(&g, eid, &ns);
        // 1 base + 3 reifier + 1 property = 5
        assert_eq!(quads.len(), 5);
    }
}
