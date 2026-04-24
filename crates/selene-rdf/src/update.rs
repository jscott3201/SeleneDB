//! SPARQL Update execution (INSERT DATA, DELETE DATA).
//!
//! Translates SPARQL Update operations into property graph mutations by
//! reverse-mapping RDF quads through the namespace URI scheme. Each quad
//! is classified by its predicate:
//!
//! - `rdf:type <ns>type/<Label>` -> add/remove label
//! - `<ns>prop/<key> "value"` -> set/remove property
//! - `<ns>rel/<label> <ns>node/<target>` -> create/delete edge
//!
//! Only INSERT DATA and DELETE DATA are supported in this phase. Pattern-
//! matched DELETE/INSERT WHERE will use spareval's PreparedDeleteInsertUpdate.

use oxrdf::{NamedOrBlankNode, Quad, Term};
use selene_core::EdgeId;
use selene_core::interner::IStr;
use selene_core::label_set::LabelSet;
use selene_core::property_map::PropertyMap;
use selene_graph::SeleneGraph;
use selene_graph::csr::CsrAdjacency;
use spareval::{DeleteInsertQuad, QueryEvaluator};
use spargebra::{GraphUpdateOperation, SparqlParser};

use crate::namespace::{ParsedUri, RDF_TYPE, RdfNamespace};
use crate::terms::literal_to_value;

/// Errors from SPARQL Update execution.
#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("SPARQL parse error: {0}")]
    Parse(String),
    #[error("unsupported update operation: {0}")]
    Unsupported(String),
    #[error("invalid quad: {0}")]
    InvalidQuad(String),
    #[error("graph mutation error: {0}")]
    Graph(#[from] selene_graph::GraphError),
    #[error("node not found: {0}")]
    NodeNotFound(u64),
}

/// Result of a SPARQL Update execution.
#[derive(Debug, Default)]
pub struct UpdateResult {
    pub nodes_created: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub edges_created: usize,
    pub edges_deleted: usize,
}

/// Execute a SPARQL Update against a `SharedGraph` and publish the new
/// snapshot so subsequent reads observe the mutation.
///
/// # Durability caveat
///
/// The returned `Vec<Change>` is currently **always empty**. The in-memory
/// graph is updated and `publish_snapshot` makes the new state visible to
/// all readers, but WAL / changelog / version-store consumers do **not**
/// see these writes — and therefore SPARQL Update mutations do not
/// replicate, do not survive a process restart, and do not appear in
/// temporal queries. Callers that rely on `persist_or_die` for durability
/// (as `ops::rdf::sparql_update` does) are passing an empty changeset; the
/// call is a no-op for WAL consumers until this is addressed.
///
/// The fix requires re-plumbing `apply_insert_data` / `apply_delete_data`
/// (and the compound `DELETE-INSERT-WHERE` path) to emit `Change` records
/// through `TrackedMutation`. This is tracked as deferred work and is the
/// last step to bring SPARQL Update to parity with GQL mutations.
///
/// This entry point is still strictly better than the pre-1.3.0 handler,
/// which bypassed authz, replica checks, and the snapshot publish path —
/// so it is safe to ship with the durability caveat documented, and
/// `ops::rdf::sparql_update` gates it to admin-only for that reason.
pub fn execute_update_shared(
    shared: &selene_graph::SharedGraph,
    namespace: &RdfNamespace,
    update_str: &str,
) -> Result<(UpdateResult, Vec<selene_core::changeset::Change>), UpdateError> {
    let result = {
        let mut inner = shared.inner().write();
        execute_update(&mut inner, namespace, update_str)?
    };
    shared.publish_snapshot();
    Ok((result, Vec::new()))
}

/// Execute a SPARQL Update against a mutable Selene graph.
///
/// Supports INSERT DATA and DELETE DATA. Returns counts of mutations applied.
/// This is the low-level entry used by [`execute_update_shared`]; server code
/// should prefer the shared variant so WAL / changelog consumers see the
/// write.
pub fn execute_update(
    graph: &mut SeleneGraph,
    namespace: &RdfNamespace,
    update_str: &str,
) -> Result<UpdateResult, UpdateError> {
    let update = SparqlParser::new()
        .parse_update(update_str)
        .map_err(|e| UpdateError::Parse(e.to_string()))?;

    let mut result = UpdateResult::default();

    for op in &update.operations {
        match op {
            GraphUpdateOperation::InsertData { data } => {
                let quads: Vec<Quad> = data.iter().map(spargebra_quad_to_oxrdf).collect();
                apply_insert_data(graph, namespace, &quads, &mut result)?;
            }
            GraphUpdateOperation::DeleteData { data } => {
                let quads: Vec<Quad> = data.iter().map(spargebra_ground_quad_to_oxrdf).collect();
                apply_delete_data(graph, namespace, &quads, &mut result)?;
            }
            GraphUpdateOperation::DeleteInsert {
                delete,
                insert,
                using,
                pattern,
            } => {
                apply_delete_insert(
                    graph,
                    namespace,
                    delete,
                    insert,
                    using.clone(),
                    pattern,
                    &mut result,
                )?;
            }
            GraphUpdateOperation::Clear {
                silent,
                graph: target,
            } => {
                apply_clear(graph, target, *silent, &mut result)?;
            }
            GraphUpdateOperation::Drop {
                silent,
                graph: target,
            } => {
                // DROP has same semantics as CLEAR for a single-graph system.
                apply_clear(graph, target, *silent, &mut result)?;
            }
            other => {
                return Err(UpdateError::Unsupported(format!(
                    "{} is not yet supported",
                    update_op_name(other)
                )));
            }
        }
    }

    Ok(result)
}

/// Apply INSERT DATA quads as property graph mutations.
fn apply_insert_data(
    graph: &mut SeleneGraph,
    ns: &RdfNamespace,
    quads: &[Quad],
    result: &mut UpdateResult,
) -> Result<(), UpdateError> {
    let mut m = graph.mutate();

    for quad in quads {
        let subject_uri = subject_as_str(quad)?;
        let predicate_uri = quad.predicate.as_str();
        let subject_parsed = ns.parse(subject_uri);

        // Determine the node ID (create if needed for new subjects).
        let node_id = match &subject_parsed {
            Some(ParsedUri::Node(id)) => *id,
            _ => {
                // Unknown subject URI: skip (we only mutate known nodes).
                continue;
            }
        };

        if predicate_uri == RDF_TYPE {
            // rdf:type -> add label
            let label = match &quad.object {
                Term::NamedNode(nn) => {
                    if let Some(ParsedUri::Type(label)) = ns.parse(nn.as_str()) {
                        label
                    } else {
                        IStr::new(RdfNamespace::extract_local_name(nn.as_str()))
                    }
                }
                _ => continue,
            };
            // Ensure node exists
            if m.graph().get_node(node_id).is_none() {
                m.create_node(
                    selene_core::label_set::LabelSet::from_strs(&[label.as_str()]),
                    PropertyMap::new(),
                )?;
                result.nodes_created += 1;
                result.labels_added += 1;
                continue;
            }
            m.add_label(node_id, label)?;
            result.labels_added += 1;
        } else if let Some(ParsedUri::Property(key)) = ns.parse(predicate_uri) {
            // prop/<key> -> set property
            let value = match &quad.object {
                Term::Literal(lit) => literal_to_value(lit),
                _ => continue,
            };
            if m.graph().get_node(node_id).is_none() {
                m.create_node(LabelSet::new(), PropertyMap::from_pairs([(key, value)]))?;
                result.nodes_created += 1;
                result.properties_set += 1;
                continue;
            }
            m.set_property(node_id, key, value)?;
            result.properties_set += 1;
        } else if let Some(ParsedUri::Relationship(label)) = ns.parse(predicate_uri) {
            // rel/<label> -> create edge
            let target_id = match &quad.object {
                Term::NamedNode(nn) => {
                    if let Some(ParsedUri::Node(id)) = ns.parse(nn.as_str()) {
                        id
                    } else {
                        continue;
                    }
                }
                _ => continue,
            };
            m.create_edge(node_id, label, target_id, PropertyMap::new())?;
            result.edges_created += 1;
        }
        // Other predicates (external URIs) are silently skipped.
    }

    m.commit(0)?;
    Ok(())
}

/// Apply DELETE DATA quads as property graph mutations.
fn apply_delete_data(
    graph: &mut SeleneGraph,
    ns: &RdfNamespace,
    quads: &[Quad],
    result: &mut UpdateResult,
) -> Result<(), UpdateError> {
    let mut m = graph.mutate();

    for quad in quads {
        let subject_uri = subject_as_str(quad)?;
        let predicate_uri = quad.predicate.as_str();
        let subject_parsed = ns.parse(subject_uri);

        let node_id = match &subject_parsed {
            Some(ParsedUri::Node(id)) => *id,
            _ => continue,
        };

        // Skip if node doesn't exist (DELETE DATA is idempotent per spec).
        if m.graph().get_node(node_id).is_none() {
            continue;
        }

        if predicate_uri == RDF_TYPE {
            // rdf:type -> remove label
            let label = match &quad.object {
                Term::NamedNode(nn) => {
                    if let Some(ParsedUri::Type(label)) = ns.parse(nn.as_str()) {
                        label
                    } else {
                        IStr::new(RdfNamespace::extract_local_name(nn.as_str()))
                    }
                }
                _ => continue,
            };
            m.remove_label(node_id, label.as_str())?;
            result.labels_removed += 1;
        } else if let Some(ParsedUri::Property(key)) = ns.parse(predicate_uri) {
            // prop/<key> -> remove property
            m.remove_property(node_id, key.as_str())?;
            result.properties_removed += 1;
        } else if let Some(ParsedUri::Relationship(label)) = ns.parse(predicate_uri) {
            // rel/<label> -> delete edge(s) matching source + label + target
            let target_id = match &quad.object {
                Term::NamedNode(nn) => {
                    if let Some(ParsedUri::Node(id)) = ns.parse(nn.as_str()) {
                        id
                    } else {
                        continue;
                    }
                }
                _ => continue,
            };
            // Find and delete matching edges.
            let edge_ids: Vec<EdgeId> = m
                .graph()
                .outgoing(node_id)
                .iter()
                .copied()
                .filter(|&eid| {
                    m.graph()
                        .get_edge(eid)
                        .is_some_and(|e| e.label == label && e.target == target_id)
                })
                .collect();
            for eid in edge_ids {
                m.delete_edge(eid)?;
                result.edges_deleted += 1;
            }
        }
    }

    m.commit(0)?;
    Ok(())
}

/// Apply CLEAR or DROP on a graph target.
///
/// For a property graph with a single default graph:
/// - `DefaultGraph` / `AllGraphs`: delete all nodes and edges
/// - `NamedGraphs`: no-op (no user-defined named graphs)
/// - `NamedNode(...)`: error unless silent
fn apply_clear(
    graph: &mut SeleneGraph,
    target: &spargebra::algebra::GraphTarget,
    silent: bool,
    result: &mut UpdateResult,
) -> Result<(), UpdateError> {
    use spargebra::algebra::GraphTarget;
    match target {
        GraphTarget::DefaultGraph | GraphTarget::AllGraphs => {
            let mut m = graph.mutate();
            // Delete all edges first (edges reference nodes).
            let edge_ids: Vec<EdgeId> = m.graph().all_edge_ids().collect();
            for eid in &edge_ids {
                let _ = m.delete_edge(*eid);
            }
            result.edges_deleted += edge_ids.len();
            // Delete all nodes.
            let node_ids: Vec<selene_core::NodeId> = m.graph().all_node_ids().collect();
            for nid in &node_ids {
                let _ = m.delete_node(*nid);
            }
            result.nodes_created = 0; // reset; we track deletions via edges/props
            m.commit(0)?;
            Ok(())
        }
        GraphTarget::NamedGraphs => {
            // No user-defined named graphs in Selene. No-op.
            Ok(())
        }
        GraphTarget::NamedNode(nn) => {
            if silent {
                Ok(())
            } else {
                Err(UpdateError::Unsupported(format!(
                    "CLEAR/DROP on named graph <{}> (Selene only has a default graph)",
                    nn.as_str()
                )))
            }
        }
    }
}

/// Apply a DELETE/INSERT WHERE operation.
///
/// Evaluates the WHERE pattern against a read-only snapshot of the graph
/// (via spareval), collects the resulting delete/insert quads, then applies
/// them as mutations. Deletes are applied before inserts to avoid conflicts.
fn apply_delete_insert(
    graph: &mut SeleneGraph,
    ns: &RdfNamespace,
    delete: &[spargebra::term::GroundQuadPattern],
    insert: &[spargebra::term::QuadPattern],
    using: Option<spargebra::algebra::QueryDataset>,
    pattern: &spargebra::algebra::GraphPattern,
    result: &mut UpdateResult,
) -> Result<(), UpdateError> {
    use crate::adapter::SeleneDataset;

    // Phase 1: evaluate WHERE against an immutable snapshot.
    let csr = CsrAdjacency::build(graph);
    let dataset = SeleneDataset::new(graph, &csr, ns, None);

    let evaluator = QueryEvaluator::new();
    let prepared = evaluator.prepare_delete_insert(
        delete.to_vec(),
        insert.to_vec(),
        None, // base_iri
        using,
        pattern,
    );
    let iter = prepared
        .execute(dataset)
        .map_err(|e| UpdateError::Parse(e.to_string()))?;

    // Collect all quads (releases the immutable borrow on graph).
    let mut deletes = Vec::new();
    let mut inserts = Vec::new();
    for item in iter {
        match item.map_err(|e| UpdateError::Parse(e.to_string()))? {
            DeleteInsertQuad::Delete(q) => deletes.push(q),
            DeleteInsertQuad::Insert(q) => inserts.push(q),
        }
    }

    // Phase 2: apply mutations (deletes first, then inserts).
    if !deletes.is_empty() {
        apply_delete_data(graph, ns, &deletes, result)?;
    }
    if !inserts.is_empty() {
        apply_insert_data(graph, ns, &inserts, result)?;
    }

    Ok(())
}

/// Extract subject URI string from a quad.
fn subject_as_str(quad: &Quad) -> Result<&str, UpdateError> {
    match &quad.subject {
        NamedOrBlankNode::NamedNode(nn) => Ok(nn.as_str()),
        NamedOrBlankNode::BlankNode(_) => Err(UpdateError::InvalidQuad(
            "blank node subjects not yet supported in SPARQL Update".into(),
        )),
    }
}

/// Convert a spargebra `Quad` (subject is NamedOrBlankNode) to an oxrdf `Quad`.
fn spargebra_quad_to_oxrdf(q: &spargebra::term::Quad) -> Quad {
    let subject: NamedOrBlankNode = q.subject.clone();
    let graph_name = spargebra_graph_name(&q.graph_name);
    Quad::new(subject, q.predicate.clone(), q.object.clone(), graph_name)
}

/// Convert a spargebra `GroundQuad` (subject is always NamedNode) to an oxrdf `Quad`.
fn spargebra_ground_quad_to_oxrdf(q: &spargebra::term::GroundQuad) -> Quad {
    let subject: NamedOrBlankNode = q.subject.clone().into();
    let object: Term = q.object.clone().into();
    let graph_name = spargebra_graph_name(&q.graph_name);
    Quad::new(subject, q.predicate.clone(), object, graph_name)
}

fn spargebra_graph_name(gn: &spargebra::term::GraphName) -> oxrdf::GraphName {
    match gn {
        spargebra::term::GraphName::NamedNode(nn) => oxrdf::GraphName::NamedNode(nn.clone()),
        spargebra::term::GraphName::DefaultGraph => oxrdf::GraphName::DefaultGraph,
    }
}

/// Human-readable name for an update operation (for error messages).
fn update_op_name(op: &GraphUpdateOperation) -> &'static str {
    match op {
        GraphUpdateOperation::InsertData { .. } => "INSERT DATA",
        GraphUpdateOperation::DeleteData { .. } => "DELETE DATA",
        GraphUpdateOperation::DeleteInsert { .. } => "DELETE/INSERT WHERE",
        GraphUpdateOperation::Load { .. } => "LOAD",
        GraphUpdateOperation::Clear { .. } => "CLEAR",
        GraphUpdateOperation::Create { .. } => "CREATE",
        GraphUpdateOperation::Drop { .. } => "DROP",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::label_set::LabelSet;
    use selene_core::{NodeId, Value};
    use selene_graph::SeleneGraph;

    const NS: &str = "https://example.com/building/";

    fn ns() -> RdfNamespace {
        RdfNamespace::new(NS)
    }

    fn graph_with_sensor() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["Sensor"]),
            PropertyMap::from_pairs([(IStr::new("unit"), Value::str("degC"))]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["Room"]),
            PropertyMap::from_pairs([(IStr::new("name"), Value::str("Lab"))]),
        )
        .unwrap();
        m.create_edge(
            NodeId(1),
            IStr::new("locatedIn"),
            NodeId(2),
            PropertyMap::new(),
        )
        .unwrap();
        m.commit(0).unwrap();
        g
    }

    #[test]
    fn insert_data_set_property() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!(
            "INSERT DATA {{ <{NS}node/1> <{NS}prop/temp> \"72.5\"^^<http://www.w3.org/2001/XMLSchema#double> }}"
        );
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.properties_set, 1);
        let node = g.get_node(NodeId(1)).unwrap();
        assert_eq!(node.property("temp"), Some(&Value::Float(72.5)));
    }

    #[test]
    fn insert_data_add_label() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!("INSERT DATA {{ <{NS}node/1> a <{NS}type/TemperatureSensor> }}");
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.labels_added, 1);
        let node = g.get_node(NodeId(1)).unwrap();
        assert!(node.labels.contains(IStr::new("TemperatureSensor")));
    }

    #[test]
    fn insert_data_create_edge() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!("INSERT DATA {{ <{NS}node/1> <{NS}rel/feeds> <{NS}node/2> }}");
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.edges_created, 1);
        let edges: Vec<_> = g
            .outgoing(NodeId(1))
            .iter()
            .filter_map(|&eid| g.get_edge(eid))
            .collect();
        assert!(edges.iter().any(|e| e.label == IStr::new("feeds")));
    }

    #[test]
    fn delete_data_remove_property() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!("DELETE DATA {{ <{NS}node/1> <{NS}prop/unit> \"degC\" }}");
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.properties_removed, 1);
        let node = g.get_node(NodeId(1)).unwrap();
        assert!(node.property("unit").is_none());
    }

    #[test]
    fn delete_data_remove_label() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!("DELETE DATA {{ <{NS}node/1> a <{NS}type/Sensor> }}");
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.labels_removed, 1);
        let node = g.get_node(NodeId(1)).unwrap();
        assert!(!node.labels.contains(IStr::new("Sensor")));
    }

    #[test]
    fn delete_data_remove_edge() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!("DELETE DATA {{ <{NS}node/1> <{NS}rel/locatedIn> <{NS}node/2> }}");
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.edges_deleted, 1);
        let edges: Vec<_> = g
            .outgoing(NodeId(1))
            .iter()
            .filter_map(|&eid| g.get_edge(eid))
            .collect();
        assert!(!edges.iter().any(|e| e.label == IStr::new("locatedIn")));
    }

    #[test]
    fn delete_data_nonexistent_node_is_idempotent() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!("DELETE DATA {{ <{NS}node/999> <{NS}prop/x> \"y\" }}");
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.properties_removed, 0);
    }

    #[test]
    fn unsupported_operation_returns_error() {
        let mut g = graph_with_sensor();
        let ns = ns();
        // LOAD is not supported
        let sparql = "LOAD <http://example.com/data.ttl>";
        let result = execute_update(&mut g, &ns, sparql);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("LOAD"), "error: {err}");
    }

    #[test]
    fn mixed_insert_and_delete() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!(
            "INSERT DATA {{ <{NS}node/1> <{NS}prop/temp> \"72.5\"^^<http://www.w3.org/2001/XMLSchema#double> }} ;\
             DELETE DATA {{ <{NS}node/1> <{NS}prop/unit> \"degC\" }}"
        );
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.properties_set, 1);
        assert_eq!(result.properties_removed, 1);
    }

    // ── Phase 2: DELETE/INSERT WHERE ──

    #[test]
    fn delete_where_removes_matching() {
        let mut g = graph_with_sensor();
        let ns = ns();
        // Delete the unit property from all Sensor nodes
        let sparql = format!(
            "DELETE {{ ?s <{NS}prop/unit> ?v }} WHERE {{ ?s a <{NS}type/Sensor> . ?s <{NS}prop/unit> ?v }}"
        );
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.properties_removed, 1);
        let node = g.get_node(NodeId(1)).unwrap();
        assert!(node.property("unit").is_none());
    }

    #[test]
    fn insert_where_sets_matching() {
        let mut g = graph_with_sensor();
        let ns = ns();
        // Add an alert property to all Sensor nodes
        let sparql = format!(
            "INSERT {{ ?s <{NS}prop/alert> \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> }} \
             WHERE {{ ?s a <{NS}type/Sensor> }}"
        );
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.properties_set, 1);
        let node = g.get_node(NodeId(1)).unwrap();
        assert_eq!(node.property("alert"), Some(&Value::Bool(true)));
    }

    #[test]
    fn delete_insert_where_replaces_property() {
        let mut g = graph_with_sensor();
        let ns = ns();
        // Replace unit from degC to degF on all Sensor nodes
        let sparql = format!(
            "DELETE {{ ?s <{NS}prop/unit> ?old }} \
             INSERT {{ ?s <{NS}prop/unit> \"degF\" }} \
             WHERE {{ ?s a <{NS}type/Sensor> . ?s <{NS}prop/unit> ?old }}"
        );
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.properties_removed, 1);
        assert_eq!(result.properties_set, 1);
        let node = g.get_node(NodeId(1)).unwrap();
        assert_eq!(node.property("unit"), Some(&Value::str("degF")));
    }

    #[test]
    fn delete_where_no_match_is_noop() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let sparql = format!(
            "DELETE {{ ?s <{NS}prop/nonexistent> ?v }} WHERE {{ ?s <{NS}prop/nonexistent> ?v }}"
        );
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.properties_removed, 0);
    }

    #[test]
    fn insert_where_add_edge() {
        let mut g = graph_with_sensor();
        let ns = ns();
        // Create a "monitors" edge from each Sensor to the Room
        let sparql = format!(
            "INSERT {{ ?s <{NS}rel/monitors> ?r }} \
             WHERE {{ ?s a <{NS}type/Sensor> . ?r a <{NS}type/Room> }}"
        );
        let result = execute_update(&mut g, &ns, &sparql).unwrap();
        assert_eq!(result.edges_created, 1);
        let edges: Vec<_> = g
            .outgoing(NodeId(1))
            .iter()
            .filter_map(|&eid| g.get_edge(eid))
            .collect();
        assert!(edges.iter().any(|e| e.label == IStr::new("monitors")));
    }

    // ── Phase 3: CLEAR / DROP ──

    #[test]
    fn clear_default_deletes_everything() {
        let mut g = graph_with_sensor();
        let ns = ns();
        assert!(g.node_count() > 0);
        let result = execute_update(&mut g, &ns, "CLEAR DEFAULT").unwrap();
        assert!(result.edges_deleted > 0);
        assert_eq!(g.node_count(), 0);
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn drop_default_same_as_clear() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let result = execute_update(&mut g, &ns, "DROP DEFAULT").unwrap();
        assert_eq!(g.node_count(), 0);
        assert!(result.edges_deleted > 0);
    }

    #[test]
    fn clear_named_graph_silent_is_noop() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let count_before = g.node_count();
        let result =
            execute_update(&mut g, &ns, "CLEAR SILENT GRAPH <http://example.com/g>").unwrap();
        assert_eq!(g.node_count(), count_before);
        assert_eq!(result.edges_deleted, 0);
    }

    #[test]
    fn clear_named_graph_without_silent_errors() {
        let mut g = graph_with_sensor();
        let ns = ns();
        let result = execute_update(&mut g, &ns, "CLEAR GRAPH <http://example.com/g>");
        assert!(result.is_err());
    }
}
