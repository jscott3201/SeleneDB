//! Edge CRUD operations.

use selene_core::{EdgeId, IStr, NodeId, PropertyMap, Value};
use selene_wire::dto::entity::EdgeDto;

use super::{OpError, edge_to_dto, graph_err, persist_or_die, require_in_scope};
use crate::auth::handshake::AuthContext;
use crate::auth::reserved::reject_reserved_edge_label;
use crate::bootstrap::ServerState;

/// Fetch an edge's label and promote dictionary-encoded string properties.
///
/// Looks up the edge by `edge_id`, retrieves its label, then applies
/// dictionary promotion to any string values whose schemas declare
/// `dictionary = true`.
pub(crate) fn prepare_modify_edge_props(
    state: &ServerState,
    edge_id: u64,
    props: &mut [(IStr, Value)],
) {
    let edge_label: Option<IStr> = state
        .graph
        .read(|g| g.get_edge(EdgeId(edge_id)).map(|e| e.label));
    if let Some(ref label) = edge_label {
        super::promote_dictionary_values(state, &[*label], props);
    }
}

/// Result type for edge list operations.
pub struct EdgeListResult {
    pub edges: Vec<EdgeDto>,
    pub total: u64,
}

pub fn get_edge(state: &ServerState, auth: &AuthContext, id: u64) -> Result<EdgeDto, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    let dto = state
        .graph
        .read(|g| g.get_edge(EdgeId(id)).map(edge_to_dto))
        .ok_or(OpError::NotFound { entity: "edge", id })?;

    require_in_scope(&auth, NodeId(dto.source))?;
    Ok(dto)
}

pub fn create_edge(
    state: &ServerState,
    auth: &AuthContext,
    source: u64,
    target: u64,
    label: IStr,
    props: PropertyMap,
    upsert: bool,
) -> Result<EdgeDto, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    reject_reserved_edge_label(label.as_str())?;
    require_in_scope(&auth, NodeId(source))?;
    require_in_scope(&auth, NodeId(target))?;

    if upsert {
        // Check for existing edge with same (source, target, label)
        let existing: Option<EdgeId> = state.graph.read(|g| {
            g.outgoing(NodeId(source))
                .iter()
                .find(|&&eid| {
                    g.get_edge(eid)
                        .is_some_and(|e| e.label == label && e.target == NodeId(target))
                })
                .copied()
        });

        if let Some(edge_id) = existing {
            // Update properties on existing edge
            if !props.is_empty() {
                let set_props: Vec<(IStr, Value)> =
                    props.iter().map(|(k, v)| (*k, v.clone())).collect();
                let ((), changes) = state
                    .graph
                    .write(|m| {
                        for (key, value) in set_props {
                            m.set_edge_property(edge_id, key, value)?;
                        }
                        Ok(())
                    })
                    .map_err(graph_err)?;
                if !changes.is_empty() {
                    persist_or_die(state, &changes);
                }
            }
            return state
                .graph
                .read(|g| g.get_edge(edge_id).map(edge_to_dto))
                .ok_or(OpError::Internal("edge disappeared after upsert".into()));
        }
    }

    let (edge_id, changes) = state
        .graph
        .write(|m| m.create_edge(NodeId(source), label, NodeId(target), props))
        .map_err(graph_err)?;

    persist_or_die(state, &changes);

    state
        .graph
        .read(|g| g.get_edge(edge_id).map(edge_to_dto))
        .ok_or(OpError::Internal("edge disappeared after create".into()))
}

pub fn modify_edge(
    state: &ServerState,
    auth: &AuthContext,
    id: u64,
    set_properties: Vec<(IStr, Value)>,
    remove_properties: Vec<String>,
) -> Result<EdgeDto, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    let edge_id = EdgeId(id);

    let source_id = state
        .graph
        .read(|g| g.get_edge(edge_id).map(|e| e.source))
        .ok_or(OpError::NotFound { entity: "edge", id })?;
    require_in_scope(&auth, source_id)?;

    let ((), changes) = state
        .graph
        .write(|m| {
            for (key, value) in set_properties {
                m.set_edge_property(edge_id, key, value)?;
            }
            for key in &remove_properties {
                m.remove_edge_property(edge_id, key)?;
            }
            Ok(())
        })
        .map_err(graph_err)?;

    persist_or_die(state, &changes);

    state
        .graph
        .read(|g| g.get_edge(edge_id).map(edge_to_dto))
        .ok_or(OpError::NotFound { entity: "edge", id })
}

pub fn delete_edge(state: &ServerState, auth: &AuthContext, id: u64) -> Result<(), OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    let edge_id = EdgeId(id);

    let source_id = state
        .graph
        .read(|g| g.get_edge(edge_id).map(|e| e.source))
        .ok_or(OpError::NotFound { entity: "edge", id })?;
    require_in_scope(&auth, source_id)?;

    let ((), changes) = state
        .graph
        .write(|m| m.delete_edge(edge_id))
        .map_err(graph_err)?;

    persist_or_die(state, &changes);
    Ok(())
}

/// Get edges connected to a node with optional direction and label filtering.
///
/// Uses the adjacency index for O(degree) lookup instead of scanning all edges.
/// Returns `total` as the filtered count, and `edges` as the paginated slice.
/// When `direction` is "outgoing" or "incoming", only that direction is scanned.
/// When `label_filter` is non-empty, only edges with matching labels are returned.
pub fn node_edges(
    state: &ServerState,
    auth: &AuthContext,
    node_id: u64,
    direction: Option<&str>,
    label_filter: Option<&[String]>,
    offset: usize,
    limit: usize,
) -> Result<DirectedEdgeResult, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    require_in_scope(&auth, NodeId(node_id))?;

    let include_outgoing = !matches!(direction, Some("incoming"));
    let include_incoming = !matches!(direction, Some("outgoing"));

    let (outgoing, incoming, node_exists) = state.graph.read(|g| {
        if !g.contains_node(NodeId(node_id)) {
            return (vec![], vec![], false);
        }

        let label_matches = |e: &selene_graph::EdgeRef<'_>| -> bool {
            match label_filter {
                Some(labels) if !labels.is_empty() => labels.iter().any(|l| e.label.as_str() == l),
                _ => true,
            }
        };

        let mut seen = std::collections::HashSet::new();
        let mut out_edges = Vec::new();
        let mut in_edges = Vec::new();

        if include_outgoing {
            for &eid in g.outgoing(NodeId(node_id)) {
                if seen.insert(eid)
                    && let Some(e) = g.get_edge(eid)
                    && label_matches(&e)
                    && (auth.is_admin() || (auth.in_scope(e.source) && auth.in_scope(e.target)))
                {
                    // Include target node name for agent convenience
                    let target_name = g
                        .get_node(e.target)
                        .and_then(|n| n.properties.get(IStr::new("name")))
                        .and_then(|v| match v {
                            selene_core::Value::String(s) => Some(s.to_string()),
                            _ => None,
                        });
                    out_edges.push(DirectedEdge {
                        edge: edge_to_dto(e),
                        neighbor_name: target_name,
                    });
                }
            }
        }

        if include_incoming {
            for &eid in g.incoming(NodeId(node_id)) {
                if seen.insert(eid)
                    && let Some(e) = g.get_edge(eid)
                    && label_matches(&e)
                    && (auth.is_admin() || (auth.in_scope(e.source) && auth.in_scope(e.target)))
                {
                    let source_name = g
                        .get_node(e.source)
                        .and_then(|n| n.properties.get(IStr::new("name")))
                        .and_then(|v| match v {
                            selene_core::Value::String(s) => Some(s.to_string()),
                            _ => None,
                        });
                    in_edges.push(DirectedEdge {
                        edge: edge_to_dto(e),
                        neighbor_name: source_name,
                    });
                }
            }
        }

        (out_edges, in_edges, true)
    });

    if !node_exists {
        return Err(OpError::NotFound {
            entity: "node",
            id: node_id,
        });
    }

    let total = (outgoing.len() + incoming.len()) as u64;
    let outgoing_page: Vec<_> = outgoing.into_iter().skip(offset).take(limit).collect();
    let remaining = limit.saturating_sub(outgoing_page.len());
    let incoming_skip =
        offset.saturating_sub(outgoing_page.len() + offset.min(outgoing_page.len()));
    let incoming_page: Vec<_> = incoming
        .into_iter()
        .skip(incoming_skip)
        .take(remaining)
        .collect();

    Ok(DirectedEdgeResult {
        outgoing: outgoing_page,
        incoming: incoming_page,
        total,
    })
}

/// A single edge with the neighbor node's name for agent convenience.
#[derive(serde::Serialize)]
pub struct DirectedEdge {
    #[serde(flatten)]
    pub edge: EdgeDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub neighbor_name: Option<String>,
}

/// Result of a directed node_edges query.
#[derive(serde::Serialize)]
pub struct DirectedEdgeResult {
    pub outgoing: Vec<DirectedEdge>,
    pub incoming: Vec<DirectedEdge>,
    pub total: u64,
}

pub fn list_edges(
    state: &ServerState,
    auth: &AuthContext,
    label: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<EdgeListResult, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    let (edges, total) = state.graph.read(|g| {
        if auth.is_admin() && label.is_none() {
            // Fast path: admin with no label filter can use O(1) edge_count
            // and skip/take directly from the iterator without collecting.
            let total = g.edge_count() as u64;
            let edges: Vec<EdgeDto> = g
                .all_edge_ids()
                .skip(offset)
                .take(limit)
                .filter_map(|id| g.get_edge(id).map(edge_to_dto))
                .collect();
            (edges, total)
        } else {
            // Scoped or label-filtered path: must collect to get total count.
            let scoped_iter: Box<dyn Iterator<Item = EdgeId> + '_> = if let Some(label) = label {
                if auth.is_admin() {
                    Box::new(g.edges_by_label(label))
                } else {
                    Box::new(g.edges_by_label(label).filter(|id| {
                        g.get_edge(*id)
                            .is_some_and(|e| auth.in_scope(e.source) && auth.in_scope(e.target))
                    }))
                }
            } else {
                Box::new(g.all_edge_ids().filter(|id| {
                    g.get_edge(*id)
                        .is_some_and(|e| auth.in_scope(e.source) && auth.in_scope(e.target))
                }))
            };

            let scoped_ids: Vec<EdgeId> = scoped_iter.collect();
            let total = scoped_ids.len() as u64;
            let edges: Vec<EdgeDto> = scoped_ids
                .into_iter()
                .skip(offset)
                .take(limit)
                .filter_map(|id| g.get_edge(id).map(edge_to_dto))
                .collect();

            (edges, total)
        }
    });

    Ok(EdgeListResult { edges, total })
}
