//! Edge CRUD operations.

use selene_core::{EdgeId, IStr, NodeId, PropertyMap, Value};
use selene_wire::dto::entity::EdgeDto;

use super::{OpError, edge_to_dto, graph_err, persist_or_die, require_in_scope};
use crate::auth::handshake::AuthContext;
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
) -> Result<EdgeDto, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    require_in_scope(&auth, NodeId(source))?;
    require_in_scope(&auth, NodeId(target))?;

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

/// Get edges connected to a node (both incoming and outgoing) with pagination.
///
/// Uses the adjacency index for O(degree) lookup instead of scanning all edges.
/// Returns `total` as the unfiltered count, and `edges` as the paginated slice.
pub fn node_edges(
    state: &ServerState,
    auth: &AuthContext,
    node_id: u64,
    offset: usize,
    limit: usize,
) -> Result<EdgeListResult, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    require_in_scope(&auth, NodeId(node_id))?;

    let (edges, total, node_exists) = state.graph.read(|g| {
        if !g.contains_node(NodeId(node_id)) {
            return (vec![], 0u64, false);
        }

        let mut seen = std::collections::HashSet::new();
        let mut all_edges = Vec::new();

        for &eid in g.outgoing(NodeId(node_id)) {
            if seen.insert(eid)
                && let Some(e) = g.get_edge(eid)
                && (auth.is_admin() || (auth.in_scope(e.source) && auth.in_scope(e.target)))
            {
                all_edges.push(edge_to_dto(e));
            }
        }
        for &eid in g.incoming(NodeId(node_id)) {
            if seen.insert(eid)
                && let Some(e) = g.get_edge(eid)
                && (auth.is_admin() || (auth.in_scope(e.source) && auth.in_scope(e.target)))
            {
                all_edges.push(edge_to_dto(e));
            }
        }

        let total = all_edges.len() as u64;
        let edges = all_edges.into_iter().skip(offset).take(limit).collect();
        (edges, total, true)
    });

    if !node_exists {
        return Err(OpError::NotFound {
            entity: "node",
            id: node_id,
        });
    }

    Ok(EdgeListResult { edges, total })
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
