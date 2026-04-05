//! Node CRUD operations.

use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_wire::dto::entity::NodeDto;

use super::{OpError, graph_err, node_to_dto, persist_or_die, require_in_scope};
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

/// Fetch a node's labels and promote dictionary-encoded string properties.
///
/// Looks up the node by `node_id`, retrieves its labels, then applies
/// dictionary promotion to any string values whose schemas declare
/// `dictionary = true`.
pub(crate) fn prepare_modify_node_props(
    state: &ServerState,
    node_id: u64,
    props: &mut [(IStr, Value)],
) {
    if props.is_empty() {
        return;
    }
    let labels: Vec<IStr> = state.graph.read(|g| {
        g.get_node(NodeId(node_id))
            .map(|n| n.labels.iter().collect())
            .unwrap_or_default()
    });
    super::promote_dictionary_values(state, &labels, props);
}

/// Result type for list operations.
pub struct NodeListResult {
    pub nodes: Vec<NodeDto>,
    pub total: u64,
}

pub fn get_node(state: &ServerState, auth: &AuthContext, id: u64) -> Result<NodeDto, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    let node_id = NodeId(id);
    require_in_scope(&auth, node_id)?;

    state
        .graph
        .read(|g| g.get_node(node_id).map(node_to_dto))
        .ok_or(OpError::NotFound { entity: "node", id })
}

pub fn create_node(
    state: &ServerState,
    auth: &AuthContext,
    labels: LabelSet,
    props: PropertyMap,
    parent_id: Option<u64>,
) -> Result<NodeDto, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);

    // Non-admin must provide parent_id for containment placement
    if !auth.is_admin() && parent_id.is_none() {
        return Err(OpError::InvalidRequest(
            "non-admin principals must provide parent_id for containment".into(),
        ));
    }

    // If parent_id provided, verify it's in scope
    if let Some(pid) = parent_id {
        require_in_scope(&auth, NodeId(pid))?;
    }

    let (node_id, changes) = state
        .graph
        .write(|m| {
            let id = m.create_node(labels, props)?;
            // Auto-create containment edge if parent specified
            if let Some(pid) = parent_id {
                m.create_edge(NodeId(pid), IStr::new("contains"), id, PropertyMap::new())?;
            }
            Ok(id)
        })
        .map_err(graph_err)?;

    persist_or_die(state, &changes);

    state
        .graph
        .read(|g| g.get_node(node_id).map(node_to_dto))
        .ok_or(OpError::Internal("node disappeared after create".into()))
}

pub fn modify_node(
    state: &ServerState,
    auth: &AuthContext,
    id: u64,
    set_properties: Vec<(IStr, Value)>,
    remove_properties: Vec<String>,
    add_labels: Vec<IStr>,
    remove_labels: Vec<String>,
) -> Result<NodeDto, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    let node_id = NodeId(id);
    require_in_scope(&auth, node_id)?;

    let ((), changes) = state
        .graph
        .write(|m| {
            for (key, value) in set_properties {
                m.set_property(node_id, key, value)?;
            }
            for key in &remove_properties {
                m.remove_property(node_id, key)?;
            }
            for label in add_labels {
                m.add_label(node_id, label)?;
            }
            for label in &remove_labels {
                m.remove_label(node_id, label)?;
            }
            Ok(())
        })
        .map_err(graph_err)?;

    persist_or_die(state, &changes);

    state
        .graph
        .read(|g| g.get_node(node_id).map(node_to_dto))
        .ok_or(OpError::NotFound { entity: "node", id })
}

pub fn delete_node(state: &ServerState, auth: &AuthContext, id: u64) -> Result<(), OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    let node_id = NodeId(id);
    require_in_scope(&auth, node_id)?;

    let ((), changes) = state
        .graph
        .write(|m| m.delete_node(node_id))
        .map_err(graph_err)?;

    persist_or_die(state, &changes);
    Ok(())
}

pub fn list_nodes(
    state: &ServerState,
    auth: &AuthContext,
    label: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<NodeListResult, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    let (nodes, total) = state.graph.read(|g| {
        // Build an iterator over candidate node IDs — no intermediate Vec
        let candidates: Box<dyn Iterator<Item = NodeId> + '_> = if let Some(label) = label {
            Box::new(g.nodes_by_label(label))
        } else {
            Box::new(g.all_node_ids())
        };

        // For total count: admin = node_count (O(1)) or label count,
        // scoped = scope.len() (O(1)). We compute this without collecting.
        let total = if auth.is_admin() {
            if let Some(label) = label {
                g.nodes_by_label(label).count() as u64
            } else {
                g.node_count() as u64
            }
        } else {
            // Scoped user: count is bounded by scope size
            if let Some(label) = label {
                g.nodes_by_label(label)
                    .filter(|id| auth.in_scope(*id))
                    .count() as u64
            } else {
                auth.scope.len()
            }
        };

        // Page through with iterator — allocates only `limit` items
        let nodes: Vec<NodeDto> = candidates
            .filter(|id| auth.in_scope(*id))
            .skip(offset)
            .take(limit)
            .filter_map(|id| g.get_node(id).map(node_to_dto))
            .collect();

        (nodes, total)
    });

    Ok(NodeListResult { nodes, total })
}
