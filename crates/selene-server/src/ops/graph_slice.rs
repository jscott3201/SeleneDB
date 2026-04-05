//! Graph slicing operations.

use selene_core::NodeId;
use selene_wire::dto::entity::{EdgeDto, NodeDto};
use selene_wire::dto::graph_slice::SliceType;

use super::{edge_to_dto, node_to_dto};
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

/// Graph slice result with optional pagination totals.
pub struct GraphSliceResult {
    pub nodes: Vec<NodeDto>,
    pub edges: Vec<EdgeDto>,
    pub total_nodes: Option<usize>,
    pub total_edges: Option<usize>,
}

pub fn graph_slice(
    state: &ServerState,
    auth: &AuthContext,
    slice_type: &SliceType,
    limit: Option<usize>,
    offset: Option<usize>,
) -> GraphSliceResult {
    let auth = super::refresh_scope_if_stale(state, auth);
    state.graph.read(|g| {
        use selene_graph::snapshot::{slice_by_labels, slice_containment, slice_full};

        let slice = match slice_type {
            SliceType::Full => slice_full(g),
            SliceType::ByLabels { labels } => {
                let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
                slice_by_labels(g, &label_refs)
            }
            SliceType::Containment { root_id, max_depth } => {
                slice_containment(g, NodeId(*root_id), *max_depth)
            }
        };

        // Auth-filter first
        let all_nodes: Vec<NodeDto> = slice
            .nodes
            .iter()
            .filter(|n| auth.in_scope(n.id))
            .map(|n| node_to_dto(n.into()))
            .collect();
        let all_edges: Vec<EdgeDto> = slice
            .edges
            .iter()
            .filter(|e| auth.in_scope(e.source) && auth.in_scope(e.target))
            .map(|e| edge_to_dto(e.into()))
            .collect();

        // Apply pagination if requested
        if limit.is_some() || offset.is_some() {
            let total_nodes = all_nodes.len();
            let total_edges = all_edges.len();
            let off = offset.unwrap_or(0);
            let lim = limit.unwrap_or(usize::MAX);

            let paginated_nodes: Vec<NodeDto> = all_nodes.into_iter().skip(off).take(lim).collect();

            // Only include edges that connect paginated nodes
            let node_ids: std::collections::HashSet<u64> =
                paginated_nodes.iter().map(|n| n.id).collect();
            let paginated_edges: Vec<EdgeDto> = all_edges
                .into_iter()
                .filter(|e| node_ids.contains(&e.source) && node_ids.contains(&e.target))
                .collect();

            GraphSliceResult {
                nodes: paginated_nodes,
                edges: paginated_edges,
                total_nodes: Some(total_nodes),
                total_edges: Some(total_edges),
            }
        } else {
            GraphSliceResult {
                nodes: all_nodes,
                edges: all_edges,
                total_nodes: None,
                total_edges: None,
            }
        }
    })
}
