//! Graph slicing operations.

use std::collections::{HashSet, VecDeque};

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
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
            SliceType::Traverse {
                root_id,
                edge_labels,
                direction,
                max_depth,
            } => {
                let label_filter: Vec<IStr> = edge_labels.iter().map(|l| IStr::new(l)).collect();
                slice_traverse(g, NodeId(*root_id), &label_filter, direction, *max_depth)
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

/// BFS traversal from a root node following specified edge labels and direction.
///
/// Returns a `GraphSlice` containing all visited nodes and the traversed edges.
/// Nodes include a `depth` property indicating distance from root.
fn slice_traverse(
    g: &SeleneGraph,
    root: NodeId,
    edge_labels: &[IStr],
    direction: &str,
    max_depth: u32,
) -> selene_graph::snapshot::GraphSlice {
    use selene_core::{Edge, Node, Value};

    let follow_outgoing = direction != "incoming";
    let follow_incoming = direction != "outgoing";

    let mut visited: HashSet<NodeId> = HashSet::new();
    let mut collected_edges: HashSet<selene_core::EdgeId> = HashSet::new();
    let mut queue: VecDeque<(NodeId, u32)> = VecDeque::new();
    let mut result_nodes: Vec<Node> = Vec::new();
    let mut result_edges: Vec<Edge> = Vec::new();

    let add_node = |nref: selene_graph::NodeRef<'_>, depth: u32, nodes: &mut Vec<Node>| {
        let mut n = nref.to_owned_node();
        n.properties
            .insert(IStr::new("_depth"), Value::Int(i64::from(depth)));
        nodes.push(n);
    };

    // Add root node
    if let Some(node) = g.get_node(root) {
        add_node(node, 0, &mut result_nodes);
        visited.insert(root);
        queue.push_back((root, 0));
    }

    while let Some((current, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }

        let label_matches =
            |label: &IStr| -> bool { edge_labels.is_empty() || edge_labels.contains(label) };

        if follow_outgoing {
            for &eid in g.outgoing(current) {
                if let Some(e) = g.get_edge(eid) {
                    if label_matches(&e.label)
                        && visited.insert(e.target)
                        && let Some(target) = g.get_node(e.target)
                    {
                        add_node(target, depth + 1, &mut result_nodes);
                        queue.push_back((e.target, depth + 1));
                    }
                    if collected_edges.insert(eid)
                        && visited.contains(&e.source)
                        && visited.contains(&e.target)
                    {
                        result_edges.push(e.to_owned_edge());
                    }
                }
            }
        }

        if follow_incoming {
            for &eid in g.incoming(current) {
                if let Some(e) = g.get_edge(eid) {
                    if label_matches(&e.label)
                        && visited.insert(e.source)
                        && let Some(source) = g.get_node(e.source)
                    {
                        add_node(source, depth + 1, &mut result_nodes);
                        queue.push_back((e.source, depth + 1));
                    }
                    if collected_edges.insert(eid)
                        && visited.contains(&e.source)
                        && visited.contains(&e.target)
                    {
                        result_edges.push(e.to_owned_edge());
                    }
                }
            }
        }
    }

    // Collect remaining edges between visited nodes
    for &nid in &visited {
        for &eid in g.outgoing(nid) {
            if let Some(e) = g.get_edge(eid)
                && visited.contains(&e.target)
                && collected_edges.insert(eid)
            {
                result_edges.push(e.to_owned_edge());
            }
        }
    }

    selene_graph::snapshot::GraphSlice {
        nodes: result_nodes,
        edges: result_edges,
    }
}
