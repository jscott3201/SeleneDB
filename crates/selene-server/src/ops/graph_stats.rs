//! Graph statistics: per-label node and edge counts.

use std::collections::HashMap;

use crate::ServerState;
use crate::auth::handshake::AuthContext;

/// Per-label breakdown of graph entities.
pub struct GraphStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub node_labels: HashMap<String, u64>,
    pub edge_labels: HashMap<String, u64>,
}

/// Compute graph statistics with scope-aware filtering.
pub fn graph_stats(state: &ServerState, auth: &AuthContext) -> GraphStats {
    let auth = super::refresh_scope_if_stale(state, auth);
    state.graph.read(|g| {
        let mut node_labels: HashMap<String, u64> = HashMap::new();
        let mut node_total: u64 = 0;
        for nid in &g.all_node_bitmap() {
            let node_id = selene_core::NodeId(u64::from(nid));
            if !auth.in_scope(node_id) {
                continue;
            }
            if let Some(node) = g.get_node(node_id) {
                node_total += 1;
                for label in node.labels.iter() {
                    *node_labels.entry(label.as_str().to_string()).or_default() += 1;
                }
            }
        }
        let mut edge_labels: HashMap<String, u64> = HashMap::new();
        let mut edge_total: u64 = 0;
        for eid in &g.all_edge_bitmap() {
            if let Some(edge) = g.get_edge(selene_core::EdgeId(u64::from(eid))) {
                if !auth.in_scope(edge.source) || !auth.in_scope(edge.target) {
                    continue;
                }
                edge_total += 1;
                *edge_labels
                    .entry(edge.label.as_str().to_string())
                    .or_default() += 1;
            }
        }
        GraphStats {
            node_count: node_total,
            edge_count: edge_total,
            node_labels,
            edge_labels,
        }
    })
}
