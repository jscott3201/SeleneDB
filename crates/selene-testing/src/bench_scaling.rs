//! Graph scaling helper for benchmarks.
//!
//! Wraps `reference_building(scale)` with target node count approximation.
//! Each building adds 48 nodes; 6 nodes are fixed overhead (campus + parking).

use selene_graph::SeleneGraph;

use crate::reference_building::reference_building;

/// Nodes per building in the reference model.
const NODES_PER_BUILDING: u64 = 48;
/// Fixed overhead nodes (1 campus + 1 parking + 2 levels + 2 sensors).
const FIXED_OVERHEAD: u64 = 6;

/// Build a reference building graph targeting approximately `target_nodes` nodes.
///
/// The actual node count will be `scale * 48 + 6` where `scale = (target - 6) / 48`.
/// Minimum scale is 1 (~54 nodes).
pub fn build_scaled_graph(target_nodes: u64) -> SeleneGraph {
    let scale = target_to_scale(target_nodes);
    reference_building(scale)
}

/// Build a reference building graph with summary statistics.
pub fn build_scaled_graph_with_summary(target_nodes: u64) -> (SeleneGraph, ScalingSummary) {
    let scale = target_to_scale(target_nodes);
    let g = reference_building(scale);
    let summary = ScalingSummary {
        target_nodes,
        scale,
        actual_nodes: g.node_count(),
        actual_edges: g.edge_count(),
    };
    (g, summary)
}

/// Statistics from `build_scaled_graph`.
#[derive(Debug)]
pub struct ScalingSummary {
    pub target_nodes: u64,
    pub scale: usize,
    pub actual_nodes: usize,
    pub actual_edges: usize,
}

fn target_to_scale(target_nodes: u64) -> usize {
    let scale = target_nodes.saturating_sub(FIXED_OVERHEAD) / NODES_PER_BUILDING;
    (scale as usize).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_to_scale_math() {
        assert_eq!(target_to_scale(54), 1);
        assert_eq!(target_to_scale(100), 1);
        assert_eq!(target_to_scale(200), 4);
        assert_eq!(target_to_scale(1_000), 20);
        assert_eq!(target_to_scale(10_000), 208);
        assert_eq!(target_to_scale(100_000), 2083);
    }

    #[test]
    fn build_scaled_graph_produces_expected_sizes() {
        let g1 = build_scaled_graph(200);
        assert!(g1.node_count() >= 50);
        assert!(g1.edge_count() > 0);

        let g2 = build_scaled_graph(1_000);
        assert!(g2.node_count() > g1.node_count());

        // Verify deterministic
        let g2b = build_scaled_graph(1_000);
        assert_eq!(g2.node_count(), g2b.node_count());
    }

    #[test]
    fn build_scaled_graph_with_summary_reports_correctly() {
        let (g, summary) = build_scaled_graph_with_summary(1_000);
        assert_eq!(summary.actual_nodes, g.node_count());
        assert_eq!(summary.actual_edges, g.edge_count());
        assert_eq!(summary.target_nodes, 1_000);
        assert!(summary.scale > 0);
    }
}
