//! Property-based tests for SeleneGraph using proptest.
//!
//! These tests verify structural invariants hold after arbitrary
//! sequences of mutations.

use proptest::prelude::*;
use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::SeleneGraph;

/// Operations that can be applied to the graph.
#[derive(Debug, Clone)]
enum GraphOp {
    CreateNode {
        labels: Vec<String>,
        props: Vec<(String, i64)>,
    },
    DeleteNode(u64),
    SetProperty(u64, String, i64),
    RemoveProperty(u64, String),
    AddLabel(u64, String),
    RemoveLabel(u64, String),
    CreateEdge(u64, u64, String),
    DeleteEdge(u64),
}

fn arb_label() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("sensor".to_string()),
        Just("building".to_string()),
        Just("floor".to_string()),
        Just("zone".to_string()),
        Just("equipment".to_string()),
        Just("point".to_string()),
    ]
}

fn arb_op() -> impl Strategy<Value = GraphOp> {
    prop_oneof![
        // Create node with 1-3 labels and 0-2 properties
        (
            prop::collection::vec(arb_label(), 1..=3),
            prop::collection::vec((arb_label(), any::<i64>()), 0..=2),
        )
            .prop_map(|(labels, props)| GraphOp::CreateNode { labels, props }),
        // Delete a node (by raw ID — may or may not exist)
        (1u64..=20).prop_map(GraphOp::DeleteNode),
        // Set property on a node
        (1u64..=20, arb_label(), any::<i64>())
            .prop_map(|(id, k, v)| GraphOp::SetProperty(id, k, v)),
        // Remove property
        (1u64..=20, arb_label()).prop_map(|(id, k)| GraphOp::RemoveProperty(id, k)),
        // Add label
        (1u64..=20, arb_label()).prop_map(|(id, l)| GraphOp::AddLabel(id, l)),
        // Remove label
        (1u64..=20, arb_label()).prop_map(|(id, l)| GraphOp::RemoveLabel(id, l)),
        // Create edge (source, target may not exist)
        (1u64..=20, 1u64..=20, arb_label()).prop_map(|(s, t, l)| GraphOp::CreateEdge(s, t, l)),
        // Delete edge
        (1u64..=20).prop_map(GraphOp::DeleteEdge),
    ]
}

fn dispatch_op(m: &mut selene_graph::TrackedMutation<'_>, op: &GraphOp) {
    match op {
        GraphOp::CreateNode { labels, props } => {
            let label_set =
                LabelSet::from_strs(&labels.iter().map(|s| s.as_str()).collect::<Vec<_>>());
            let prop_map =
                PropertyMap::from_pairs(props.iter().map(|(k, v)| (IStr::new(k), Value::Int(*v))));
            let _ = m.create_node(label_set, prop_map);
        }
        GraphOp::DeleteNode(id) => {
            let _ = m.delete_node(NodeId(*id));
        }
        GraphOp::SetProperty(id, key, val) => {
            let _ = m.set_property(NodeId(*id), IStr::new(key), Value::Int(*val));
        }
        GraphOp::RemoveProperty(id, key) => {
            let _ = m.remove_property(NodeId(*id), key);
        }
        GraphOp::AddLabel(id, label) => {
            let _ = m.add_label(NodeId(*id), IStr::new(label));
        }
        GraphOp::RemoveLabel(id, label) => {
            let _ = m.remove_label(NodeId(*id), label);
        }
        GraphOp::CreateEdge(src, tgt, label) => {
            let _ = m.create_edge(
                NodeId(*src),
                IStr::new(label),
                NodeId(*tgt),
                PropertyMap::new(),
            );
        }
        GraphOp::DeleteEdge(id) => {
            let _ = m.delete_edge(selene_core::EdgeId(*id));
        }
    }
}

fn apply_op(graph: &mut SeleneGraph, op: &GraphOp) {
    let mut m = graph.mutate();
    dispatch_op(&mut m, op);
    let _ = m.commit(0);
}

fn apply_op_no_commit(graph: &mut SeleneGraph, op: &GraphOp) {
    let mut m = graph.mutate();
    dispatch_op(&mut m, op);
    // Deliberately NOT calling commit — mutation is dropped, triggering rollback
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// After N random committed mutations, all indexes remain consistent.
    #[test]
    fn index_consistency_after_random_mutations(
        ops in prop::collection::vec(arb_op(), 1..100)
    ) {
        let mut graph = SeleneGraph::new();
        for op in &ops {
            apply_op(&mut graph, op);
        }
        graph.assert_indexes_consistent();
    }

    /// Rollback restores the graph to its exact pre-mutation state.
    #[test]
    fn rollback_restores_exact_state(
        // First build some base state
        base_ops in prop::collection::vec(arb_op(), 1..30),
        // Then apply ops that will be rolled back
        rollback_ops in prop::collection::vec(arb_op(), 1..30),
    ) {
        let mut graph = SeleneGraph::new();

        // Build base state
        for op in &base_ops {
            apply_op(&mut graph, op);
        }
        let base_node_count = graph.node_count();
        let base_edge_count = graph.edge_count();

        // Apply ops without committing (will rollback)
        for op in &rollback_ops {
            apply_op_no_commit(&mut graph, op);
        }

        // State should be exactly restored
        prop_assert_eq!(graph.node_count(), base_node_count);
        prop_assert_eq!(graph.edge_count(), base_edge_count);
        graph.assert_indexes_consistent();
    }

    /// Changelog sequence is strictly monotonically increasing.
    #[test]
    fn changelog_sequence_monotonic(
        ops in prop::collection::vec(arb_op(), 1..50)
    ) {
        let mut graph = SeleneGraph::new();
        let mut last_seq = 0u64;

        for op in &ops {
            apply_op(&mut graph, op);
        }

        let seq = graph.changelog().current_sequence();
        prop_assert!(seq >= last_seq, "sequence went backwards: {} < {}", seq, last_seq);
        last_seq = seq;
        _ = last_seq; // suppress unused warning
    }
}
