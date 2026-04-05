use super::*;
use crate::types::value::GqlList;
use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::SeleneGraph;
use smol_str::SmolStr;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ── Fixture helpers ─────────────────────────────────────────────

/// Linear chain: 1 -> 2 -> 3 -> 4
fn chain_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    for _ in 0..4 {
        m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
            .unwrap();
    }
    m.create_edge(NodeId(1), IStr::new("link"), NodeId(2), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(2), IStr::new("link"), NodeId(3), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(3), IStr::new("link"), NodeId(4), PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();
    g
}

/// Triangle: 1 -- 2 -- 3 -- 1 (bidirectional edges for undirected semantics)
fn triangle_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    for _ in 0..3 {
        m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
            .unwrap();
    }
    m.create_edge(NodeId(1), IStr::new("link"), NodeId(2), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(2), IStr::new("link"), NodeId(1), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(2), IStr::new("link"), NodeId(3), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(3), IStr::new("link"), NodeId(2), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(3), IStr::new("link"), NodeId(1), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(1), IStr::new("link"), NodeId(3), PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();
    g
}

/// Disconnected: {1,2} linked, {3,4} linked, 5 isolated
fn disconnected_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    for _ in 0..5 {
        m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
            .unwrap();
    }
    m.create_edge(NodeId(1), IStr::new("link"), NodeId(2), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(2), IStr::new("link"), NodeId(1), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(3), IStr::new("link"), NodeId(4), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(4), IStr::new("link"), NodeId(3), PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();
    g
}

/// Star graph: 1 at center, edges to 2,3,4,5
fn star_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    for _ in 0..5 {
        m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
            .unwrap();
    }
    for target in 2..=5 {
        m.create_edge(
            NodeId(1),
            IStr::new("link"),
            NodeId(target),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(
            NodeId(target),
            IStr::new("link"),
            NodeId(1),
            PropertyMap::new(),
        )
        .unwrap();
    }
    m.commit(0).unwrap();
    g
}

/// Multi-label graph: nodes with "sensor" and "device" labels
fn multi_label_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    m.create_node(LabelSet::from_strs(&["sensor"]), PropertyMap::new())
        .unwrap();
    m.create_node(LabelSet::from_strs(&["sensor"]), PropertyMap::new())
        .unwrap();
    m.create_node(LabelSet::from_strs(&["device"]), PropertyMap::new())
        .unwrap();
    m.create_edge(
        NodeId(1),
        IStr::new("monitors"),
        NodeId(3),
        PropertyMap::new(),
    )
    .unwrap();
    m.create_edge(
        NodeId(2),
        IStr::new("monitors"),
        NodeId(3),
        PropertyMap::new(),
    )
    .unwrap();
    m.commit(0).unwrap();
    g
}

/// DAG: 1 -> 2, 1 -> 3, 2 -> 4, 3 -> 4
fn dag_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    for _ in 0..4 {
        m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
            .unwrap();
    }
    m.create_edge(NodeId(1), IStr::new("link"), NodeId(2), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(1), IStr::new("link"), NodeId(3), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(2), IStr::new("link"), NodeId(4), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(3), IStr::new("link"), NodeId(4), PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();
    g
}

/// Containment graph: 1 -contains-> 2 -contains-> 3
fn containment_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    for _ in 0..4 {
        m.create_node(LabelSet::from_strs(&["zone"]), PropertyMap::new())
            .unwrap();
    }
    m.create_edge(
        NodeId(1),
        IStr::new("contains"),
        NodeId(2),
        PropertyMap::new(),
    )
    .unwrap();
    m.create_edge(
        NodeId(2),
        IStr::new("contains"),
        NodeId(3),
        PropertyMap::new(),
    )
    .unwrap();
    m.commit(0).unwrap();
    g
}

/// Weighted graph: chain 1->2->3 with cost properties
fn weighted_chain() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    for _ in 0..3 {
        m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
            .unwrap();
    }
    m.create_edge(
        NodeId(1),
        IStr::new("link"),
        NodeId(2),
        PropertyMap::from_pairs(vec![(IStr::new("cost"), Value::Float(1.0))]),
    )
    .unwrap();
    m.create_edge(
        NodeId(2),
        IStr::new("link"),
        NodeId(3),
        PropertyMap::from_pairs(vec![(IStr::new("cost"), Value::Float(2.0))]),
    )
    .unwrap();
    m.commit(0).unwrap();
    g
}

/// Cyclic graph: 1 -> 2 -> 3 -> 1
fn cycle_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    for _ in 0..3 {
        m.create_node(LabelSet::from_strs(&["node"]), PropertyMap::new())
            .unwrap();
    }
    m.create_edge(NodeId(1), IStr::new("link"), NodeId(2), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(2), IStr::new("link"), NodeId(3), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(3), IStr::new("link"), NodeId(1), PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();
    g
}

fn empty_graph() -> SeleneGraph {
    SeleneGraph::new()
}

fn make_string_list(items: &[&str]) -> GqlValue {
    GqlValue::List(GqlList {
        element_type: GqlType::String,
        elements: Arc::from(
            items
                .iter()
                .map(|s| GqlValue::String(SmolStr::new(s)))
                .collect::<Vec<_>>(),
        ),
    })
}

// ── Projection management ───────────────────────────────────────

#[test]
fn project_creates_projection_and_returns_stats() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphProject {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("test"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, GqlValue::String(SmolStr::new("test")));
    assert_eq!(rows[0][1].1, GqlValue::Int(4)); // 4 nodes
    assert_eq!(rows[0][2].1, GqlValue::Int(3)); // 3 edges
}

#[test]
fn project_with_label_filter() {
    let g = multi_label_graph();
    let catalog = new_shared_catalog();
    let proc = GraphProject {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("sensors_only")),
        make_string_list(&["sensor"]),
        GqlValue::Null,
        GqlValue::Null,
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows[0][1].1, GqlValue::Int(2)); // 2 sensor nodes
}

#[test]
fn project_with_edge_label_filter() {
    let g = multi_label_graph();
    let catalog = new_shared_catalog();
    let proc = GraphProject {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("monitors_only")),
        GqlValue::Null,
        make_string_list(&["monitors"]),
        GqlValue::Null,
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows[0][2].1, GqlValue::Int(2)); // 2 monitors edges
}

#[test]
fn project_on_empty_graph() {
    let g = empty_graph();
    let catalog = new_shared_catalog();
    let proc = GraphProject {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("empty"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows[0][1].1, GqlValue::Int(0));
    assert_eq!(rows[0][2].1, GqlValue::Int(0));
}

#[test]
fn project_missing_name_arg_errors() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphProject {
        catalog: catalog.clone(),
    };
    let result = proc.execute(&[], &g, None, None);
    assert!(result.is_err());
}

#[test]
fn drop_removes_projection() {
    let g = chain_graph();
    let catalog = new_shared_catalog();

    // Create projection first
    let project = GraphProject {
        catalog: catalog.clone(),
    };
    project
        .execute(&[GqlValue::String(SmolStr::new("to_drop"))], &g, None, None)
        .unwrap();

    // Drop it
    let drop_proc = GraphDrop {
        catalog: catalog.clone(),
    };
    let rows = drop_proc
        .execute(&[GqlValue::String(SmolStr::new("to_drop"))], &g, None, None)
        .unwrap();
    assert_eq!(rows[0][0].1, GqlValue::Bool(true));
}

#[test]
fn drop_nonexistent_returns_false() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphDrop {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(&[GqlValue::String(SmolStr::new("nope"))], &g, None, None)
        .unwrap();
    assert_eq!(rows[0][0].1, GqlValue::Bool(false));
}

#[test]
fn list_projections_returns_all() {
    let g = chain_graph();
    let catalog = new_shared_catalog();

    // Create two projections
    let project = GraphProject {
        catalog: catalog.clone(),
    };
    project
        .execute(&[GqlValue::String(SmolStr::new("a"))], &g, None, None)
        .unwrap();
    project
        .execute(&[GqlValue::String(SmolStr::new("b"))], &g, None, None)
        .unwrap();

    let list_proc = GraphListProjections {
        catalog: catalog.clone(),
    };
    let rows = list_proc.execute(&[], &g, None, None).unwrap();
    assert_eq!(rows.len(), 2);

    let names: HashSet<_> = rows
        .iter()
        .map(|r| r[0].1.as_str().unwrap().to_string())
        .collect();
    assert!(names.contains("a"));
    assert!(names.contains("b"));
}

#[test]
fn list_projections_empty_catalog() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphListProjections {
        catalog: catalog.clone(),
    };
    let rows = proc.execute(&[], &g, None, None).unwrap();
    assert!(rows.is_empty());
}

// ── WCC ─────────────────────────────────────────────────────────

#[test]
fn wcc_connected_graph_single_component() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphWcc {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("wcc_test"))],
            &g,
            None,
            None,
        )
        .unwrap();
    assert_eq!(rows.len(), 4);

    // All nodes should have the same component ID
    let components: HashSet<i64> = rows
        .iter()
        .map(|r| match &r[1].1 {
            GqlValue::Int(v) => *v,
            _ => panic!("expected Int"),
        })
        .collect();
    assert_eq!(components.len(), 1);
}

#[test]
fn wcc_disconnected_graph_multiple_components() {
    let g = disconnected_graph();
    let catalog = new_shared_catalog();
    let proc = GraphWcc {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("wcc_disc"))],
            &g,
            None,
            None,
        )
        .unwrap();
    assert_eq!(rows.len(), 5);

    let components: HashSet<i64> = rows
        .iter()
        .map(|r| match &r[1].1 {
            GqlValue::Int(v) => *v,
            _ => panic!("expected Int"),
        })
        .collect();
    // Expect 3 components: {1,2}, {3,4}, {5}
    assert_eq!(components.len(), 3);
}

#[test]
fn wcc_empty_graph() {
    let g = empty_graph();
    let catalog = new_shared_catalog();
    let proc = GraphWcc {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("wcc_empty"))],
            &g,
            None,
            None,
        )
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn wcc_auto_builds_projection_if_missing() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphWcc {
        catalog: catalog.clone(),
    };
    // Name does not exist in catalog yet, should auto-build
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("auto_wcc"))],
            &g,
            None,
            None,
        )
        .unwrap();
    assert_eq!(rows.len(), 4);
    // Verify projection was created
    assert!(catalog.read().contains("auto_wcc"));
}

// ── SCC ─────────────────────────────────────────────────────────

#[test]
fn scc_cycle_graph_single_component() {
    let g = cycle_graph();
    let catalog = new_shared_catalog();
    let proc = GraphScc {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("scc_cycle"))],
            &g,
            None,
            None,
        )
        .unwrap();
    assert_eq!(rows.len(), 3);

    let components: HashSet<i64> = rows
        .iter()
        .map(|r| match &r[1].1 {
            GqlValue::Int(v) => *v,
            _ => panic!("expected Int"),
        })
        .collect();
    assert_eq!(components.len(), 1);
}

#[test]
fn scc_dag_all_singleton_components() {
    let g = dag_graph();
    let catalog = new_shared_catalog();
    let proc = GraphScc {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(&[GqlValue::String(SmolStr::new("scc_dag"))], &g, None, None)
        .unwrap();
    assert_eq!(rows.len(), 4);

    // Each node in a DAG is its own SCC
    let components: HashSet<i64> = rows
        .iter()
        .map(|r| match &r[1].1 {
            GqlValue::Int(v) => *v,
            _ => panic!("expected Int"),
        })
        .collect();
    assert_eq!(components.len(), 4);
}

// ── Topological sort ────────────────────────────────────────────

#[test]
fn topo_sort_dag_returns_valid_ordering() {
    let g = dag_graph();
    let catalog = new_shared_catalog();
    let proc = GraphTopoSort {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("topo_dag"))],
            &g,
            None,
            None,
        )
        .unwrap();
    assert_eq!(rows.len(), 4);

    // Build node -> position map
    let positions: HashMap<i64, i64> = rows
        .iter()
        .map(|r| {
            let nid = match &r[0].1 {
                GqlValue::Int(v) => *v,
                _ => panic!("expected Int"),
            };
            let pos = match &r[1].1 {
                GqlValue::Int(v) => *v,
                _ => panic!("expected Int"),
            };
            (nid, pos)
        })
        .collect();

    // In DAG: 1 -> 2, 1 -> 3, 2 -> 4, 3 -> 4
    // Node 1 must come before 2 and 3; 2 and 3 before 4
    assert!(positions[&1] < positions[&2]);
    assert!(positions[&1] < positions[&3]);
    assert!(positions[&2] < positions[&4]);
    assert!(positions[&3] < positions[&4]);
}

#[test]
fn topo_sort_cyclic_graph_errors() {
    let g = cycle_graph();
    let catalog = new_shared_catalog();
    let proc = GraphTopoSort {
        catalog: catalog.clone(),
    };
    let result = proc.execute(
        &[GqlValue::String(SmolStr::new("topo_cycle"))],
        &g,
        None,
        None,
    );
    assert!(result.is_err());
}

// ── Articulation points ─────────────────────────────────────────

#[test]
fn articulation_points_chain_identifies_cut_vertices() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphArticulationPoints {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("ap_chain"))],
            &g,
            None,
            None,
        )
        .unwrap();
    // In chain 1->2->3->4: nodes 2 and 3 are cut vertices
    let ap_ids: HashSet<i64> = rows
        .iter()
        .map(|r| match &r[0].1 {
            GqlValue::Int(v) => *v,
            _ => panic!("expected Int"),
        })
        .collect();
    assert!(ap_ids.contains(&2));
    assert!(ap_ids.contains(&3));
}

#[test]
fn articulation_points_triangle_has_none() {
    let g = triangle_graph();
    let catalog = new_shared_catalog();
    let proc = GraphArticulationPoints {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(&[GqlValue::String(SmolStr::new("ap_tri"))], &g, None, None)
        .unwrap();
    // A triangle has no articulation points
    assert!(rows.is_empty());
}

// ── Bridges ─────────────────────────────────────────────────────

#[test]
fn bridges_chain_identifies_bridge_edges() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphBridges {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("br_chain"))],
            &g,
            None,
            None,
        )
        .unwrap();
    // All edges in a chain are bridges
    assert_eq!(rows.len(), 3);
}

#[test]
fn bridges_triangle_has_none() {
    let g = triangle_graph();
    let catalog = new_shared_catalog();
    let proc = GraphBridges {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(&[GqlValue::String(SmolStr::new("br_tri"))], &g, None, None)
        .unwrap();
    assert!(rows.is_empty());
}

// ── Validate ────────────────────────────────────────────────────

#[test]
fn validate_clean_graph_no_issues() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphValidate {
        catalog: catalog.clone(),
    };
    let rows = proc
        .execute(
            &[GqlValue::String(SmolStr::new("val_clean"))],
            &g,
            None,
            None,
        )
        .unwrap();
    assert!(rows.is_empty());
}

// ── isAncestor ──────────────────────────────────────────────────

#[test]
fn is_ancestor_true_for_direct_parent() {
    let g = containment_graph();
    let proc = GraphIsAncestor;
    let args = vec![GqlValue::Int(1), GqlValue::Int(2)];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, GqlValue::Bool(true));
}

#[test]
fn is_ancestor_true_for_transitive_ancestor() {
    let g = containment_graph();
    let proc = GraphIsAncestor;
    // 1 -contains-> 2 -contains-> 3, so 1 is ancestor of 3
    let args = vec![GqlValue::Int(1), GqlValue::Int(3)];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows[0][0].1, GqlValue::Bool(true));
}

#[test]
fn is_ancestor_false_for_non_ancestor() {
    let g = containment_graph();
    let proc = GraphIsAncestor;
    // 4 has no containment relationship to 3
    let args = vec![GqlValue::Int(4), GqlValue::Int(3)];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows[0][0].1, GqlValue::Bool(false));
}

#[test]
fn is_ancestor_false_reverse_direction() {
    let g = containment_graph();
    let proc = GraphIsAncestor;
    // 3 is not an ancestor of 1
    let args = vec![GqlValue::Int(3), GqlValue::Int(1)];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows[0][0].1, GqlValue::Bool(false));
}

#[test]
fn is_ancestor_missing_args_errors() {
    let g = containment_graph();
    let proc = GraphIsAncestor;
    // Missing descendant argument
    let result = proc.execute(&[GqlValue::Int(1)], &g, None, None);
    assert!(result.is_err());
    // No arguments at all
    let result = proc.execute(&[], &g, None, None);
    assert!(result.is_err());
}

// ── PageRank ────────────────────────────────────────────────────

#[test]
fn pagerank_star_hub_highest_rank() {
    let g = star_graph();
    let catalog = new_shared_catalog();
    let proc = GraphPagerank {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("pr_star"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 5);

    // Find the hub node (1) and verify it has the highest score
    let scores: HashMap<i64, f64> = rows
        .iter()
        .map(|r| {
            let nid = match &r[0].1 {
                GqlValue::Int(v) => *v,
                _ => panic!("expected Int"),
            };
            let score = match &r[1].1 {
                GqlValue::Float(v) => *v,
                _ => panic!("expected Float"),
            };
            (nid, score)
        })
        .collect();

    let hub_score = scores[&1];
    for (&nid, &score) in &scores {
        if nid != 1 {
            assert!(hub_score >= score, "hub should have highest PageRank");
        }
    }
}

#[test]
fn pagerank_with_custom_damping_and_iterations() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphPagerank {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("pr_custom")),
        GqlValue::Float(0.9),
        GqlValue::Int(50),
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 4);

    // All scores should be positive
    for row in &rows {
        match &row[1].1 {
            GqlValue::Float(v) => assert!(*v > 0.0),
            _ => panic!("expected Float"),
        }
    }
}

// ── Betweenness ─────────────────────────────────────────────────

#[test]
fn betweenness_chain_middle_node_highest() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphBetweenness {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("btwn_chain"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 4);

    let scores: HashMap<i64, f64> = rows
        .iter()
        .map(|r| {
            let nid = match &r[0].1 {
                GqlValue::Int(v) => *v,
                _ => panic!("expected Int"),
            };
            let score = match &r[1].1 {
                GqlValue::Float(v) => *v,
                _ => panic!("expected Float"),
            };
            (nid, score)
        })
        .collect();

    // In chain 1->2->3->4, middle nodes 2 and 3 have highest betweenness
    // Endpoints 1 and 4 have lowest
    assert!(scores[&2] >= scores[&1]);
    assert!(scores[&3] >= scores[&4]);
}

#[test]
fn betweenness_with_sample_size() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphBetweenness {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("btwn_sample")),
        GqlValue::Int(2),
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 4);
}

// ── Shortest path ───────────────────────────────────────────────

#[test]
fn shortest_path_finds_path() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphShortestPath {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("sp_chain")),
        GqlValue::Int(1),
        GqlValue::Int(4),
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert!(!rows.is_empty());

    // Path should include source and target
    let node_ids: Vec<i64> = rows
        .iter()
        .map(|r| match &r[0].1 {
            GqlValue::Int(v) => *v,
            _ => panic!("expected Int"),
        })
        .collect();
    assert_eq!(*node_ids.first().unwrap(), 1);
    assert_eq!(*node_ids.last().unwrap(), 4);
}

#[test]
fn shortest_path_unreachable_returns_empty() {
    let g = disconnected_graph();
    let catalog = new_shared_catalog();
    let proc = GraphShortestPath {
        catalog: catalog.clone(),
    };
    // Node 1 and 5 are in different components
    let args = vec![
        GqlValue::String(SmolStr::new("sp_disc")),
        GqlValue::Int(1),
        GqlValue::Int(5),
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn shortest_path_same_node_returns_single() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphShortestPath {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("sp_self")),
        GqlValue::Int(1),
        GqlValue::Int(1),
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    // Path from node to itself should be a single node
    assert_eq!(rows.len(), 1);
}

#[test]
fn shortest_path_missing_args_errors() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphShortestPath {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("sp_err")),
        GqlValue::Int(1),
        // Missing 'to' argument
    ];
    let result = proc.execute(&args, &g, None, None);
    assert!(result.is_err());
}

// ── SSSP ────────────────────────────────────────────────────────

#[test]
fn sssp_chain_distances_increase() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphSssp {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("sssp_chain")),
        GqlValue::Int(1),
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();

    let distances: HashMap<i64, f64> = rows
        .iter()
        .map(|r| {
            let nid = match &r[0].1 {
                GqlValue::Int(v) => *v,
                _ => panic!("expected Int"),
            };
            let dist = match &r[1].1 {
                GqlValue::Float(v) => *v,
                _ => panic!("expected Float"),
            };
            (nid, dist)
        })
        .collect();

    // Source node should have distance 0
    assert_eq!(distances[&1], 0.0);
    // Distances should increase along the chain
    assert!(distances[&2] < distances[&3]);
    assert!(distances[&3] < distances[&4]);
}

// ── APSP ────────────────────────────────────────────────────────

#[test]
fn apsp_chain_returns_all_pairs() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphApsp {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("apsp_chain"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    // For 4 nodes, we get up to 4*4 = 16 pairs, but only reachable ones
    assert!(!rows.is_empty());
}

// ── Label propagation ───────────────────────────────────────────

#[test]
fn label_propagation_assigns_communities() {
    let g = disconnected_graph();
    let catalog = new_shared_catalog();
    let proc = GraphLabelPropagation {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("lp_disc"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 5);

    // Each node should have a community ID
    for row in &rows {
        match &row[1].1 {
            GqlValue::Int(_) => {}
            other => panic!("expected Int communityId, got {other:?}"),
        }
    }
}

#[test]
fn label_propagation_with_max_iter() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphLabelPropagation {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("lp_iter")), GqlValue::Int(5)];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 4);
}

// ── Louvain ─────────────────────────────────────────────────────

#[test]
fn louvain_detects_communities() {
    let g = disconnected_graph();
    let catalog = new_shared_catalog();
    let proc = GraphLouvain {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("louv_disc"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows.len(), 5);

    // Each row should have nodeId, communityId, and level
    for row in &rows {
        assert_eq!(row.len(), 3);
        match (&row[0].1, &row[1].1, &row[2].1) {
            (GqlValue::Int(_), GqlValue::Int(_), GqlValue::Int(_)) => {}
            _ => panic!("expected (Int, Int, Int) row"),
        }
    }
}

// ── Triangle count ──────────────────────────────────────────────

#[test]
fn triangle_count_on_triangle() {
    let g = triangle_graph();
    let catalog = new_shared_catalog();
    let proc = GraphTriangleCount {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("tc_tri"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();

    // In a triangle, each node participates in 1 triangle
    let total_triangles: i64 = rows
        .iter()
        .map(|r| match &r[1].1 {
            GqlValue::Int(v) => *v,
            _ => panic!("expected Int"),
        })
        .sum();
    // Sum of per-node counts = 3 * num_triangles (each triangle counted once per vertex)
    assert!(total_triangles >= 3);
}

#[test]
fn triangle_count_no_triangles() {
    let g = chain_graph();
    let catalog = new_shared_catalog();
    let proc = GraphTriangleCount {
        catalog: catalog.clone(),
    };
    let args = vec![GqlValue::String(SmolStr::new("tc_chain"))];
    let rows = proc.execute(&args, &g, None, None).unwrap();

    // A chain has no triangles
    let total: i64 = rows
        .iter()
        .map(|r| match &r[1].1 {
            GqlValue::Int(v) => *v,
            _ => 0,
        })
        .sum();
    assert_eq!(total, 0);
}

// ── Weighted projection ─────────────────────────────────────────

#[test]
fn project_with_weight_property() {
    let g = weighted_chain();
    let catalog = new_shared_catalog();
    let proc = GraphProject {
        catalog: catalog.clone(),
    };
    let args = vec![
        GqlValue::String(SmolStr::new("weighted")),
        GqlValue::Null,
        GqlValue::Null,
        GqlValue::String(SmolStr::new("cost")),
    ];
    let rows = proc.execute(&args, &g, None, None).unwrap();
    assert_eq!(rows[0][1].1, GqlValue::Int(3)); // 3 nodes
    assert_eq!(rows[0][2].1, GqlValue::Int(2)); // 2 edges
}
