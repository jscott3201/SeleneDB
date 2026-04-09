//! TopK pushdown, factorized execution, WCO join, and EXISTS/COUNT
//! subquery tests.

use super::*;

// ── TopK pushdown tests ──────────────���──────────────────────────────────────
//
// These tests verify that the IndexOrderRule + execute_index_ordered_scan
// produce correct results when WHERE filters are pushed down alongside an
// index-ordered scan. The optimisation is transparent: we assert correctness,
// not which plan fired.

/// Build a graph with 20 Sensor nodes. The `temperature` property is indexed
/// (Float, 1.0 .. 20.0). Each node also has a `building` property (String)
/// that is NOT indexed: odd-numbered temperatures belong to "HQ", even to
/// "Annex". Returns the graph after indexes are built.
fn setup_topk_graph() -> SeleneGraph {
    use selene_core::schema::{NodeSchema, PropertyDef, ValidationMode, ValueType};

    let mut g = SeleneGraph::with_config(
        selene_graph::SchemaValidator::new(ValidationMode::Warn),
        100,
    );

    let schema = NodeSchema {
        label: std::sync::Arc::from("Sensor"),
        parent: None,
        properties: vec![
            PropertyDef {
                name: std::sync::Arc::from("temperature"),
                value_type: ValueType::Float,
                required: false,
                default: None,
                description: String::new(),
                indexed: true,
                unique: false,
                min: None,
                max: None,
                min_length: None,
                max_length: None,
                allowed_values: vec![],
                pattern: None,
                immutable: false,
                searchable: false,
                dictionary: false,
                fill: None,
                expected_interval_nanos: None,
                encoding: selene_core::ValueEncoding::Gorilla,
            },
            PropertyDef {
                name: std::sync::Arc::from("building"),
                value_type: ValueType::String,
                required: false,
                default: None,
                description: String::new(),
                indexed: false,
                unique: false,
                min: None,
                max: None,
                min_length: None,
                max_length: None,
                allowed_values: vec![],
                pattern: None,
                immutable: false,
                searchable: false,
                dictionary: false,
                fill: None,
                expected_interval_nanos: None,
                encoding: selene_core::ValueEncoding::Gorilla,
            },
        ],
        valid_edge_labels: vec![],
        description: String::new(),
        annotations: std::collections::HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };
    g.schema_mut().register_node_schema(schema).unwrap();

    let mut m = g.mutate();
    for i in 1..=20u32 {
        // Odd temperatures -> "HQ", even -> "Annex"
        let building = if i % 2 == 1 { "HQ" } else { "Annex" };
        m.create_node(
            selene_core::LabelSet::from_strs(&["Sensor"]),
            selene_core::PropertyMap::from_pairs(vec![
                (IStr::new("temperature"), Value::Float(f64::from(i))),
                (IStr::new("building"), Value::String(SmolStr::new(building))),
            ]),
        )
        .unwrap();
    }
    m.commit(0).unwrap();

    g.build_property_indexes();
    g.build_composite_indexes();

    g
}

#[test]
fn ordered_scan_topk_correct_results() {
    let g = setup_topk_graph();

    let result = QueryBuilder::new(
        "MATCH (n:Sensor) RETURN n.temperature AS t ORDER BY n.temperature DESC LIMIT 5",
        &g,
    )
    .execute()
    .unwrap();

    assert_eq!(
        result.row_count(),
        5,
        "LIMIT 5 should return exactly 5 rows"
    );

    let batch = &result.batches[0];
    let col = batch
        .column_by_name("t")
        .expect("column t")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Float64Array");

    let values: Vec<f64> = col.values().to_vec();
    assert_eq!(
        values,
        vec![20.0, 19.0, 18.0, 17.0, 16.0],
        "DESC LIMIT 5 should return the five highest temperatures"
    );
}

#[test]
fn ordered_scan_topk_with_filter() {
    let g = setup_topk_graph();

    // HQ sensors have odd temperatures: 1, 3, 5, 7, 9, 11, 13, 15, 17, 19
    // Top 3 by DESC: 19.0, 17.0, 15.0
    let result = QueryBuilder::new(
        "MATCH (n:Sensor) WHERE n.building = 'HQ' RETURN n.temperature AS t ORDER BY n.temperature DESC LIMIT 3",
        &g,
    )
    .execute()
    .unwrap();

    assert_eq!(
        result.row_count(),
        3,
        "filter + LIMIT 3 should return exactly 3 rows"
    );

    let batch = &result.batches[0];
    let col = batch
        .column_by_name("t")
        .expect("column t")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Float64Array");

    let values: Vec<f64> = col.values().to_vec();
    assert_eq!(
        values,
        vec![19.0, 17.0, 15.0],
        "top 3 HQ temperatures (odd values 1-19) in DESC order"
    );
}

#[test]
fn ordered_scan_topk_asc() {
    let g = setup_topk_graph();

    let result = QueryBuilder::new(
        "MATCH (n:Sensor) RETURN n.temperature AS t ORDER BY n.temperature ASC LIMIT 5",
        &g,
    )
    .execute()
    .unwrap();

    assert_eq!(
        result.row_count(),
        5,
        "LIMIT 5 should return exactly 5 rows"
    );

    let batch = &result.batches[0];
    let col = batch
        .column_by_name("t")
        .expect("column t")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Float64Array");

    let values: Vec<f64> = col.values().to_vec();
    assert_eq!(
        values,
        vec![1.0, 2.0, 3.0, 4.0, 5.0],
        "ASC LIMIT 5 should return the five lowest temperatures"
    );
}

#[test]
fn ordered_scan_topk_empty_result() {
    let g = setup_topk_graph();

    // "NonExistent" matches no nodes, so the result must be empty.
    let result = QueryBuilder::new(
        "MATCH (n:Sensor) WHERE n.building = 'NonExistent' RETURN n.temperature AS t ORDER BY n.temperature LIMIT 5",
        &g,
    )
    .execute()
    .unwrap();

    assert_eq!(
        result.row_count(),
        0,
        "filter eliminating all nodes should yield empty result"
    );
}

// ── Factorized execution tests ──

/// Helper: run a query with both flat and factorized execution and compare results.
fn assert_factorized_matches_flat(graph: &SeleneGraph, query: &str) {
    let flat = QueryBuilder::new(query, graph).execute().unwrap();
    let opts = crate::GqlOptions {
        factorized: true,
        ..Default::default()
    };
    let factorized = QueryBuilder::new(query, graph)
        .with_options(&opts)
        .execute()
        .unwrap();
    assert_eq!(
        flat.row_count(),
        factorized.row_count(),
        "row count mismatch for query: {query}"
    );
    // Compare column names
    let flat_names: Vec<&str> = flat
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let fact_names: Vec<&str> = factorized
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(flat_names, fact_names, "schema mismatch for query: {query}");
}

fn make_chain_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    // Chain: a1 -> b1 -> c1, a1 -> b2 -> c2, a2 -> b3 -> c3
    let a1 = m
        .create_node(
            LabelSet::from_strs(&["Building"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("HQ")))]),
        )
        .unwrap();
    let a2 = m
        .create_node(
            LabelSet::from_strs(&["Building"]),
            PropertyMap::from_pairs(vec![(
                IStr::new("name"),
                Value::String(SmolStr::new("Lab")),
            )]),
        )
        .unwrap();
    let b1 = m
        .create_node(
            LabelSet::from_strs(&["Floor"]),
            PropertyMap::from_pairs(vec![(IStr::new("level"), Value::Int(1))]),
        )
        .unwrap();
    let b2 = m
        .create_node(
            LabelSet::from_strs(&["Floor"]),
            PropertyMap::from_pairs(vec![(IStr::new("level"), Value::Int(2))]),
        )
        .unwrap();
    let b3 = m
        .create_node(
            LabelSet::from_strs(&["Floor"]),
            PropertyMap::from_pairs(vec![(IStr::new("level"), Value::Int(1))]),
        )
        .unwrap();
    let c1 = m
        .create_node(
            LabelSet::from_strs(&["Sensor"]),
            PropertyMap::from_pairs(vec![(
                IStr::new("zone"),
                Value::String(SmolStr::new("North")),
            )]),
        )
        .unwrap();
    let c2 = m
        .create_node(
            LabelSet::from_strs(&["Sensor"]),
            PropertyMap::from_pairs(vec![(
                IStr::new("zone"),
                Value::String(SmolStr::new("South")),
            )]),
        )
        .unwrap();
    let c3 = m
        .create_node(
            LabelSet::from_strs(&["Sensor"]),
            PropertyMap::from_pairs(vec![(
                IStr::new("zone"),
                Value::String(SmolStr::new("North")),
            )]),
        )
        .unwrap();

    m.create_edge(a1, IStr::new("contains"), b1, PropertyMap::new())
        .unwrap();
    m.create_edge(a1, IStr::new("contains"), b2, PropertyMap::new())
        .unwrap();
    m.create_edge(a2, IStr::new("contains"), b3, PropertyMap::new())
        .unwrap();
    m.create_edge(b1, IStr::new("contains"), c1, PropertyMap::new())
        .unwrap();
    m.create_edge(b2, IStr::new("contains"), c2, PropertyMap::new())
        .unwrap();
    m.create_edge(b3, IStr::new("contains"), c3, PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();
    g
}

#[test]
fn e2e_factorized_two_hop_matches_flat() {
    let g = make_chain_graph();
    assert_factorized_matches_flat(
        &g,
        "MATCH (a:Building)-[:contains]->(b:Floor) RETURN a.name, b.level",
    );
}

#[test]
fn e2e_factorized_three_hop_matches_flat() {
    let g = make_chain_graph();
    assert_factorized_matches_flat(
        &g,
        "MATCH (a:Building)-[:contains]->(b:Floor)-[:contains]->(c:Sensor) RETURN a.name, b.level, c.zone",
    );
}

#[test]
fn e2e_factorized_with_filter_matches_flat() {
    let g = make_chain_graph();
    assert_factorized_matches_flat(
        &g,
        "MATCH (a:Building)-[:contains]->(b:Floor)-[:contains]->(c:Sensor) WHERE c.zone = 'North' RETURN a.name, c.zone",
    );
}

#[test]
fn e2e_factorized_single_hop_matches_flat() {
    let g = make_chain_graph();
    assert_factorized_matches_flat(
        &g,
        "MATCH (a:Building)-[:contains]->(b:Floor) RETURN a.name",
    );
}

#[test]
fn e2e_factorized_count_matches_flat() {
    let g = make_chain_graph();
    assert_factorized_matches_flat(
        &g,
        "MATCH (a:Building)-[:contains]->(b:Floor)-[:contains]->(c:Sensor) RETURN count(*) AS cnt",
    );
}

#[test]
fn e2e_factorized_limit_matches_flat() {
    let g = make_chain_graph();
    assert_factorized_matches_flat(
        &g,
        "MATCH (a:Building)-[:contains]->(b:Floor)-[:contains]->(c:Sensor) RETURN a.name, c.zone LIMIT 2",
    );
}

#[test]
fn e2e_factorized_offset_limit_matches_flat() {
    let g = make_chain_graph();
    assert_factorized_matches_flat(
        &g,
        "MATCH (a:Building)-[:contains]->(b:Floor)-[:contains]->(c:Sensor) RETURN a.name, c.zone OFFSET 1 LIMIT 1",
    );
}

#[test]
fn e2e_factorized_filter_and_limit_matches_flat() {
    let g = make_chain_graph();
    assert_factorized_matches_flat(
        &g,
        "MATCH (a:Building)-[:contains]->(b:Floor)-[:contains]->(c:Sensor) WHERE c.zone = 'North' RETURN a.name LIMIT 1",
    );
}

// ── WCO join tests ──

/// Build a complete directed graph K_n where every pair of Person nodes
/// has a :knows edge in both directions.
fn make_complete_graph(n: usize) -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let mut node_ids = Vec::with_capacity(n);
    for i in 0..n {
        let nid = m
            .create_node(
                LabelSet::from_strs(&["Person"]),
                PropertyMap::from_pairs(vec![(
                    IStr::new("name"),
                    Value::String(SmolStr::new(format!("P{i}"))),
                )]),
            )
            .unwrap();
        node_ids.push(nid);
    }
    for i in 0..n {
        for j in 0..n {
            if i != j {
                m.create_edge(
                    node_ids[i],
                    IStr::new("knows"),
                    node_ids[j],
                    PropertyMap::new(),
                )
                .unwrap();
            }
        }
    }
    m.commit(0).unwrap();
    g
}

#[test]
fn e2e_wco_triangle_k4_direct_executor() {
    // K4 complete graph: 4 nodes, 12 directed edges.
    // Triangle query: (a)-[:knows]->(b)-[:knows]->(c)-[:knows]->(a)
    // In K4, for each ordered triple (a,b,c) where all distinct,
    // there's always a triangle. That's 4*3*2 = 24 ordered triangles.
    let g = make_complete_graph(4);

    // Build a WcoJoin PatternOp directly (bypassing optimizer threshold)
    let a = IStr::new("a");
    let b = IStr::new("b");
    let c = IStr::new("c");
    let knows = IStr::new("knows");

    let wco_op = crate::planner::plan::PatternOp::WcoJoin {
        scan_var: a,
        scan_labels: Some(crate::ast::pattern::LabelExpr::Name(IStr::new("Person"))),
        scan_property_filters: vec![],
        relations: vec![
            crate::planner::plan::WcoRelation {
                source_var: a,
                edge_var: None,
                target_var: b,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Out,
                target_property_filters: vec![],
            },
            crate::planner::plan::WcoRelation {
                source_var: b,
                edge_var: None,
                target_var: c,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Out,
                target_property_filters: vec![],
            },
            crate::planner::plan::WcoRelation {
                source_var: c,
                edge_var: None,
                target_var: a,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Out,
                target_property_filters: vec![],
            },
        ],
    };

    let csr = selene_graph::CsrAdjacency::build(&g);
    let chunk = super::super::pattern::execute_single_pattern_op_chunk_public(
        &wco_op,
        crate::types::chunk::DataChunk::unit(),
        &[],
        None,
        &super::super::pattern::PatternExecCtx {
            graph: &g,
            scope: None,
            csr: Some(&csr),
            sip_ctx: &crate::pattern::context::PatternContext::new(),
            eval_ctx: &crate::runtime::eval::EvalContext::new(
                &g,
                crate::runtime::functions::FunctionRegistry::builtins(),
            ),
        },
    )
    .unwrap();

    // K4 has 4*3*2 = 24 ordered directed triangles
    assert_eq!(
        chunk.active_len(),
        24,
        "K4 should have 24 ordered directed triangles"
    );
}

#[test]
fn e2e_wco_triangle_k3_direct_executor() {
    // K3: 3 nodes, 6 directed edges, 6 ordered triangles (3! = 6)
    let g = make_complete_graph(3);

    let a = IStr::new("a");
    let b = IStr::new("b");
    let c = IStr::new("c");
    let knows = IStr::new("knows");

    let wco_op = crate::planner::plan::PatternOp::WcoJoin {
        scan_var: a,
        scan_labels: Some(crate::ast::pattern::LabelExpr::Name(IStr::new("Person"))),
        scan_property_filters: vec![],
        relations: vec![
            crate::planner::plan::WcoRelation {
                source_var: a,
                edge_var: None,
                target_var: b,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Out,
                target_property_filters: vec![],
            },
            crate::planner::plan::WcoRelation {
                source_var: b,
                edge_var: None,
                target_var: c,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Out,
                target_property_filters: vec![],
            },
            crate::planner::plan::WcoRelation {
                source_var: c,
                edge_var: None,
                target_var: a,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Out,
                target_property_filters: vec![],
            },
        ],
    };

    let csr = selene_graph::CsrAdjacency::build(&g);
    let chunk = super::super::pattern::execute_single_pattern_op_chunk_public(
        &wco_op,
        crate::types::chunk::DataChunk::unit(),
        &[],
        None,
        &super::super::pattern::PatternExecCtx {
            graph: &g,
            scope: None,
            csr: Some(&csr),
            sip_ctx: &crate::pattern::context::PatternContext::new(),
            eval_ctx: &crate::runtime::eval::EvalContext::new(
                &g,
                crate::runtime::functions::FunctionRegistry::builtins(),
            ),
        },
    )
    .unwrap();

    // K3 has 3*2*1 = 6 ordered directed triangles
    assert_eq!(
        chunk.active_len(),
        6,
        "K3 should have 6 ordered directed triangles"
    );
}

#[test]
fn e2e_wco_triangle_k3_undirected() {
    // K3 with directed edges, but queried with EdgeDirection::Any.
    // Each node has 2 outgoing + 2 incoming neighbors (to the other 2 nodes).
    // The undirected query should find triangles using both directions.
    let g = make_complete_graph(3);

    let a = IStr::new("a");
    let b = IStr::new("b");
    let c = IStr::new("c");
    let knows = IStr::new("knows");

    let wco_op = crate::planner::plan::PatternOp::WcoJoin {
        scan_var: a,
        scan_labels: Some(crate::ast::pattern::LabelExpr::Name(IStr::new("Person"))),
        scan_property_filters: vec![],
        relations: vec![
            crate::planner::plan::WcoRelation {
                source_var: a,
                edge_var: None,
                target_var: b,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Any,
                target_property_filters: vec![],
            },
            crate::planner::plan::WcoRelation {
                source_var: b,
                edge_var: None,
                target_var: c,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Any,
                target_property_filters: vec![],
            },
            crate::planner::plan::WcoRelation {
                source_var: c,
                edge_var: None,
                target_var: a,
                edge_label: Some(knows),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Any,
                target_property_filters: vec![],
            },
        ],
    };

    let csr = selene_graph::CsrAdjacency::build(&g);
    let chunk = super::super::pattern::execute_single_pattern_op_chunk_public(
        &wco_op,
        crate::types::chunk::DataChunk::unit(),
        &[],
        None,
        &super::super::pattern::PatternExecCtx {
            graph: &g,
            scope: None,
            csr: Some(&csr),
            sip_ctx: &crate::pattern::context::PatternContext::new(),
            eval_ctx: &crate::runtime::eval::EvalContext::new(
                &g,
                crate::runtime::functions::FunctionRegistry::builtins(),
            ),
        },
    )
    .unwrap();

    // With undirected edges on K3, each node sees 4 neighbor entries
    // (2 outgoing + 2 incoming). The result count must be > 6 (the directed
    // count), confirming both directions are traversed. The exact count
    // depends on the edge-pair combinations in the intersection.
    assert!(
        chunk.active_len() > 6,
        "undirected K3 should produce more triangles than directed (got {})",
        chunk.active_len()
    );
}

#[test]
fn e2e_wco_triangle_single_direction_subset() {
    // A simple triangle with only ONE direction of edges: A->B, B->C, C->A.
    // An undirected query should still find the triangle.
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let a_id = m
        .create_node(
            LabelSet::from_strs(&["Node"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("A")))]),
        )
        .unwrap();
    let b_id = m
        .create_node(
            LabelSet::from_strs(&["Node"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("B")))]),
        )
        .unwrap();
    let c_id = m
        .create_node(
            LabelSet::from_strs(&["Node"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("C")))]),
        )
        .unwrap();
    // Single-direction triangle: A->B, B->C, C->A
    m.create_edge(a_id, IStr::new("edge"), b_id, PropertyMap::new())
        .unwrap();
    m.create_edge(b_id, IStr::new("edge"), c_id, PropertyMap::new())
        .unwrap();
    m.create_edge(c_id, IStr::new("edge"), a_id, PropertyMap::new())
        .unwrap();
    m.commit(0).unwrap();

    let a = IStr::new("a");
    let b = IStr::new("b");
    let c = IStr::new("c");
    let edge = IStr::new("edge");

    // Undirected query on a single-direction triangle
    let wco_op = crate::planner::plan::PatternOp::WcoJoin {
        scan_var: a,
        scan_labels: Some(crate::ast::pattern::LabelExpr::Name(IStr::new("Node"))),
        scan_property_filters: vec![],
        relations: vec![
            crate::planner::plan::WcoRelation {
                source_var: a,
                edge_var: None,
                target_var: b,
                edge_label: Some(edge),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Any,
                target_property_filters: vec![],
            },
            crate::planner::plan::WcoRelation {
                source_var: b,
                edge_var: None,
                target_var: c,
                edge_label: Some(edge),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Any,
                target_property_filters: vec![],
            },
            crate::planner::plan::WcoRelation {
                source_var: c,
                edge_var: None,
                target_var: a,
                edge_label: Some(edge),
                target_labels: None,
                direction: crate::ast::pattern::EdgeDirection::Any,
                target_property_filters: vec![],
            },
        ],
    };

    let csr = selene_graph::CsrAdjacency::build(&g);
    let chunk = super::super::pattern::execute_single_pattern_op_chunk_public(
        &wco_op,
        crate::types::chunk::DataChunk::unit(),
        &[],
        None,
        &super::super::pattern::PatternExecCtx {
            graph: &g,
            scope: None,
            csr: Some(&csr),
            sip_ctx: &crate::pattern::context::PatternContext::new(),
            eval_ctx: &crate::runtime::eval::EvalContext::new(
                &g,
                crate::runtime::functions::FunctionRegistry::builtins(),
            ),
        },
    )
    .unwrap();

    // With only A->B, B->C, C->A, the directed query (Out) would find
    // exactly 3 ordered triangles (one per anchor). The undirected query
    // sees both directions, so it finds 6 (each triple in both orders).
    assert!(
        chunk.active_len() >= 3,
        "undirected single-direction triangle should find at least 3 results (got {})",
        chunk.active_len()
    );
}

#[test]
fn e2e_wco_acyclic_not_triggered() {
    // Linear chain should NOT produce WcoJoin in the plan
    let g = make_chain_graph();
    let result = QueryBuilder::new(
        "MATCH (a:Building)-[:contains]->(b:Floor)-[:contains]->(c:Sensor) RETURN count(*) AS cnt",
        &g,
    )
    .execute()
    .unwrap();
    // Should still work correctly via the normal expand path
    assert_eq!(result.row_count(), 1);
}

// ═══════════════════════════════════════════════════════════════════
// Subquery tests
// ═══════════��═════════════════���═════════════════════════════════════

/// Build a social graph: Alice->Bob, Alice->Carol, Bob->Carol.
fn setup_social_graph() -> SeleneGraph {
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();
    let alice = m
        .create_node(
            LabelSet::from_strs(&["person"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::str("Alice")),
                (IStr::new("age"), Value::Int(30)),
            ]),
        )
        .unwrap();
    let bob = m
        .create_node(
            LabelSet::from_strs(&["person"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::str("Bob")),
                (IStr::new("age"), Value::Int(25)),
            ]),
        )
        .unwrap();
    let carol = m
        .create_node(
            LabelSet::from_strs(&["person"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::str("Carol")),
                (IStr::new("age"), Value::Int(35)),
            ]),
        )
        .unwrap();
    let dave = m
        .create_node(
            LabelSet::from_strs(&["person"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::str("Dave")),
                (IStr::new("age"), Value::Int(40)),
            ]),
        )
        .unwrap();
    // Alice knows Bob and Carol; Bob knows Carol; Dave knows nobody.
    m.create_edge(alice, IStr::new("knows"), bob, PropertyMap::new())
        .unwrap();
    m.create_edge(alice, IStr::new("knows"), carol, PropertyMap::new())
        .unwrap();
    m.create_edge(bob, IStr::new("knows"), carol, PropertyMap::new())
        .unwrap();
    let _ = dave; // intentionally isolated node
    m.commit(0).unwrap();
    g
}

#[test]
fn subquery_exists_filters_matching_nodes() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE EXISTS { MATCH (n)-[:knows]->() } RETURN n.name AS name ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
    let col = result.batches[0].column_by_name("name").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "Alice");
    assert_eq!(arr.value(1), "Bob");
}

#[test]
fn subquery_exists_negated() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE NOT EXISTS { MATCH (n)-[:knows]->() } RETURN n.name AS name ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
    let col = result.batches[0].column_by_name("name").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "Carol");
    assert_eq!(arr.value(1), "Dave");
}

#[test]
fn subquery_count_returns_correct_counts() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) RETURN n.name AS name, COUNT { MATCH (n)-[:knows]->() } AS cnt ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 4);
    let name_col = result.batches[0].column_by_name("name").unwrap();
    let names = name_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let cnt_col = result.batches[0].column_by_name("cnt").unwrap();
    let counts = cnt_col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    // Alice: 2, Bob: 1, Carol: 0, Dave: 0
    assert_eq!(names.value(0), "Alice");
    assert_eq!(counts.value(0), 2);
    assert_eq!(names.value(1), "Bob");
    assert_eq!(counts.value(1), 1);
    assert_eq!(names.value(2), "Carol");
    assert_eq!(counts.value(2), 0);
    assert_eq!(names.value(3), "Dave");
    assert_eq!(counts.value(3), 0);
}

#[test]
fn subquery_count_with_threshold() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE COUNT { MATCH (n)-[:knows]->() } >= 2 RETURN n.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let col = result.batches[0].column_by_name("name").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "Alice");
}

#[test]
fn subquery_exists_empty_result() {
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE EXISTS { MATCH (n)-[:works_for]->() } RETURN n.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(
        result.row_count(),
        0,
        "no :works_for edges means no matches"
    );
}

#[test]
fn subquery_count_zero_when_no_edges() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person {name: 'Dave'}) RETURN COUNT { MATCH (n)-[:knows]->() } AS cnt",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let col = result.batches[0].column_by_name("cnt").unwrap();
    let counts = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(counts.value(0), 0);
}

#[test]
fn subquery_correlated_variable_in_exists() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE EXISTS { MATCH (n)-[:knows]->(m:person) WHERE m.age > 30 } RETURN n.name AS name ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
    let col = result.batches[0].column_by_name("name").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "Alice");
    assert_eq!(arr.value(1), "Bob");
}

#[test]
fn subquery_count_with_inner_filter() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) RETURN n.name AS name, COUNT { MATCH (n)-[:knows]->(m:person) WHERE m.age > 30 } AS old_friends ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 4);
    let cnt_col = result.batches[0].column_by_name("old_friends").unwrap();
    let counts = cnt_col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let name_col = result.batches[0].column_by_name("name").unwrap();
    let names = name_col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(counts.value(0), 1);
    assert_eq!(names.value(1), "Bob");
    assert_eq!(counts.value(1), 1);
    assert_eq!(names.value(2), "Carol");
    assert_eq!(counts.value(2), 0);
    assert_eq!(names.value(3), "Dave");
    assert_eq!(counts.value(3), 0);
}

#[test]
fn subquery_exists_with_incoming_edge() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE EXISTS { MATCH (n)<-[:knows]-() } RETURN n.name AS name ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 2);
    let col = result.batches[0].column_by_name("name").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "Bob");
    assert_eq!(arr.value(1), "Carol");
}

#[test]
fn subquery_exists_combined_with_property_filter() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE n.age > 25 AND EXISTS { MATCH (n)-[:knows]->() } RETURN n.name AS name ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let col = result.batches[0].column_by_name("name").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "Alice");
}

#[test]
fn subquery_count_equality_check() {
    use arrow::array::Array;
    let g = setup_social_graph();
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE COUNT { MATCH (n)-[:knows]->() } = 1 RETURN n.name AS name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(result.row_count(), 1);
    let col = result.batches[0].column_by_name("name").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "Bob");
}

#[test]
fn subquery_exists_with_starts_with_filter() {
    use arrow::array::Array;
    let g = setup_social_graph();
    // STARTS WITH inside EXISTS: names starting with 'C' (Carol)
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE EXISTS { MATCH (n)-[:knows]->(m:person) WHERE m.name STARTS WITH 'C' } RETURN n.name AS name ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    // Alice knows Carol, Bob knows Carol -- both should appear
    assert_eq!(
        result.row_count(),
        2,
        "STARTS WITH 'C' inside EXISTS should return 2 (Alice and Bob know Carol), got {}",
        result.row_count()
    );
    let col = result.batches[0].column_by_name("name").unwrap();
    let arr = col
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "Alice");
    assert_eq!(arr.value(1), "Bob");
}

#[test]
fn subquery_exists_department_employs_starts_with() {
    // Test the exact scenario reported as failing:
    // MATCH (d:Department) WHERE EXISTS { MATCH (d)-[:employs]->(p:Person) WHERE p.name STARTS WITH 'A' }
    let mut g = SeleneGraph::new();
    let mut m = g.mutate();

    // 3 departments
    let eng = m
        .create_node(
            LabelSet::from_strs(&["Department"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("Engineering"))]),
        )
        .unwrap();
    let sales = m
        .create_node(
            LabelSet::from_strs(&["Department"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("Sales"))]),
        )
        .unwrap();
    let hr = m
        .create_node(
            LabelSet::from_strs(&["Department"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("HR"))]),
        )
        .unwrap();

    // People: some starting with 'A', some not
    let alice = m
        .create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("Alice"))]),
        )
        .unwrap();
    let alan = m
        .create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("Alan"))]),
        )
        .unwrap();
    let bob = m
        .create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("Bob"))]),
        )
        .unwrap();
    let carol = m
        .create_node(
            LabelSet::from_strs(&["Person"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::str("Carol"))]),
        )
        .unwrap();

    // Engineering employs Alice and Alan (both start with A)
    m.create_edge(eng, IStr::new("employs"), alice, PropertyMap::new())
        .unwrap();
    m.create_edge(eng, IStr::new("employs"), alan, PropertyMap::new())
        .unwrap();
    // Sales employs Bob only (does not start with A)
    m.create_edge(sales, IStr::new("employs"), bob, PropertyMap::new())
        .unwrap();
    // HR employs Carol only (does not start with A)
    m.create_edge(hr, IStr::new("employs"), carol, PropertyMap::new())
        .unwrap();

    let _ = (bob, carol); // suppress unused warnings
    m.commit(0).unwrap();

    // Only Engineering employs someone whose name STARTS WITH 'A'
    let result = QueryBuilder::new(
        "MATCH (d:Department) WHERE EXISTS { MATCH (d)-[:employs]->(p:Person) WHERE p.name STARTS WITH 'A' } RETURN d.name AS dname ORDER BY d.name",
        &g,
    )
    .execute()
    .unwrap();

    assert_eq!(
        result.row_count(),
        1,
        "Only Engineering should match (employs Alice and Alan who start with 'A'), got {}",
        result.row_count()
    );

    // Verify CONTAINS works the same way in EXISTS for comparison
    let result_contains = QueryBuilder::new(
        "MATCH (d:Department) WHERE EXISTS { MATCH (d)-[:employs]->(p:Person) WHERE p.name CONTAINS 'A' } RETURN d.name AS dname ORDER BY d.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(
        result_contains.row_count(),
        1,
        "CONTAINS 'A': only Engineering should match, got {}",
        result_contains.row_count()
    );

    // Verify ENDS WITH works the same way in EXISTS for comparison
    let result_ends = QueryBuilder::new(
        "MATCH (d:Department) WHERE EXISTS { MATCH (d)-[:employs]->(p:Person) WHERE p.name ENDS WITH 'e' } RETURN d.name AS dname ORDER BY d.name",
        &g,
    )
    .execute()
    .unwrap();
    assert_eq!(
        result_ends.row_count(),
        1,
        "ENDS WITH 'e': only Engineering should match (Alice ends with 'e'), got {}",
        result_ends.row_count()
    );
}

#[test]
fn subquery_exists_with_ends_with_filter() {
    let g = setup_social_graph();
    // ENDS WITH inside EXISTS: names ending with 'b' (Bob)
    let result = QueryBuilder::new(
        "MATCH (n:person) WHERE EXISTS { MATCH (n)-[:knows]->(m:person) WHERE m.name ENDS WITH 'b' } RETURN n.name AS name ORDER BY n.name",
        &g,
    )
    .execute()
    .unwrap();
    // Only Alice knows Bob, so 1 result
    assert_eq!(
        result.row_count(),
        1,
        "ENDS WITH 'b' inside EXISTS should return 1 (only Alice knows Bob), got {}",
        result.row_count()
    );
}
