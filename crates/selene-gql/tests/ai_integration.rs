//! AI MVP integration tests: cross-feature verification with synthetic vectors.
//!
//! Tests GraphRAG data flow, agent memory structure, community search,
//! schema dump, and parse check working together. Uses pre-computed
//! vectors to bypass the embedding model, so these run in CI without
//! model files.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use selene_core::schema::{NodeSchema, PropertyDef, ValueType};
use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
use selene_gql::{GqlValue, QueryBuilder};
use selene_graph::SeleneGraph;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// Build a 384-dim unit vector with the given leading values, rest zeros.
fn make_vec(leading: &[f32]) -> Arc<[f32]> {
    let mut v = vec![0.0f32; 384];
    for (i, &val) in leading.iter().enumerate() {
        v[i] = val;
    }
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    Arc::from(v)
}

/// Build the AI integration test fixture graph.
///
/// Returns the graph and the query vector (identical to node 1's embedding).
fn build_ai_fixture() -> (SeleneGraph, Arc<[f32]>) {
    let mut graph = SeleneGraph::new();

    let vec_a = make_vec(&[1.0]);
    let vec_b = make_vec(&[0.9, 0.436]);
    let vec_c = make_vec(&[0.8, 0.6]);
    let vec_d = make_vec(&[0.0, 0.0, 1.0]);
    let vec_e = make_vec(&[0.0, 1.0]);
    let query_vec = vec_a.clone();

    let mut m = graph.mutate();

    // Node 1: Sensor with embedding
    m.create_node(
        LabelSet::from_strs(&["Sensor"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("name"), Value::from("TempSensor1")),
            (IStr::new("embedding"), Value::Vector(vec_a)),
        ]),
    )
    .unwrap();

    // Node 2: Sensor with embedding (high similarity to node 1)
    m.create_node(
        LabelSet::from_strs(&["Sensor"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("name"), Value::from("HumiditySensor1")),
            (IStr::new("embedding"), Value::Vector(vec_b)),
        ]),
    )
    .unwrap();

    // Node 3: Zone (no embedding)
    m.create_node(
        LabelSet::from_strs(&["Zone"]),
        PropertyMap::from_pairs(vec![(IStr::new("name"), Value::from("Zone1"))]),
    )
    .unwrap();

    // Node 4: Floor (no embedding)
    m.create_node(
        LabelSet::from_strs(&["Floor"]),
        PropertyMap::from_pairs(vec![(IStr::new("name"), Value::from("Floor1"))]),
    )
    .unwrap();

    // Node 5: CommunitySummary
    m.create_node(
        LabelSet::from_strs(&["__CommunitySummary"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("community_id"), Value::UInt(0)),
            (
                IStr::new("label_distribution"),
                Value::from("Sensor:2,Zone:1"),
            ),
            (IStr::new("key_entities"), Value::from("TempSensor1")),
            (IStr::new("node_count"), Value::Int(3)),
        ]),
    )
    .unwrap();

    // Node 6: Memory (moderate similarity to query)
    m.create_node(
        LabelSet::from_strs(&["__Memory"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("namespace"), Value::from("test")),
            (IStr::new("content"), Value::from("temperature rising")),
            (IStr::new("embedding"), Value::Vector(vec_c)),
            (IStr::new("memory_type"), Value::from("fact")),
            (IStr::new("confidence"), Value::Float(1.0)),
            (IStr::new("created_at"), Value::Int(1000)),
            (IStr::new("valid_from"), Value::Int(0)),
            (IStr::new("valid_until"), Value::Int(0)),
        ]),
    )
    .unwrap();

    // Node 7: Memory (orthogonal to query)
    m.create_node(
        LabelSet::from_strs(&["__Memory"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("namespace"), Value::from("test")),
            (IStr::new("content"), Value::from("humidity stable")),
            (IStr::new("embedding"), Value::Vector(vec_d)),
            (IStr::new("memory_type"), Value::from("fact")),
            (IStr::new("confidence"), Value::Float(0.8)),
            (IStr::new("created_at"), Value::Int(2000)),
            (IStr::new("valid_from"), Value::Int(0)),
            (IStr::new("valid_until"), Value::Int(0)),
        ]),
    )
    .unwrap();

    // Node 8: Memory in different namespace
    m.create_node(
        LabelSet::from_strs(&["__Memory"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("namespace"), Value::from("other")),
            (IStr::new("content"), Value::from("unrelated")),
            (IStr::new("embedding"), Value::Vector(vec_e)),
            (IStr::new("memory_type"), Value::from("fact")),
            (IStr::new("confidence"), Value::Float(1.0)),
            (IStr::new("created_at"), Value::Int(3000)),
            (IStr::new("valid_from"), Value::Int(0)),
            (IStr::new("valid_until"), Value::Int(0)),
        ]),
    )
    .unwrap();

    // Node 9: MemoryConfig
    m.create_node(
        LabelSet::from_strs(&["__MemoryConfig"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("namespace"), Value::from("test")),
            (IStr::new("max_memories"), Value::Int(5)),
            (IStr::new("default_ttl_ms"), Value::Int(0)),
            (IStr::new("eviction_policy"), Value::from("clock")),
        ]),
    )
    .unwrap();

    // Edges: containment hierarchy
    let contains = IStr::new("contains");
    m.create_edge(NodeId(4), contains, NodeId(3), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(3), contains, NodeId(1), PropertyMap::new())
        .unwrap();
    m.create_edge(NodeId(3), contains, NodeId(2), PropertyMap::new())
        .unwrap();

    m.commit(0).unwrap();

    (graph, query_vec)
}

// ---------------------------------------------------------------------------
// Test 1: Vector search ranks sensors by cosine similarity
// ---------------------------------------------------------------------------

#[test]
fn vector_search_ranks_by_cosine_similarity() {
    let (graph, query_vec) = build_ai_fixture();

    let query = "CALL graph.vectorSearch('Sensor', 'embedding', $qvec, 2) \
                 YIELD nodeId, score \
                 RETURN nodeId, score";

    let mut params = selene_gql::ParameterMap::new();
    params.insert(IStr::new("qvec"), GqlValue::Vector(query_vec));

    let result = QueryBuilder::new(query, &graph)
        .with_parameters(&params)
        .execute()
        .unwrap();

    assert_eq!(result.row_count(), 2, "should return 2 sensor nodes");

    let batch = &result.batches[0];
    let id_col = batch
        .column_by_name("nodeId")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let score_col = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(
        id_col.value(0),
        1,
        "node 1 should rank first (highest similarity)"
    );
    assert_eq!(id_col.value(1), 2, "node 2 should rank second");
    assert!(
        score_col.value(0) > score_col.value(1),
        "first score ({}) should exceed second ({})",
        score_col.value(0),
        score_col.value(1)
    );
}

// ---------------------------------------------------------------------------
// Test 2: Vector search uses HNSW when available
// ---------------------------------------------------------------------------

#[test]
fn vector_search_uses_hnsw_when_available() {
    let (mut graph, query_vec) = build_ai_fixture();

    // Build HNSW index from node embeddings
    let hnsw = Arc::new(selene_graph::hnsw::HnswIndex::new(
        selene_graph::hnsw::HnswParams::default(),
        384,
    ));
    let embedding_key = IStr::new("embedding");
    for nid in graph.all_node_ids() {
        if let Some(node) = graph.get_node(nid)
            && let Some(Value::Vector(v)) = node.properties.get(embedding_key)
        {
            hnsw.insert(nid, v.clone());
        }
    }
    hnsw.snapshot();
    graph.set_hnsw_index(hnsw);

    let query = "CALL graph.vectorSearch('Sensor', 'embedding', $qvec, 2) \
                 YIELD nodeId, score \
                 RETURN nodeId, score";

    let mut params = selene_gql::ParameterMap::new();
    params.insert(IStr::new("qvec"), GqlValue::Vector(query_vec));

    let result = QueryBuilder::new(query, &graph)
        .with_parameters(&params)
        .execute()
        .unwrap();

    assert_eq!(result.row_count(), 2);

    let batch = &result.batches[0];
    let id_col = batch
        .column_by_name("nodeId")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(id_col.value(0), 1, "HNSW should return node 1 first");
}

// ---------------------------------------------------------------------------
// Test 3: Memory nodes queryable by namespace
// ---------------------------------------------------------------------------

#[test]
fn memory_nodes_queryable_by_namespace() {
    let (graph, _) = build_ai_fixture();

    let query = "MATCH (m:__Memory {namespace: 'test'}) \
                 RETURN m.content AS content, m.confidence AS conf \
                 ORDER BY m.created_at ASC";
    let result = QueryBuilder::new(query, &graph).execute().unwrap();

    assert_eq!(result.row_count(), 2, "should find 2 memories in 'test'");

    let batch = &result.batches[0];
    let content = batch
        .column_by_name("content")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(content.value(0), "temperature rising");
    assert_eq!(content.value(1), "humidity stable");
}

// ---------------------------------------------------------------------------
// Test 4: Memory vector search ranks across all memories
// ---------------------------------------------------------------------------

#[test]
fn memory_vector_search_ranks_by_similarity() {
    let (graph, query_vec) = build_ai_fixture();

    let query = "CALL graph.vectorSearch('__Memory', 'embedding', $qvec, 10) \
                 YIELD nodeId, score \
                 RETURN nodeId, score";

    let mut params = selene_gql::ParameterMap::new();
    params.insert(IStr::new("qvec"), GqlValue::Vector(query_vec));

    let result = QueryBuilder::new(query, &graph)
        .with_parameters(&params)
        .execute()
        .unwrap();

    assert_eq!(result.row_count(), 3, "should return all 3 __Memory nodes");

    let batch = &result.batches[0];
    let id_col = batch
        .column_by_name("nodeId")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Node 6 (vec_c, ~0.8 cosine) should rank first
    assert_eq!(
        id_col.value(0),
        6,
        "node 6 should rank first among memories"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Schema dump excludes AI system labels by default
// ---------------------------------------------------------------------------

#[test]
fn schema_dump_excludes_ai_system_labels() {
    let (mut graph, _) = build_ai_fixture();

    graph
        .schema_mut()
        .register_node_schema(
            NodeSchema::builder("Sensor")
                .property(PropertyDef::simple("name", ValueType::String, true))
                .build(),
        )
        .unwrap();
    graph
        .schema_mut()
        .register_node_schema(
            NodeSchema::builder("__Memory")
                .property(PropertyDef::simple("content", ValueType::String, true))
                .build(),
        )
        .unwrap();

    let query = "CALL graph.schemaDump(false) YIELD schema RETURN schema";
    let result = QueryBuilder::new(query, &graph).execute().unwrap();

    assert_eq!(result.row_count(), 1);

    let batch = &result.batches[0];
    let schema_text = batch
        .column_by_name("schema")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);

    assert!(schema_text.contains(":Sensor"), "should include Sensor");
    assert!(
        !schema_text.contains("__Memory"),
        "should exclude __Memory by default"
    );
}

// ---------------------------------------------------------------------------
// Test 6: Schema dump includes system labels when requested
// ---------------------------------------------------------------------------

#[test]
fn schema_dump_includes_system_labels_when_requested() {
    let (mut graph, _) = build_ai_fixture();

    graph
        .schema_mut()
        .register_node_schema(
            NodeSchema::builder("__Memory")
                .property(PropertyDef::simple("content", ValueType::String, true))
                .build(),
        )
        .unwrap();

    let query = "CALL graph.schemaDump(true) YIELD schema RETURN schema";
    let result = QueryBuilder::new(query, &graph).execute().unwrap();

    let batch = &result.batches[0];
    let schema_text = batch
        .column_by_name("schema")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);

    assert!(
        schema_text.contains("__Memory"),
        "should include __Memory when includeSystem=true"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Community summary nodes have structural data
// ---------------------------------------------------------------------------

#[test]
fn community_summary_nodes_have_structural_data() {
    let (graph, _) = build_ai_fixture();

    let query = "MATCH (c:__CommunitySummary) \
                 RETURN c.community_id AS cid, c.label_distribution AS ldist, \
                 c.key_entities AS entities, c.node_count AS cnt";
    let result = QueryBuilder::new(query, &graph).execute().unwrap();

    assert_eq!(result.row_count(), 1, "should have 1 community summary");

    let batch = &result.batches[0];
    let ldist = batch
        .column_by_name("ldist")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert!(ldist.contains("Sensor:2"), "should contain Sensor:2");

    let cnt = batch
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 3, "community should have 3 nodes");
}

// ---------------------------------------------------------------------------
// Test 8: BFS expansion traverses containment hierarchy
// ---------------------------------------------------------------------------

#[test]
fn bfs_expansion_traverses_containment() {
    let (graph, _) = build_ai_fixture();

    // BFS follows outgoing edges. Node 3 (Zone1) has outgoing `contains`
    // edges to nodes 1 and 2 (sensors). Start from Zone1 and verify the
    // sensors are reachable.
    let neighbors = selene_graph::algorithms::bfs_with_depth(&graph, NodeId(3), None, 1);

    assert!(
        !neighbors.is_empty(),
        "BFS from Zone1 should find contained sensors"
    );

    let neighbor_ids: Vec<u64> = neighbors.iter().map(|(nid, _)| nid.0).collect();
    assert!(
        neighbor_ids.contains(&1),
        "should reach TempSensor1 (node 1) via contains edge"
    );
    assert!(
        neighbor_ids.contains(&2),
        "should reach HumiditySensor1 (node 2) via contains edge"
    );

    // Two-hop BFS from Floor1 (node 4) should reach Zone1 and both sensors.
    let deep = selene_graph::algorithms::bfs_with_depth(&graph, NodeId(4), None, 2);
    let deep_ids: Vec<u64> = deep.iter().map(|(nid, _)| nid.0).collect();
    assert!(
        deep_ids.contains(&3),
        "should reach Zone1 (node 3) from Floor1"
    );
    assert!(
        deep_ids.contains(&1),
        "should reach TempSensor1 (node 1) from Floor1 at depth 2"
    );
}

// ---------------------------------------------------------------------------
// Test 9: GraphRAG local data flow (vector search then BFS)
// ---------------------------------------------------------------------------

#[test]
fn graphrag_local_data_flow_vector_then_bfs() {
    let (graph, query_vec) = build_ai_fixture();

    // Step 1: Vector search finds top-1 sensor
    let query = "CALL graph.vectorSearch('Sensor', 'embedding', $qvec, 1) \
                 YIELD nodeId, score \
                 RETURN nodeId, score";

    let mut params = selene_gql::ParameterMap::new();
    params.insert(IStr::new("qvec"), GqlValue::Vector(query_vec));

    let result = QueryBuilder::new(query, &graph)
        .with_parameters(&params)
        .execute()
        .unwrap();

    assert_eq!(result.row_count(), 1);

    let batch = &result.batches[0];
    let seed_id = batch
        .column_by_name("nodeId")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(seed_id, 1, "seed should be TempSensor1");

    // Step 2: Expand context via GQL pattern matching. A real GraphRAG
    // local retriever follows edges in both directions from the seed node.
    // Use a MATCH query to find the zone that contains the seed sensor.
    let expand_query =
        format!("MATCH (z)-[:contains]->(s) FILTER id(s) = {seed_id} RETURN id(z) AS zoneId");
    let expand_result = QueryBuilder::new(&expand_query, &graph).execute().unwrap();
    assert_eq!(
        expand_result.row_count(),
        1,
        "sensor should have a containing zone"
    );

    let zone_id = expand_result.batches[0]
        .column_by_name("zoneId")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(zone_id, 3, "containing zone should be Zone1");

    // Step 3: BFS from the zone to find all contained entities
    let expanded =
        selene_graph::algorithms::bfs_with_depth(&graph, NodeId(zone_id as u64), None, 1);

    let reachable: Vec<u64> = expanded.iter().map(|(nid, _)| nid.0).collect();
    assert!(
        reachable.contains(&1),
        "should reach TempSensor1 from Zone1"
    );
    assert!(
        reachable.contains(&2),
        "should reach HumiditySensor1 from Zone1"
    );
}

// ---------------------------------------------------------------------------
// Test 10: Schema dump + parse check (Text2GQL workflow)
// ---------------------------------------------------------------------------

#[test]
fn parse_check_validates_schema_derived_query() {
    let (mut graph, _) = build_ai_fixture();

    graph
        .schema_mut()
        .register_node_schema(
            NodeSchema::builder("Sensor")
                .property(PropertyDef::simple("name", ValueType::String, true))
                .property(PropertyDef::simple("temp", ValueType::Float, false))
                .build(),
        )
        .unwrap();

    // Step 1: Get schema dump
    let dump_query = "CALL graph.schemaDump(false) YIELD schema RETURN schema";
    let result = QueryBuilder::new(dump_query, &graph).execute().unwrap();

    let batch = &result.batches[0];
    let schema_text = batch
        .column_by_name("schema")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);

    assert!(schema_text.contains(".name"));
    assert!(schema_text.contains(".temp"));

    // Step 2: Validate a query that uses schema properties
    let derived_query = "MATCH (s:Sensor) FILTER s.temp > 72.0 RETURN s.name AS name";
    assert!(
        selene_gql::parse_statement(derived_query).is_ok(),
        "query derived from schema should parse"
    );

    // Step 3: Verify an invalid query fails
    assert!(
        selene_gql::parse_statement("SELEKT * FROM Sensor").is_err(),
        "invalid query should fail to parse"
    );
}

// ---------------------------------------------------------------------------
// Test 11: Memory config queryable via GQL
// ---------------------------------------------------------------------------

#[test]
fn memory_config_queryable() {
    let (graph, _) = build_ai_fixture();

    let query = "MATCH (c:__MemoryConfig {namespace: 'test'}) \
                 RETURN c.max_memories AS max_mem, \
                 c.eviction_policy AS policy";
    let result = QueryBuilder::new(query, &graph).execute().unwrap();

    assert_eq!(result.row_count(), 1);

    let batch = &result.batches[0];
    let max_mem = batch
        .column_by_name("max_mem")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(max_mem, 5);

    let policy = batch
        .column_by_name("policy")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(policy, "clock");
}

// ---------------------------------------------------------------------------
// Test 12: Cross-feature query counting memories and sensors
// ---------------------------------------------------------------------------

#[test]
fn cross_feature_memory_and_sensor_counts() {
    let (graph, _) = build_ai_fixture();

    let query = "MATCH (m:__Memory {namespace: 'test'}) RETURN count(m) AS cnt";
    let result = QueryBuilder::new(query, &graph).execute().unwrap();

    let batch = &result.batches[0];
    let cnt = batch
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(cnt, 2);

    let query2 = "MATCH (s:Sensor) RETURN count(s) AS cnt";
    let result2 = QueryBuilder::new(query2, &graph).execute().unwrap();

    let batch2 = &result2.batches[0];
    let sensor_cnt = batch2
        .column_by_name("cnt")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(sensor_cnt, 2);
}

// ---------------------------------------------------------------------------
// Test 13: Parameters propagate through CALL procedure in subquery
// ---------------------------------------------------------------------------

#[test]
fn parameters_propagate_through_subquery_call() {
    let (graph, _query_vec) = build_ai_fixture();

    // Use $param in MATCH inline properties ({key: $param}) inside a
    // CALL { subquery } to verify parameter propagation through the
    // pattern scan layer (EvalContext threading).
    let query = "MATCH (s:Sensor) \
                 CALL { \
                     MATCH (m:__Memory {namespace: $ns}) \
                     RETURN count(m) AS mem_count \
                 } \
                 RETURN count(s) AS sensor_count, mem_count";

    let mut params = selene_gql::ParameterMap::new();
    params.insert(IStr::new("ns"), GqlValue::String("test".into()));

    let result = QueryBuilder::new(query, &graph)
        .with_parameters(&params)
        .execute()
        .unwrap();

    assert!(
        result.row_count() > 0,
        "subquery with $param should return results"
    );

    let batch = &result.batches[0];
    let mem_count = batch
        .column_by_name("mem_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        mem_count, 2,
        "should find 2 memories in 'test' namespace via $ns param"
    );
}

// ---------------------------------------------------------------------------
// Test 14: Parameters work in CALL procedure arguments
// ---------------------------------------------------------------------------

#[test]
fn parameters_in_call_procedure_arguments() {
    let (graph, query_vec) = build_ai_fixture();

    // Verify $param works in CALL procedure arguments (fixed in fec2d0e).
    let query = "CALL graph.vectorSearch($label, $prop, $qvec, $k) \
                 YIELD nodeId, score \
                 RETURN nodeId, score";

    let mut params = selene_gql::ParameterMap::new();
    params.insert(IStr::new("label"), GqlValue::String("Sensor".into()));
    params.insert(IStr::new("prop"), GqlValue::String("embedding".into()));
    params.insert(IStr::new("qvec"), GqlValue::Vector(query_vec));
    params.insert(IStr::new("k"), GqlValue::Int(2));

    let result = QueryBuilder::new(query, &graph)
        .with_parameters(&params)
        .execute()
        .unwrap();

    assert_eq!(
        result.row_count(),
        2,
        "should return 2 results with all params"
    );
}
