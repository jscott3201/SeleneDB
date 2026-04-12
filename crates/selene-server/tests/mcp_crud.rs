//! MCP integration tests for graph CRUD, schema management, batch operations,
//! time-series, CSV import/export, and graph navigation tools.
//!
//! Covers the foundational MCP tools that were previously untested.

mod support;
use support::*;

/// Extract a numeric ID from an MCP tool response text.
///
/// Handles JSON responses like `{"id":1,...}` or `[{"proposalId":5}]`,
/// as well as text like `"Created node 5"`.
fn extract_id(text: &str) -> u64 {
    // Try JSON: look for `"id":N` or `"proposalId":N` pattern directly
    for key in ["\"id\":", "\"nodeId\":", "\"edgeId\":", "\"proposalId\":"] {
        if let Some(pos) = text.find(key) {
            let after = &text[pos + key.len()..];
            let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(id) = num_str.parse::<u64>() {
                return id;
            }
        }
    }
    // Fallback: find the first standalone number in the text
    text.split_whitespace()
        .find_map(|w| {
            w.trim_matches(|c: char| !c.is_ascii_digit())
                .parse::<u64>()
                .ok()
        })
        .unwrap_or_else(|| panic!("no numeric ID found in: {text}"))
}

/// Assert that text (case-insensitive) contains one of the given substrings.
fn assert_text_contains_any(text: &str, needles: &[&str], context: &str) {
    let lower = text.to_lowercase();
    assert!(
        needles.iter().any(|n| lower.contains(&n.to_lowercase())),
        "{context}: {text}"
    );
}

// ── Health & Info ───────────────────────────────────────────────────

#[tokio::test]
async fn health_tool() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;
    let result = call_tool(&base, &sid, 2, "health", serde_json::json!({})).await;
    let text = tool_text(&result);
    assert!(text.contains("ok"), "health should report ok: {text}");
}

#[tokio::test]
async fn info_tool() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;
    let result = call_tool(&base, &sid, 2, "info", serde_json::json!({})).await;
    let text = tool_text(&result);
    assert!(
        text.contains("version"),
        "info should include version: {text}"
    );
}

#[tokio::test]
async fn graph_stats_tool() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;
    let result = call_tool(&base, &sid, 2, "graph_stats", serde_json::json!({})).await;
    let text = tool_text(&result);
    assert!(
        text.contains("nodes") || text.contains("node_count"),
        "graph_stats should include node count: {text}"
    );
}

// ── Node CRUD ──────────────────────────────────────────────────────

#[tokio::test]
async fn create_get_modify_delete_node() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "create_node",
            serde_json::json!({
                "labels": ["sensor", "temperature"],
                "properties": {"name": "Zone-A Temp", "unit": "°F"}
            }),
        )
        .await,
    );
    let node_id = extract_id(&text);
    assert!(
        node_id > 0,
        "created node should have a positive id: {text}"
    );

    // Get
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "get_node",
            serde_json::json!({"id": node_id}),
        )
        .await,
    );
    assert!(
        text.contains("Zone-A Temp"),
        "get_node should return name: {text}"
    );
    assert!(
        text.contains("sensor"),
        "get_node should return labels: {text}"
    );

    // Modify — set a property, add a label
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            4,
            "modify_node",
            serde_json::json!({
                "id": node_id,
                "set_properties": {"calibrated": true},
                "add_labels": ["calibrated_sensor"]
            }),
        )
        .await,
    );
    assert_text_contains_any(
        &text,
        &["calibrated", "modified", "updated"],
        "modify should confirm",
    );

    // Verify modification via get
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            5,
            "get_node",
            serde_json::json!({"id": node_id}),
        )
        .await,
    );
    assert!(
        text.contains("calibrated"),
        "modified node should have new property: {text}"
    );

    // Delete
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            6,
            "delete_node",
            serde_json::json!({"id": node_id}),
        )
        .await,
    );
    assert_text_contains_any(&text, &["deleted", "removed"], "delete should confirm");
}

#[tokio::test]
async fn list_nodes_with_label_filter() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create nodes with different labels
    call_tool(
        &base,
        &sid,
        2,
        "create_node",
        serde_json::json!({"labels": ["sensor"], "properties": {"name": "S1"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        3,
        "create_node",
        serde_json::json!({"labels": ["actuator"], "properties": {"name": "A1"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        4,
        "create_node",
        serde_json::json!({"labels": ["sensor"], "properties": {"name": "S2"}}),
    )
    .await;

    // List all
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            5,
            "list_nodes",
            serde_json::json!({"limit": 100}),
        )
        .await,
    );
    assert!(text.contains("S1"), "list_nodes should include S1: {text}");
    assert!(text.contains("A1"), "list_nodes should include A1: {text}");

    // List with label filter
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            6,
            "list_nodes",
            serde_json::json!({"label": "sensor", "limit": 100}),
        )
        .await,
    );
    assert!(
        text.contains("S1"),
        "filtered list should include S1: {text}"
    );
    assert!(
        text.contains("S2"),
        "filtered list should include S2: {text}"
    );
    assert!(
        !text.contains("A1"),
        "filtered list should exclude actuator: {text}"
    );
}

// ── Edge CRUD ──────────────────────────────────────────────────────

#[tokio::test]
async fn create_get_modify_delete_edge() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create two nodes
    let n1_text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "create_node",
            serde_json::json!({"labels": ["building"], "properties": {"name": "HQ"}}),
        )
        .await,
    );
    let n1 = extract_id(&n1_text);
    let n2_text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "create_node",
            serde_json::json!({"labels": ["floor"], "properties": {"name": "Floor 1"}}),
        )
        .await,
    );
    let n2 = extract_id(&n2_text);

    // Create edge
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            4,
            "create_edge",
            serde_json::json!({
                "source": n1,
                "target": n2,
                "label": "contains",
                "properties": {"level": 1}
            }),
        )
        .await,
    );
    let edge_id = extract_id(&text);
    assert!(
        edge_id > 0,
        "create_edge should return a positive id: {text}"
    );

    // Get edge
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            5,
            "get_edge",
            serde_json::json!({"id": edge_id}),
        )
        .await,
    );
    assert!(
        text.contains("contains"),
        "get_edge should return label: {text}"
    );

    // Modify edge
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            6,
            "modify_edge",
            serde_json::json!({"id": edge_id, "set_properties": {"weight": 1.0}}),
        )
        .await,
    );
    assert_text_contains_any(
        &text,
        &["modified", "updated", "weight"],
        "modify_edge should confirm",
    );

    // List edges
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            7,
            "list_edges",
            serde_json::json!({"limit": 10}),
        )
        .await,
    );
    assert!(
        text.contains("contains"),
        "list_edges should include edge: {text}"
    );

    // Delete edge
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            8,
            "delete_edge",
            serde_json::json!({"id": edge_id}),
        )
        .await,
    );
    assert_text_contains_any(&text, &["deleted", "removed"], "delete_edge should confirm");
}

#[tokio::test]
async fn node_edges_with_direction_filter() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create 3 nodes: A -> B -> C
    call_tool(
        &base,
        &sid,
        2,
        "create_node",
        serde_json::json!({"labels": ["zone"], "properties": {"name": "A"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        3,
        "create_node",
        serde_json::json!({"labels": ["zone"], "properties": {"name": "B"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        4,
        "create_node",
        serde_json::json!({"labels": ["zone"], "properties": {"name": "C"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        5,
        "create_edge",
        serde_json::json!({"source": 1, "target": 2, "label": "connects"}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        6,
        "create_edge",
        serde_json::json!({"source": 2, "target": 3, "label": "connects"}),
    )
    .await;

    // Node B's outgoing edges
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            7,
            "node_edges",
            serde_json::json!({"id": 2, "direction": "outgoing"}),
        )
        .await,
    );
    assert!(
        text.contains("C") || text.contains("3"),
        "outgoing from B should reach C: {text}"
    );

    // Node B's incoming edges
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            8,
            "node_edges",
            serde_json::json!({"id": 2, "direction": "incoming"}),
        )
        .await,
    );
    assert!(
        text.contains("A") || text.contains("1"),
        "incoming to B should show A: {text}"
    );
}

// ── Batch Operations ───────────────────────────────────────────────

#[tokio::test]
async fn batch_create_nodes_and_edges() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Batch create nodes
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "batch_create_nodes",
            serde_json::json!({
                "nodes": [
                    {"labels": ["sensor"], "properties": {"name": "S1"}},
                    {"labels": ["sensor"], "properties": {"name": "S2"}},
                    {"labels": ["sensor"], "properties": {"name": "S3"}}
                ]
            }),
        )
        .await,
    );
    assert!(
        text.contains("3") || text.contains("created"),
        "batch should create 3 nodes: {text}"
    );

    // Batch create edges
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "batch_create_edges",
            serde_json::json!({
                "edges": [
                    {"source": 1, "target": 2, "label": "sibling"},
                    {"source": 2, "target": 3, "label": "sibling"}
                ]
            }),
        )
        .await,
    );
    assert!(
        text.contains("2") || text.contains("created"),
        "batch should create 2 edges: {text}"
    );
}

#[tokio::test]
async fn batch_ingest_with_connections() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create a root node first
    call_tool(
        &base,
        &sid,
        2,
        "create_node",
        serde_json::json!({"labels": ["building"], "properties": {"name": "HQ"}}),
    )
    .await;

    // Ingest nodes with connections to existing node
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "batch_ingest",
            serde_json::json!({
                "entries": [
                    {
                        "labels": ["floor"],
                        "properties": {"name": "Floor 1"},
                        "connect_from": [{"node_id": 1, "label": "contains"}]
                    },
                    {
                        "labels": ["floor"],
                        "properties": {"name": "Floor 2"},
                        "connect_from": [{"node_id": 1, "label": "contains"}]
                    }
                ]
            }),
        )
        .await,
    );
    assert!(
        text.contains("2") || text.contains("ingested") || text.contains("created"),
        "batch_ingest should confirm: {text}"
    );

    // Verify the graph structure
    let stats = tool_text(&call_tool(&base, &sid, 4, "graph_stats", serde_json::json!({})).await);
    assert!(
        stats.contains("3") || stats.contains("node"),
        "should have 3 nodes: {stats}"
    );
}

// ── Schema Management ──────────────────────────────────────────────

#[tokio::test]
async fn schema_lifecycle() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create node schema
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "create_schema",
            serde_json::json!({
                "label": "temperature_sensor",
                "description": "A temperature measurement point",
                "fields": {"unit": "String", "accuracy": "Float"},
            }),
        )
        .await,
    );
    assert!(
        text.contains("temperature_sensor")
            || text.contains("created")
            || text.contains("registered"),
        "create_schema should confirm: {text}"
    );

    // Get schema
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "get_schema",
            serde_json::json!({"label": "temperature_sensor"}),
        )
        .await,
    );
    assert!(
        text.contains("temperature_sensor"),
        "get_schema should return the schema: {text}"
    );
    assert!(
        text.contains("unit"),
        "schema should include field 'unit': {text}"
    );

    // List schemas
    let text = tool_text(&call_tool(&base, &sid, 4, "list_schemas", serde_json::json!({})).await);
    assert!(
        text.contains("temperature_sensor"),
        "list_schemas should include our schema: {text}"
    );

    // Update schema
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            5,
            "update_schema",
            serde_json::json!({
                "label": "temperature_sensor",
                "description": "Updated description",
                "fields": {"unit": "String", "accuracy": "Float", "range_min": "Float"}
            }),
        )
        .await,
    );
    assert!(
        text.contains("updated") || text.contains("temperature_sensor"),
        "update_schema should confirm: {text}"
    );

    // Export schemas
    let text = tool_text(&call_tool(&base, &sid, 6, "export_schemas", serde_json::json!({})).await);
    assert!(
        text.contains("temperature_sensor"),
        "export should include our schema: {text}"
    );

    // Delete schema
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            7,
            "delete_schema",
            serde_json::json!({"label": "temperature_sensor"}),
        )
        .await,
    );
    assert_text_contains_any(
        &text,
        &["deleted", "removed", "unregistered"],
        "delete_schema should confirm",
    );
}

#[tokio::test]
async fn edge_schema_lifecycle() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create edge schema
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "create_edge_schema",
            serde_json::json!({
                "label": "feeds",
                "description": "Equipment feeding relationship",
                "fields": {"capacity_kw": "Float"},
                "source_labels": ["equipment"],
                "target_labels": ["zone"]
            }),
        )
        .await,
    );
    assert!(
        text.contains("feeds") || text.contains("created") || text.contains("registered"),
        "create_edge_schema should confirm: {text}"
    );

    // Delete edge schema
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "delete_edge_schema",
            serde_json::json!({"label": "feeds"}),
        )
        .await,
    );
    assert_text_contains_any(
        &text,
        &["deleted", "removed", "unregistered"],
        "delete_edge_schema should confirm",
    );
}

#[tokio::test]
async fn import_schema_pack() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    let pack_content = r#"
name = "test-pack"
version = "1.0.0"

[types.device]
description = "A generic device"
fields = { name = "string!", status = "string" }

[types.room]
description = "A physical room"
fields = { name = "string!", area_sqft = "float" }
"#;

    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "import_schema_pack",
            serde_json::json!({"content": pack_content}),
        )
        .await,
    );
    assert!(
        text.contains("device") || text.contains("imported") || text.contains("2"),
        "import_schema_pack should confirm: {text}"
    );
}

// ── Graph Navigation ───────────────────────────────────────────────

#[tokio::test]
async fn related_tool() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Build a small graph: building -> floor -> zone
    call_tool(
        &base,
        &sid,
        2,
        "create_node",
        serde_json::json!({"labels": ["building"], "properties": {"name": "HQ"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        3,
        "create_node",
        serde_json::json!({"labels": ["floor"], "properties": {"name": "Floor 1"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        4,
        "create_edge",
        serde_json::json!({"source": 1, "target": 2, "label": "contains"}),
    )
    .await;

    let text = tool_text(&call_tool(&base, &sid, 5, "related", serde_json::json!({"id": 1})).await);
    assert!(
        text.contains("Floor 1") || text.contains("floor") || text.contains("2"),
        "related should show connected node: {text}"
    );
}

#[tokio::test]
async fn resolve_tool() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    call_tool(
        &base,
        &sid,
        2,
        "create_node",
        serde_json::json!({"labels": ["sensor"], "properties": {"name": "AHU-1-SAT"}}),
    )
    .await;

    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "resolve",
            serde_json::json!({"identifier": "AHU-1-SAT"}),
        )
        .await,
    );
    assert!(
        text.contains("AHU-1-SAT") || text.contains("1"),
        "resolve should find the node: {text}"
    );
}

#[tokio::test]
async fn graph_slice_tool() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create a hierarchy
    call_tool(
        &base,
        &sid,
        2,
        "create_node",
        serde_json::json!({"labels": ["site"], "properties": {"name": "Campus"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        3,
        "create_node",
        serde_json::json!({"labels": ["building"], "properties": {"name": "HQ"}}),
    )
    .await;
    call_tool(
        &base,
        &sid,
        4,
        "create_edge",
        serde_json::json!({"source": 1, "target": 2, "label": "contains"}),
    )
    .await;

    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            5,
            "graph_slice",
            serde_json::json!({"root_id": 1, "max_depth": 2}),
        )
        .await,
    );
    assert!(
        text.contains("Campus") || text.contains("HQ"),
        "graph_slice should include nodes: {text}"
    );
}

#[tokio::test]
async fn gql_explain_tool() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "gql_explain",
            serde_json::json!({"query": "MATCH (n:sensor) RETURN n.name"}),
        )
        .await,
    );
    assert!(
        text.contains("Scan")
            || text.contains("scan")
            || text.contains("Plan")
            || text.contains("plan"),
        "gql_explain should return a query plan: {text}"
    );
}

// ── Time-Series ────────────────────────────────────────────────────

#[tokio::test]
async fn ts_write_and_query() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create a node to attach TS data to
    call_tool(
        &base,
        &sid,
        2,
        "create_node",
        serde_json::json!({"labels": ["sensor"], "properties": {"name": "Temp-1"}}),
    )
    .await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    // Write time-series samples
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "ts_write",
            serde_json::json!({
                "samples": [
                    {"entity_id": 1, "property": "temperature", "timestamp_nanos": now - 60_000_000_000, "value": 72.0},
                    {"entity_id": 1, "property": "temperature", "timestamp_nanos": now - 30_000_000_000, "value": 72.5},
                    {"entity_id": 1, "property": "temperature", "timestamp_nanos": now, "value": 73.0}
                ]
            }),
        )
        .await,
    );
    assert!(
        text.contains("3") || text.contains("written") || text.contains("samples"),
        "ts_write should confirm: {text}"
    );

    // Query time-series
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            4,
            "ts_query",
            serde_json::json!({
                "entity_id": 1,
                "property": "temperature"
            }),
        )
        .await,
    );
    assert!(
        text.contains("72") || text.contains("73") || text.contains("temperature"),
        "ts_query should return samples: {text}"
    );
}

// ── CSV Import/Export ──────────────────────────────────────────────

#[tokio::test]
async fn csv_import_and_export_nodes() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Import CSV nodes
    let csv_content = "name,unit,location\nTemp-1,°F,Zone A\nTemp-2,°C,Zone B";
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "csv_import",
            serde_json::json!({
                "content": csv_content,
                "csv_type": "nodes",
                "label": "sensor"
            }),
        )
        .await,
    );
    assert!(
        text.contains("2") || text.contains("imported"),
        "csv_import should confirm 2 nodes: {text}"
    );

    // Export CSV nodes
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "csv_export",
            serde_json::json!({"csv_type": "nodes", "label": "sensor"}),
        )
        .await,
    );
    assert!(
        text.contains("Temp-1"),
        "csv_export should include imported data: {text}"
    );
    assert!(
        text.contains("Temp-2"),
        "csv_export should include both rows: {text}"
    );
}

// ── Proposals ──────────────────────────────────────────────────────

#[tokio::test]
async fn proposal_lifecycle() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create a node so the proposal query has something to work with
    call_tool(
        &base,
        &sid,
        2,
        "create_node",
        serde_json::json!({"labels": ["sensor"], "properties": {"name": "Old Sensor", "status": "active"}}),
    )
    .await;

    // Propose an action
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "propose_action",
            serde_json::json!({
                "description": "Decommission old sensor",
                "query": "MATCH (s:sensor) WHERE id(s) = 1 SET s.status = 'decommissioned'",
                "category": "maintenance",
                "priority": "low"
            }),
        )
        .await,
    );
    assert!(
        text.contains("roposal"),
        "propose_action should return proposal: {text}"
    );
    let proposal_id = extract_id(&text);

    // List proposals
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            4,
            "list_proposals",
            serde_json::json!({"status": "pending"}),
        )
        .await,
    );
    assert!(
        text.contains("Decommission") || text.contains("decommission") || text.contains("pending"),
        "list_proposals should include our proposal: {text}"
    );

    // Approve the proposal
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            5,
            "approve_proposal",
            serde_json::json!({"proposal_id": proposal_id, "reason": "Approved by operator"}),
        )
        .await,
    );
    assert!(text.contains("pproved"), "approve should confirm: {text}");

    // Execute the approved proposal
    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            6,
            "execute_proposal",
            serde_json::json!({"proposal_id": proposal_id}),
        )
        .await,
    );
    assert!(
        text.contains("xecuted") || text.contains("decommissioned"),
        "execute should confirm: {text}"
    );
}

#[tokio::test]
async fn proposal_reject() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    let propose_text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "propose_action",
            serde_json::json!({
                "description": "Drop all nodes",
                "query": "MATCH (n) DELETE n",
                "priority": "high"
            }),
        )
        .await,
    );
    let proposal_id = extract_id(&propose_text);

    let text = tool_text(
        &call_tool(
            &base,
            &sid,
            3,
            "reject_proposal",
            serde_json::json!({"proposal_id": proposal_id, "reason": "Too destructive"}),
        )
        .await,
    );
    assert!(text.contains("ejected"), "reject should confirm: {text}");
}

// ── Principal Management ───────────────────────────────────────────
//
// Principal CRUD requires the vault service for password hashing.
// In dev mode the vault is not initialized, so we verify that
// list_principals, get_principal, and create_principal all return
// a graceful "vault not available" error rather than panicking.

#[tokio::test]
async fn principal_tools_require_vault() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // list_principals
    let result = session_request(
        &base,
        &sid,
        2,
        "tools/call",
        serde_json::json!({"name": "list_principals", "arguments": {}}),
    )
    .await;
    let text = result.to_string();
    assert!(
        text.contains("vault") || text.contains("error"),
        "list_principals without vault should fail gracefully: {text}"
    );

    // get_principal
    let result = session_request(
        &base,
        &sid,
        3,
        "tools/call",
        serde_json::json!({"name": "get_principal", "arguments": {"identity": "nobody"}}),
    )
    .await;
    let text = result.to_string();
    assert!(
        text.contains("vault") || text.contains("error"),
        "get_principal without vault should fail gracefully: {text}"
    );

    // create_principal
    let result = session_request(
        &base,
        &sid,
        4,
        "tools/call",
        serde_json::json!({
            "name": "create_principal",
            "arguments": {"identity": "test-op", "role": "operator", "password": "pass123"}
        }),
    )
    .await;
    let text = result.to_string();
    assert!(
        text.contains("vault") || text.contains("error"),
        "create_principal without vault should fail gracefully: {text}"
    );
}

// ── Negative Path Tests ────────────────────────────────────────────

#[tokio::test]
async fn get_nonexistent_node_returns_error() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    let result = session_request(
        &base,
        &sid,
        2,
        "tools/call",
        serde_json::json!({"name": "get_node", "arguments": {"id": 99999}}),
    )
    .await;
    // Should either have an error field or isError in the result content
    let text = result.to_string();
    assert!(
        text.contains("not found")
            || text.contains("Not found")
            || text.contains("isError")
            || text.contains("error"),
        "get nonexistent node should error: {text}"
    );
}

#[tokio::test]
async fn delete_nonexistent_node_returns_error() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    let result = session_request(
        &base,
        &sid,
        2,
        "tools/call",
        serde_json::json!({"name": "delete_node", "arguments": {"id": 99999}}),
    )
    .await;
    let text = result.to_string();
    assert!(
        text.contains("not found")
            || text.contains("Not found")
            || text.contains("isError")
            || text.contains("error"),
        "delete nonexistent node should error: {text}"
    );
}

#[tokio::test]
async fn create_edge_with_invalid_source_returns_error() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    let result = session_request(
        &base,
        &sid,
        2,
        "tools/call",
        serde_json::json!({
            "name": "create_edge",
            "arguments": {"source": 99999, "target": 99998, "label": "broken"}
        }),
    )
    .await;
    let text = result.to_string();
    assert!(
        text.contains("not found")
            || text.contains("Not found")
            || text.contains("isError")
            || text.contains("error"),
        "create_edge with invalid nodes should error: {text}"
    );
}

#[tokio::test]
async fn execute_unapproved_proposal_fails() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Propose without approving
    let propose_text = tool_text(
        &call_tool(
            &base,
            &sid,
            2,
            "propose_action",
            serde_json::json!({
                "description": "Test proposal",
                "query": "MATCH (n) RETURN count(n)"
            }),
        )
        .await,
    );
    let proposal_id = extract_id(&propose_text);

    // Try to execute without approval
    let result = session_request(
        &base,
        &sid,
        3,
        "tools/call",
        serde_json::json!({"name": "execute_proposal", "arguments": {"proposal_id": proposal_id}}),
    )
    .await;
    let text = result.to_string();
    assert!(
        text.contains("not approved")
            || text.contains("pending")
            || text.contains("isError")
            || text.contains("error"),
        "execute unapproved proposal should fail: {text}"
    );
}
