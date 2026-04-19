//! MCP AI integration tests over Streamable HTTP.
//!
//! Full-stack tests for GraphRAG and Text2GQL MCP tools.
//!
//! Run: `cargo test -p selene-server --test mcp_ai`

mod support;
use support::*;

// ---------------------------------------------------------------------------
// Test 1: schema_dump returns schema over MCP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schema_dump_tool_returns_schema() {
    let (base, _server) = start_server().await;
    let session = initialize(&base).await;

    call_tool(
        &base,
        &session,
        2,
        "gql_query",
        serde_json::json!({"query": "INSERT (:Sensor {name: 'S1'})"}),
    )
    .await;

    let result = call_tool(&base, &session, 3, "schema_dump", serde_json::json!({})).await;
    let text = tool_text(&result);

    assert!(!text.is_empty(), "schema dump should not be empty");
    assert!(
        text.contains("nodes:") || text.contains("# Stats"),
        "should contain stats section: {text}"
    );
}

// ---------------------------------------------------------------------------
// Test 2: gql_parse_check validates and rejects queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gql_parse_check_validates_queries() {
    let (base, _server) = start_server().await;
    let session = initialize(&base).await;

    let valid = call_tool(
        &base,
        &session,
        2,
        "gql_parse_check",
        serde_json::json!({"query": "MATCH (n:Sensor) RETURN n.name AS name"}),
    )
    .await;
    let text = tool_text(&valid);
    assert!(
        text.contains("true") || text.contains("valid"),
        "valid query should pass: {text}"
    );

    let invalid = call_tool(
        &base,
        &session,
        3,
        "gql_parse_check",
        serde_json::json!({"query": "SELEKT * FROM sensors"}),
    )
    .await;
    let text = tool_text(&invalid);
    assert!(
        text.contains("false") || text.contains("error"),
        "invalid query should fail: {text}"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Text2GQL workflow (schema dump then parse check)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn text2gql_workflow_schema_then_parse() {
    let (base, _server) = start_server().await;
    let session = initialize(&base).await;

    let schema_result = call_tool(&base, &session, 2, "schema_dump", serde_json::json!({})).await;
    let schema_text = tool_text(&schema_result);
    assert!(!schema_text.is_empty(), "schema should not be empty");

    let check_result = call_tool(
        &base,
        &session,
        3,
        "gql_parse_check",
        serde_json::json!({"query": "MATCH (s:Sensor) RETURN s.name AS name"}),
    )
    .await;
    let check_text = tool_text(&check_result);
    assert!(
        check_text.contains("true") || check_text.contains("valid"),
        "schema-derived query should be valid: {check_text}"
    );
}

// ---------------------------------------------------------------------------
// Test 5: build_communities creates summary nodes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn build_communities_creates_summaries() {
    let (base, _server) = start_server().await;
    let session = initialize(&base).await;

    for query in &[
        "INSERT (:Device {name: 'D1'})",
        "INSERT (:Device {name: 'D2'})",
        "INSERT (:Device {name: 'D3'})",
        "MATCH (a:Device {name: 'D1'}), (b:Device {name: 'D2'}) INSERT (a)-[:connected]->(b)",
        "MATCH (a:Device {name: 'D2'}), (b:Device {name: 'D3'}) INSERT (a)-[:connected]->(b)",
    ] {
        call_tool(
            &base,
            &session,
            2,
            "gql_query",
            serde_json::json!({"query": query}),
        )
        .await;
    }

    let result = call_tool(
        &base,
        &session,
        10,
        "build_communities",
        serde_json::json!({"min_community_size": 2}),
    )
    .await;
    assert!(
        result.get("error").is_none(),
        "build_communities should succeed: {result}"
    );
    let text = tool_text(&result);
    assert!(
        text.to_lowercase().contains("communit") || text.contains("Built"),
        "should report communities: {text}"
    );

    let check = call_tool(
        &base,
        &session,
        11,
        "gql_query",
        serde_json::json!({
            "query": "MATCH (c:__CommunitySummary) RETURN count(c) AS cnt"
        }),
    )
    .await;
    let check_text = tool_text(&check);
    assert!(
        check_text.contains('1'),
        "should find at least 1 community summary: {check_text}"
    );
}

// ---------------------------------------------------------------------------
// Test 6: gql-examples resource accessible
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gql_examples_resource_readable() {
    let (base, _server) = start_server().await;
    let session = initialize(&base).await;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "resources/read",
        "params": {
            "uri": "selene://gql-examples"
        }
    });

    let resp = mcp_post(&base, &body, Some(&session)).await;
    assert_eq!(resp.status(), 200);

    let text = resp.text().await.unwrap();
    let results = parse_sse_results(&text);
    assert!(!results.is_empty());

    let resource_text = results[0]["result"]["contents"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        resource_text.contains("MATCH"),
        "should contain MATCH patterns: {resource_text}"
    );
    assert!(
        resource_text.contains("INSERT"),
        "should contain INSERT patterns: {resource_text}"
    );
}
