//! MCP AI integration tests over Streamable HTTP.
//!
//! Full-stack tests for GraphRAG, agent memory, and Text2GQL MCP tools.
//! Tests marked `#[ignore]` require the EmbeddingGemma model
//! at `data/models/embeddinggemma-300m`.
//!
//! Run: `cargo test -p selene-server --test mcp_ai`
//! Run ignored: `cargo test -p selene-server --test mcp_ai -- --ignored`

use std::sync::Arc;

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use selene_server::bootstrap;
use selene_server::config::SeleneConfig;

// ---------------------------------------------------------------------------
// Helpers (same pattern as mcp.rs)
// ---------------------------------------------------------------------------

fn has_model() -> bool {
    let path = std::env::var("SELENE_MODEL_PATH")
        .unwrap_or_else(|_| "data/models/embeddinggemma-300m".to_string());
    std::path::Path::new(&path)
        .join("model.safetensors")
        .exists()
}

async fn start_server() -> String {
    let dir = tempfile::tempdir().unwrap();
    let mut config = SeleneConfig::dev(dir.path());
    config.http.listen_addr = "127.0.0.1:0".parse().unwrap();

    selene_server::ops::init_start_time();
    let state = bootstrap::bootstrap(config, None).await.unwrap();
    let state = Arc::new(state);

    let app = selene_server::http::router(state.clone()).layer(axum::Extension(state));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    std::mem::forget(dir);
    format!("http://{addr}")
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn mcp_post(
    base: &str,
    body: &serde_json::Value,
    session_id: Option<&str>,
) -> reqwest::Response {
    let mut req = client()
        .post(format!("{base}/mcp"))
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json, text/event-stream")
        .json(body);

    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }

    req.send().await.unwrap()
}

fn parse_sse_results(body: &str) -> Vec<serde_json::Value> {
    body.lines()
        .filter_map(|line| {
            let data = line.strip_prefix("data:")?;
            let trimmed = data.trim();
            if trimmed.is_empty() {
                return None;
            }
            serde_json::from_str(trimmed).ok()
        })
        .collect()
}

async fn initialize(base: &str) -> String {
    let init_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {
                "name": "selene-ai-test",
                "version": "0.1.0"
            }
        }
    });

    let resp = mcp_post(base, &init_req, None).await;
    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let body = resp.text().await.unwrap();
    let _ = parse_sse_results(&body);

    let initialized = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    });
    mcp_post(base, &initialized, Some(&session_id)).await;

    session_id
}

async fn call_tool(
    base: &str,
    session_id: &str,
    id: u64,
    tool_name: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments,
        }
    });

    let resp = mcp_post(base, &body, Some(session_id)).await;
    assert_eq!(resp.status(), 200, "{tool_name} should return 200");

    let text = resp.text().await.unwrap();
    let results = parse_sse_results(&text);
    assert!(
        !results.is_empty(),
        "{tool_name} should return at least one SSE message"
    );
    results[0].clone()
}

fn tool_text(result: &serde_json::Value) -> String {
    result["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("")
        .to_string()
}

// ---------------------------------------------------------------------------
// Test 1: schema_dump returns schema over MCP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schema_dump_tool_returns_schema() {
    let base = start_server().await;
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
    let base = start_server().await;
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
    let base = start_server().await;
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
// Test 4: configure_memory persists config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn configure_memory_sets_config() {
    let base = start_server().await;
    let session = initialize(&base).await;

    let config_result = call_tool(
        &base,
        &session,
        2,
        "configure_memory",
        serde_json::json!({
            "namespace": "config_test",
            "max_memories": 50,
            "eviction_policy": "oldest"
        }),
    )
    .await;
    assert!(
        config_result.get("error").is_none(),
        "configure_memory should succeed: {config_result}"
    );
    let config_text = tool_text(&config_result);
    assert!(
        config_text.contains("config_test"),
        "should confirm namespace configured: {config_text}"
    );

    // Configure again with different values to verify idempotent MERGE
    let update_result = call_tool(
        &base,
        &session,
        3,
        "configure_memory",
        serde_json::json!({
            "namespace": "config_test",
            "max_memories": 100
        }),
    )
    .await;
    assert!(
        update_result.get("error").is_none(),
        "second configure_memory should succeed: {update_result}"
    );
}

// ---------------------------------------------------------------------------
// Test 5: build_communities creates summary nodes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn build_communities_creates_summaries() {
    let base = start_server().await;
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
    let base = start_server().await;
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

// ---------------------------------------------------------------------------
// Test 7: remember then recall (requires model)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires EmbeddingGemma model"]
async fn remember_then_recall() {
    if !has_model() {
        eprintln!("SKIP: embedding model not available");
        return;
    }

    let base = start_server().await;
    let session = initialize(&base).await;

    let remember_result = call_tool(
        &base,
        &session,
        2,
        "remember",
        serde_json::json!({
            "namespace": "integration_test",
            "content": "The building HVAC system uses a variable air volume design",
            "memory_type": "fact"
        }),
    )
    .await;
    assert!(
        remember_result.get("error").is_none(),
        "remember should succeed: {remember_result}"
    );

    let recall_result = call_tool(
        &base,
        &session,
        3,
        "recall",
        serde_json::json!({
            "namespace": "integration_test",
            "query": "HVAC system design",
            "k": 5
        }),
    )
    .await;
    assert!(
        recall_result.get("error").is_none(),
        "recall should succeed: {recall_result}"
    );
    let recall_text = tool_text(&recall_result);
    assert!(
        recall_text.contains("variable air volume") || recall_text.contains("HVAC"),
        "recall should find the stored memory: {recall_text}"
    );
}

// ---------------------------------------------------------------------------
// Test 8: remember then forget (requires model)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires EmbeddingGemma model"]
async fn remember_then_forget() {
    if !has_model() {
        eprintln!("SKIP: embedding model not available");
        return;
    }

    let base = start_server().await;
    let session = initialize(&base).await;

    call_tool(
        &base,
        &session,
        2,
        "remember",
        serde_json::json!({
            "namespace": "forget_test",
            "content": "temporary fact for deletion test",
            "memory_type": "fact"
        }),
    )
    .await;

    let forget_result = call_tool(
        &base,
        &session,
        3,
        "forget",
        serde_json::json!({
            "namespace": "forget_test",
            "query": "temporary fact"
        }),
    )
    .await;
    assert!(
        forget_result.get("error").is_none(),
        "forget should succeed: {forget_result}"
    );
    let forget_text = tool_text(&forget_result);
    assert!(
        forget_text.to_lowercase().contains("delet"),
        "should confirm deletion: {forget_text}"
    );

    let recall_result = call_tool(
        &base,
        &session,
        4,
        "recall",
        serde_json::json!({
            "namespace": "forget_test",
            "query": "temporary fact",
            "k": 5
        }),
    )
    .await;
    let recall_text = tool_text(&recall_result);
    assert!(
        recall_text.contains("0 results") || recall_text.contains("No memories"),
        "recall after forget should return empty: {recall_text}"
    );
}
