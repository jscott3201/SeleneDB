//! MCP integration tests over Streamable HTTP.
//!
//! Boots the full HTTP server in dev mode, then exercises the MCP endpoint
//! at `/mcp` using raw JSON-RPC over HTTP with SSE response parsing.
//! Covers the MCP lifecycle: initialize handshake, tool listing and
//! invocation, resource listing and reading, prompts, and logging.

mod support;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use support::*;

// ── Tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn initialize_handshake() {
    let (base, _server) = start_server().await;
    let init_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {
                "name": "selene-test",
                "version": "0.1.0"
            }
        }
    });

    let resp = mcp_post(&base, &init_req, None).await;
    assert_eq!(resp.status(), 200);

    // Session ID must be present.
    assert!(
        resp.headers().contains_key("mcp-session-id"),
        "response must include mcp-session-id header"
    );

    let body = resp.text().await.unwrap();
    let results = parse_sse_results(&body);
    assert!(!results.is_empty());

    let init_result = &results[0]["result"];
    assert!(
        init_result.get("protocolVersion").is_some(),
        "must include protocolVersion"
    );
    assert!(
        init_result.get("capabilities").is_some(),
        "must include capabilities"
    );
    assert!(
        init_result.get("serverInfo").is_some(),
        "must include serverInfo"
    );

    // Verify server identification.
    assert_eq!(init_result["serverInfo"]["name"], "selene");

    // Verify capabilities include tools, resources, prompts, and logging.
    let caps = &init_result["capabilities"];
    assert!(
        caps.get("tools").is_some(),
        "capabilities must include tools"
    );
    assert!(
        caps.get("resources").is_some(),
        "capabilities must include resources"
    );
    assert!(
        caps.get("prompts").is_some(),
        "capabilities must include prompts"
    );
    assert!(
        caps.get("logging").is_some(),
        "capabilities must include logging"
    );
}

#[tokio::test]
async fn tools_list() {
    let (base, _server) = start_server().await;
    let session_id = initialize(&base).await;

    let result = session_request(&base, &session_id, 2, "tools/list", serde_json::json!({})).await;

    let tools = result["result"]["tools"]
        .as_array()
        .expect("tools/list must return an array of tools");

    // Collect tool names for diagnostics on failure.
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();

    // Verify the expected count. The exact number may change as tools are added;
    // assert at least 30 to catch major regressions without being too brittle.
    assert!(
        tools.len() >= 30,
        "expected at least 30 tools, got {}: {names:?}",
        tools.len()
    );

    // Verify a few well-known tools are present.
    assert!(names.contains(&"gql_query"), "gql_query tool must exist");
    assert!(names.contains(&"get_node"), "get_node tool must exist");
    assert!(
        names.contains(&"create_node"),
        "create_node tool must exist"
    );
    assert!(
        names.contains(&"graph_stats"),
        "graph_stats tool must exist"
    );
}

#[tokio::test]
async fn tools_call_gql_query() {
    let (base, _server) = start_server().await;
    let session_id = initialize(&base).await;

    // First, create a node so we have data to query.
    let insert_result = session_request(
        &base,
        &session_id,
        2,
        "tools/call",
        serde_json::json!({
            "name": "gql_query",
            "arguments": {
                "query": "INSERT (:test_mcp {val: 42})"
            }
        }),
    )
    .await;
    assert!(
        insert_result.get("error").is_none(),
        "INSERT should not error: {insert_result}"
    );

    // Now query it back.
    let result = session_request(
        &base,
        &session_id,
        3,
        "tools/call",
        serde_json::json!({
            "name": "gql_query",
            "arguments": {
                "query": "MATCH (n:test_mcp) RETURN n.val AS val"
            }
        }),
    )
    .await;

    let call_result = &result["result"];
    assert!(
        call_result.get("content").is_some(),
        "tool call result must have content, got: {result}"
    );

    let content = call_result["content"]
        .as_array()
        .expect("content must be an array");
    assert!(!content.is_empty(), "content must not be empty");

    // The text content should contain the query result.
    let text = content[0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("first content item must have text, got: {}", content[0]));
    assert!(
        text.contains("00000"),
        "gql_query result should contain a success status code (00000), got: {text}"
    );
    assert!(
        text.contains("1 rows"),
        "result should indicate 1 row returned, got: {text}"
    );
}

#[tokio::test]
async fn resources_list() {
    let (base, _server) = start_server().await;
    let session_id = initialize(&base).await;

    let result = session_request(
        &base,
        &session_id,
        2,
        "resources/list",
        serde_json::json!({}),
    )
    .await;

    let resources = result["result"]["resources"]
        .as_array()
        .expect("resources/list must return an array");

    // There should be at least 4 static resources.
    assert!(
        resources.len() >= 4,
        "expected at least 4 resources, got {}",
        resources.len()
    );

    let uris: Vec<&str> = resources.iter().filter_map(|r| r["uri"].as_str()).collect();
    assert!(
        uris.contains(&"selene://health"),
        "health resource must exist"
    );
    assert!(
        uris.contains(&"selene://stats"),
        "stats resource must exist"
    );
    assert!(
        uris.contains(&"selene://schemas"),
        "schemas resource must exist"
    );
    assert!(uris.contains(&"selene://info"), "info resource must exist");
}

#[tokio::test]
async fn resources_read_health() {
    let (base, _server) = start_server().await;
    let session_id = initialize(&base).await;

    let result = session_request(
        &base,
        &session_id,
        2,
        "resources/read",
        serde_json::json!({
            "uri": "selene://health"
        }),
    )
    .await;

    let contents = result["result"]["contents"]
        .as_array()
        .expect("resources/read must return contents array");
    assert!(!contents.is_empty(), "contents must not be empty");

    // The text should be valid JSON with a "status" field.
    let text = contents[0]["text"]
        .as_str()
        .expect("content must have text");
    let health: serde_json::Value =
        serde_json::from_str(text).expect("health resource must be valid JSON");
    assert_eq!(health["status"], "ok", "health status should be 'ok'");
}

#[tokio::test]
async fn prompts_list() {
    let (base, _server) = start_server().await;
    let session_id = initialize(&base).await;

    let result =
        session_request(&base, &session_id, 2, "prompts/list", serde_json::json!({})).await;

    let prompts = result["result"]["prompts"]
        .as_array()
        .expect("prompts/list must return an array");

    assert_eq!(
        prompts.len(),
        4,
        "expected 4 prompts, got {}",
        prompts.len()
    );

    let names: Vec<&str> = prompts.iter().filter_map(|p| p["name"].as_str()).collect();
    assert!(
        names.contains(&"explore-graph"),
        "explore-graph prompt must exist"
    );
    assert!(
        names.contains(&"query-helper"),
        "query-helper prompt must exist"
    );
    assert!(
        names.contains(&"import-guide"),
        "import-guide prompt must exist"
    );
    assert!(names.contains(&"text2gql"), "text2gql prompt must exist");
}

#[tokio::test]
async fn logging_set_level() {
    let (base, _server) = start_server().await;
    let session_id = initialize(&base).await;

    let result = session_request(
        &base,
        &session_id,
        2,
        "logging/setLevel",
        serde_json::json!({
            "level": "debug"
        }),
    )
    .await;

    // logging/setLevel returns an empty result on success.
    assert!(
        result.get("result").is_some(),
        "logging/setLevel must return a result"
    );
    assert!(
        result.get("error").is_none(),
        "logging/setLevel must not return an error"
    );
}

// ── Protocol edge cases ─────────────────────────────────────────────

#[tokio::test]
async fn missing_accept_header_rejected() {
    let (base, _server) = start_server().await;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "test", "version": "0.1.0" }
        }
    });

    // Send without proper Accept header.
    let resp = client()
        .post(format!("{base}/mcp"))
        .header(CONTENT_TYPE, "application/json")
        .json(&body)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        406,
        "request without Accept: text/event-stream should be 406 Not Acceptable"
    );
}

#[tokio::test]
async fn wrong_content_type_rejected() {
    let (base, _server) = start_server().await;

    let resp = client()
        .post(format!("{base}/mcp"))
        .header(CONTENT_TYPE, "text/plain")
        .header(ACCEPT, "application/json, text/event-stream")
        .body(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        415,
        "request with wrong Content-Type should be 415 Unsupported Media Type"
    );
}

#[tokio::test]
async fn request_without_session_requires_initialize() {
    let (base, _server) = start_server().await;

    // Send a non-initialize request without a session ID.
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    });

    let resp = mcp_post(&base, &body, None).await;
    assert_eq!(
        resp.status(),
        422,
        "non-initialize request without session should be 422 Unprocessable Entity"
    );
}

#[tokio::test]
async fn invalid_session_id_rejected() {
    let (base, _server) = start_server().await;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    });

    let resp = mcp_post(&base, &body, Some("nonexistent-session-id")).await;
    assert_eq!(
        resp.status(),
        404,
        "request with invalid session ID should be 404 Not Found"
    );

    // Verify enriched JSON body with recovery instructions.
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("application/json"),
        "expired session response should be JSON, got: {content_type}"
    );

    let retry_after = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        retry_after, "0",
        "Retry-After header should be 0 for immediate retry"
    );

    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["error"], "session_expired");
    assert!(json["message"].as_str().unwrap().contains("expired"));
    assert_eq!(
        json["ttl_seconds"], 3600,
        "ttl_seconds should match the default MCP session timeout"
    );
    assert!(json["recovery"].as_str().unwrap().contains("initialize"));
}

// ── Helpers for TTL tier tests ────────────────────────────────────

/// Helper: call an MCP tool and return the text content from the result.
async fn call_tool_text(
    base: &str,
    sid: &str,
    id: u64,
    name: &str,
    args: serde_json::Value,
) -> String {
    tool_text(&call_tool(base, sid, id, name, args).await)
}

// ── TTL Tiers in Memory ────────────────────────────────────────────

#[tokio::test]
async fn memory_ttl_tier_config_and_validation() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Configure memory namespace with TTL tiers.
    let config_text = call_tool_text(
        &base,
        &sid,
        2,
        "configure_memory",
        serde_json::json!({
            "namespace": "tier_test",
            "max_memories": 100,
            "ttl_tiers": "{\"ephemeral\":3600000,\"session\":86400000,\"persistent\":0}"
        }),
    )
    .await;
    assert!(
        config_text.contains("tier_test"),
        "should confirm config: {config_text}"
    );

    // Verify tiers stored via GQL.
    let verify_text = call_tool_text(
        &base,
        &sid,
        3,
        "gql_query",
        serde_json::json!({
            "query": "MATCH (c:__MemoryConfig) FILTER c.namespace = 'tier_test' RETURN c.ttl_tiers AS tiers"
        }),
    )
    .await;
    assert!(
        verify_text.contains("ephemeral")
            && verify_text.contains("session")
            && verify_text.contains("persistent"),
        "tiers should be stored in config: {verify_text}"
    );

    // Invalid tiers JSON should be rejected.
    let bad_json = session_request(
        &base,
        &sid,
        4,
        "tools/call",
        serde_json::json!({
            "name": "configure_memory",
            "arguments": {
                "namespace": "tier_test",
                "ttl_tiers": "not valid json"
            }
        }),
    )
    .await;
    assert!(
        bad_json.get("error").is_some(),
        "invalid tiers JSON should be rejected: {bad_json}"
    );

    // Unknown tier should fail in remember.
    let bad_tier = session_request(
        &base,
        &sid,
        5,
        "tools/call",
        serde_json::json!({
            "name": "remember",
            "arguments": {
                "namespace": "tier_test",
                "content": "bad tier",
                "tier": "nonexistent"
            }
        }),
    )
    .await;
    let error_msg = bad_tier
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("");
    assert!(
        error_msg.contains("unknown tier 'nonexistent'"),
        "should report unknown tier: {bad_tier}"
    );

    // Tier + valid_until should fail (mutually exclusive).
    let conflict = session_request(
        &base,
        &sid,
        6,
        "tools/call",
        serde_json::json!({
            "name": "remember",
            "arguments": {
                "namespace": "tier_test",
                "content": "conflict test",
                "tier": "ephemeral",
                "valid_until": 9_999_999_999_999_i64
            }
        }),
    )
    .await;
    let conflict_msg = conflict
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("");
    assert!(
        conflict_msg.contains("mutually exclusive"),
        "tier + valid_until should be rejected: {conflict}"
    );
}
