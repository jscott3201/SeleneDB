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

// ── Trace system tests ────────────────────────────────────────────

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

#[tokio::test]
async fn trace_log_with_thinking_and_user_query() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Log a trace with the new fields.
    let text = call_tool_text(
        &base,
        &sid,
        2,
        "log_trace",
        serde_json::json!({
            "session_id": "test-session-1",
            "turn": 0,
            "tool_name": "get_zone_comfort",
            "tool_params": "{\"zone\": \"Zone 301\"}",
            "tool_result_summary": "comfort_score: 0.85",
            "agent_response": "Zone 301 is comfortable.",
            "thinking": "I should check the comfort score for Zone 301.",
            "user_query": "How comfortable is Zone 301?"
        }),
    )
    .await;
    assert!(text.contains("traceId"), "should return traceId: {text}");

    // Export and verify the new fields appear.
    let exported = call_tool_text(
        &base,
        &sid,
        3,
        "export_traces",
        serde_json::json!({
            "session_id": "test-session-1",
            "format": "jsonl"
        }),
    )
    .await;
    assert!(
        exported.contains("thinking"),
        "exported trace should contain thinking field: {exported}"
    );
    assert!(
        exported.contains("How comfortable is Zone 301?"),
        "exported trace should contain user_query: {exported}"
    );
}

#[tokio::test]
async fn trace_about_edges() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Create an entity node and get its ID.
    call_tool_text(
        &base,
        &sid,
        2,
        "gql_query",
        serde_json::json!({
            "query": "INSERT (:Zone {name: 'Zone 301'})"
        }),
    )
    .await;

    // Query back the zone ID.
    let query_text = call_tool_text(
        &base,
        &sid,
        3,
        "gql_query",
        serde_json::json!({
            "query": "MATCH (z:Zone {name: 'Zone 301'}) RETURN id(z) AS zid"
        }),
    )
    .await;

    // Extract the zone ID from the result text (id() returns Int).
    let zid: i64 = {
        let json_start = query_text.find('[').unwrap_or(0);
        let json_end = query_text.rfind(']').map_or(query_text.len(), |i| i + 1);
        let json_str = &query_text[json_start..json_end];
        let rows: Vec<serde_json::Value> = serde_json::from_str(json_str).unwrap_or_default();
        rows[0]["zid"].as_i64().expect("zone ID must be a number")
    };

    // Log a trace with about_node_ids pointing to the zone.
    call_tool_text(
        &base,
        &sid,
        4,
        "log_trace",
        serde_json::json!({
            "session_id": "test-about",
            "turn": 0,
            "tool_name": "get_zone_comfort",
            "tool_params": "{}",
            "tool_result_summary": "ok",
            "about_node_ids": [zid]
        }),
    )
    .await;

    // Verify the :about edge exists.
    let edge_text = call_tool_text(
        &base,
        &sid,
        5,
        "gql_query",
        serde_json::json!({
            "query": "MATCH ()-[e:about]->() RETURN count(e) AS cnt"
        }),
    )
    .await;
    // Should find exactly 1 :about edge.
    assert!(
        edge_text.contains("\"cnt\":1"),
        "should find 1 about edge: {edge_text}"
    );
}

#[tokio::test]
async fn trace_sequential_edges() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Log 3 sequential turns.
    for turn in 0..3 {
        call_tool_text(
            &base,
            &sid,
            turn + 2,
            "log_trace",
            serde_json::json!({
                "session_id": "test-seq",
                "turn": turn,
                "tool_name": format!("tool_{turn}"),
                "tool_params": "{}",
                "tool_result_summary": "ok"
            }),
        )
        .await;
    }

    // Verify :next_turn edges exist.
    let edge_text = call_tool_text(
        &base,
        &sid,
        5,
        "gql_query",
        serde_json::json!({
            "query": "MATCH (a)-[e:next_turn]->(b) RETURN a.turn AS from_turn, b.turn AS to_turn ORDER BY a.turn"
        }),
    )
    .await;

    // Should have edges: 0->1 and 1->2.
    assert!(
        edge_text.contains("2 rows"),
        "should have 2 next_turn edges: {edge_text}"
    );
}

#[tokio::test]
async fn trace_log_session() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    let text = call_tool_text(
        &base,
        &sid,
        2,
        "log_session",
        serde_json::json!({
            "session_id": "test-session-meta",
            "system_prompt": "You are Athena, a building intelligence agent.",
            "building": "Maple Street Tower",
            "operator_role": "facilities_manager",
            "weather": "45°F, cloudy",
            "active_tier": "peak"
        }),
    )
    .await;
    assert!(
        text.contains("sessionId"),
        "should return sessionId: {text}"
    );

    // Verify node exists.
    let query_text = call_tool_text(
        &base,
        &sid,
        3,
        "gql_query",
        serde_json::json!({
            "query": "MATCH (s:__TraceSession {session_id: 'test-session-meta'}) \
                      RETURN s.building AS building, s.system_prompt AS sp"
        }),
    )
    .await;
    assert!(
        query_text.contains("Maple Street Tower"),
        "should find building: {query_text}"
    );
}

#[tokio::test]
async fn trace_log_session_idempotent() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Call log_session twice with different data.
    call_tool_text(
        &base,
        &sid,
        2,
        "log_session",
        serde_json::json!({
            "session_id": "test-idempotent",
            "building": "Building A"
        }),
    )
    .await;

    call_tool_text(
        &base,
        &sid,
        3,
        "log_session",
        serde_json::json!({
            "session_id": "test-idempotent",
            "building": "Building B"
        }),
    )
    .await;

    // Should have exactly one node (MERGE), with updated building.
    let query_text = call_tool_text(
        &base,
        &sid,
        4,
        "gql_query",
        serde_json::json!({
            "query": "MATCH (s:__TraceSession {session_id: 'test-idempotent'}) RETURN s.building AS building"
        }),
    )
    .await;
    assert!(
        query_text.contains("Building B"),
        "should have updated building: {query_text}"
    );
    assert!(
        query_text.contains("1 rows"),
        "should have exactly 1 node (MERGE): {query_text}"
    );
}

#[tokio::test]
async fn trace_log_outcome() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    let text = call_tool_text(
        &base,
        &sid,
        2,
        "log_outcome",
        serde_json::json!({
            "session_id": "test-outcome",
            "success": true,
            "outcome_summary": "Zone reached target temperature within 20 minutes.",
            "quality_score": 9
        }),
    )
    .await;
    assert!(
        text.contains("outcomeId"),
        "should return outcomeId: {text}"
    );

    // Verify node exists.
    let query_text = call_tool_text(
        &base,
        &sid,
        3,
        "gql_query",
        serde_json::json!({
            "query": "MATCH (o:__TraceOutcome {session_id: 'test-outcome'}) \
                      RETURN o.success AS success, o.quality_score AS score"
        }),
    )
    .await;
    assert!(
        query_text.contains("1 rows"),
        "should have 1 outcome node: {query_text}"
    );
}

#[tokio::test]
async fn trace_export_huggingface() {
    let (base, _server) = start_server().await;
    let sid = initialize(&base).await;

    // Set up session metadata.
    call_tool_text(
        &base,
        &sid,
        2,
        "log_session",
        serde_json::json!({
            "session_id": "hf-test",
            "system_prompt": "You are a building agent.",
            "building": "Test Tower"
        }),
    )
    .await;

    // Log 2 traces.
    call_tool_text(
        &base,
        &sid,
        3,
        "log_trace",
        serde_json::json!({
            "session_id": "hf-test",
            "turn": 0,
            "tool_name": "get_zone_comfort",
            "tool_params": "{\"zone\": \"Zone 1\"}",
            "tool_result_summary": "comfort: 0.9",
            "agent_response": "Zone 1 is comfortable.",
            "thinking": "Check Zone 1 first.",
            "user_query": "How is Zone 1?"
        }),
    )
    .await;

    call_tool_text(
        &base,
        &sid,
        4,
        "log_trace",
        serde_json::json!({
            "session_id": "hf-test",
            "turn": 1,
            "tool_name": "adjust_setpoint",
            "tool_params": "{\"zone\": \"Zone 2\", \"temp\": 72}",
            "tool_result_summary": "setpoint updated",
            "agent_response": "Adjusted Zone 2 to 72°F.",
            "user_query": "Warm up Zone 2."
        }),
    )
    .await;

    // Log outcome.
    call_tool_text(
        &base,
        &sid,
        5,
        "log_outcome",
        serde_json::json!({
            "session_id": "hf-test",
            "success": true,
            "outcome_summary": "All zones comfortable.",
            "quality_score": 8
        }),
    )
    .await;

    // Export in HuggingFace format.
    let exported = call_tool_text(
        &base,
        &sid,
        6,
        "export_traces",
        serde_json::json!({
            "session_id": "hf-test",
            "format": "huggingface"
        }),
    )
    .await;

    // Verify the structure.
    assert!(
        exported.contains("HuggingFace chat"),
        "header should mention HuggingFace: {exported}"
    );

    // Parse the JSONL line (skip the header line).
    let lines: Vec<&str> = exported.lines().collect();
    assert!(
        lines.len() >= 2,
        "should have header + at least 1 JSONL line: {exported}"
    );
    let example: serde_json::Value =
        serde_json::from_str(lines[1]).expect("JSONL line should be valid JSON");

    // Check messages array exists and has expected roles.
    let messages = example["messages"]
        .as_array()
        .expect("should have messages array");
    assert!(
        messages.len() >= 5,
        "should have at least 5 messages (system + 2 turns): got {}",
        messages.len()
    );

    // First message should be the system prompt.
    assert_eq!(messages[0]["role"], "system");
    assert!(
        messages[0]["content"]
            .as_str()
            .unwrap()
            .contains("building agent")
    );

    // Should have tools array.
    let tools = example["tools"]
        .as_array()
        .expect("should have tools array");
    assert_eq!(tools.len(), 2, "should list 2 unique tools");

    // Should have metadata with success and quality_score.
    assert_eq!(example["metadata"]["success"], true);
    assert_eq!(example["metadata"]["quality_score"], 8);
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
