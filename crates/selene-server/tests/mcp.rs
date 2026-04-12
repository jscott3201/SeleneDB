//! MCP integration tests over Streamable HTTP.
//!
//! Boots the full HTTP server in dev mode, then exercises the MCP endpoint
//! at `/mcp` using raw JSON-RPC over HTTP with SSE response parsing.
//! Covers the MCP lifecycle: initialize handshake, tool listing and
//! invocation, resource listing and reading, prompts, and logging.

use std::sync::Arc;

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use selene_server::bootstrap;
use selene_server::config::SeleneConfig;

// ── Helpers ──────────────────────────────────────────────────────────

/// Start a test HTTP server on a random port and return the base URL.
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

    // Keep tempdir alive for the process lifetime.
    std::mem::forget(dir);

    format!("http://{addr}")
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

/// POST a JSON-RPC message to `/mcp` and return the raw SSE response body.
/// Includes required headers for the MCP Streamable HTTP protocol.
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

/// Extract JSON-RPC result objects from an SSE response body.
///
/// SSE lines look like:
///   `data:{"jsonrpc":"2.0","id":1,"result":{...}}`
///
/// Priming events may contain `data:` (empty) which are skipped.
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

/// Run the initialize handshake and return the session ID.
async fn initialize(base: &str) -> String {
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

    let resp = mcp_post(base, &init_req, None).await;
    assert_eq!(resp.status(), 200, "initialize should return 200");

    let session_id = resp
        .headers()
        .get("mcp-session-id")
        .expect("initialize response must include mcp-session-id header")
        .to_str()
        .unwrap()
        .to_string();

    // Verify the SSE body contains a valid initialize result.
    let body = resp.text().await.unwrap();
    let results = parse_sse_results(&body);
    assert!(
        !results.is_empty(),
        "initialize SSE body should contain at least one JSON-RPC message"
    );

    let result = &results[0];
    assert_eq!(result["jsonrpc"], "2.0");
    assert_eq!(result["id"], 1);
    assert!(
        result.get("result").is_some(),
        "initialize response must have a 'result' field"
    );

    // Send the `initialized` notification to complete the handshake.
    let initialized_notification = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    });
    let notif_resp = mcp_post(base, &initialized_notification, Some(&session_id)).await;
    assert!(
        notif_resp.status().is_success(),
        "initialized notification should succeed"
    );

    session_id
}

/// Send a JSON-RPC request within an established session and return
/// the first result object from the SSE response.
async fn session_request(
    base: &str,
    session_id: &str,
    id: u64,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });

    let resp = mcp_post(base, &body, Some(session_id)).await;
    assert_eq!(
        resp.status(),
        200,
        "{method} should return 200, got {}",
        resp.status()
    );

    let text = resp.text().await.unwrap();
    let results = parse_sse_results(&text);
    assert!(
        !results.is_empty(),
        "{method} SSE body should contain at least one JSON-RPC message"
    );
    results[0].clone()
}

// ── Tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn initialize_handshake() {
    let base = start_server().await;
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
    let base = start_server().await;
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
    let base = start_server().await;
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
    let base = start_server().await;
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
    let base = start_server().await;
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
    let base = start_server().await;
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
    let base = start_server().await;
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
    let base = start_server().await;
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
    let base = start_server().await;

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
    let base = start_server().await;

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
    let base = start_server().await;

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
async fn call_tool(base: &str, sid: &str, id: u64, name: &str, args: serde_json::Value) -> String {
    let result = session_request(
        base,
        sid,
        id,
        "tools/call",
        serde_json::json!({ "name": name, "arguments": args }),
    )
    .await;
    assert!(
        result.get("error").is_none(),
        "{name} should not error: {result}"
    );
    let content = result["result"]["content"]
        .as_array()
        .expect("tool result must have content");
    content[0]["text"].as_str().unwrap_or("").to_string()
}

#[tokio::test]
async fn trace_log_with_thinking_and_user_query() {
    let base = start_server().await;
    let sid = initialize(&base).await;

    // Log a trace with the new fields.
    let text = call_tool(
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
    let exported = call_tool(
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
    let base = start_server().await;
    let sid = initialize(&base).await;

    // Create an entity node and get its ID.
    call_tool(
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
    let query_text = call_tool(
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
    call_tool(
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
    let edge_text = call_tool(
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
    let base = start_server().await;
    let sid = initialize(&base).await;

    // Log 3 sequential turns.
    for turn in 0..3 {
        call_tool(
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
    let edge_text = call_tool(
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
    let base = start_server().await;
    let sid = initialize(&base).await;

    let text = call_tool(
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
    let query_text = call_tool(
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
    let base = start_server().await;
    let sid = initialize(&base).await;

    // Call log_session twice with different data.
    call_tool(
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

    call_tool(
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
    let query_text = call_tool(
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
    let base = start_server().await;
    let sid = initialize(&base).await;

    let text = call_tool(
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
    let query_text = call_tool(
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
    let base = start_server().await;
    let sid = initialize(&base).await;

    // Set up session metadata.
    call_tool(
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
    call_tool(
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

    call_tool(
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
    call_tool(
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
    let exported = call_tool(
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


// ── Bridge / coordination tool tests ────────────────────────────────

/// Extract a numeric ID from a tool response containing JSON like `[{"id":5}]`.
fn extract_id_from_response(text: &str) -> u64 {
    let start = text.find('[').unwrap_or(0);
    let end = text.rfind(']').map_or(text.len(), |i| i + 1);
    let rows: Vec<serde_json::Value> = serde_json::from_str(&text[start..end]).expect("response should contain JSON array");
    rows[0]["id"].as_u64().expect("first row should have numeric id")
}

#[tokio::test]
async fn bridge_register_agent_and_list() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    let text = call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-reg-list", "project": "test-project-reg", "supported_tools": ["code_review", "testing"], "domain_expertise": ["rust", "security"], "model_family": "claude-opus-4"})).await;
    assert!(text.contains("Agent registered"), "should confirm registration: {text}");
    let list_text = call_tool(&base, &sid, 3, "list_agents", serde_json::json!({"project": "test-project-reg"})).await;
    assert!(list_text.contains("test-reg-list"), "agent missing: {list_text}");
    assert!(list_text.contains("code_review"), "supported_tools missing: {list_text}");
    assert!(list_text.contains("rust"), "domain_expertise missing: {list_text}");
    assert!(list_text.contains("claude-opus-4"), "model_family missing: {list_text}");
    call_tool(&base, &sid, 4, "deregister_agent", serde_json::json!({"agent_id": "test-reg-list"})).await;
}

#[tokio::test]
async fn bridge_heartbeat_working_locally() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-hb-local", "project": "test-project-hb"})).await;
    let text = call_tool(&base, &sid, 3, "heartbeat", serde_json::json!({"agent_id": "test-hb-local", "status": "working_locally"})).await;
    assert!(text.contains("Heartbeat OK"), "heartbeat should succeed: {text}");
    call_tool(&base, &sid, 4, "deregister_agent", serde_json::json!({"agent_id": "test-hb-local"})).await;
}

#[tokio::test]
async fn bridge_deregister_releases_tasks() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-dereg-tasks", "project": "test-project-dereg"})).await;
    let propose_text = call_tool(&base, &sid, 3, "propose_task", serde_json::json!({"proposer_agent": "test-dereg-tasks", "project": "test-project-dereg", "title": "Deregister test task", "description": "Task for deregister test", "assignee_agent": "test-dereg-tasks"})).await;
    let task_id = extract_id_from_response(&propose_text);
    call_tool(&base, &sid, 4, "accept_task", serde_json::json!({"agent_id": "test-dereg-tasks", "task_id": task_id})).await;
    call_tool(&base, &sid, 5, "deregister_agent", serde_json::json!({"agent_id": "test-dereg-tasks"})).await;
    let list_text = call_tool(&base, &sid, 6, "list_tasks", serde_json::json!({"project": "test-project-dereg"})).await;
    assert!(list_text.contains("proposed"), "task should revert to proposed: {list_text}");
}

#[tokio::test]
async fn bridge_share_and_get_context() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-ctx-agent", "project": "test-project-ctx"})).await;
    let share_text = call_tool(&base, &sid, 3, "share_context", serde_json::json!({"author": "test-ctx-agent", "context_type": "discovery", "scope": "test-project-ctx", "content": "Found a critical performance bottleneck in query planner"})).await;
    assert!(share_text.contains("Context shared"), "should confirm sharing: {share_text}");
    let get_text = call_tool(&base, &sid, 4, "get_shared_context", serde_json::json!({"scope": "test-project-ctx"})).await;
    assert!(get_text.contains("critical performance bottleneck"), "should retrieve shared content: {get_text}");
    call_tool(&base, &sid, 5, "deregister_agent", serde_json::json!({"agent_id": "test-ctx-agent"})).await;
}

#[tokio::test]
async fn bridge_claim_and_release_intent() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-intent-a", "project": "test-project-intent"})).await;
    let claim_text = call_tool(&base, &sid, 3, "claim_intent", serde_json::json!({"agent_id": "test-intent-a", "action": "refactoring optimizer", "targets": ["crates/selene-gql/src/optimizer"], "level": "exclusive"})).await;
    assert!(claim_text.contains("Intent claimed"), "should confirm claim: {claim_text}");
    let conflicts_text = call_tool(&base, &sid, 4, "check_conflicts", serde_json::json!({"agent_id": "test-intent-b", "targets": ["crates/selene-gql/src/optimizer/rules.rs"]})).await;
    assert!(conflicts_text.contains("conflict"), "should detect overlap: {conflicts_text}");
    let release_text = call_tool(&base, &sid, 5, "release_intent", serde_json::json!({"agent_id": "test-intent-a"})).await;
    assert!(release_text.contains("released"), "should confirm release: {release_text}");
    let no_conflicts = call_tool(&base, &sid, 6, "check_conflicts", serde_json::json!({"agent_id": "test-intent-b", "targets": ["crates/selene-gql/src/optimizer"]})).await;
    assert!(no_conflicts.contains("No conflicts"), "no conflicts after release: {no_conflicts}");
    call_tool(&base, &sid, 7, "deregister_agent", serde_json::json!({"agent_id": "test-intent-a"})).await;
}

#[tokio::test]
async fn bridge_find_capable_agent_scoring() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-cap-tester", "project": "test-project-cap", "supported_tools": ["testing", "benchmarking"], "domain_expertise": ["performance"]})).await;
    call_tool(&base, &sid, 3, "register_agent", serde_json::json!({"agent_id": "test-cap-security", "project": "test-project-cap", "supported_tools": ["code_review", "security_audit"], "domain_expertise": ["security", "cryptography"]})).await;
    let find_text = call_tool(&base, &sid, 4, "find_capable_agent", serde_json::json!({"required_tools": ["testing"], "project": "test-project-cap"})).await;
    assert!(find_text.contains("test-cap-tester"), "tester should appear: {find_text}");
    let json_start = find_text.find('[').unwrap_or(0);
    let json_end = find_text.rfind(']').map_or(find_text.len(), |i| i + 1);
    let agents: Vec<serde_json::Value> = serde_json::from_str(&find_text[json_start..json_end]).unwrap_or_default();
    assert!(!agents.is_empty(), "should find at least one agent: {find_text}");
    assert_eq!(agents[0]["agent_id"], "test-cap-tester", "tester should rank first");
    call_tool(&base, &sid, 5, "deregister_agent", serde_json::json!({"agent_id": "test-cap-tester"})).await;
    call_tool(&base, &sid, 6, "deregister_agent", serde_json::json!({"agent_id": "test-cap-security"})).await;
}

#[tokio::test]
async fn bridge_agent_stats_lifecycle() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-stats-agent", "project": "test-project-stats"})).await;
    let propose_text = call_tool(&base, &sid, 3, "propose_task", serde_json::json!({"proposer_agent": "test-stats-agent", "project": "test-project-stats", "title": "Stats test task", "description": "A task for stats testing", "assignee_agent": "test-stats-agent"})).await;
    let task_id = extract_id_from_response(&propose_text);
    call_tool(&base, &sid, 4, "accept_task", serde_json::json!({"agent_id": "test-stats-agent", "task_id": task_id})).await;
    let complete_text = call_tool(&base, &sid, 5, "complete_task", serde_json::json!({"agent_id": "test-stats-agent", "task_id": task_id, "success": true, "output_data": "{\"result\": \"all tests passed\"}"})).await;
    assert!(complete_text.contains("completed"), "should confirm completion: {complete_text}");
    let list_text = call_tool(&base, &sid, 6, "list_tasks", serde_json::json!({"project": "test-project-stats"})).await;
    assert!(list_text.contains("completed"), "task should show completed: {list_text}");
    let stats_text = call_tool(&base, &sid, 7, "agent_stats", serde_json::json!({"agent_id": "test-stats-agent"})).await;
    let stats: serde_json::Value = serde_json::from_str(&stats_text).expect("stats should be valid JSON");
    assert_eq!(stats["agent_id"], "test-stats-agent", "should report correct agent: {stats_text}");
    assert!(stats.get("tasks_completed").is_some(), "should have tasks_completed field: {stats_text}");
    assert!(stats.get("tasks_failed").is_some(), "should have tasks_failed field: {stats_text}");
    assert!(stats.get("success_rate").is_some(), "should have success_rate field: {stats_text}");
    assert!(stats.get("recent_tasks").is_some(), "should have recent_tasks field: {stats_text}");
    call_tool(&base, &sid, 8, "deregister_agent", serde_json::json!({"agent_id": "test-stats-agent"})).await;
}

#[tokio::test]
async fn bridge_task_lifecycle_happy_path() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-lifecycle-agent", "project": "test-project-lifecycle"})).await;
    let propose_text = call_tool(&base, &sid, 3, "propose_task", serde_json::json!({"proposer_agent": "test-lifecycle-agent", "project": "test-project-lifecycle", "title": "Lifecycle happy path", "description": "Full lifecycle test"})).await;
    let task_id = extract_id_from_response(&propose_text);
    let list1 = call_tool(&base, &sid, 4, "list_tasks", serde_json::json!({"project": "test-project-lifecycle"})).await;
    assert!(list1.contains("proposed"), "task should be proposed: {list1}");
    call_tool(&base, &sid, 5, "accept_task", serde_json::json!({"agent_id": "test-lifecycle-agent", "task_id": task_id})).await;
    let list2 = call_tool(&base, &sid, 6, "list_tasks", serde_json::json!({"project": "test-project-lifecycle"})).await;
    assert!(list2.contains("accepted"), "task should be accepted: {list2}");
    let complete_text = call_tool(&base, &sid, 7, "complete_task", serde_json::json!({"agent_id": "test-lifecycle-agent", "task_id": task_id, "success": true, "output_data": "{\"files_changed\": 3}"})).await;
    assert!(complete_text.contains("completed"), "should confirm completion: {complete_text}");
    let list3 = call_tool(&base, &sid, 8, "list_tasks", serde_json::json!({"project": "test-project-lifecycle"})).await;
    assert!(list3.contains("completed"), "task should be completed: {list3}");
    call_tool(&base, &sid, 9, "deregister_agent", serde_json::json!({"agent_id": "test-lifecycle-agent"})).await;
}

#[tokio::test]
async fn bridge_task_reject_flow() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-reject-agent", "project": "test-project-reject"})).await;
    let propose_text = call_tool(&base, &sid, 3, "propose_task", serde_json::json!({"proposer_agent": "test-reject-agent", "project": "test-project-reject", "title": "Reject test task", "description": "This task will be rejected"})).await;
    let task_id = extract_id_from_response(&propose_text);
    let reject_text = call_tool(&base, &sid, 4, "reject_task", serde_json::json!({"agent_id": "test-reject-agent", "task_id": task_id, "reason": "Out of scope for current sprint"})).await;
    assert!(reject_text.contains("rejected"), "should confirm rejection: {reject_text}");
    let list_text = call_tool(&base, &sid, 5, "list_tasks", serde_json::json!({"project": "test-project-reject"})).await;
    assert!(list_text.contains("rejected"), "task should be rejected: {list_text}");
    call_tool(&base, &sid, 6, "deregister_agent", serde_json::json!({"agent_id": "test-reject-agent"})).await;
}

#[tokio::test]
async fn bridge_task_accept_prevents_stealing() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-steal-a", "project": "test-project-steal"})).await;
    call_tool(&base, &sid, 3, "register_agent", serde_json::json!({"agent_id": "test-steal-b", "project": "test-project-steal"})).await;
    let propose_text = call_tool(&base, &sid, 4, "propose_task", serde_json::json!({"proposer_agent": "test-steal-a", "project": "test-project-steal", "title": "Targeted task", "description": "Only agent A should accept", "assignee_agent": "test-steal-a"})).await;
    let task_id = extract_id_from_response(&propose_text);
    let result = session_request(&base, &sid, 5, "tools/call", serde_json::json!({"name": "accept_task", "arguments": {"agent_id": "test-steal-b", "task_id": task_id}})).await;
    assert!(result.get("error").is_some(), "agent B should not accept task targeted at A: {result}");
    call_tool(&base, &sid, 6, "deregister_agent", serde_json::json!({"agent_id": "test-steal-a"})).await;
    call_tool(&base, &sid, 7, "deregister_agent", serde_json::json!({"agent_id": "test-steal-b"})).await;
}

#[tokio::test]
async fn bridge_task_complete_requires_assignee() {
    let base = start_server().await;
    let sid = initialize(&base).await;
    call_tool(&base, &sid, 2, "register_agent", serde_json::json!({"agent_id": "test-complete-a", "project": "test-project-complete"})).await;
    call_tool(&base, &sid, 3, "register_agent", serde_json::json!({"agent_id": "test-complete-b", "project": "test-project-complete"})).await;
    let propose_text = call_tool(&base, &sid, 4, "propose_task", serde_json::json!({"proposer_agent": "test-complete-a", "project": "test-project-complete", "title": "Assignee-only completion", "description": "Only agent A should complete", "assignee_agent": "test-complete-a"})).await;
    let task_id = extract_id_from_response(&propose_text);
    call_tool(&base, &sid, 5, "accept_task", serde_json::json!({"agent_id": "test-complete-a", "task_id": task_id})).await;
    let result = session_request(&base, &sid, 6, "tools/call", serde_json::json!({"name": "complete_task", "arguments": {"agent_id": "test-complete-b", "task_id": task_id, "success": true}})).await;
    assert!(result.get("error").is_some(), "agent B should not complete task assigned to A: {result}");
    call_tool(&base, &sid, 7, "deregister_agent", serde_json::json!({"agent_id": "test-complete-a"})).await;
    call_tool(&base, &sid, 8, "deregister_agent", serde_json::json!({"agent_id": "test-complete-b"})).await;
}
