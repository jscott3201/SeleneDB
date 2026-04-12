//! Shared test helpers for selene-server integration tests.
//!
//! Provides `TestServer` (owns TempDir + base URL), auth context factories,
//! and MCP/SSE protocol helpers. Include via `mod support;` in test files.

#![allow(dead_code, unused_imports)]

use std::sync::Arc;

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use selene_core::NodeId;
use selene_server::auth::Role;
use selene_server::auth::handshake::AuthContext;
use selene_server::bootstrap::{self, ServerState};
use selene_server::config::SeleneConfig;

// Re-export selene-testing helpers for convenience.
pub use selene_testing::helpers::{labels, props};

// ── TestServer ─────────────────────────────────────────────────────

/// A test HTTP server that owns its data directory.
///
/// Aborts the server task and cleans up the `TempDir` on drop instead of
/// leaking via `std::mem::forget`.
pub struct TestServer {
    pub base_url: String,
    pub state: Arc<ServerState>,
    _handle: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self._handle.abort();
    }
}

impl TestServer {
    /// Boot a dev-mode HTTP server on a random port.
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SeleneConfig::dev(dir.path());
        config.http.listen_addr = "127.0.0.1:0".parse().unwrap();

        selene_server::ops::init_start_time();
        let state = bootstrap::bootstrap(config, None).await.unwrap();
        let state = Arc::new(state);

        let app = selene_server::http::router(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{addr}"),
            state,
            _handle: handle,
            _dir: dir,
        }
    }

    /// Boot a production-mode HTTP server (dev_mode=false) with an API key.
    pub async fn start_with_api_key(api_key: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut config = SeleneConfig::dev(dir.path());
        config.dev_mode = false;
        config.mcp.enabled = true;
        config.mcp.api_key = Some(api_key.into());
        config.http.listen_addr = "127.0.0.1:0".parse().unwrap();

        selene_server::ops::init_start_time();
        let state = bootstrap::bootstrap(config, None).await.unwrap();
        let state = Arc::new(state);

        let app = selene_server::http::router(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{addr}"),
            state,
            _handle: handle,
            _dir: dir,
        }
    }
}

// ── Auth factories ─────────────────────────────────────────────────

/// Dev admin context with full access.
pub fn admin() -> AuthContext {
    AuthContext::dev_admin()
}

/// Read-only context with empty scope.
#[allow(dead_code)]
pub fn reader() -> AuthContext {
    AuthContext {
        principal_node_id: NodeId(999),
        role: Role::Reader,
        scope: roaring::RoaringBitmap::new(),
        scope_generation: 0,
    }
}

/// Operator context scoped to the given node IDs.
///
/// # Panics
/// Panics if any ID exceeds `u32::MAX` (RoaringBitmap limitation).
pub fn scoped(scope_ids: &[u64]) -> AuthContext {
    let mut scope = roaring::RoaringBitmap::new();
    for &id in scope_ids {
        let id32 = u32::try_from(id).unwrap_or_else(|_| {
            panic!("scoped() node ID {id} exceeds u32::MAX — RoaringBitmap cannot represent it")
        });
        scope.insert(id32);
    }
    AuthContext {
        principal_node_id: NodeId(999),
        role: Role::Operator,
        scope,
        scope_generation: 0,
    }
}

// ── HTTP client ────────────────────────────────────────────────────

/// Default HTTP client for test requests.
pub fn client() -> reqwest::Client {
    reqwest::Client::new()
}

// ── MCP protocol helpers ───────────────────────────────────────────

/// POST a JSON-RPC message to `/mcp` and return the raw response.
pub async fn mcp_post(
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
pub fn parse_sse_results(body: &str) -> Vec<serde_json::Value> {
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

/// Run the MCP initialize handshake and return the session ID.
pub async fn initialize(base: &str) -> String {
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

    let body = resp.text().await.unwrap();
    let results = parse_sse_results(&body);
    assert!(
        !results.is_empty(),
        "initialize SSE body should contain at least one JSON-RPC message"
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
pub async fn session_request(
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

/// Call an MCP tool and return the full JSON-RPC result object.
pub async fn call_tool(
    base: &str,
    session_id: &str,
    id: u64,
    tool_name: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let result = session_request(
        base,
        session_id,
        id,
        "tools/call",
        serde_json::json!({ "name": tool_name, "arguments": arguments }),
    )
    .await;
    assert!(
        result.get("error").is_none(),
        "{tool_name} should not error: {result}"
    );
    result
}

/// Extract the text content from an MCP tool result.
///
/// Panics with a clear message if the response shape is unexpected.
pub fn tool_text(result: &serde_json::Value) -> String {
    let content = result
        .get("result")
        .and_then(|r| r.get("content"))
        .and_then(|c| c.as_array())
        .expect("expected MCP tool response result.content to be an array");
    let first = content
        .first()
        .expect("expected MCP tool response result.content to have at least one item");
    first
        .get("text")
        .and_then(|t| t.as_str())
        .expect("expected MCP tool response result.content[0].text to be a string")
        .to_string()
}

/// Boot a dev-mode HTTP server and return `(base_url, TestServer)`.
///
/// The `TestServer` handle must be held alive for the duration of the test.
/// Its `TempDir` is cleaned up on drop (no `std::mem::forget` leak).
pub async fn start_server() -> (String, TestServer) {
    let server = TestServer::start().await;
    let url = server.base_url.clone();
    (url, server)
}
