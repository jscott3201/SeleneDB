//! End-to-end HTTP API tests.
//!
//! Boots the full HTTP server in-process on a random port and exercises
//! every endpoint through reqwest. Tests the complete lifecycle:
//! schema import → node/edge CRUD → time-series → SQL → graph slice.

mod support;

use base64::Engine as _;
use reqwest::StatusCode;
use sha2::Digest as _;
use support::*;

async fn start_http_server() -> (String, TestServer) {
    start_server().await
}

// ── Health ────────────────────────────────────────────────────────────

#[tokio::test]
async fn health_check_dev_mode() {
    let (base, _server) = start_http_server().await;
    // In dev mode, no auth header still gets full response (dev auto-admin).
    let resp = client().get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["node_count"], 0);
    assert_eq!(body["edge_count"], 0);
    assert_eq!(body["role"], "primary");
}

// ── Node CRUD ────────────────────────────────────────────────────────

#[tokio::test]
async fn node_lifecycle() {
    let (base, _server) = start_http_server().await;
    let c = client();

    // Create — properties use tagged Value enum: {"String": "°F"}, {"Int": 3}
    let resp = c
        .post(format!("{base}/nodes"))
        .json(&serde_json::json!({
            "labels": ["sensor", "temperature"],
            "properties": {"unit": {"String": "°F"}, "floor": {"Int": 3}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let node: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(node["id"], 1);
    assert!(
        node["labels"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("sensor"))
    );

    // Get
    let resp = c.get(format!("{base}/nodes/1")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["id"], 1);

    // Modify
    let resp = c
        .put(format!("{base}/nodes/1"))
        .json(&serde_json::json!({
            "set_properties": {"value": 72.5},
            "add_labels": ["active"],
            "remove_properties": ["floor"]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let modified: serde_json::Value = resp.json().await.unwrap();
    // Response now uses plain JSON values (not tagged enums)
    assert_eq!(modified["properties"]["value"], 72.5);
    assert!(
        modified["labels"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("active"))
    );
    assert!(
        !modified["properties"]
            .as_object()
            .unwrap()
            .contains_key("floor")
    );

    // List
    let resp = c
        .get(format!("{base}/nodes?label=sensor"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(list["total"], 1);

    // Delete
    let resp = c.delete(format!("{base}/nodes/1")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify gone
    let resp = c.get(format!("{base}/nodes/1")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn node_not_found() {
    let (base, _server) = start_http_server().await;
    let resp = client()
        .get(format!("{base}/nodes/999"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── Edge CRUD ────────────────────────────────────────────────────────

#[tokio::test]
async fn edge_lifecycle() {
    let (base, _server) = start_http_server().await;
    let c = client();

    // Create two nodes
    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["building"]}))
        .send()
        .await
        .unwrap();
    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["sensor"]}))
        .send()
        .await
        .unwrap();

    // Create edge
    let resp = c
        .post(format!("{base}/edges"))
        .json(&serde_json::json!({
            "source": 1, "target": 2, "label": "contains",
            "properties": {"floor": 3}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let edge: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(edge["source"], 1);
    assert_eq!(edge["target"], 2);
    assert_eq!(edge["label"], "contains");

    // Get
    let resp = c
        .get(format!("{base}/edges/{}", edge["id"]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Modify
    let resp = c
        .put(format!("{base}/edges/{}", edge["id"]))
        .json(&serde_json::json!({"set_properties": {"priority": 1}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let modified: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(modified["properties"]["priority"], 1);

    // List
    let resp = c
        .get(format!("{base}/edges?label=contains"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(list["total"], 1);

    // Delete
    let resp = c
        .delete(format!("{base}/edges/{}", edge["id"]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

// ── Containment (parent_id) ──────────────────────────────────────────

#[tokio::test]
async fn create_node_with_parent_id() {
    let (base, _server) = start_http_server().await;
    let c = client();

    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["building"], "properties": {"name": "HQ"}}))
        .send()
        .await
        .unwrap();

    let resp = c
        .post(format!("{base}/nodes"))
        .json(&serde_json::json!({
            "labels": ["sensor"],
            "parent_id": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Verify containment edge was auto-created
    let resp = c
        .get(format!("{base}/edges?label=contains"))
        .send()
        .await
        .unwrap();
    let edges: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(edges["total"], 1);
    assert_eq!(edges["edges"][0]["source"], 1);
    assert_eq!(edges["edges"][0]["target"], 2);
}

// ── Time-Series ──────────────────────────────────────────────────────

#[tokio::test]
async fn ts_write_and_query() {
    let (base, _server) = start_http_server().await;
    let c = client();

    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["sensor"]}))
        .send()
        .await
        .unwrap();

    // Write samples
    let resp = c
        .post(format!("{base}/ts/write"))
        .json(&serde_json::json!({
            "samples": [
                {"entity_id": 1, "property": "temp", "timestamp_nanos": 1_000_000_000, "value": 72.5},
                {"entity_id": 1, "property": "temp", "timestamp_nanos": 2_000_000_000, "value": 73.0},
                {"entity_id": 1, "property": "temp", "timestamp_nanos": 3_000_000_000_i64, "value": 73.5}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["written"], 3);

    // Query range
    let resp = c
        .get(format!("{base}/ts/1/temp?start=1000000000&end=2000000000"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let samples: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(samples.len(), 2);
}

// ── SQL Query ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "SQL endpoint removed — use GQL"]
async fn sql_query() {
    let (base, _server) = start_http_server().await;
    let c = client();

    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["sensor"], "properties": {"name": "S1"}}))
        .send()
        .await
        .unwrap();
    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["building"], "properties": {"name": "HQ"}}))
        .send()
        .await
        .unwrap();

    let resp = c
        .post(format!("{base}/sql"))
        .json(&serde_json::json!({"sql": "SELECT count(*) as cnt FROM nodes"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["row_count"], 1);
    assert!(body["rows"][0]["cnt"].as_i64().unwrap() >= 2);
}

#[tokio::test]
#[ignore = "SQL endpoint removed — use GQL"]
async fn sql_rejects_dml() {
    let (base, _server) = start_http_server().await;
    let resp = client()
        .post(format!("{base}/sql"))
        .json(&serde_json::json!({"sql": "DROP TABLE nodes"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── DSL Query ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "DSL endpoint removed — use GQL"]
async fn dsl_query() {
    let (base, _server) = start_http_server().await;
    let c = client();

    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["sensor"], "properties": {"name": "S1"}}))
        .send()
        .await
        .unwrap();

    let resp = c
        .post(format!("{base}/dsl"))
        .json(&serde_json::json!({"pipeline": "nodes | select id | limit 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["row_count"], 1);
}

// ── Graph Slice ──────────────────────────────────────────────────────

#[tokio::test]
async fn graph_slice_full() {
    let (base, _server) = start_http_server().await;
    let c = client();

    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["a"]}))
        .send()
        .await
        .unwrap();
    c.post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["b"]}))
        .send()
        .await
        .unwrap();
    c.post(format!("{base}/edges"))
        .json(&serde_json::json!({"source": 1, "target": 2, "label": "link"}))
        .send()
        .await
        .unwrap();

    let resp = c
        .post(format!("{base}/graph/slice"))
        .json(&serde_json::json!({"slice_type": "full"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(body["edges"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn graph_slice_with_pagination() {
    let (base, _server) = start_http_server().await;
    let c = client();

    for i in 0..5 {
        c.post(format!("{base}/nodes"))
            .json(&serde_json::json!({"labels": ["item"], "properties": {"idx": i}}))
            .send()
            .await
            .unwrap();
    }

    let resp = c
        .post(format!("{base}/graph/slice"))
        .json(&serde_json::json!({"slice_type": "full", "limit": 2, "offset": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(body["total_nodes"], 5);
}

// ── Schema Management ────────────────────────────────────────────────

#[tokio::test]
async fn schema_lifecycle() {
    let (base, _server) = start_http_server().await;
    let c = client();

    // Register node schema
    let resp = c
        .post(format!("{base}/schemas/nodes"))
        .json(&serde_json::json!({
            "label": "test_sensor",
            "properties": [
                {"name": "unit", "value_type": "String", "required": false,
                 "default": {"String": "°F"}, "description": "", "indexed": false}
            ],
            "valid_edge_labels": [],
            "description": "Test sensor",
            "annotations": {}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // List schemas
    let resp = c.get(format!("{base}/schemas")).send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    let node_schemas = body["node_schemas"].as_array().unwrap();
    assert!(node_schemas.iter().any(|s| s["label"] == "test_sensor"));

    // Get specific schema
    let resp = c
        .get(format!("{base}/schemas/nodes/test_sensor"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Delete schema
    let resp = c
        .delete(format!("{base}/schemas/nodes/test_sensor"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify gone
    let resp = c
        .get(format!("{base}/schemas/nodes/test_sensor"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn import_schema_pack() {
    let (base, _server) = start_http_server().await;
    let c = client();

    let toml = r#"
name = "test"
version = "0.1"
description = "Test pack"

[types.widget]
description = "A test widget"
fields = { color = "string = 'blue'" }
"#;

    let resp = c
        .post(format!("{base}/schemas/import"))
        .header("content-type", "text/plain")
        .body(toml.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["pack"], "test");
    assert_eq!(body["node_schemas_registered"], 1);
}

// ── Full Lifecycle ───────────────────────────────────────────────────

#[tokio::test]
#[ignore = "uses removed CRUD HTTP endpoints — needs rewrite to GQL"]
async fn full_building_lifecycle() {
    let (base, _server) = start_http_server().await;
    let c = client();

    // 1. Import common schema pack
    let resp = c
        .post(format!("{base}/schemas/import"))
        .header("content-type", "text/plain")
        .body(include_str!("../../selene-packs/packs/common.toml").to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 2. Create building hierarchy: site → building → floor → zone → sensor
    let resp = c
        .post(format!("{base}/nodes"))
        .json(&serde_json::json!({"labels": ["site"], "properties": {"name": "Campus"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let site: serde_json::Value = resp.json().await.unwrap();

    let resp = c
        .post(format!("{base}/nodes"))
        .json(&serde_json::json!({
            "labels": ["building"],
            "properties": {"name": "HQ"},
            "parent_id": site["id"]
        }))
        .send()
        .await
        .unwrap();
    let building: serde_json::Value = resp.json().await.unwrap();

    let resp = c
        .post(format!("{base}/nodes"))
        .json(&serde_json::json!({
            "labels": ["floor"],
            "properties": {"name": "Floor 1", "level": 1},
            "parent_id": building["id"]
        }))
        .send()
        .await
        .unwrap();
    let floor: serde_json::Value = resp.json().await.unwrap();

    let resp = c
        .post(format!("{base}/nodes"))
        .json(&serde_json::json!({
            "labels": ["zone"],
            "properties": {"name": "Zone A"},
            "parent_id": floor["id"]
        }))
        .send()
        .await
        .unwrap();
    let zone: serde_json::Value = resp.json().await.unwrap();

    let resp = c
        .post(format!("{base}/nodes"))
        .json(&serde_json::json!({
            "labels": ["point"],
            "properties": {"name": "Temp-1", "unit": "°F"},
            "parent_id": zone["id"]
        }))
        .send()
        .await
        .unwrap();
    let sensor: serde_json::Value = resp.json().await.unwrap();

    // 3. Verify containment edges were created
    let resp = c
        .get(format!("{base}/edges?label=contains"))
        .send()
        .await
        .unwrap();
    let edges: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(edges["total"], 4); // site→building, building→floor, floor→zone, zone→sensor

    // 4. Write time-series data
    let sensor_id = sensor["id"].as_u64().unwrap();
    let now = 1_710_000_000_000_000_000_i64;
    let samples: Vec<serde_json::Value> = (0..10)
        .map(|i| {
            serde_json::json!({
                "entity_id": sensor_id,
                "property": "temperature",
                "timestamp_nanos": now + i * 60_000_000_000i64,
                "value": 72.0 + i as f64 * 0.1
            })
        })
        .collect();

    let resp = c
        .post(format!("{base}/ts/write"))
        .json(&serde_json::json!({"samples": samples}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 5. SQL query: join nodes with time-series
    let resp = c
        .post(format!("{base}/sql"))
        .json(&serde_json::json!({
            "sql": format!(
                "SELECT n.id, n.properties->>'name' as name, count(t.value) as readings \
                 FROM nodes n JOIN ts_hot t ON n.id = t.entity_id \
                 WHERE n.id = {sensor_id} \
                 GROUP BY n.id, n.properties->>'name'"
            )
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["row_count"], 1);
    let row = &body["rows"][0];
    assert_eq!(row["name"], "Temp-1");
    assert_eq!(row["readings"], 10);

    // 6. Graph slice: containment subtree from building
    let resp = c
        .post(format!("{base}/graph/slice"))
        .json(&serde_json::json!({
            "slice_type": "containment",
            "root_id": building["id"],
            "max_depth": 10
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let slice: serde_json::Value = resp.json().await.unwrap();
    // Building + floor + zone + sensor = 4 nodes (building is root, included)
    assert!(slice["nodes"].as_array().unwrap().len() >= 3);

    // 7. Health check shows correct counts (dev mode auto-admin gives full response)
    let resp = c.get(format!("{base}/health")).send().await.unwrap();
    let health: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(health["node_count"], 5);
    assert_eq!(health["edge_count"], 4);
}

// ── OAuth Integration Tests ─────────────────────────────────────────

/// Start a test HTTP server in production mode (dev_mode=false) with a
/// configured API key. OAuth endpoints are still available because MCP
/// is enabled.
async fn start_oauth_server() -> (String, TestServer) {
    let server = TestServer::start_with_api_key("test-key-12345").await;
    let url = server.base_url.clone();
    (url, server)
}

/// Client that does not follow redirects (needed for authorize flow).
fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

// ── Test 1: Metadata discovery ──────────────────────────────────────

#[tokio::test]
async fn oauth_metadata_discovery() {
    let (base, _server) = start_http_server().await;
    let resp = client()
        .get(format!("{base}/.well-known/oauth-authorization-server"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["authorization_endpoint"]
            .as_str()
            .unwrap()
            .contains("/oauth/authorize")
    );
    assert!(
        body["token_endpoint"]
            .as_str()
            .unwrap()
            .contains("/oauth/token")
    );
    assert!(
        body["registration_endpoint"]
            .as_str()
            .unwrap()
            .contains("/oauth/register")
    );

    let scopes = body["scopes_supported"]
        .as_array()
        .expect("scopes_supported should be an array");
    assert!(
        scopes.iter().any(|s| s.as_str() == Some("service")),
        "scopes_supported should contain 'service'"
    );
}

// ── Test 2: Client registration + client credentials flow ───────────

#[tokio::test]
async fn oauth_client_credentials_flow() {
    let (base, _server) = start_oauth_server().await;
    let c = client();

    // Register a new OAuth client.
    let reg_resp = c
        .post(format!("{base}/oauth/register"))
        .json(&serde_json::json!({
            "client_name": "test-agent",
            "redirect_uris": ["http://localhost/callback"],
            "scope": "service"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reg_resp.status(), StatusCode::CREATED);

    let reg: serde_json::Value = reg_resp.json().await.unwrap();
    let client_id = reg["client_id"].as_str().unwrap();
    let client_secret = reg["client_secret"].as_str().unwrap();
    assert!(
        client_id.starts_with("mcp-"),
        "client_id should start with 'mcp-'"
    );
    assert!(!client_secret.is_empty(), "client_secret should be present");

    // Exchange credentials for an access token.
    let token_resp = c
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(token_resp.status(), StatusCode::OK);

    let token_body: serde_json::Value = token_resp.json().await.unwrap();
    assert!(
        token_body["access_token"]
            .as_str()
            .is_some_and(|t: &str| !t.is_empty()),
        "access_token should be a non-empty string"
    );
    assert_eq!(token_body["token_type"], "Bearer");
    assert!(
        token_body["refresh_token"]
            .as_str()
            .is_some_and(|t: &str| !t.is_empty()),
        "refresh_token should be present"
    );
}

// ── Test 3: Token refresh ───────────────────────────────────────────

#[tokio::test]
async fn oauth_token_refresh() {
    let (base, _server) = start_oauth_server().await;
    let c = client();

    // Register + get initial tokens.
    let reg: serde_json::Value = c
        .post(format!("{base}/oauth/register"))
        .json(&serde_json::json!({
            "client_name": "refresh-agent",
            "redirect_uris": ["http://localhost/callback"],
            "scope": "service"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let token_body: serde_json::Value = c
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", reg["client_id"].as_str().unwrap()),
            ("client_secret", reg["client_secret"].as_str().unwrap()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let refresh_token = token_body["refresh_token"].as_str().unwrap();

    // Refresh the token (requires client_id + client_secret).
    let refresh_resp = c
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", reg["client_id"].as_str().unwrap()),
            ("client_secret", reg["client_secret"].as_str().unwrap()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(refresh_resp.status(), StatusCode::OK);

    let refreshed: serde_json::Value = refresh_resp.json().await.unwrap();
    assert!(
        refreshed["access_token"]
            .as_str()
            .is_some_and(|t: &str| !t.is_empty()),
        "refreshed access_token should be present"
    );
    assert!(
        refreshed["refresh_token"]
            .as_str()
            .is_some_and(|t: &str| !t.is_empty()),
        "refreshed refresh_token should be present"
    );
    // The new refresh token should differ from the original (single-use).
    assert_ne!(
        refreshed["refresh_token"].as_str().unwrap(),
        refresh_token,
        "refresh tokens must be single-use"
    );
}

// ── Test 4: API key backward compatibility ──────────────────────────

#[tokio::test]
async fn oauth_api_key_backward_compat() {
    let (base, _server) = start_oauth_server().await;
    let c = client();

    // The static API key should grant access to /mcp (not 401).
    let resp = c
        .get(format!("{base}/mcp"))
        .header("Authorization", "Bearer test-key-12345")
        .send()
        .await
        .unwrap();
    // MCP endpoint may return various statuses (e.g. 405 for GET if it
    // expects a different method, or a valid MCP response), but it must
    // NOT be 401 since the API key is valid.
    assert_ne!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "valid API key should not produce 401"
    );
}

// ── Test 5: Unauthenticated rejection ───────────────────────────────

#[tokio::test]
async fn oauth_unauthenticated_rejection() {
    let (base, _server) = start_oauth_server().await;
    let c = client();

    // No Authorization header on a non-dev-mode server.
    let resp = c.get(format!("{base}/mcp")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let body = resp.text().await.unwrap();
    assert!(
        body.contains("Bearer"),
        "401 response should mention Bearer authentication"
    );
}

// ── Test 6: Authorization code + PKCE flow ──────────────────────────

#[tokio::test]
async fn oauth_authorization_code_pkce_flow() {
    let (base, _server) = start_oauth_server().await;
    let c = no_redirect_client();

    // 1. Register a client.
    let reg: serde_json::Value = client()
        .post(format!("{base}/oauth/register"))
        .json(&serde_json::json!({
            "client_name": "pkce-agent",
            "redirect_uris": ["http://localhost/callback"],
            "scope": "service"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let client_id = reg["client_id"].as_str().unwrap();
    let client_secret = reg["client_secret"].as_str().unwrap();
    let redirect_uri = "http://localhost/callback";

    // 2. Generate PKCE code_verifier and code_challenge (S256).
    let code_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    let code_challenge = {
        let digest = sha2::Sha256::digest(code_verifier.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    };

    // 3. Authorize (auto-approve since require_approval defaults to false).
    let auth_resp = c
        .get(format!(
            "{base}/oauth/authorize?response_type=code\
             &client_id={client_id}\
             &redirect_uri={redirect_uri}\
             &code_challenge={code_challenge}\
             &code_challenge_method=S256\
             &state=test123"
        ))
        .send()
        .await
        .unwrap();

    // 4. Expect a redirect (303 or 302) carrying the authorization code.
    assert!(
        auth_resp.status().is_redirection(),
        "authorize should redirect, got {}",
        auth_resp.status()
    );

    let location = auth_resp
        .headers()
        .get("location")
        .expect("redirect should have Location header")
        .to_str()
        .unwrap();

    // Parse the code and state from the redirect URL.
    let url = reqwest::Url::parse(location).unwrap();
    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
    let auth_code = params.get("code").expect("redirect should contain code");
    assert_eq!(
        params.get("state").map(|s| s.as_ref()),
        Some("test123"),
        "state parameter must round-trip"
    );

    // 5. Exchange the authorization code for tokens.
    let token_resp = client()
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", auth_code.as_ref()),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", redirect_uri),
            ("code_verifier", code_verifier),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(token_resp.status(), StatusCode::OK);

    // 6. Verify tokens are present.
    let tokens: serde_json::Value = token_resp.json().await.unwrap();
    assert!(
        tokens["access_token"]
            .as_str()
            .is_some_and(|t: &str| !t.is_empty()),
        "access_token should be present"
    );
    assert!(
        tokens["refresh_token"]
            .as_str()
            .is_some_and(|t: &str| !t.is_empty()),
        "refresh_token should be present"
    );
}

// ── Test 7: Auth code reuse rejected ──────────────────────────────

#[tokio::test]
async fn oauth_auth_code_reuse_rejected() {
    let (base, _server) = start_oauth_server().await;
    let c = no_redirect_client();

    // 1. Register a client.
    let reg: serde_json::Value = client()
        .post(format!("{base}/oauth/register"))
        .json(&serde_json::json!({
            "client_name": "reuse-agent",
            "redirect_uris": ["http://localhost/callback"],
            "scope": "service"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let client_id = reg["client_id"].as_str().unwrap();
    let client_secret = reg["client_secret"].as_str().unwrap();
    let redirect_uri = "http://localhost/callback";

    // 2. Generate PKCE.
    let code_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    let code_challenge = {
        let digest = sha2::Sha256::digest(code_verifier.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    };

    // 3. Obtain an authorization code.
    let auth_resp = c
        .get(format!(
            "{base}/oauth/authorize?response_type=code\
             &client_id={client_id}\
             &redirect_uri={redirect_uri}\
             &code_challenge={code_challenge}\
             &code_challenge_method=S256"
        ))
        .send()
        .await
        .unwrap();
    assert!(auth_resp.status().is_redirection());

    let location = auth_resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap();
    let url = reqwest::Url::parse(location).unwrap();
    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
    let auth_code = params.get("code").unwrap().to_string();

    // 4. First exchange succeeds.
    let resp1 = client()
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", &auth_code),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", redirect_uri),
            ("code_verifier", code_verifier),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // 5. Second exchange with the same code must fail.
    let resp2 = client()
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", &auth_code),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", redirect_uri),
            ("code_verifier", code_verifier),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp2.json().await.unwrap();
    assert_eq!(body["error"], "invalid_grant");
}

// ── Test 8: Wrong PKCE verifier rejected ──────────────────────────

#[tokio::test]
async fn oauth_wrong_pkce_verifier_rejected() {
    let (base, _server) = start_oauth_server().await;
    let c = no_redirect_client();

    // 1. Register.
    let reg: serde_json::Value = client()
        .post(format!("{base}/oauth/register"))
        .json(&serde_json::json!({
            "client_name": "pkce-bad-agent",
            "redirect_uris": ["http://localhost/callback"],
            "scope": "service"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let client_id = reg["client_id"].as_str().unwrap();
    let client_secret = reg["client_secret"].as_str().unwrap();
    let redirect_uri = "http://localhost/callback";

    // 2. PKCE: generate a challenge from the real verifier.
    let real_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    let code_challenge = {
        let digest = sha2::Sha256::digest(real_verifier.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest)
    };

    // 3. Obtain an authorization code.
    let auth_resp = c
        .get(format!(
            "{base}/oauth/authorize?response_type=code\
             &client_id={client_id}\
             &redirect_uri={redirect_uri}\
             &code_challenge={code_challenge}\
             &code_challenge_method=S256"
        ))
        .send()
        .await
        .unwrap();
    assert!(auth_resp.status().is_redirection());

    let location = auth_resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap();
    let url = reqwest::Url::parse(location).unwrap();
    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
    let auth_code = params.get("code").unwrap().to_string();

    // 4. Exchange with a WRONG verifier.
    let resp = client()
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", &auth_code),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("redirect_uri", redirect_uri),
            ("code_verifier", "completely-wrong-verifier-value"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_grant");
}

// ── Test 9: Wrong client_secret rejected ──────────────────────────

#[tokio::test]
async fn oauth_wrong_client_secret_rejected() {
    let (base, _server) = start_oauth_server().await;
    let c = client();

    // Register a client.
    let reg: serde_json::Value = c
        .post(format!("{base}/oauth/register"))
        .json(&serde_json::json!({
            "client_name": "secret-test-agent",
            "redirect_uris": ["http://localhost/callback"],
            "scope": "service"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let client_id = reg["client_id"].as_str().unwrap();

    // Attempt client_credentials with the wrong secret.
    let resp = c
        .post(format!("{base}/oauth/token"))
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", "this-is-the-wrong-secret"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_client");
}
