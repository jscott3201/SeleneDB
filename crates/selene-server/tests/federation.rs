//! Federation E2E tests — two Selene nodes communicating over QUIC.
//!
//! These tests require the `dev-tls` feature for self-signed certificates.
#![cfg(feature = "dev-tls")]

use std::sync::Arc;

use selene_client::{ClientConfig, SeleneClient};
use selene_server::auth::handshake::AuthContext;
use selene_server::bootstrap;
use selene_server::config::SeleneConfig;
use selene_server::federation::config::{FederationConfig, PeerRole};
use selene_server::quic::handler;
use selene_server::tls;
use selene_wire::dto::federation::FederationRegisterPayload;

use bytes::Bytes;
use selene_wire::frame::HEADER_SIZE;
use selene_wire::{Frame, MsgType, WireFlags};

fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Start a test server with federation enabled.
async fn start_federation_server(
    node_name: &str,
) -> (std::net::SocketAddr, Arc<selene_server::ServerState>) {
    ensure_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let mut config = SeleneConfig::dev(dir.path());
    config.listen_addr = "127.0.0.1:0".parse().unwrap();
    config.federation = FederationConfig {
        enabled: true,
        node_name: node_name.into(),
        role: PeerRole::Building,
        bootstrap_peers: vec![],
        peer_ttl_secs: 300,
        refresh_interval_secs: 60,
    };

    selene_server::ops::init_start_time();
    let state = bootstrap::bootstrap(config, None).await.unwrap();
    let state = Arc::new(state);

    let (server_config, _certs) = tls::dev_server_config().unwrap();
    let endpoint = quinn::Endpoint::server(server_config, state.config().listen_addr).unwrap();
    let addr = endpoint.local_addr().unwrap();

    let state2 = Arc::clone(&state);
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let state = Arc::clone(&state2);
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    while let Ok((send, recv)) = conn.accept_bi().await {
                        let state = Arc::clone(&state);
                        tokio::spawn(async move {
                            let _ = handle_stream(state, send, recv).await;
                        });
                    }
                }
            });
        }
    });

    std::mem::forget(dir);
    (addr, state)
}

async fn handle_stream(
    state: Arc<selene_server::ServerState>,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) -> anyhow::Result<()> {
    let mut header = [0u8; HEADER_SIZE];
    recv.read_exact(&mut header).await?;

    let msg_type = MsgType::try_from(header[0])?;
    let flags = WireFlags::from_bits_truncate(header[1]);
    let len = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;

    let mut payload = vec![0u8; len];
    if len > 0 {
        recv.read_exact(&mut payload).await?;
    }

    let request = Frame {
        msg_type,
        flags,
        payload: Bytes::from(payload),
    };

    let auth = Arc::new(AuthContext::dev_admin());
    let response = handler::handle_request(&state, &auth, request).await;
    let encoded = response.encode();
    send.write_all(&encoded).await?;
    send.finish().map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(())
}

async fn connect_to(addr: std::net::SocketAddr) -> SeleneClient {
    let config = ClientConfig {
        server_addr: addr,
        server_name: "localhost".into(),
        insecure: true,
        tls: None,
        auth: None,
    };
    SeleneClient::connect(&config).await.unwrap()
}

#[tokio::test]
async fn federation_register_and_peer_list() {
    let (addr_a, state_a) = start_federation_server("node_a").await;
    let client = connect_to(addr_a).await;

    // Register a peer
    let reg = FederationRegisterPayload {
        node_name: "node_b".into(),
        address: "10.2.1.1:4510".into(),
        schema_labels: vec!["sensor".into(), "ahu".into()],
        role: "building".into(),
        bloom_filter: None,
    };
    client.federation_register(reg).await.unwrap();

    // Verify peer is in the registry
    let fed_svc = state_a
        .services()
        .get::<selene_server::federation::FederationService>()
        .unwrap();
    let registry = &fed_svc.registry;
    assert_eq!(registry.peer_count(), 1);
    let peer = registry.get("node_b").unwrap();
    assert_eq!(peer.address, "10.2.1.1:4510");
    assert_eq!(peer.schema_labels, vec!["sensor", "ahu"]);

    // Request peer list
    let peer_list = client.federation_peer_list().await.unwrap();
    // Should include both node_a (self) and node_b
    assert_eq!(peer_list.peers.len(), 2);
    let names: Vec<&str> = peer_list
        .peers
        .iter()
        .map(|p| p.node_name.as_str())
        .collect();
    assert!(names.contains(&"node_a"));
    assert!(names.contains(&"node_b"));
}

#[tokio::test]
async fn two_node_gql_federation() {
    // Start two servers
    let (addr_a, _state_a) = start_federation_server("node_a").await;
    let (addr_b, _state_b) = start_federation_server("node_b").await;

    let client_a = connect_to(addr_a).await;
    let client_b = connect_to(addr_b).await;

    // Create data on each node via GQL
    let result = client_a
        .gql("INSERT (:building {name: 'HQ'})")
        .await
        .unwrap();
    assert_eq!(result.status_code, "00000");

    let result = client_b
        .gql("INSERT (:sensor {name: 'Temp-1'})")
        .await
        .unwrap();
    assert_eq!(result.status_code, "00000");

    // Each node only sees its own data
    let result_a = client_a
        .gql("MATCH (n) RETURN count(*) AS cnt")
        .await
        .unwrap();
    assert_eq!(result_a.status_code, "00000");
    assert_eq!(result_a.row_count, 1);

    let result_b = client_b
        .gql("MATCH (n) RETURN count(*) AS cnt")
        .await
        .unwrap();
    assert_eq!(result_b.status_code, "00000");
    assert_eq!(result_b.row_count, 1);
}

#[tokio::test]
async fn federation_gql_forwarding() {
    // Start a server and forward a GQL query to it via the federation protocol
    let (addr, _state) = start_federation_server("test_node").await;
    let client = connect_to(addr).await;

    // Create data
    client.gql("INSERT (:sensor {name: 'S1'})").await.unwrap();
    client.gql("INSERT (:sensor {name: 'S2'})").await.unwrap();

    // Forward a GQL query via the federation protocol
    let req = selene_wire::dto::federation::FederationGqlRequest {
        query: "MATCH (n:sensor) RETURN n.name AS name".into(),
        json_format: true,
        forwarded_scope: None,
    };
    let resp = client.federation_gql(req).await.unwrap();

    assert_eq!(resp.status_code, "00000");
    assert_eq!(resp.row_count, 2);
    assert!(resp.json_result.is_some());
    let json = resp.json_result.unwrap();
    assert!(json.contains("S1"));
    assert!(json.contains("S2"));
}
