//! End-to-end integration tests — server + client over real QUIC.
//! All CRUD operations use GQL (the primary query interface).
#![cfg(feature = "dev-tls")]

use std::sync::Arc;

use selene_client::{ClientConfig, SeleneClient};
use selene_server::auth::handshake::AuthContext;
use selene_server::bootstrap;
use selene_server::config::{SeleneConfig, TlsConfig};
use selene_server::quic::handler;
use selene_server::quic::subscription;
use selene_server::tls;
use selene_wire::dto::ts::TsSampleDto;

use bytes::Bytes;
use selene_wire::frame::HEADER_SIZE;
use selene_wire::{Frame, MsgType, WireFlags};

async fn start_test_server() -> std::net::SocketAddr {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let mut config = SeleneConfig::dev(dir.path());
    config.listen_addr = "127.0.0.1:0".parse().unwrap();

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
    addr
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

    if request.msg_type == MsgType::ChangelogSubscribe {
        let auth = Arc::new(AuthContext::dev_admin());
        return subscription::handle_subscription(state, auth, send, recv, request).await;
    }

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

// ── Health ─────────────────────────────────────────────────────────

#[tokio::test]
async fn health_check() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;
    let health = client.health().await.unwrap();
    assert_eq!(health.status, "ok");
    assert_eq!(health.node_count, 0);
    assert_eq!(health.edge_count, 0);
}

// ── Node CRUD via GQL ──────────────────────────────────────────────

#[tokio::test]
async fn gql_insert_and_match_node() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;

    let insert = client.gql("INSERT (:sensor {unit: '°F'})").await.unwrap();
    assert_eq!(insert.status_code, "00000");
    assert!(
        insert
            .mutations
            .as_ref()
            .is_some_and(|m| m.nodes_created == 1)
    );

    let result = client
        .gql("MATCH (s:sensor) RETURN s.unit AS unit")
        .await
        .unwrap();
    assert_eq!(result.row_count, 1);
}

#[tokio::test]
async fn gql_insert_set_delete_node() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;

    client.gql("INSERT (:ahu)").await.unwrap();

    let set_result = client
        .gql("MATCH (n:ahu) SET n.name = 'AHU-1'")
        .await
        .unwrap();
    assert!(
        set_result
            .mutations
            .as_ref()
            .is_some_and(|m| m.properties_set >= 1)
    );

    let del = client.gql("MATCH (n:ahu) DETACH DELETE n").await.unwrap();
    assert!(del.mutations.as_ref().is_some_and(|m| m.nodes_deleted >= 1));

    let check = client
        .gql("MATCH (n:ahu) RETURN count(*) AS cnt")
        .await
        .unwrap();
    assert_eq!(check.row_count, 1); // 1 row with cnt=0
}

#[tokio::test]
async fn gql_list_nodes_by_label() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;

    for _ in 0..5 {
        client.gql("INSERT (:sensor)").await.unwrap();
    }
    client.gql("INSERT (:building)").await.unwrap();

    let all = client
        .gql("MATCH (n) RETURN count(*) AS cnt")
        .await
        .unwrap();
    assert_eq!(all.row_count, 1);

    let sensors = client
        .gql("MATCH (n:sensor) RETURN count(*) AS cnt")
        .await
        .unwrap();
    assert_eq!(sensors.row_count, 1);
}

// ── Edge CRUD via GQL ──────────────────────────────────────────────

#[tokio::test]
async fn gql_insert_and_match_edge() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;

    client.gql("INSERT (:site)").await.unwrap();
    client.gql("INSERT (:building)").await.unwrap();
    client
        .gql("MATCH (s:site), (b:building) INSERT (s)-[:contains]->(b)")
        .await
        .unwrap();

    let result = client
        .gql("MATCH (s:site)-[e:contains]->(b:building) RETURN type(e) AS label")
        .await
        .unwrap();
    assert_eq!(result.row_count, 1);
}

#[tokio::test]
async fn gql_delete_edge() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;

    client.gql("INSERT (:a)").await.unwrap();
    client.gql("INSERT (:b)").await.unwrap();
    client
        .gql("MATCH (a:a), (b:b) INSERT (a)-[:links]->(b)")
        .await
        .unwrap();

    let del = client.gql("MATCH ()-[e:links]->() DELETE e").await.unwrap();
    assert!(del.mutations.as_ref().is_some_and(|m| m.edges_deleted >= 1));
}

// ── Time-Series ────────────────────────────────────────────────────

#[tokio::test]
async fn ts_write_and_range_query() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;

    client.gql("INSERT (:sensor)").await.unwrap();

    let samples = vec![
        TsSampleDto {
            entity_id: 1,
            property: "temp".into(),
            timestamp_nanos: 1000,
            value: 72.5,
        },
        TsSampleDto {
            entity_id: 1,
            property: "temp".into(),
            timestamp_nanos: 2000,
            value: 73.0,
        },
        TsSampleDto {
            entity_id: 1,
            property: "temp".into(),
            timestamp_nanos: 3000,
            value: 73.5,
        },
    ];

    let count = client.ts_write(samples).await.unwrap();
    assert_eq!(count, 3);

    let results = client.ts_range(1, "temp", 1500, 3500, None).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, 73.0);
    assert_eq!(results[1].value, 73.5);
}

// ── GQL Query ──────────────────────────────────────────────────────

#[tokio::test]
async fn gql_count_query() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;

    client.gql("INSERT (:sensor)").await.unwrap();
    client.gql("INSERT (:building)").await.unwrap();

    let result = client
        .gql("MATCH (n) RETURN count(*) AS cnt")
        .await
        .unwrap();
    assert_eq!(result.row_count, 1);
    assert_eq!(result.status_code, "00000");
}

// ── Snapshot / Recovery ────────────────────────────────────────────

#[tokio::test]
async fn snapshot_and_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let mut config = SeleneConfig::dev(&data_dir);
        config.listen_addr = "127.0.0.1:0".parse().unwrap();
        selene_server::ops::init_start_time();
        let state = bootstrap::bootstrap(config, None).await.unwrap();
        let state = Arc::new(state);

        let ((), changes) = state
            .graph()
            .write(|m| {
                let n1 = m.create_node(
                    selene_core::LabelSet::from_strs(&["site"]),
                    selene_core::PropertyMap::new(),
                )?;
                let n2 = m.create_node(
                    selene_core::LabelSet::from_strs(&["building"]),
                    selene_core::PropertyMap::new(),
                )?;
                m.create_edge(
                    n1,
                    selene_core::IStr::new("contains"),
                    n2,
                    selene_core::PropertyMap::new(),
                )?;
                Ok(())
            })
            .unwrap();

        state
            .wal()
            .lock()
            .append(
                &changes,
                selene_core::entity::now_nanos() as u64,
                selene_core::Origin::Local,
            )
            .unwrap();
        selene_server::tasks::take_snapshot(&state).unwrap();
    }

    {
        let mut config = SeleneConfig::dev(&data_dir);
        config.listen_addr = "127.0.0.1:0".parse().unwrap();
        let state = bootstrap::bootstrap(config, None).await.unwrap();
        let (node_count, edge_count) = state.graph().read(|g| (g.node_count(), g.edge_count()));
        assert_eq!(node_count, 2);
        assert_eq!(edge_count, 1);
    }
}

// ── mTLS ───────────────────────────────────────────────────────────

#[tokio::test]
async fn mtls_connection() {
    use selene_client::config::ClientTlsConfig;
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let certs = selene_testing::tls::generate_test_certs();

    let cert_path = data_dir.join("server.crt");
    let key_path = data_dir.join("server.key");
    let ca_path = data_dir.join("ca.crt");
    let client_cert_path = data_dir.join("client.crt");
    let client_key_path = data_dir.join("client.key");

    std::fs::write(&cert_path, &certs.server_cert_chain_pem).unwrap();
    std::fs::write(&key_path, &certs.server_key_pem).unwrap();
    std::fs::write(&ca_path, &certs.ca_cert_pem).unwrap();
    std::fs::write(&client_cert_path, &certs.client_cert_chain_pem).unwrap();
    std::fs::write(&client_key_path, &certs.client_key_pem).unwrap();

    let tls_config = TlsConfig {
        cert_path: cert_path.clone(),
        key_path: key_path.clone(),
        ca_cert_path: Some(ca_path.clone()),
    };

    let mut config = SeleneConfig::dev(&data_dir);
    config.listen_addr = "127.0.0.1:0".parse().unwrap();
    config.tls = Some(tls_config.clone());

    selene_server::ops::init_start_time();
    let state = bootstrap::bootstrap(config, None).await.unwrap();
    let state = Arc::new(state);

    let (server_config, _) = tls::prod_server_config(&tls_config).unwrap();
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

    let client_config = selene_client::ClientConfig {
        server_addr: addr,
        server_name: "localhost".into(),
        insecure: false,
        tls: Some(ClientTlsConfig {
            ca_cert_path: ca_path,
            cert_path: Some(client_cert_path),
            key_path: Some(client_key_path),
        }),
        auth: None,
    };

    let client = SeleneClient::connect(&client_config).await.unwrap();
    let health = client.health().await.unwrap();
    assert_eq!(health.status, "ok");

    let result = client.gql("INSERT (:test)").await.unwrap();
    assert!(
        result
            .mutations
            .as_ref()
            .is_some_and(|m| m.nodes_created == 1)
    );
}

// ── Changelog Subscription ─────────────────────────────────────────

#[tokio::test]
#[ignore = "changelog dispatch requires WAL coalescer background task (not wired in test server)"]
async fn changelog_subscribe_and_receive_events() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;
    let mut sub = client.subscribe_changelog(0, None).await.unwrap();
    client.gql("INSERT (:sensor)").await.unwrap();
    client.gql("INSERT (:building)").await.unwrap();

    let event1 = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next_event())
        .await
        .unwrap()
        .unwrap();
    assert!(!event1.sync_lost);
    assert!(event1.sequence > 0);

    let event2 = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next_event())
        .await
        .unwrap()
        .unwrap();
    assert!(event2.sequence > event1.sequence);
    sub.ack(event2.sequence).await.unwrap();
}

#[tokio::test]
#[ignore = "changelog dispatch requires WAL coalescer background task (not wired in test server)"]
async fn changelog_subscribe_with_catchup() {
    let addr = start_test_server().await;
    let client = connect_to(addr).await;
    client.gql("INSERT (:a)").await.unwrap();
    client.gql("INSERT (:b)").await.unwrap();
    client.gql("INSERT (:c)").await.unwrap();

    let mut sub = client.subscribe_changelog(0, None).await.unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next_event())
        .await
        .unwrap()
        .unwrap();
    assert!(!event.sync_lost);
    assert!(!event.changes.is_empty());
}
