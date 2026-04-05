//! Async QUIC client implementation for Selene.
//!
//! High-level API for entity CRUD, time-series, GQL queries, and health
//! checks over the SWP wire protocol.

use std::sync::Arc;

use bytes::Bytes;
use quinn::Connection;
use selene_wire::dto::error::ErrorResponse;
use selene_wire::dto::service::HealthResponse;
use selene_wire::dto::ts::{TsPayload, TsRangeRequest, TsSampleDto, TsWriteRequest};
use selene_wire::frame::Frame;
use selene_wire::serialize::{deserialize_payload, serialize_payload};
use selene_wire::{MsgType, WireFlags};

use crate::config::ClientConfig;
use crate::error::ClientError;

/// Async client for a Selene server.
pub struct SeleneClient {
    connection: Connection,
    flags: WireFlags,
    /// Handshake response from successful authentication, if any.
    handshake_info: Option<selene_wire::dto::service::HandshakeResponse>,
}

impl SeleneClient {
    /// Connect to a Selene server. Performs auto-handshake if credentials are configured.
    pub async fn connect(config: &ClientConfig) -> Result<Self, ClientError> {
        let tls_config = if config.insecure {
            #[cfg(not(feature = "insecure"))]
            {
                return Err(ClientError::Other(
                    "insecure mode disabled at compile time (enable 'insecure' feature)".into(),
                ));
            }
            #[cfg(feature = "insecure")]
            {
                let mut cfg = rustls::ClientConfig::builder()
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_no_client_auth();
                cfg.dangerous()
                    .set_certificate_verifier(Arc::new(InsecureVerifier));
                cfg
            }
        } else if let Some(tls) = &config.tls {
            Self::build_prod_tls(tls)?
        } else {
            return Err(ClientError::Other(
                "non-insecure mode requires TLS config".into(),
            ));
        };

        let mut tls_config = tls_config;
        tls_config.alpn_protocols = vec![b"selene/1".to_vec()];

        let client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
                .map_err(|e| ClientError::Other(format!("TLS config error: {e}")))?,
        ));

        let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| ClientError::Other(format!("endpoint error: {e}")))?;
        endpoint.set_default_client_config(client_config);

        let connection = endpoint
            .connect(config.server_addr, &config.server_name)?
            .await?;

        let mut client = Self {
            connection,
            flags: WireFlags::empty(),
            handshake_info: None,
        };

        if let Some(auth) = &config.auth {
            client
                .handshake(&auth.auth_type, &auth.identity, &auth.credentials)
                .await?;
        }

        Ok(client)
    }

    fn build_prod_tls(
        tls: &crate::config::ClientTlsConfig,
    ) -> Result<rustls::ClientConfig, ClientError> {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        use rustls_pki_types::pem::PemObject;

        let ca_pem = std::fs::read(&tls.ca_cert_path)
            .map_err(|e| ClientError::Other(format!("read CA cert: {e}")))?;
        let ca_certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(ca_pem.as_slice())
                .collect::<Result<_, _>>()
                .map_err(|e| ClientError::Other(format!("parse CA cert: {e}")))?;

        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| ClientError::Other(format!("add CA cert: {e}")))?;
        }

        if let (Some(cert_path), Some(key_path)) = (&tls.cert_path, &tls.key_path) {
            let cert_pem = std::fs::read(cert_path)
                .map_err(|e| ClientError::Other(format!("read client cert: {e}")))?;
            let client_certs: Vec<CertificateDer<'static>> =
                CertificateDer::pem_slice_iter(cert_pem.as_slice())
                    .collect::<Result<_, _>>()
                    .map_err(|e| ClientError::Other(format!("parse client cert: {e}")))?;

            let key_pem = std::fs::read(key_path)
                .map_err(|e| ClientError::Other(format!("read client key: {e}")))?;
            let client_key: PrivateKeyDer<'static> =
                PrivateKeyDer::from_pem_slice(key_pem.as_slice())
                    .map_err(|e| ClientError::Other(format!("parse client key: {e}")))?;

            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(client_certs, client_key)
                .map_err(|e| ClientError::Other(format!("client auth cert: {e}")))
        } else {
            Ok(rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth())
        }
    }

    /// Use JSON format for wire messages (useful for debugging).
    pub fn use_json(&mut self) {
        self.flags = WireFlags::JSON_FORMAT;
    }

    /// Return the handshake response, if the client has authenticated.
    pub fn handshake_info(&self) -> Option<&selene_wire::dto::service::HandshakeResponse> {
        self.handshake_info.as_ref()
    }

    /// Perform the authentication handshake.
    ///
    /// Required before any other operations in production mode.
    /// Optional in dev mode (server accepts unauthenticated connections).
    pub async fn handshake(
        &mut self,
        auth_type: &str,
        identity: &str,
        credentials: &str,
    ) -> Result<selene_wire::dto::service::HandshakeResponse, ClientError> {
        use selene_wire::dto::service::{HandshakeRequest, HandshakeResponse};

        let req = HandshakeRequest {
            auth_type: auth_type.into(),
            identity: identity.into(),
            credentials: credentials.into(),
        };

        let payload = serialize_payload(&req, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize handshake: {e}")))?;

        let frame = Frame {
            msg_type: MsgType::Handshake,
            flags: self.flags,
            payload: Bytes::from(payload),
        };

        let (mut send, mut recv) = self.connection.open_bi().await?;

        let encoded = frame.encode();
        send.write_all(&encoded).await?;
        send.finish()
            .map_err(|e| ClientError::Other(format!("finish handshake: {e}")))?;

        let resp_frame = selene_wire::io::read_frame(&mut recv)
            .await
            .map_err(|e| ClientError::Other(format!("handshake read: {e}")))?;

        if resp_frame.msg_type == MsgType::Error {
            let err: selene_wire::dto::error::ErrorResponse =
                deserialize_payload(&resp_frame.payload, resp_frame.flags)
                    .map_err(|e| ClientError::Other(format!("handshake error deserialize: {e}")))?;
            return Err(err.into());
        }

        let resp: HandshakeResponse = deserialize_payload(&resp_frame.payload, resp_frame.flags)
            .map_err(|e| ClientError::Other(format!("handshake response deserialize: {e}")))?;

        self.handshake_info = Some(resp.clone());
        Ok(resp)
    }

    /// Write time-series samples.
    pub async fn ts_write(&self, samples: Vec<TsSampleDto>) -> Result<u64, ClientError> {
        self.rpc(MsgType::TsWrite, &TsWriteRequest { samples })
            .await
    }

    /// Query time-series range.
    pub async fn ts_range(
        &self,
        entity_id: u64,
        property: &str,
        start_nanos: i64,
        end_nanos: i64,
        limit: Option<u64>,
    ) -> Result<Vec<TsSampleDto>, ClientError> {
        let resp: TsPayload = self
            .rpc(
                MsgType::TsRangeQuery,
                &TsRangeRequest {
                    entity_id,
                    property: property.into(),
                    start_nanos,
                    end_nanos,
                    limit,
                },
            )
            .await?;
        Ok(resp.samples)
    }

    /// Execute a GQL query and return the result (Arrow IPC by default).
    pub async fn gql(
        &self,
        query: &str,
    ) -> Result<selene_wire::dto::gql::GqlResultResponse, ClientError> {
        let req = selene_wire::dto::gql::GqlQueryRequest {
            query: query.into(),
            parameters: None,
            explain: false,
            profile: false,
            timeout_ms: None,
            forwarded_scope: None,
        };
        let payload = serialize_payload(&req, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;
        let request_frame = Frame {
            msg_type: MsgType::GqlQuery,
            flags: self.flags,
            payload: Bytes::from(payload),
        };
        self.rpc_raw(request_frame).await
    }

    /// Execute a GQL query, returning both the metadata DTO and raw result data.
    ///
    /// Forces JSON wire format so results are human-readable. Returned `data`
    /// contains JSON-serialized result rows (may be empty for mutations).
    pub async fn gql_with_data(
        &self,
        query: &str,
    ) -> Result<(selene_wire::dto::gql::GqlResultResponse, String), ClientError> {
        let json_flags = WireFlags::JSON_FORMAT;
        let req = selene_wire::dto::gql::GqlQueryRequest {
            query: query.into(),
            parameters: None,
            explain: false,
            profile: false,
            timeout_ms: None,
            forwarded_scope: None,
        };
        let payload = serialize_payload(&req, json_flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;
        let request_frame = Frame {
            msg_type: MsgType::GqlQuery,
            flags: json_flags,
            payload: Bytes::from(payload),
        };

        let resp_frame = self.send_frame(request_frame).await?;

        // Payload contains a JSON GqlResultResponse followed by optional JSON row data.
        let raw = String::from_utf8_lossy(&resp_frame.payload).into_owned();
        let dto_end = find_json_object_end(&raw).unwrap_or(raw.len());
        let dto_str = &raw[..dto_end];
        let data_str = raw[dto_end..].to_string();

        let dto: selene_wire::dto::gql::GqlResultResponse = serde_json::from_str(dto_str)
            .map_err(|e| ClientError::Other(format!("deserialize GQL response: {e}")))?;

        Ok((dto, data_str))
    }

    /// Execute a GQL query with parameters.
    pub async fn gql_with_params(
        &self,
        query: &str,
        params: std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<selene_wire::dto::gql::GqlResultResponse, ClientError> {
        let req = selene_wire::dto::gql::GqlQueryRequest {
            query: query.into(),
            parameters: Some(params),
            explain: false,
            profile: false,
            timeout_ms: None,
            forwarded_scope: None,
        };
        let payload = serialize_payload(&req, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;
        let request_frame = Frame {
            msg_type: MsgType::GqlQuery,
            flags: self.flags,
            payload: Bytes::from(payload),
        };
        self.rpc_raw(request_frame).await
    }

    /// Get the EXPLAIN plan for a GQL query without executing it.
    pub async fn gql_explain(
        &self,
        query: &str,
    ) -> Result<selene_wire::dto::gql::GqlResultResponse, ClientError> {
        self.gql_explain_with_profile(query, false).await
    }

    /// Send a GQL EXPLAIN request, optionally with per-operator profiling.
    pub async fn gql_explain_with_profile(
        &self,
        query: &str,
        profile: bool,
    ) -> Result<selene_wire::dto::gql::GqlResultResponse, ClientError> {
        let req = selene_wire::dto::gql::GqlQueryRequest {
            query: query.into(),
            parameters: None,
            explain: true,
            profile,
            timeout_ms: None,
            forwarded_scope: None,
        };
        let payload = serialize_payload(&req, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;
        let request_frame = Frame {
            msg_type: MsgType::GqlExplain,
            flags: self.flags,
            payload: Bytes::from(payload),
        };
        self.rpc_raw(request_frame).await
    }

    /// Perform a health check.
    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        self.rpc(MsgType::Health, &()).await
    }

    /// Send a telemetry sample as a fire-and-forget QUIC datagram.
    ///
    /// Bypasses stream-per-request overhead for high-frequency telemetry.
    /// Datagrams are unreliable; occasional loss is acceptable for
    /// time-series data where adjacent samples fill gaps.
    pub fn send_telemetry_datagram(
        &self,
        entity_id: u64,
        property: &str,
        timestamp_nanos: i64,
        value: f64,
    ) -> Result<(), ClientError> {
        use selene_wire::datagram::TelemetryDatagram;

        let dg = TelemetryDatagram {
            entity_id,
            property: property.into(),
            timestamp_nanos,
            value,
        };
        let bytes = dg.encode();
        self.connection
            .send_datagram(Bytes::from(bytes))
            .map_err(|e| ClientError::Other(format!("datagram send: {e}")))
    }

    /// Send a federation registration to the peer.
    pub async fn federation_register(
        &self,
        payload: selene_wire::dto::federation::FederationRegisterPayload,
    ) -> Result<(), ClientError> {
        self.rpc_void(MsgType::FederationRegister, &payload).await
    }

    /// Request the peer's list of known federation peers.
    pub async fn federation_peer_list(
        &self,
    ) -> Result<selene_wire::dto::federation::FederationPeerListResponse, ClientError> {
        self.rpc(
            MsgType::FederationPeerList,
            &selene_wire::dto::federation::FederationPeerListRequest {},
        )
        .await
    }

    /// Forward a GQL query to this peer for federated execution.
    pub async fn federation_gql(
        &self,
        req: selene_wire::dto::federation::FederationGqlRequest,
    ) -> Result<selene_wire::dto::federation::FederationGqlResponse, ClientError> {
        let payload = serialize_payload(&req, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;
        let frame = Frame {
            msg_type: MsgType::FederationGqlRequest,
            flags: self.flags,
            payload: Bytes::from(payload),
        };
        self.rpc_raw(frame).await
    }

    /// Push buffered sync entries to an upstream hub.
    ///
    /// Returns the ack response containing the highest acknowledged sequence
    /// and any property-level conflicts detected during merge.
    pub async fn sync_push(
        &self,
        request: selene_wire::dto::sync::SyncPushRequest,
    ) -> Result<selene_wire::dto::sync::SyncPushAckResponse, ClientError> {
        self.rpc(MsgType::SyncPush, &request).await
    }

    /// Send a subscription request to the hub and receive a filtered
    /// snapshot with scope bitmap.
    pub async fn sync_subscribe(
        &self,
        request: selene_wire::dto::sync::SyncSubscribeRequest,
    ) -> Result<selene_wire::dto::sync::SyncSubscribeResponse, ClientError> {
        self.rpc(MsgType::SyncSubscribe, &request).await
    }

    /// Request a full graph slice (all nodes and edges).
    pub async fn graph_slice_full(
        &self,
    ) -> Result<selene_wire::dto::graph_slice::GraphSlicePayload, ClientError> {
        use selene_wire::dto::graph_slice::{GraphSliceRequest, SliceType};
        self.rpc(
            MsgType::GraphSliceRequest,
            &GraphSliceRequest {
                slice_type: SliceType::Full,
                limit: None,
                offset: None,
            },
        )
        .await
    }

    /// Request a graph slice by labels.
    pub async fn graph_slice_by_labels(
        &self,
        labels: Vec<String>,
    ) -> Result<selene_wire::dto::graph_slice::GraphSlicePayload, ClientError> {
        use selene_wire::dto::graph_slice::{GraphSliceRequest, SliceType};
        self.rpc(
            MsgType::GraphSliceRequest,
            &GraphSliceRequest {
                slice_type: SliceType::ByLabels { labels },
                limit: None,
                offset: None,
            },
        )
        .await
    }

    /// Request a containment subtree slice.
    pub async fn graph_slice_containment(
        &self,
        root_id: u64,
        max_depth: Option<u32>,
    ) -> Result<selene_wire::dto::graph_slice::GraphSlicePayload, ClientError> {
        use selene_wire::dto::graph_slice::{GraphSliceRequest, SliceType};
        self.rpc(
            MsgType::GraphSliceRequest,
            &GraphSliceRequest {
                slice_type: SliceType::Containment { root_id, max_depth },
                limit: None,
                offset: None,
            },
        )
        .await
    }

    /// Subscribe to changelog events starting after `since_sequence`.
    ///
    /// Returns a `ChangelogSubscription` that yields events as they occur.
    /// Use `since_sequence: 0` to start from the current position.
    ///
    /// `peer_name` should be `Some(name)` for sync edge nodes so the hub can
    /// look up the correct per-peer subscription filter. Pass `None` for CDC
    /// replica subscribers and other non-sync clients.
    pub async fn subscribe_changelog(
        &self,
        since_sequence: u64,
        peer_name: Option<String>,
    ) -> Result<ChangelogSubscription, ClientError> {
        use selene_wire::dto::changelog::{
            ChangelogAckRequest, ChangelogEventDto, ChangelogSubscribeRequest,
        };
        use tokio::sync::mpsc;

        let req = ChangelogSubscribeRequest {
            since_sequence,
            peer_name,
        };
        let payload = serialize_payload(&req, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;

        let frame = Frame {
            msg_type: MsgType::ChangelogSubscribe,
            flags: self.flags,
            payload: Bytes::from(payload),
        };

        let (mut send, mut recv) = self.connection.open_bi().await?;

        let encoded = frame.encode();
        send.write_all(&encoded).await?;

        let (event_tx, event_rx) = mpsc::channel(64);
        let (ack_tx, mut ack_rx) = mpsc::channel::<u64>(64);
        let flags = self.flags;

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = read_frame_client(&mut recv) => {
                        match result {
                            Ok(frame) if frame.msg_type == MsgType::ChangelogEvent => {
                                match deserialize_payload::<ChangelogEventDto>(&frame.payload, frame.flags) {
                                    Ok(event) => {
                                        if event_tx.send(event).await.is_err() {
                                            break; // receiver dropped
                                        }
                                    }
                                    Err(e) => {
                                        tracing::debug!("event deserialize error: {e}");
                                        break;
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }

                    Some(seq) = ack_rx.recv() => {
                        let ack = ChangelogAckRequest { acked_sequence: seq };
                        if let Ok(payload) = serialize_payload(&ack, flags) {
                            let ack_frame = Frame {
                                msg_type: MsgType::ChangelogAck,
                                flags,
                                payload: Bytes::from(payload),
                            };
                            let encoded = ack_frame.encode();
                            if send.write_all(&encoded).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(ChangelogSubscription {
            events: event_rx,
            ack_tx,
            _task: task,
        })
    }

    /// Open a QUIC stream, send a frame, read the response, and check for
    /// server-side errors. Returns the raw response frame on success.
    async fn send_frame(&self, request_frame: Frame) -> Result<Frame, ClientError> {
        let (mut send, mut recv) = self.connection.open_bi().await?;
        let encoded = request_frame.encode();
        send.write_all(&encoded).await?;
        send.finish()
            .map_err(|e| ClientError::Other(format!("finish stream: {e}")))?;

        let resp_frame = selene_wire::io::read_frame(&mut recv)
            .await
            .map_err(|e| ClientError::Other(format!("read response: {e}")))?;

        if resp_frame.msg_type == MsgType::Error {
            let err: ErrorResponse = deserialize_payload(&resp_frame.payload, resp_frame.flags)
                .map_err(|e| ClientError::Other(format!("deserialize error response: {e}")))?;
            return Err(err.into());
        }

        Ok(resp_frame)
    }

    /// Send a request and expect an empty Ok response.
    async fn rpc_void<Req: serde::Serialize>(
        &self,
        msg_type: MsgType,
        request: &Req,
    ) -> Result<(), ClientError> {
        let payload = serialize_payload(request, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;

        let request_frame = Frame {
            msg_type,
            flags: self.flags,
            payload: Bytes::from(payload),
        };

        self.send_frame(request_frame).await?;
        Ok(())
    }

    async fn rpc<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        msg_type: MsgType,
        request: &Req,
    ) -> Result<Resp, ClientError> {
        let payload = serialize_payload(request, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;

        let request_frame = Frame {
            msg_type,
            flags: self.flags,
            payload: Bytes::from(payload),
        };

        let resp_frame = self.send_frame(request_frame).await?;
        deserialize_payload(&resp_frame.payload, resp_frame.flags)
            .map_err(|e| ClientError::Other(format!("deserialize response: {e}")))
    }

    /// Send a pre-built frame and deserialize the response.
    async fn rpc_raw<Resp: serde::de::DeserializeOwned>(
        &self,
        request_frame: Frame,
    ) -> Result<Resp, ClientError> {
        let resp_frame = self.send_frame(request_frame).await?;
        deserialize_payload(&resp_frame.payload, resp_frame.flags)
            .map_err(|e| ClientError::Other(format!("deserialize response: {e}")))
    }

    /// Request a binary snapshot from the server and collect all chunks.
    ///
    /// Returns `(snapshot_bytes, snapshot_sequence)` where `snapshot_sequence`
    /// is the changelog sequence at the time the snapshot was taken. The replica
    /// should replay changelog entries with sequence > snapshot_sequence.
    pub async fn request_snapshot(&self, graph_name: &str) -> Result<(Vec<u8>, u64), ClientError> {
        use selene_wire::dto::snapshot::{SnapshotChunk, SnapshotRequest};

        let req = SnapshotRequest {
            graph_name: graph_name.into(),
        };
        let payload = serialize_payload(&req, self.flags)
            .map_err(|e| ClientError::Other(format!("serialize: {e}")))?;

        let frame = Frame {
            msg_type: MsgType::SnapshotRequest,
            flags: self.flags,
            payload: Bytes::from(payload),
        };

        let (mut send, mut recv) = self.connection.open_bi().await?;

        let encoded = frame.encode();
        send.write_all(&encoded).await?;
        send.finish()
            .map_err(|e| ClientError::Other(format!("finish stream: {e}")))?;

        let mut snapshot_bytes = Vec::new();
        let snapshot_sequence;

        /// Maximum snapshot size: 1 GB. Prevents unbounded memory growth if
        /// the server sends an unexpectedly large or malicious stream.
        const MAX_SNAPSHOT_SIZE: usize = 1_073_741_824;

        loop {
            let resp = read_frame_client(&mut recv).await?;

            if resp.msg_type == MsgType::Error {
                let err: ErrorResponse = deserialize_payload(&resp.payload, resp.flags)
                    .map_err(|e| ClientError::Other(format!("error deserialize: {e}")))?;
                return Err(err.into());
            }

            if resp.msg_type != MsgType::SnapshotChunk {
                return Err(ClientError::UnexpectedResponse(resp.msg_type));
            }

            let chunk: SnapshotChunk = deserialize_payload(&resp.payload, resp.flags)
                .map_err(|e| ClientError::Other(format!("chunk deserialize: {e}")))?;

            snapshot_bytes.extend_from_slice(&chunk.data);

            if snapshot_bytes.len() > MAX_SNAPSHOT_SIZE {
                return Err(ClientError::Other(format!(
                    "snapshot exceeds maximum size of {MAX_SNAPSHOT_SIZE} bytes",
                )));
            }

            if chunk.is_last {
                snapshot_sequence = chunk.snapshot_sequence.unwrap_or(0);
                break;
            }
        }

        tracing::info!(
            bytes = snapshot_bytes.len(),
            snapshot_sequence,
            "snapshot received from primary"
        );

        Ok((snapshot_bytes, snapshot_sequence))
    }
}

/// A changelog subscription handle.
///
/// Receives events from the server and sends acks back.
pub struct ChangelogSubscription {
    events: tokio::sync::mpsc::Receiver<selene_wire::dto::changelog::ChangelogEventDto>,
    ack_tx: tokio::sync::mpsc::Sender<u64>,
    _task: tokio::task::JoinHandle<()>,
}

impl ChangelogSubscription {
    /// Receive the next changelog event. Returns `None` if the stream closes.
    pub async fn next_event(&mut self) -> Option<selene_wire::dto::changelog::ChangelogEventDto> {
        self.events.recv().await
    }

    /// Acknowledge processing of events up to the given sequence.
    pub async fn ack(&self, sequence: u64) -> Result<(), ClientError> {
        self.ack_tx
            .send(sequence)
            .await
            .map_err(|_| ClientError::Other("subscription closed".into()))
    }
}

/// Read a single SWP frame from a QUIC recv stream.
/// Delegates to `selene_wire::io::read_frame` which enforces MAX_PAYLOAD.
async fn read_frame_client(recv: &mut quinn::RecvStream) -> Result<Frame, ClientError> {
    selene_wire::io::read_frame(recv)
        .await
        .map_err(|e| ClientError::Other(format!("read frame: {e}")))
}

/// TLS certificate verifier that accepts any certificate. Dev mode only,
/// gated behind the `insecure` feature flag.
#[cfg(feature = "insecure")]
#[derive(Debug)]
struct InsecureVerifier;

#[cfg(feature = "insecure")]
impl rustls::client::danger::ServerCertVerifier for InsecureVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Find the end position (exclusive) of the first JSON object in a string.
/// Handles nested braces and string literals with escaped quotes.
///
/// Does not interpret `\uXXXX` unicode escapes. Safe in practice because
/// serde_json emits `"`, `{`, `}` unescaped, and Selene's wire format
/// never produces `\uXXXX` for ASCII control characters.
fn find_json_object_end(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;

    for (i, ch) in s.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if ch == '\\' && in_string {
            escape = true;
            continue;
        }
        if ch == '"' {
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- find_json_object_end tests ---

    #[test]
    fn find_json_object_end_simple_object() {
        let s = r#"{"key":"value"}"#;
        assert_eq!(find_json_object_end(s), Some(s.len()));
    }

    #[test]
    fn find_json_object_end_nested_objects() {
        let s = r#"{"a":{"b":{"c":1}}}"#;
        assert_eq!(find_json_object_end(s), Some(s.len()));
    }

    #[test]
    fn find_json_object_end_with_trailing_data() {
        // Simulates the gql_with_data response: a JSON DTO followed by row data.
        let s = r#"{"rows":3}[1,2,3]"#;
        assert_eq!(find_json_object_end(s), Some(10));
        assert_eq!(&s[..10], r#"{"rows":3}"#);
        assert_eq!(&s[10..], "[1,2,3]");
    }

    #[test]
    fn find_json_object_end_string_with_braces() {
        // Braces inside string literals must be ignored.
        let s = r#"{"query":"MATCH (n) WHERE n.name = \"{test}\""}"#;
        assert_eq!(find_json_object_end(s), Some(s.len()));
    }

    #[test]
    fn find_json_object_end_escaped_quotes() {
        // The string value contains an escaped quote: the parser must
        // not exit the string prematurely.
        let s = r#"{"msg":"say \"hello\""}"#;
        assert_eq!(find_json_object_end(s), Some(s.len()));
    }

    #[test]
    fn find_json_object_end_empty_object() {
        assert_eq!(find_json_object_end("{}"), Some(2));
    }

    #[test]
    fn find_json_object_end_empty_string() {
        assert_eq!(find_json_object_end(""), None);
    }

    #[test]
    fn find_json_object_end_no_object() {
        assert_eq!(find_json_object_end("just plain text"), None);
        assert_eq!(find_json_object_end("[1,2,3]"), None);
    }

    #[test]
    fn find_json_object_end_unclosed_object() {
        assert_eq!(find_json_object_end(r#"{"key":"value""#), None);
        assert_eq!(find_json_object_end("{"), None);
    }

    #[test]
    fn find_json_object_end_deeply_nested() {
        // 50 levels of nesting.
        let mut open = String::new();
        for i in 0..50 {
            use std::fmt::Write;
            write!(open, r#"{{"k{i}":"#).unwrap();
        }
        let close: String = std::iter::repeat_n("1}", 50).collect();
        let s = format!("{open}{close}");
        let result = find_json_object_end(&s);
        assert_eq!(result, Some(s.len()));
    }
}
