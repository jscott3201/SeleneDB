//! QUIC listener — accepts connections and spawns per-stream handlers.
//!
//! In dev mode, connections get an implicit admin AuthContext.
//! In production mode, the first stream must be a Handshake.

use std::sync::Arc;

use bytes::Bytes;
use quinn::{Endpoint, RecvStream, SendStream, ServerConfig};
use selene_wire::dto::error::{ErrorResponse, codes};
use selene_wire::dto::service::{HandshakeRequest, HandshakeResponse};
use selene_wire::frame::Frame;
use selene_wire::io::{read_frame, write_frame};
use selene_wire::msg_type::MsgType;
use selene_wire::serialize::{deserialize_payload, serialize_payload};
use tracing::Instrument;

use super::handler;
use crate::auth::handshake::{self, AuthContext};
use crate::bootstrap::ServerState;

/// Start the QUIC listener and serve requests.
#[tracing::instrument(skip_all, fields(addr = %state.config.listen_addr, max_connections = state.config.quic_max_connections))]
pub async fn serve(state: Arc<ServerState>, server_config: ServerConfig) -> anyhow::Result<()> {
    let endpoint = Endpoint::server(server_config, state.config.listen_addr)?;
    let max_connections = state.config.quic_max_connections;
    let active_connections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    tracing::info!(addr = %state.config.listen_addr, max_connections, "QUIC listener started");

    while let Some(incoming) = endpoint.accept().await {
        let current = active_connections.load(std::sync::atomic::Ordering::Relaxed);
        if current >= max_connections {
            tracing::warn!(
                current,
                max = max_connections,
                remote = %incoming.remote_address(),
                "connection rejected: at capacity"
            );
            incoming.refuse();
            continue;
        }

        let state = Arc::clone(&state);
        let counter = Arc::clone(&active_connections);
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let remote = incoming.remote_address();
        let conn_span = tracing::info_span!("quic_conn", %remote);
        tokio::spawn(
            async move {
                match incoming.await {
                    Ok(conn) => {
                        tracing::debug!("connection accepted");
                        handle_connection(state, conn).await;
                    }
                    Err(e) => {
                        tracing::warn!("connection failed: {e}");
                    }
                }
                counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
            .instrument(conn_span),
        );
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(remote = %conn.remote_address()))]
async fn handle_connection(state: Arc<ServerState>, conn: quinn::Connection) {
    // In dev mode, skip handshake and use admin context
    let auth = if state.config.dev_mode {
        Arc::new(AuthContext::dev_admin())
    } else {
        // First stream must be handshake
        match perform_handshake(&state, &conn).await {
            Ok(ctx) => Arc::new(ctx),
            Err(e) => {
                tracing::warn!(remote = %conn.remote_address(), "handshake failed: {e}");
                conn.close(1u32.into(), b"handshake failed");
                return;
            }
        }
    };

    loop {
        tokio::select! {
            // Accept bidi streams for request/response
            result = conn.accept_bi() => {
                match result {
                    Ok((send, recv)) => {
                        let state = Arc::clone(&state);
                        let auth = Arc::clone(&auth);
                        let stream_span = tracing::info_span!("quic_stream", principal = auth.principal_node_id.0);
                        tokio::spawn(async move {
                            if let Err(e) = handle_stream(state, auth, send, recv).await {
                                tracing::debug!("stream error: {e}");
                            }
                        }.instrument(stream_span));
                    }
                    Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                        tracing::debug!("connection closed by peer");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("connection error: {e}");
                        break;
                    }
                }
            }

            // Read fire-and-forget telemetry datagrams
            result = conn.read_datagram() => {
                match result {
                    Ok(data) => {
                        handle_datagram(&state, &auth, &data);
                    }
                    Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                        break;
                    }
                    Err(e) => {
                        tracing::debug!("datagram error: {e}");
                        break;
                    }
                }
            }
        }
    }
}

/// Perform the handshake on the first bidi stream of a connection.
async fn perform_handshake(
    state: &ServerState,
    conn: &quinn::Connection,
) -> anyhow::Result<AuthContext> {
    let (mut send, mut recv) =
        tokio::time::timeout(std::time::Duration::from_secs(5), conn.accept_bi())
            .await
            .map_err(|_| anyhow::anyhow!("handshake timeout"))??;

    let frame = read_frame(&mut recv).await?;

    if frame.msg_type != MsgType::Handshake {
        let err = ErrorResponse {
            code: codes::AUTHENTICATION_FAILED,
            message: "first message must be Handshake".into(),
            suggestion: None,
        };
        let payload = serialize_payload(&err, frame.flags).unwrap_or_default();
        let resp = Frame {
            msg_type: MsgType::Error,
            flags: frame.flags,
            payload: Bytes::from(payload),
        };
        write_frame(&mut send, &resp).await?;
        send.finish()?;
        anyhow::bail!("expected Handshake, got {:?}", frame.msg_type);
    }

    let req: HandshakeRequest = deserialize_payload(&frame.payload, frame.flags)
        .map_err(|e| anyhow::anyhow!("handshake deserialize: {e}"))?;

    let auth_ctx = handshake::authenticate(
        &state.graph,
        &req.auth_type,
        &req.identity,
        &req.credentials,
        state.config.dev_mode,
    )
    .map_err(|e| {
        tracing::warn!("authentication failed: {e}");
        anyhow::anyhow!("authentication failed: {e}")
    })?;

    // Send handshake response
    let scope_root_ids: Vec<u64> = if auth_ctx.is_admin() {
        vec![] // admin has global scope
    } else {
        state.graph.read(|g| {
            crate::auth::projection::scope_roots(g, auth_ctx.principal_node_id)
                .iter()
                .map(|id| id.0)
                .collect()
        })
    };

    let resp = HandshakeResponse {
        principal_id: auth_ctx.principal_node_id.0,
        role: auth_ctx.role.to_string(),
        scope_root_ids,
    };

    let payload = serialize_payload(&resp, frame.flags)
        .map_err(|e| anyhow::anyhow!("handshake response serialize: {e}"))?;
    let resp_frame = Frame {
        msg_type: MsgType::Ok,
        flags: frame.flags,
        payload: Bytes::from(payload),
    };
    write_frame(&mut send, &resp_frame).await?;
    send.finish()?;

    tracing::info!(
        principal = auth_ctx.principal_node_id.0,
        role = %auth_ctx.role,
        "connection authenticated"
    );

    Ok(auth_ctx)
}

#[tracing::instrument(skip_all, fields(principal = auth.principal_node_id.0, role = %auth.role))]
async fn handle_stream(
    state: Arc<ServerState>,
    auth: Arc<AuthContext>,
    mut send: SendStream,
    mut recv: RecvStream,
) -> anyhow::Result<()> {
    // Read request frame
    let request = read_frame(&mut recv).await?;

    // Route changelog subscriptions to the dedicated handler
    if request.msg_type == MsgType::ChangelogSubscribe {
        return super::subscription::handle_subscription(state, auth, send, recv, request).await;
    }

    // Route snapshot requests to the streaming handler (admin only)
    if request.msg_type == MsgType::SnapshotRequest {
        if !auth.is_admin() {
            let err = ErrorResponse {
                code: codes::AUTHORIZATION_DENIED,
                message: "snapshot streaming requires admin role".into(),
                suggestion: None,
            };
            let payload = serialize_payload(&err, request.flags).unwrap_or_default();
            let resp = Frame {
                msg_type: MsgType::Error,
                flags: request.flags,
                payload: Bytes::from(payload),
            };
            write_frame(&mut send, &resp).await?;
            send.finish()?;
            return Ok(());
        }
        return super::snapshot::stream_snapshot(state, &mut send, request.flags)
            .await
            .map(|()| {
                let _ = send.finish();
            });
    }

    // Reject handshake on non-first stream
    if request.msg_type == MsgType::Handshake {
        let err = ErrorResponse {
            code: codes::INVALID_REQUEST,
            message: "Handshake only allowed as first message on connection".into(),
            suggestion: None,
        };
        let payload = serialize_payload(&err, request.flags).unwrap_or_default();
        let resp = Frame {
            msg_type: MsgType::Error,
            flags: request.flags,
            payload: Bytes::from(payload),
        };
        write_frame(&mut send, &resp).await?;
        send.finish()?;
        return Ok(());
    }

    // Handle and produce response
    let response = handler::handle_request(&state, &auth, request).await;

    // Write response frame
    write_frame(&mut send, &response).await?;
    send.finish()?;

    Ok(())
}

/// Handle a fire-and-forget telemetry datagram.
fn handle_datagram(state: &ServerState, auth: &AuthContext, data: &[u8]) {
    match selene_wire::datagram::TelemetryDatagram::decode(data) {
        Ok(dg) => {
            // Scope check: entity must be within principal's scope
            if !auth.in_scope(selene_core::NodeId(dg.entity_id)) {
                tracing::trace!(entity_id = dg.entity_id, "datagram rejected: out of scope");
                return;
            }
            // Encoding hints are set by the HTTP/MCP ts_write path which has
            // graph access. Datagrams skip schema lookup to preserve sub-microsecond
            // latency; buffers created here default to Gorilla encoding.
            state.hot_tier.append(
                selene_core::NodeId(dg.entity_id),
                &dg.property,
                selene_ts::TimeSample {
                    timestamp_nanos: dg.timestamp_nanos,
                    value: dg.value,
                },
            );
        }
        Err(e) => {
            tracing::trace!("invalid datagram: {e}");
        }
    }
}
