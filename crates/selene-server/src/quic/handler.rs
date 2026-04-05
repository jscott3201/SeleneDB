//! SWP request handler — thin dispatch layer over ops.

use std::sync::Arc;

use bytes::Bytes;
use selene_wire::dto::error::{ErrorResponse, codes};
use selene_wire::dto::graph_slice::{GraphSlicePayload, GraphSliceRequest};
use selene_wire::dto::ts::{TsPayload, TsRangeRequest, TsWriteRequest};
use selene_wire::serialize::{deserialize_payload, serialize_payload};
use selene_wire::{Frame, MsgType, WireFlags};

use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use crate::http::routes::is_gql_write;
use crate::ops::{self, OpError};

/// Handle a single SWP request frame and produce a response frame.
pub async fn handle_request(
    state: &Arc<ServerState>,
    auth: &Arc<AuthContext>,
    frame: Frame,
) -> Frame {
    let flags = frame.flags;
    let result = dispatch(state, auth, frame.msg_type, &frame.payload, flags).await;

    match result {
        Ok((msg_type, payload)) => Frame {
            msg_type,
            flags,
            payload: Bytes::from(payload),
        },
        Err(err_resp) => {
            let payload = serialize_payload(&err_resp, flags).unwrap_or_default();
            Frame {
                msg_type: MsgType::Error,
                flags,
                payload: Bytes::from(payload),
            }
        }
    }
}

async fn dispatch(
    state: &Arc<ServerState>,
    auth: &Arc<AuthContext>,
    msg_type: MsgType,
    payload: &[u8],
    flags: WireFlags,
) -> Result<(MsgType, Vec<u8>), ErrorResponse> {
    // Authorization check
    let action = msg_type_to_action(msg_type);
    if let Some(action) = action
        && !state.auth_engine.authorize_action(auth, action)
    {
        return Err(ErrorResponse {
            code: codes::AUTHORIZATION_DENIED,
            message: "access denied".into(),
            suggestion: None,
        });
    }

    match msg_type {
        MsgType::Health => {
            let resp = ops::health::health(state);
            Ok((MsgType::Ok, ser(&resp, flags)?))
        }
        MsgType::TsWrite => {
            let req: TsWriteRequest = de(payload, flags)?;
            let st = Arc::clone(state);
            let au = Arc::clone(auth);
            let count = state
                .mutation_batcher
                .submit(move || ops::ts::ts_write(&st, &au, req.samples))
                .await
                .map_err(|e| ErrorResponse {
                    code: codes::INTERNAL_ERROR,
                    message: e.to_string(),
                    suggestion: None,
                })?
                .map_err(to_wire)?;
            Ok((MsgType::Ok, ser(&count, flags)?))
        }
        MsgType::TsRangeQuery => {
            let req: TsRangeRequest = de(payload, flags)?;
            let samples = ops::ts::ts_range(
                state,
                auth,
                req.entity_id,
                &req.property,
                req.start_nanos,
                req.end_nanos,
                req.limit.map(|n| n as usize),
            )
            .map_err(to_wire)?;
            Ok((MsgType::TsPayload, ser(&TsPayload { samples }, flags)?))
        }
        MsgType::GqlQuery | MsgType::GqlExplain => {
            let req: selene_wire::dto::gql::GqlQueryRequest = de(payload, flags)?;
            let explain = req.explain || matches!(msg_type, MsgType::GqlExplain);
            let format = if flags.contains(WireFlags::JSON_FORMAT) {
                ops::gql::ResultFormat::Json
            } else {
                ops::gql::ResultFormat::ArrowIpc
            };
            // Convert JSON parameters to Value parameters for the ops layer
            let params = req.parameters.as_ref().map(|p| {
                p.iter()
                    .map(|(k, v)| (k.clone(), ops::json_to_value(v.clone())))
                    .collect::<std::collections::HashMap<String, selene_core::Value>>()
            });

            // Classify: read-only queries bypass batcher for lower latency.
            // Mutations and DDL route through the batcher for write ordering.
            let needs_batcher = is_gql_write(&req.query);

            let result = if needs_batcher {
                let st = Arc::clone(state);
                let au = Arc::clone(auth);
                let query = req.query.clone();
                state
                    .mutation_batcher
                    .submit(move || {
                        ops::gql::execute_gql_with_timeout(
                            &st,
                            &au,
                            &query,
                            params.as_ref(),
                            explain,
                            req.profile,
                            format,
                            req.timeout_ms,
                        )
                    })
                    .await
                    .map_err(|e| ErrorResponse {
                        code: codes::INTERNAL_ERROR,
                        message: e.to_string(),
                        suggestion: None,
                    })?
            } else {
                ops::gql::execute_gql_with_timeout(
                    state,
                    auth,
                    &req.query,
                    params.as_ref(),
                    explain,
                    req.profile,
                    format,
                    req.timeout_ms,
                )
            }
            .map_err(|e| ErrorResponse {
                code: codes::INTERNAL_ERROR,
                message: e.to_string(),
                suggestion: None,
            })?;

            let mutations =
                result
                    .mutations
                    .as_ref()
                    .map(|m| selene_wire::dto::gql::MutationStatsDto {
                        nodes_created: m.nodes_created,
                        nodes_deleted: m.nodes_deleted,
                        edges_created: m.edges_created,
                        edges_deleted: m.edges_deleted,
                        properties_set: m.properties_set,
                        properties_removed: m.properties_removed,
                    });
            let resp = selene_wire::dto::gql::GqlResultResponse {
                status_code: result.status_code,
                message: result.message,
                row_count: result.row_count,
                mutations,
                plan: result.plan,
            };

            // Attach Arrow IPC or JSON data as payload after the metadata
            let mut resp_bytes = ser(&resp, flags)?;
            if let Some(arrow_data) = result.data_arrow {
                resp_bytes.extend_from_slice(&arrow_data);
            } else if let Some(json_data) = result.data_json {
                resp_bytes.extend_from_slice(json_data.as_bytes());
            }

            Ok((MsgType::GqlResultPayload, resp_bytes))
        }
        MsgType::GraphSliceRequest => {
            let req: GraphSliceRequest = de(payload, flags)?;
            let result =
                ops::graph_slice::graph_slice(state, auth, &req.slice_type, req.limit, req.offset);
            Ok((
                MsgType::GraphSlicePayload,
                ser(
                    &GraphSlicePayload {
                        nodes: result.nodes,
                        edges: result.edges,
                        total_nodes: result.total_nodes,
                        total_edges: result.total_edges,
                    },
                    flags,
                )?,
            ))
        }
        // Federation messages
        MsgType::FederationRegister => {
            if let Some(fed_svc) = state.services.get::<crate::federation::FederationService>() {
                crate::federation::handler::handle_federation_register(
                    &fed_svc.registry,
                    payload,
                    flags,
                )
            } else {
                Err(ErrorResponse {
                    code: codes::INVALID_REQUEST,
                    message: "federation not enabled".into(),
                    suggestion: None,
                })
            }
        }
        MsgType::FederationPeerList => {
            if let Some(fed_svc) = state.services.get::<crate::federation::FederationService>() {
                crate::federation::handler::handle_federation_peer_list(
                    &fed_svc.registry,
                    payload,
                    flags,
                )
            } else {
                Err(ErrorResponse {
                    code: codes::INVALID_REQUEST,
                    message: "federation not enabled".into(),
                    suggestion: None,
                })
            }
        }
        MsgType::FederationGqlRequest => {
            crate::federation::handler::handle_federation_gql(state, auth, payload, flags)
        }
        MsgType::SyncPush => {
            let req: selene_wire::dto::sync::SyncPushRequest = de(payload, flags)?;
            crate::sync::validate_sync_push(state, &req).map_err(|e| ErrorResponse {
                code: codes::INVALID_REQUEST,
                message: e.to_string(),
                suggestion: Some("Reduce the number of entries or changes per batch".into()),
            })?;
            let ack = crate::sync::handle_sync_push(state, req, auth);
            Ok((MsgType::SyncPushAck, ser(&ack, flags)?))
        }
        MsgType::SyncSubscribe => {
            let req: selene_wire::dto::sync::SyncSubscribeRequest = de(payload, flags)?;
            crate::sync::validate_sync_subscribe(state, &req).map_err(|e| ErrorResponse {
                code: codes::INVALID_REQUEST,
                message: e.clone(),
                suggestion: Some("Reduce subscription complexity".into()),
            })?;
            let response = crate::sync::handle_sync_subscribe(state, req, auth);
            Ok((MsgType::SyncSubscribeResponse, ser(&response, flags)?))
        }
        _ => Err(ErrorResponse {
            code: codes::INVALID_REQUEST,
            message: format!("unsupported message type: {msg_type:?}"),
            suggestion: Some(
                "Use Health, TsWrite, TsRangeQuery, GqlQuery, or GraphSliceRequest".into(),
            ),
        }),
    }
}

fn msg_type_to_action(msg_type: MsgType) -> Option<Action> {
    match msg_type {
        MsgType::Health => None,
        MsgType::TsWrite => Some(Action::TsWrite),
        MsgType::TsRangeQuery => Some(Action::TsRead),
        // GQL types: ops layer does fine-grained auth, but transport-level
        // check provides defense-in-depth against unauthenticated access.
        MsgType::GqlQuery | MsgType::GqlExplain => Some(Action::GqlQuery),
        MsgType::GraphSliceRequest => Some(Action::GqlQuery),
        MsgType::FederationRegister
        | MsgType::FederationPeerList
        | MsgType::FederationGqlRequest => Some(Action::FederationManage),
        MsgType::SyncPush | MsgType::SyncSubscribe => Some(Action::FederationManage),
        _ => None,
    }
}

// ── SWP ser/deser helpers ───────────────────────────────────────────

fn de<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    flags: WireFlags,
) -> Result<T, ErrorResponse> {
    deserialize_payload(payload, flags).map_err(|e| ErrorResponse {
        code: codes::INVALID_REQUEST,
        message: format!("deserialization error: {e}"),
        suggestion: None,
    })
}

fn ser<T: serde::Serialize>(value: &T, flags: WireFlags) -> Result<Vec<u8>, ErrorResponse> {
    serialize_payload(value, flags).map_err(|e| ErrorResponse {
        code: codes::INTERNAL_ERROR,
        message: format!("serialization error: {e}"),
        suggestion: None,
    })
}

/// Map `OpError` to SWP `ErrorResponse`.
fn to_wire(e: OpError) -> ErrorResponse {
    match e {
        OpError::NotFound { entity, id } => ErrorResponse {
            code: codes::NOT_FOUND,
            message: format!("{entity} {id} not found"),
            suggestion: None,
        },
        OpError::AuthDenied => ErrorResponse {
            code: codes::AUTHORIZATION_DENIED,
            message: "access denied".into(),
            suggestion: None,
        },
        OpError::SchemaViolation(msg) => ErrorResponse {
            code: codes::SCHEMA_VIOLATION,
            message: msg,
            suggestion: Some(
                "Check node labels and required properties against registered schemas".into(),
            ),
        },
        OpError::InvalidRequest(msg) => ErrorResponse {
            code: codes::INVALID_REQUEST,
            message: msg,
            suggestion: None,
        },
        OpError::QueryError(msg) => ErrorResponse {
            code: codes::INTERNAL_ERROR,
            message: format!("GQL error: {msg}"),
            suggestion: Some(
                "Check GQL syntax. Use MATCH/RETURN for queries, INSERT/SET/DELETE for mutations."
                    .into(),
            ),
        },
        OpError::Internal(msg) => ErrorResponse {
            code: codes::INTERNAL_ERROR,
            message: msg,
            suggestion: None,
        },
        OpError::ReadOnly => ErrorResponse {
            code: codes::INVALID_REQUEST,
            message: "read-only replica".into(),
            suggestion: Some("Send mutations to the primary node".into()),
        },
        OpError::ResourcesExhausted(msg) => ErrorResponse {
            code: codes::INTERNAL_ERROR,
            message: msg,
            suggestion: Some("Increase memory.budget_mb or delete unused data".into()),
        },
        OpError::Conflict(msg) => ErrorResponse {
            code: codes::CONFLICT,
            message: msg,
            suggestion: None,
        },
    }
}
