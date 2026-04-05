//! Federation message handlers — registration, peer list, and GQL forwarding.

use std::time::Instant;

use selene_wire::dto::error::{ErrorResponse, codes};
use selene_wire::dto::federation::{
    FederationGqlRequest, FederationGqlResponse, FederationPeerListResponse,
    FederationRegisterPayload,
};
use selene_wire::serialize::{deserialize_payload, serialize_payload};
use selene_wire::{MsgType, WireFlags};

use super::config::PeerRole;
use super::registry::{PeerInfo, PeerRegistry};

/// Handle a FederationRegister message — register the peer in our registry.
pub fn handle_federation_register(
    registry: &PeerRegistry,
    payload: &[u8],
    flags: WireFlags,
) -> Result<(MsgType, Vec<u8>), ErrorResponse> {
    let reg: FederationRegisterPayload =
        deserialize_payload(payload, flags).map_err(|e| ErrorResponse {
            code: codes::INVALID_REQUEST,
            message: format!("federation register deserialize: {e}"),
            suggestion: None,
        })?;

    registry.register(PeerInfo {
        name: reg.node_name,
        address: reg.address,
        schema_labels: reg.schema_labels,
        role: PeerRole::from_str(&reg.role),
        last_seen: Instant::now(),
        connected: true,
        bloom_filter: reg
            .bloom_filter
            .as_deref()
            .and_then(super::bloom::BloomFilter::from_bytes),
    });

    Ok((MsgType::Ok, vec![]))
}

/// Handle a FederationPeerList request — return all known peers.
pub fn handle_federation_peer_list(
    registry: &PeerRegistry,
    _payload: &[u8],
    flags: WireFlags,
) -> Result<(MsgType, Vec<u8>), ErrorResponse> {
    let peers = registry.all_peers_including_self();

    let resp = FederationPeerListResponse {
        peers: peers
            .iter()
            .map(|p| FederationRegisterPayload {
                node_name: p.name.clone(),
                address: p.address.clone(),
                schema_labels: p.schema_labels.clone(),
                role: p.role.as_str().into(),
                bloom_filter: p.bloom_filter.as_ref().map(|bf| bf.to_bytes()),
            })
            .collect(),
    };

    let bytes = serialize_payload(&resp, flags).map_err(|e| ErrorResponse {
        code: codes::INTERNAL_ERROR,
        message: format!("federation peer list serialize: {e}"),
        suggestion: None,
    })?;

    Ok((MsgType::FederationPeerListResponse, bytes))
}

/// Handle a forwarded GQL query from a federated peer.
///
/// Executes the query against the local default graph with the forwarded scope.
pub fn handle_federation_gql(
    state: &crate::bootstrap::ServerState,
    auth: &crate::auth::handshake::AuthContext,
    payload: &[u8],
    flags: WireFlags,
) -> Result<(MsgType, Vec<u8>), ErrorResponse> {
    let req: FederationGqlRequest =
        deserialize_payload(payload, flags).map_err(|e| ErrorResponse {
            code: codes::INVALID_REQUEST,
            message: format!("federation gql deserialize: {e}"),
            suggestion: None,
        })?;

    let format = if req.json_format {
        crate::ops::gql::ResultFormat::Json
    } else {
        crate::ops::gql::ResultFormat::ArrowIpc
    };

    // Execute against local default graph with the caller's auth context
    let result = crate::ops::gql::execute_gql(state, auth, &req.query, None, false, false, format)
        .map_err(|e| ErrorResponse {
            code: codes::INTERNAL_ERROR,
            message: format!("federation gql execute: {e}"),
            suggestion: None,
        })?;

    let resp = FederationGqlResponse {
        status_code: result.status_code,
        message: result.message,
        row_count: result.row_count,
        ipc_bytes: result.data_arrow,
        json_result: result.data_json,
        error: None,
    };

    let bytes = serialize_payload(&resp, flags).map_err(|e| ErrorResponse {
        code: codes::INTERNAL_ERROR,
        message: format!("federation gql response serialize: {e}"),
        suggestion: None,
    })?;

    Ok((MsgType::FederationGqlResponse, bytes))
}
