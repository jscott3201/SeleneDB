//! MCP tool implementations for API-key management.

use rmcp::ErrorData as McpError;
use rmcp::model::CallToolResult;

use super::{SeleneTools, mcp_auth, op_err, structured_result};
use crate::http::mcp::params::{CreateApiKeyParams, ListApiKeysParams, RevokeApiKeyParams};
use crate::ops;

pub(super) async fn create_api_key_impl(
    tools: &SeleneTools,
    p: CreateApiKeyParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let result = ops::api_keys::create_api_key(
        &tools.state,
        &auth,
        &p.name,
        &p.identity,
        p.ttl_days,
        p.scopes,
    )
    .map_err(op_err)?;
    Ok(structured_result(
        serde_json::to_value(&result).unwrap_or_default(),
    ))
}

pub(super) async fn list_api_keys_impl(
    tools: &SeleneTools,
    p: ListApiKeysParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let keys =
        ops::api_keys::list_api_keys(&tools.state, &auth, p.identity.as_deref()).map_err(op_err)?;
    Ok(structured_result(
        serde_json::to_value(&keys).unwrap_or_default(),
    ))
}

pub(super) async fn revoke_api_key_impl(
    tools: &SeleneTools,
    p: RevokeApiKeyParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let key = ops::api_keys::revoke_api_key(&tools.state, &auth, p.key_id).map_err(op_err)?;
    Ok(structured_result(
        serde_json::to_value(&key).unwrap_or_default(),
    ))
}
