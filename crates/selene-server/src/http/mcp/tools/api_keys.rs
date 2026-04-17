//! MCP tool implementations for API-key management.

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, Content};

use super::{SeleneTools, format_json, mcp_auth, op_err};
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
    Ok(CallToolResult::success(vec![Content::text(format_json(
        &result,
    ))]))
}

pub(super) async fn list_api_keys_impl(
    tools: &SeleneTools,
    p: ListApiKeysParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let keys = ops::api_keys::list_api_keys(&tools.state, &auth, p.identity.as_deref())
        .map_err(op_err)?;
    Ok(CallToolResult::success(vec![Content::text(format_json(
        &keys,
    ))]))
}

pub(super) async fn revoke_api_key_impl(
    tools: &SeleneTools,
    p: RevokeApiKeyParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let key = ops::api_keys::revoke_api_key(&tools.state, &auth, p.key_id).map_err(op_err)?;
    Ok(CallToolResult::success(vec![Content::text(format_json(
        &key,
    ))]))
}
