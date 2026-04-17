//! MCP tool implementations for OAuth token revocation.

use rmcp::ErrorData as McpError;
use rmcp::model::CallToolResult;

use super::{SeleneTools, mcp_auth, op_err, structured_result};
use crate::http::mcp::params::{RevokeTokenParams, UnrevokeTokenParams};
use crate::ops;

pub(super) async fn revoke_token_impl(
    tools: &SeleneTools,
    p: RevokeTokenParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let result = ops::tokens::revoke_token(&tools.state, &auth, &p.token).map_err(op_err)?;
    Ok(structured_result(
        serde_json::to_value(&result).unwrap_or_default(),
    ))
}

pub(super) async fn list_revoked_tokens_impl(
    tools: &SeleneTools,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let list = ops::tokens::list_revoked_tokens(&tools.state, &auth).map_err(op_err)?;
    Ok(structured_result(
        serde_json::to_value(&list).unwrap_or_default(),
    ))
}

pub(super) async fn unrevoke_token_impl(
    tools: &SeleneTools,
    p: UnrevokeTokenParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let result = ops::tokens::unrevoke_token(&tools.state, &auth, &p.jti).map_err(op_err)?;
    Ok(structured_result(
        serde_json::to_value(&result).unwrap_or_default(),
    ))
}
