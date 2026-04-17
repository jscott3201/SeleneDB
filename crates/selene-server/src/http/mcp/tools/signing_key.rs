//! MCP tool for OAuth signing-key rotation.

use rmcp::ErrorData as McpError;
use rmcp::model::CallToolResult;

use super::{SeleneTools, mcp_auth, op_err, structured_result};
use crate::http::mcp::params::RotateSigningKeyParams;
use crate::ops;

pub(super) async fn rotate_signing_key_impl(
    tools: &SeleneTools,
    p: RotateSigningKeyParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let result = ops::signing_key::rotate_signing_key(&tools.state, &auth, p.retire_for_secs)
        .map_err(op_err)?;
    Ok(structured_result(
        serde_json::to_value(&result).unwrap_or_default(),
    ))
}
