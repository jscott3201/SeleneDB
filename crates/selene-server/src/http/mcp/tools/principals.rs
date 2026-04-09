//! MCP tool implementations for principal management.

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, Content};

use super::{SeleneTools, format_json, mcp_auth, op_err};
use crate::http::mcp::params::{
    CreatePrincipalParams, DisablePrincipalParams, GetPrincipalParams, RotateCredentialParams,
    UpdatePrincipalParams,
};
use crate::ops;

pub(super) async fn list_principals_impl(tools: &SeleneTools) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let principals = ops::principals::list_principals(&tools.state, &auth).map_err(op_err)?;
    Ok(CallToolResult::success(vec![Content::text(format_json(
        &principals,
    ))]))
}

pub(super) async fn get_principal_impl(
    tools: &SeleneTools,
    p: GetPrincipalParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let principal =
        ops::principals::get_principal(&tools.state, &auth, &p.identity).map_err(op_err)?;
    Ok(CallToolResult::success(vec![Content::text(format_json(
        &principal,
    ))]))
}

pub(super) async fn create_principal_impl(
    tools: &SeleneTools,
    p: CreatePrincipalParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let principal = ops::principals::create_principal(
        &tools.state,
        &auth,
        &p.identity,
        &p.role,
        p.password.as_deref(),
    )
    .map_err(op_err)?;
    Ok(CallToolResult::success(vec![Content::text(format_json(
        &principal,
    ))]))
}

pub(super) async fn update_principal_impl(
    tools: &SeleneTools,
    p: UpdatePrincipalParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let principal = ops::principals::update_principal(
        &tools.state,
        &auth,
        &p.identity,
        p.role.as_deref(),
        p.enabled,
    )
    .map_err(op_err)?;
    Ok(CallToolResult::success(vec![Content::text(format_json(
        &principal,
    ))]))
}

pub(super) async fn disable_principal_impl(
    tools: &SeleneTools,
    p: DisablePrincipalParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let principal =
        ops::principals::disable_principal(&tools.state, &auth, &p.identity).map_err(op_err)?;
    Ok(CallToolResult::success(vec![Content::text(format_json(
        &principal,
    ))]))
}

pub(super) async fn rotate_credential_impl(
    tools: &SeleneTools,
    p: RotateCredentialParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    ops::principals::rotate_credential(&tools.state, &auth, &p.identity, &p.new_password)
        .map_err(op_err)?;
    Ok(CallToolResult::success(vec![Content::text(format!(
        "Credential rotated for principal '{}'",
        p.identity
    ))]))
}
