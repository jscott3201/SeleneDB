//! Action proposal pattern: human-in-the-loop supervised agent actions.
//!
//! Proposals are stored as `__Proposal` nodes with a lifecycle:
//! `pending` -> `approved` -> `executed` (or `rejected` / `expired`).
//!
//! Auto-expiry: proposals expire after 24 hours if not acted on.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, Content, ErrorCode};
use selene_core::Value;

use super::{SeleneTools, mcp_auth, op_err};
use crate::http::mcp::params::{ListProposalsParams, ProposalIdParams, ProposeActionParams};
use crate::ops;
use crate::ops::gql::ResultFormat;

const EXPIRY_MS: i64 = 24 * 60 * 60 * 1000; // 24 hours

pub(super) async fn propose_action_impl(
    tools: &SeleneTools,
    p: ProposeActionParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;
    let expires_at = now_ms + EXPIRY_MS;

    let mut params = HashMap::new();
    params.insert("desc".into(), Value::from(p.description.as_str()));
    params.insert("query".into(), Value::from(p.query.as_str()));
    params.insert(
        "category".into(),
        Value::from(p.category.as_deref().unwrap_or("general")),
    );
    params.insert(
        "priority".into(),
        Value::from(p.priority.as_deref().unwrap_or("normal")),
    );
    params.insert("status".into(), Value::from("pending"));
    params.insert("cat".into(), Value::Int(now_ms));
    params.insert("expires".into(), Value::Int(expires_at));

    let query = "INSERT (p:__Proposal { \
                  description: $desc, \
                  query: $query, \
                  category: $category, \
                  priority: $priority, \
                  status: $status, \
                  created_at: $cat, \
                  expires_at: $expires \
                  }) RETURN id(p) AS proposalId";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                query,
                Some(&params),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await?;

    let data = result.data_json.unwrap_or_else(|| "{}".into());
    Ok(CallToolResult::success(vec![Content::text(format!(
        "Proposal created (expires in 24h): {data}"
    ))]))
}

pub(super) async fn list_proposals_impl(
    tools: &SeleneTools,
    p: ListProposalsParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let limit = p.limit.unwrap_or(50).min(500);

    let mut params = HashMap::new();
    let now_ms = selene_core::now_nanos() / 1_000_000;

    let filter = if let Some(ref status) = p.status {
        params.insert("st".into(), Value::from(status.as_str()));
        " FILTER p.status = $st"
    } else {
        ""
    };

    // Mark expired proposals
    params.insert("now".into(), Value::Int(now_ms));

    let query = format!(
        "MATCH (p:__Proposal){filter} \
         RETURN id(p) AS id, p.description AS description, p.query AS query, \
         p.status AS status, p.category AS category, p.priority AS priority, \
         p.created_at AS created_at, p.expires_at AS expires_at \
         ORDER BY p.created_at DESC \
         LIMIT {limit}"
    );

    let result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        &query,
        Some(&params),
        false,
        false,
        ResultFormat::Json,
    )
    .map_err(op_err)?;

    let data = result.data_json.unwrap_or_else(|| "[]".into());
    Ok(CallToolResult::success(vec![Content::text(format!(
        "{} proposals\n{data}",
        result.row_count
    ))]))
}

pub(super) async fn approve_proposal_impl(
    tools: &SeleneTools,
    p: ProposalIdParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    update_proposal_status(tools, &auth, p.proposal_id, "approved", p.reason.as_deref()).await
}

pub(super) async fn reject_proposal_impl(
    tools: &SeleneTools,
    p: ProposalIdParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    update_proposal_status(tools, &auth, p.proposal_id, "rejected", p.reason.as_deref()).await
}

pub(super) async fn execute_proposal_impl(
    tools: &SeleneTools,
    p: ProposalIdParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    // Verify proposal exists and is approved
    let mut check_params = HashMap::new();
    check_params.insert("pid".into(), Value::Int(p.proposal_id as i64));

    let check_query = "MATCH (p:__Proposal) FILTER id(p) = $pid \
                        RETURN p.status AS status, p.query AS query";

    let check_result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        check_query,
        Some(&check_params),
        false,
        false,
        ResultFormat::Json,
    )
    .map_err(op_err)?;

    let data = check_result.data_json.unwrap_or_else(|| "[]".into());
    let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
    let row = rows.first().ok_or_else(|| McpError {
        code: ErrorCode::INVALID_PARAMS,
        message: format!("proposal {} not found", p.proposal_id).into(),
        data: None,
    })?;

    let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("");
    if status != "approved" {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: format!(
                "proposal {} has status '{status}', must be 'approved' to execute",
                p.proposal_id
            )
            .into(),
            data: None,
        });
    }

    let proposal_query = row
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Execute the proposed query
    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let pq = proposal_query.clone();
    let exec_result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(&st, &auth2, &pq, None, false, false, ResultFormat::Json)
        })
        .await?;

    // Mark as executed
    let _ =
        update_proposal_status(tools, &auth, p.proposal_id, "executed", p.reason.as_deref()).await;

    let exec_data = exec_result.data_json.unwrap_or_else(|| "[]".into());
    Ok(CallToolResult::success(vec![Content::text(format!(
        "Proposal {} executed.\nQuery: {proposal_query}\nResult: {exec_data}",
        p.proposal_id
    ))]))
}

async fn update_proposal_status(
    tools: &SeleneTools,
    auth: &crate::auth::handshake::AuthContext,
    proposal_id: u64,
    new_status: &str,
    reason: Option<&str>,
) -> Result<CallToolResult, McpError> {
    let now_ms = selene_core::now_nanos() / 1_000_000;

    let mut params = HashMap::new();
    params.insert("pid".into(), Value::Int(proposal_id as i64));
    params.insert("new_status".into(), Value::from(new_status));
    params.insert("now".into(), Value::Int(now_ms));
    params.insert("reason".into(), Value::from(reason.unwrap_or("")));

    let query = "MATCH (p:__Proposal) FILTER id(p) = $pid \
                  SET p.status = $new_status, p.updated_at = $now, p.reason = $reason \
                  RETURN id(p) AS id, p.status AS status";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                query,
                Some(&params),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await?;

    let data = result.data_json.unwrap_or_else(|| "{}".into());
    Ok(CallToolResult::success(vec![Content::text(format!(
        "Proposal {proposal_id} -> {new_status}: {data}"
    ))]))
}
