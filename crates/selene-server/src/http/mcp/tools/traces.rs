//! Training data export: __Trace system label for interaction logging.
//!
//! Stores tool-call interaction traces as __Trace nodes for fine-tuning
//! data extraction. The `log_trace` tool is called by agent orchestrators
//! after each tool call, not by the agent itself. The `export_traces` tool
//! exports traces in JSONL format compatible with TRL, Axolotl, and Unsloth.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, Content};
use selene_core::Value;

use super::{SeleneTools, mcp_auth, op_err};
use crate::http::mcp::params::{ExportTracesParams, LogTraceParams};
use crate::ops;
use crate::ops::gql::ResultFormat;

/// Implement the `log_trace` tool.
pub(super) async fn log_trace_impl(
    tools: &SeleneTools,
    p: LogTraceParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    let mut params = HashMap::new();
    params.insert("session_id".into(), Value::from(p.session_id.as_str()));
    params.insert("turn".into(), Value::Int(p.turn));
    params.insert("tool_name".into(), Value::from(p.tool_name.as_str()));
    params.insert("tool_params".into(), Value::from(p.tool_params.as_str()));
    params.insert(
        "tool_result_summary".into(),
        Value::from(p.tool_result_summary.as_str()),
    );
    params.insert(
        "agent_response".into(),
        Value::from(p.agent_response.as_deref().unwrap_or("")),
    );
    params.insert(
        "feedback".into(),
        Value::from(p.feedback.as_deref().unwrap_or("none")),
    );
    params.insert(
        "correction".into(),
        Value::from(p.correction.as_deref().unwrap_or("")),
    );
    params.insert(
        "model_id".into(),
        Value::from(p.model_id.as_deref().unwrap_or("")),
    );
    params.insert("latency_ms".into(), Value::Int(p.latency_ms.unwrap_or(0)));

    let now_ms = selene_core::now_nanos() / 1_000_000;
    params.insert("ts".into(), Value::Int(now_ms));

    let query = "INSERT (t:__Trace { \
                  session_id: $session_id, \
                  turn: $turn, \
                  tool_name: $tool_name, \
                  tool_params: $tool_params, \
                  tool_result_summary: $tool_result_summary, \
                  agent_response: $agent_response, \
                  feedback: $feedback, \
                  correction: $correction, \
                  model_id: $model_id, \
                  latency_ms: $latency_ms, \
                  timestamp: $ts \
                  }) RETURN id(t) AS traceId";

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
        "Trace logged: {data}"
    ))]))
}

/// Implement the `export_traces` tool.
pub(super) async fn export_traces_impl(
    tools: &SeleneTools,
    p: ExportTracesParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    // Build filter clauses
    let mut conditions = Vec::new();
    let mut params = HashMap::new();

    if let Some(ref session) = p.session_id {
        conditions.push("t.session_id = $sid");
        params.insert("sid".into(), Value::from(session.as_str()));
    }
    if let Some(ref tool) = p.tool_name {
        conditions.push("t.tool_name = $tname");
        params.insert("tname".into(), Value::from(tool.as_str()));
    }
    if let Some(ref feedback) = p.feedback {
        conditions.push("t.feedback = $fb");
        params.insert("fb".into(), Value::from(feedback.as_str()));
    }
    if let Some(ref model) = p.model_id {
        conditions.push("t.model_id = $mid");
        params.insert("mid".into(), Value::from(model.as_str()));
    }
    if let Some(start) = p.start_ms {
        conditions.push("t.timestamp >= $start");
        params.insert("start".into(), Value::Int(start));
    }
    if let Some(end) = p.end_ms {
        conditions.push("t.timestamp <= $end");
        params.insert("end".into(), Value::Int(end));
    }

    let filter = if conditions.is_empty() {
        String::new()
    } else {
        format!(" FILTER {}", conditions.join(" AND "))
    };

    let limit = p.limit.unwrap_or(1000).min(10_000);
    let query = format!(
        "MATCH (t:__Trace){filter} \
         RETURN t.session_id AS session_id, t.turn AS turn, \
         t.tool_name AS tool_name, t.tool_params AS tool_params, \
         t.tool_result_summary AS result_summary, \
         t.agent_response AS agent_response, \
         t.feedback AS feedback, t.correction AS correction, \
         t.model_id AS model_id, t.latency_ms AS latency_ms, \
         t.timestamp AS timestamp \
         ORDER BY t.timestamp \
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

    let format = p.format.as_deref().unwrap_or("jsonl");
    let data = result.data_json.unwrap_or_else(|| "[]".into());

    if format == "jsonl" {
        // Convert JSON array to JSONL (one object per line)
        let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
        let jsonl: String = rows
            .iter()
            .map(|r| serde_json::to_string(r).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{} traces exported (JSONL)\n{jsonl}",
            rows.len()
        ))]))
    } else {
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{} traces exported\n{data}",
            result.row_count
        ))]))
    }
}
