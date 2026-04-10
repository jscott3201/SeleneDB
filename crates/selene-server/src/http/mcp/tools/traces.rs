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

use super::{SeleneTools, mcp_auth, op_err, reject_replica};
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use crate::http::mcp::params::{
    ExportTracesParams, LogOutcomeParams, LogSessionParams, LogTraceParams,
};
use crate::ops;
use crate::ops::gql::ResultFormat;

/// Implement the `log_trace` tool.
pub(super) async fn log_trace_impl(
    tools: &SeleneTools,
    p: LogTraceParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let session_id = p.session_id.clone();
    let turn = p.turn;

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
    params.insert(
        "thinking".into(),
        Value::from(p.thinking.as_deref().unwrap_or("")),
    );
    params.insert(
        "user_query".into(),
        Value::from(p.user_query.as_deref().unwrap_or("")),
    );

    let now_ms = selene_core::now_nanos() / 1_000_000;
    params.insert("ts".into(), Value::Int(now_ms));

    let about_ids = p.about_node_ids;

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
                  thinking: $thinking, \
                  user_query: $user_query, \
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

    // Parse the created trace node ID for edge creation.
    // Note: id() returns GqlValue::Int, so use as_i64().
    let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
    let trace_id = rows
        .first()
        .and_then(|r| r.get("traceId"))
        .and_then(|v| v.as_i64());

    // Create :about edges to referenced entities.
    if let (Some(trace_id), Some(node_ids)) = (trace_id, &about_ids) {
        for &target_id in node_ids {
            let mut edge_params = HashMap::new();
            edge_params.insert("src".into(), Value::Int(trace_id));
            edge_params.insert("tgt".into(), Value::Int(target_id as i64));

            let edge_query = "MATCH (c) WHERE id(c) = $src \
                              MATCH (t) WHERE id(t) = $tgt \
                              INSERT (c)-[:about]->(t)";

            let _ = ops::gql::execute_gql(
                &tools.state,
                &auth,
                edge_query,
                Some(&edge_params),
                false,
                false,
                ResultFormat::Json,
            );
        }
    }

    // Create :next_turn edge from the previous turn's trace (best-effort).
    if let Some(trace_id) = trace_id
        && turn > 0
    {
        let prev_turn = turn - 1;
        let mut seq_params = HashMap::new();
        seq_params.insert("sid".into(), Value::from(session_id.as_str()));
        seq_params.insert("prev_turn".into(), Value::Int(prev_turn));
        seq_params.insert("tid".into(), Value::Int(trace_id));

        let seq_query = "MATCH (prev:__Trace) \
                         WHERE prev.session_id = $sid AND prev.turn = $prev_turn \
                         MATCH (cur) WHERE id(cur) = $tid \
                         INSERT (prev)-[:next_turn]->(cur)";

        let _ = ops::gql::execute_gql(
            &tools.state,
            &auth,
            seq_query,
            Some(&seq_params),
            false,
            false,
            ResultFormat::Json,
        );
    }

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
    params.insert("lim".into(), Value::Int(limit as i64));

    let query = format!(
        "MATCH (t:__Trace){filter} \
         RETURN t.session_id AS session_id, t.turn AS turn, \
         t.tool_name AS tool_name, t.tool_params AS tool_params, \
         t.tool_result_summary AS result_summary, \
         t.agent_response AS agent_response, \
         t.feedback AS feedback, t.correction AS correction, \
         t.model_id AS model_id, t.latency_ms AS latency_ms, \
         t.thinking AS thinking, t.user_query AS user_query, \
         t.timestamp AS timestamp \
         ORDER BY t.timestamp \
         LIMIT $lim"
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

    match format {
        "jsonl" => {
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
        }
        "huggingface" => {
            let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
            let output = format_huggingface(&rows, &tools.state, &auth);
            Ok(CallToolResult::success(vec![Content::text(output)]))
        }
        _ => Ok(CallToolResult::success(vec![Content::text(format!(
            "{} traces exported\n{data}",
            result.row_count
        ))])),
    }
}

/// Implement the `log_session` tool.
pub(super) async fn log_session_impl(
    tools: &SeleneTools,
    p: LogSessionParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;

    let mut params = HashMap::new();
    params.insert("sid".into(), Value::from(p.session_id.as_str()));
    params.insert(
        "sp".into(),
        Value::from(p.system_prompt.as_deref().unwrap_or("")),
    );
    params.insert(
        "building".into(),
        Value::from(p.building.as_deref().unwrap_or("")),
    );
    params.insert(
        "role".into(),
        Value::from(p.operator_role.as_deref().unwrap_or("")),
    );
    params.insert(
        "weather".into(),
        Value::from(p.weather.as_deref().unwrap_or("")),
    );
    params.insert(
        "tier".into(),
        Value::from(p.active_tier.as_deref().unwrap_or("")),
    );
    params.insert(
        "meta".into(),
        Value::from(p.metadata.as_deref().unwrap_or("")),
    );
    params.insert("now".into(), Value::Int(now_ms));

    let query = "MERGE (s:__TraceSession {session_id: $sid}) \
                  ON CREATE SET \
                      s.system_prompt = $sp, \
                      s.building = $building, \
                      s.operator_role = $role, \
                      s.weather = $weather, \
                      s.active_tier = $tier, \
                      s.metadata = $meta, \
                      s.created_at = $now, \
                      s.updated_at = $now \
                  ON MATCH SET \
                      s.system_prompt = $sp, \
                      s.building = $building, \
                      s.operator_role = $role, \
                      s.weather = $weather, \
                      s.active_tier = $tier, \
                      s.metadata = $meta, \
                      s.updated_at = $now \
                  RETURN id(s) AS sessionId";

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
        "Session logged: {data}"
    ))]))
}

/// Implement the `log_outcome` tool.
pub(super) async fn log_outcome_impl(
    tools: &SeleneTools,
    p: LogOutcomeParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;

    let mut params = HashMap::new();
    params.insert("sid".into(), Value::from(p.session_id.as_str()));
    params.insert("success".into(), Value::Bool(p.success));
    params.insert("summary".into(), Value::from(p.outcome_summary.as_str()));
    params.insert(
        "reason".into(),
        Value::from(p.failure_reason.as_deref().unwrap_or("")),
    );
    params.insert("score".into(), Value::Int(p.quality_score.unwrap_or(-1)));
    params.insert("now".into(), Value::Int(now_ms));

    let query = "MERGE (o:__TraceOutcome {session_id: $sid}) \
                  ON CREATE SET \
                      o.success = $success, \
                      o.outcome_summary = $summary, \
                      o.failure_reason = $reason, \
                      o.quality_score = $score, \
                      o.created_at = $now, \
                      o.updated_at = $now \
                  ON MATCH SET \
                      o.success = $success, \
                      o.outcome_summary = $summary, \
                      o.failure_reason = $reason, \
                      o.quality_score = $score, \
                      o.updated_at = $now \
                  RETURN id(o) AS outcomeId";

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
        "Outcome logged: {data}"
    ))]))
}

// ── HuggingFace chat format export ─────────────────────────────────

/// Format trace rows as HuggingFace chat-format JSONL (one line per session).
fn format_huggingface(
    rows: &[serde_json::Value],
    state: &ServerState,
    auth: &AuthContext,
) -> String {
    use serde_json::json;
    use std::collections::BTreeMap;

    // Group traces by session_id, preserving turn order.
    let mut sessions: BTreeMap<String, Vec<&serde_json::Value>> = BTreeMap::new();
    for row in rows {
        let sid = row
            .get("session_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        sessions.entry(sid.to_string()).or_default().push(row);
    }

    // Sort each session's traces by turn number.
    for traces in sessions.values_mut() {
        traces.sort_by_key(|t| t.get("turn").and_then(|v| v.as_i64()).unwrap_or(0));
    }

    let mut lines = Vec::new();

    for (sid, traces) in &sessions {
        let mut messages = Vec::new();
        let mut tool_names: Vec<String> = Vec::new();

        // Fetch session metadata for system prompt.
        let sp = get_session_system_prompt(state, auth, sid);
        if !sp.is_empty() {
            messages.push(json!({"role": "system", "content": sp}));
        }

        // Build messages from traces.
        for trace in traces {
            let tool_name = trace
                .get("tool_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tool_params = trace
                .get("tool_params")
                .and_then(|v| v.as_str())
                .unwrap_or("{}");
            let result_summary = trace
                .get("result_summary")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let agent_response = trace
                .get("agent_response")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let thinking = trace.get("thinking").and_then(|v| v.as_str()).unwrap_or("");
            let user_query = trace
                .get("user_query")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            // User message (if present).
            if !user_query.is_empty() {
                messages.push(json!({"role": "user", "content": user_query}));
            }

            // Assistant message with tool call.
            let mut assistant_msg = json!({
                "role": "assistant",
                "content": serde_json::Value::Null,
                "tool_calls": [{
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "arguments": tool_params
                    }
                }]
            });
            if !thinking.is_empty() {
                assistant_msg["thinking"] = json!(thinking);
            }
            messages.push(assistant_msg);

            // Tool result message.
            messages.push(json!({
                "role": "tool",
                "name": tool_name,
                "content": result_summary
            }));

            // Final assistant response (if present).
            if !agent_response.is_empty() {
                messages.push(json!({"role": "assistant", "content": agent_response}));
            }

            // Track unique tool names.
            if !tool_name.is_empty() && !tool_names.contains(&tool_name.to_string()) {
                tool_names.push(tool_name.to_string());
            }
        }

        // Build tools array.
        let tools: Vec<serde_json::Value> = tool_names
            .iter()
            .map(|name| {
                json!({
                    "type": "function",
                    "function": {"name": name}
                })
            })
            .collect();

        // Build metadata from outcome.
        let mut metadata = json!({"session_id": sid});
        if let Some(outcome) = get_session_outcome(state, auth, sid) {
            metadata["success"] = outcome
                .get("success")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            metadata["quality_score"] = outcome
                .get("quality_score")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
        }

        // Get model_id from first trace.
        if let Some(model) = traces
            .first()
            .and_then(|t| t.get("model_id"))
            .and_then(|v| v.as_str())
            && !model.is_empty()
        {
            metadata["model_id"] = json!(model);
        }

        let example = json!({
            "messages": messages,
            "tools": tools,
            "metadata": metadata
        });
        lines.push(serde_json::to_string(&example).unwrap_or_default());
    }

    let session_count = sessions.len();
    let trace_count = rows.len();
    format!(
        "{trace_count} traces across {session_count} sessions exported (HuggingFace chat)\n{}",
        lines.join("\n")
    )
}

/// Get the system prompt from a `__TraceSession` node.
fn get_session_system_prompt(state: &ServerState, auth: &AuthContext, session_id: &str) -> String {
    let mut params = HashMap::new();
    params.insert("sid".into(), Value::from(session_id));

    let query = "MATCH (s:__TraceSession) FILTER s.session_id = $sid \
                 RETURN s.system_prompt AS system_prompt";
    let result = ops::gql::execute_gql(
        state,
        auth,
        query,
        Some(&params),
        false,
        false,
        ResultFormat::Json,
    );
    match result {
        Ok(r) => {
            let data = r.data_json.unwrap_or_default();
            let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
            rows.first()
                .and_then(|r| r.get("system_prompt"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        }
        Err(_) => String::new(),
    }
}

/// Get outcome data from a `__TraceOutcome` node.
fn get_session_outcome(
    state: &ServerState,
    auth: &AuthContext,
    session_id: &str,
) -> Option<serde_json::Value> {
    let mut params = HashMap::new();
    params.insert("sid".into(), Value::from(session_id));

    let query = "MATCH (o:__TraceOutcome) FILTER o.session_id = $sid \
                 RETURN o.success AS success, \
                 o.quality_score AS quality_score, \
                 o.outcome_summary AS outcome_summary";
    let result = ops::gql::execute_gql(
        state,
        auth,
        query,
        Some(&params),
        false,
        false,
        ResultFormat::Json,
    )
    .ok()?;

    let data = result.data_json?;
    let rows: Vec<serde_json::Value> = serde_json::from_str(&data).ok()?;
    rows.into_iter().next()
}
