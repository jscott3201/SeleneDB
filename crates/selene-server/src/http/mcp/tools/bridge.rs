//! Context bridge: multi-agent coordination via shared graph state.
//!
//! Implements the graph-native blackboard pattern for agent presence,
//! shared context, and intent-based conflict avoidance.
//!
//! Node types:
//! - `__AgentSession`: agent presence and liveness
//! - `__SharedContext`: published discoveries, decisions, warnings
//! - `__Intent`: work claims with advisory/exclusive/locked levels

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, Content, ErrorCode};
use selene_core::Value;

use super::{SeleneTools, mcp_auth, op_err, reject_replica};
use crate::http::mcp::params::*;
use crate::ops;
use crate::ops::gql::ResultFormat;

// ── Agent Session ───────────────────────────────────────────────────

pub(super) async fn register_agent_impl(
    tools: &SeleneTools,
    p: RegisterAgentParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;

    // Atomic upsert via MERGE — avoids TOCTOU race between read and write.
    let mut params = HashMap::new();
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));
    params.insert("project".into(), Value::from(p.project.as_str()));
    params.insert("status".into(), Value::from("active"));
    params.insert("now".into(), Value::Int(now_ms));
    params.insert(
        "working_on".into(),
        p.working_on.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert(
        "capabilities".into(),
        p.capabilities.as_deref().map_or(Value::Null, Value::from),
    );

    let files_str = p
        .files_touched
        .as_ref()
        .map(|f| serde_json::to_string(f).unwrap_or_else(|_| "[]".into()));
    params.insert(
        "files".into(),
        files_str.as_deref().map_or(Value::Null, Value::from),
    );

    // Structured capability fields (JSON-serialised Vec for storage).
    let tools_str = p
        .supported_tools
        .as_ref()
        .map(|t| serde_json::to_string(t).unwrap_or_else(|_| "[]".into()));
    params.insert(
        "supported_tools".into(),
        tools_str.as_deref().map_or(Value::Null, Value::from),
    );
    let expertise_str = p
        .domain_expertise
        .as_ref()
        .map(|e| serde_json::to_string(e).unwrap_or_else(|_| "[]".into()));
    params.insert(
        "domain_expertise".into(),
        expertise_str.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert(
        "model_family".into(),
        p.model_family.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert(
        "context_window".into(),
        p.context_window.map_or(Value::Null, Value::Int),
    );

    let query = "MERGE (a:__AgentSession {agent_id: $aid}) \
                  ON CREATE SET \
                      a.project = $project, \
                      a.status = $status, \
                      a.working_on = $working_on, \
                      a.files_touched = $files, \
                      a.capabilities = $capabilities, \
                      a.supported_tools = $supported_tools, \
                      a.domain_expertise = $domain_expertise, \
                      a.model_family = $model_family, \
                      a.context_window = $context_window, \
                      a.heartbeat_at = $now, \
                      a.started_at = $now \
                  ON MATCH SET \
                      a.project = $project, \
                      a.status = $status, \
                      a.working_on = $working_on, \
                      a.files_touched = $files, \
                      a.capabilities = $capabilities, \
                      a.supported_tools = $supported_tools, \
                      a.domain_expertise = $domain_expertise, \
                      a.model_family = $model_family, \
                      a.context_window = $context_window, \
                      a.heartbeat_at = $now \
                  RETURN id(a) AS id";

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

    // Surface Convention nodes matching the agent's project or global scope.
    let mut conv_params = HashMap::new();
    conv_params.insert("project".into(), Value::from(p.project.as_str()));

    let conv_query = "MATCH (c:Convention) \
                       WHERE c.scope = $project OR c.scope = 'global' \
                       RETURN c.name AS name, c.scope AS scope, \
                       c.severity AS severity, c.description AS description \
                       ORDER BY c.severity DESC, c.name ASC \
                       LIMIT 50";

    let conv_result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        conv_query,
        Some(&conv_params),
        false,
        false,
        ResultFormat::Json,
    )
    .map_err(op_err)?;

    let mut text = format!("Agent registered: {data}");
    let conv_count = conv_result.row_count;
    if conv_count > 0 {
        let conv_data = conv_result.data_json.unwrap_or_else(|| "[]".to_string());
        let _ = write!(
            text,
            "\n\n{conv_count} active convention(s) for project '{}':\n{conv_data}",
            p.project
        );
    }

    Ok(CallToolResult::success(vec![Content::text(text)]))
}

pub(super) async fn heartbeat_impl(
    tools: &SeleneTools,
    p: HeartbeatParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;

    let mut params = HashMap::new();
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));
    params.insert("now".into(), Value::Int(now_ms));

    // Build dynamic SET clause based on provided fields
    let mut set_parts = vec!["a.heartbeat_at = $now", "a.status = 'active'"];
    if let Some(ref working_on) = p.working_on {
        params.insert("working_on".into(), Value::from(working_on.as_str()));
        set_parts.push("a.working_on = $working_on");
    }
    if let Some(ref files) = p.files_touched {
        let files_str = serde_json::to_string(files).unwrap_or_else(|_| "[]".into());
        params.insert("files".into(), Value::from(files_str.as_str()));
        set_parts.push("a.files_touched = $files");
    }

    let set_clause = set_parts.join(", ");
    let query = format!(
        "MATCH (a:__AgentSession {{agent_id: $aid}}) \
         SET {set_clause} \
         RETURN id(a) AS id"
    );

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                &query,
                Some(&params),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await?;

    if result.row_count == 0 {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: format!("No active session found for agent '{}'", p.agent_id).into(),
            data: None,
        });
    }

    Ok(CallToolResult::success(vec![Content::text("Heartbeat OK")]))
}

pub(super) async fn deregister_agent_impl(
    tools: &SeleneTools,
    p: DeregisterAgentParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let mut params = HashMap::new();
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));

    // 1. Mark session as done
    let session_query = "MATCH (a:__AgentSession {agent_id: $aid}) \
                          SET a.status = 'done' \
                          RETURN id(a) AS id";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let params2 = params.clone();
    tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                session_query,
                Some(&params2),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await?;

    // 2. Release all intents for this agent
    let intent_query = "MATCH (i:__Intent {agent_id: $aid}) \
                         FILTER i.status = 'claimed' \
                         SET i.status = 'released' \
                         RETURN count(i) AS released";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                intent_query,
                Some(&params),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await?;

    let intents_data = result.data_json.unwrap_or_else(|| "{}".into());

    // 3. Reassign active tasks back to proposed for re-delegation.
    let task_query = "MATCH (t:__Task {assignee_agent: $aid}) \
                       FILTER t.status = 'accepted' OR t.status = 'working' \
                       SET t.status = 'proposed', t.assignee_agent = NULL, \
                           t.updated_at = $now \
                       RETURN count(t) AS reassigned";

    let mut task_params = HashMap::new();
    task_params.insert("aid".into(), Value::from(p.agent_id.as_str()));
    task_params.insert(
        "now".into(),
        Value::Int(selene_core::now_nanos() / 1_000_000),
    );

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let task_result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                task_query,
                Some(&task_params),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await?;

    let tasks_data = task_result.data_json.unwrap_or_else(|| "{}".into());
    Ok(CallToolResult::success(vec![Content::text(format!(
        "Agent '{}' deregistered. Intents released: {intents_data}. Tasks reassigned: {tasks_data}",
        p.agent_id
    ))]))
}

pub(super) async fn list_agents_impl(
    tools: &SeleneTools,
    p: ListAgentsParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    let mut params = HashMap::new();
    let mut filters = Vec::new();

    if let Some(ref project) = p.project {
        params.insert("project".into(), Value::from(project.as_str()));
        filters.push("a.project = $project");
    }
    if let Some(ref status) = p.status {
        params.insert("status".into(), Value::from(status.as_str()));
        filters.push("a.status = $status");
    }

    let filter_clause = if filters.is_empty() {
        String::new()
    } else {
        format!(" FILTER {}", filters.join(" AND "))
    };

    let query = format!(
        "MATCH (a:__AgentSession){filter_clause} \
         RETURN a.agent_id AS agent_id, a.project AS project, \
         a.status AS status, a.working_on AS working_on, \
         a.files_touched AS files_touched, \
         a.capabilities AS capabilities, \
         a.supported_tools AS supported_tools, \
         a.domain_expertise AS domain_expertise, \
         a.model_family AS model_family, \
         a.context_window AS context_window, \
         a.heartbeat_at AS heartbeat_at, a.started_at AS started_at \
         ORDER BY a.heartbeat_at DESC"
    );

    let gql_params = if params.is_empty() {
        None
    } else {
        Some(&params)
    };

    let result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        &query,
        gql_params,
        false,
        false,
        ResultFormat::Json,
    )
    .map_err(op_err)?;

    let data = result.data_json.unwrap_or_else(|| "[]".into());
    let mut text = format!("Active agents ({} found):\n", result.row_count);
    let _ = write!(text, "{data}");
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

// ── Shared Context ──────────────────────────────────────────────────

pub(super) async fn share_context_impl(
    tools: &SeleneTools,
    p: ShareContextParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;

    let mut params = HashMap::new();
    params.insert("author".into(), Value::from(p.author.as_str()));
    params.insert("ctype".into(), Value::from(p.context_type.as_str()));
    params.insert("scope".into(), Value::from(p.scope.as_str()));
    params.insert("content".into(), Value::from(p.content.as_str()));
    params.insert("visibility".into(), Value::from(p.visibility.as_str()));
    params.insert("now".into(), Value::Int(now_ms));

    let targets_str = p
        .targets
        .as_ref()
        .map(|t| serde_json::to_string(t).unwrap_or_else(|_| "[]".into()));
    params.insert(
        "targets".into(),
        targets_str.as_deref().map_or(Value::Null, Value::from),
    );

    let ttl = p.ttl_ms.unwrap_or(0);
    params.insert("ttl".into(), Value::Int(ttl));
    let expires_at = if ttl > 0 { now_ms + ttl } else { 0 };
    params.insert("expires_at".into(), Value::Int(expires_at));

    let query = "INSERT (c:__SharedContext { \
                  author: $author, \
                  context_type: $ctype, \
                  scope: $scope, \
                  targets: $targets, \
                  content: $content, \
                  visibility: $visibility, \
                  ttl_ms: $ttl, \
                  created_at: $now, \
                  expires_at: $expires_at \
                  }) RETURN id(c) AS id";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let about_ids = p.about_node_ids.clone();
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

    // Create "about" edges to referenced nodes if provided
    if let Some(ref node_ids) = about_ids
        && !node_ids.is_empty()
    {
        // Parse the created context node ID from the result
        let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
        if let Some(ctx_id) = rows
            .first()
            .and_then(|r| r.get("id"))
            .and_then(|v| v.as_u64())
        {
            for &target_id in node_ids {
                let mut edge_params = HashMap::new();
                edge_params.insert("src".into(), Value::Int(ctx_id as i64));
                edge_params.insert("tgt".into(), Value::Int(target_id as i64));

                let edge_query = "MATCH (c) WHERE id(c) = $src \
                                   MATCH (t) WHERE id(t) = $tgt \
                                   INSERT (c)-[:about]->(t)";

                let st = Arc::clone(&tools.state);
                let auth2 = auth.clone();
                let _ = tools
                    .submit_mut(move || {
                        ops::gql::execute_gql(
                            &st,
                            &auth2,
                            edge_query,
                            Some(&edge_params),
                            false,
                            false,
                            ResultFormat::Json,
                        )
                    })
                    .await;
            }
        }
    }

    // Also link to the author's agent session
    let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
    if let Some(ctx_id) = rows
        .first()
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_u64())
    {
        let mut link_params = HashMap::new();
        link_params.insert("aid".into(), Value::from(p.author.as_str()));
        link_params.insert("cid".into(), Value::Int(ctx_id as i64));

        let link_query = "MATCH (a:__AgentSession {agent_id: $aid}) \
                           MATCH (c) WHERE id(c) = $cid \
                           INSERT (a)-[:published]->(c)";

        let st = Arc::clone(&tools.state);
        let auth2 = auth.clone();
        let _ = tools
            .submit_mut(move || {
                ops::gql::execute_gql(
                    &st,
                    &auth2,
                    link_query,
                    Some(&link_params),
                    false,
                    false,
                    ResultFormat::Json,
                )
            })
            .await;
    }

    Ok(CallToolResult::success(vec![Content::text(format!(
        "Context shared: {data}"
    ))]))
}

pub(super) async fn get_shared_context_impl(
    tools: &SeleneTools,
    p: GetSharedContextParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;
    let limit = p.limit.unwrap_or(50).min(500);
    let include_expired = p.include_expired.unwrap_or(false);

    let mut params = HashMap::new();
    let mut filters = Vec::new();

    if let Some(ref scope) = p.scope {
        params.insert("scope".into(), Value::from(scope.as_str()));
        filters.push("c.scope = $scope");
    }
    if let Some(ref ctype) = p.context_type {
        params.insert("ctype".into(), Value::from(ctype.as_str()));
        filters.push("c.context_type = $ctype");
    }
    if let Some(since) = p.since_ms {
        params.insert("since".into(), Value::Int(since));
        filters.push("c.created_at > $since");
    }
    if !include_expired {
        params.insert("now".into(), Value::Int(now_ms));
        filters.push("(c.expires_at = 0 OR c.expires_at > $now)");
    }

    // Enforce visibility: only return project/global context by default.
    // Directed visibility (agent:<id>) is private and not exposed here.
    filters.push("(c.visibility = 'project' OR c.visibility = 'global' OR c.visibility IS NULL)");

    params.insert("lim".into(), Value::Int(limit as i64));

    let filter_clause = if filters.is_empty() {
        String::new()
    } else {
        format!(" FILTER {}", filters.join(" AND "))
    };

    let query = format!(
        "MATCH (c:__SharedContext){filter_clause} \
         RETURN id(c) AS id, c.author AS author, c.context_type AS context_type, \
         c.scope AS scope, c.targets AS targets, c.content AS content, \
         c.visibility AS visibility, c.created_at AS created_at, \
         c.expires_at AS expires_at \
         ORDER BY c.created_at DESC \
         LIMIT $lim"
    );

    let gql_params = if params.is_empty() {
        None
    } else {
        Some(&params)
    };

    let result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        &query,
        gql_params,
        false,
        false,
        ResultFormat::Json,
    )
    .map_err(op_err)?;

    let data = result.data_json.unwrap_or_else(|| "[]".into());

    // If target_prefix filter requested, apply post-query (JSON array stored as string)
    let final_data = if let Some(ref prefix) = p.target_prefix {
        let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
        let filtered: Vec<&serde_json::Value> = rows
            .iter()
            .filter(|row| {
                row.get("targets")
                    .and_then(|t| t.as_str())
                    .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                    .is_some_and(|targets| targets.iter().any(|t| t.starts_with(prefix.as_str())))
            })
            .collect();
        serde_json::to_string_pretty(&filtered).unwrap_or_else(|_| "[]".into())
    } else {
        data
    };

    let mut text = format!("Shared context ({} found):\n", result.row_count);
    let _ = write!(text, "{final_data}");
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

// ── Intents & Conflict Detection ────────────────────────────────────

pub(super) async fn claim_intent_impl(
    tools: &SeleneTools,
    p: ClaimIntentParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    // Validate level
    let level = p.level.as_str();
    if !matches!(level, "advisory" | "exclusive" | "locked") {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: "level must be one of: advisory, exclusive, locked".into(),
            data: None,
        });
    }

    let now_ms = selene_core::now_nanos() / 1_000_000;
    let targets_str = serde_json::to_string(&p.targets).unwrap_or_else(|_| "[]".into());

    // Check for existing conflicts on exclusive/locked targets
    let mut conflict_text = String::new();
    if level != "advisory" {
        let mut check_params = HashMap::new();
        check_params.insert("aid".into(), Value::from(p.agent_id.as_str()));

        let check_query = "MATCH (i:__Intent) \
                            FILTER i.status = 'claimed' AND i.agent_id <> $aid \
                            AND (i.level = 'exclusive' OR i.level = 'locked') \
                            RETURN i.agent_id AS agent_id, i.action AS action, \
                            i.targets AS targets, i.level AS level";

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

        let rows: Vec<serde_json::Value> =
            serde_json::from_str(&check_result.data_json.unwrap_or_else(|| "[]".into()))
                .unwrap_or_default();

        // Check for path overlap — only reject on overlapping locked intents
        let mut has_locked_overlap = false;
        for row in &rows {
            let their_targets: Vec<String> = row
                .get("targets")
                .and_then(|t| t.as_str())
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();

            let their_level = row
                .get("level")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            for my_target in &p.targets {
                for their_target in &their_targets {
                    if my_target.starts_with(their_target.as_str())
                        || their_target.starts_with(my_target.as_str())
                    {
                        let agent = row
                            .get("agent_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        let action = row
                            .get("action")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        let _ = writeln!(
                            conflict_text,
                            "CONFLICT: agent '{agent}' has {their_level} claim on \
                             '{their_target}' (action: {action})"
                        );
                        if their_level == "locked" {
                            has_locked_overlap = true;
                        }
                    }
                }
            }
        }

        // If an overlapping intent is locked, reject with an error
        if has_locked_overlap {
            return Err(McpError {
                code: ErrorCode::INVALID_REQUEST,
                message: format!("Cannot claim — locked by another agent:\n{conflict_text}").into(),
                data: None,
            });
        }
    }

    // Create the intent
    let mut params = HashMap::new();
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));
    params.insert("action".into(), Value::from(p.action.as_str()));
    params.insert("targets".into(), Value::from(targets_str.as_str()));
    params.insert("level".into(), Value::from(level));
    params.insert("status".into(), Value::from("claimed"));
    params.insert("now".into(), Value::Int(now_ms));
    params.insert(
        "reason".into(),
        p.reason.as_deref().map_or(Value::Null, Value::from),
    );

    let query = "INSERT (i:__Intent { \
                  agent_id: $aid, \
                  action: $action, \
                  targets: $targets, \
                  level: $level, \
                  status: $status, \
                  claimed_at: $now, \
                  reason: $reason \
                  }) RETURN id(i) AS id";

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

    // Link intent to agent session
    let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
    if let Some(intent_id) = rows
        .first()
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_u64())
    {
        let mut link_params = HashMap::new();
        link_params.insert("aid".into(), Value::from(p.agent_id.as_str()));
        link_params.insert("iid".into(), Value::Int(intent_id as i64));

        let link_query = "MATCH (a:__AgentSession {agent_id: $aid}) \
                           MATCH (i) WHERE id(i) = $iid \
                           INSERT (a)-[:claims]->(i)";

        let st = Arc::clone(&tools.state);
        let auth2 = auth.clone();
        let _ = tools
            .submit_mut(move || {
                ops::gql::execute_gql(
                    &st,
                    &auth2,
                    link_query,
                    Some(&link_params),
                    false,
                    false,
                    ResultFormat::Json,
                )
            })
            .await;
    }

    let mut text = format!("Intent claimed ({level}): {data}");
    if !conflict_text.is_empty() {
        let _ = write!(
            text,
            "\n\nWarning — overlapping claims detected:\n{conflict_text}"
        );
    }
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

pub(super) async fn release_intent_impl(
    tools: &SeleneTools,
    p: ReleaseIntentParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let mut params = HashMap::new();
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));

    let query = if let Some(intent_id) = p.intent_id {
        params.insert("iid".into(), Value::Int(intent_id as i64));
        "MATCH (i:__Intent {agent_id: $aid}) \
         FILTER id(i) = $iid AND i.status = 'claimed' \
         SET i.status = 'released' \
         RETURN id(i) AS id"
    } else {
        "MATCH (i:__Intent {agent_id: $aid}) \
         FILTER i.status = 'claimed' \
         SET i.status = 'released' \
         RETURN count(i) AS released"
    };

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
        "Intents released: {data}"
    ))]))
}

pub(super) async fn check_conflicts_impl(
    tools: &SeleneTools,
    p: CheckConflictsParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    let mut params = HashMap::new();
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));

    let query = "MATCH (i:__Intent) \
                  FILTER i.status = 'claimed' AND i.agent_id <> $aid \
                  AND (i.level = 'exclusive' OR i.level = 'locked') \
                  RETURN i.agent_id AS agent_id, i.action AS action, \
                  i.targets AS targets, i.level AS level, i.claimed_at AS claimed_at";

    let result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        query,
        Some(&params),
        false,
        false,
        ResultFormat::Json,
    )
    .map_err(op_err)?;

    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&result.data_json.unwrap_or_else(|| "[]".into())).unwrap_or_default();

    // Filter to only intents with overlapping targets
    let mut conflicts = Vec::new();
    for row in &rows {
        let their_targets: Vec<String> = row
            .get("targets")
            .and_then(|t| t.as_str())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let overlaps: bool = p.targets.iter().any(|my_t| {
            their_targets.iter().any(|their_t| {
                my_t.starts_with(their_t.as_str()) || their_t.starts_with(my_t.as_str())
            })
        });

        if overlaps {
            conflicts.push(row);
        }
    }

    if conflicts.is_empty() {
        Ok(CallToolResult::success(vec![Content::text(
            "No conflicts found — targets are clear.",
        )]))
    } else {
        let conflicts_json =
            serde_json::to_string_pretty(&conflicts).unwrap_or_else(|_| "[]".into());
        Ok(CallToolResult::success(vec![Content::text(format!(
            "{} conflict(s) found:\n{conflicts_json}",
            conflicts.len()
        ))]))
    }
}

// ── Agent Capability Discovery ────────────────────────────────────

pub(super) async fn find_capable_agent_impl(
    tools: &SeleneTools,
    p: FindCapableAgentParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    if p.required_tools.is_none() && p.required_expertise.is_none() && p.query.is_none() {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: "At least one of required_tools, required_expertise, or query \
                      must be provided"
                .into(),
            data: None,
        });
    }

    let now_ms = selene_core::now_nanos() / 1_000_000;
    let active_within = p.active_within_ms.unwrap_or(300_000);

    let mut params = HashMap::new();
    params.insert("cutoff".into(), Value::Int(now_ms - active_within));

    let mut filters = vec![
        "a.status = 'active'".to_string(),
        "a.heartbeat_at > $cutoff".to_string(),
    ];

    if let Some(ref project) = p.project {
        params.insert("project".into(), Value::from(project.as_str()));
        filters.push("a.project = $project".to_string());
    }

    let filter_clause = format!(" FILTER {}", filters.join(" AND "));

    let query = format!(
        "MATCH (a:__AgentSession){filter_clause} \
         RETURN a.agent_id AS agent_id, a.project AS project, \
         a.working_on AS working_on, \
         a.capabilities AS capabilities, \
         a.supported_tools AS supported_tools, \
         a.domain_expertise AS domain_expertise, \
         a.model_family AS model_family, \
         a.context_window AS context_window, \
         a.heartbeat_at AS heartbeat_at"
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

    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&result.data_json.unwrap_or_else(|| "[]".into())).unwrap_or_default();

    // Score each agent by capability match.
    let mut scored: Vec<(f64, serde_json::Value)> = rows
        .into_iter()
        .map(|row| {
            let mut score = 0.0_f64;

            // Structured tool match (up to 50 points)
            if let Some(ref required) = p.required_tools {
                let their_tools: Vec<String> = row
                    .get("supported_tools")
                    .and_then(|t| t.as_str())
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();
                if !required.is_empty() {
                    let matched = required
                        .iter()
                        .filter(|r| their_tools.iter().any(|t| t.eq_ignore_ascii_case(r)))
                        .count();
                    score += (matched as f64 / required.len() as f64) * 50.0;
                }
            }

            // Structured expertise match (up to 30 points)
            if let Some(ref required) = p.required_expertise {
                let their_exp: Vec<String> = row
                    .get("domain_expertise")
                    .and_then(|t| t.as_str())
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();
                if !required.is_empty() {
                    let matched = required
                        .iter()
                        .filter(|r| their_exp.iter().any(|e| e.eq_ignore_ascii_case(r)))
                        .count();
                    score += (matched as f64 / required.len() as f64) * 30.0;
                }
            }

            // Free-text substring match on capabilities (up to 20 points)
            if let Some(ref query_text) = p.query
                && let Some(caps) = row.get("capabilities").and_then(|c| c.as_str())
            {
                let query_lower = query_text.to_lowercase();
                let caps_lower = caps.to_lowercase();
                if caps_lower.contains(&query_lower) {
                    score += 20.0;
                } else {
                    let words: Vec<&str> = query_lower.split_whitespace().collect();
                    if !words.is_empty() {
                        let matched = words.iter().filter(|w| caps_lower.contains(*w)).count();
                        score += (matched as f64 / words.len() as f64) * 15.0;
                    }
                }
            }

            (score, row)
        })
        .collect();

    // Keep only agents that matched something, sorted by score.
    scored.retain(|(s, _)| *s > 0.0);
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    if scored.is_empty() {
        return Ok(CallToolResult::success(vec![Content::text(
            "No capable agents found matching the criteria.",
        )]));
    }

    let results: Vec<serde_json::Value> = scored
        .into_iter()
        .map(|(score, mut row)| {
            row["match_score"] = serde_json::json!((score * 100.0).round() / 100.0);
            row
        })
        .collect();

    let text = format!(
        "{} capable agent(s) found:\n{}",
        results.len(),
        serde_json::to_string(&results).unwrap_or_else(|_| "[]".into())
    );
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

// ── Task Delegation ───────────────────────────────────────────────

const VALID_PRIORITIES: &[&str] = &["low", "medium", "high", "critical"];

pub(super) async fn propose_task_impl(
    tools: &SeleneTools,
    p: ProposeTaskParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    if !VALID_PRIORITIES.contains(&p.priority.as_str()) {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: format!(
                "Invalid priority '{}'. Must be one of: {}",
                p.priority,
                VALID_PRIORITIES.join(", ")
            )
            .into(),
            data: None,
        });
    }

    let now_ms = selene_core::now_nanos() / 1_000_000;

    let mut params = HashMap::new();
    params.insert("title".into(), Value::from(p.title.as_str()));
    params.insert("description".into(), Value::from(p.description.as_str()));
    params.insert("proposer".into(), Value::from(p.proposer_agent.as_str()));
    params.insert(
        "assignee".into(),
        p.assignee_agent.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert("project".into(), Value::from(p.project.as_str()));
    params.insert("priority".into(), Value::from(p.priority.as_str()));
    let tools_str = p
        .required_tools
        .as_ref()
        .map(|t| serde_json::to_string(t).unwrap_or_else(|_| "[]".into()));
    params.insert(
        "required_tools".into(),
        tools_str.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert(
        "input_data".into(),
        p.input_data.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert("now".into(), Value::Int(now_ms));

    // Create the task node.
    let insert_query = "INSERT (t:__Task { \
                             title: $title, \
                             description: $description, \
                             status: 'proposed', \
                             proposer_agent: $proposer, \
                             assignee_agent: $assignee, \
                             project: $project, \
                             priority: $priority, \
                             required_tools: $required_tools, \
                             input_data: $input_data, \
                             created_at: $now, \
                             updated_at: $now \
                         }) RETURN id(t) AS id";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let params2 = params.clone();
    let result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                insert_query,
                Some(&params2),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await?;

    let data = result.data_json.unwrap_or_else(|| "{}".into());
    let task_id: Option<u64> = serde_json::from_str::<Vec<serde_json::Value>>(&data)
        .ok()
        .and_then(|rows| rows.first().cloned())
        .and_then(|row| row.get("id").and_then(|v| v.as_u64()));

    // Create :proposed edge from proposer's __AgentSession.
    if let Some(tid) = task_id {
        let edge_params = HashMap::from([
            ("proposer".into(), Value::from(p.proposer_agent.as_str())),
            ("tid".into(), Value::Int(tid as i64)),
        ]);
        let edge_query = "MATCH (a:__AgentSession {agent_id: $proposer}) \
                           MATCH (t:__Task) WHERE id(t) = $tid \
                           INSERT (a)-[:proposed]->(t)";

        let st = Arc::clone(&tools.state);
        let auth2 = auth.clone();
        let _ = tools
            .submit_mut(move || {
                ops::gql::execute_gql(
                    &st,
                    &auth2,
                    edge_query,
                    Some(&edge_params),
                    false,
                    false,
                    ResultFormat::Json,
                )
            })
            .await;
    }

    let mut text = format!("Task proposed: {data}");
    if p.assignee_agent.is_some() && task_id.is_some() {
        let _ = write!(
            text,
            " (targeted at agent '{}')",
            p.assignee_agent.as_deref().unwrap_or("?")
        );
    }
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

pub(super) async fn accept_task_impl(
    tools: &SeleneTools,
    p: AcceptTaskParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;

    let mut params = HashMap::new();
    params.insert("tid".into(), Value::Int(p.task_id as i64));
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));
    params.insert("now".into(), Value::Int(now_ms));

    let query = "MATCH (t:__Task) WHERE id(t) = $tid \
                  FILTER t.status = 'proposed' \
                  SET t.status = 'accepted', t.assignee_agent = $aid, \
                      t.updated_at = $now \
                  RETURN id(t) AS id, t.title AS title";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let params2 = params.clone();
    let result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                query,
                Some(&params2),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await?;

    if result.row_count == 0 {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: format!("Task {} not found or not in 'proposed' status", p.task_id).into(),
            data: None,
        });
    }

    // Create :assigned edge.
    let edge_query = "MATCH (a:__AgentSession {agent_id: $aid}) \
                       MATCH (t:__Task) WHERE id(t) = $tid \
                       INSERT (a)-[:assigned]->(t)";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let _ = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                edge_query,
                Some(&params),
                false,
                false,
                ResultFormat::Json,
            )
        })
        .await;

    let data = result.data_json.unwrap_or_else(|| "{}".into());
    Ok(CallToolResult::success(vec![Content::text(format!(
        "Task accepted by '{}': {data}",
        p.agent_id
    ))]))
}

pub(super) async fn reject_task_impl(
    tools: &SeleneTools,
    p: RejectTaskParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let now_ms = selene_core::now_nanos() / 1_000_000;

    let mut params = HashMap::new();
    params.insert("tid".into(), Value::Int(p.task_id as i64));
    params.insert(
        "reason".into(),
        p.reason.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert("now".into(), Value::Int(now_ms));

    let query = "MATCH (t:__Task) WHERE id(t) = $tid \
                  FILTER t.status = 'proposed' \
                  SET t.status = 'rejected', t.failure_reason = $reason, \
                      t.updated_at = $now \
                  RETURN id(t) AS id";

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

    if result.row_count == 0 {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: format!("Task {} not found or not in 'proposed' status", p.task_id).into(),
            data: None,
        });
    }

    Ok(CallToolResult::success(vec![Content::text(format!(
        "Task {} rejected by '{}'",
        p.task_id, p.agent_id
    ))]))
}

pub(super) async fn complete_task_impl(
    tools: &SeleneTools,
    p: CompleteTaskParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    if !p.success && p.failure_reason.as_ref().is_none_or(|r| r.is_empty()) {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: "failure_reason is required when success is false".into(),
            data: None,
        });
    }

    let now_ms = selene_core::now_nanos() / 1_000_000;
    let target_status = if p.success { "completed" } else { "failed" };

    let mut params = HashMap::new();
    params.insert("tid".into(), Value::Int(p.task_id as i64));
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));
    params.insert("status".into(), Value::from(target_status));
    params.insert(
        "output".into(),
        p.output_data.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert(
        "reason".into(),
        p.failure_reason.as_deref().map_or(Value::Null, Value::from),
    );
    params.insert("now".into(), Value::Int(now_ms));

    // Only the assignee can complete, and the task must be in a completable state.
    let query = "MATCH (t:__Task) WHERE id(t) = $tid \
                  FILTER t.assignee_agent = $aid \
                      AND (t.status = 'accepted' OR t.status = 'working' \
                           OR t.status = 'input_required') \
                  SET t.status = $status, t.output_data = $output, \
                      t.failure_reason = $reason, \
                      t.updated_at = $now, t.completed_at = $now \
                  RETURN id(t) AS id, t.title AS title";

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

    if result.row_count == 0 {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: format!(
                "Task {} not found, not assigned to '{}', or not in a completable status \
                 (accepted/working/input_required)",
                p.task_id, p.agent_id
            )
            .into(),
            data: None,
        });
    }

    let data = result.data_json.unwrap_or_else(|| "{}".into());
    Ok(CallToolResult::success(vec![Content::text(format!(
        "Task {target_status}: {data}"
    ))]))
}

pub(super) async fn list_tasks_impl(
    tools: &SeleneTools,
    p: ListTasksParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;

    let mut params = HashMap::new();
    let mut filters = Vec::new();

    if let Some(ref project) = p.project {
        params.insert("project".into(), Value::from(project.as_str()));
        filters.push("t.project = $project");
    }
    if let Some(ref status) = p.status {
        params.insert("status".into(), Value::from(status.as_str()));
        filters.push("t.status = $status");
    }
    if let Some(ref proposer) = p.proposer_agent {
        params.insert("proposer".into(), Value::from(proposer.as_str()));
        filters.push("t.proposer_agent = $proposer");
    }
    if let Some(ref assignee) = p.assignee_agent {
        params.insert("assignee".into(), Value::from(assignee.as_str()));
        filters.push("t.assignee_agent = $assignee");
    }

    let limit = p.limit.unwrap_or(50).min(500);
    params.insert("lim".into(), Value::Int(limit as i64));

    let filter_clause = if filters.is_empty() {
        String::new()
    } else {
        format!(" FILTER {}", filters.join(" AND "))
    };

    let query = format!(
        "MATCH (t:__Task){filter_clause} \
         RETURN id(t) AS id, t.title AS title, t.status AS status, \
         t.proposer_agent AS proposer, t.assignee_agent AS assignee, \
         t.project AS project, t.priority AS priority, \
         t.created_at AS created_at, t.updated_at AS updated_at \
         ORDER BY t.updated_at DESC \
         LIMIT $lim"
    );

    let gql_params = if params.is_empty() {
        None
    } else {
        Some(&params)
    };

    let result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        &query,
        gql_params,
        false,
        false,
        ResultFormat::Json,
    )
    .map_err(op_err)?;

    let data = result.data_json.unwrap_or_else(|| "[]".to_string());
    let text = format!("{} task(s):\n{data}", result.row_count);
    Ok(CallToolResult::success(vec![Content::text(text)]))
}
