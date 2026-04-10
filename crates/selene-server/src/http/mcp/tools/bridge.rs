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

    // Upsert: if agent_id already exists, update it; otherwise create.
    let mut params = HashMap::new();
    params.insert("aid".into(), Value::from(p.agent_id.as_str()));
    params.insert("project".into(), Value::from(p.project.as_str()));
    params.insert("status".into(), Value::from("active"));
    params.insert("now".into(), Value::Int(now_ms));
    params.insert(
        "working_on".into(),
        p.working_on
            .as_deref()
            .map_or(Value::Null, Value::from),
    );
    params.insert(
        "capabilities".into(),
        p.capabilities
            .as_deref()
            .map_or(Value::Null, Value::from),
    );

    let files_str = p
        .files_touched
        .as_ref()
        .map(|f| serde_json::to_string(f).unwrap_or_else(|_| "[]".into()));
    params.insert(
        "files".into(),
        files_str
            .as_deref()
            .map_or(Value::Null, Value::from),
    );

    // Check if session already exists
    let check_query = "MATCH (a:__AgentSession {agent_id: $aid}) \
                        RETURN id(a) AS id";
    let check_result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        check_query,
        Some(&params),
        false,
        false,
        ResultFormat::Json,
    )
    .map_err(op_err)?;

    let existing: Vec<serde_json::Value> = serde_json::from_str(
        &check_result.data_json.unwrap_or_else(|| "[]".into()),
    )
    .unwrap_or_default();

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();

    if existing.is_empty() {
        // Create new session
        let query = "INSERT (a:__AgentSession { \
                      agent_id: $aid, \
                      project: $project, \
                      status: $status, \
                      working_on: $working_on, \
                      files_touched: $files, \
                      capabilities: $capabilities, \
                      heartbeat_at: $now, \
                      started_at: $now \
                      }) RETURN id(a) AS id, 'created' AS action";

        let result = tools
            .submit_mut(move || {
                ops::gql::execute_gql(
                    &st, &auth2, query, Some(&params), false, false, ResultFormat::Json,
                )
            })
            .await?;

        let data = result.data_json.unwrap_or_else(|| "{}".into());
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Agent registered: {data}"
        ))]))
    } else {
        // Update existing session
        let query = "MATCH (a:__AgentSession {agent_id: $aid}) \
                      SET a.status = $status, \
                          a.project = $project, \
                          a.heartbeat_at = $now, \
                          a.working_on = $working_on, \
                          a.files_touched = $files, \
                          a.capabilities = $capabilities \
                      RETURN id(a) AS id, 'updated' AS action";

        let result = tools
            .submit_mut(move || {
                ops::gql::execute_gql(
                    &st, &auth2, query, Some(&params), false, false, ResultFormat::Json,
                )
            })
            .await?;

        let data = result.data_json.unwrap_or_else(|| "{}".into());
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Agent session updated: {data}"
        ))]))
    }
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
                &st, &auth2, &query, Some(&params), false, false, ResultFormat::Json,
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

    let data = result.data_json.unwrap_or_else(|| "{}".into());
    Ok(CallToolResult::success(vec![Content::text(format!(
        "Agent '{}' deregistered. Intents released: {data}",
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
        targets_str
            .as_deref()
            .map_or(Value::Null, Value::from),
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
                &st, &auth2, query, Some(&params), false, false, ResultFormat::Json,
            )
        })
        .await?;

    let data = result.data_json.unwrap_or_else(|| "{}".into());

    // Create "about" edges to referenced nodes if provided
    if let Some(ref node_ids) = about_ids
        && !node_ids.is_empty()
    {
        // Parse the created context node ID from the result
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(&data).unwrap_or_default();
        if let Some(ctx_id) = rows.first().and_then(|r| r.get("id")).and_then(|v| v.as_u64())
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
    if let Some(ctx_id) = rows.first().and_then(|r| r.get("id")).and_then(|v| v.as_u64()) {
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
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(&data).unwrap_or_default();
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

        let rows: Vec<serde_json::Value> = serde_json::from_str(
            &check_result.data_json.unwrap_or_else(|| "[]".into()),
        )
        .unwrap_or_default();

        // Check for path overlap
        for row in &rows {
            let their_targets: Vec<String> = row
                .get("targets")
                .and_then(|t| t.as_str())
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();

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
                        let their_level = row
                            .get("level")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        let _ = writeln!(
                            conflict_text,
                            "CONFLICT: agent '{agent}' has {their_level} claim on \
                             '{their_target}' (action: {action})"
                        );
                    }
                }
            }
        }

        // If target is locked by another agent, reject
        if !conflict_text.is_empty()
            && rows.iter().any(|r| {
                r.get("level")
                    .and_then(|v| v.as_str())
                    .is_some_and(|l| l == "locked")
            })
        {
            return Ok(CallToolResult::success(vec![Content::text(format!(
                "Cannot claim — locked by another agent:\n{conflict_text}"
            ))]));
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
        p.reason
            .as_deref()
            .map_or(Value::Null, Value::from),
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
                &st, &auth2, query, Some(&params), false, false, ResultFormat::Json,
            )
        })
        .await?;

    let data = result.data_json.unwrap_or_else(|| "{}".into());

    // Link intent to agent session
    let rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
    if let Some(intent_id) = rows.first().and_then(|r| r.get("id")).and_then(|v| v.as_u64()) {
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
        let _ = write!(text, "\n\nWarning — overlapping claims detected:\n{conflict_text}");
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
                &st, &auth2, query, Some(&params), false, false, ResultFormat::Json,
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

    let rows: Vec<serde_json::Value> = serde_json::from_str(
        &result.data_json.unwrap_or_else(|| "[]".into()),
    )
    .unwrap_or_default();

    // Filter to only intents with overlapping targets
    let mut conflicts = Vec::new();
    for row in &rows {
        let their_targets: Vec<String> = row
            .get("targets")
            .and_then(|t| t.as_str())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        let overlaps: bool = p.targets.iter().any(|my_t| {
            their_targets
                .iter()
                .any(|their_t| my_t.starts_with(their_t.as_str()) || their_t.starts_with(my_t.as_str()))
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
