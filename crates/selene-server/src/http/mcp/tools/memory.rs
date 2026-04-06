//! Agent memory tool implementations: remember, recall, forget, configure_memory.
//!
//! Includes eviction helpers (clock algorithm, oldest-first, lowest-confidence)
//! and their unit tests.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, Content, ErrorCode};

use crate::http::mcp::params::*;
use crate::http::mcp::{SeleneTools, mcp_auth, op_err, reject_replica};
use crate::ops;
use selene_core::Value;

pub(super) async fn remember_impl(
    tools: &SeleneTools,
    p: RememberParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;
    let namespace = p.namespace;
    let content = p.content;
    let memory_type = p.memory_type;
    let entities = p.entities.unwrap_or_default();

    // 1. Read __MemoryConfig for namespace (defaults if absent)
    let (max_memories, default_ttl_ms, eviction_policy) = {
        let mut config_params = HashMap::new();
        config_params.insert("ns".into(), Value::from(namespace.as_str()));
        let config_query = "MATCH (c:__MemoryConfig {namespace: $ns}) \
                            RETURN c.max_memories AS max_memories, \
                            c.default_ttl_ms AS default_ttl_ms, \
                            c.eviction_policy AS eviction_policy";
        let config_result = ops::gql::execute_gql(
            &tools.state,
            &auth,
            config_query,
            Some(&config_params),
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;

        let config_str = config_result.data_json.unwrap_or_else(|| "[]".to_string());
        let config_rows: Vec<serde_json::Value> =
            serde_json::from_str(&config_str).unwrap_or_default();
        if let Some(row) = config_rows.first() {
            let max = row
                .get("max_memories")
                .and_then(|v| v.as_i64())
                .unwrap_or(1000);
            let ttl = row
                .get("default_ttl_ms")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let policy = row
                .get("eviction_policy")
                .and_then(|v| v.as_str())
                .unwrap_or("clock")
                .to_string();
            (max, ttl, policy)
        } else {
            (1000i64, 0i64, "clock".to_string())
        }
    };

    // 2. Count __Memory nodes in namespace
    let count = {
        let mut count_params = HashMap::new();
        count_params.insert("ns".into(), Value::from(namespace.as_str()));
        let count_query = "MATCH (m:__Memory {namespace: $ns}) RETURN count(m) AS cnt";
        let count_result = ops::gql::execute_gql(
            &tools.state,
            &auth,
            count_query,
            Some(&count_params),
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;
        let count_str = count_result.data_json.unwrap_or_else(|| "[]".to_string());
        let count_rows: Vec<serde_json::Value> =
            serde_json::from_str(&count_str).unwrap_or_default();
        count_rows
            .first()
            .and_then(|r| r.get("cnt"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    };

    // 3. Evict if at capacity (max_memories > 0 means bounded)
    if max_memories > 0 && count >= max_memories {
        // Get all __Memory nodes in namespace with created_at
        let mut mem_params = HashMap::new();
        mem_params.insert("ns".into(), Value::from(namespace.as_str()));
        let mem_query = "MATCH (m:__Memory {namespace: $ns}) \
                         RETURN id(m) AS nodeId, m.created_at AS created_at, \
                         m.valid_until AS valid_until, m.confidence AS confidence \
                         ORDER BY m.created_at ASC";
        let mem_result = ops::gql::execute_gql(
            &tools.state,
            &auth,
            mem_query,
            Some(&mem_params),
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;
        let mem_str = mem_result.data_json.unwrap_or_else(|| "[]".to_string());
        let mem_rows: Vec<serde_json::Value> = serde_json::from_str(&mem_str).unwrap_or_default();

        let now_ms_evict = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let memories: Vec<(u64, i64, i64, f64)> = mem_rows
            .iter()
            .filter_map(|r| {
                let nid = r.get("nodeId")?.as_u64()?;
                let ca = r.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0);
                let vu = r.get("valid_until").and_then(|v| v.as_i64()).unwrap_or(0);
                let conf = r.get("confidence").and_then(|v| v.as_f64()).unwrap_or(1.0);
                Some((nid, ca, vu, conf))
            })
            .collect();

        if let Some(evict_id) = {
            // Prefer evicting expired memories before running the policy sweep
            let expired_candidate = memories
                .iter()
                .filter(|&&(_, _, vu, _)| vu > 0 && vu < now_ms_evict)
                .min_by_key(|&&(_, ca, _, _)| ca)
                .map(|&(nid, _, _, _)| nid);

            if let Some(eid) = expired_candidate {
                Some(eid)
            } else {
                let clock_mems: Vec<(u64, i64)> =
                    memories.iter().map(|&(nid, ca, _, _)| (nid, ca)).collect();
                match eviction_policy.as_str() {
                    "oldest" => find_oldest_candidate(&clock_mems),
                    "lowest_confidence" => {
                        let conf_mems: Vec<(u64, i64, f64)> = memories
                            .iter()
                            .map(|&(nid, ca, _, conf)| (nid, ca, conf))
                            .collect();
                        find_lowest_confidence_candidate(&conf_mems)
                    }
                    _ => {
                        let mut counters = tools.state.clock_counters.write();
                        let ns_counters = counters.entry(namespace.clone()).or_default();
                        find_eviction_candidate(&clock_mems, ns_counters)
                    }
                }
            }
        } {
            // Delete the eviction candidate
            let mut del_params = HashMap::new();
            del_params.insert("evict_id".into(), Value::UInt(evict_id));
            del_params.insert("ns".into(), Value::from(namespace.as_str()));
            let del_query = "MATCH (m:__Memory {namespace: $ns}) \
                             FILTER id(m) = $evict_id \
                             DETACH DELETE m";
            let st = Arc::clone(&tools.state);
            let auth2 = auth.clone();
            tools
                .submit_mut(move || {
                    ops::gql::execute_gql(
                        &st,
                        &auth2,
                        del_query,
                        Some(&del_params),
                        false,
                        false,
                        ops::gql::ResultFormat::Json,
                    )
                })
                .await?;

            // Remove evicted node from counters; drop empty namespace entry
            let mut counters = tools.state.clock_counters.write();
            if let Some(ns_counters) = counters.get_mut(&namespace) {
                ns_counters.remove(&evict_id);
                if ns_counters.is_empty() {
                    counters.remove(&namespace);
                }
            }
        }
    }

    // 4. Compute valid_until
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let valid_until = if let Some(vu) = p.valid_until {
        vu
    } else if default_ttl_ms > 0 {
        now_ms + default_ttl_ms
    } else {
        0
    };

    // 5. INSERT __Memory node with embed($content) for embedding
    let mut insert_params = HashMap::new();
    insert_params.insert("ns".into(), Value::from(namespace.as_str()));
    insert_params.insert("content".into(), Value::from(content.as_str()));
    insert_params.insert("mtype".into(), Value::from(memory_type.as_str()));
    insert_params.insert("vfrom".into(), Value::Int(now_ms));
    insert_params.insert("vuntil".into(), Value::Int(valid_until));
    insert_params.insert("conf".into(), Value::Float(1.0));
    insert_params.insert("cat".into(), Value::Int(now_ms));

    let insert_query = "INSERT (m:__Memory { \
                        namespace: $ns, \
                        content: $content, \
                        embedding: embed($content), \
                        memory_type: $mtype, \
                        valid_from: $vfrom, \
                        valid_until: $vuntil, \
                        confidence: $conf, \
                        created_at: $cat \
                        }) \
                        RETURN id(m) AS nodeId";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    let insert_result = tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                insert_query,
                Some(&insert_params),
                false,
                false,
                ops::gql::ResultFormat::Json,
            )
        })
        .await?;

    let result_str = insert_result.data_json.unwrap_or_else(|| "[]".to_string());
    let result_rows: Vec<serde_json::Value> = serde_json::from_str(&result_str).unwrap_or_default();
    let node_id = result_rows
        .first()
        .and_then(|r| r.get("nodeId"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            op_err(ops::OpError::Internal(
                "failed to get node ID from INSERT result".into(),
            ))
        })?;

    // 6. If entities provided: MERGE __Entity nodes and create __MENTIONS edges
    if !entities.is_empty() {
        for entity_name in &entities {
            let mut entity_params = HashMap::new();
            entity_params.insert("ns".into(), Value::from(namespace.as_str()));
            entity_params.insert("ename".into(), Value::from(entity_name.as_str()));
            entity_params.insert("mid".into(), Value::UInt(node_id));

            let entity_query = "MERGE (e:__Entity {namespace: $ns, name: $ename}) \
                 SET e.entity_type = 'auto'";

            let st = Arc::clone(&tools.state);
            let auth2 = auth.clone();
            let ep = entity_params.clone();
            tools
                .submit_mut(move || {
                    ops::gql::execute_gql(
                        &st,
                        &auth2,
                        entity_query,
                        Some(&ep),
                        false,
                        false,
                        ops::gql::ResultFormat::Json,
                    )
                })
                .await?;

            // Create __MENTIONS edge from memory to entity
            let edge_query = "MATCH (m:__Memory) FILTER id(m) = $mid \
                 MATCH (e:__Entity {namespace: $ns, name: $ename}) \
                 INSERT (m)-[:__MENTIONS]->(e)";

            let st = Arc::clone(&tools.state);
            let auth2 = auth.clone();
            tools
                .submit_mut(move || {
                    ops::gql::execute_gql(
                        &st,
                        &auth2,
                        edge_query,
                        Some(&entity_params),
                        false,
                        false,
                        ops::gql::ResultFormat::Json,
                    )
                })
                .await?;
        }
    }

    let mut text = format!("Stored memory (node {node_id}) in namespace '{namespace}'");
    if !entities.is_empty() {
        let _ = write!(text, " with {} entity links", entities.len());
    }
    if valid_until > 0 {
        let _ = write!(text, ", expires at {valid_until}");
    }
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

pub(super) async fn recall_impl(
    tools: &SeleneTools,
    p: RecallParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let namespace = p.namespace;
    let k = p.k.unwrap_or(10);

    if k <= 0 {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: "k must be a positive integer".into(),
            data: None,
        });
    }

    // Call memory.recall procedure via GQL
    let query = "CALL memory.recall($ns, $queryText, $k) \
                 YIELD nodeId, content, memoryType, score, confidence, createdAt \
                 RETURN nodeId, content, memoryType, score, confidence, createdAt";

    let mut gql_params = HashMap::new();
    gql_params.insert("ns".into(), Value::from(namespace.as_str()));
    gql_params.insert("queryText".into(), Value::from(p.query.as_str()));
    gql_params.insert("k".into(), Value::Int(k));

    let result = ops::gql::execute_gql(
        &tools.state,
        &auth,
        query,
        Some(&gql_params),
        false,
        false,
        ops::gql::ResultFormat::Json,
    )
    .map_err(op_err)?;

    // Parse result to get node IDs for clock counter updates
    let data_str = result.data_json.unwrap_or_else(|| "[]".to_string());
    let rows: Vec<serde_json::Value> = serde_json::from_str(&data_str).unwrap_or_default();

    // Increment clock counters for returned nodes
    let result_node_ids: Vec<u64> = rows
        .iter()
        .filter_map(|r| r.get("nodeId")?.as_u64())
        .collect();

    if !result_node_ids.is_empty() {
        let mut counters = tools.state.clock_counters.write();
        let ns_counters = counters.entry(namespace.clone()).or_default();
        for node_id in &result_node_ids {
            let counter = ns_counters.entry(*node_id).or_insert(0);
            *counter = (*counter + 1).min(3); // cap at 3
        }
    }

    let text = format!(
        "Recalled {} memories from namespace '{namespace}'\n{data_str}",
        rows.len()
    );
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

pub(super) async fn forget_impl(
    tools: &SeleneTools,
    p: ForgetParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;
    let namespace = p.namespace;

    if p.node_id.is_none() && p.query.is_none() {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: "forget requires either node_id or query".into(),
            data: None,
        });
    }

    if let Some(node_id) = p.node_id {
        // Delete specific node by ID (with namespace check)
        let mut del_params = HashMap::new();
        del_params.insert("nid".into(), Value::UInt(node_id));
        del_params.insert("ns".into(), Value::from(namespace.as_str()));
        let del_query = "MATCH (m:__Memory {namespace: $ns}) \
                         FILTER id(m) = $nid \
                         DETACH DELETE m";

        let st = Arc::clone(&tools.state);
        let auth2 = auth.clone();
        tools
            .submit_mut(move || {
                ops::gql::execute_gql(
                    &st,
                    &auth2,
                    del_query,
                    Some(&del_params),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                )
            })
            .await?;

        // Clean up clock counter
        let mut counters = tools.state.clock_counters.write();
        if let Some(ns_counters) = counters.get_mut(&namespace) {
            ns_counters.remove(&node_id);
        }

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Deleted memory node {node_id} from namespace '{namespace}'"
        ))]))
    } else if let Some(query_text) = p.query {
        // Match by content CONTAINS and delete
        let mut del_params = HashMap::new();
        del_params.insert("ns".into(), Value::from(namespace.as_str()));
        del_params.insert("q".into(), Value::from(query_text.as_str()));

        // First find matching nodes to get their IDs for counter cleanup
        let find_query = "MATCH (m:__Memory {namespace: $ns}) \
                          FILTER m.content CONTAINS $q \
                          RETURN id(m) AS nodeId";
        let find_result = ops::gql::execute_gql(
            &tools.state,
            &auth,
            find_query,
            Some(&del_params),
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;

        let find_str = find_result.data_json.unwrap_or_else(|| "[]".to_string());
        let find_rows: Vec<serde_json::Value> = serde_json::from_str(&find_str).unwrap_or_default();
        let deleted_ids: Vec<u64> = find_rows
            .iter()
            .filter_map(|r| r.get("nodeId")?.as_u64())
            .collect();

        // Delete matching nodes
        let del_query = "MATCH (m:__Memory {namespace: $ns}) \
                         FILTER m.content CONTAINS $q \
                         DETACH DELETE m";
        let st = Arc::clone(&tools.state);
        let auth2 = auth.clone();
        tools
            .submit_mut(move || {
                ops::gql::execute_gql(
                    &st,
                    &auth2,
                    del_query,
                    Some(&del_params),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                )
            })
            .await?;

        // Clean up clock counters
        if !deleted_ids.is_empty() {
            let mut counters = tools.state.clock_counters.write();
            if let Some(ns_counters) = counters.get_mut(&namespace) {
                for id in &deleted_ids {
                    ns_counters.remove(id);
                }
            }
        }

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Deleted {} memories matching '{}' from namespace '{namespace}'",
            deleted_ids.len(),
            query_text
        ))]))
    } else {
        unreachable!()
    }
}

pub(super) async fn configure_memory_impl(
    tools: &SeleneTools,
    p: ConfigureMemoryParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    if let Some(ref policy) = p.eviction_policy {
        const VALID_POLICIES: &[&str] = &["clock", "oldest", "lowest_confidence"];
        if !VALID_POLICIES.contains(&policy.as_str()) {
            return Err(McpError {
                code: ErrorCode::INVALID_PARAMS,
                message: format!(
                    "unknown eviction policy '{policy}'; valid options: clock, oldest, lowest_confidence"
                )
                .into(),
                data: None,
            });
        }
    }

    let namespace = p.namespace;

    let mut gql_params = HashMap::new();
    gql_params.insert("ns".into(), Value::from(namespace.as_str()));
    gql_params.insert("max".into(), p.max_memories.map_or(Value::Null, Value::Int));
    gql_params.insert(
        "ttl".into(),
        p.default_ttl_ms.map_or(Value::Null, Value::Int),
    );
    gql_params.insert(
        "policy".into(),
        p.eviction_policy
            .as_deref()
            .map_or(Value::Null, Value::from),
    );

    let query = "MERGE (c:__MemoryConfig {namespace: $ns}) \
                 SET c.max_memories = COALESCE($max, c.max_memories), \
                 c.default_ttl_ms = COALESCE($ttl, c.default_ttl_ms), \
                 c.eviction_policy = COALESCE($policy, c.eviction_policy)";

    let st = Arc::clone(&tools.state);
    let auth2 = auth.clone();
    tools
        .submit_mut(move || {
            ops::gql::execute_gql(
                &st,
                &auth2,
                query,
                Some(&gql_params),
                false,
                false,
                ops::gql::ResultFormat::Json,
            )
        })
        .await?;

    let text = format!("Configured memory for namespace '{namespace}'");
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

// ── Eviction helpers ─────────────────────────────────────────────────

/// Find the node to evict using the enhanced clock algorithm (2-bit counters).
///
/// The sweep iterates memories ordered by `created_at` (oldest first).
/// A node with counter 0 is evicted immediately. Nodes with counter > 0
/// have their counter decremented by 1. If no node reaches 0 after a
/// full sweep (safety net), the node with the lowest counter (tiebreak
/// by oldest `created_at`) is evicted.
///
/// Cold start (empty counters after restart): all counters default to 0,
/// so the oldest memory is evicted first.
fn find_eviction_candidate(
    memories: &[(u64, i64)], // (node_id, created_at)
    counters: &mut std::collections::HashMap<u64, u8>,
) -> Option<u64> {
    if memories.is_empty() {
        return None;
    }

    // Prune counter entries for nodes no longer in the memories list.
    // This prevents a slow leak when memories are deleted outside the forget tool.
    let live_ids: std::collections::HashSet<u64> = memories.iter().map(|&(nid, _)| nid).collect();
    counters.retain(|nid, _| live_ids.contains(nid));

    // First pass: find a node with counter == 0, decrementing as we go
    for &(node_id, _created_at) in memories {
        let counter = counters.entry(node_id).or_insert(0);
        if *counter == 0 {
            return Some(node_id);
        }
        *counter -= 1;
    }

    // Safety net: all counters were > 0 and have been decremented.
    // Evict the node with the lowest counter (tiebreak: oldest created_at).
    // After decrementing, find the minimum.
    let mut best: Option<(u64, u8, i64)> = None; // (node_id, counter, created_at)
    for &(node_id, created_at) in memories {
        let counter = *counters.get(&node_id).unwrap_or(&0);
        match best {
            None => best = Some((node_id, counter, created_at)),
            Some((_, best_counter, best_ca)) => {
                if counter < best_counter || (counter == best_counter && created_at < best_ca) {
                    best = Some((node_id, counter, created_at));
                }
            }
        }
    }

    best.map(|(node_id, _, _)| node_id)
}

/// Evict the oldest memory (smallest `created_at`). No counter state needed.
fn find_oldest_candidate(memories: &[(u64, i64)]) -> Option<u64> {
    memories
        .iter()
        .min_by_key(|&&(_, ca)| ca)
        .map(|&(nid, _)| nid)
}

/// Evict the memory with the lowest confidence score. Tiebreak by oldest
/// `created_at` so that equally uncertain memories favor recency.
fn find_lowest_confidence_candidate(memories: &[(u64, i64, f64)]) -> Option<u64> {
    memories
        .iter()
        .min_by(|a, b| {
            a.2.partial_cmp(&b.2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        })
        .map(|&(nid, _, _)| nid)
}

// ── Eviction tests ───────────────────────────────────────────────────

#[cfg(test)]
mod memory_eviction_tests {
    use super::{find_eviction_candidate, find_lowest_confidence_candidate, find_oldest_candidate};
    use std::collections::HashMap;

    #[test]
    fn clock_evicts_zero_counter_first() {
        let memories = vec![(10, 100), (20, 200)];
        let mut counters = HashMap::new();
        counters.insert(10u64, 0u8);
        counters.insert(20, 1);

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(evicted, Some(10));
    }

    #[test]
    fn clock_decrements_on_sweep() {
        let memories = vec![(10, 100), (20, 200)];
        let mut counters = HashMap::new();
        counters.insert(10u64, 3u8);
        counters.insert(20, 2);

        let _evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(*counters.get(&10).unwrap(), 2);
        assert_eq!(*counters.get(&20).unwrap(), 1);
    }

    #[test]
    fn clock_evicts_oldest_at_tiebreak() {
        let memories = vec![(10, 50), (20, 100)];
        let mut counters = HashMap::new();
        counters.insert(10u64, 0u8);
        counters.insert(20, 0);

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(evicted, Some(10));
    }

    #[test]
    fn clock_cap_at_three() {
        let mut counter: u8 = 2;
        counter = (counter + 1).min(3);
        assert_eq!(counter, 3);
        counter = (counter + 1).min(3);
        assert_eq!(counter, 3);
    }

    #[test]
    fn clock_safety_net() {
        let memories = vec![(10, 100), (20, 200)];
        let mut counters = HashMap::new();
        counters.insert(10u64, 1u8);
        counters.insert(20, 2);

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(evicted, Some(10));
    }

    #[test]
    fn clock_cold_start() {
        let memories = vec![(10, 100), (20, 200), (30, 300)];
        let mut counters = HashMap::new();

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(evicted, Some(10));
    }

    #[test]
    fn clock_empty_namespace() {
        let memories: Vec<(u64, i64)> = vec![];
        let mut counters = HashMap::new();

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(evicted, None);
    }

    #[test]
    fn clock_single_memory() {
        let memories = vec![(42, 100)];
        let mut counters = HashMap::new();

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(evicted, Some(42));
    }

    #[test]
    fn eviction_unlimited_at_zero() {
        let memories = vec![(10, 100)];
        let mut counters = HashMap::new();
        counters.insert(10u64, 3u8);

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(evicted, Some(10));
    }

    #[test]
    fn default_ttl_auto_sets_valid_until() {
        let now_ms: i64 = 1_000_000;
        let default_ttl_ms: i64 = 60_000;
        let caller_valid_until: Option<i64> = None;

        let valid_until = if let Some(vu) = caller_valid_until {
            vu
        } else if default_ttl_ms > 0 {
            now_ms + default_ttl_ms
        } else {
            0
        };

        assert_eq!(valid_until, 1_060_000);
    }

    #[test]
    fn clock_frequently_recalled_survives_multiple_rounds() {
        let memories = vec![(10, 100), (20, 200)];
        let mut counters = HashMap::new();
        counters.insert(10u64, 3u8);
        counters.insert(20, 0);

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(evicted, Some(20), "round 1: cold node evicted");

        let memories = vec![(10, 100), (30, 300)];
        counters.remove(&20);
        counters.insert(30, 0);

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(
            evicted,
            Some(30),
            "round 2: cold node evicted, popular survives"
        );

        let memories = vec![(10, 100), (40, 400)];
        counters.remove(&30);
        counters.insert(40, 0);

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(
            evicted,
            Some(40),
            "round 3: cold node evicted, popular survives"
        );

        let memories = vec![(10, 100)];
        counters.remove(&40);

        let evicted = find_eviction_candidate(&memories, &mut counters);
        assert_eq!(
            evicted,
            Some(10),
            "round 4: popular node finally evicted when alone"
        );
    }

    #[test]
    fn forget_requires_target() {
        let node_id: Option<u64> = None;
        let query: Option<String> = None;
        let needs_target = node_id.is_none() && query.is_none();
        assert!(needs_target);
    }

    #[test]
    fn forget_by_node_id_accepted() {
        let node_id: Option<u64> = Some(42);
        let query: Option<String> = None;
        let needs_target = node_id.is_none() && query.is_none();
        assert!(!needs_target);
    }

    #[test]
    fn forget_by_query_accepted() {
        let node_id: Option<u64> = None;
        let query: Option<String> = Some("test content".to_string());
        let needs_target = node_id.is_none() && query.is_none();
        assert!(!needs_target);
    }

    #[test]
    fn eviction_respects_max_memories() {
        let max_memories: i64 = 3;
        let count: i64 = 3;
        let should_evict = max_memories > 0 && count >= max_memories;
        assert!(should_evict);

        let count_under: i64 = 2;
        let should_evict_under = max_memories > 0 && count_under >= max_memories;
        assert!(!should_evict_under);
    }

    // ── Oldest policy tests ─────────────────────────────────────────

    #[test]
    fn oldest_evicts_smallest_created_at() {
        let memories = vec![(10, 300), (20, 100), (30, 200)];
        assert_eq!(find_oldest_candidate(&memories), Some(20));
    }

    #[test]
    fn oldest_empty() {
        let memories: Vec<(u64, i64)> = vec![];
        assert_eq!(find_oldest_candidate(&memories), None);
    }

    #[test]
    fn oldest_single() {
        let memories = vec![(42, 500)];
        assert_eq!(find_oldest_candidate(&memories), Some(42));
    }

    // ── Lowest confidence policy tests ──────────────────────────────

    #[test]
    fn lowest_confidence_evicts_least_confident() {
        let memories = vec![(10, 100, 0.9), (20, 200, 0.3), (30, 300, 0.7)];
        assert_eq!(find_lowest_confidence_candidate(&memories), Some(20));
    }

    #[test]
    fn lowest_confidence_tiebreak_oldest() {
        let memories = vec![(10, 300, 0.5), (20, 100, 0.5), (30, 200, 0.8)];
        assert_eq!(find_lowest_confidence_candidate(&memories), Some(20));
    }

    #[test]
    fn lowest_confidence_empty() {
        let memories: Vec<(u64, i64, f64)> = vec![];
        assert_eq!(find_lowest_confidence_candidate(&memories), None);
    }

    #[test]
    fn lowest_confidence_all_default() {
        let memories = vec![(10, 300, 1.0), (20, 100, 1.0), (30, 200, 1.0)];
        assert_eq!(find_lowest_confidence_candidate(&memories), Some(20));
    }
}
