//! GraphRAG AI tool implementations: community detection and hybrid search.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, Content, ErrorCode};

use crate::http::mcp::format::structured_text_result;
use crate::http::mcp::params::*;
use crate::http::mcp::{SeleneTools, mcp_auth, op_err, reject_replica};
use crate::ops;
use selene_core::Value;

pub(super) async fn build_communities_impl(
    tools: &SeleneTools,
    p: BuildCommunitiesParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;
    let min_size = p.min_community_size.unwrap_or(2);
    let start = std::time::Instant::now();

    // 1. Build projection excluding __ labels and run Louvain
    let communities = tools
        .state
        .graph
        .read(|graph| build_community_data(graph, min_size));

    if communities.is_empty() {
        return Ok(CallToolResult::success(vec![Content::text(
            "No communities found (graph may be empty or fully disconnected).",
        )]));
    }

    // 2. MERGE __CommunitySummary nodes via parameterized GQL
    let community_count = communities.len();
    let total_f64 = community_count as f64;
    let mut total_nodes_covered = 0usize;
    for (i, community) in communities.iter().enumerate() {
        tools
            .send_progress(
                i as f64,
                Some(total_f64),
                Some(&format!(
                    "Processing community {}/{}",
                    i + 1,
                    community_count
                )),
            )
            .await;
        total_nodes_covered += community.node_count;
        let mut params_map = HashMap::new();
        params_map.insert("cid".into(), Value::UInt(community.community_id));
        params_map.insert(
            "label_dist".into(),
            Value::from(community.label_distribution.as_str()),
        );
        params_map.insert(
            "key_entities".into(),
            Value::from(community.key_entities.as_str()),
        );
        params_map.insert("node_count".into(), Value::Int(community.node_count as i64));
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        params_map.insert("updated_at".into(), Value::Int(now_ms));

        let query = "MERGE (c:__CommunitySummary {community_id: $cid}) \
                     SET c.label_distribution = $label_dist, \
                     c.key_entities = $key_entities, \
                     c.node_count = $node_count, \
                     c.updated_at = $updated_at";

        let st = Arc::clone(&tools.state);
        let auth2 = auth.clone();
        tools
            .submit_mut(move || {
                ops::gql::execute_gql(
                    &st,
                    &auth2,
                    query,
                    Some(&params_map),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                )
            })
            .await?;
    }

    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let text = format!(
        "Built {community_count} communities covering {total_nodes_covered} nodes in {elapsed_secs:.1}s",
    );
    Ok(structured_text_result(
        text,
        serde_json::json!({
            "community_count": community_count,
            "total_nodes_covered": total_nodes_covered,
            "elapsed_secs": elapsed_secs,
        }),
    ))
}

pub(super) async fn graphrag_search_impl(
    tools: &SeleneTools,
    p: GraphRagSearchParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let k = p.k.unwrap_or(10);
    let max_hops = p.max_hops.unwrap_or(2);
    let mode = p.mode.unwrap_or_else(|| "local".to_string());

    if k <= 0 {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: "k must be a positive integer".into(),
            data: None,
        });
    }
    if p.query_vector.is_empty() {
        return Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: "query_vector must be a non-empty array of floats".into(),
            data: None,
        });
    }

    let query = "CALL graphrag.search($queryVec, $k, $maxHops, $mode) \
                 YIELD node_id, score, source, context, depth \
                 RETURN node_id, score, source, context, depth";

    let mut gql_params = HashMap::new();
    gql_params.insert("queryVec".into(), Value::Vector(Arc::from(p.query_vector)));
    gql_params.insert("k".into(), Value::Int(k));
    gql_params.insert("maxHops".into(), Value::Int(max_hops));
    gql_params.insert("mode".into(), Value::from(mode.as_str()));

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

    if !result.status_code.starts_with("00") && !result.status_code.starts_with("02") {
        return Ok(CallToolResult::success(vec![Content::text(format!(
            "Error: {}",
            result.message
        ))]));
    }

    let data = result.data_json.unwrap_or_else(|| "[]".to_string());
    let parsed: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
    let text = format!("GraphRAG search: {} results\n{data}", result.row_count);
    Ok(structured_text_result(
        text,
        serde_json::json!({
            "row_count": result.row_count,
            "results": parsed,
        }),
    ))
}

// ── Community detection helpers ──────────────────────────────────────

/// Structural profile for a detected community.
struct CommunityData {
    community_id: u64,
    label_distribution: String,
    key_entities: String,
    node_count: usize,
}

/// Build community data from the graph using Louvain detection.
///
/// Creates a projection excluding system labels (__ prefix), runs Louvain,
/// groups results by community, and computes structural profiles.
fn build_community_data(graph: &selene_graph::SeleneGraph, min_size: usize) -> Vec<CommunityData> {
    use std::collections::HashMap as StdHashMap;

    let config = selene_algorithms::ProjectionConfig {
        name: "__build_communities".to_string(),
        node_labels: vec![],
        edge_labels: vec![],
        weight_property: None,
    };
    let proj = selene_algorithms::GraphProjection::build(graph, &config, None);
    let louvain_result = selene_algorithms::louvain(&proj);

    // Group nodes by community, excluding __ label nodes
    let mut community_nodes: StdHashMap<u64, Vec<selene_core::NodeId>> = StdHashMap::new();
    for (nid, cid, _level) in &louvain_result {
        if let Some(node) = graph.get_node(*nid) {
            let has_user_label = node.labels.iter().any(|l| !l.as_str().starts_with("__"));
            if has_user_label {
                community_nodes.entry(*cid).or_default().push(*nid);
            }
        }
    }

    let name_key = selene_core::IStr::new("name");
    let desc_key = selene_core::IStr::new("description");

    let mut result = Vec::new();
    for (cid, members) in &community_nodes {
        if members.len() < min_size {
            continue;
        }

        // Label distribution
        let mut label_counts: StdHashMap<&str, usize> = StdHashMap::new();
        for &nid in members {
            if let Some(node) = graph.get_node(nid) {
                for label in node.labels.iter() {
                    if !label.as_str().starts_with("__") {
                        *label_counts.entry(label.as_str()).or_insert(0) += 1;
                    }
                }
            }
        }
        let mut label_pairs: Vec<_> = label_counts.into_iter().collect();
        label_pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        let label_dist = label_pairs
            .iter()
            .map(|(l, c)| format!("{l}:{c}"))
            .collect::<Vec<_>>()
            .join(",");

        // Key entities: top-5 nodes by name or description
        let mut entity_names: Vec<String> = Vec::new();
        for &nid in members {
            if entity_names.len() >= 5 {
                break;
            }
            if let Some(node) = graph.get_node(nid) {
                if let Some(selene_core::Value::String(s)) = node.properties.get(name_key) {
                    entity_names.push(s.to_string());
                } else if let Some(selene_core::Value::InternedStr(s)) =
                    node.properties.get(name_key)
                {
                    entity_names.push(s.as_str().to_string());
                } else if let Some(selene_core::Value::String(s)) = node.properties.get(desc_key) {
                    entity_names.push(s.to_string());
                }
            }
        }
        let key_entities = entity_names.join(", ");

        result.push(CommunityData {
            community_id: *cid,
            label_distribution: label_dist,
            key_entities,
            node_count: members.len(),
        });
    }

    result.sort_by(|a, b| b.node_count.cmp(&a.node_count));
    result
}
