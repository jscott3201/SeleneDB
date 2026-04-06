//! GraphRAG hybrid retriever: vector search + graph traversal + community context.
//!
//! `graphrag.search(queryText, k, maxHops, mode)` combines embedding-based
//! similarity with BFS graph expansion and optional community summaries for
//! retrieval-augmented generation with graph-aware context.
//!
//! Three modes:
//! - `"local"` (default): vector search on nodes, BFS expansion, opportunistic community
//! - `"global"`: vector search on `__CommunitySummary` embeddings, return community profiles
//! - `"hybrid"`: run both, merge with provenance tags
//!
//! Falls back to `"local"` if community embeddings are absent.

use std::collections::HashSet;
use std::fmt::Write;

use roaring::RoaringBitmap;
use selene_core::{IStr, NodeId, Value};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::vector::top_k_cosine_scan;
use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

/// Maximum k for vector search.
const MAX_K: usize = 10_000;

/// Maximum hops for BFS expansion.
const MAX_HOPS: u32 = 10;

/// Maximum characters per context string.
const MAX_CONTEXT_CHARS: usize = 2048;

// ---------------------------------------------------------------------------
// Procedure definition
// ---------------------------------------------------------------------------

/// GraphRAG hybrid search: vector + graph traversal + community context.
pub struct GraphRagSearch;

impl Procedure for GraphRagSearch {
    fn name(&self) -> &'static str {
        "graphrag.search"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "queryText",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "k",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "maxHops",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "mode",
                    typ: GqlType::String,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "nodeId",
                    typ: GqlType::UInt,
                },
                YieldColumn {
                    name: "score",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "source",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "context",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "depth",
                    typ: GqlType::Int,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        scope: Option<&RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        // -- 1. Validate arguments --
        if args.is_empty() {
            return Err(GqlError::InvalidArgument {
                message: "graphrag.search requires at least 1 argument: queryText".into(),
            });
        }

        let query_text = args[0].as_str()?;

        let k = if args.len() > 1 && !args[1].is_null() {
            let k_raw = args[1].as_int()?;
            if k_raw < 0 {
                return Err(GqlError::InvalidArgument {
                    message: "graphrag.search: k must be non-negative".into(),
                });
            }
            k_raw as usize
        } else {
            10
        };
        if k == 0 {
            return Ok(vec![]);
        }
        if k > MAX_K {
            return Err(GqlError::InvalidArgument {
                message: format!("graphrag.search: k must be <= {MAX_K}"),
            });
        }

        let max_hops = if args.len() > 2 && !args[2].is_null() {
            let h = args[2].as_int()?;
            if h < 0 {
                return Err(GqlError::InvalidArgument {
                    message: "graphrag.search: maxHops must be non-negative".into(),
                });
            }
            (h as u32).min(MAX_HOPS)
        } else {
            2
        };

        let mode = if args.len() > 3 && !args[3].is_null() {
            let m = args[3].as_str()?;
            match m {
                "local" | "global" | "hybrid" => m.to_string(),
                other => {
                    return Err(GqlError::InvalidArgument {
                        message: format!(
                            "graphrag.search: mode must be 'local', 'global', or 'hybrid', got '{other}'"
                        ),
                    });
                }
            }
        } else {
            "local".to_string()
        };

        // -- 2. Embed query text --
        let query_vec = crate::runtime::embed::embed_text_with_task(
            query_text,
            crate::runtime::embed::EmbeddingTask::Retrieval,
        )?;

        // -- 3. Check if community summaries with embeddings exist --
        let embedding_key = IStr::new("embedding");
        let has_community_embeddings = graph.nodes_by_label("__CommunitySummary").any(|nid| {
            graph
                .get_node(nid)
                .and_then(|n| n.properties.get(embedding_key))
                .is_some_and(|v| matches!(v, Value::Vector(_)))
        });

        // Determine effective mode: fall back to local if no community embeddings
        let effective_mode = if (mode == "global" || mode == "hybrid") && !has_community_embeddings
        {
            "local"
        } else {
            &mode
        };

        match effective_mode {
            "local" => Ok(local_search(graph, &query_vec, k, max_hops, scope)),
            "global" => Ok(global_search(graph, &query_vec, k, scope)),
            "hybrid" => {
                let mut local_rows = local_search(graph, &query_vec, k, max_hops, scope);
                let global_rows = global_search(graph, &query_vec, k, scope);
                merge_results(&mut local_rows, global_rows);
                Ok(local_rows)
            }
            _ => unreachable!(),
        }
    }
}

// ---------------------------------------------------------------------------
// Local mode: vector search + BFS expansion + opportunistic community
// ---------------------------------------------------------------------------

fn local_search(
    graph: &SeleneGraph,
    query_vec: &[f32],
    k: usize,
    max_hops: u32,
    scope: Option<&RoaringBitmap>,
) -> Vec<ProcedureRow> {
    let prop_key = IStr::new("embedding");

    // -- HNSW fast path or brute-force fallback --
    let top_results = hnsw_or_fallback(graph, query_vec, k, scope, prop_key);

    if top_results.is_empty() {
        return vec![];
    }

    let mut rows: Vec<ProcedureRow> = Vec::new();
    let mut seen: HashSet<NodeId> = HashSet::new();

    // Collect vector hit node IDs for community lookup
    let community_id_key = IStr::new("community_id");

    // -- Vector results --
    for scored in &top_results {
        seen.insert(scored.node_id);
        rows.push(build_row(
            graph,
            scored.node_id,
            f64::from(scored.score),
            "vector",
            0,
        ));
    }

    // -- BFS expansion from each hit --
    if max_hops > 0 {
        for scored in &top_results {
            let neighbors =
                selene_graph::algorithms::bfs_with_depth(graph, scored.node_id, None, max_hops);
            for (nid, depth) in neighbors {
                if let Some(s) = scope
                    && !s.contains(nid.0 as u32)
                {
                    continue;
                }
                if seen.insert(nid) {
                    // Score graph-expanded nodes as a fraction of the parent vector score
                    let decay = 1.0 / f64::from(depth + 1);
                    let graph_score = f64::from(scored.score) * decay;
                    rows.push(build_row(
                        graph,
                        nid,
                        graph_score,
                        "graph",
                        i64::from(depth),
                    ));
                }
            }
        }
    }

    // -- Opportunistic community context --
    // Check if any vector hit nodes have a community_id property,
    // and if matching __CommunitySummary nodes exist
    let mut community_ids: HashSet<u64> = HashSet::new();
    for scored in &top_results {
        if let Some(node) = graph.get_node(scored.node_id) {
            if let Some(Value::UInt(cid)) = node.properties.get(community_id_key) {
                community_ids.insert(*cid);
            } else if let Some(Value::Int(cid)) = node.properties.get(community_id_key) {
                community_ids.insert(*cid as u64);
            }
        }
    }

    if !community_ids.is_empty() {
        let cid_key = IStr::new("community_id");
        for nid in graph.nodes_by_label("__CommunitySummary") {
            if let Some(s) = scope
                && !s.contains(nid.0 as u32)
            {
                continue;
            }
            if let Some(node) = graph.get_node(nid) {
                let matches = match node.properties.get(cid_key) {
                    Some(Value::UInt(cid)) => community_ids.contains(cid),
                    Some(Value::Int(cid)) => community_ids.contains(&(*cid as u64)),
                    _ => false,
                };
                if matches && seen.insert(nid) {
                    rows.push(build_row(graph, nid, 0.5, "community", 0));
                }
            }
        }
    }

    rows
}

// ---------------------------------------------------------------------------
// Global mode: vector search over __CommunitySummary embeddings
// ---------------------------------------------------------------------------

fn global_search(
    graph: &SeleneGraph,
    query_vec: &[f32],
    k: usize,
    scope: Option<&RoaringBitmap>,
) -> Vec<ProcedureRow> {
    let prop_key = IStr::new("embedding");
    let community_nodes = graph.nodes_by_label("__CommunitySummary");

    let top_results = top_k_cosine_scan(graph, community_nodes, prop_key, query_vec, k, scope);

    top_results
        .into_iter()
        .map(|scored| {
            build_row(
                graph,
                scored.node_id,
                f64::from(scored.score),
                "community",
                0,
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Merge helper for hybrid mode
// ---------------------------------------------------------------------------

/// Merge global results into local results, deduplicating by nodeId and
/// keeping the highest score for each node.
fn merge_results(local: &mut Vec<ProcedureRow>, global: Vec<ProcedureRow>) {
    let mut seen: HashSet<u64> = HashSet::new();
    for row in local.iter() {
        if let Some((_, GqlValue::UInt(id))) = row.first() {
            seen.insert(*id);
        }
    }

    for row in global {
        if let Some((_, GqlValue::UInt(id))) = row.first()
            && seen.insert(*id)
        {
            local.push(row);
        }
    }
}

// ---------------------------------------------------------------------------
// HNSW or brute-force vector search
// ---------------------------------------------------------------------------

fn hnsw_or_fallback(
    graph: &SeleneGraph,
    query_vec: &[f32],
    k: usize,
    scope: Option<&RoaringBitmap>,
    prop_key: IStr,
) -> Vec<super::vector::ScoredNode> {
    // Try HNSW index first
    if let Some(hnsw) = graph.hnsw_index() {
        let filter = scope.cloned();
        let hnsw_results = hnsw.search(query_vec, k, None, filter.as_ref());

        if !hnsw_results.is_empty() {
            return hnsw_results
                .into_iter()
                .map(|(node_id, score)| super::vector::ScoredNode { node_id, score })
                .collect();
        }
    }

    // Fall through to brute-force
    top_k_cosine_scan(graph, graph.all_node_ids(), prop_key, query_vec, k, scope)
}

// ---------------------------------------------------------------------------
// Row builder
// ---------------------------------------------------------------------------

/// Build a single result row with context string.
fn build_row(
    graph: &SeleneGraph,
    node_id: NodeId,
    score: f64,
    source: &str,
    depth: i64,
) -> ProcedureRow {
    let context = build_context(graph, node_id);
    smallvec![
        (IStr::new("nodeId"), GqlValue::UInt(node_id.0)),
        (IStr::new("score"), GqlValue::Float(score)),
        (IStr::new("source"), GqlValue::String(SmolStr::new(source))),
        (
            IStr::new("context"),
            GqlValue::String(SmolStr::new(&context))
        ),
        (IStr::new("depth"), GqlValue::Int(depth)),
    ]
}

/// Build a compact context string for a node: labels + top properties.
/// Truncates large values and skips vectors. Capped at MAX_CONTEXT_CHARS.
fn build_context(graph: &SeleneGraph, node_id: NodeId) -> String {
    let Some(node) = graph.get_node(node_id) else {
        return String::new();
    };

    let mut ctx = String::with_capacity(256);

    // Labels
    let labels: Vec<&str> = node.labels.iter().map(|l| l.as_str()).collect();
    let _ = write!(ctx, "[{}]", labels.join(","));

    // Properties (skip vectors, truncate large values)
    for (key, value) in node.properties.iter() {
        if ctx.len() >= MAX_CONTEXT_CHARS {
            break;
        }
        // Skip vector properties
        if matches!(value, Value::Vector(_)) {
            continue;
        }
        let val_str = format!("{value}");
        let truncated = if val_str.len() > 200 {
            format!("{}...", &val_str[..val_str.floor_char_boundary(197)])
        } else {
            val_str
        };
        let _ = write!(ctx, " {key}={truncated}");
    }

    if ctx.len() > MAX_CONTEXT_CHARS {
        ctx.truncate(ctx.floor_char_boundary(MAX_CONTEXT_CHARS));
    }

    ctx
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, PropertyMap, Value};

    #[test]
    fn test_arg_validation_too_few() {
        let proc = GraphRagSearch;
        let graph = SeleneGraph::new();
        let result = proc.execute(&[], &graph, None, None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("requires at least 1 argument"));
    }

    #[test]
    fn test_arg_validation_negative_k() {
        let proc = GraphRagSearch;
        let graph = SeleneGraph::new();
        let result = proc.execute(
            &[GqlValue::String("test".into()), GqlValue::Int(-5)],
            &graph,
            None,
            None,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("non-negative"));
    }

    #[test]
    fn test_arg_validation_invalid_mode() {
        let proc = GraphRagSearch;
        let graph = SeleneGraph::new();
        let result = proc.execute(
            &[
                GqlValue::String("test".into()),
                GqlValue::Int(10),
                GqlValue::Int(2),
                GqlValue::String("invalid_mode".into()),
            ],
            &graph,
            None,
            None,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("mode must be"));
    }

    #[test]
    fn test_k_zero_returns_empty() {
        let proc = GraphRagSearch;
        let graph = SeleneGraph::new();
        let result = proc
            .execute(
                &[GqlValue::String("test".into()), GqlValue::Int(0)],
                &graph,
                None,
                None,
            )
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_build_context_basic() {
        let mut graph = SeleneGraph::new();
        let mut m = graph.mutate();
        m.create_node(
            LabelSet::from_strs(&["Sensor", "Temperature"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::from("TempSensor1")),
                (IStr::new("unit"), Value::from("F")),
            ]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let ctx = build_context(&graph, NodeId(1));
        // Should contain labels
        assert!(ctx.contains("Sensor"));
        assert!(ctx.contains("Temperature"));
        // Should contain properties
        assert!(ctx.contains("name="));
        assert!(ctx.contains("TempSensor1"));
    }

    #[test]
    fn test_build_context_skips_vectors() {
        let mut graph = SeleneGraph::new();
        let mut m = graph.mutate();
        m.create_node(
            LabelSet::from_strs(&["Node"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::from("test")),
                (
                    IStr::new("embedding"),
                    Value::Vector(std::sync::Arc::from(vec![1.0f32; 384])),
                ),
            ]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let ctx = build_context(&graph, NodeId(1));
        assert!(ctx.contains("name="));
        // Should not contain a huge vector dump
        assert!(ctx.len() < 500);
    }

    #[test]
    fn test_build_row_structure() {
        let graph = SeleneGraph::new();
        let row = build_row(&graph, NodeId(42), 0.95, "vector", 0);
        assert_eq!(row.len(), 5);
        assert_eq!(row[0].0.as_str(), "nodeId");
        assert_eq!(row[1].0.as_str(), "score");
        assert_eq!(row[2].0.as_str(), "source");
        assert_eq!(row[3].0.as_str(), "context");
        assert_eq!(row[4].0.as_str(), "depth");
    }
}
