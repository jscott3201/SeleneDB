//! Agent memory recall procedure: vector-based retrieval with temporal filtering.
//!
//! `memory.recall(namespace, queryText, k)` searches `__Memory` nodes by
//! namespace, filters by temporal validity, and ranks by cosine similarity
//! against the embedded query text.
//!
//! This procedure is read-only. Clock counter updates for the enhanced
//! eviction algorithm happen in the MCP `recall` tool, not here.

use roaring::RoaringBitmap;
use selene_core::{IStr, NodeId, Value};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::vector::top_k_cosine_scan;
use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

/// Maximum k for memory recall.
const MAX_K: usize = 10_000;

/// Agent memory recall: vector similarity search with temporal filtering.
pub struct MemoryRecall;

impl Procedure for MemoryRecall {
    fn name(&self) -> &'static str {
        "memory.recall"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "namespace",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "queryText",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "k",
                    typ: GqlType::Int,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "nodeId",
                    typ: GqlType::UInt,
                },
                YieldColumn {
                    name: "content",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "memoryType",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "score",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "confidence",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "createdAt",
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
        if args.len() < 3 {
            return Err(GqlError::InvalidArgument {
                message: "memory.recall requires 3 arguments: namespace, queryText, k".into(),
            });
        }

        let namespace = args[0].as_str()?;
        let query_text = args[1].as_str()?;
        let k_raw = args[2].as_int()?;

        if k_raw < 0 {
            return Err(GqlError::InvalidArgument {
                message: "memory.recall: k must be non-negative".into(),
            });
        }
        let k = k_raw as usize;
        if k == 0 {
            return Ok(vec![]);
        }
        if k > MAX_K {
            return Err(GqlError::InvalidArgument {
                message: format!("memory.recall: k must be <= {MAX_K}"),
            });
        }

        // -- 2. Embed query text --
        let query_vec = crate::runtime::embed::embed_text_with_task(
            query_text,
            crate::runtime::embed::EmbeddingTask::Document,
        )?;

        // -- 3. Filter __Memory nodes by namespace + temporal validity --
        let namespace_key = IStr::new("namespace");
        let valid_from_key = IStr::new("valid_from");
        let valid_until_key = IStr::new("valid_until");
        let embedding_key = IStr::new("embedding");

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Collect candidate node IDs that pass namespace + temporal filters
        let candidates: Vec<NodeId> = graph
            .nodes_by_label("__Memory")
            .filter(|nid| {
                // Respect scope if provided
                if let Some(s) = scope
                    && !s.contains(nid.0 as u32)
                {
                    return false;
                }

                let Some(node) = graph.get_node(*nid) else {
                    return false;
                };

                // Namespace match
                let ns_match = match node.properties.get(namespace_key) {
                    Some(Value::String(s)) => s.as_str() == namespace,
                    Some(Value::InternedStr(s)) => s.as_str() == namespace,
                    _ => false,
                };
                if !ns_match {
                    return false;
                }

                // Must have an embedding vector
                if !matches!(node.properties.get(embedding_key), Some(Value::Vector(_))) {
                    return false;
                }

                // Temporal validity: now >= valid_from
                let valid_from = match node.properties.get(valid_from_key) {
                    Some(Value::Int(v)) => *v,
                    _ => 0,
                };
                if now_ms < valid_from {
                    return false;
                }

                // Temporal validity: valid_until == 0 OR now <= valid_until
                let valid_until = match node.properties.get(valid_until_key) {
                    Some(Value::Int(v)) => *v,
                    _ => 0,
                };
                if valid_until != 0 && now_ms > valid_until {
                    return false;
                }

                true
            })
            .collect();

        if candidates.is_empty() {
            return Ok(vec![]);
        }

        // -- 4. Vector similarity search over filtered candidates --
        let scored = top_k_cosine_scan(
            graph,
            candidates.into_iter(),
            embedding_key,
            &query_vec,
            k,
            None, // scope already applied during filtering
        );

        // -- 5. Build result rows --
        let content_key = IStr::new("content");
        let memory_type_key = IStr::new("memory_type");
        let confidence_key = IStr::new("confidence");
        let created_at_key = IStr::new("created_at");

        Ok(scored
            .into_iter()
            .map(|s| {
                let node = graph.get_node(s.node_id);
                let content = node
                    .as_ref()
                    .and_then(|n| match n.properties.get(content_key) {
                        Some(Value::String(v)) => Some(v.as_str()),
                        Some(Value::InternedStr(v)) => Some(v.as_str()),
                        _ => None,
                    })
                    .unwrap_or("");
                let memory_type = node
                    .as_ref()
                    .and_then(|n| match n.properties.get(memory_type_key) {
                        Some(Value::String(v)) => Some(v.as_str()),
                        Some(Value::InternedStr(v)) => Some(v.as_str()),
                        _ => None,
                    })
                    .unwrap_or("fact");
                let confidence = node
                    .as_ref()
                    .and_then(|n| match n.properties.get(confidence_key) {
                        Some(Value::Float(v)) => Some(*v),
                        Some(Value::Int(v)) => Some(*v as f64),
                        _ => None,
                    })
                    .unwrap_or(1.0);
                let created_at = node
                    .as_ref()
                    .and_then(|n| match n.properties.get(created_at_key) {
                        Some(Value::Int(v)) => Some(*v),
                        _ => None,
                    })
                    .unwrap_or(0);

                smallvec![
                    (IStr::new("nodeId"), GqlValue::UInt(s.node_id.0)),
                    (
                        IStr::new("content"),
                        GqlValue::String(smol_str::SmolStr::new(content))
                    ),
                    (
                        IStr::new("memoryType"),
                        GqlValue::String(smol_str::SmolStr::new(memory_type))
                    ),
                    (IStr::new("score"), GqlValue::Float(f64::from(s.score))),
                    (IStr::new("confidence"), GqlValue::Float(confidence)),
                    (IStr::new("createdAt"), GqlValue::Int(created_at)),
                ]
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recall_rejects_too_few_args() {
        let proc = MemoryRecall;
        let graph = SeleneGraph::new();

        // Zero args
        let result = proc.execute(&[], &graph, None, None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 3 arguments")
        );

        // Two args
        let result = proc.execute(
            &[
                GqlValue::String("ns".into()),
                GqlValue::String("query".into()),
            ],
            &graph,
            None,
            None,
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("requires 3 arguments")
        );
    }

    #[test]
    fn recall_rejects_negative_k() {
        let proc = MemoryRecall;
        let graph = SeleneGraph::new();
        let result = proc.execute(
            &[
                GqlValue::String("ns".into()),
                GqlValue::String("query".into()),
                GqlValue::Int(-1),
            ],
            &graph,
            None,
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-negative"));
    }

    #[test]
    fn recall_returns_empty_for_zero_k() {
        let proc = MemoryRecall;
        let graph = SeleneGraph::new();
        let result = proc
            .execute(
                &[
                    GqlValue::String("ns".into()),
                    GqlValue::String("query".into()),
                    GqlValue::Int(0),
                ],
                &graph,
                None,
                None,
            )
            .unwrap();
        assert!(result.is_empty());
    }
}
