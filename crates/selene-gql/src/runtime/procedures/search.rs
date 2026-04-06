//! Full-text search procedures: BM25 ranked retrieval and hybrid search.
//!
//! These procedures delegate to a SearchIndex set via a static OnceLock
//! (same pattern as the embedding engine). The SearchIndex is set at server
//! startup from bootstrap.rs.

use std::sync::{Arc, OnceLock};

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── Search index access ─────────────────────────────────────────────

/// Trait for search index operations (decouples selene-gql from selene-server).
pub trait SearchProvider: Send + Sync {
    /// Search a specific (label, property) index.
    fn search(
        &self,
        label: &str,
        property: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(NodeId, f32)>, String>;

    /// Search across ALL searchable properties for a label.
    /// Default implementation returns empty (override in SearchIndex).
    fn search_all_properties(
        &self,
        _label: &str,
        _query: &str,
        _limit: usize,
    ) -> Result<Vec<(NodeId, f32)>, String> {
        Ok(vec![])
    }
}

static SEARCH_PROVIDER: OnceLock<Arc<dyn SearchProvider>> = OnceLock::new();

/// Set the search provider. Called once at server startup.
pub fn set_search_provider(provider: Arc<dyn SearchProvider>) {
    let _ = SEARCH_PROVIDER.set(provider);
}

fn get_search_provider() -> Result<&'static Arc<dyn SearchProvider>, GqlError> {
    SEARCH_PROVIDER.get().ok_or_else(|| GqlError::InvalidArgument {
        message: "full-text search not available (no searchable schemas or --features search not enabled)".into(),
    })
}

// ── graph.textSearch ────────────────────────────────────────────────

/// `CALL graph.textSearch('sensor', 'name', 'supply air temperature', 10)`
pub struct TextSearch;

impl Procedure for TextSearch {
    fn name(&self) -> &'static str {
        "graph.textSearch"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "label",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "query",
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
                    name: "score",
                    typ: GqlType::Float,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        _graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        if args.len() < 4 {
            return Err(GqlError::InvalidArgument {
                message: "graph.textSearch requires 4 arguments: label, property, query, k".into(),
            });
        }

        let label = args[0].as_str()?;
        let property = args[1].as_str()?;
        let query_text = args[2].as_str()?;
        let k_raw = args[3].as_int()?;
        if k_raw <= 0 {
            return Ok(vec![]);
        }
        let k = k_raw as usize;

        let provider = get_search_provider()?;
        let results = provider
            .search(label, property, query_text, k)
            .map_err(|e| GqlError::InvalidArgument { message: e })?;

        Ok(results
            .into_iter()
            .filter(|(nid, _)| scope.is_none_or(|s| s.contains(nid.0 as u32)))
            .map(|(nid, score)| {
                smallvec![
                    (IStr::new("nodeId"), GqlValue::UInt(nid.0)),
                    (IStr::new("score"), GqlValue::Float(f64::from(score))),
                ]
            })
            .collect())
    }
}

// ── graph.hybridSearch ──────────────────────────────────────────────

/// `CALL graph.hybridSearch('sensor', 'supply air temperature', 10)`
///
/// Combines BM25 text search + cosine vector search via reciprocal rank fusion.
/// Requires both `search` and `vector` features.
pub struct HybridSearch;

impl Procedure for HybridSearch {
    fn name(&self) -> &'static str {
        "graph.hybridSearch"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "label",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "query",
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
                    name: "score",
                    typ: GqlType::Float,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        if args.len() < 3 {
            return Err(GqlError::InvalidArgument {
                message: "graph.hybridSearch requires 3 arguments: label, query, k".into(),
            });
        }

        let label = args[0].as_str()?;
        let query_text = args[1].as_str()?;
        let k_raw = args[2].as_int()?;
        if k_raw <= 0 {
            return Ok(vec![]);
        }
        let k = k_raw as usize;

        let fetch_k = k * 2; // Over-fetch for fusion

        // 1. BM25 text search across all searchable properties for the label
        let text_results = get_search_provider()
            .and_then(|p| {
                p.search_all_properties(label, query_text, fetch_k)
                    .map_err(|_| GqlError::InvalidArgument {
                        message: "text search unavailable".into(),
                    })
            })
            .unwrap_or_default();

        // 2. Vector search
        let query_vec = crate::runtime::embed::embed_text(query_text)?;
        let prop_key = IStr::new("embedding");
        let vec_results = super::vector::top_k_cosine_scan(
            graph,
            graph.nodes_by_label(label),
            prop_key,
            &query_vec,
            fetch_k,
            scope,
        );

        // 3. Reciprocal Rank Fusion
        use std::collections::HashMap;
        const RRF_K: f64 = 60.0;
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for (rank, (nid, _)) in text_results.iter().enumerate() {
            if scope.is_none_or(|s| s.contains(nid.0 as u32)) {
                *scores.entry(nid.0).or_default() += 1.0 / (RRF_K + rank as f64 + 1.0);
            }
        }
        for (rank, scored) in vec_results.iter().enumerate() {
            *scores.entry(scored.node_id.0).or_default() += 1.0 / (RRF_K + rank as f64 + 1.0);
        }

        // Sort by fused score descending
        let mut fused: Vec<(u64, f64)> = scores.into_iter().collect();
        fused.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        fused.truncate(k);

        Ok(fused
            .into_iter()
            .map(|(nid, score)| {
                smallvec![
                    (IStr::new("nodeId"), GqlValue::UInt(nid)),
                    (IStr::new("score"), GqlValue::Float(score)),
                ]
            })
            .collect())
    }
}
