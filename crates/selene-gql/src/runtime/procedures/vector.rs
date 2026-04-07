//! Vector search procedures: HNSW-accelerated and brute-force top-k similarity search.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use roaring::RoaringBitmap;
use selene_core::{IStr, NodeId, Value};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

/// Scored node for the min-heap (inverted ordering for top-k).
pub(crate) struct ScoredNode {
    pub(crate) node_id: NodeId,
    pub(crate) score: f32,
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering: smallest score at top of heap (min-heap for top-k)
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
    }
}

/// SIMD-friendly dot product using 8-wide accumulator.
///
/// 8 independent accumulators break the sequential reduction dependency chain
/// and saturate dual-issue FMA pipelines (M1 can issue 2 NEON FMA/cycle).
/// LLVM auto-vectorizes to ARM NEON, x86 SSE/AVX, WASM SIMD.
/// Portable: no intrinsics, no nightly features.
///
/// For 384-dim vectors (all-MiniLM-L6-v2): 384/8 = 48 iterations, zero remainder.
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = [0.0f32; 8];
    let chunks_a = a.chunks_exact(8);
    let chunks_b = b.chunks_exact(8);
    let rem_a = chunks_a.remainder();
    let rem_b = chunks_b.remainder();
    for (a8, b8) in chunks_a.zip(chunks_b) {
        sum[0] += a8[0] * b8[0];
        sum[1] += a8[1] * b8[1];
        sum[2] += a8[2] * b8[2];
        sum[3] += a8[3] * b8[3];
        sum[4] += a8[4] * b8[4];
        sum[5] += a8[5] * b8[5];
        sum[6] += a8[6] * b8[6];
        sum[7] += a8[7] * b8[7];
    }
    // Pairwise summation for better numerical stability
    let mut total = (sum[0] + sum[1]) + (sum[2] + sum[3]) + (sum[4] + sum[5]) + (sum[6] + sum[7]);
    for (a, b) in rem_a.iter().zip(rem_b) {
        total += a * b;
    }
    total
}

/// Check if a vector is approximately unit-length (L2 norm ≈ 1.0).
fn is_unit_vector(v: &[f32]) -> bool {
    let mag_sq = dot_product(v, v);
    (1.0 - mag_sq).abs() < 1e-5
}

/// Cosine similarity between two f32 slices.
///
/// Uses SIMD-friendly dot_product internally. When both vectors are
/// unit-length, the caller should use dot_product directly instead
/// (see is_unit_vector).
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let mag_a = dot_product(a, a).sqrt();
    let mag_b = dot_product(b, b).sqrt();
    if mag_a == 0.0 || mag_b == 0.0 {
        0.0
    } else {
        dot / (mag_a * mag_b)
    }
}

/// `CALL graph.vectorSearch('sensor', 'embedding', $queryVec, 10)`
///
/// Brute-force top-k cosine similarity search. O(N) scan + O(N log k) heap.
/// Nodes without the specified property or with non-vector values are skipped.
pub struct VectorSearch;

impl Procedure for VectorSearch {
    fn name(&self) -> &'static str {
        "graph.vectorSearch"
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
                    name: "queryVector",
                    typ: GqlType::Vector,
                },
                ProcedureParam {
                    name: "k",
                    typ: GqlType::Int,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
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
        if args.len() < 4 {
            return Err(GqlError::InvalidArgument {
                message: "graph.vectorSearch requires 4 arguments: label, property, queryVector, k"
                    .into(),
            });
        }

        let label = args[0].as_str()?;
        let property = args[1].as_str()?;
        let query_vec = match &args[2] {
            GqlValue::Vector(v) => v.as_ref(),
            other => {
                return Err(GqlError::type_error(format!(
                    "graph.vectorSearch: queryVector must be VECTOR, got {}",
                    other.gql_type()
                )));
            }
        };
        let k_raw = args[3].as_int()?;
        if k_raw < 0 {
            return Err(GqlError::InvalidArgument {
                message: "graph.vectorSearch: k must be non-negative".into(),
            });
        }
        let k = k_raw as usize;

        const MAX_K: usize = 10_000;
        if k == 0 {
            return Ok(vec![]);
        }
        if k > MAX_K {
            return Err(GqlError::InvalidArgument {
                message: format!("graph.vectorSearch: k must be <= {MAX_K}, got {k}"),
            });
        }

        // Fast path: try HNSW index first (approximate nearest neighbor).
        // Route to namespace based on label: system labels (__*) go to their
        // own namespace, user labels go to the default namespace.
        let hnsw_ns = if label.starts_with("__") {
            label.to_lowercase()
        } else {
            String::new()
        };
        if let Some(hnsw) = graph.hnsw_index_for(&hnsw_ns) {
            // Build a combined label + scope filter bitmap.
            let label_bitmap: RoaringBitmap =
                graph.label_bitmap(label).cloned().unwrap_or_default();
            let filter = if let Some(scope_bm) = scope {
                let mut combined = label_bitmap;
                combined &= scope_bm;
                combined
            } else {
                label_bitmap
            };

            // Use the params default ef_search (None delegates to HnswParams).
            let hnsw_results = hnsw.search(query_vec, k, None, Some(&filter));

            if !hnsw_results.is_empty() {
                return Ok(hnsw_results
                    .into_iter()
                    .map(|(node_id, score)| {
                        smallvec![
                            (IStr::new("node_id"), GqlValue::Int(node_id.0 as i64)),
                            (IStr::new("score"), GqlValue::Float(f64::from(score))),
                        ]
                    })
                    .collect());
            }
            // Fall through to brute-force when HNSW returned empty
            // (e.g., index not yet populated for this label).
        }

        let prop_key = IStr::new(property);
        let results = top_k_cosine_scan(
            graph,
            graph.nodes_by_label(label),
            prop_key,
            query_vec,
            k,
            scope,
        );

        Ok(results
            .into_iter()
            .map(|s| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(s.node_id.0 as i64)),
                    (IStr::new("score"), GqlValue::Float(f64::from(s.score))),
                ]
            })
            .collect())
    }
}

/// Shared top-k cosine scan used by vectorSearch, semanticSearch, similarNodes, and hybridSearch.
///
/// Uses VectorProvider (contiguous store) when available for cache-friendly
/// sequential access with pre-computed normalization flags. Falls back to
/// PropertyMap access otherwise (tests, standalone usage).
pub(crate) fn top_k_cosine_scan(
    graph: &SeleneGraph,
    node_iter: impl Iterator<Item = NodeId>,
    prop_key: IStr,
    query_vec: &[f32],
    k: usize,
    scope: Option<&roaring::RoaringBitmap>,
) -> Vec<ScoredNode> {
    let mut heap: BinaryHeap<ScoredNode> = BinaryHeap::with_capacity(k.min(1024) + 1);
    let query_is_unit = is_unit_vector(query_vec);

    // Try contiguous VectorProvider first (2-3x faster due to cache locality)
    if let Some(provider) = super::vector_provider::get_vector_provider() {
        // Collect node IDs respecting scope
        let ids: Vec<NodeId> = if let Some(scope_bm) = scope {
            node_iter
                .filter(|nid| scope_bm.contains(nid.0 as u32))
                .collect()
        } else {
            node_iter.collect()
        };

        // Try provider-side scoring first (int8 optimized path)
        let scored = provider.scan_with_scores(
            &mut ids.iter().copied(),
            &prop_key,
            query_vec,
            &mut |node_id, score| {
                heap.push(ScoredNode { node_id, score });
                if heap.len() > k {
                    heap.pop();
                }
            },
        );

        if !scored {
            // Fallback: scan raw f32 vectors and score in caller
            provider.scan_vectors(
                &mut ids.into_iter(),
                &prop_key,
                query_vec.len(),
                &mut |node_id, vec_data, is_normalized| {
                    let score = if query_is_unit && is_normalized {
                        dot_product(vec_data, query_vec)
                    } else {
                        cosine_similarity(vec_data, query_vec)
                    };
                    heap.push(ScoredNode { node_id, score });
                    if heap.len() > k {
                        heap.pop();
                    }
                },
            );
        }
    } else {
        // Fallback: read vectors from PropertyMap (scattered Arc<[f32]>)
        for node_id in node_iter {
            if let Some(scope_bm) = scope
                && !scope_bm.contains(node_id.0 as u32)
            {
                continue;
            }
            let Some(node) = graph.get_node(node_id) else {
                continue;
            };
            let Some(Value::Vector(vec_data)) = node.properties.get(prop_key) else {
                continue;
            };
            if vec_data.len() != query_vec.len() {
                continue;
            }

            // Fallback must check normalization per-vector (adds ~1 pass overhead)
            let score = if query_is_unit && is_unit_vector(vec_data) {
                dot_product(vec_data, query_vec)
            } else {
                cosine_similarity(vec_data, query_vec)
            };

            heap.push(ScoredNode { node_id, score });
            if heap.len() > k {
                heap.pop();
            }
        }
    }

    let mut results: Vec<ScoredNode> = heap.into_vec();
    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));
    results
}

// ── graph.semanticSearch ────────────────────────────────────────────

/// `CALL graph.semanticSearch('supply air temperature sensor', 10)`
/// `CALL graph.semanticSearch('supply air temperature sensor', 10, 'sensor')`
///
/// Combines embed() + vector search + containment path traversal.
/// Requires `--features vector` for the embed() call.
pub struct SemanticSearch;

impl Procedure for SemanticSearch {
    fn name(&self) -> &'static str {
        "graph.semanticSearch"
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
                // Optional third param: label filter
            ],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "score",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "path",
                    typ: GqlType::String,
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
        if args.len() < 2 {
            return Err(GqlError::InvalidArgument {
                message: "graph.semanticSearch requires at least 2 arguments: queryText, k".into(),
            });
        }

        let query_text = args[0].as_str()?;
        let k_raw = args[1].as_int()?;
        if k_raw < 0 {
            return Err(GqlError::InvalidArgument {
                message: "graph.semanticSearch: k must be non-negative".into(),
            });
        }
        let k = k_raw as usize;
        if k == 0 {
            return Ok(vec![]);
        }
        const MAX_K: usize = 10_000;
        if k > MAX_K {
            return Err(GqlError::InvalidArgument {
                message: format!("graph.semanticSearch: k must be <= {MAX_K}"),
            });
        }

        // Optional label filter (3rd argument)
        let label_filter = if args.len() > 2 {
            Some(args[2].as_str()?)
        } else {
            None
        };

        // 1. Embed the query text
        let query_vec = crate::runtime::embed::embed_text_with_task(
            query_text,
            crate::runtime::embed::EmbeddingTask::Retrieval,
        )?;

        // 2. Scan nodes with cosine similarity
        let prop_key = IStr::new("embedding");
        let results = if let Some(label) = label_filter {
            top_k_cosine_scan(
                graph,
                graph.nodes_by_label(label),
                prop_key,
                &query_vec,
                k,
                scope,
            )
        } else {
            top_k_cosine_scan(graph, graph.all_node_ids(), prop_key, &query_vec, k, scope)
        };

        // 3. For each result, walk up the containment hierarchy
        let name_key = IStr::new("name");
        Ok(results
            .into_iter()
            .map(|s| {
                let path_nodes = selene_graph::algorithms::containment::walk_ancestors(
                    graph,
                    s.node_id,
                    &["contains", "has_sensor", "supplies"],
                );
                let path_str = path_nodes
                    .iter()
                    .filter_map(|nid| {
                        // Hide names of out-of-scope ancestor nodes
                        if let Some(scope_bm) = scope
                            && !scope_bm.contains(nid.0 as u32)
                        {
                            return Some("[restricted]".to_string());
                        }
                        graph
                            .get_node(*nid)
                            .and_then(|n| n.properties.get(name_key).map(|v| format!("{v}")))
                    })
                    .collect::<Vec<_>>()
                    .join(" > ");

                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(s.node_id.0 as i64)),
                    (IStr::new("score"), GqlValue::Float(f64::from(s.score))),
                    (
                        IStr::new("path"),
                        GqlValue::String(smol_str::SmolStr::new(&path_str))
                    ),
                ]
            })
            .collect())
    }
}

// ── graph.similarNodes ──────────────────────────────────────────────

/// `CALL graph.similarNodes(42, 'embedding', 10)`
///
/// Find the k nodes most similar to a given node's vector property.
/// Automatically infers labels from the reference node.
pub struct SimilarNodes;

impl Procedure for SimilarNodes {
    fn name(&self) -> &'static str {
        "graph.similarNodes"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "node_id",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "k",
                    typ: GqlType::Int,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
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
                message: "graph.similarNodes requires 3 arguments: nodeId, property, k".into(),
            });
        }

        let ref_id = NodeId(args[0].as_int()? as u64);
        let property = args[1].as_str()?;
        let k_raw = args[2].as_int()?;
        if k_raw < 0 {
            return Err(GqlError::InvalidArgument {
                message: "graph.similarNodes: k must be non-negative".into(),
            });
        }
        let k = k_raw as usize;
        if k == 0 {
            return Ok(vec![]);
        }
        const MAX_K: usize = 10_000;
        if k > MAX_K {
            return Err(GqlError::InvalidArgument {
                message: format!("graph.similarNodes: k must be <= {MAX_K}"),
            });
        }

        // Get the reference node and its vector
        let ref_node = graph.get_node(ref_id).ok_or(GqlError::NotFound {
            entity: "node",
            id: ref_id.0,
        })?;

        let prop_key = IStr::new(property);
        let ref_vec = match ref_node.properties.get(prop_key) {
            Some(Value::Vector(v)) => v.as_ref(),
            Some(_) => {
                return Err(GqlError::InvalidArgument {
                    message: format!(
                        "node {} property '{property}' is not a vector; similarNodes requires a vector embedding property",
                        ref_id.0
                    ),
                });
            }
            None => {
                return Err(GqlError::InvalidArgument {
                    message: format!(
                        "node {} does not have property '{property}'; ensure the node has an embedding stored in this property",
                        ref_id.0
                    ),
                });
            }
        };

        // Infer label from reference node: search nodes with the same first label
        let label: Option<IStr> = ref_node.labels.iter().next();

        // Scan similar nodes (excluding the reference node itself)
        let node_iter: Box<dyn Iterator<Item = NodeId>> = if let Some(l) = label {
            Box::new(
                graph
                    .nodes_by_label(l.as_str())
                    .filter(move |nid| *nid != ref_id),
            )
        } else {
            Box::new(graph.all_node_ids().filter(move |nid| *nid != ref_id))
        };

        let results = top_k_cosine_scan(graph, node_iter, prop_key, ref_vec, k, scope);

        Ok(results
            .into_iter()
            .map(|s| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(s.node_id.0 as i64)),
                    (IStr::new("score"), GqlValue::Float(f64::from(s.score))),
                ]
            })
            .collect())
    }
}

// ── graph.scopedVectorSearch ────────────────────────────────────────

/// `CALL graph.scopedVectorSearch(1, 3, 'embedding', $queryVec, 10)`
///
/// Vector search restricted to the BFS neighborhood of a root node.
/// Steps: (1) BFS to collect candidate node IDs, (2) top-k cosine scan
/// over only those candidates. Huge win for localized queries like
/// "find similar sensors on this floor."
pub struct ScopedVectorSearch;

impl Procedure for ScopedVectorSearch {
    fn name(&self) -> &'static str {
        "graph.scopedVectorSearch"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "rootNodeId",
                    typ: GqlType::UInt,
                },
                ProcedureParam {
                    name: "maxHops",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "property",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "queryVector",
                    typ: GqlType::Vector,
                },
                ProcedureParam {
                    name: "k",
                    typ: GqlType::Int,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
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
        if args.len() < 5 {
            return Err(GqlError::InvalidArgument {
                message: "graph.scopedVectorSearch requires 5 arguments: \
                          rootNodeId, maxHops, property, queryVector, k"
                    .into(),
            });
        }

        let root_id = NodeId(match &args[0] {
            GqlValue::Int(id) if *id >= 0 => *id as u64,
            GqlValue::UInt(id) => *id,
            other => {
                return Err(GqlError::type_error(format!(
                    "scopedVectorSearch: rootNodeId must be INT, got {}",
                    other.gql_type()
                )));
            }
        });

        let max_hops = args[1].as_int()?;
        if max_hops <= 0 {
            return Err(GqlError::InvalidArgument {
                message: "scopedVectorSearch: maxHops must be > 0".into(),
            });
        }
        let max_hops = max_hops.min(20) as u32;

        let property = args[2].as_str()?;
        let query_vec = match &args[3] {
            GqlValue::Vector(v) => v.as_ref(),
            other => {
                return Err(GqlError::type_error(format!(
                    "scopedVectorSearch: queryVector must be VECTOR, got {}",
                    other.gql_type()
                )));
            }
        };

        let k = args[4].as_int()? as usize;
        if k == 0 {
            return Ok(vec![]);
        }
        const MAX_K: usize = 10_000;
        if k > MAX_K {
            return Err(GqlError::InvalidArgument {
                message: format!("scopedVectorSearch: k must be <= {MAX_K}"),
            });
        }

        // 1. BFS to collect neighborhood candidate node IDs
        let neighbors = selene_graph::algorithms::traversal::bfs(graph, root_id, None, max_hops);

        if neighbors.is_empty() {
            return Ok(vec![]);
        }

        // 2. Top-k cosine search over only the neighborhood candidates
        let prop_key = IStr::new(property);
        let results =
            top_k_cosine_scan(graph, neighbors.into_iter(), prop_key, query_vec, k, scope);

        Ok(results
            .into_iter()
            .map(|s| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(s.node_id.0 as i64)),
                    (IStr::new("score"), GqlValue::Float(f64::from(s.score))),
                ]
            })
            .collect())
    }
}

// ── graph.scopedSemanticSearch ──────────────────────────────────────

/// `CALL graph.scopedSemanticSearch(1, 3, 'find temperature anomalies', 10)`
///
/// Combines embed() + BFS-scoped vector search. Handles embedding
/// internally (unlike scopedVectorSearch which requires a pre-computed vector).
/// Steps: (1) embed query text, (2) BFS from root to collect candidates,
/// (3) top-k cosine scan over the neighborhood.
pub struct ScopedSemanticSearch;

impl Procedure for ScopedSemanticSearch {
    fn name(&self) -> &'static str {
        "graph.scopedSemanticSearch"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "rootNodeId",
                    typ: GqlType::UInt,
                },
                ProcedureParam {
                    name: "maxHops",
                    typ: GqlType::Int,
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
                    name: "node_id",
                    typ: GqlType::Int,
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
        if args.len() < 4 {
            return Err(GqlError::InvalidArgument {
                message: "graph.scopedSemanticSearch requires 4 arguments: \
                          rootNodeId, maxHops, queryText, k"
                    .into(),
            });
        }

        let root_id = NodeId(match &args[0] {
            GqlValue::Int(id) if *id >= 0 => *id as u64,
            GqlValue::UInt(id) => *id,
            other => {
                return Err(GqlError::type_error(format!(
                    "scopedSemanticSearch: rootNodeId must be INT, got {}",
                    other.gql_type()
                )));
            }
        });

        let max_hops = args[1].as_int()?;
        if max_hops <= 0 {
            return Err(GqlError::InvalidArgument {
                message: "scopedSemanticSearch: maxHops must be > 0".into(),
            });
        }
        let max_hops = max_hops.min(20) as u32;

        let query_text = args[2].as_str()?;

        let k = args[3].as_int()? as usize;
        if k == 0 {
            return Ok(vec![]);
        }
        const MAX_K: usize = 10_000;
        if k > MAX_K {
            return Err(GqlError::InvalidArgument {
                message: format!("scopedSemanticSearch: k must be <= {MAX_K}"),
            });
        }

        // 1. Embed the query text
        let query_vec = crate::runtime::embed::embed_text_with_task(
            query_text,
            crate::runtime::embed::EmbeddingTask::Retrieval,
        )?;

        // 2. BFS to collect neighborhood candidates
        let neighbors = selene_graph::algorithms::traversal::bfs(graph, root_id, None, max_hops);

        if neighbors.is_empty() {
            return Ok(vec![]);
        }

        // 3. Top-k cosine search over the neighborhood
        let prop_key = IStr::new("embedding");
        let results =
            top_k_cosine_scan(graph, neighbors.into_iter(), prop_key, &query_vec, k, scope);

        Ok(results
            .into_iter()
            .map(|s| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(s.node_id.0 as i64)),
                    (IStr::new("score"), GqlValue::Float(f64::from(s.score))),
                ]
            })
            .collect())
    }
}

// ── graph.rebuildVectorIndex ────────────────────────────────────────

/// `CALL graph.rebuildVectorIndex('embedding')`
///
/// Counts the nodes that carry a vector value for the given property.
/// The procedure signals how many nodes are indexable; the actual HNSW rebuild
/// is driven by the server background task (HT11) through `SharedGraph::write`.
///
/// Receiving `&SeleneGraph` (immutable) means this procedure cannot persist
/// a new index directly. Use it as a diagnostic tool to confirm vector
/// coverage before triggering an explicit server-side rebuild.
pub struct RebuildVectorIndex;

impl Procedure for RebuildVectorIndex {
    fn name(&self) -> &'static str {
        "graph.rebuildVectorIndex"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "property",
                typ: GqlType::String,
            }],
            yields: vec![YieldColumn {
                name: "indexed",
                typ: GqlType::Int,
            }],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _hot_tier: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        if args.is_empty() {
            return Err(GqlError::InvalidArgument {
                message: "graph.rebuildVectorIndex requires 1 argument: property".into(),
            });
        }

        let property = args[0].as_str()?;
        let prop_key = IStr::new(property);

        // Count nodes that carry a vector value for this property.
        let count = graph
            .all_node_ids()
            .filter(|&nid| {
                graph
                    .get_node(nid)
                    .and_then(|n| n.properties.get(prop_key))
                    .is_some_and(|v| matches!(v, Value::Vector(_)))
            })
            .count() as i64;

        Ok(vec![smallvec![(
            IStr::new("indexed"),
            GqlValue::Int(count)
        )]])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use selene_core::{LabelSet, PropertyMap};

    fn test_graph_with_vectors() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let prop = IStr::new("embedding");

        // 5 nodes with known vectors at different angles
        for i in 0..5u64 {
            let labels = LabelSet::from_strs(&["sensor"]);
            let angle = (i as f32) * std::f32::consts::FRAC_PI_4;
            let vec = vec![angle.cos(), angle.sin()];
            let mut props = PropertyMap::new();
            props.insert(prop, Value::Vector(Arc::from(vec)));
            let mut m = g.mutate();
            let nid = m.create_node(labels, props).unwrap();
            m.commit(0).unwrap();
            assert_eq!(nid.0, i + 1);
        }
        g
    }

    #[test]
    fn vector_search_top_k() {
        let g = test_graph_with_vectors();
        let query = vec![1.0f32, 0.0]; // angle = 0 degrees
        let args = vec![
            GqlValue::String("sensor".into()),
            GqlValue::String("embedding".into()),
            GqlValue::Vector(Arc::from(query)),
            GqlValue::Int(3),
        ];
        let rows = VectorSearch.execute(&args, &g, None, None).unwrap();
        assert_eq!(rows.len(), 3);
        // First result should be node 1 (exact match at 0 degrees)
        assert_eq!(rows[0][0].1, GqlValue::Int(1));
        if let GqlValue::Float(score) = &rows[0][1].1 {
            assert!(*score > 0.99);
        }
    }

    #[test]
    fn vector_search_empty_graph() {
        let g = SeleneGraph::new();
        let args = vec![
            GqlValue::String("sensor".into()),
            GqlValue::String("embedding".into()),
            GqlValue::Vector(Arc::from(vec![1.0f32, 0.0])),
            GqlValue::Int(5),
        ];
        let rows = VectorSearch.execute(&args, &g, None, None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn vector_search_k_zero() {
        let g = test_graph_with_vectors();
        let args = vec![
            GqlValue::String("sensor".into()),
            GqlValue::String("embedding".into()),
            GqlValue::Vector(Arc::from(vec![1.0f32, 0.0])),
            GqlValue::Int(0),
        ];
        let rows = VectorSearch.execute(&args, &g, None, None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn vector_search_k_larger_than_n() {
        let g = test_graph_with_vectors();
        let args = vec![
            GqlValue::String("sensor".into()),
            GqlValue::String("embedding".into()),
            GqlValue::Vector(Arc::from(vec![1.0f32, 0.0])),
            GqlValue::Int(100),
        ];
        let rows = VectorSearch.execute(&args, &g, None, None).unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn vector_search_skips_missing_property() {
        let mut g = SeleneGraph::new();
        // Node 1: no embedding
        {
            let mut m = g.mutate();
            m.create_node(LabelSet::from_strs(&["sensor"]), PropertyMap::new())
                .unwrap();
            m.commit(0).unwrap();
        }
        // Node 2: has embedding
        {
            let mut props = PropertyMap::new();
            props.insert(
                IStr::new("embedding"),
                Value::Vector(Arc::from(vec![1.0f32, 0.0])),
            );
            let mut m = g.mutate();
            m.create_node(LabelSet::from_strs(&["sensor"]), props)
                .unwrap();
            m.commit(0).unwrap();
        }

        let args = vec![
            GqlValue::String("sensor".into()),
            GqlValue::String("embedding".into()),
            GqlValue::Vector(Arc::from(vec![1.0f32, 0.0])),
            GqlValue::Int(10),
        ];
        let rows = VectorSearch.execute(&args, &g, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, GqlValue::Int(2));
    }

    // ── dot_product / is_unit_vector tests ────────────────────────

    #[test]
    fn dot_product_basic() {
        assert!((dot_product(&[1.0, 0.0], &[0.0, 1.0]) - 0.0).abs() < 1e-6);
        assert!((dot_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]) - 32.0).abs() < 1e-4);
    }

    #[test]
    fn dot_product_384_dim() {
        let a: Vec<f32> = (0..384).map(|i| (i as f32 * 0.01).sin()).collect();
        let b: Vec<f32> = (0..384).map(|i| (i as f32 * 0.01).cos()).collect();
        let naive: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
        let fast = dot_product(&a, &b);
        assert!((naive - fast).abs() < 1e-3, "naive={naive}, fast={fast}");
    }

    #[test]
    fn dot_product_remainder() {
        // 5 elements: 4 chunked + 1 remainder
        assert!(
            (dot_product(&[1.0, 2.0, 3.0, 4.0, 5.0], &[1.0, 1.0, 1.0, 1.0, 1.0]) - 15.0).abs()
                < 1e-6
        );
    }

    #[test]
    fn is_unit_vector_works() {
        let unit = vec![1.0f32, 0.0];
        assert!(is_unit_vector(&unit));
        let non_unit = vec![2.0f32, 0.0];
        assert!(!is_unit_vector(&non_unit));
        // L2-normalized 384-dim vector
        let mut v: Vec<f32> = (0..384).map(|i| (i as f32 * 0.01).sin()).collect();
        let mag = dot_product(&v, &v).sqrt();
        for x in &mut v {
            *x /= mag;
        }
        assert!(is_unit_vector(&v));
    }

    #[test]
    fn top_k_uses_dot_shortcut_for_normalized() {
        // Create graph with L2-normalized vectors
        let mut g = SeleneGraph::new();
        let prop = IStr::new("embedding");
        for i in 0..10u64 {
            let angle = (i as f32) * std::f32::consts::FRAC_PI_4 / 2.5;
            let vec = vec![angle.cos(), angle.sin()]; // Already unit-length
            let mut props = PropertyMap::new();
            props.insert(prop, Value::Vector(Arc::from(vec)));
            let mut m = g.mutate();
            m.create_node(LabelSet::from_strs(&["sensor"]), props)
                .unwrap();
            m.commit(0).unwrap();
        }

        let query = vec![1.0f32, 0.0]; // Unit vector
        let results = top_k_cosine_scan(&g, g.nodes_by_label("sensor"), prop, &query, 3, None);
        assert_eq!(results.len(), 3);
        // Node 1 (angle=0) should be best match
        assert_eq!(results[0].node_id.0, 1);
        assert!(results[0].score > 0.99);
    }

    #[test]
    fn top_k_handles_mixed_normalization() {
        // Mix of normalized and unnormalized vectors
        let mut g = SeleneGraph::new();
        let prop = IStr::new("embedding");
        // Node 1: unit vector [1, 0]
        {
            let mut props = PropertyMap::new();
            props.insert(prop, Value::Vector(Arc::from(vec![1.0f32, 0.0])));
            let mut m = g.mutate();
            m.create_node(LabelSet::from_strs(&["sensor"]), props)
                .unwrap();
            m.commit(0).unwrap();
        }
        // Node 2: NON-unit vector [2, 0] -- same direction, different magnitude
        {
            let mut props = PropertyMap::new();
            props.insert(prop, Value::Vector(Arc::from(vec![2.0f32, 0.0])));
            let mut m = g.mutate();
            m.create_node(LabelSet::from_strs(&["sensor"]), props)
                .unwrap();
            m.commit(0).unwrap();
        }

        let query = vec![1.0f32, 0.0];
        let results = top_k_cosine_scan(&g, g.nodes_by_label("sensor"), prop, &query, 2, None);
        assert_eq!(results.len(), 2);
        // Both should have score ≈ 1.0 (same direction)
        assert!(results[0].score > 0.99);
        assert!(results[1].score > 0.99);
    }

    #[test]
    fn cosine_similarity_matches_naive() {
        let a = vec![1.0f32, 2.0, 3.0];
        let b = vec![4.0f32, 5.0, 6.0];
        let result = cosine_similarity(&a, &b);
        // Known value: 32 / (sqrt(14) * sqrt(77)) ≈ 0.9746
        assert!((result - 0.9746).abs() < 0.001);
    }
}
