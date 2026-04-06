//! Community-enhanced RAG: combines vector similarity search with
//! Louvain community detection to provide graph-context-aware retrieval.
//!
//! `graph.communitySearch(queryText, k)` embeds the query, finds the k
//! nearest vector matches, detects their Louvain communities, and returns
//! results enriched with community ID, size, members, and label distribution.
//!
//! An optional `communityProp` parameter enables a read-through shortcut:
//! if nodes already have a pre-computed community property (from a prior
//! `CALL graph.louvain()` + `SET` workflow), the procedure reads that
//! property instead of running Louvain.

use std::collections::HashMap;
use std::sync::Arc;

use roaring::RoaringBitmap;
use selene_core::{IStr, NodeId, Value};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::algorithms::SharedCatalog;
use super::vector::top_k_cosine_scan;
use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlList, GqlType, GqlValue};

/// Maximum community members returned per result row.
const MAX_COMMUNITY_MEMBERS: usize = 50;

/// Community map pair: node-to-community and community-to-nodes.
type CommunityMaps = (HashMap<NodeId, u64>, HashMap<u64, Vec<NodeId>>);

/// Maximum k for vector search.
const MAX_K: usize = 10_000;

/// Internal projection name for the default full-graph Louvain projection.
const PROJ_NAME: &str = "__community_search_default";

// ---------------------------------------------------------------------------
// Procedure definition
// ---------------------------------------------------------------------------

/// Community-enhanced semantic search: vector similarity + Louvain context.
pub struct CommunitySearch {
    pub catalog: SharedCatalog,
}

impl Procedure for CommunitySearch {
    fn name(&self) -> &'static str {
        "graph.communitySearch"
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
                    name: "communityProp",
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
                    name: "communityId",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "communitySize",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "communityMembers",
                    typ: GqlType::List(Box::new(GqlType::UInt)),
                },
                YieldColumn {
                    name: "communityLabels",
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
        scope: Option<&RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        // ── 1. Validate arguments ──
        if args.len() < 2 {
            return Err(GqlError::InvalidArgument {
                message: "graph.communitySearch requires at least 2 arguments: queryText, k".into(),
            });
        }

        let query_text = args[0].as_str()?;
        let k_raw = args[1].as_int()?;
        if k_raw < 0 {
            return Err(GqlError::InvalidArgument {
                message: "graph.communitySearch: k must be non-negative".into(),
            });
        }
        let k = k_raw as usize;
        if k == 0 {
            return Ok(vec![]);
        }
        if k > MAX_K {
            return Err(GqlError::InvalidArgument {
                message: format!("graph.communitySearch: k must be <= {MAX_K}"),
            });
        }

        let community_prop = if args.len() > 2 && !args[2].is_null() {
            Some(args[2].as_str()?)
        } else {
            None
        };

        // ── 2. Embed query text ──
        let query_vec = crate::runtime::embed::embed_text_with_task(
            query_text,
            crate::runtime::embed::EmbeddingTask::Retrieval,
        )?;

        // ── 3. Vector search (HNSW fast path or brute-force) ──
        let prop_key = IStr::new("embedding");
        let top_results =
            top_k_cosine_scan(graph, graph.all_node_ids(), prop_key, &query_vec, k, scope);

        if top_results.is_empty() {
            return Ok(vec![]);
        }

        // ── 4. Community detection ──
        let (node_to_community, community_to_nodes) =
            build_community_maps(graph, &top_results, community_prop, &self.catalog, scope)?;

        // ── 5. Build result rows ──
        let rows = top_results
            .into_iter()
            .map(|scored| {
                let community_id = node_to_community.get(&scored.node_id).copied().unwrap_or(0);

                let members = community_to_nodes
                    .get(&community_id)
                    .map_or(&[][..], |v| v.as_slice());

                let community_size = members.len() as i64;

                // Cap members list and filter by scope
                let member_values: Vec<GqlValue> = members
                    .iter()
                    .filter(|nid| scope.is_none_or(|s| s.contains(nid.0 as u32)))
                    .take(MAX_COMMUNITY_MEMBERS)
                    .map(|nid| GqlValue::UInt(nid.0))
                    .collect();

                let labels_str = community_label_summary(graph, members, scope);

                smallvec![
                    (IStr::new("nodeId"), GqlValue::UInt(scored.node_id.0)),
                    (IStr::new("score"), GqlValue::Float(f64::from(scored.score))),
                    (IStr::new("communityId"), GqlValue::Int(community_id as i64)),
                    (IStr::new("communitySize"), GqlValue::Int(community_size)),
                    (
                        IStr::new("communityMembers"),
                        GqlValue::List(GqlList {
                            element_type: GqlType::UInt,
                            elements: Arc::from(member_values),
                        }),
                    ),
                    (
                        IStr::new("communityLabels"),
                        GqlValue::String(smol_str::SmolStr::new(labels_str)),
                    ),
                ]
            })
            .collect();

        Ok(rows)
    }
}

// ---------------------------------------------------------------------------
// Community detection helpers
// ---------------------------------------------------------------------------

/// Build community maps, using either a pre-computed property or Louvain.
///
/// Returns (node_to_community, community_to_nodes) maps covering all nodes
/// in the graph (not just the vector search results), so that community
/// member lists are complete.
fn build_community_maps(
    graph: &SeleneGraph,
    top_results: &[super::vector::ScoredNode],
    community_prop: Option<&str>,
    catalog: &SharedCatalog,
    _scope: Option<&RoaringBitmap>,
) -> Result<CommunityMaps, GqlError> {
    // Try property read-through if communityProp was specified
    if let Some(prop_name) = community_prop {
        let key = IStr::new(prop_name);
        let mut node_to_community = HashMap::new();
        let mut all_present = true;

        for scored in top_results {
            if let Some(node) = graph.get_node(scored.node_id) {
                match node.properties.get(key) {
                    Some(Value::Int(c)) => {
                        node_to_community.insert(scored.node_id, *c as u64);
                    }
                    Some(Value::UInt(c)) => {
                        node_to_community.insert(scored.node_id, *c);
                    }
                    _ => {
                        all_present = false;
                        break;
                    }
                }
            } else {
                all_present = false;
                break;
            }
        }

        if all_present {
            // Build community_to_nodes from the full graph for complete member lists
            let mut community_to_nodes: HashMap<u64, Vec<NodeId>> = HashMap::new();
            for nid in graph.all_node_ids() {
                if let Some(node) = graph.get_node(nid) {
                    if let Some(Value::Int(c)) = node.properties.get(key) {
                        community_to_nodes.entry(*c as u64).or_default().push(nid);
                    } else if let Some(Value::UInt(c)) = node.properties.get(key) {
                        community_to_nodes.entry(*c).or_default().push(nid);
                    }
                }
            }
            return Ok((node_to_community, community_to_nodes));
        }
        // Fall through to Louvain if any result node was missing the property
    }

    // Run Louvain community detection
    run_louvain_community_maps(graph, catalog)
}

/// Run Louvain via the shared projection catalog and build community maps.
fn run_louvain_community_maps(
    graph: &SeleneGraph,
    catalog: &SharedCatalog,
) -> Result<CommunityMaps, GqlError> {
    use selene_algorithms::ProjectionConfig;

    // Build or retrieve a full-graph projection (check-and-insert under
    // write lock to avoid concurrent threads both rebuilding the projection).
    {
        let cat = catalog.write();
        if !cat.contains(PROJ_NAME) {
            let config = ProjectionConfig {
                name: PROJ_NAME.to_string(),
                node_labels: vec![],
                edge_labels: vec![],
                weight_property: None,
            };
            cat.project(graph, &config, None);
        }
    }

    let cat = catalog.read();
    let proj_ref = cat
        .get(PROJ_NAME)
        .ok_or_else(|| GqlError::internal("failed to build projection for community search"))?;

    let louvain_result = selene_algorithms::louvain(proj_ref.projection());

    let mut node_to_community = HashMap::with_capacity(louvain_result.len());
    let mut community_to_nodes: HashMap<u64, Vec<NodeId>> = HashMap::new();

    for (nid, cid, _level) in louvain_result {
        node_to_community.insert(nid, cid);
        community_to_nodes.entry(cid).or_default().push(nid);
    }

    Ok((node_to_community, community_to_nodes))
}

// ---------------------------------------------------------------------------
// Label summary
// ---------------------------------------------------------------------------

/// Build a label distribution summary for a community's members.
///
/// Returns a string like `"Sensor:12,Zone:5,Floor:3"` with labels sorted
/// by frequency descending. Respects scope by skipping out-of-scope nodes.
fn community_label_summary(
    graph: &SeleneGraph,
    members: &[NodeId],
    scope: Option<&RoaringBitmap>,
) -> String {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for &nid in members {
        if let Some(s) = scope
            && !s.contains(nid.0 as u32)
        {
            continue;
        }
        if let Some(node) = graph.get_node(nid) {
            for label in node.labels.iter() {
                *counts.entry(label.as_str()).or_insert(0) += 1;
            }
        }
    }
    let mut pairs: Vec<_> = counts.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    pairs
        .iter()
        .map(|(l, c)| format!("{l}:{c}"))
        .collect::<Vec<_>>()
        .join(",")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_community_label_summary_basic() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            selene_core::LabelSet::from_strs(&["Sensor"]),
            selene_core::PropertyMap::new(),
        )
        .unwrap();
        m.create_node(
            selene_core::LabelSet::from_strs(&["Sensor"]),
            selene_core::PropertyMap::new(),
        )
        .unwrap();
        m.create_node(
            selene_core::LabelSet::from_strs(&["Zone"]),
            selene_core::PropertyMap::new(),
        )
        .unwrap();
        m.commit(0).unwrap();

        let members = vec![NodeId(1), NodeId(2), NodeId(3)];
        let summary = community_label_summary(&g, &members, None);
        assert_eq!(summary, "Sensor:2,Zone:1");
    }

    #[test]
    fn test_community_label_summary_empty() {
        let g = SeleneGraph::new();
        let summary = community_label_summary(&g, &[], None);
        assert_eq!(summary, "");
    }

    #[test]
    fn test_community_label_summary_with_scope() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            selene_core::LabelSet::from_strs(&["Sensor"]),
            selene_core::PropertyMap::new(),
        )
        .unwrap();
        m.create_node(
            selene_core::LabelSet::from_strs(&["Zone"]),
            selene_core::PropertyMap::new(),
        )
        .unwrap();
        m.commit(0).unwrap();

        // Scope only includes node 1
        let mut scope = RoaringBitmap::new();
        scope.insert(1);
        let members = vec![NodeId(1), NodeId(2)];
        let summary = community_label_summary(&g, &members, Some(&scope));
        assert_eq!(summary, "Sensor:1");
    }
}
