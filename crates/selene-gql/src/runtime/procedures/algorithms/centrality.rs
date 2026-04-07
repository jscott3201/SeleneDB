//! Centrality algorithms: graph.pagerank, graph.betweenness

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use super::{SharedCatalog, get_projection_or_build};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── graph.pagerank ──────────────────────────────────────────────────

pub struct GraphPagerank {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphPagerank {
    fn name(&self) -> &'static str {
        "graph.pagerank"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "projection",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "damping",
                    typ: GqlType::Float,
                },
                ProcedureParam {
                    name: "maxIter",
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
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let name = get_projection_or_build(&self.catalog, args, graph)?;
        let damping = args.get(1).and_then(|v| v.as_float().ok()).unwrap_or(0.85);
        let max_iter = args.get(2).and_then(|v| v.as_int().ok()).unwrap_or(20) as usize;

        let cat = self.catalog.read();
        let proj_ref = cat
            .get(&name)
            .ok_or_else(|| GqlError::internal("projection not found"))?;
        let result = selene_algorithms::pagerank(proj_ref.projection(), damping, max_iter);

        Ok(result
            .into_iter()
            .map(|(nid, score)| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("score"), GqlValue::Float(score)),
                ]
            })
            .collect())
    }
}

// ── graph.betweenness ───────────────────────────────────────────────

pub struct GraphBetweenness {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphBetweenness {
    fn name(&self) -> &'static str {
        "graph.betweenness"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "projection",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "sampleSize",
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
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let name = get_projection_or_build(&self.catalog, args, graph)?;
        let sample_size = args
            .get(1)
            .and_then(|v| match v {
                GqlValue::Null => None,
                _ => v.as_int().ok(),
            })
            .map(|n| n as usize);

        let cat = self.catalog.read();
        let proj_ref = cat
            .get(&name)
            .ok_or_else(|| GqlError::internal("projection not found"))?;
        let result = selene_algorithms::betweenness(proj_ref.projection(), sample_size);

        Ok(result
            .into_iter()
            .map(|(nid, score)| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("score"), GqlValue::Float(score)),
                ]
            })
            .collect())
    }
}
