//! Community detection algorithms: graph.labelPropagation, graph.louvain,
//! graph.triangleCount

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use super::{SharedCatalog, get_projection_or_build};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── graph.labelPropagation ──────────────────────────────────────────

pub struct GraphLabelPropagation {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphLabelPropagation {
    fn name(&self) -> &'static str {
        "graph.labelPropagation"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "projection",
                    typ: GqlType::String,
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
                    name: "community_id",
                    typ: GqlType::Int,
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
        let max_iter = args.get(1).and_then(|v| v.as_int().ok()).unwrap_or(10) as usize;

        let cat = self.catalog.read();
        let proj_ref = cat
            .get(&name)
            .ok_or_else(|| GqlError::internal("projection not found"))?;
        let result = selene_algorithms::label_propagation(proj_ref.projection(), max_iter);

        Ok(result
            .into_iter()
            .map(|(nid, cid)| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("community_id"), GqlValue::Int(cid as i64)),
                ]
            })
            .collect())
    }
}

// ── graph.louvain ───────────────────────────────────────────────────

pub struct GraphLouvain {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphLouvain {
    fn name(&self) -> &'static str {
        "graph.louvain"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "community_id",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "level",
                    typ: GqlType::Int,
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
        let cat = self.catalog.read();
        let proj_ref = cat
            .get(&name)
            .ok_or_else(|| GqlError::internal("projection not found"))?;
        let result = selene_algorithms::louvain(proj_ref.projection());

        Ok(result
            .into_iter()
            .map(|(nid, cid, level)| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("community_id"), GqlValue::Int(cid as i64)),
                    (IStr::new("level"), GqlValue::Int(i64::from(level))),
                ]
            })
            .collect())
    }
}

// ── graph.triangleCount ─────────────────────────────────────────────

pub struct GraphTriangleCount {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphTriangleCount {
    fn name(&self) -> &'static str {
        "graph.triangleCount"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "count",
                    typ: GqlType::Int,
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
        let cat = self.catalog.read();
        let proj_ref = cat
            .get(&name)
            .ok_or_else(|| GqlError::internal("projection not found"))?;
        let result = selene_algorithms::triangle_count(proj_ref.projection());

        Ok(result
            .into_iter()
            .map(|(nid, count)| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("count"), GqlValue::Int(count as i64)),
                ]
            })
            .collect())
    }
}
