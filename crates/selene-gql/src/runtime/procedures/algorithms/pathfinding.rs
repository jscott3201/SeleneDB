//! Pathfinding algorithms: graph.shortestPath, graph.sssp, graph.apsp

use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;

use super::super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use super::{SharedCatalog, get_projection_or_build};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── graph.shortestPath ──────────────────────────────────────────────

pub struct GraphShortestPath {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphShortestPath {
    fn name(&self) -> &'static str {
        "graph.shortestPath"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "projection",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "from",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "to",
                    typ: GqlType::Int,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "cost",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "index",
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
        let from = args
            .get(1)
            .ok_or_else(|| GqlError::InvalidArgument {
                message: "graph.shortestPath requires 'from' node id".into(),
            })?
            .as_int()?;
        let to = args
            .get(2)
            .ok_or_else(|| GqlError::InvalidArgument {
                message: "graph.shortestPath requires 'to' node id".into(),
            })?
            .as_int()?;

        let cat = self.catalog.read();
        let proj_ref = cat
            .get(&name)
            .ok_or_else(|| GqlError::internal("projection not found"))?;

        match selene_algorithms::dijkstra(
            proj_ref.projection(),
            NodeId(from as u64),
            NodeId(to as u64),
        ) {
            Some(result) => Ok(result
                .nodes
                .iter()
                .enumerate()
                .map(|(idx, &nid)| {
                    smallvec![
                        (IStr::new("node_id"), GqlValue::Int(nid.0 as i64)),
                        (IStr::new("cost"), GqlValue::Float(result.cost)),
                        (IStr::new("index"), GqlValue::Int(idx as i64)),
                    ]
                })
                .collect()),
            None => Ok(vec![]),
        }
    }
}

// ── graph.sssp ──────────────────────────────────────────────────────

pub struct GraphSssp {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphSssp {
    fn name(&self) -> &'static str {
        "graph.sssp"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "projection",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "source",
                    typ: GqlType::Int,
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "node_id",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "distance",
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
        let source = args
            .get(1)
            .ok_or_else(|| GqlError::InvalidArgument {
                message: "graph.sssp requires source node id".into(),
            })?
            .as_int()?;

        let cat = self.catalog.read();
        let proj_ref = cat
            .get(&name)
            .ok_or_else(|| GqlError::internal("projection not found"))?;
        let result = selene_algorithms::sssp(proj_ref.projection(), NodeId(source as u64));

        Ok(result
            .into_iter()
            .map(|(nid, dist)| {
                smallvec![
                    (IStr::new("node_id"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("distance"), GqlValue::Float(dist)),
                ]
            })
            .collect())
    }
}

// ── graph.apsp ──────────────────────────────────────────────────────

pub struct GraphApsp {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphApsp {
    fn name(&self) -> &'static str {
        "graph.apsp"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "source",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "target",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "distance",
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
        let cat = self.catalog.read();
        let proj_ref = cat
            .get(&name)
            .ok_or_else(|| GqlError::internal("projection not found"))?;

        match selene_algorithms::apsp(proj_ref.projection(), 5000) {
            Ok(result) => Ok(result
                .into_iter()
                .map(|(src, tgt, dist)| {
                    smallvec![
                        (IStr::new("source"), GqlValue::Int(src.0 as i64)),
                        (IStr::new("target"), GqlValue::Int(tgt.0 as i64)),
                        (IStr::new("distance"), GqlValue::Float(dist)),
                    ]
                })
                .collect()),
            Err(e) => Err(GqlError::internal(e.to_string())),
        }
    }
}
