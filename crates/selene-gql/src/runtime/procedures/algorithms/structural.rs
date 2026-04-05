//! Structural algorithms: graph.wcc, graph.scc, graph.topoSort,
//! graph.articulationPoints, graph.bridges, graph.validate, graph.isAncestor

use selene_algorithms::{
    ContainmentIndex, articulation_points, bridges, scc, topological_sort, validate, wcc,
};
use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use super::{SharedCatalog, get_projection_or_build};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── graph.wcc ───────────────────────────────────────────────────────

pub struct GraphWcc {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphWcc {
    fn name(&self) -> &'static str {
        "graph.wcc"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "nodeId",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "componentId",
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
        let result = wcc(proj_ref.projection());
        Ok(result
            .into_iter()
            .map(|(nid, cid)| {
                smallvec![
                    (IStr::new("nodeId"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("componentId"), GqlValue::Int(cid as i64)),
                ]
            })
            .collect())
    }
}

// ── graph.scc ───────────────────────────────────────────────────────

pub struct GraphScc {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphScc {
    fn name(&self) -> &'static str {
        "graph.scc"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "nodeId",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "componentId",
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
        let result = scc(proj_ref.projection());
        Ok(result
            .into_iter()
            .map(|(nid, cid)| {
                smallvec![
                    (IStr::new("nodeId"), GqlValue::Int(nid.0 as i64)),
                    (IStr::new("componentId"), GqlValue::Int(cid as i64)),
                ]
            })
            .collect())
    }
}

// ── graph.topoSort ──────────────────────────────────────────────────

pub struct GraphTopoSort {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphTopoSort {
    fn name(&self) -> &'static str {
        "graph.topoSort"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "nodeId",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "position",
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

        match topological_sort(proj_ref.projection()) {
            Ok(result) => Ok(result
                .into_iter()
                .map(|(nid, pos)| {
                    smallvec![
                        (IStr::new("nodeId"), GqlValue::Int(nid.0 as i64)),
                        (IStr::new("position"), GqlValue::Int(pos as i64)),
                    ]
                })
                .collect()),
            Err(e) => Err(GqlError::internal(e.to_string())),
        }
    }
}

// ── graph.articulationPoints ────────────────────────────────────────

pub struct GraphArticulationPoints {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphArticulationPoints {
    fn name(&self) -> &'static str {
        "graph.articulationPoints"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![YieldColumn {
                name: "nodeId",
                typ: GqlType::Int,
            }],
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
        let result = articulation_points(proj_ref.projection());
        Ok(result
            .into_iter()
            .map(|nid| smallvec![(IStr::new("nodeId"), GqlValue::Int(nid.0 as i64))])
            .collect())
    }
}

// ── graph.bridges ───────────────────────────────────────────────────

pub struct GraphBridges {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphBridges {
    fn name(&self) -> &'static str {
        "graph.bridges"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "sourceId",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "targetId",
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
        let result = bridges(proj_ref.projection());
        Ok(result
            .into_iter()
            .map(|(src, tgt)| {
                smallvec![
                    (IStr::new("sourceId"), GqlValue::Int(src.0 as i64)),
                    (IStr::new("targetId"), GqlValue::Int(tgt.0 as i64)),
                ]
            })
            .collect())
    }
}

// ── graph.validate ──────────────────────────────────────────────────

pub struct GraphValidate {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphValidate {
    fn name(&self) -> &'static str {
        "graph.validate"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "projection",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "severity",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "issue",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "nodeId",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "message",
                    typ: GqlType::String,
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
        let issues = validate(proj_ref.projection());
        Ok(issues
            .into_iter()
            .map(|issue| {
                smallvec![
                    (
                        IStr::new("severity"),
                        GqlValue::String(SmolStr::new(issue.severity.to_string()))
                    ),
                    (
                        IStr::new("issue"),
                        GqlValue::String(SmolStr::new(&issue.issue_type))
                    ),
                    (
                        IStr::new("nodeId"),
                        issue
                            .node_id
                            .map_or(GqlValue::Null, |n| GqlValue::Int(n.0 as i64))
                    ),
                    (
                        IStr::new("message"),
                        GqlValue::String(SmolStr::new(&issue.message))
                    ),
                ]
            })
            .collect())
    }
}

// ── graph.isAncestor ────────────────────────────────────────────────

pub struct GraphIsAncestor;

impl Procedure for GraphIsAncestor {
    fn name(&self) -> &'static str {
        "graph.isAncestor"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "ancestor",
                    typ: GqlType::Int,
                },
                ProcedureParam {
                    name: "descendant",
                    typ: GqlType::Int,
                },
            ],
            yields: vec![YieldColumn {
                name: "result",
                typ: GqlType::Bool,
            }],
        }
    }
    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let ancestor = args
            .first()
            .ok_or_else(|| GqlError::InvalidArgument {
                message: "graph.isAncestor requires ancestor id".into(),
            })?
            .as_int()?;
        let descendant = args
            .get(1)
            .ok_or_else(|| GqlError::InvalidArgument {
                message: "graph.isAncestor requires descendant id".into(),
            })?
            .as_int()?;

        let idx = ContainmentIndex::build(graph);
        let result = idx.is_ancestor(NodeId(ancestor as u64), NodeId(descendant as u64));

        Ok(vec![smallvec![(
            IStr::new("result"),
            GqlValue::Bool(result)
        )]])
    }
}
