//! Projection management: graph.project, graph.drop, graph.listProjections

use selene_algorithms::ProjectionConfig;
use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use super::{SharedCatalog, extract_name, extract_optional_string_list};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

// ── graph.project ───────────────────────────────────────────────────

pub struct GraphProject {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphProject {
    fn name(&self) -> &'static str {
        "graph.project"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![
                ProcedureParam {
                    name: "name",
                    typ: GqlType::String,
                },
                ProcedureParam {
                    name: "nodeLabels",
                    typ: GqlType::List(Box::new(GqlType::String)),
                },
                ProcedureParam {
                    name: "edgeLabels",
                    typ: GqlType::List(Box::new(GqlType::String)),
                },
            ],
            yields: vec![
                YieldColumn {
                    name: "name",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "node_count",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "edge_count",
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
        let name = extract_name(args, 0, "graph.project")?;
        let node_labels = extract_optional_string_list(args, 1);
        let edge_labels = extract_optional_string_list(args, 2);
        let weight_property = args.get(3).and_then(|v| match v {
            GqlValue::Null => None,
            GqlValue::String(s) => Some(IStr::new(s.as_str())),
            _ => None,
        });

        let config = ProjectionConfig {
            name: name.clone(),
            node_labels,
            edge_labels,
            weight_property,
        };
        let (nc, ec) = self.catalog.write().project(graph, &config, None);

        Ok(vec![smallvec![
            (IStr::new("name"), GqlValue::String(SmolStr::new(&name))),
            (IStr::new("node_count"), GqlValue::Int(nc as i64)),
            (IStr::new("edge_count"), GqlValue::Int(ec as i64)),
        ]])
    }
}

// ── graph.drop ──────────────────────────────────────────────────────

pub struct GraphDrop {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphDrop {
    fn name(&self) -> &'static str {
        "graph.drop"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "name",
                typ: GqlType::String,
            }],
            yields: vec![YieldColumn {
                name: "dropped",
                typ: GqlType::Bool,
            }],
        }
    }
    fn execute(
        &self,
        args: &[GqlValue],
        _graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let name = extract_name(args, 0, "graph.drop")?;
        let dropped = self.catalog.write().drop_projection(&name);
        Ok(vec![smallvec![(
            IStr::new("dropped"),
            GqlValue::Bool(dropped)
        )]])
    }
}

// ── graph.listProjections ───────────────────────────────────────────

pub struct GraphListProjections {
    pub catalog: SharedCatalog,
}

impl Procedure for GraphListProjections {
    fn name(&self) -> &'static str {
        "graph.listProjections"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![
                YieldColumn {
                    name: "name",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "node_count",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "edge_count",
                    typ: GqlType::Int,
                },
            ],
        }
    }
    fn execute(
        &self,
        _args: &[GqlValue],
        _graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let list = self.catalog.read().list();
        Ok(list
            .into_iter()
            .map(|(name, nc, ec)| {
                smallvec![
                    (IStr::new("name"), GqlValue::String(SmolStr::new(&name))),
                    (IStr::new("node_count"), GqlValue::Int(nc as i64)),
                    (IStr::new("edge_count"), GqlValue::Int(ec as i64)),
                ]
            })
            .collect())
    }
}
