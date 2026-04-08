//! Schema introspection procedures: schema.nodeLabels, schema.edgeLabels, schema.nodeSchema.

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

/// CALL schema.nodeLabels() YIELD label, property_count
pub struct SchemaNodeLabels;

impl Procedure for SchemaNodeLabels {
    fn name(&self) -> &'static str {
        "schema.nodeLabels"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![
                YieldColumn {
                    name: "label",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "property_count",
                    typ: GqlType::Int,
                },
            ],
        }
    }

    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ts: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let mut schemas: Vec<_> = graph
            .schema()
            .all_node_schemas()
            .map(|ns| (ns.label.as_ref().to_owned(), ns.properties.len()))
            .collect();
        schemas.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(schemas
            .into_iter()
            .map(|(label, count)| {
                smallvec![
                    (IStr::new("label"), GqlValue::String(SmolStr::new(&label))),
                    (IStr::new("property_count"), GqlValue::Int(count as i64)),
                ]
            })
            .collect())
    }
}

/// CALL schema.edgeLabels() YIELD label, property_count
pub struct SchemaEdgeLabels;

impl Procedure for SchemaEdgeLabels {
    fn name(&self) -> &'static str {
        "schema.edgeLabels"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![
                YieldColumn {
                    name: "label",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "property_count",
                    typ: GqlType::Int,
                },
            ],
        }
    }

    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ts: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let mut schemas: Vec<_> = graph
            .schema()
            .all_edge_schemas()
            .map(|es| (es.label.as_ref().to_owned(), es.properties.len()))
            .collect();
        schemas.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(schemas
            .into_iter()
            .map(|(label, count)| {
                smallvec![
                    (IStr::new("label"), GqlValue::String(SmolStr::new(&label))),
                    (IStr::new("property_count"), GqlValue::Int(count as i64)),
                ]
            })
            .collect())
    }
}

/// CALL schema.nodeSchema(label) YIELD name, type, required, searchable, indexed, dictionary
pub struct SchemaNodeSchema;

impl Procedure for SchemaNodeSchema {
    fn name(&self) -> &'static str {
        "schema.nodeSchema"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "label",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "name",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "type",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "required",
                    typ: GqlType::Bool,
                },
                YieldColumn {
                    name: "searchable",
                    typ: GqlType::Bool,
                },
                YieldColumn {
                    name: "indexed",
                    typ: GqlType::Bool,
                },
                YieldColumn {
                    name: "dictionary",
                    typ: GqlType::Bool,
                },
            ],
        }
    }

    fn execute(
        &self,
        args: &[GqlValue],
        graph: &SeleneGraph,
        _ts: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let label = args
            .first()
            .ok_or_else(|| GqlError::type_error("schema.nodeSchema requires a label argument"))?
            .as_str()?;

        let ns = graph.schema().node_schema(label).ok_or_else(|| {
            GqlError::type_error(format!("no schema registered for label '{label}'"))
        })?;

        let mut rows = Vec::with_capacity(ns.properties.len());
        for prop in &ns.properties {
            let type_name = format!("{:?}", prop.value_type);
            rows.push(smallvec![
                (
                    IStr::new("name"),
                    GqlValue::String(SmolStr::new(prop.name.as_ref()))
                ),
                (
                    IStr::new("type"),
                    GqlValue::String(SmolStr::new(&type_name))
                ),
                (IStr::new("required"), GqlValue::Bool(prop.required)),
                (IStr::new("searchable"), GqlValue::Bool(prop.searchable)),
                (IStr::new("indexed"), GqlValue::Bool(prop.indexed)),
                (IStr::new("dictionary"), GqlValue::Bool(prop.dictionary)),
            ]);
        }
        Ok(rows)
    }
}
