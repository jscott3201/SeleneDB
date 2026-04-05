//! Schema dump procedure for LLM-friendly schema output.
//!
//! `graph.schemaDump(includeSystem?)` produces a compact text blob
//! summarizing all registered node and edge schemas, node/edge counts,
//! and valid edge connections. Optimized for minimal token usage in
//! LLM context windows.
//!
//! By default, labels starting with `__` (system labels) are excluded.
//! Pass `true` as the first argument to include them.

use std::fmt::Write;

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

pub struct SchemaDump;

impl Procedure for SchemaDump {
    fn name(&self) -> &'static str {
        "graph.schemaDump"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "includeSystem",
                typ: GqlType::Bool,
            }],
            yields: vec![YieldColumn {
                name: "schema",
                typ: GqlType::String,
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
        let include_system = match args.first() {
            Some(GqlValue::Bool(b)) => *b,
            Some(_) => {
                return Err(GqlError::InvalidArgument {
                    message: "graph.schemaDump optional argument must be a boolean".into(),
                });
            }
            None => false,
        };

        let mut out = String::new();

        // Collect and sort node schemas for deterministic output.
        let mut node_schemas: Vec<_> = graph
            .schema()
            .all_node_schemas()
            .filter(|s| include_system || !s.label.starts_with("__"))
            .collect();
        node_schemas.sort_by(|a, b| a.label.cmp(&b.label));

        // Collect and sort edge schemas for deterministic output.
        let mut edge_schemas: Vec<_> = graph
            .schema()
            .all_edge_schemas()
            .filter(|s| include_system || !s.label.starts_with("__"))
            .collect();
        edge_schemas.sort_by(|a, b| a.label.cmp(&b.label));

        // Node types section.
        if !node_schemas.is_empty() {
            out.push_str("# Node types\n");
            for ns in &node_schemas {
                let _ = write!(out, ":{}", ns.label);
                if !ns.description.is_empty() {
                    let _ = write!(out, " -- {}", ns.description);
                }
                out.push('\n');

                for p in &ns.properties {
                    let _ = write!(out, "  .{}: {:?}", p.name, p.value_type);
                    if p.required {
                        out.push_str(" REQUIRED");
                    }
                    if p.unique {
                        out.push_str(" UNIQUE");
                    }
                    if p.indexed {
                        out.push_str(" INDEXED");
                    }
                    out.push('\n');
                }

                if !ns.valid_edge_labels.is_empty() {
                    let edges: Vec<&str> =
                        ns.valid_edge_labels.iter().map(|e| e.as_ref()).collect();
                    let _ = writeln!(out, "  edges: {}", edges.join(", "));
                }
            }
        }

        // Edge types section.
        if !edge_schemas.is_empty() {
            if !node_schemas.is_empty() {
                out.push('\n');
            }
            out.push_str("# Edge types\n");
            for es in &edge_schemas {
                let _ = write!(out, ":{}", es.label);

                if !es.source_labels.is_empty() {
                    let srcs: Vec<&str> = es.source_labels.iter().map(|l| l.as_ref()).collect();
                    let _ = write!(out, " FROM {}", srcs.join("|"));
                }
                if !es.target_labels.is_empty() {
                    let tgts: Vec<&str> = es.target_labels.iter().map(|l| l.as_ref()).collect();
                    let _ = write!(out, " TO {}", tgts.join("|"));
                }

                if !es.description.is_empty() {
                    let _ = write!(out, " -- {}", es.description);
                }
                out.push('\n');

                for p in &es.properties {
                    let _ = write!(out, "  .{}: {:?}", p.name, p.value_type);
                    if p.required {
                        out.push_str(" REQUIRED");
                    }
                    out.push('\n');
                }
            }
        }

        // Stats section.
        if !node_schemas.is_empty() || !edge_schemas.is_empty() {
            out.push('\n');
        }
        let _ = writeln!(out, "# Stats");
        let _ = writeln!(out, "nodes: {}", graph.node_count());
        let _ = writeln!(out, "edges: {}", graph.edge_count());

        let row: ProcedureRow = smallvec![(
            IStr::new("schema"),
            GqlValue::String(SmolStr::new(out.trim()))
        )];
        Ok(vec![row])
    }
}

#[cfg(test)]
mod tests {
    use selene_core::schema::{EdgeSchema, NodeSchema, PropertyDef, ValueType};
    use selene_graph::SeleneGraph;

    use super::*;

    fn graph_with_schemas() -> SeleneGraph {
        let mut g = SeleneGraph::new();

        let sensor_schema = NodeSchema::builder("sensor")
            .property(
                PropertyDef::builder("name", ValueType::String)
                    .required(true)
                    .build(),
            )
            .property(PropertyDef::simple("temp", ValueType::Float, false))
            .valid_edge("monitors")
            .description("A temperature sensor")
            .build();
        g.schema_mut().register_node_schema(sensor_schema).unwrap();

        let contains_schema = EdgeSchema::builder("contains")
            .property(PropertyDef::simple("since", ValueType::Int, false))
            .source_label("building")
            .source_label("floor")
            .target_label("floor")
            .target_label("sensor")
            .description("Containment hierarchy")
            .build();
        g.schema_mut()
            .register_edge_schema(contains_schema)
            .unwrap();

        g
    }

    #[test]
    fn schema_dump_includes_node_types() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        let schema_text = rows[0][0].1.as_str().unwrap();

        assert!(schema_text.contains(":sensor"), "should contain node label");
        assert!(
            schema_text.contains(".name: String REQUIRED"),
            "should contain required property"
        );
        assert!(
            schema_text.contains(".temp: Float"),
            "should contain optional property"
        );
        assert!(
            schema_text.contains("A temperature sensor"),
            "should contain description"
        );
        assert!(
            schema_text.contains("edges: monitors"),
            "should contain valid edges"
        );
    }

    #[test]
    fn schema_dump_includes_edge_types() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        let schema_text = rows[0][0].1.as_str().unwrap();

        assert!(
            schema_text.contains(":contains"),
            "should contain edge label"
        );
        assert!(
            schema_text.contains("FROM building|floor"),
            "should contain source labels"
        );
        assert!(
            schema_text.contains("TO floor|sensor"),
            "should contain target labels"
        );
        assert!(
            schema_text.contains("Containment hierarchy"),
            "should contain edge description"
        );
        assert!(
            schema_text.contains(".since: Int"),
            "should contain edge property"
        );
    }

    #[test]
    fn schema_dump_excludes_system_labels() {
        let mut g = graph_with_schemas();

        let system_schema = NodeSchema::builder("__Memory")
            .property(PropertyDef::simple("content", ValueType::String, true))
            .description("System memory node")
            .build();
        g.schema_mut().register_node_schema(system_schema).unwrap();

        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        let schema_text = rows[0][0].1.as_str().unwrap();

        assert!(
            !schema_text.contains("__Memory"),
            "system labels should be excluded by default"
        );
        assert!(
            schema_text.contains(":sensor"),
            "non-system labels should remain"
        );
    }

    #[test]
    fn schema_dump_includes_system_when_requested() {
        let mut g = graph_with_schemas();

        let system_schema = NodeSchema::builder("__Memory")
            .property(PropertyDef::simple("content", ValueType::String, true))
            .description("System memory node")
            .build();
        g.schema_mut().register_node_schema(system_schema).unwrap();

        let proc = SchemaDump;
        let args = vec![GqlValue::Bool(true)];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        let schema_text = rows[0][0].1.as_str().unwrap();

        assert!(
            schema_text.contains("__Memory"),
            "system labels should be included when requested"
        );
        assert!(
            schema_text.contains(":sensor"),
            "non-system labels should also be present"
        );
    }

    #[test]
    fn schema_dump_empty_schema() {
        let g = SeleneGraph::new();
        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        let schema_text = rows[0][0].1.as_str().unwrap();

        assert!(
            schema_text.contains("# Stats"),
            "should contain stats section"
        );
        assert!(schema_text.contains("nodes: 0"), "should show zero nodes");
        assert!(schema_text.contains("edges: 0"), "should show zero edges");
        assert!(
            !schema_text.contains("# Node types"),
            "should not contain node types header when empty"
        );
        assert!(
            !schema_text.contains("# Edge types"),
            "should not contain edge types header when empty"
        );
    }
}
