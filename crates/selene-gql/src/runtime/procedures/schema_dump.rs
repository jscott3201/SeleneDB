//! Schema dump procedure for LLM-friendly schema output.
//!
//! `graph.schemaDump(includeSystem?, compact?, label?)` produces a text blob
//! summarizing registered node and edge schemas, node/edge counts,
//! and valid edge connections. Optimized for minimal token usage in
//! LLM context windows.
//!
//! Arguments (all optional, positional):
//!   - `includeSystem` (bool, default false): include `__`-prefixed system schemas
//!   - `compact` (bool, default true): compact mode shows type names with property
//!     counts and required markers only; full mode includes descriptions and all
//!     property details
//!   - `label` (string): filter to a single type by label (returns full detail
//!     regardless of compact flag)

use std::fmt::Write;

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::{Procedure, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

pub struct SchemaDump;

impl Procedure for SchemaDump {
    fn name(&self) -> &'static str {
        "graph.schemaDump"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            // All arguments are optional and parsed positionally in `execute()`.
            // Keep the required signature arity at 0 so existing calls like
            // `CALL graph.schemaDump()` and `CALL graph.schemaDump(false)`
            // remain valid.
            params: vec![],
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
        let include_system = parse_bool_arg(args, 0, "includeSystem")?;
        let compact = parse_bool_arg(args, 1, "compact")?.unwrap_or(true);
        let label_filter = parse_string_arg(args, 2, "label")?;

        let include_system = include_system.unwrap_or(false);

        // When filtering by label, always return full detail.
        if let Some(label) = &label_filter {
            return dump_single_label(graph, label, include_system);
        }

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

        if compact {
            format_compact(&mut out, &node_schemas, &edge_schemas, graph);
        } else {
            format_full(&mut out, &node_schemas, &edge_schemas, graph);
        }

        let row: ProcedureRow = smallvec![(
            IStr::new("schema"),
            GqlValue::String(SmolStr::new(out.trim()))
        )];
        Ok(vec![row])
    }
}

/// Parse an optional bool argument at the given position.
fn parse_bool_arg(args: &[GqlValue], idx: usize, name: &str) -> Result<Option<bool>, GqlError> {
    match args.get(idx) {
        Some(GqlValue::Bool(b)) => Ok(Some(*b)),
        Some(GqlValue::Null) | None => Ok(None),
        Some(_) => Err(GqlError::InvalidArgument {
            message: format!("graph.schemaDump `{name}` argument must be a boolean"),
        }),
    }
}

/// Parse an optional string argument at the given position.
fn parse_string_arg(args: &[GqlValue], idx: usize, name: &str) -> Result<Option<String>, GqlError> {
    match args.get(idx) {
        Some(GqlValue::String(s)) => Ok(Some(s.to_string())),
        Some(GqlValue::Null) | None => Ok(None),
        Some(_) => Err(GqlError::InvalidArgument {
            message: format!("graph.schemaDump `{name}` argument must be a string"),
        }),
    }
}

/// Dump full detail for a single label (tries node schemas first, then edge).
fn dump_single_label(
    graph: &SeleneGraph,
    label: &str,
    include_system: bool,
) -> Result<Vec<ProcedureRow>, GqlError> {
    if !include_system && label.starts_with("__") {
        return Err(GqlError::InvalidArgument {
            message: "System schemas (__ prefix) require includeSystem = true".into(),
        });
    }

    let mut out = String::new();

    // Try node schema first.
    if let Some(ns) = graph
        .schema()
        .all_node_schemas()
        .find(|s| s.label.as_ref() == label)
    {
        let _ = write!(out, ":{}  [node type]", ns.label);
        if !ns.description.is_empty() {
            let _ = write!(out, "\n  {}", ns.description);
        }
        out.push('\n');
        format_node_properties(&mut out, ns);

        if !ns.valid_edge_labels.is_empty() {
            let edges: Vec<&str> = ns.valid_edge_labels.iter().map(|e| e.as_ref()).collect();
            let _ = writeln!(out, "  edges: {}", edges.join(", "));
        }

        // Show edges that connect to/from this type.
        let mut related_edges = Vec::new();
        for es in graph.schema().all_edge_schemas() {
            let is_source = es.source_labels.iter().any(|l| l.as_ref() == label);
            let is_target = es.target_labels.iter().any(|l| l.as_ref() == label);
            if is_source || is_target {
                related_edges.push((es.label.clone(), is_source, is_target));
            }
        }
        if !related_edges.is_empty() {
            out.push_str("  connected edges:\n");
            for (elabel, is_src, is_tgt) in &related_edges {
                let dir = match (is_src, is_tgt) {
                    (true, true) => "↔",
                    (true, false) => "→",
                    (false, true) => "←",
                    _ => unreachable!(),
                };
                let _ = writeln!(out, "    {dir} :{elabel}");
            }
        }
    } else if let Some(es) = graph
        .schema()
        .all_edge_schemas()
        .find(|s| s.label.as_ref() == label)
    {
        // Edge schema detail.
        let _ = write!(out, ":{}  [edge type]", es.label);
        if !es.source_labels.is_empty() {
            let srcs: Vec<&str> = es.source_labels.iter().map(|l| l.as_ref()).collect();
            let _ = write!(out, "\n  from: {}", srcs.join(" | "));
        }
        if !es.target_labels.is_empty() {
            let tgts: Vec<&str> = es.target_labels.iter().map(|l| l.as_ref()).collect();
            let _ = write!(out, "\n  to: {}", tgts.join(" | "));
        }
        if !es.description.is_empty() {
            let _ = write!(out, "\n  {}", es.description);
        }
        out.push('\n');
        for p in &es.properties {
            let _ = write!(out, "  .{}: {:?}", p.name, p.value_type);
            if p.required {
                out.push_str(" REQUIRED");
            }
            out.push('\n');
        }
    } else {
        return Err(GqlError::InvalidArgument {
            message: format!("No schema found for label '{label}'"),
        });
    }

    let row: ProcedureRow = smallvec![(
        IStr::new("schema"),
        GqlValue::String(SmolStr::new(out.trim()))
    )];
    Ok(vec![row])
}

/// Compact format: type names with property counts and required markers.
fn format_compact(
    out: &mut String,
    node_schemas: &[&selene_core::schema::NodeSchema],
    edge_schemas: &[&selene_core::schema::EdgeSchema],
    graph: &SeleneGraph,
) {
    if !node_schemas.is_empty() {
        let _ = writeln!(out, "# Node types ({})", node_schemas.len());
        for ns in node_schemas {
            let total = ns.properties.len();
            let required = ns.properties.iter().filter(|p| p.required).count();
            let _ = write!(out, ":{}", ns.label);
            if total > 0 {
                let _ = write!(out, " ({total} props");
                if required > 0 {
                    let _ = write!(out, ", {required} req");
                }
                out.push(')');
            }
            out.push('\n');
        }
    }

    if !edge_schemas.is_empty() {
        if !node_schemas.is_empty() {
            out.push('\n');
        }
        let _ = writeln!(out, "# Edge types ({})", edge_schemas.len());
        for es in edge_schemas {
            let _ = write!(out, ":{}", es.label);
            if !es.source_labels.is_empty() || !es.target_labels.is_empty() {
                let srcs = if es.source_labels.is_empty() {
                    "*".to_string()
                } else {
                    es.source_labels
                        .iter()
                        .map(|l| l.as_ref())
                        .collect::<Vec<_>>()
                        .join("|")
                };
                let tgts = if es.target_labels.is_empty() {
                    "*".to_string()
                } else {
                    es.target_labels
                        .iter()
                        .map(|l| l.as_ref())
                        .collect::<Vec<_>>()
                        .join("|")
                };
                let _ = write!(out, " ({srcs} → {tgts})");
            }
            out.push('\n');
        }
    }

    // Stats section.
    if !node_schemas.is_empty() || !edge_schemas.is_empty() {
        out.push('\n');
    }
    let _ = writeln!(out, "# Stats");
    let _ = writeln!(out, "nodes: {}", graph.node_count());
    let _ = writeln!(out, "edges: {}", graph.edge_count());
    out.push_str("\nTip: Use schema_dump with label parameter for full property details, or get_schema for a single type.");
}

/// Full format: complete property details and descriptions (original behavior).
fn format_full(
    out: &mut String,
    node_schemas: &[&selene_core::schema::NodeSchema],
    edge_schemas: &[&selene_core::schema::EdgeSchema],
    graph: &SeleneGraph,
) {
    // Node types section.
    if !node_schemas.is_empty() {
        out.push_str("# Node types\n");
        for ns in node_schemas {
            let _ = write!(out, ":{}", ns.label);
            if !ns.description.is_empty() {
                let _ = write!(out, " -- {}", ns.description);
            }
            out.push('\n');
            format_node_properties(out, ns);

            if !ns.valid_edge_labels.is_empty() {
                let edges: Vec<&str> = ns.valid_edge_labels.iter().map(|e| e.as_ref()).collect();
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
        for es in edge_schemas {
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
}

/// Format property details for a node schema.
fn format_node_properties(out: &mut String, ns: &selene_core::schema::NodeSchema) {
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

    // ── Default (compact) mode tests ────────────────────────────────

    #[test]
    fn compact_default_shows_type_counts() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        let text = rows[0][0].1.as_str().unwrap();

        assert!(
            text.contains("# Node types (1)"),
            "should show node type count"
        );
        assert!(
            text.contains("# Edge types (1)"),
            "should show edge type count"
        );
        assert!(
            text.contains(":sensor (2 props, 1 req)"),
            "should show property counts"
        );
        assert!(
            text.contains(":contains (building|floor → floor|sensor)"),
            "should show edge connectivity"
        );
    }

    #[test]
    fn compact_default_omits_descriptions() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        let text = rows[0][0].1.as_str().unwrap();

        assert!(
            !text.contains("A temperature sensor"),
            "compact mode should omit descriptions"
        );
        assert!(
            !text.contains("Containment hierarchy"),
            "compact mode should omit edge descriptions"
        );
        assert!(
            !text.contains(".name:"),
            "compact mode should omit individual properties"
        );
    }

    #[test]
    fn compact_default_shows_tip() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        let text = rows[0][0].1.as_str().unwrap();

        assert!(
            text.contains("Tip:"),
            "compact mode should include usage tip"
        );
    }

    // ── Full mode tests ─────────────────────────────────────────────

    #[test]
    fn full_mode_includes_all_details() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        // args: includeSystem=false, compact=false
        let args = vec![GqlValue::Bool(false), GqlValue::Bool(false)];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        let text = rows[0][0].1.as_str().unwrap();

        assert!(text.contains(":sensor"), "should contain node label");
        assert!(
            text.contains(".name: String REQUIRED"),
            "should contain required property"
        );
        assert!(
            text.contains(".temp: Float"),
            "should contain optional property"
        );
        assert!(
            text.contains("A temperature sensor"),
            "should contain description"
        );
        assert!(
            text.contains("edges: monitors"),
            "should contain valid edges"
        );
        assert!(text.contains(":contains"), "should contain edge label");
        assert!(
            text.contains("FROM building|floor"),
            "should contain source labels"
        );
        assert!(
            text.contains("TO floor|sensor"),
            "should contain target labels"
        );
        assert!(
            text.contains("Containment hierarchy"),
            "should contain edge description"
        );
        assert!(text.contains(".since: Int"), "should contain edge property");
    }

    // ── Label filter tests ──────────────────────────────────────────

    #[test]
    fn label_filter_returns_node_detail() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        // args: includeSystem=false, compact=true, label="sensor"
        let args = vec![
            GqlValue::Bool(false),
            GqlValue::Bool(true),
            GqlValue::String("sensor".into()),
        ];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        let text = rows[0][0].1.as_str().unwrap();

        assert!(text.contains(":sensor"), "should contain the label");
        assert!(text.contains("[node type]"), "should indicate node type");
        assert!(
            text.contains(".name: String REQUIRED"),
            "should contain full property details"
        );
        assert!(
            text.contains("A temperature sensor"),
            "should contain description even in compact mode"
        );
        assert!(text.contains("← :contains"), "should show connected edges");
        assert!(
            !text.contains("# Stats"),
            "label filter should not include stats"
        );
    }

    #[test]
    fn label_filter_returns_edge_detail() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        let args = vec![
            GqlValue::Bool(false),
            GqlValue::Bool(true),
            GqlValue::String("contains".into()),
        ];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        let text = rows[0][0].1.as_str().unwrap();

        assert!(text.contains(":contains"), "should contain the label");
        assert!(text.contains("[edge type]"), "should indicate edge type");
        assert!(
            text.contains("from: building | floor"),
            "should show source labels"
        );
        assert!(
            text.contains("to: floor | sensor"),
            "should show target labels"
        );
        assert!(
            text.contains("Containment hierarchy"),
            "should contain description"
        );
    }

    #[test]
    fn label_filter_unknown_returns_error() {
        let g = graph_with_schemas();
        let proc = SchemaDump;
        let args = vec![
            GqlValue::Bool(false),
            GqlValue::Bool(true),
            GqlValue::String("nonexistent".into()),
        ];
        let result = proc.execute(&args, &g, None, None);
        assert!(result.is_err(), "unknown label should return error");
    }

    // ── System label tests ──────────────────────────────────────────

    #[test]
    fn excludes_system_labels_by_default() {
        let mut g = graph_with_schemas();

        let system_schema = NodeSchema::builder("__CommunitySummary")
            .property(PropertyDef::simple("key_entities", ValueType::String, true))
            .description("System community summary node")
            .build();
        g.schema_mut().register_node_schema(system_schema).unwrap();

        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        let text = rows[0][0].1.as_str().unwrap();

        assert!(
            !text.contains("__CommunitySummary"),
            "system labels should be excluded by default"
        );
        assert!(text.contains(":sensor"), "non-system labels should remain");
    }

    #[test]
    fn includes_system_when_requested() {
        let mut g = graph_with_schemas();

        let system_schema = NodeSchema::builder("__CommunitySummary")
            .property(PropertyDef::simple("key_entities", ValueType::String, true))
            .description("System community summary node")
            .build();
        g.schema_mut().register_node_schema(system_schema).unwrap();

        let proc = SchemaDump;
        let args = vec![GqlValue::Bool(true)];
        let rows = proc.execute(&args, &g, None, None).unwrap();
        let text = rows[0][0].1.as_str().unwrap();

        assert!(
            text.contains("__CommunitySummary"),
            "system labels should be included when requested"
        );
        assert!(
            text.contains(":sensor"),
            "non-system labels should also be present"
        );
    }

    // ── Empty schema test ───────────────────────────────────────────

    #[test]
    fn empty_schema_shows_stats_only() {
        let g = SeleneGraph::new();
        let proc = SchemaDump;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        let text = rows[0][0].1.as_str().unwrap();

        assert!(text.contains("# Stats"), "should contain stats section");
        assert!(text.contains("nodes: 0"), "should show zero nodes");
        assert!(text.contains("edges: 0"), "should show zero edges");
        assert!(
            !text.contains("# Node types"),
            "should not contain node types header when empty"
        );
        assert!(
            !text.contains("# Edge types"),
            "should not contain edge types header when empty"
        );
    }
}
