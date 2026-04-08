//! Schema introspection procedures: graph.labels, graph.edge_types, graph.node_count,
//! graph.edge_count, graph.schema, graph.constraints, graph.discoverSchema.

use std::collections::HashMap;

use selene_core::{IStr, NodeId, Value};
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::{Procedure, ProcedureParam, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

pub struct GraphLabels;
impl Procedure for GraphLabels {
    fn name(&self) -> &'static str {
        "graph.labels"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![YieldColumn {
                name: "label",
                typ: GqlType::String,
            }],
        }
    }
    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let mut labels = std::collections::BTreeSet::new();
        for nid in &graph.all_node_bitmap() {
            if let Some(node) = graph.get_node(selene_core::NodeId(u64::from(nid))) {
                for l in node.labels.iter() {
                    labels.insert(l.as_str().to_string());
                }
            }
        }
        Ok(labels
            .into_iter()
            .map(|l| {
                smallvec::smallvec![(
                    selene_core::IStr::new("label"),
                    GqlValue::String(smol_str::SmolStr::new(&l))
                )]
            })
            .collect())
    }
}

pub struct GraphEdgeTypes;
impl Procedure for GraphEdgeTypes {
    fn name(&self) -> &'static str {
        "graph.edge_types"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![YieldColumn {
                name: "type",
                typ: GqlType::String,
            }],
        }
    }
    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let mut types = std::collections::BTreeSet::new();
        for eid in &graph.all_edge_bitmap() {
            if let Some(edge) = graph.get_edge(selene_core::EdgeId(u64::from(eid))) {
                types.insert(edge.label.as_str().to_string());
            }
        }
        Ok(types
            .into_iter()
            .map(|t| {
                smallvec::smallvec![(
                    selene_core::IStr::new("type"),
                    GqlValue::String(smol_str::SmolStr::new(&t))
                )]
            })
            .collect())
    }
}

pub struct GraphNodeCount;
impl Procedure for GraphNodeCount {
    fn name(&self) -> &'static str {
        "graph.node_count"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![YieldColumn {
                name: "count",
                typ: GqlType::Int,
            }],
        }
    }
    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        Ok(vec![smallvec::smallvec![(
            selene_core::IStr::new("count"),
            GqlValue::Int(graph.node_count() as i64)
        )]])
    }
}

pub struct GraphEdgeCount;
impl Procedure for GraphEdgeCount {
    fn name(&self) -> &'static str {
        "graph.edge_count"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![YieldColumn {
                name: "count",
                typ: GqlType::Int,
            }],
        }
    }
    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        Ok(vec![smallvec![(
            IStr::new("count"),
            GqlValue::Int(graph.edge_count() as i64)
        )]])
    }
}

// ── graph.schema(label): registered schema details ──────────────────

pub struct GraphSchema;
impl Procedure for GraphSchema {
    fn name(&self) -> &'static str {
        "graph.schema"
    }
    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "label",
                typ: GqlType::String,
            }],
            yields: vec![
                YieldColumn {
                    name: "property",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "valueType",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "required",
                    typ: GqlType::Bool,
                },
                YieldColumn {
                    name: "unique",
                    typ: GqlType::Bool,
                },
                YieldColumn {
                    name: "indexed",
                    typ: GqlType::Bool,
                },
                YieldColumn {
                    name: "immutable",
                    typ: GqlType::Bool,
                },
                YieldColumn {
                    name: "constraints",
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
        let label = args
            .first()
            .ok_or_else(|| GqlError::InvalidArgument {
                message: "graph.schema requires label".into(),
            })?
            .as_str()?;

        let schema =
            graph
                .schema()
                .node_schema(label)
                .ok_or_else(|| GqlError::InvalidArgument {
                    message: format!("no schema registered for label '{label}'"),
                })?;

        Ok(schema
            .properties
            .iter()
            .map(|p| {
                // Build constraint description string
                let mut constraints = Vec::new();
                if let Some(min) = p.min {
                    constraints.push(format!("min={min}"));
                }
                if let Some(max) = p.max {
                    constraints.push(format!("max={max}"));
                }
                if let Some(min_len) = p.min_length {
                    constraints.push(format!("min_length={min_len}"));
                }
                if let Some(max_len) = p.max_length {
                    constraints.push(format!("max_length={max_len}"));
                }
                if !p.allowed_values.is_empty() {
                    constraints.push(format!("enum({})", p.allowed_values.len()));
                }
                if p.pattern.is_some() {
                    constraints.push("pattern".into());
                }
                let constraint_str = if constraints.is_empty() {
                    "none".to_string()
                } else {
                    constraints.join(", ")
                };

                smallvec![
                    (
                        IStr::new("property"),
                        GqlValue::String(SmolStr::new(p.name.as_ref()))
                    ),
                    (
                        IStr::new("valueType"),
                        GqlValue::String(SmolStr::new(format!("{:?}", p.value_type)))
                    ),
                    (IStr::new("required"), GqlValue::Bool(p.required)),
                    (IStr::new("unique"), GqlValue::Bool(p.unique)),
                    (IStr::new("indexed"), GqlValue::Bool(p.indexed)),
                    (IStr::new("immutable"), GqlValue::Bool(p.immutable)),
                    (
                        IStr::new("constraints"),
                        GqlValue::String(SmolStr::new(&constraint_str))
                    ),
                ]
            })
            .collect())
    }
}

// ── graph.constraints(): all constraints across all schemas ─────────

pub struct GraphConstraints;
impl Procedure for GraphConstraints {
    fn name(&self) -> &'static str {
        "graph.constraints"
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
                    name: "constraintType",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "properties",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "description",
                    typ: GqlType::String,
                },
            ],
        }
    }
    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        let mut rows = Vec::new();

        for schema in graph.schema().all_node_schemas() {
            let label = schema.label.as_ref();

            // Uniqueness constraints
            for p in &schema.properties {
                if p.unique {
                    rows.push(smallvec![
                        (IStr::new("label"), GqlValue::String(SmolStr::new(label))),
                        (
                            IStr::new("constraintType"),
                            GqlValue::String(SmolStr::new("unique"))
                        ),
                        (
                            IStr::new("properties"),
                            GqlValue::String(SmolStr::new(p.name.as_ref()))
                        ),
                        (
                            IStr::new("description"),
                            GqlValue::String(SmolStr::new(format!(
                                "Property '{}' must be unique",
                                p.name
                            )))
                        ),
                    ]);
                }
                if p.immutable {
                    rows.push(smallvec![
                        (IStr::new("label"), GqlValue::String(SmolStr::new(label))),
                        (
                            IStr::new("constraintType"),
                            GqlValue::String(SmolStr::new("immutable"))
                        ),
                        (
                            IStr::new("properties"),
                            GqlValue::String(SmolStr::new(p.name.as_ref()))
                        ),
                        (
                            IStr::new("description"),
                            GqlValue::String(SmolStr::new(format!(
                                "Property '{}' cannot be changed after creation",
                                p.name
                            )))
                        ),
                    ]);
                }
                if p.min.is_some() || p.max.is_some() {
                    let desc = match (p.min, p.max) {
                        (Some(min), Some(max)) => {
                            format!("Property '{}' must be in range [{}, {}]", p.name, min, max)
                        }
                        (Some(min), None) => format!("Property '{}' must be >= {}", p.name, min),
                        (None, Some(max)) => format!("Property '{}' must be <= {}", p.name, max),
                        _ => unreachable!(),
                    };
                    rows.push(smallvec![
                        (IStr::new("label"), GqlValue::String(SmolStr::new(label))),
                        (
                            IStr::new("constraintType"),
                            GqlValue::String(SmolStr::new("range"))
                        ),
                        (
                            IStr::new("properties"),
                            GqlValue::String(SmolStr::new(p.name.as_ref()))
                        ),
                        (
                            IStr::new("description"),
                            GqlValue::String(SmolStr::new(&desc))
                        ),
                    ]);
                }
                if !p.allowed_values.is_empty() {
                    rows.push(smallvec![
                        (IStr::new("label"), GqlValue::String(SmolStr::new(label))),
                        (
                            IStr::new("constraintType"),
                            GqlValue::String(SmolStr::new("enum"))
                        ),
                        (
                            IStr::new("properties"),
                            GqlValue::String(SmolStr::new(p.name.as_ref()))
                        ),
                        (
                            IStr::new("description"),
                            GqlValue::String(SmolStr::new(format!(
                                "Property '{}' must be one of {} allowed values",
                                p.name,
                                p.allowed_values.len()
                            )))
                        ),
                    ]);
                }
                if p.pattern.is_some() {
                    rows.push(smallvec![
                        (IStr::new("label"), GqlValue::String(SmolStr::new(label))),
                        (
                            IStr::new("constraintType"),
                            GqlValue::String(SmolStr::new("pattern"))
                        ),
                        (
                            IStr::new("properties"),
                            GqlValue::String(SmolStr::new(p.name.as_ref()))
                        ),
                        (
                            IStr::new("description"),
                            GqlValue::String(SmolStr::new(format!(
                                "Property '{}' must match regex pattern",
                                p.name
                            )))
                        ),
                    ]);
                }
            }

            // Node key constraint
            if !schema.key_properties.is_empty() {
                let key_str = schema
                    .key_properties
                    .iter()
                    .map(|k| k.as_ref())
                    .collect::<Vec<_>>()
                    .join(", ");
                rows.push(smallvec![
                    (IStr::new("label"), GqlValue::String(SmolStr::new(label))),
                    (
                        IStr::new("constraintType"),
                        GqlValue::String(SmolStr::new("node_key"))
                    ),
                    (
                        IStr::new("properties"),
                        GqlValue::String(SmolStr::new(&key_str))
                    ),
                    (
                        IStr::new("description"),
                        GqlValue::String(SmolStr::new(format!(
                            "Composite key ({key_str}) must be unique"
                        )))
                    ),
                ]);
            }
        }

        // Edge cardinality constraints
        for schema in graph.schema().all_edge_schemas() {
            let label = schema.label.as_ref();
            if schema.max_out_degree.is_some()
                || schema.max_in_degree.is_some()
                || schema.min_out_degree.is_some()
                || schema.min_in_degree.is_some()
            {
                let mut parts = Vec::new();
                if let Some(v) = schema.min_out_degree {
                    parts.push(format!("min_out={v}"));
                }
                if let Some(v) = schema.max_out_degree {
                    parts.push(format!("max_out={v}"));
                }
                if let Some(v) = schema.min_in_degree {
                    parts.push(format!("min_in={v}"));
                }
                if let Some(v) = schema.max_in_degree {
                    parts.push(format!("max_in={v}"));
                }
                rows.push(smallvec![
                    (IStr::new("label"), GqlValue::String(SmolStr::new(label))),
                    (
                        IStr::new("constraintType"),
                        GqlValue::String(SmolStr::new("cardinality"))
                    ),
                    (
                        IStr::new("properties"),
                        GqlValue::String(SmolStr::new(parts.join(", ")))
                    ),
                    (
                        IStr::new("description"),
                        GqlValue::String(SmolStr::new(format!(
                            "Edge '{}' cardinality: {}",
                            label,
                            parts.join(", ")
                        )))
                    ),
                ]);
            }
        }

        Ok(rows)
    }
}

// ── graph.discoverSchema(): infer schema from existing data ─────────

pub struct GraphDiscoverSchema;
impl Procedure for GraphDiscoverSchema {
    fn name(&self) -> &'static str {
        "graph.discoverSchema"
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
                    name: "property",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "inferredType",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "nullRate",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "uniqueRate",
                    typ: GqlType::Float,
                },
                YieldColumn {
                    name: "sampleSize",
                    typ: GqlType::Int,
                },
            ],
        }
    }
    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        // Discover: for each label, scan all nodes and infer property types

        // Step 1: Group nodes by label
        let mut label_nodes: HashMap<String, Vec<NodeId>> = HashMap::new();
        for nid in &graph.all_node_bitmap() {
            let node_id = NodeId(u64::from(nid));
            if let Some(node) = graph.get_node(node_id) {
                for label in node.labels.iter() {
                    label_nodes
                        .entry(label.as_str().to_string())
                        .or_default()
                        .push(node_id);
                }
            }
        }

        let mut rows = Vec::new();

        for (label, node_ids) in &label_nodes {
            let total = node_ids.len();
            if total == 0 {
                continue;
            }

            // Step 2: Discover properties and their types
            let mut prop_stats: HashMap<String, PropDiscovery> = HashMap::new();

            for &nid in node_ids {
                if let Some(node) = graph.get_node(nid) {
                    for (key, value) in node.properties.iter() {
                        let stat = prop_stats
                            .entry(key.as_str().to_string())
                            .or_insert_with(PropDiscovery::new);
                        stat.total += 1;
                        if value.is_null() {
                            stat.nulls += 1;
                        } else {
                            stat.observe_type(value);
                            let val_str = format!("{value:?}");
                            stat.unique_values.insert(val_str);
                        }
                    }
                }
            }

            // Step 3: Emit one row per (label, property)
            let mut props: Vec<_> = prop_stats.into_iter().collect();
            props.sort_by(|a, b| a.0.cmp(&b.0));

            for (prop_name, stat) in props {
                let null_rate = if total > 0 {
                    1.0 - (stat.total as f64 / total as f64)
                } else {
                    0.0
                };
                let non_null = stat.total - stat.nulls;
                let unique_rate = if non_null > 0 {
                    stat.unique_values.len() as f64 / non_null as f64
                } else {
                    0.0
                };

                rows.push(smallvec![
                    (IStr::new("label"), GqlValue::String(SmolStr::new(label))),
                    (
                        IStr::new("property"),
                        GqlValue::String(SmolStr::new(&prop_name))
                    ),
                    (
                        IStr::new("inferredType"),
                        GqlValue::String(SmolStr::new(stat.majority_type()))
                    ),
                    (IStr::new("nullRate"), GqlValue::Float(null_rate)),
                    (IStr::new("uniqueRate"), GqlValue::Float(unique_rate)),
                    (IStr::new("sampleSize"), GqlValue::Int(total as i64)),
                ]);
            }
        }

        // Sort by label, then property for deterministic output
        rows.sort_by(|a: &ProcedureRow, b: &ProcedureRow| {
            let la = a[0].1.as_str().unwrap_or("");
            let lb = b[0].1.as_str().unwrap_or("");
            let pa = a[1].1.as_str().unwrap_or("");
            let pb = b[1].1.as_str().unwrap_or("");
            la.cmp(lb).then(pa.cmp(pb))
        });

        Ok(rows)
    }
}

/// Internal state for property type discovery.
struct PropDiscovery {
    total: usize,
    nulls: usize,
    type_counts: HashMap<String, usize>,
    unique_values: std::collections::HashSet<String>,
}

impl PropDiscovery {
    fn new() -> Self {
        Self {
            total: 0,
            nulls: 0,
            type_counts: HashMap::new(),
            unique_values: std::collections::HashSet::new(),
        }
    }

    fn observe_type(&mut self, value: &Value) {
        let type_name = match value {
            Value::Bool(_) => "Bool",
            Value::Int(_) => "Int",
            Value::Float(_) => "Float",
            Value::String(_) => "String",
            Value::Timestamp(_) => "Timestamp",
            Value::Bytes(_) => "Bytes",
            Value::List(_) => "List",
            Value::UInt(_) => "UInt",
            Value::Date(_) => "Date",
            Value::LocalDateTime(_) => "LocalDateTime",
            Value::Duration(_) => "Duration",
            Value::Vector(_) => "Vector",
            Value::InternedStr(_) => "String",
            Value::Null => "Null",
        };
        *self.type_counts.entry(type_name.to_string()).or_insert(0) += 1;
    }

    fn majority_type(&self) -> String {
        self.type_counts
            .iter()
            .max_by_key(|&(_, count)| *count)
            .map_or_else(|| "Any".to_string(), |(t, _)| t.clone())
    }
}

// ── graph.diff ──────────────────────────────────────────────────────────────

/// `CALL graph.diff($sinceNanos) YIELD entity_type, change_type, label, count`
///
/// Reports what changed since a given timestamp (nanos since epoch).
/// Scans all nodes and edges, comparing created_at/updated_at against the
/// threshold. Returns summary rows grouped by entity type, change type, and
/// label. Useful for session continuity: "what happened since my last session?"
pub struct GraphDiff;

impl Procedure for GraphDiff {
    fn name(&self) -> &'static str {
        "graph.diff"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![ProcedureParam {
                name: "sinceNanos",
                typ: GqlType::Int,
            }],
            yields: vec![
                YieldColumn {
                    name: "entity_type",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "change_type",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "label",
                    typ: GqlType::String,
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
        use std::collections::HashMap;

        if args.is_empty() {
            return Err(GqlError::InvalidArgument {
                message: "graph.diff requires 1 argument: sinceNanos (timestamp in nanoseconds)"
                    .into(),
            });
        }
        let since = args[0].as_int()?;

        let entity_type_key = IStr::new("entity_type");
        let change_type_key = IStr::new("change_type");
        let label_key = IStr::new("label");
        let count_key = IStr::new("count");

        // Count node changes by (change_type, first_label)
        let mut node_counts: HashMap<(&str, String), i64> = HashMap::new();
        for node_id in graph.all_node_ids() {
            if let Some(node) = graph.get_node(node_id) {
                let first_label = node
                    .labels
                    .iter()
                    .next()
                    .map_or_else(|| "(unlabeled)".to_string(), |l| l.as_str().to_string());
                if node.created_at >= since {
                    *node_counts.entry(("created", first_label)).or_insert(0) += 1;
                } else if node.updated_at >= since {
                    *node_counts.entry(("modified", first_label)).or_insert(0) += 1;
                }
            }
        }

        // Count edge changes by label (edges only have created_at)
        let mut edge_counts: HashMap<String, i64> = HashMap::new();
        for edge_id in graph.all_edge_ids() {
            if let Some(edge) = graph.get_edge(edge_id)
                && edge.created_at >= since
            {
                *edge_counts
                    .entry(edge.label.as_str().to_string())
                    .or_insert(0) += 1;
            }
        }

        // Build result rows sorted by count descending
        let mut rows = Vec::new();

        let mut node_entries: Vec<_> = node_counts.into_iter().collect();
        node_entries.sort_by(|a, b| b.1.cmp(&a.1));
        for ((change, label), count) in node_entries {
            rows.push(smallvec::smallvec![
                (entity_type_key, GqlValue::String(change.into())),
                (change_type_key, GqlValue::String("node".into())),
                (label_key, GqlValue::String(label.into())),
                (count_key, GqlValue::Int(count)),
            ]);
        }

        let mut edge_entries: Vec<_> = edge_counts.into_iter().collect();
        edge_entries.sort_by(|a, b| b.1.cmp(&a.1));
        for (label, count) in edge_entries {
            rows.push(smallvec::smallvec![
                (entity_type_key, GqlValue::String("created".into())),
                (change_type_key, GqlValue::String("edge".into())),
                (label_key, GqlValue::String(label.into())),
                (count_key, GqlValue::Int(count)),
            ]);
        }

        Ok(rows)
    }
}

// ── graph.validate ──────────────────────────────────────────────────────────

/// `CALL graph.validate() YIELD check, status, count, details`
///
/// Validates structural integrity of the graph. Checks:
/// - Dangling edges: edges whose source or target node does not exist
/// - Duplicate edges: multiple edges with same (source, target, label)
/// - Orphaned nodes: nodes with zero edges (excluding __ system labels)
pub struct GraphValidate;

impl Procedure for GraphValidate {
    fn name(&self) -> &'static str {
        "graph.validate"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![
                YieldColumn {
                    name: "check",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "status",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "count",
                    typ: GqlType::Int,
                },
                YieldColumn {
                    name: "details",
                    typ: GqlType::String,
                },
            ],
        }
    }

    fn execute(
        &self,
        _args: &[GqlValue],
        graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        use std::collections::HashMap;
        let check_key = IStr::new("check");
        let status_key = IStr::new("status");
        let count_key = IStr::new("count");
        let details_key = IStr::new("details");

        let mut rows = Vec::new();

        // Check 1: Dangling edges (source or target node missing)
        let mut dangling = 0i64;
        let mut dangling_ids = Vec::new();
        for edge_id in graph.all_edge_ids() {
            if let Some(edge) = graph.get_edge(edge_id) {
                let src_ok = graph.get_node(edge.source).is_some();
                let tgt_ok = graph.get_node(edge.target).is_some();
                if !src_ok || !tgt_ok {
                    dangling += 1;
                    if dangling_ids.len() < 10 {
                        dangling_ids.push(edge_id.0.to_string());
                    }
                }
            }
        }
        let dangling_detail = if dangling_ids.is_empty() {
            String::new()
        } else {
            format!("edge_ids: [{}]", dangling_ids.join(", "))
        };
        rows.push(smallvec::smallvec![
            (check_key, GqlValue::String("dangling_edges".into())),
            (
                status_key,
                GqlValue::String(if dangling == 0 { "pass" } else { "fail" }.into())
            ),
            (count_key, GqlValue::Int(dangling)),
            (details_key, GqlValue::String(dangling_detail.into())),
        ]);

        // Check 2: Duplicate edges (same source, target, label)
        let mut edge_keys: HashMap<(u64, u64, IStr), i64> = HashMap::new();
        for edge_id in graph.all_edge_ids() {
            if let Some(edge) = graph.get_edge(edge_id) {
                *edge_keys
                    .entry((edge.source.0, edge.target.0, edge.label))
                    .or_insert(0) += 1;
            }
        }
        let duplicates: i64 = edge_keys.values().filter(|&&c| c > 1).map(|c| c - 1).sum();
        let dup_detail = if duplicates == 0 {
            String::new()
        } else {
            let examples: Vec<String> = edge_keys
                .iter()
                .filter(|&(_, &c)| c > 1)
                .take(5)
                .map(|((s, t, l), c)| format!("({s})-[:{l}]->({t}) x{c}"))
                .collect();
            examples.join(", ")
        };
        rows.push(smallvec::smallvec![
            (check_key, GqlValue::String("duplicate_edges".into())),
            (
                status_key,
                GqlValue::String(if duplicates == 0 { "pass" } else { "warn" }.into())
            ),
            (count_key, GqlValue::Int(duplicates)),
            (details_key, GqlValue::String(dup_detail.into())),
        ]);

        // Check 3: Orphaned nodes (zero edges, excluding system labels)
        let mut orphans = 0i64;
        let mut orphan_examples = Vec::new();
        for node_id in graph.all_node_ids() {
            if let Some(node) = graph.get_node(node_id) {
                // Skip system labels (__ prefix)
                if node.labels.iter().any(|l| l.as_str().starts_with("__")) {
                    continue;
                }
                let out_count = graph.outgoing(node_id).len();
                let in_count = graph.incoming(node_id).len();
                if out_count == 0 && in_count == 0 {
                    orphans += 1;
                    if orphan_examples.len() < 10 {
                        let name = node
                            .properties
                            .get(IStr::new("name"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("?");
                        orphan_examples.push(format!("{}:{}", node_id.0, name));
                    }
                }
            }
        }
        let orphan_detail = if orphan_examples.is_empty() {
            String::new()
        } else {
            orphan_examples.join(", ")
        };
        rows.push(smallvec::smallvec![
            (check_key, GqlValue::String("orphaned_nodes".into())),
            (
                status_key,
                GqlValue::String(if orphans == 0 { "pass" } else { "info" }.into())
            ),
            (count_key, GqlValue::Int(orphans)),
            (details_key, GqlValue::String(orphan_detail.into())),
        ]);

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};
    use selene_graph::SeleneGraph;
    use smol_str::SmolStr;

    // ── Fixtures ────────────────────────────────────────────────────

    fn populated_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S1"))),
                (IStr::new("temp"), Value::Float(72.5)),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S2"))),
                (IStr::new("temp"), Value::Float(68.0)),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["device"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("D1")))]),
        )
        .unwrap();
        m.create_edge(
            NodeId(1),
            IStr::new("monitors"),
            NodeId(3),
            PropertyMap::new(),
        )
        .unwrap();
        m.create_edge(NodeId(2), IStr::new("feeds"), NodeId(3), PropertyMap::new())
            .unwrap();
        m.commit(0).unwrap();
        g
    }

    fn empty_graph() -> SeleneGraph {
        SeleneGraph::new()
    }

    // ── graph.labels ────────────────────────────────────────────────

    #[test]
    fn labels_returns_all_distinct_labels() {
        let g = populated_graph();
        let proc = GraphLabels;
        let rows = proc.execute(&[], &g, None, None).unwrap();

        let labels: Vec<String> = rows
            .iter()
            .map(|r| r[0].1.as_str().unwrap().to_string())
            .collect();
        assert!(labels.contains(&"sensor".to_string()));
        assert!(labels.contains(&"device".to_string()));
        assert_eq!(labels.len(), 2);
    }

    #[test]
    fn labels_empty_graph() {
        let g = empty_graph();
        let proc = GraphLabels;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert!(rows.is_empty());
    }

    // ── graph.edge_types ────────────────────────────────────────────

    #[test]
    fn edge_types_returns_all_distinct_types() {
        let g = populated_graph();
        let proc = GraphEdgeTypes;
        let rows = proc.execute(&[], &g, None, None).unwrap();

        let types: Vec<String> = rows
            .iter()
            .map(|r| r[0].1.as_str().unwrap().to_string())
            .collect();
        assert!(types.contains(&"monitors".to_string()));
        assert!(types.contains(&"feeds".to_string()));
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn edge_types_empty_graph() {
        let g = empty_graph();
        let proc = GraphEdgeTypes;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert!(rows.is_empty());
    }

    // ── graph.node_count ────────────────────────────────────────────

    #[test]
    fn node_count_returns_correct_count() {
        let g = populated_graph();
        let proc = GraphNodeCount;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, GqlValue::Int(3));
    }

    #[test]
    fn node_count_empty_graph() {
        let g = empty_graph();
        let proc = GraphNodeCount;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert_eq!(rows[0][0].1, GqlValue::Int(0));
    }

    // ── graph.edge_count ────────────────────────────────────────────

    #[test]
    fn edge_count_returns_correct_count() {
        let g = populated_graph();
        let proc = GraphEdgeCount;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, GqlValue::Int(2));
    }

    #[test]
    fn edge_count_empty_graph() {
        let g = empty_graph();
        let proc = GraphEdgeCount;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert_eq!(rows[0][0].1, GqlValue::Int(0));
    }

    // ── graph.discoverSchema ────────────────────────────────────────

    #[test]
    fn discover_schema_infers_property_types() {
        let g = populated_graph();
        let proc = GraphDiscoverSchema;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert!(!rows.is_empty());

        // Rows are sorted by (label, property). Verify we see expected properties.
        let mut found_device_name = false;
        let mut found_sensor_name = false;
        let mut found_sensor_temp = false;

        for row in &rows {
            let label = row[0].1.as_str().unwrap();
            let property = row[1].1.as_str().unwrap();
            let inferred_type = row[2].1.as_str().unwrap();

            match (label, property) {
                ("device", "name") => {
                    assert_eq!(inferred_type, "String");
                    found_device_name = true;
                }
                ("sensor", "name") => {
                    assert_eq!(inferred_type, "String");
                    found_sensor_name = true;
                }
                ("sensor", "temp") => {
                    assert_eq!(inferred_type, "Float");
                    found_sensor_temp = true;
                }
                _ => {}
            }
        }

        assert!(found_device_name, "device.name not found");
        assert!(found_sensor_name, "sensor.name not found");
        assert!(found_sensor_temp, "sensor.temp not found");
    }

    #[test]
    fn discover_schema_empty_graph() {
        let g = empty_graph();
        let proc = GraphDiscoverSchema;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn discover_schema_null_rate_correct() {
        // Create a graph where one sensor has "temp" and one does not
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S1"))),
                (IStr::new("temp"), Value::Float(72.5)),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S2")))]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let proc = GraphDiscoverSchema;
        let rows = proc.execute(&[], &g, None, None).unwrap();

        // Find the "temp" property for "sensor" label
        let temp_row = rows
            .iter()
            .find(|r| r[0].1.as_str().unwrap() == "sensor" && r[1].1.as_str().unwrap() == "temp")
            .expect("temp property not found");

        // nullRate should be 0.5 (1 of 2 sensor nodes lacks this property)
        match &temp_row[3].1 {
            GqlValue::Float(v) => assert!((*v - 0.5).abs() < 0.01),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn discover_schema_unique_rate() {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        // Two sensors with the same name value
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(
                IStr::new("name"),
                Value::String(SmolStr::new("same")),
            )]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![(
                IStr::new("name"),
                Value::String(SmolStr::new("same")),
            )]),
        )
        .unwrap();
        m.commit(0).unwrap();

        let proc = GraphDiscoverSchema;
        let rows = proc.execute(&[], &g, None, None).unwrap();

        let name_row = rows
            .iter()
            .find(|r| r[1].1.as_str().unwrap() == "name")
            .expect("name not found");

        // uniqueRate: 1 unique value / 2 non-null values = 0.5
        match &name_row[4].1 {
            GqlValue::Float(v) => assert!((*v - 0.5).abs() < 0.01),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    // ── graph.schema (requires registered schema) ───────────────────

    #[test]
    fn schema_missing_label_errors() {
        let g = populated_graph();
        let proc = GraphSchema;
        let args = vec![GqlValue::String(SmolStr::new("nonexistent"))];
        let result = proc.execute(&args, &g, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn schema_missing_arg_errors() {
        let g = populated_graph();
        let proc = GraphSchema;
        let result = proc.execute(&[], &g, None, None);
        assert!(result.is_err());
    }

    // ── graph.constraints (empty when no schemas registered) ────────

    #[test]
    fn constraints_empty_when_no_schemas() {
        let g = populated_graph();
        let proc = GraphConstraints;
        let rows = proc.execute(&[], &g, None, None).unwrap();
        assert!(rows.is_empty());
    }

    // ── Signature checks ────────────────────────────────────────────

    #[test]
    fn procedure_names_match_convention() {
        assert_eq!(GraphLabels.name(), "graph.labels");
        assert_eq!(GraphEdgeTypes.name(), "graph.edge_types");
        assert_eq!(GraphNodeCount.name(), "graph.node_count");
        assert_eq!(GraphEdgeCount.name(), "graph.edge_count");
        assert_eq!(GraphSchema.name(), "graph.schema");
        assert_eq!(GraphConstraints.name(), "graph.constraints");
        assert_eq!(GraphDiscoverSchema.name(), "graph.discoverSchema");
        assert_eq!(GraphValidate.name(), "graph.validate");
        assert_eq!(GraphDiff.name(), "graph.diff");
    }
}
