//! DDL statement execution: CREATE/DROP/SHOW for triggers, node types, edge
//! types, and materialized views.

use std::collections::HashMap;
use std::sync::Arc;

use selene_graph::SharedGraph;

use crate::ast::expr::{AggregateOp, Expr};
use crate::ast::statement::{CreateTriggerStmt, DdlPropertyDef, Projection, ReturnClause};
use crate::types::error::{GqlError, GqlStatus, MutationStats};
use crate::types::result::GqlResult;

// ── Trigger DDL ─────────────────────────────────────────────────────

pub(super) fn create_trigger(
    shared: &SharedGraph,
    stmt: &CreateTriggerStmt,
) -> Result<GqlResult, GqlError> {
    let trigger = selene_core::TriggerDef {
        name: Arc::from(stmt.name.as_str()),
        event: stmt.event,
        label: Arc::from(stmt.label.as_str()),
        condition: stmt.condition.clone(),
        action: stmt.action.clone(),
    };
    shared
        .inner()
        .write()
        .trigger_registry_mut()
        .register(trigger)
        .map_err(|e| GqlError::internal(e.to_string()))?;
    shared.publish_snapshot();
    crate::runtime::triggers::invalidate_trigger_caches();
    Ok(GqlResult::empty())
}

pub(super) fn drop_trigger(shared: &SharedGraph, name: &str) -> Result<GqlResult, GqlError> {
    shared
        .inner()
        .write()
        .trigger_registry_mut()
        .remove(name)
        .map_err(|e| GqlError::internal(e.to_string()))?;
    shared.publish_snapshot();
    crate::runtime::triggers::invalidate_trigger_caches();
    Ok(GqlResult::empty())
}

pub(super) fn show_triggers(shared: &SharedGraph) -> Result<GqlResult, GqlError> {
    let snapshot = shared.load_snapshot();
    let triggers = snapshot.trigger_registry().list();
    let names: Vec<String> = triggers.iter().map(|t| t.name.to_string()).collect();
    let events: Vec<String> = triggers.iter().map(|t| format!("{:?}", t.event)).collect();
    let labels: Vec<String> = triggers.iter().map(|t| t.label.to_string()).collect();
    let conditions: Vec<String> = triggers
        .iter()
        .map(|t| t.condition.clone().unwrap_or_default())
        .collect();
    let actions: Vec<String> = triggers.iter().map(|t| t.action.clone()).collect();

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("event", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("label", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("condition", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("action", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(names)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(events)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(labels)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(conditions)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(actions)) as arrow::array::ArrayRef,
        ],
    )
    .map_err(|e| GqlError::internal(format!("arrow: {e}")))?;

    Ok(GqlResult {
        schema,
        batches: vec![batch],
        status: GqlStatus::success(triggers.len()),
        mutations: MutationStats::default(),
        profile: None,
        changes: vec![],
    })
}

// ── Node Type DDL ───────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(super) fn create_node_type(
    shared: &SharedGraph,
    label: &str,
    parent: Option<&str>,
    properties: &[DdlPropertyDef],
    or_replace: bool,
    if_not_exists: bool,
    validation_mode: Option<selene_core::ValidationMode>,
) -> Result<GqlResult, GqlError> {
    if let Some(parent_label) = parent {
        let snap = shared.load_snapshot();
        if snap.schema().node_schema(parent_label).is_none() {
            return Err(GqlError::internal(format!(
                "parent type ':{parent_label}' does not exist, register it first"
            )));
        }
        if snap.schema().has_inheritance_cycle(label, parent_label) {
            return Err(GqlError::internal(format!(
                "inheritance cycle: ':{label}' \u{2192} ... \u{2192} ':{label}'"
            )));
        }
    }

    let props = build_property_defs(properties)?;
    let schema = selene_core::NodeSchema {
        label: Arc::from(label),
        parent: parent.map(Arc::from),
        properties: props,
        valid_edge_labels: Vec::new(),
        description: String::new(),
        annotations: HashMap::new(),
        version: Default::default(),
        validation_mode,
        key_properties: Vec::new(),
    };

    {
        let mut guard = shared.inner().write();
        if or_replace {
            guard
                .schema_mut()
                .register_node_schema(schema)
                .map_err(|e| GqlError::internal(e.to_string()))?;
        } else if if_not_exists {
            guard
                .schema_mut()
                .register_node_schema_if_new(schema)
                .map_err(|e| GqlError::internal(e.to_string()))?;
        } else {
            let is_new = guard
                .schema_mut()
                .register_node_schema_if_new(schema)
                .map_err(|e| GqlError::internal(e.to_string()))?;
            if !is_new {
                return Err(GqlError::internal(format!(
                    "node type ':{label}' already exists, use OR REPLACE to overwrite"
                )));
            }
        }
        guard.build_property_indexes();
        guard.build_composite_indexes();
    }
    shared.publish_snapshot();

    Ok(GqlResult::empty())
}

pub(super) fn drop_node_type(
    shared: &SharedGraph,
    label: &str,
    if_exists: bool,
) -> Result<GqlResult, GqlError> {
    {
        let mut guard = shared.inner().write();
        let removed = guard.schema_mut().unregister_node_schema(label);
        if removed.is_none() && !if_exists {
            return Err(GqlError::internal(format!(
                "node type ':{label}' does not exist"
            )));
        }
        guard.build_property_indexes();
        guard.build_composite_indexes();
    }
    shared.publish_snapshot();

    Ok(GqlResult::empty())
}

pub(super) fn show_node_types(shared: &SharedGraph) -> Result<GqlResult, GqlError> {
    let snap = shared.load_snapshot();
    let schemas: Vec<&selene_core::NodeSchema> = snap.schema().all_node_schemas().collect();

    let labels: Vec<String> = schemas.iter().map(|s| s.label.to_string()).collect();
    let parents: Vec<String> = schemas
        .iter()
        .map(|s| s.parent.as_ref().map(|p| p.to_string()).unwrap_or_default())
        .collect();
    let prop_counts: Vec<i64> = schemas.iter().map(|s| s.properties.len() as i64).collect();
    let descriptions: Vec<String> = schemas.iter().map(|s| s.description.clone()).collect();

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("label", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("parent", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("property_count", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("description", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(labels)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(parents)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::Int64Array::from(prop_counts)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(descriptions)) as arrow::array::ArrayRef,
        ],
    )
    .map_err(|e| GqlError::internal(format!("arrow: {e}")))?;

    Ok(GqlResult {
        schema,
        batches: vec![batch],
        status: GqlStatus::success(schemas.len()),
        mutations: MutationStats::default(),
        profile: None,
        changes: vec![],
    })
}

// ── Edge Type DDL ───────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(super) fn create_edge_type(
    shared: &SharedGraph,
    label: &str,
    source_labels: &[String],
    target_labels: &[String],
    properties: &[DdlPropertyDef],
    or_replace: bool,
    if_not_exists: bool,
    validation_mode: Option<selene_core::ValidationMode>,
) -> Result<GqlResult, GqlError> {
    let props = build_property_defs(properties)?;
    let schema = selene_core::EdgeSchema {
        label: Arc::from(label),
        properties: props,
        description: String::new(),
        source_labels: source_labels
            .iter()
            .map(|s| Arc::from(s.as_str()))
            .collect(),
        target_labels: target_labels
            .iter()
            .map(|s| Arc::from(s.as_str()))
            .collect(),
        annotations: HashMap::new(),
        version: Default::default(),
        validation_mode,
        max_out_degree: None,
        max_in_degree: None,
        min_out_degree: None,
        min_in_degree: None,
    };

    {
        let mut guard = shared.inner().write();
        if or_replace {
            guard
                .schema_mut()
                .register_edge_schema(schema)
                .map_err(|e| GqlError::internal(e.to_string()))?;
        } else if if_not_exists {
            guard
                .schema_mut()
                .register_edge_schema_if_new(schema)
                .map_err(|e| GqlError::internal(e.to_string()))?;
        } else {
            let is_new = guard
                .schema_mut()
                .register_edge_schema_if_new(schema)
                .map_err(|e| GqlError::internal(e.to_string()))?;
            if !is_new {
                return Err(GqlError::internal(format!(
                    "edge type ':{label}' already exists, use OR REPLACE to overwrite"
                )));
            }
        }
        guard.build_property_indexes();
        guard.build_composite_indexes();
    }
    shared.publish_snapshot();

    Ok(GqlResult::empty())
}

pub(super) fn drop_edge_type(
    shared: &SharedGraph,
    label: &str,
    if_exists: bool,
) -> Result<GqlResult, GqlError> {
    {
        let mut guard = shared.inner().write();
        let removed = guard.schema_mut().unregister_edge_schema(label);
        if removed.is_none() && !if_exists {
            return Err(GqlError::internal(format!(
                "edge type ':{label}' does not exist"
            )));
        }
    }
    shared.publish_snapshot();

    Ok(GqlResult::empty())
}

pub(super) fn show_edge_types(shared: &SharedGraph) -> Result<GqlResult, GqlError> {
    let snap = shared.load_snapshot();
    let schemas: Vec<&selene_core::EdgeSchema> = snap.schema().all_edge_schemas().collect();

    let labels: Vec<String> = schemas.iter().map(|s| s.label.to_string()).collect();
    let sources: Vec<String> = schemas
        .iter()
        .map(|s| {
            s.source_labels
                .iter()
                .map(|l| format!(":{l}"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect();
    let targets: Vec<String> = schemas
        .iter()
        .map(|s| {
            s.target_labels
                .iter()
                .map(|l| format!(":{l}"))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect();
    let prop_counts: Vec<i64> = schemas.iter().map(|s| s.properties.len() as i64).collect();

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("label", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("source_labels", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("target_labels", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("property_count", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(labels)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(sources)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(targets)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::Int64Array::from(prop_counts)) as arrow::array::ArrayRef,
        ],
    )
    .map_err(|e| GqlError::internal(format!("arrow: {e}")))?;

    Ok(GqlResult {
        schema,
        batches: vec![batch],
        status: GqlStatus::success(schemas.len()),
        mutations: MutationStats::default(),
        profile: None,
        changes: vec![],
    })
}

// ── Materialized View DDL ───────────────────────────────────────────

pub(super) fn create_materialized_view(
    shared: &SharedGraph,
    name: &str,
    or_replace: bool,
    if_not_exists: bool,
    definition_text: &str,
    match_clause: &crate::ast::pattern::MatchClause,
    return_clause: &ReturnClause,
) -> Result<GqlResult, GqlError> {
    let upper = name.to_uppercase();

    let mut match_labels = Vec::new();
    for pat in &match_clause.patterns {
        for elem in &pat.elements {
            if let crate::ast::pattern::PatternElement::Node(np) = elem
                && let Some(crate::ast::pattern::LabelExpr::Name(lbl)) = &np.labels
            {
                let s = lbl.as_str().to_string();
                if !match_labels.contains(&s) {
                    match_labels.push(s);
                }
            }
        }
    }

    let mut predicate_properties = Vec::new();
    if let Some(ref pred) = match_clause.where_clause {
        collect_predicate_properties(pred, &mut predicate_properties);
    }

    let aggregates = extract_view_aggregates(&return_clause.projections);

    if aggregates
        .iter()
        .any(|a| a.kind == selene_graph::ViewAggregateKind::FullRecompute)
    {
        return Err(GqlError::InvalidArgument {
            message:
                "materialized views only support count, sum, avg, min, max, and collect aggregates"
                    .into(),
        });
    }

    let def = selene_graph::ViewDefinition {
        name: upper.clone(),
        definition_text: definition_text.to_string(),
        match_labels,
        predicate_properties,
        aggregates,
    };

    {
        let mut guard = shared.inner().write();
        if or_replace {
            guard.view_registry_mut().register_or_replace(def);
        } else if if_not_exists {
            let _ = guard.view_registry_mut().register(def);
        } else {
            guard
                .view_registry_mut()
                .register(def)
                .map_err(|e| GqlError::internal(e.to_string()))?;
        }
    }
    shared.publish_snapshot();

    Ok(GqlResult::ddl_success(&format!(
        "materialized view '{name}' created"
    )))
}

pub(super) fn drop_materialized_view(
    shared: &SharedGraph,
    name: &str,
    if_exists: bool,
) -> Result<GqlResult, GqlError> {
    let upper = name.to_uppercase();
    {
        let mut guard = shared.inner().write();
        if if_exists {
            guard.view_registry_mut().remove_if_exists(&upper);
        } else {
            guard
                .view_registry_mut()
                .remove(&upper)
                .map_err(|e| GqlError::internal(e.to_string()))?;
        }
    }
    shared.publish_snapshot();

    Ok(GqlResult::ddl_success(&format!(
        "materialized view '{name}' dropped"
    )))
}

pub(super) fn show_materialized_views(shared: &SharedGraph) -> Result<GqlResult, GqlError> {
    let snapshot = shared.load_snapshot();
    let views = snapshot.view_registry().list();

    let names: Vec<String> = views.iter().map(|v| v.name.clone()).collect();
    let definitions: Vec<String> = views.iter().map(|v| v.definition_text.clone()).collect();
    let agg_summaries: Vec<String> = views
        .iter()
        .map(|v| {
            v.aggregates
                .iter()
                .map(|a| {
                    let src = a.source_property.as_deref().unwrap_or("*");
                    format!("{}({}) AS {}", format_agg_kind(a.kind), src, a.alias)
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect();

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("definition", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("aggregates", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(names)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(definitions)) as arrow::array::ArrayRef,
            Arc::new(arrow::array::StringArray::from(agg_summaries)) as arrow::array::ArrayRef,
        ],
    )
    .map_err(|e| GqlError::internal(format!("arrow: {e}")))?;

    Ok(GqlResult {
        schema,
        batches: vec![batch],
        status: GqlStatus::success(views.len()),
        mutations: MutationStats::default(),
        profile: None,
        changes: vec![],
    })
}

// ── DDL helper functions ────────────────────────────────────────────

/// Extract aggregate descriptors from RETURN projections for view registration.
fn extract_view_aggregates(projections: &[Projection]) -> Vec<selene_graph::ViewAggregate> {
    let mut aggs = Vec::new();
    for proj in projections {
        let alias = proj
            .alias
            .map(|a| a.as_str().to_string())
            .unwrap_or_default();
        match &proj.expr {
            Expr::Function(f) if f.count_star => {
                aggs.push(selene_graph::ViewAggregate {
                    alias,
                    kind: selene_graph::ViewAggregateKind::CountStar,
                    source_property: None,
                });
            }
            Expr::Aggregate(agg) => {
                let kind = match agg.op {
                    AggregateOp::Count => selene_graph::ViewAggregateKind::Count,
                    AggregateOp::Sum => selene_graph::ViewAggregateKind::Sum,
                    AggregateOp::Avg => selene_graph::ViewAggregateKind::Avg,
                    AggregateOp::Min => selene_graph::ViewAggregateKind::Min,
                    AggregateOp::Max => selene_graph::ViewAggregateKind::Max,
                    AggregateOp::CollectList => selene_graph::ViewAggregateKind::Collect,
                    AggregateOp::StddevSamp | AggregateOp::StddevPop => {
                        selene_graph::ViewAggregateKind::FullRecompute
                    }
                };
                let source_property = agg.expr.as_ref().and_then(|e| match e.as_ref() {
                    Expr::Property(_, prop) => Some(prop.as_str().to_string()),
                    _ => None,
                });
                aggs.push(selene_graph::ViewAggregate {
                    alias,
                    kind,
                    source_property,
                });
            }
            _ => {
                aggs.push(selene_graph::ViewAggregate {
                    alias,
                    kind: selene_graph::ViewAggregateKind::FullRecompute,
                    source_property: None,
                });
            }
        }
    }
    aggs
}

/// Collect property names referenced in a WHERE predicate (for change filtering).
fn collect_predicate_properties(expr: &Expr, out: &mut Vec<String>) {
    expr.walk(&mut |e| {
        if let Expr::Property(_, prop) = e {
            let s = prop.as_str().to_string();
            if !out.contains(&s) {
                out.push(s);
            }
        }
    });
}

/// Format a `ViewAggregateKind` for display.
fn format_agg_kind(kind: selene_graph::ViewAggregateKind) -> &'static str {
    match kind {
        selene_graph::ViewAggregateKind::Count => "count",
        selene_graph::ViewAggregateKind::CountStar => "count",
        selene_graph::ViewAggregateKind::Sum => "sum",
        selene_graph::ViewAggregateKind::Avg => "avg",
        selene_graph::ViewAggregateKind::Min => "min",
        selene_graph::ViewAggregateKind::Max => "max",
        selene_graph::ViewAggregateKind::Collect => "collect",
        selene_graph::ViewAggregateKind::FullRecompute => "recompute",
    }
}

/// Convert parsed DDL property definitions to core PropertyDef values.
pub(super) fn build_property_defs(
    ddl_props: &[DdlPropertyDef],
) -> Result<Vec<selene_core::PropertyDef>, GqlError> {
    ddl_props
        .iter()
        .map(|d| {
            let value_type = map_ddl_value_type(&d.value_type)?;
            let default = if let Some(ref expr) = d.default {
                match expr {
                    Expr::Literal(gql_val) => {
                        let val = selene_core::Value::try_from(gql_val)
                            .map_err(|e| GqlError::internal(format!("default value: {e}")))?;
                        Some(val)
                    }
                    _ => return Err(GqlError::internal("DEFAULT must be a literal value")),
                }
            } else {
                None
            };

            Ok(selene_core::PropertyDef {
                name: Arc::from(d.name.as_str()),
                value_type,
                required: d.required,
                default,
                description: String::new(),
                indexed: d.indexed,
                unique: d.unique,
                min: None,
                max: None,
                min_length: None,
                max_length: None,
                allowed_values: Vec::new(),
                pattern: None,
                immutable: d.immutable,
                searchable: d.searchable,
                dictionary: d.dictionary,
                fill: d.fill.as_ref().and_then(|s| match s.as_str() {
                    "LOCF" => Some(selene_core::FillStrategy::Locf),
                    "LINEAR" => Some(selene_core::FillStrategy::Linear),
                    _ => None,
                }),
                expected_interval_nanos: d
                    .expected_interval
                    .as_ref()
                    .and_then(|s| crate::runtime::eval::parse_duration(s).ok()),
                encoding: match d.encoding.as_deref() {
                    None | Some("GORILLA") => selene_core::ValueEncoding::Gorilla,
                    Some("RLE") => selene_core::ValueEncoding::Rle,
                    Some("DICTIONARY") => selene_core::ValueEncoding::Dictionary,
                    Some(other) => {
                        return Err(GqlError::internal(format!(
                            "unknown ENCODING '{other}', expected GORILLA, RLE, or DICTIONARY"
                        )));
                    }
                },
            })
        })
        .collect()
}

/// Map a DDL type name string to the core ValueType enum.
fn map_ddl_value_type(s: &str) -> Result<selene_core::ValueType, GqlError> {
    s.parse().map_err(|e: String| GqlError::internal(e))
}
