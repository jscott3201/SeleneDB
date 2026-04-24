//! Schema management tool implementations: create, update, export, import.

use std::fmt::Write;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, Content};

use crate::http::mcp::format::{structured_result, structured_text_result};
use crate::http::mcp::params::*;
use crate::http::mcp::{SeleneTools, mcp_auth, op_err, reject_replica};
use crate::ops;

pub(super) async fn list_schemas_impl(tools: &SeleneTools) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let node_schemas = ops::schema::list_node_schemas(&tools.state, &auth).map_err(op_err)?;
    let edge_schemas = ops::schema::list_edge_schemas(&tools.state, &auth).map_err(op_err)?;
    Ok(structured_result(serde_json::json!({
        "node_schemas": node_schemas.iter().map(|s| {
            let mut obj = serde_json::json!({
                "label": &*s.label,
                "description": &s.description,
                "properties": s.properties.len(),
                "parent": s.parent.as_deref(),
            });
            if !s.annotations.is_empty() {
                let annot: serde_json::Map<String, serde_json::Value> = s.annotations.iter()
                    .map(|(k, v)| (k.to_string(), crate::ops::value_to_json(v)))
                    .collect();
                obj["annotations"] = serde_json::Value::Object(annot);
            }
            obj
        }).collect::<Vec<_>>(),
        "edge_schemas": edge_schemas.iter().map(|s| {
            serde_json::json!({
                "label": &*s.label,
                "description": &s.description,
            })
        }).collect::<Vec<_>>(),
    })))
}

pub(super) async fn get_schema_impl(
    tools: &SeleneTools,
    p: SchemaLabelParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let label = &p.label;

    // Try node schema first
    if let Ok(schema) = ops::schema::get_node_schema(&tools.state, &auth, label) {
        return Ok(structured_result(serde_json::json!({
            "type": "node",
            "schema": schema,
        })));
    }

    // Fallback to edge schema
    let schema = ops::schema::get_edge_schema(&tools.state, &auth, label).map_err(op_err)?;
    Ok(structured_result(serde_json::json!({
        "type": "edge",
        "schema": schema,
    })))
}

pub(super) async fn create_schema_impl(
    tools: &SeleneTools,
    p: CreateSchemaParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    // Parse fields using compact shorthand
    let mut properties = Vec::new();
    for (name, spec) in &p.fields {
        let prop = selene_packs::parse_field_spec(name, spec)
            .map_err(|e| op_err(ops::OpError::InvalidRequest(format!("field '{name}': {e}"))))?;
        properties.push(prop);
    }
    properties.sort_by(|a, b| a.name.cmp(&b.name));

    let mut annotations = std::collections::HashMap::new();
    for (k, v) in p.annotations {
        let value = crate::ops::json_to_value(v);
        annotations.insert(std::sync::Arc::from(k.as_str()), value);
    }

    let schema = selene_core::schema::NodeSchema {
        label: std::sync::Arc::from(p.label.as_str()),
        parent: p.extends.map(|e| std::sync::Arc::from(e.as_str())),
        properties,
        valid_edge_labels: p
            .edges
            .into_iter()
            .map(|e| std::sync::Arc::from(e.as_str()))
            .collect(),
        description: p.description.unwrap_or_default(),
        annotations,
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    let label = p.label.clone();
    let st = Arc::clone(&tools.state);
    // `register_node_schema` is idempotent: a byte-equal duplicate
    // returns `AlreadyExistsEqual` instead of erroring, so a defensive
    // agent that pre-calls create_schema before batch_ingest gets a
    // clean no-op rather than a failure it has to reason about.
    let outcome = tools
        .submit_mut(move || ops::schema::register_node_schema(&st, &auth, schema))
        .await?;
    let read_auth = mcp_auth(tools)?;
    let registered =
        ops::schema::get_node_schema(&tools.state, &read_auth, &label).map_err(op_err)?;

    let (status, action, human) = match outcome {
        ops::schema::SchemaRegisterOutcome::Created => {
            let prop_count = registered.properties.len();
            (
                "created",
                "registered",
                format!(
                    "Created schema '{label}' with {prop_count} properties. \
                     Nodes with this label will be validated on write."
                ),
            )
        }
        ops::schema::SchemaRegisterOutcome::AlreadyExistsEqual => (
            "already_exists",
            "no-op",
            format!("Schema '{label}' already exists with the proposed shape — no change made."),
        ),
    };

    Ok(structured_text_result(
        human,
        serde_json::json!({
            "status": status,
            "action": action,
            "label": label,
            "schema": registered,
        }),
    ))
}

pub(super) async fn update_schema_impl(
    tools: &SeleneTools,
    p: UpdateSchemaParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let mut properties = Vec::new();
    for (name, spec) in &p.fields {
        let prop = selene_packs::parse_field_spec(name, spec)
            .map_err(|e| op_err(ops::OpError::InvalidRequest(format!("field '{name}': {e}"))))?;
        properties.push(prop);
    }
    properties.sort_by(|a, b| a.name.cmp(&b.name));

    let mut annotations = std::collections::HashMap::new();
    for (k, v) in p.annotations {
        let value = crate::ops::json_to_value(v);
        annotations.insert(std::sync::Arc::from(k.as_str()), value);
    }

    let schema = selene_core::schema::NodeSchema {
        label: std::sync::Arc::from(p.label.as_str()),
        parent: p.extends.map(|e| std::sync::Arc::from(e.as_str())),
        properties,
        valid_edge_labels: p
            .edges
            .into_iter()
            .map(|e| std::sync::Arc::from(e.as_str()))
            .collect(),
        description: p.description.unwrap_or_default(),
        annotations,
        version: Default::default(),
        validation_mode: None,
        key_properties: vec![],
    };

    let label = p.label.clone();
    let st = std::sync::Arc::clone(&tools.state);
    tools
        .submit_mut(move || ops::schema::register_node_schema_force(&st, &auth, schema))
        .await?;

    Ok(CallToolResult::success(vec![Content::text(format!(
        "Updated schema '{label}'"
    ))]))
}

pub(super) async fn export_schemas_impl(tools: &SeleneTools) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    let node_schemas = ops::schema::list_node_schemas(&tools.state, &auth).map_err(op_err)?;
    let edge_schemas = ops::schema::list_edge_schemas(&tools.state, &auth).map_err(op_err)?;

    // Build compact format
    let mut types = serde_json::Map::new();
    for schema in &node_schemas {
        let mut fields = serde_json::Map::new();
        for prop in &schema.properties {
            let mut spec = format!("{:?}", prop.value_type).to_lowercase();
            if prop.required {
                spec.push('!');
            }
            if let Some(ref default) = prop.default {
                let _ = write!(spec, " = {default}");
            }
            fields.insert(prop.name.to_string(), serde_json::Value::String(spec));
        }

        let mut type_def = serde_json::Map::new();
        if let Some(ref parent) = schema.parent {
            type_def.insert("extends".into(), serde_json::json!(parent.as_ref()));
        }
        if !schema.description.is_empty() {
            type_def.insert("description".into(), serde_json::json!(schema.description));
        }
        if !schema.annotations.is_empty() {
            let annot_map: serde_json::Map<String, serde_json::Value> = schema
                .annotations
                .iter()
                .map(|(k, v)| (k.to_string(), crate::ops::value_to_json(v)))
                .collect();
            type_def.insert("annotations".into(), serde_json::Value::Object(annot_map));
        }
        if !fields.is_empty() {
            type_def.insert("fields".into(), serde_json::Value::Object(fields));
        }

        types.insert(
            schema.label.to_string(),
            serde_json::Value::Object(type_def),
        );
    }

    let mut relationships = serde_json::Map::new();
    for schema in &edge_schemas {
        let mut edge_def = serde_json::Map::new();
        if !schema.description.is_empty() {
            edge_def.insert("description".into(), serde_json::json!(schema.description));
        }
        if !schema.source_labels.is_empty() {
            edge_def.insert(
                "source".into(),
                serde_json::json!(
                    schema
                        .source_labels
                        .iter()
                        .map(|l| l.as_ref())
                        .collect::<Vec<_>>()
                ),
            );
        }
        if !schema.target_labels.is_empty() {
            edge_def.insert(
                "target".into(),
                serde_json::json!(
                    schema
                        .target_labels
                        .iter()
                        .map(|l| l.as_ref())
                        .collect::<Vec<_>>()
                ),
            );
        }
        relationships.insert(
            schema.label.to_string(),
            serde_json::Value::Object(edge_def),
        );
    }

    let export = serde_json::json!({
        "name": "exported",
        "version": "1.0",
        "types": types,
        "relationships": relationships,
    });

    Ok(structured_result(export))
}

pub(super) async fn create_edge_schema_impl(
    tools: &SeleneTools,
    p: CreateEdgeSchemaParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;

    let mut properties = Vec::new();
    for (name, spec) in &p.fields {
        let prop = selene_packs::parse_field_spec(name, spec)
            .map_err(|e| op_err(ops::OpError::InvalidRequest(format!("field '{name}': {e}"))))?;
        properties.push(prop);
    }
    properties.sort_by(|a, b| a.name.cmp(&b.name));

    let schema = selene_core::schema::EdgeSchema {
        label: std::sync::Arc::from(p.label.as_str()),
        properties,
        description: p.description.unwrap_or_default(),
        source_labels: p
            .source_labels
            .into_iter()
            .map(|s| std::sync::Arc::from(s.as_str()))
            .collect(),
        target_labels: p
            .target_labels
            .into_iter()
            .map(|s| std::sync::Arc::from(s.as_str()))
            .collect(),
        annotations: std::collections::HashMap::new(),
        version: Default::default(),
        validation_mode: None,
        max_out_degree: None,
        max_in_degree: None,
        min_out_degree: p.min_out_degree,
        min_in_degree: p.min_in_degree,
    };

    let label = p.label.clone();
    let st = Arc::clone(&tools.state);
    tools
        .submit_mut(move || ops::schema::register_edge_schema(&st, &auth, schema))
        .await?;

    Ok(CallToolResult::success(vec![Content::text(format!(
        "Created edge schema '{label}'"
    ))]))
}

pub(super) async fn import_schema_pack_impl(
    tools: &SeleneTools,
    p: ImportPackParams,
) -> Result<CallToolResult, McpError> {
    let auth = mcp_auth(tools)?;
    reject_replica(&tools.state)?;
    let pack = selene_packs::load_from_str(&p.content).map_err(|e| {
        op_err(ops::OpError::InvalidRequest(format!(
            "invalid schema pack: {e}"
        )))
    })?;
    let st = Arc::clone(&tools.state);
    let result = tools
        .submit_mut(move || ops::schema::import_pack(&st, &auth, pack))
        .await?;
    let text = format!(
        "Imported pack '{}': {} node schemas ({} skipped), {} edge schemas ({} skipped)",
        result.pack_name,
        result.node_schemas_registered,
        result.node_schemas_skipped,
        result.edge_schemas_registered,
        result.edge_schemas_skipped
    );
    Ok(structured_text_result(
        text,
        serde_json::json!({
            "pack_name": result.pack_name,
            "node_schemas_registered": result.node_schemas_registered,
            "node_schemas_skipped": result.node_schemas_skipped,
            "edge_schemas_registered": result.edge_schemas_registered,
            "edge_schemas_skipped": result.edge_schemas_skipped,
        }),
    ))
}
