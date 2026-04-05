//! MCP tool implementations for graph, time-series, schema, and data operations.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, ErrorCode};
use rmcp::{ErrorData as McpError, tool, tool_router};

use super::params::*;
use super::{SeleneTools, mcp_auth, op_err, reject_replica};
use crate::ops;
use crate::ops::json_to_value;
use selene_core::Value;

#[tool_router]
impl SeleneTools {
    pub(crate) fn build_tool_router() -> rmcp::handler::server::tool::ToolRouter<Self> {
        Self::tool_router()
    }

    // ── GQL ──────────────────────────────────────────────────────────

    #[tool(
        name = "gql_query",
        description = "Execute a GQL query against the property graph. Primary query interface. Examples: 'MATCH (s:sensor) RETURN s.name AS name', 'MATCH (b:building)-[:contains]->(f:floor) RETURN b.name AS building, f.name AS floor', 'INSERT (:sensor {name: \"NewSensor\", temp: 72.5})', 'MATCH (s:sensor) FILTER s.temp > 72 SET s.alert = TRUE'. Returns GQLSTATUS and JSON results."
    )]
    async fn gql_query(&self, params: Parameters<GqlParams>) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = params.0;
        let gql_params = p.parameters.as_ref().map(|map| {
            map.iter()
                .map(|(k, v)| (k.clone(), ops::json_to_value(v.clone())))
                .collect::<HashMap<String, selene_core::Value>>()
        });
        let query = p.query;
        let timeout_ms = p.timeout_ms;

        // Route write queries through the mutation batcher for serialized ordering
        let result = if crate::http::routes::is_gql_write(&query) {
            let st = Arc::clone(&self.state);
            let auth2 = auth.clone();
            self.submit_mut(move || {
                ops::gql::execute_gql_with_timeout(
                    &st,
                    &auth2,
                    &query,
                    gql_params.as_ref(),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                    timeout_ms,
                )
            })
            .await?
        } else {
            ops::gql::execute_gql_with_timeout(
                &self.state,
                &auth,
                &query,
                gql_params.as_ref(),
                false,
                false,
                ops::gql::ResultFormat::Json,
                timeout_ms,
            )
            .map_err(op_err)?
        };
        let data = result.data_json.unwrap_or_else(|| "[]".to_string());
        let mut text = format!("Status: {} — {}\n", result.status_code, result.message);
        if let Some(m) = &result.mutations {
            let _ = writeln!(
                text,
                "Mutations: {} nodes created, {} deleted, {} edges created, {} deleted, {} props set, {} removed",
                m.nodes_created,
                m.nodes_deleted,
                m.edges_created,
                m.edges_deleted,
                m.properties_set,
                m.properties_removed
            );
        }
        let _ = write!(text, "{data}\n({} rows)", result.row_count);
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        name = "gql_explain",
        description = "Show the execution plan for a GQL query without executing it. Useful for understanding how queries are optimized. Example: 'MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name AS name'"
    )]
    async fn gql_explain(
        &self,
        params: Parameters<GqlExplainParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let result = ops::gql::execute_gql(
            &self.state,
            &auth,
            &params.0.query,
            None,
            true,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;
        let plan = result
            .plan
            .unwrap_or_else(|| "No plan available".to_string());
        Ok(CallToolResult::success(vec![Content::text(plan)]))
    }

    // ── Node CRUD ────────────────────────────────────────────────────

    #[tool(
        name = "get_node",
        description = "Get a node by its numeric ID. Returns the node's labels, properties, timestamps, and version."
    )]
    async fn get_node(&self, params: Parameters<NodeIdParams>) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let node = ops::nodes::get_node(&self.state, &auth, params.0.id).map_err(op_err)?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&node).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "create_node",
        description = "Create a new node with labels and optional properties. Properties are flat key-value pairs (nested objects are stored as JSON strings). Use parent_id to place it in the containment hierarchy (auto-creates a 'contains' edge). Schema defaults are applied automatically."
    )]
    async fn create_node(
        &self,
        params: Parameters<CreateNodeParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let label_strs: Vec<&str> = params.0.labels.iter().map(|s| s.as_str()).collect();
        let labels = selene_core::LabelSet::from_strs(&label_strs);
        let schema = self.state.graph.read(|g| {
            let label = params.0.labels.first().map_or("", |s| s.as_str());
            g.schema().node_schema(label).cloned()
        });
        let props =
            ops::json_props_with_schema(params.0.properties, schema.as_ref()).map_err(op_err)?;
        let parent_id = params.0.parent_id;
        let st = Arc::clone(&self.state);
        let node = self
            .submit_mut(move || ops::nodes::create_node(&st, &auth, labels, props, parent_id))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&node).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "modify_node",
        description = "Modify a node: set/remove properties, add/remove labels. All fields are optional — only specified changes are applied."
    )]
    async fn modify_node(
        &self,
        params: Parameters<ModifyNodeParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let p = params.0;
        let mut set_props: Vec<(selene_core::IStr, Value)> = p
            .set_properties
            .into_iter()
            .map(|(k, v)| {
                let key = selene_core::try_intern(&k).ok_or_else(|| {
                    op_err(crate::ops::OpError::InvalidRequest(
                        "interner at capacity: too many unique property keys".into(),
                    ))
                })?;
                Ok((key, json_to_value(v)))
            })
            .collect::<Result<Vec<_>, McpError>>()?;
        ops::nodes::prepare_modify_node_props(&self.state, p.id, &mut set_props);
        let add_labels: Vec<selene_core::IStr> = p
            .add_labels
            .iter()
            .map(|s| selene_core::IStr::new(s))
            .collect();
        let remove_props = p.remove_properties;
        let remove_lbls = p.remove_labels;
        let node_id = p.id;
        let st = Arc::clone(&self.state);
        let node = self
            .submit_mut(move || {
                ops::nodes::modify_node(
                    &st,
                    &auth,
                    node_id,
                    set_props,
                    remove_props,
                    add_labels,
                    remove_lbls,
                )
            })
            .await?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&node).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "delete_node",
        description = "Delete a node and all its connected edges. This is irreversible."
    )]
    async fn delete_node(
        &self,
        params: Parameters<NodeIdParams>,
    ) -> Result<CallToolResult, McpError> {
        let node_id = params.0.id;
        self.mutate(move |st, auth| ops::nodes::delete_node(st, auth, node_id))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Deleted node {node_id}"
        ))]))
    }

    #[tool(
        name = "list_nodes",
        description = "List nodes, optionally filtered by label. Use limit/offset for pagination. Returns node objects with all properties."
    )]
    async fn list_nodes(
        &self,
        params: Parameters<ListNodesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let result = ops::nodes::list_nodes(
            &self.state,
            &auth,
            params.0.label.as_deref(),
            params.0.limit.unwrap_or(100).min(10_000) as usize,
            params.0.offset.unwrap_or(0) as usize,
        )
        .map_err(op_err)?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&serde_json::json!({
                "nodes": result.nodes,
                "total": result.total,
            }))
            .unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "node_edges",
        description = "Get edges connected to a node (both incoming and outgoing). Uses the adjacency index for fast lookup. Returns edge objects with source, target, label, and properties. Supports pagination via limit/offset; total reflects the full count before pagination."
    )]
    async fn node_edges(
        &self,
        params: Parameters<NodeEdgesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = &params.0;
        let offset = p.offset.unwrap_or(0);
        let limit = p.limit.unwrap_or(1000).min(10_000);
        let result =
            ops::edges::node_edges(&self.state, &auth, p.id, offset, limit).map_err(op_err)?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&serde_json::json!({
                "node_id": p.id,
                "edges": result.edges,
                "total": result.total,
            }))
            .unwrap_or_default(),
        )]))
    }

    // ── Edge CRUD ────────────────────────────────────────────────────

    #[tool(
        name = "get_edge",
        description = "Get an edge by its numeric ID. Returns source, target, label, and properties."
    )]
    async fn get_edge(&self, params: Parameters<EdgeIdParams>) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let edge = ops::edges::get_edge(&self.state, &auth, params.0.id).map_err(op_err)?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&edge).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "create_edge",
        description = "Create a directed edge between two nodes. Common labels: 'contains' (hierarchy), 'feeds' (distribution), 'isPointOf' (sensor→equipment), 'monitors', 'hasLocation'."
    )]
    async fn create_edge(
        &self,
        params: Parameters<CreateEdgeParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let p = params.0;
        let label = selene_core::IStr::new(&p.label);
        let props =
            ops::json_props_with_edge_schema(p.properties, &self.state, label).map_err(op_err)?;
        let source = p.source;
        let target = p.target;
        let st = Arc::clone(&self.state);
        let edge = self
            .submit_mut(move || ops::edges::create_edge(&st, &auth, source, target, label, props))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&edge).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "modify_edge",
        description = "Modify an edge's properties. Set new properties or remove existing ones."
    )]
    async fn modify_edge(
        &self,
        params: Parameters<ModifyEdgeParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let p = params.0;
        let mut set_props: Vec<(selene_core::IStr, Value)> = p
            .set_properties
            .into_iter()
            .map(|(k, v)| {
                let key = selene_core::try_intern(&k).ok_or_else(|| {
                    op_err(crate::ops::OpError::InvalidRequest(
                        "interner at capacity: too many unique property keys".into(),
                    ))
                })?;
                Ok((key, json_to_value(v)))
            })
            .collect::<Result<Vec<_>, McpError>>()?;
        ops::edges::prepare_modify_edge_props(&self.state, p.id, &mut set_props);
        let edge_id = p.id;
        let remove_props = p.remove_properties;
        let st = Arc::clone(&self.state);
        let edge = self
            .submit_mut(move || {
                ops::edges::modify_edge(&st, &auth, edge_id, set_props, remove_props)
            })
            .await?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&edge).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "delete_edge",
        description = "Delete an edge by ID. This is irreversible."
    )]
    async fn delete_edge(
        &self,
        params: Parameters<EdgeIdParams>,
    ) -> Result<CallToolResult, McpError> {
        let edge_id = params.0.id;
        self.mutate(move |st, auth| ops::edges::delete_edge(st, auth, edge_id))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Deleted edge {edge_id}"
        ))]))
    }

    #[tool(
        name = "list_edges",
        description = "List edges, optionally filtered by label. Use limit/offset for pagination."
    )]
    async fn list_edges(
        &self,
        params: Parameters<ListEdgesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let result = ops::edges::list_edges(
            &self.state,
            &auth,
            params.0.label.as_deref(),
            params.0.limit.unwrap_or(100).min(10_000) as usize,
            params.0.offset.unwrap_or(0) as usize,
        )
        .map_err(op_err)?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&serde_json::json!({
                "edges": result.edges,
                "total": result.total,
            }))
            .unwrap_or_default(),
        )]))
    }

    // ── Time-Series ──────────────────────────────────────────────────

    #[tool(
        name = "ts_write",
        description = "Write time-series samples. entity_id must reference an existing node. timestamp_nanos is nanoseconds since Unix epoch (seconds * 1_000_000_000). value is always a float. The entity must exist in the graph."
    )]
    async fn ts_write(
        &self,
        params: Parameters<TsWriteParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let samples: Vec<selene_wire::dto::ts::TsSampleDto> = params
            .0
            .samples
            .into_iter()
            .map(|s| selene_wire::dto::ts::TsSampleDto {
                entity_id: s.entity_id,
                property: s.property,
                timestamp_nanos: s.timestamp_nanos,
                value: s.value,
            })
            .collect();
        let st = Arc::clone(&self.state);
        let count = self
            .submit_mut(move || ops::ts::ts_write(&st, &auth, samples))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Wrote {count} samples"
        ))]))
    }

    #[tool(
        name = "ts_query",
        description = "Query time-series samples for a specific node and property. Returns timestamp/value pairs from the hot tier (retention period is configurable, default 24h). Use start/end timestamps to filter."
    )]
    async fn ts_query(
        &self,
        params: Parameters<TsQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = params.0;
        let samples = ops::ts::ts_range(
            &self.state,
            &auth,
            p.entity_id,
            &p.property,
            p.start.unwrap_or(0),
            p.end.unwrap_or(i64::MAX),
            Some(p.limit.unwrap_or(1000) as usize),
        )
        .map_err(op_err)?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&samples).unwrap_or_default(),
        )]))
    }

    // ── Graph Slice ──────────────────────────────────────────────────

    #[tool(
        name = "graph_slice",
        description = "Get a snapshot of the graph. Slice types: 'full' (everything), 'labels' (nodes with specific labels + connecting edges), 'containment' (subtree from a root node). Supports pagination via limit/offset."
    )]
    async fn graph_slice(
        &self,
        params: Parameters<GraphSliceParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = params.0;
        let slice_type = match p.slice_type.as_str() {
            "full" => selene_wire::dto::graph_slice::SliceType::Full,
            "labels" => selene_wire::dto::graph_slice::SliceType::ByLabels {
                labels: p.labels.unwrap_or_default(),
            },
            "containment" => selene_wire::dto::graph_slice::SliceType::Containment {
                root_id: p.root_id.unwrap_or(1),
                max_depth: p.max_depth,
            },
            other => {
                return Err(McpError {
                    code: ErrorCode::INVALID_PARAMS,
                    message: format!(
                        "invalid slice_type '{other}' — use full, labels, or containment"
                    )
                    .into(),
                    data: None,
                });
            }
        };
        let result =
            ops::graph_slice::graph_slice(&self.state, &auth, &slice_type, p.limit, p.offset);

        let mut resp = serde_json::json!({
            "nodes": result.nodes.len(),
            "edges": result.edges.len(),
        });
        if let Some(total) = result.total_nodes {
            resp["total_nodes"] = serde_json::json!(total);
            resp["total_edges"] = serde_json::json!(result.total_edges);
        }
        // Include full data for small slices, summary for large ones
        if result.nodes.len() <= 200 {
            resp["data"] = serde_json::json!({
                "nodes": result.nodes,
                "edges": result.edges,
            });
        } else {
            resp["note"] = serde_json::json!(
                "Large result set. Use limit/offset for pagination, or use gql_query for filtered access."
            );
        }

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&resp).unwrap_or_default(),
        )]))
    }

    // ── Health ────────────────────────────────────────────────────────

    #[tool(
        name = "health",
        description = "Check server health. Returns uptime, node/edge counts, and status."
    )]
    async fn health(&self) -> Result<CallToolResult, McpError> {
        let resp = ops::health::health(&self.state);
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&resp).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "info",
        description = "Get server metadata: version, runtime profile, dev mode, and enabled feature flags."
    )]
    async fn info(&self) -> Result<CallToolResult, McpError> {
        let info = crate::ops::info::server_info(&self.state);
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&info).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "graph_stats",
        description = "Get graph statistics with per-label breakdowns of node and edge counts. More detailed than health -- shows how many nodes exist for each label."
    )]
    async fn graph_stats(&self) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let stats = ops::graph_stats::graph_stats(&self.state, &auth);
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&serde_json::json!({
                "node_count": stats.node_count,
                "edge_count": stats.edge_count,
                "node_labels": stats.node_labels,
                "edge_labels": stats.edge_labels,
            }))
            .unwrap_or_default(),
        )]))
    }

    // ── React Flow ────────────────────────────────────────────────────

    #[tool(
        name = "export_reactflow",
        description = "Export the graph in React Flow format ({nodes, edges} with id, position, data, source, target, label). Compatible with https://reactflow.dev for visual graph editing. Optionally filter by label."
    )]
    async fn export_reactflow(
        &self,
        params: Parameters<RFExportParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let graph = ops::reactflow::export_reactflow(&self.state, &auth, params.0.label.as_deref());
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&graph).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "import_reactflow",
        description = "Import a React Flow graph ({nodes, edges}). Each node becomes a Selene node (type→label, data→properties). Each edge becomes a Selene edge (label from edge label or 'connected'). Returns a mapping from React Flow IDs to Selene IDs."
    )]
    async fn import_reactflow(
        &self,
        params: Parameters<ops::reactflow::RFGraph>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let graph = params.0;
        let st = Arc::clone(&self.state);
        let result = self
            .submit_mut(move || ops::reactflow::import_reactflow(&st, &auth, graph))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Imported {} nodes, {} edges. ID mapping: {:?}",
            result.nodes_created, result.edges_created, result.id_map
        ))]))
    }

    // ── Schema Management ────────────────────────────────────────────

    #[tool(
        name = "list_schemas",
        description = "List all registered node and edge schemas. Schemas define expected property types, required fields, defaults, and validation rules."
    )]
    async fn list_schemas(&self) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let node_schemas = ops::schema::list_node_schemas(&self.state, &auth).map_err(op_err)?;
        let edge_schemas = ops::schema::list_edge_schemas(&self.state, &auth).map_err(op_err)?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&serde_json::json!({
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
            }))
            .unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "get_schema",
        description = "Get the full definition of a schema by label. Tries node schemas first, then edge schemas. Shows property definitions, types, required flags, defaults, and annotations."
    )]
    async fn get_schema(
        &self,
        params: Parameters<SchemaLabelParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let label = &params.0.label;

        // Try node schema first
        if let Ok(schema) = ops::schema::get_node_schema(&self.state, &auth, label) {
            return Ok(CallToolResult::success(vec![Content::text(
                serde_json::to_string_pretty(&serde_json::json!({
                    "type": "node",
                    "schema": schema,
                }))
                .unwrap_or_default(),
            )]));
        }

        // Fallback to edge schema
        let schema = ops::schema::get_edge_schema(&self.state, &auth, label).map_err(op_err)?;
        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&serde_json::json!({
                "type": "edge",
                "schema": schema,
            }))
            .unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "create_schema",
        description = "Create a new node type schema using field shorthand. Fields: 'string!' (required), 'float = 72.5' (with default), 'bool' (optional). Use 'extends' to inherit from a parent type (e.g., 'equipment', 'point'). Schema validation is applied on node creation and property updates."
    )]
    async fn create_schema(
        &self,
        params: Parameters<CreateSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let p = params.0;

        // Parse fields using compact shorthand
        let mut properties = Vec::new();
        for (name, spec) in &p.fields {
            let prop = selene_packs::parse_field_spec(name, spec).map_err(|e| {
                op_err(ops::OpError::InvalidRequest(format!("field '{name}': {e}")))
            })?;
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
        let st = Arc::clone(&self.state);
        self.submit_mut(move || ops::schema::register_node_schema(&st, &auth, schema))
            .await?;
        let read_auth = mcp_auth(self)?;
        let registered =
            ops::schema::get_node_schema(&self.state, &read_auth, &label).map_err(op_err)?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Created schema '{}' with {} properties. Nodes with this label will be validated on write.",
            label,
            registered.properties.len()
        ))]))
    }

    #[tool(
        name = "update_schema",
        description = "Update an existing node schema. Fields are replaced entirely (not merged). Use get_schema first to see the current definition, then provide the complete updated fields."
    )]
    async fn update_schema(
        &self,
        params: Parameters<UpdateSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let p = params.0;

        let mut properties = Vec::new();
        for (name, spec) in &p.fields {
            let prop = selene_packs::parse_field_spec(name, spec).map_err(|e| {
                op_err(ops::OpError::InvalidRequest(format!("field '{name}': {e}")))
            })?;
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
        let st = std::sync::Arc::clone(&self.state);
        self.submit_mut(move || ops::schema::register_node_schema_force(&st, &auth, schema))
            .await?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Updated schema '{label}'"
        ))]))
    }

    #[tool(
        name = "delete_schema",
        description = "Delete a node schema by label. Validation for this label is removed immediately."
    )]
    async fn delete_schema(
        &self,
        params: Parameters<SchemaLabelParams>,
    ) -> Result<CallToolResult, McpError> {
        let label = params.0.label;
        let label2 = label.clone();
        self.mutate(move |st, auth| ops::schema::unregister_node_schema(st, auth, &label2))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Deleted schema '{label}'"
        ))]))
    }

    #[tool(
        name = "create_edge_schema",
        description = "Create a new edge type schema. Fields use shorthand: 'string!' (required), 'float = 72.5' (with default). Use source_labels/target_labels to constrain which node types can be connected."
    )]
    async fn create_edge_schema(
        &self,
        params: Parameters<CreateEdgeSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let p = params.0;

        let mut properties = Vec::new();
        for (name, spec) in &p.fields {
            let prop = selene_packs::parse_field_spec(name, spec).map_err(|e| {
                op_err(ops::OpError::InvalidRequest(format!("field '{name}': {e}")))
            })?;
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
            min_out_degree: None,
            min_in_degree: None,
        };

        let label = p.label.clone();
        let st = Arc::clone(&self.state);
        self.submit_mut(move || ops::schema::register_edge_schema(&st, &auth, schema))
            .await?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Created edge schema '{label}'"
        ))]))
    }

    #[tool(
        name = "delete_edge_schema",
        description = "Delete an edge schema by label. Validation for this edge type is removed immediately."
    )]
    async fn delete_edge_schema(
        &self,
        params: Parameters<SchemaLabelParams>,
    ) -> Result<CallToolResult, McpError> {
        let label = params.0.label;
        let label2 = label.clone();
        self.mutate(move |st, auth| ops::schema::unregister_edge_schema(st, auth, &label2))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Deleted edge schema '{label}'"
        ))]))
    }

    #[tool(
        name = "export_schemas",
        description = "Export all registered schemas as compact JSON. The output can be saved and re-imported later via import_schema_pack."
    )]
    async fn export_schemas(&self) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let node_schemas = ops::schema::list_node_schemas(&self.state, &auth).map_err(op_err)?;
        let edge_schemas = ops::schema::list_edge_schemas(&self.state, &auth).map_err(op_err)?;

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

        Ok(CallToolResult::success(vec![Content::text(
            serde_json::to_string_pretty(&export).unwrap_or_default(),
        )]))
    }

    #[tool(
        name = "import_schema_pack",
        description = "Import a schema pack from compact JSON or TOML. Auto-detects format. Fields use shorthand: 'string!' (required), 'float = 72.5' (with default)."
    )]
    async fn import_schema_pack(
        &self,
        params: Parameters<ImportPackParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let pack = selene_packs::load_from_str(&params.0.content).map_err(|e| {
            op_err(ops::OpError::InvalidRequest(format!(
                "invalid schema pack: {e}"
            )))
        })?;
        let st = Arc::clone(&self.state);
        let result = self
            .submit_mut(move || ops::schema::import_pack(&st, &auth, pack))
            .await?;
        Ok(CallToolResult::success(vec![Content::text(format!(
            "Imported pack '{}': {} node schemas ({} skipped), {} edge schemas ({} skipped)",
            result.pack_name,
            result.node_schemas_registered,
            result.node_schemas_skipped,
            result.edge_schemas_registered,
            result.edge_schemas_skipped
        ))]))
    }

    // ── CSV Import/Export ─────────────────────────────────────────

    #[tool(
        name = "csv_import",
        description = "Import nodes or edges from CSV data. For nodes: each row becomes a node with the specified label; columns become properties. For edges: CSV must have source_id, target_id, and label columns; additional columns become edge properties. Type inference: integers, floats, and booleans are auto-detected."
    )]
    async fn csv_import(
        &self,
        params: Parameters<McpCsvImportParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let p = params.0;
        let delimiter = p.delimiter.as_bytes().first().copied().unwrap_or(b',');
        let reader = std::io::Cursor::new(p.content.into_bytes());
        let is_edges = p.csv_type == "edges";

        let st = Arc::clone(&self.state);
        let result = if is_edges {
            let config = ops::csv_io::CsvEdgeImportConfig {
                delimiter,
                ..Default::default()
            };
            self.submit_mut(move || ops::csv_io::import_edges_csv(&st, &auth, reader, &config))
                .await?
        } else {
            let label = p.label.ok_or_else(|| {
                op_err(ops::OpError::InvalidRequest(
                    "label is required for node import".into(),
                ))
            })?;
            let config = ops::csv_io::CsvNodeImportConfig {
                label,
                delimiter,
                ..Default::default()
            };
            self.submit_mut(move || ops::csv_io::import_nodes_csv(&st, &auth, reader, &config))
                .await?
        };

        let mut text = format!(
            "Imported: {} nodes created, {} edges created",
            result.nodes_created, result.edges_created
        );
        if result.rows_skipped > 0 {
            let _ = write!(text, ", {} rows skipped", result.rows_skipped);
        }
        if !result.errors.is_empty() {
            let _ = write!(text, "\nErrors: {}", result.errors.join("; "));
        }
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        name = "csv_export",
        description = "Export nodes or edges as CSV. For nodes: columns are id plus all property keys. For edges: columns are id, source, target, label, plus property keys. Optional label filter narrows the export."
    )]
    async fn csv_export(
        &self,
        params: Parameters<McpCsvExportParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = params.0;
        let csv_data = if p.csv_type == "edges" {
            ops::csv_io::export_edges_csv(&self.state, &auth, p.label.as_deref()).map_err(op_err)?
        } else {
            ops::csv_io::export_nodes_csv(&self.state, &auth, p.label.as_deref()).map_err(op_err)?
        };
        Ok(CallToolResult::success(vec![Content::text(csv_data)]))
    }

    // ── Vector Search (feature-gated) ─────────────────────────────

    #[tool(
        name = "semantic_search",
        description = "Search the graph using natural language. Embeds the query text into a vector, finds the most similar nodes, and returns them with their containment path (e.g., building > floor > zone > sensor). Requires the embedding model to be loaded."
    )]
    async fn semantic_search(
        &self,
        params: Parameters<SemanticSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = &params.0;

        if p.k <= 0 {
            return Err(McpError {
                code: rmcp::model::ErrorCode::INVALID_PARAMS,
                message: "k must be a positive integer".into(),
                data: None,
            });
        }

        // Build GQL CALL statement (escape single quotes by doubling)
        let safe_text = p.query_text.replace('\'', "''");
        let query = if let Some(ref label) = p.label {
            let safe_label = label.replace('\'', "''");
            format!(
                "CALL graph.semanticSearch('{safe_text}', {}, '{safe_label}') YIELD nodeId, score, path RETURN nodeId, score, path",
                p.k
            )
        } else {
            format!(
                "CALL graph.semanticSearch('{safe_text}', {}) YIELD nodeId, score, path RETURN nodeId, score, path",
                p.k
            )
        };

        let result = ops::gql::execute_gql(
            &self.state,
            &auth,
            &query,
            None,
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;

        let data = result.data_json.unwrap_or_else(|| "[]".to_string());
        let text = format!(
            "Semantic search for '{}': {} results\n{data}",
            p.query_text, result.row_count
        );
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        name = "similar_nodes",
        description = "Find nodes most similar to a given node based on vector embeddings. Returns the k most similar nodes ranked by cosine similarity."
    )]
    async fn similar_nodes(
        &self,
        params: Parameters<SimilarNodesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = &params.0;

        if p.k <= 0 {
            return Err(McpError {
                code: rmcp::model::ErrorCode::INVALID_PARAMS,
                message: "k must be a positive integer".into(),
                data: None,
            });
        }

        let safe_prop = p.property.replace('\'', "''");
        let query = format!(
            "CALL graph.similarNodes({}, '{safe_prop}', {}) YIELD nodeId, score RETURN nodeId, score",
            p.node_id, p.k
        );

        let result = ops::gql::execute_gql(
            &self.state,
            &auth,
            &query,
            None,
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;

        let data = result.data_json.unwrap_or_else(|| "[]".to_string());
        let text = format!(
            "Similar to node {}: {} results\n{data}",
            p.node_id, result.row_count
        );
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    // ── RDF/SPARQL ────────────────────────────────────────────────────
    // These route through GQL CALL procedures which handle feature-gating
    // at runtime. If the rdf/rdf-sparql features are disabled, the CALL
    // returns an error rather than failing at compile time.

    #[tool(
        name = "export_rdf",
        description = "Export the graph as RDF (Turtle format). Requires the 'rdf' feature to be enabled on the server. Returns serialized RDF triples."
    )]
    async fn export_rdf(&self) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let query = "CALL graph.exportRdf('turtle') YIELD data RETURN data";
        let result = ops::gql::execute_gql(
            &self.state,
            &auth,
            query,
            None,
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;
        let data = result.data_json.unwrap_or_else(|| "[]".to_string());
        Ok(CallToolResult::success(vec![Content::text(data)]))
    }

    #[tool(
        name = "sparql_query",
        description = "Execute a SPARQL query against the graph's RDF view. Requires 'rdf-sparql' feature. Returns JSON results."
    )]
    async fn sparql_query(
        &self,
        params: Parameters<SparqlQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let safe_query = params.0.query.replace('\'', "''");
        let gql = format!("CALL graph.sparql('{safe_query}') YIELD result RETURN result");
        let result = ops::gql::execute_gql(
            &self.state,
            &auth,
            &gql,
            None,
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;
        let data = result.data_json.unwrap_or_else(|| "[]".to_string());
        Ok(CallToolResult::success(vec![Content::text(data)]))
    }

    // ── Text2GQL Toolkit ─────────────────────────────────────────────

    #[tool(
        name = "schema_dump",
        description = "Get a compact, LLM-optimized dump of the graph schema. Returns all node types, edge types, properties, constraints, and statistics in a format designed for minimal token usage. Use before writing GQL queries to understand the data model."
    )]
    async fn schema_dump(&self) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let query = "CALL graph.schemaDump() YIELD schema RETURN schema";
        let result = ops::gql::execute_gql(
            &self.state,
            &auth,
            query,
            None,
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;
        let data = result.data_json.unwrap_or_else(|| "[]".to_string());
        Ok(CallToolResult::success(vec![Content::text(data)]))
    }

    #[tool(
        name = "gql_parse_check",
        description = "Parse a GQL query and return structured errors if it fails. Returns {valid: true} on success, or {valid: false, errors: [{message, suggestion}]} on failure. Use to validate GQL before execution or to get repair hints."
    )]
    async fn gql_parse_check(
        &self,
        params: Parameters<ParseCheckParams>,
    ) -> Result<CallToolResult, McpError> {
        let query = &params.0.query;
        match selene_gql::parse_statement(query) {
            Ok(_) => {
                let result = serde_json::json!({
                    "valid": true,
                    "query": query,
                });
                Ok(CallToolResult::success(vec![Content::text(
                    serde_json::to_string_pretty(&result).unwrap_or_default(),
                )]))
            }
            Err(e) => {
                let message = e.to_string();
                let suggestion = parse_error_suggestion(&message);
                let result = serde_json::json!({
                    "valid": false,
                    "query": query,
                    "errors": [{
                        "message": message,
                        "suggestion": suggestion,
                    }],
                });
                Ok(CallToolResult::success(vec![Content::text(
                    serde_json::to_string_pretty(&result).unwrap_or_default(),
                )]))
            }
        }
    }

    // ── GraphRAG AI Tools (feature-gated: ai) ───────────────────────

    #[cfg(feature = "ai")]
    #[tool(
        name = "build_communities",
        description = "Run Louvain community detection on the graph and create __CommunitySummary nodes with structural profiles (label distribution, key entities, node count). Excludes system labels (__ prefix). Use enrich_communities afterwards to add embeddings for global search mode."
    )]
    async fn build_communities(
        &self,
        params: Parameters<BuildCommunitiesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let min_size = params.0.min_community_size.unwrap_or(2);
        let start = std::time::Instant::now();

        // 1. Build projection excluding __ labels and run Louvain
        let communities = self
            .state
            .graph
            .read(|graph| build_community_data(graph, min_size));

        if communities.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                "No communities found (graph may be empty or fully disconnected).",
            )]));
        }

        // 2. MERGE __CommunitySummary nodes via parameterized GQL
        let community_count = communities.len();
        let mut total_nodes_covered = 0usize;
        for community in &communities {
            total_nodes_covered += community.node_count;
            let mut params_map = HashMap::new();
            params_map.insert("cid".into(), Value::UInt(community.community_id));
            params_map.insert(
                "label_dist".into(),
                Value::from(community.label_distribution.as_str()),
            );
            params_map.insert(
                "key_entities".into(),
                Value::from(community.key_entities.as_str()),
            );
            params_map.insert("node_count".into(), Value::Int(community.node_count as i64));
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            params_map.insert("updated_at".into(), Value::Int(now_ms));

            let query = "MERGE (c:__CommunitySummary {community_id: $cid}) \
                         SET c.label_distribution = $label_dist, \
                         c.key_entities = $key_entities, \
                         c.node_count = $node_count, \
                         c.updated_at = $updated_at";

            let st = Arc::clone(&self.state);
            let auth2 = auth.clone();
            self.submit_mut(move || {
                ops::gql::execute_gql(
                    &st,
                    &auth2,
                    query,
                    Some(&params_map),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                )
            })
            .await?;
        }

        let elapsed = start.elapsed();
        let text = format!(
            "Built {community_count} communities covering {total_nodes_covered} nodes in {:.1}s",
            elapsed.as_secs_f64()
        );
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[cfg(feature = "ai")]
    #[tool(
        name = "enrich_communities",
        description = "Add vector embeddings to __CommunitySummary nodes by composing text from structural profiles and calling embed(). Enables global and hybrid search modes in graphrag_search. Run build_communities first."
    )]
    async fn enrich_communities(&self) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;

        // 1. MATCH all __CommunitySummary nodes
        let query = "MATCH (c:__CommunitySummary) \
                     RETURN id(c) AS nodeId, c.label_distribution AS labels, \
                     c.key_entities AS entities, c.node_count AS count";
        let result = ops::gql::execute_gql(
            &self.state,
            &auth,
            query,
            None,
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;

        if result.row_count == 0 {
            return Ok(CallToolResult::success(vec![Content::text(
                "No __CommunitySummary nodes found. Run build_communities first.",
            )]));
        }

        // Parse result JSON to get node data
        let data_str = result.data_json.unwrap_or_else(|| "[]".to_string());
        let rows: Vec<serde_json::Value> = serde_json::from_str(&data_str).unwrap_or_default();

        let mut enriched = 0u64;
        for row in &rows {
            let node_id = row.get("nodeId").and_then(|v| v.as_u64()).unwrap_or(0);
            if node_id == 0 {
                continue;
            }

            let labels = row.get("labels").and_then(|v| v.as_str()).unwrap_or("");
            let entities = row.get("entities").and_then(|v| v.as_str()).unwrap_or("");
            let count = row.get("count").and_then(|v| v.as_i64()).unwrap_or(0);

            // Compose text for embedding
            let text = format!(
                "Community with {count} nodes. Labels: {labels}. Key entities: {entities}."
            );

            // SET embedding via embed() function
            let mut params_map = HashMap::new();
            params_map.insert("id".into(), Value::UInt(node_id));
            params_map.insert("text".into(), Value::from(text.as_str()));

            let set_query = "MATCH (c:__CommunitySummary) FILTER id(c) = $id \
                            SET c.embedding = embed($text)";

            let st = Arc::clone(&self.state);
            let auth2 = auth.clone();
            self.submit_mut(move || {
                ops::gql::execute_gql(
                    &st,
                    &auth2,
                    set_query,
                    Some(&params_map),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                )
            })
            .await?;
            enriched += 1;
        }

        let text = format!("Enriched {enriched} community summaries with embeddings.");
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[cfg(feature = "ai")]
    #[tool(
        name = "graphrag_search",
        description = "Search the graph using GraphRAG: combines vector similarity, graph traversal (BFS expansion), and optional community context. Modes: 'local' (default, vector + BFS + community), 'global' (community embeddings only), 'hybrid' (both merged). Returns nodes with scores, provenance source, context snippets, and traversal depth."
    )]
    async fn graphrag_search(
        &self,
        params: Parameters<GraphRagSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = params.0;
        let k = p.k.unwrap_or(10);
        let max_hops = p.max_hops.unwrap_or(2);
        let mode = p.mode.unwrap_or_else(|| "local".to_string());

        if k <= 0 {
            return Err(McpError {
                code: ErrorCode::INVALID_PARAMS,
                message: "k must be a positive integer".into(),
                data: None,
            });
        }

        let query = "CALL graphrag.search($queryText, $k, $maxHops, $mode) \
                     YIELD nodeId, score, source, context, depth \
                     RETURN nodeId, score, source, context, depth";

        let mut gql_params = HashMap::new();
        gql_params.insert("queryText".into(), Value::from(p.query.as_str()));
        gql_params.insert("k".into(), Value::Int(k));
        gql_params.insert("maxHops".into(), Value::Int(max_hops));
        gql_params.insert("mode".into(), Value::from(mode.as_str()));

        let result = ops::gql::execute_gql(
            &self.state,
            &auth,
            query,
            Some(&gql_params),
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .map_err(op_err)?;

        let data = result.data_json.unwrap_or_else(|| "[]".to_string());
        let text = format!(
            "GraphRAG search for '{}': {} results\n{data}",
            p.query, result.row_count
        );
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }
}

/// Produce a syntax hint based on common GQL parse error patterns.
fn parse_error_suggestion(message: &str) -> String {
    let msg = message.to_lowercase();
    if msg.contains("expected") && msg.contains("match") {
        return "Queries typically start with MATCH, INSERT, MERGE, DELETE, or CALL.".to_string();
    }
    if msg.contains("filter") {
        return "FILTER clauses use: FILTER n.property operator value. \
                Operators: =, <>, <, >, <=, >=, AND, OR, NOT."
            .to_string();
    }
    if msg.contains("return") {
        return "RETURN clause syntax: RETURN expr AS alias. \
                Use commas to separate multiple return items."
            .to_string();
    }
    if msg.contains("insert") {
        return "INSERT syntax: INSERT (:Label {prop: value}) or \
                INSERT (src)-[:EDGE_TYPE]->(tgt)."
            .to_string();
    }
    if msg.contains("set") {
        return "SET syntax: SET n.property = value. \
                Separate multiple assignments with commas."
            .to_string();
    }
    if msg.contains("call") || msg.contains("yield") {
        return "Procedure syntax: CALL proc.name(args) YIELD col1, col2 RETURN col1.".to_string();
    }
    "Check GQL syntax: queries use MATCH/FILTER/RETURN for reads, \
     INSERT/MERGE/SET/DELETE for writes, and CALL/YIELD for procedures."
        .to_string()
}

// ── GraphRAG community detection helpers ────────────────────────────

/// Structural profile for a detected community.
#[cfg(feature = "ai")]
struct CommunityData {
    community_id: u64,
    label_distribution: String,
    key_entities: String,
    node_count: usize,
}

/// Build community data from the graph using Louvain detection.
///
/// Creates a projection excluding system labels (__ prefix), runs Louvain,
/// groups results by community, and computes structural profiles.
#[cfg(feature = "ai")]
fn build_community_data(graph: &selene_graph::SeleneGraph, min_size: usize) -> Vec<CommunityData> {
    use std::collections::HashMap as StdHashMap;

    // Build a full-graph projection, but we want to exclude __ labels.
    // Use ProjectionConfig with empty filters (includes all), then we filter
    // the Louvain results to skip __ nodes.
    let config = selene_algorithms::ProjectionConfig {
        name: "__build_communities".to_string(),
        node_labels: vec![],
        edge_labels: vec![],
        weight_property: None,
    };
    let proj = selene_algorithms::GraphProjection::build(graph, &config, None);
    let louvain_result = selene_algorithms::louvain(&proj);

    // Group nodes by community, excluding __ label nodes
    let mut community_nodes: StdHashMap<u64, Vec<selene_core::NodeId>> = StdHashMap::new();
    for (nid, cid, _level) in &louvain_result {
        // Skip nodes with only system labels
        if let Some(node) = graph.get_node(*nid) {
            let has_user_label = node.labels.iter().any(|l| !l.as_str().starts_with("__"));
            if has_user_label {
                community_nodes.entry(*cid).or_default().push(*nid);
            }
        }
    }

    let name_key = selene_core::IStr::new("name");
    let desc_key = selene_core::IStr::new("description");

    let mut result = Vec::new();
    for (cid, members) in &community_nodes {
        if members.len() < min_size {
            continue;
        }

        // Label distribution
        let mut label_counts: StdHashMap<&str, usize> = StdHashMap::new();
        for &nid in members {
            if let Some(node) = graph.get_node(nid) {
                for label in node.labels.iter() {
                    if !label.as_str().starts_with("__") {
                        *label_counts.entry(label.as_str()).or_insert(0) += 1;
                    }
                }
            }
        }
        let mut label_pairs: Vec<_> = label_counts.into_iter().collect();
        label_pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        let label_dist = label_pairs
            .iter()
            .map(|(l, c)| format!("{l}:{c}"))
            .collect::<Vec<_>>()
            .join(",");

        // Key entities: top-5 nodes by name or description
        let mut entity_names: Vec<String> = Vec::new();
        for &nid in members {
            if entity_names.len() >= 5 {
                break;
            }
            if let Some(node) = graph.get_node(nid) {
                if let Some(selene_core::Value::String(s)) = node.properties.get(name_key) {
                    entity_names.push(s.to_string());
                } else if let Some(selene_core::Value::InternedStr(s)) =
                    node.properties.get(name_key)
                {
                    entity_names.push(s.as_str().to_string());
                } else if let Some(selene_core::Value::String(s)) = node.properties.get(desc_key) {
                    entity_names.push(s.to_string());
                }
            }
        }
        let key_entities = entity_names.join(", ");

        result.push(CommunityData {
            community_id: *cid,
            label_distribution: label_dist,
            key_entities,
            node_count: members.len(),
        });
    }

    result.sort_by(|a, b| b.node_count.cmp(&a.node_count));
    result
}
