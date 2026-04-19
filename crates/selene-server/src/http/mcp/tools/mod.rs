//! MCP tool implementations for graph, time-series, schema, and data operations.

mod ai;
mod api_keys;
mod principals;
mod schemas;
mod signing_key;
mod tokens;

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, ErrorCode};
use rmcp::{ErrorData as McpError, tool, tool_router};

use super::format::{format_json, structured_result, structured_text_result};
use super::params::*;
use super::{SeleneTools, mcp_auth, op_err, reject_replica};
use crate::ops;
use crate::ops::json_to_value;
use selene_core::{IStr, NodeId, Value};

#[tool_router]
impl SeleneTools {
    pub(crate) fn build_tool_router() -> rmcp::handler::server::tool::ToolRouter<Self> {
        Self::tool_router()
    }

    // ── GQL ──────────────────────────────────────────────────────────

    #[tool(
        name = "gql_query",
        description = "Execute a GQL query against the property graph. Primary query interface. Examples: 'MATCH (s:sensor) RETURN s.name AS name', 'MATCH (b:building)-[:contains]->(f:floor) RETURN b.name AS building, f.name AS floor', 'INSERT (:sensor {name: \"NewSensor\", temp: 72.5})', 'MATCH (s:sensor) FILTER s.temp > 72 SET s.alert = TRUE'. Returns GQLSTATUS and JSON results.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        let data_text = result.data_json.clone().unwrap_or_else(|| "[]".to_string());
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
                m.properties_removed,
            );
        }
        let _ = write!(text, "{data_text}\n({} rows)", result.row_count);

        // Programmatic clients should not have to parse the human summary
        // above. Hand them a structured envelope mirroring the HTTP /gql
        // response: status, message, row_count, data, and (optionally)
        // mutations counts. This is finding #1 of the MCP DX hardening pass.
        let mut structured = serde_json::json!({
            "status": result.status_code,
            "message": result.message,
            "row_count": result.row_count,
        });
        if let Some(json_str) = result.data_json.as_deref()
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_str)
        {
            structured["data"] = parsed;
        }
        if let Some(m) = &result.mutations {
            structured["mutations"] = serde_json::json!({
                "nodes_created": m.nodes_created,
                "nodes_deleted": m.nodes_deleted,
                "edges_created": m.edges_created,
                "edges_deleted": m.edges_deleted,
                "properties_set": m.properties_set,
                "properties_removed": m.properties_removed,
            });
        }
        Ok(structured_text_result(text, structured))
    }

    #[tool(
        name = "gql_explain",
        description = "Show the execution plan for a GQL query without executing it. Useful for understanding how queries are optimized. Example: 'MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name AS name'",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
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
        description = "Get a node by its numeric ID. Returns the node's labels, properties, timestamps, and version.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_node(&self, params: Parameters<NodeIdParams>) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let node = ops::nodes::get_node(&self.state, &auth, params.0.id).map_err(op_err)?;
        Ok(structured_result(
            serde_json::to_value(&node).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "create_node",
        description = "Create a new node with labels and optional properties. Properties are flat key-value pairs (nested objects are stored as JSON strings). Use parent_id to place it in the containment hierarchy (auto-creates a 'contains' edge). Schema defaults are applied automatically.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        Ok(structured_result(
            serde_json::to_value(&node).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "modify_node",
        description = "Modify a node: set/remove properties, add/remove labels. All fields are optional -- only specified changes are applied.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        Ok(structured_result(
            serde_json::to_value(&node).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "delete_node",
        description = "Delete a node and all its connected edges. This is irreversible.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        description = "List nodes, optionally filtered by label. Use limit/offset for pagination. Returns node objects with all properties.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
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
        Ok(structured_result(serde_json::json!({
            "nodes": result.nodes,
            "total": result.total,
        })))
    }

    #[tool(
        name = "node_edges",
        description = "Get edges connected to a node with optional direction and label filtering. \
        Returns edges grouped by direction (outgoing/incoming) with neighbor node names included. \
        Filter by direction ('outgoing', 'incoming', or 'both') and/or edge labels. \
        Supports pagination via limit/offset.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn node_edges(
        &self,
        params: Parameters<NodeEdgesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = &params.0;
        let offset = p.offset.unwrap_or(0);
        let limit = p.limit.unwrap_or(1000).min(10_000);
        let result = ops::edges::node_edges(
            &self.state,
            &auth,
            p.id,
            p.direction.as_deref(),
            p.labels.as_deref(),
            offset,
            limit,
        )
        .map_err(op_err)?;
        Ok(structured_result(serde_json::json!({
            "node_id": p.id,
            "outgoing": result.outgoing,
            "incoming": result.incoming,
            "total": result.total,
        })))
    }

    // ── Edge CRUD ────────────────────────────────────────────────────

    #[tool(
        name = "get_edge",
        description = "Get an edge by its numeric ID. Returns source, target, label, and properties.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_edge(&self, params: Parameters<EdgeIdParams>) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let edge = ops::edges::get_edge(&self.state, &auth, params.0.id).map_err(op_err)?;
        Ok(structured_result(
            serde_json::to_value(&edge).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "create_edge",
        description = "Create a directed edge between two nodes. Common labels: 'contains' (hierarchy), 'feeds' (distribution), 'isPointOf' (sensor->equipment), 'monitors', 'hasLocation'.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        let upsert = p.upsert.unwrap_or(false);
        let st = Arc::clone(&self.state);
        let edge = self
            .submit_mut(move || {
                ops::edges::create_edge(&st, &auth, source, target, label, props, upsert)
            })
            .await?;
        Ok(structured_result(
            serde_json::to_value(&edge).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "modify_edge",
        description = "Modify an edge's properties. Set new properties or remove existing ones.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        Ok(structured_result(
            serde_json::to_value(&edge).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "delete_edge",
        description = "Delete an edge by ID. This is irreversible.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        name = "batch_create_nodes",
        description = "Create multiple nodes in a single call. Each entry specifies labels and optional properties. Returns array of created node IDs. Much faster than individual create_node calls for bulk operations.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn batch_create_nodes(
        &self,
        params: Parameters<BatchCreateNodesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let entries = params.0.nodes;
        let st = Arc::clone(&self.state);
        let ids: Vec<u64> = self
            .submit_mut(move || {
                let mut created_ids = Vec::with_capacity(entries.len());
                for entry in entries {
                    let label_strs: Vec<&str> = entry.labels.iter().map(|s| s.as_str()).collect();
                    let labels = selene_core::LabelSet::from_strs(&label_strs);
                    let schema = st.graph.read(|g| {
                        let label = entry.labels.first().map_or("", |s| s.as_str());
                        g.schema().node_schema(label).cloned()
                    });
                    let props = ops::json_props_with_schema(entry.properties, schema.as_ref())
                        .map_err(|e| ops::OpError::Internal(e.to_string()))?;
                    let node = ops::nodes::create_node(&st, &auth, labels, props, None)?;
                    created_ids.push(node.id);
                }
                Ok(created_ids)
            })
            .await?;
        Ok(structured_result(serde_json::json!({
            "created": ids.len(),
            "ids": ids,
        })))
    }

    #[tool(
        name = "batch_create_edges",
        description = "Create multiple edges in a single call. Each entry specifies source, target, label, and optional properties. Supports per-edge upsert flag. Returns array of created/matched edge IDs.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn batch_create_edges(
        &self,
        params: Parameters<BatchCreateEdgesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let entries = params.0.edges;
        let batch_upsert = params.0.upsert.unwrap_or(false);
        let st = Arc::clone(&self.state);
        let ids: Vec<u64> = self
            .submit_mut(move || {
                let mut created_ids = Vec::with_capacity(entries.len());
                for entry in entries {
                    let label = selene_core::IStr::new(&entry.label);
                    let props = ops::json_props_with_edge_schema(entry.properties, &st, label)
                        .map_err(|e| ops::OpError::Internal(e.to_string()))?;
                    let upsert = entry.upsert.unwrap_or(batch_upsert);
                    let edge = ops::edges::create_edge(
                        &st,
                        &auth,
                        entry.source,
                        entry.target,
                        label,
                        props,
                        upsert,
                    )?;
                    created_ids.push(edge.id);
                }
                Ok(created_ids)
            })
            .await?;
        Ok(structured_result(serde_json::json!({
            "created": ids.len(),
            "ids": ids,
        })))
    }

    #[tool(
        name = "batch_ingest",
        description = "Create multiple nodes with edges in a single call. Each entry specifies labels, properties, and edges to connect to/from existing nodes. Returns created node IDs. Use for bulk-ingesting findings, concerns, or any connected entities.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn batch_ingest(
        &self,
        params: Parameters<BatchIngestParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        reject_replica(&self.state)?;
        let entries = params.0.entries;
        let st = Arc::clone(&self.state);
        let ids: Vec<u64> = self
            .submit_mut(move || {
                let mut created_ids = Vec::with_capacity(entries.len());
                for entry in entries {
                    let label_strs: Vec<&str> = entry.labels.iter().map(|s| s.as_str()).collect();
                    let labels = selene_core::LabelSet::from_strs(&label_strs);
                    let schema = st.graph.read(|g| {
                        let label = entry.labels.first().map_or("", |s| s.as_str());
                        g.schema().node_schema(label).cloned()
                    });
                    let props = ops::json_props_with_schema(entry.properties, schema.as_ref())
                        .map_err(|e| ops::OpError::Internal(e.to_string()))?;
                    let node = ops::nodes::create_node(&st, &auth, labels, props, None)?;
                    let node_id = node.id;

                    for edge in &entry.connect_to {
                        let label = selene_core::IStr::new(&edge.label);
                        let props =
                            ops::json_props_with_edge_schema(edge.properties.clone(), &st, label)
                                .map_err(|e| ops::OpError::Internal(e.to_string()))?;
                        ops::edges::create_edge(
                            &st,
                            &auth,
                            node_id,
                            edge.node_id,
                            label,
                            props,
                            false,
                        )?;
                    }

                    for edge in &entry.connect_from {
                        let label = selene_core::IStr::new(&edge.label);
                        let props =
                            ops::json_props_with_edge_schema(edge.properties.clone(), &st, label)
                                .map_err(|e| ops::OpError::Internal(e.to_string()))?;
                        ops::edges::create_edge(
                            &st,
                            &auth,
                            edge.node_id,
                            node_id,
                            label,
                            props,
                            false,
                        )?;
                    }

                    created_ids.push(node_id);
                }
                Ok(created_ids)
            })
            .await?;
        Ok(structured_result(serde_json::json!({
            "created": ids.len(),
            "ids": ids,
        })))
    }

    #[tool(
        name = "list_edges",
        description = "List edges, optionally filtered by label. Use limit/offset for pagination.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
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
        Ok(structured_result(serde_json::json!({
            "edges": result.edges,
            "total": result.total,
        })))
    }

    // ── Time-Series ──────────────────────────────────────────────────

    #[tool(
        name = "ts_write",
        description = "Write time-series samples. entity_id must reference an existing node. timestamp_nanos is nanoseconds since Unix epoch (seconds * 1_000_000_000). value is always a float. The entity must exist in the graph.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        Ok(structured_text_result(
            format!("Wrote {count} samples"),
            serde_json::json!({ "written": count }),
        ))
    }

    #[tool(
        name = "ts_query",
        description = "Query time-series samples for a specific node and property. \
        Supports aggregation: set aggregation to '5m', '15m', '1h', '1d', or 'auto' \
        to get bucketed results instead of raw samples. 'auto' picks the bucket size \
        based on the time range. Function options: 'avg' (default), 'min', 'max', 'sum', 'count'. \
        Raw mode (default) returns timestamp/value pairs with optional limit.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn ts_query(
        &self,
        params: Parameters<TsQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = params.0;
        let start = p.start.unwrap_or(0);
        let end = p.end.unwrap_or(i64::MAX);

        // Determine aggregation mode
        let agg_mode = p.aggregation.as_deref().unwrap_or("raw");
        let agg_mode = if agg_mode == "auto" {
            let range_ns = end.saturating_sub(start);
            let hours = range_ns / 3_600_000_000_000;
            match hours {
                0..4 => "raw",
                4..24 => "5m",
                24..168 => "15m", // 7 days
                168..720 => "1h", // 30 days
                _ => "1d",
            }
        } else {
            agg_mode
        };

        if agg_mode == "raw" {
            let samples = ops::ts::ts_range(
                &self.state,
                &auth,
                p.entity_id,
                &p.property,
                start,
                end,
                Some(p.limit.unwrap_or(1000) as usize),
            )
            .map_err(op_err)?;
            return Ok(structured_result(
                serde_json::to_value(&samples).unwrap_or_default(),
            ));
        }

        // Route to ts.window via GQL for aggregated results
        let agg_fn = p.function.as_deref().unwrap_or("avg");
        let query = "CALL ts.window($entityId, $prop, $aggMode, $aggFn, $aggMode) \
                     YIELD window_start, window_end, value RETURN window_start, window_end, value";

        let mut gql_params = HashMap::new();
        gql_params.insert("entityId".into(), Value::Int(p.entity_id as i64));
        gql_params.insert("prop".into(), Value::from(p.property.as_str()));
        gql_params.insert("aggMode".into(), Value::from(agg_mode));
        gql_params.insert("aggFn".into(), Value::from(agg_fn));

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
        let parsed: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
        let text = format!(
            "ts_query aggregation ({}): {} buckets\n{data}",
            agg_mode, result.row_count
        );
        Ok(structured_text_result(
            text,
            serde_json::json!({
                "agg_mode": agg_mode,
                "agg_fn": agg_fn,
                "row_count": result.row_count,
                "buckets": parsed,
            }),
        ))
    }

    // ── Graph Slice ──────────────────────────────────────────────────

    #[tool(
        name = "graph_slice",
        description = "Get a snapshot of the graph. Slice types: 'full' (everything), \
        'labels' (nodes with specific labels + connecting edges), 'containment' (subtree \
        from a root node), 'traverse' (BFS from root following specified edge labels and \
        direction to max_depth). Traverse returns nodes with _depth property. \
        Supports pagination via limit/offset.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
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
            "traverse" => selene_wire::dto::graph_slice::SliceType::Traverse {
                root_id: p.root_id.unwrap_or(1),
                edge_labels: p.labels.unwrap_or_default(),
                direction: p.direction.clone().unwrap_or_else(|| "outgoing".into()),
                max_depth: p.max_depth.unwrap_or(3),
            },
            other => {
                return Err(McpError {
                    code: ErrorCode::INVALID_PARAMS,
                    message: format!(
                        "invalid slice_type '{other}' -- use full, labels, containment, or traverse"
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

        Ok(structured_result(resp))
    }

    // ── Health ────────────────────────────────────────────────────────

    #[tool(
        name = "health",
        description = "Check server health. Returns uptime, node/edge counts, and status.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn health(&self) -> Result<CallToolResult, McpError> {
        let resp = ops::health::health(&self.state);
        Ok(structured_result(
            serde_json::to_value(&resp).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "info",
        description = "Get server metadata: version, runtime profile, dev mode, and enabled feature flags.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn info(&self) -> Result<CallToolResult, McpError> {
        let info = crate::ops::info::server_info(&self.state);
        Ok(structured_result(
            serde_json::to_value(&info).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "graph_stats",
        description = "Get graph statistics with per-label breakdowns of node and edge counts. More detailed than health -- shows how many nodes exist for each label.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn graph_stats(&self) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let stats = ops::graph_stats::graph_stats(&self.state, &auth);
        let embed = selene_gql::runtime::embed::embedding_status();
        Ok(structured_result(serde_json::json!({
            "node_count": stats.node_count,
            "edge_count": stats.edge_count,
            "node_labels": stats.node_labels,
            "edge_labels": stats.edge_labels,
            "embedding": {
                "loaded": embed.loaded,
                "model_id": embed.model_id,
                "dimensions": embed.dimensions,
                "model_path": embed.model_path,
                "error": embed.error,
            },
        })))
    }

    // ── React Flow ────────────────────────────────────────────────────

    #[tool(
        name = "export_reactflow",
        description = "Export the graph in React Flow format ({nodes, edges} with id, position, data, source, target, label). Compatible with https://reactflow.dev for visual graph editing. Optionally filter by label.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn export_reactflow(
        &self,
        params: Parameters<RFExportParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let graph = ops::reactflow::export_reactflow(&self.state, &auth, params.0.label.as_deref());
        Ok(structured_result(
            serde_json::to_value(&graph).unwrap_or_default(),
        ))
    }

    #[tool(
        name = "import_reactflow",
        description = "Import a React Flow graph ({nodes, edges}). Each node becomes a Selene node (type->label, data->properties). Each edge becomes a Selene edge (label from edge label or 'connected'). Returns a mapping from React Flow IDs to Selene IDs.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        Ok(structured_text_result(
            format!(
                "Imported {} nodes, {} edges. ID mapping: {:?}",
                result.nodes_created, result.edges_created, result.id_map
            ),
            serde_json::json!({
                "nodes_created": result.nodes_created,
                "edges_created": result.edges_created,
                "id_map": result.id_map,
            }),
        ))
    }

    // ── Schema Management (delegated to schemas module) ──────────────

    #[tool(
        name = "list_schemas",
        description = "List all registered node and edge schemas. Schemas define expected property types, required fields, defaults, and validation rules.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_schemas(&self) -> Result<CallToolResult, McpError> {
        schemas::list_schemas_impl(self).await
    }

    #[tool(
        name = "get_schema",
        description = "Get the full definition of a schema by label. Tries node schemas first, then edge schemas. Shows property definitions, types, required flags, defaults, and annotations.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_schema(
        &self,
        params: Parameters<SchemaLabelParams>,
    ) -> Result<CallToolResult, McpError> {
        schemas::get_schema_impl(self, params.0).await
    }

    #[tool(
        name = "create_schema",
        description = "Create a new node type schema using field shorthand. Fields: 'string!' (required), 'float = 72.5' (with default), 'bool' (optional). Use 'extends' to inherit from a parent type (e.g., 'equipment', 'point'). Schema validation is applied on node creation and property updates.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_schema(
        &self,
        params: Parameters<CreateSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        schemas::create_schema_impl(self, params.0).await
    }

    #[tool(
        name = "update_schema",
        description = "Update an existing node schema. Fields are replaced entirely (not merged). Use get_schema first to see the current definition, then provide the complete updated fields.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn update_schema(
        &self,
        params: Parameters<UpdateSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        schemas::update_schema_impl(self, params.0).await
    }

    #[tool(
        name = "delete_schema",
        description = "Delete a node schema by label. Validation for this label is removed immediately.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        description = "Create a new edge type schema. Fields use shorthand: 'string!' (required), 'float = 72.5' (with default). Use source_labels/target_labels to constrain which node types can be connected.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_edge_schema(
        &self,
        params: Parameters<CreateEdgeSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        schemas::create_edge_schema_impl(self, params.0).await
    }

    #[tool(
        name = "delete_edge_schema",
        description = "Delete an edge schema by label. Validation for this edge type is removed immediately.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        description = "Export all registered schemas as compact JSON. The output can be saved and re-imported later via import_schema_pack.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn export_schemas(&self) -> Result<CallToolResult, McpError> {
        schemas::export_schemas_impl(self).await
    }

    #[tool(
        name = "import_schema_pack",
        description = "Import a schema pack from compact JSON or TOML. Auto-detects format. Fields use shorthand: 'string!' (required), 'float = 72.5' (with default).",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn import_schema_pack(
        &self,
        params: Parameters<ImportPackParams>,
    ) -> Result<CallToolResult, McpError> {
        schemas::import_schema_pack_impl(self, params.0).await
    }

    // ── CSV Import/Export ─────────────────────────────────────────

    #[tool(
        name = "csv_import",
        description = "Import nodes or edges from CSV data. For nodes: each row becomes a node with the specified label; columns become properties. For edges: CSV must have source_id, target_id, and label columns; additional columns become edge properties. Type inference: integers, floats, and booleans are auto-detected.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
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
        Ok(structured_text_result(
            text,
            serde_json::json!({
                "nodes_created": result.nodes_created,
                "edges_created": result.edges_created,
                "rows_skipped": result.rows_skipped,
                "errors": result.errors,
            }),
        ))
    }

    #[tool(
        name = "csv_export",
        description = "Export nodes or edges as CSV. For nodes: columns are id plus all property keys. For edges: columns are id, source, target, label, plus property keys. Optional label filter narrows the export.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
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

    // ── Vector Search ────────────────────────────────────────────────

    #[tool(
        name = "semantic_search",
        description = "Search the graph using natural language. Embeds the query text into a vector, finds the most similar nodes, and returns them with their containment path (e.g., building > floor > zone > sensor). Use summary_mode=true for compact results (name/labels/score only). Use max_property_length to truncate long property values when include_properties=true. Supports pagination via offset (k is page size). Requires the embedding model to be loaded.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
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

        // Request k + offset from the engine so pagination can reach beyond
        // the first k results.
        let offset = p.offset.unwrap_or(0).max(0) as usize;
        let effective_k = p.k + offset as i64;

        let mut gql_params = HashMap::new();
        gql_params.insert("queryText".into(), Value::from(p.query_text.as_str()));
        gql_params.insert("k".into(), Value::Int(effective_k));

        let query = if let Some(ref label) = p.label {
            gql_params.insert("label".into(), Value::from(label.as_str()));
            "CALL graph.semanticSearch($queryText, $k, $label) \
             YIELD node_id, score, path RETURN node_id, score, path"
        } else {
            "CALL graph.semanticSearch($queryText, $k) \
             YIELD node_id, score, path RETURN node_id, score, path"
        };

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
        let all_rows: Vec<serde_json::Value> = serde_json::from_str(&data).unwrap_or_default();
        let total = all_rows.len();

        // Apply offset — we requested k+offset from the engine, now slice.
        let rows: Vec<serde_json::Value> = all_rows
            .into_iter()
            .skip(offset)
            .take(p.k as usize)
            .collect();

        let summary = p.summary_mode.unwrap_or(false);
        let include_props = p.include_properties.unwrap_or(false);

        if !include_props && !summary {
            let rendered = format_json(&rows);
            let text = format!(
                "Semantic search for '{}': {} results (showing {}, offset {})\n{}",
                p.query_text,
                total,
                rows.len(),
                offset,
                rendered
            );
            return Ok(structured_text_result(
                text,
                serde_json::json!({
                    "query": p.query_text,
                    "total": total,
                    "offset": offset,
                    "results": rows,
                }),
            ));
        }

        // Enrich results — summary_mode returns lightweight data,
        // full mode returns complete node properties.
        let max_prop_len = p.max_property_length.unwrap_or(0);

        // Load graph snapshot once to avoid per-node ArcSwap::load.
        // Refresh auth scope first so in_scope() checks are current.
        let auth = ops::refresh_scope_if_stale(&self.state, &auth);
        let snapshot = self.state.graph.load_snapshot();

        let enriched: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|row| {
                let node_id = row
                    .get("node_id")
                    .and_then(|v| v.as_i64())
                    .map_or(0, |v| v as u64);
                let mut enriched = row;
                let nid = NodeId(node_id);
                if auth.in_scope(nid)
                    && let Some(node) = snapshot.get_node(nid)
                {
                    if summary {
                        // Lightweight: read name + labels directly from NodeRef,
                        // avoiding full node_to_dto clone.
                        if let Some(name_val) = node.properties.get(IStr::new("name")) {
                            enriched["name"] = serde_json::to_value(name_val).unwrap_or_default();
                        }
                        let labels: Vec<&str> = node.labels.iter().map(|l| l.as_str()).collect();
                        enriched["labels"] = serde_json::to_value(&labels).unwrap_or_default();
                    } else {
                        let dto = ops::node_to_dto(node);
                        let mut node_json = serde_json::to_value(&dto).unwrap_or_default();
                        if max_prop_len > 0 {
                            truncate_property_values(&mut node_json, max_prop_len);
                        }
                        enriched["node"] = node_json;
                    }
                }
                enriched
            })
            .collect();

        let returned = enriched.len();
        let mut text = format!(
            "Semantic search for '{}': {} results (showing {}, offset {})\n{}",
            p.query_text,
            total,
            returned,
            offset,
            format_json(&enriched)
        );

        // Response size guard (char-boundary safe). Note: this only truncates
        // the human text; the structured payload below is unbounded and lets
        // programmatic clients see all results without the truncation hint.
        const MAX_RESPONSE_BYTES: usize = 50_000;
        if text.len() > MAX_RESPONSE_BYTES {
            let truncate_at = text
                .char_indices()
                .map(|(idx, _)| idx)
                .take_while(|&idx| idx <= MAX_RESPONSE_BYTES)
                .last()
                .unwrap_or(0);
            text.truncate(truncate_at);
            text.push_str("\n\n... (truncated — use summary_mode=true or reduce k)");
        }

        Ok(structured_text_result(
            text,
            serde_json::json!({
                "query": p.query_text,
                "total": total,
                "offset": offset,
                "returned": returned,
                "results": enriched,
            }),
        ))
    }

    #[tool(
        name = "similar_nodes",
        description = "Find nodes most similar to a given node based on vector embeddings. Returns the k most similar nodes ranked by cosine similarity.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn similar_nodes(
        &self,
        params: Parameters<SimilarNodesParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = &params.0;

        if p.k <= 0 {
            return Err(op_err(ops::OpError::InvalidRequest(
                "k must be a positive integer".into(),
            )));
        }

        // Pre-flight: confirm the node exists and the named property is a Vector.
        // Without this check the underlying CALL silently returns zero results
        // for any non-vector property, leaving the caller guessing.
        let node_id = selene_core::entity::NodeId(p.node_id);
        let prop_check = self.state.graph.read(|g| {
            let Some(node) = g.get_node(node_id) else {
                return Err(ops::OpError::NotFound {
                    entity: "node",
                    id: p.node_id,
                });
            };
            match node.property(&p.property) {
                None => Err(ops::OpError::InvalidRequest(format!(
                    "node {} has no property '{}' — populate it with an embedding first \
                     (e.g. SET n.{} = embed($text) for arbitrary nodes, or enrich_communities \
                     for __CommunitySummary nodes)",
                    p.node_id, p.property, p.property
                ))),
                Some(selene_core::Value::Vector(_)) => Ok(()),
                Some(other) => Err(ops::OpError::InvalidRequest(format!(
                    "property '{}' on node {} is {}, not a vector — overwrite it with an \
                     embedding via SET n.{} = embed($text) before similarity search",
                    p.property,
                    p.node_id,
                    other.type_name(),
                    p.property,
                ))),
            }
        });
        prop_check.map_err(op_err)?;

        let query = "CALL graph.similarNodes($nodeId, $prop, $k) \
                     YIELD node_id, score RETURN node_id, score";

        let mut gql_params = HashMap::new();
        gql_params.insert("nodeId".into(), Value::Int(p.node_id as i64));
        gql_params.insert("prop".into(), Value::from(p.property.as_str()));
        gql_params.insert("k".into(), Value::Int(p.k));

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

        let data_text = result.data_json.clone().unwrap_or_else(|| "[]".to_string());
        let text = format!(
            "Similar to node {}: {} results\n{data_text}",
            p.node_id, result.row_count
        );
        let parsed: serde_json::Value = result
            .data_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_else(|| serde_json::json!([]));
        Ok(structured_text_result(
            text,
            serde_json::json!({
                "node_id": p.node_id,
                "property": p.property,
                "row_count": result.row_count,
                "results": parsed,
            }),
        ))
    }

    // ── Vector Quantization Stats ────────────────────────────────────

    #[tool(
        name = "quantization_stats",
        description = "Get vector quantization statistics for all HNSW indexes. \
        Returns compression method, bit width, vector count, memory saved, and \
        compression ratio. Empty result when quantization is not enabled.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn quantization_stats(&self) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let query = "CALL vector.quantizationStats() YIELD namespace, method, bits, vector_count, \
             quantized_bytes, f32_bytes, compression_ratio, rescore \
             RETURN namespace, method, bits, vector_count, quantized_bytes, \
             f32_bytes, compression_ratio, rescore";

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
        if result.row_count == 0 {
            Ok(CallToolResult::success(vec![Content::text(
                "No quantized HNSW indexes found. Enable quantization in config \
                 with hnsw_quantize = true under [vector].",
            )]))
        } else {
            let parsed: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
            let text = format!(
                "Quantization stats ({} index(es)):\n{data}",
                result.row_count
            );
            Ok(structured_text_result(
                text,
                serde_json::json!({
                    "index_count": result.row_count,
                    "indexes": parsed,
                }),
            ))
        }
    }

    // ── Entity Resolution + Neighborhood ─────────────────────────────

    #[tool(
        name = "resolve",
        description = "Resolve a human-friendly name, alias, or description to a graph node. \
        Tries exact ID match, then exact name match, then semantic search. Returns the full \
        node with all properties, labels, and optional containment path. \
        Use this instead of writing GQL just to look up a node by name.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn resolve(&self, params: Parameters<ResolveParams>) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = &params.0;
        let include_path = p.include_path.unwrap_or(true);

        // Helper: build node response JSON with optional containment path
        let build_response = |node: &selene_wire::dto::entity::NodeDto,
                              resolved_by: &str,
                              extra: Option<(&str, f64)>| {
            let mut val = serde_json::to_value(node).unwrap_or_default();
            val["resolved_by"] = serde_json::Value::String(resolved_by.into());
            if let Some((key, v)) = extra {
                val[key] = serde_json::Value::from(v);
            }
            if include_path && let Some(path) = self.containment_path(&auth, node.id) {
                val["containment_path"] = serde_json::Value::String(path);
            }
            structured_result(val)
        };

        // Strategy 1: Parse as numeric ID
        if let Ok(id) = p.identifier.parse::<u64>()
            && let Ok(node) = ops::nodes::get_node(&self.state, &auth, id)
        {
            return Ok(build_response(&node, "id_lookup", None));
        }

        // Strategy 2: Exact name match via GQL
        let mut name_params = HashMap::new();
        name_params.insert("name".into(), Value::from(p.identifier.as_str()));

        let name_query = match &p.label {
            Some(label) => {
                validate_label(label)?;
                format!("MATCH (n:{label}) WHERE n.name = $name RETURN n.id LIMIT 1")
            }
            None => "MATCH (n) WHERE n.name = $name RETURN n.id LIMIT 1".to_string(),
        };

        let name_id = ops::gql::execute_gql(
            &self.state,
            &auth,
            &name_query,
            Some(&name_params),
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .ok()
        .filter(|r| r.row_count > 0)
        .and_then(|r| r.data_json)
        .and_then(|data| serde_json::from_str::<Vec<serde_json::Value>>(&data).ok())
        .and_then(|rows| rows.first()?.get("n.id")?.as_u64());

        if let Some(id) = name_id
            && let Ok(node) = ops::nodes::get_node(&self.state, &auth, id)
        {
            return Ok(build_response(&node, "name_match", None));
        }

        // Strategy 3: Semantic search fallback
        let mut sem_params = HashMap::new();
        sem_params.insert("queryText".into(), Value::from(p.identifier.as_str()));
        sem_params.insert("k".into(), Value::Int(3));

        let sem_query = if let Some(ref label) = p.label {
            sem_params.insert("label".into(), Value::from(label.as_str()));
            "CALL graph.semanticSearch($queryText, $k, $label) \
             YIELD node_id, score RETURN node_id, score"
        } else {
            "CALL graph.semanticSearch($queryText, $k) \
             YIELD node_id, score RETURN node_id, score"
        };

        let rows: Vec<serde_json::Value> = ops::gql::execute_gql(
            &self.state,
            &auth,
            sem_query,
            Some(&sem_params),
            false,
            false,
            ops::gql::ResultFormat::Json,
        )
        .ok()
        .and_then(|r| r.data_json)
        .and_then(|data| serde_json::from_str(&data).ok())
        .unwrap_or_default();

        // Return top match if similarity > 0.75
        if let Some(top) = rows.first() {
            let score = top.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let node_id = top
                .get("node_id")
                .and_then(|v| v.as_i64())
                .map_or(0, |v| v as u64);

            if score > 0.75
                && node_id > 0
                && let Ok(node) = ops::nodes::get_node(&self.state, &auth, node_id)
            {
                return Ok(build_response(
                    &node,
                    "semantic_search",
                    Some(("similarity", score)),
                ));
            }
        }

        // Suggest alternatives if no strong match
        let suggestions: Vec<serde_json::Value> = rows
            .iter()
            .filter_map(|r| {
                let nid = r.get("node_id")?.as_i64()? as u64;
                let sc = r.get("score")?.as_f64()?;
                let name = ops::nodes::get_node(&self.state, &auth, nid)
                    .ok()
                    .and_then(|n| n.properties.get("name").map(|v| v.to_string()));
                Some(serde_json::json!({ "node_id": nid, "score": sc, "name": name }))
            })
            .collect();

        if !suggestions.is_empty() {
            return Ok(structured_result(serde_json::json!({
                "error": "no_exact_match",
                "message": format!("Could not resolve '{}'. Did you mean one of these?", p.identifier),
                "suggestions": suggestions,
            })));
        }

        Ok(structured_result(serde_json::json!({
            "error": "not_found",
            "message": format!("Could not resolve '{}'", p.identifier),
        })))
    }

    #[tool(
        name = "related",
        description = "Get a node and all its connections in one call. Returns the node's full \
        properties plus its edges grouped by direction, with neighbor names and key properties \
        included. Saves multiple get_node + node_edges calls. Use this for 'tell me about X \
        and its connections'.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn related(&self, params: Parameters<RelatedParams>) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = &params.0;
        let neighbor_limit = p.neighbor_limit.unwrap_or(25);

        // Get the target node
        let node = ops::nodes::get_node(&self.state, &auth, p.id).map_err(op_err)?;

        // Get edges with direction/label filtering
        let edge_result = ops::edges::node_edges(
            &self.state,
            &auth,
            p.id,
            p.direction.as_deref(),
            p.edge_labels.as_deref(),
            0,
            neighbor_limit,
        )
        .map_err(op_err)?;

        Ok(structured_result(serde_json::json!({
            "node": node,
            "outgoing": edge_result.outgoing,
            "incoming": edge_result.incoming,
            "total_edges": edge_result.total,
        })))
    }

    // ── RDF/SPARQL ────────────────────────────────────────────────────

    #[tool(
        name = "export_rdf",
        description = "Export the graph as RDF (Turtle format). Requires the 'rdf' feature to be enabled on the server. Returns serialized RDF triples.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
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
        let parsed: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
        Ok(structured_result(parsed))
    }

    #[tool(
        name = "sparql_query",
        description = "Execute a SPARQL query against the graph's RDF view. Requires 'rdf-sparql' feature. Returns JSON results.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn sparql_query(
        &self,
        params: Parameters<SparqlQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let query = "CALL graph.sparql($query) YIELD result RETURN result";
        let mut gql_params = HashMap::new();
        gql_params.insert("query".into(), Value::from(params.0.query.as_str()));
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
        let parsed: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
        Ok(structured_result(parsed))
    }

    // ── Text2GQL Toolkit ─────────────────────────────────────────────

    #[tool(
        name = "schema_dump",
        description = "Get a compact, LLM-optimized dump of the graph schema. Returns all node types, edge types, properties, constraints, and statistics in a format designed for minimal token usage. Use before writing GQL queries to understand the data model.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn schema_dump(
        &self,
        params: Parameters<SchemaDumpParams>,
    ) -> Result<CallToolResult, McpError> {
        let auth = mcp_auth(self)?;
        let p = params.0;

        let mut gql_params = HashMap::new();
        gql_params.insert(
            "includeSystem".into(),
            Value::Bool(p.include_system.unwrap_or(false)),
        );
        gql_params.insert("compact".into(), Value::Bool(p.compact.unwrap_or(true)));
        if let Some(label) = &p.label {
            gql_params.insert("label".into(), Value::from(label.as_str()));
        } else {
            gql_params.insert("label".into(), Value::Null);
        }

        let query =
            "CALL graph.schemaDump($includeSystem, $compact, $label) YIELD schema RETURN schema";
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
        let parsed: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
        Ok(structured_result(parsed))
    }

    #[tool(
        name = "gql_parse_check",
        description = "Parse a GQL query and return structured errors if it fails. Returns {valid: true} on success, or {valid: false, errors: [{message, suggestion}]} on failure. Use to validate GQL before execution or to get repair hints.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn gql_parse_check(
        &self,
        params: Parameters<ParseCheckParams>,
    ) -> Result<CallToolResult, McpError> {
        let query = &params.0.query;
        match selene_gql::parse_statement(query) {
            Ok(_) => Ok(structured_result(serde_json::json!({
                "valid": true,
                "query": query,
            }))),
            Err(e) => {
                let message = e.to_string();
                let suggestion = parse_error_suggestion(&message);

                // Fuzzy-match labels/properties against schema for repair hints
                let repairs = self
                    .state
                    .graph
                    .read(|g| ops::gql_repair::suggest_repairs(&message, query, g));

                let mut result = serde_json::json!({
                    "valid": false,
                    "query": query,
                    "errors": [{
                        "message": message,
                        "suggestion": suggestion,
                    }],
                });

                if !repairs.is_empty() {
                    result["repairs"] = serde_json::json!(repairs);
                }

                Ok(structured_result(result))
            }
        }
    }

    // ── GraphRAG AI Tools (delegated to ai module) ───────────────────

    #[tool(
        name = "build_communities",
        description = "Run Louvain community detection on the graph and create __CommunitySummary nodes with structural profiles (label distribution, key entities, node count). Excludes system labels (__ prefix). Use enrich_communities afterwards to add embeddings for global search mode.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn build_communities(
        &self,
        params: Parameters<BuildCommunitiesParams>,
    ) -> Result<CallToolResult, McpError> {
        ai::build_communities_impl(self, params.0).await
    }

    #[tool(
        name = "enrich_communities",
        description = "Add vector embeddings to __CommunitySummary nodes by composing text from structural profiles and calling embed(). Enables global and hybrid search modes in graphrag_search. Run build_communities first.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn enrich_communities(&self) -> Result<CallToolResult, McpError> {
        ai::enrich_communities_impl(self).await
    }

    #[tool(
        name = "graphrag_search",
        description = "Search the graph using GraphRAG: combines vector similarity, graph traversal (BFS expansion), and optional community context. Modes: 'local' (default, vector + BFS + community), 'global' (community embeddings only), 'hybrid' (both merged). Returns nodes with scores, provenance source, context snippets, and traversal depth.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn graphrag_search(
        &self,
        params: Parameters<GraphRagSearchParams>,
    ) -> Result<CallToolResult, McpError> {
        ai::graphrag_search_impl(self, params.0).await
    }

    // ── Principal Management ────────────────────────────────────────

    #[tool(
        name = "list_principals",
        description = "List all principals in the secure vault. \
        Returns identity, role, enabled status, and whether a credential is set. \
        Admin-only.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_principals(&self) -> Result<CallToolResult, McpError> {
        principals::list_principals_impl(self).await
    }

    #[tool(
        name = "get_principal",
        description = "Get a single principal by identity. \
        Returns identity, role, enabled status, and whether a credential is set. \
        Admin-only.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn get_principal(
        &self,
        params: Parameters<GetPrincipalParams>,
    ) -> Result<CallToolResult, McpError> {
        principals::get_principal_impl(self, params.0).await
    }

    #[tool(
        name = "create_principal",
        description = "Create a new principal with the given identity, role, and optional password. \
        Roles: admin, service, operator, reader, device. \
        If no password is provided, the principal can only authenticate via OAuth. \
        Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_principal(
        &self,
        params: Parameters<CreatePrincipalParams>,
    ) -> Result<CallToolResult, McpError> {
        principals::create_principal_impl(self, params.0).await
    }

    #[tool(
        name = "update_principal",
        description = "Update a principal's role and/or enabled status. \
        Only specified fields are changed; omitted fields keep their current value. \
        Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn update_principal(
        &self,
        params: Parameters<UpdatePrincipalParams>,
    ) -> Result<CallToolResult, McpError> {
        principals::update_principal_impl(self, params.0).await
    }

    #[tool(
        name = "disable_principal",
        description = "Disable a principal (set enabled = false). \
        The principal's node and credentials are preserved but authentication will fail. \
        Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn disable_principal(
        &self,
        params: Parameters<DisablePrincipalParams>,
    ) -> Result<CallToolResult, McpError> {
        principals::disable_principal_impl(self, params.0).await
    }

    #[tool(
        name = "rotate_credential",
        description = "Rotate a principal's credential (set a new password). \
        The old credential is immediately invalidated. \
        Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn rotate_credential(
        &self,
        params: Parameters<RotateCredentialParams>,
    ) -> Result<CallToolResult, McpError> {
        principals::rotate_credential_impl(self, params.0).await
    }

    // ── API-key Management ─────────────────────────────────────────────

    #[tool(
        name = "create_api_key",
        description = "Issue a new bearer API key for a principal. \
        Returns the key metadata plus a one-time plaintext token (format: selk_<prefix>.<secret>). \
        The token is never recoverable afterwards — only its argon2id hash is persisted. \
        Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn create_api_key(
        &self,
        params: Parameters<CreateApiKeyParams>,
    ) -> Result<CallToolResult, McpError> {
        api_keys::create_api_key_impl(self, params.0).await
    }

    #[tool(
        name = "list_api_keys",
        description = "List issued API keys with metadata (id, name, identity, prefix, \
        created_at, expires_at, scopes, enabled). Hashes are never returned. \
        Optionally filter by identity. Admin-only.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_api_keys(
        &self,
        params: Parameters<ListApiKeysParams>,
    ) -> Result<CallToolResult, McpError> {
        api_keys::list_api_keys_impl(self, params.0).await
    }

    #[tool(
        name = "revoke_api_key",
        description = "Disable an API key by node ID. The key row is preserved for audit, \
        but verify_api_key will reject it. Idempotent. Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn revoke_api_key(
        &self,
        params: Parameters<RevokeApiKeyParams>,
    ) -> Result<CallToolResult, McpError> {
        api_keys::revoke_api_key_impl(self, params.0).await
    }

    #[tool(
        name = "rotate_signing_key",
        description = "Rotate the OAuth access-token signing key. Generates a new 32-byte \
        HMAC-SHA256 secret, persists it in the encrypted vault, and installs it on the \
        running token service. The previous key is retained in an in-memory retired ring \
        for `retire_for_secs` (default 86400) so access tokens signed under it remain \
        valid during the grace period. Refresh tokens are unaffected. Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn rotate_signing_key(
        &self,
        params: Parameters<RotateSigningKeyParams>,
    ) -> Result<CallToolResult, McpError> {
        signing_key::rotate_signing_key_impl(self, params.0).await
    }

    #[tool(
        name = "revoke_token",
        description = "Revoke an OAuth access token by its raw JWT string. Adds the token's \
        `jti` to the in-memory deny-list until its original expiry, after which the entry is \
        pruned since the token would be invalid anyway. Use to force-logout a principal or \
        invalidate a leaked token. Refresh tokens are unaffected — pair with \
        `rotate_signing_key` if you also need to cut in-flight refresh flows. Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn revoke_token(
        &self,
        params: Parameters<RevokeTokenParams>,
    ) -> Result<CallToolResult, McpError> {
        tokens::revoke_token_impl(self, params.0).await
    }

    #[tool(
        name = "list_revoked_tokens",
        description = "List current OAuth access-token deny-list entries (jti + original \
        expiry). Expired entries are filtered out. Admin-only.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn list_revoked_tokens(&self) -> Result<CallToolResult, McpError> {
        tokens::list_revoked_tokens_impl(self).await
    }

    #[tool(
        name = "unrevoke_token",
        description = "Remove a jti from the access-token deny-list, reinstating any \
        still-unexpired token that carried it. Idempotent — returns `removed=false` if the \
        jti was not on the list. Admin-only.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn unrevoke_token(
        &self,
        params: Parameters<UnrevokeTokenParams>,
    ) -> Result<CallToolResult, McpError> {
        tokens::unrevoke_token_impl(self, params.0).await
    }
}

// ── Standalone helpers ─────────────────────────────────────────────

/// Truncate string property values that exceed `max_len` characters.
/// Handles both serde-tagged enum format (`{"String": "..."}`) and plain
/// JSON strings.
fn truncate_property_values(node_json: &mut serde_json::Value, max_len: usize) {
    let Some(props) = node_json
        .get_mut("properties")
        .and_then(|p| p.as_object_mut())
    else {
        return;
    };
    for v in props.values_mut() {
        truncate_string_value(v, max_len);
    }
}

/// Truncate a single JSON value in-place if it contains a string exceeding
/// `max_len` characters. Handles `{"String": "..."}` (serde-tagged
/// `Value` enum) and plain `"..."` JSON strings.
fn truncate_string_value(v: &mut serde_json::Value, max_len: usize) {
    // Check serde-tagged format: {"String": "..."}
    let needs_tagged_truncation = v
        .get("String")
        .and_then(|s| s.as_str())
        .is_some_and(|s| s.chars().count() > max_len);

    if needs_tagged_truncation {
        let inner = v["String"].as_str().unwrap();
        let total = inner.chars().count();
        let truncated: String = inner.chars().take(max_len).collect();
        v["String"] =
            serde_json::Value::String(format!("{truncated}... (truncated, {total} chars)"));
        return;
    }

    // Plain JSON string
    let needs_plain_truncation = v.as_str().is_some_and(|s| s.chars().count() > max_len);
    if needs_plain_truncation {
        let s = v.as_str().unwrap();
        let total = s.chars().count();
        let truncated: String = s.chars().take(max_len).collect();
        *v = serde_json::Value::String(format!("{truncated}... (truncated, {total} chars)"));
    }
}

// ── Helper methods (outside the #[tool_router] impl block) ──────────

impl SeleneTools {
    /// Build a containment path string by walking "contains" edges upward.
    /// Returns e.g. "Building > Floor 3 > Zone 301 > AHU-2".
    fn containment_path(
        &self,
        _auth: &crate::auth::handshake::AuthContext,
        node_id: u64,
    ) -> Option<String> {
        let mut path_parts = Vec::new();
        let mut current_id = node_id;

        for _ in 0..10 {
            // Safety bound: max 10 levels deep
            let (name, parent) = self.state.graph.read(|g| {
                let name = g
                    .get_node(selene_core::NodeId(current_id))
                    .and_then(|n| {
                        n.properties
                            .get(selene_core::IStr::new("name"))
                            .and_then(|v| match v {
                                selene_core::Value::String(s) => Some(s.to_string()),
                                _ => None,
                            })
                    })
                    .unwrap_or_else(|| format!("[{current_id}]"));

                let parent = g
                    .incoming(selene_core::NodeId(current_id))
                    .iter()
                    .find_map(|&eid| {
                        let e = g.get_edge(eid)?;
                        if e.label.as_str() == "contains" {
                            Some(e.source.0)
                        } else {
                            None
                        }
                    });

                (name, parent)
            });

            path_parts.push(name);
            match parent {
                Some(pid) => current_id = pid,
                None => break,
            }
        }

        path_parts.reverse();
        if path_parts.len() <= 1 {
            return None;
        }
        Some(path_parts.join(" > "))
    }
}

/// Validate that a label string is a safe GQL identifier (letters, digits, underscores).
/// Labels occupy an identifier position in the grammar and cannot use `$param` placeholders.
fn validate_label(label: &str) -> Result<(), McpError> {
    if !label.is_empty() && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(())
    } else {
        Err(McpError {
            code: ErrorCode::INVALID_PARAMS,
            message: format!(
                "invalid label '{label}': must contain only alphanumeric characters and underscores"
            )
            .into(),
            data: None,
        })
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
