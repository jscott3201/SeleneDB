//! MCP resources -- read-only graph data exposed to agents.

use rmcp::model::{
    Annotated, ListResourceTemplatesResult, ListResourcesResult, PaginatedRequestParams,
    RawResource, RawResourceTemplate, ReadResourceRequestParams, ReadResourceResult,
    ResourceContents,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, RoleServer};

use super::{SeleneTools, mcp_auth, op_err};
use crate::ops;

impl SeleneTools {
    /// List all available static resources.
    pub(crate) async fn handle_list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let resources = vec![
            Annotated {
                annotations: None,
                raw: RawResource::new("selene://health", "health")
                    .with_description("Server health: uptime, node/edge counts, status")
                    .with_mime_type("application/json"),
            },
            Annotated {
                annotations: None,
                raw: RawResource::new("selene://stats", "stats")
                    .with_description("Graph statistics with per-label node and edge counts")
                    .with_mime_type("application/json"),
            },
            Annotated {
                annotations: None,
                raw: RawResource::new("selene://schemas", "schemas")
                    .with_description("All registered node and edge schemas")
                    .with_mime_type("application/json"),
            },
            Annotated {
                annotations: None,
                raw: RawResource::new("selene://info", "info")
                    .with_description("Server metadata: version, profile, dev mode, feature flags")
                    .with_mime_type("application/json"),
            },
        ];

        Ok(ListResourcesResult {
            meta: None,
            resources,
            next_cursor: None,
        })
    }

    /// List resource templates (parameterized resources).
    pub(crate) async fn handle_list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        let templates = vec![Annotated {
            annotations: None,
            raw: RawResourceTemplate::new("selene://schemas/{label}", "schema")
                .with_description("Schema definition for a specific label")
                .with_mime_type("application/json"),
        }];

        Ok(ListResourceTemplatesResult {
            meta: None,
            resource_templates: templates,
            next_cursor: None,
        })
    }

    /// Read a specific resource by URI.
    pub(crate) async fn handle_read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let uri = &request.uri;
        let auth = mcp_auth(self)?;

        let content = match uri.as_str() {
            "selene://health" => {
                let resp = ops::health::health(&self.state);
                serde_json::to_string_pretty(&resp).unwrap_or_default()
            }
            "selene://stats" => {
                let stats = ops::graph_stats::graph_stats(&self.state, &auth);
                serde_json::to_string_pretty(&serde_json::json!({
                    "node_count": stats.node_count,
                    "edge_count": stats.edge_count,
                    "node_labels": stats.node_labels,
                    "edge_labels": stats.edge_labels,
                }))
                .unwrap_or_default()
            }
            "selene://schemas" => {
                let node_schemas =
                    ops::schema::list_node_schemas(&self.state, &auth).map_err(op_err)?;
                let edge_schemas =
                    ops::schema::list_edge_schemas(&self.state, &auth).map_err(op_err)?;
                serde_json::to_string_pretty(&serde_json::json!({
                    "node_schemas": node_schemas.iter().map(|s| &*s.label).collect::<Vec<_>>(),
                    "edge_schemas": edge_schemas.iter().map(|s| &*s.label).collect::<Vec<_>>(),
                }))
                .unwrap_or_default()
            }
            "selene://info" => {
                let info = ops::info::server_info(&self.state);
                serde_json::to_string_pretty(&info).unwrap_or_default()
            }
            _ if uri.starts_with("selene://schemas/") => {
                let label = &uri["selene://schemas/".len()..];
                if let Ok(schema) = ops::schema::get_node_schema(&self.state, &auth, label) {
                    serde_json::to_string_pretty(&serde_json::json!({
                        "type": "node",
                        "schema": schema,
                    }))
                    .unwrap_or_default()
                } else if let Ok(schema) = ops::schema::get_edge_schema(&self.state, &auth, label) {
                    serde_json::to_string_pretty(&serde_json::json!({
                        "type": "edge",
                        "schema": schema,
                    }))
                    .unwrap_or_default()
                } else {
                    return Err(McpError {
                        code: rmcp::model::ErrorCode::INVALID_PARAMS,
                        message: format!("schema '{label}' not found").into(),
                        data: None,
                    });
                }
            }
            _ => {
                return Err(McpError {
                    code: rmcp::model::ErrorCode::INVALID_PARAMS,
                    message: format!("unknown resource URI: {uri}").into(),
                    data: None,
                });
            }
        };

        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(content, uri.clone()).with_mime_type("application/json"),
        ]))
    }
}
