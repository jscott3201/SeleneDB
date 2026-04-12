//! MCP resources -- read-only graph data exposed to agents.

use rmcp::model::{
    Annotated, Annotations, ListResourceTemplatesResult, ListResourcesResult,
    PaginatedRequestParams, RawResource, RawResourceTemplate, ReadResourceRequestParams,
    ReadResourceResult, ResourceContents,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, RoleServer};

use super::{SeleneTools, mcp_auth, op_err};
use crate::ops;

/// Build resource annotations with priority only (0.0 = lowest, 1.0 = highest).
fn resource_priority(priority: f32) -> Annotations {
    let mut a = Annotations::default();
    a.priority = Some(priority);
    a
}

impl SeleneTools {
    /// List all available static resources.
    pub(crate) async fn handle_list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let resources = vec![
            Annotated {
                annotations: Some(resource_priority(1.0)),
                raw: RawResource::new("selene://health", "health")
                    .with_description("Server health: uptime, node/edge counts, status")
                    .with_mime_type("application/json"),
            },
            Annotated {
                annotations: Some(resource_priority(0.8)),
                raw: RawResource::new("selene://stats", "stats")
                    .with_description("Graph statistics with per-label node and edge counts")
                    .with_mime_type("application/json"),
            },
            Annotated {
                annotations: Some(resource_priority(0.7)),
                raw: RawResource::new("selene://schemas", "schemas")
                    .with_description("All registered node and edge schemas")
                    .with_mime_type("application/json"),
            },
            Annotated {
                annotations: Some(resource_priority(0.5)),
                raw: RawResource::new("selene://info", "info")
                    .with_description("Server metadata: version, profile, dev mode, feature flags")
                    .with_mime_type("application/json"),
            },
            Annotated {
                annotations: Some(resource_priority(0.6)),
                raw: RawResource::new("selene://gql-examples", "gql-examples")
                    .with_description(
                        "Curated GQL query examples covering MATCH, INSERT, MERGE, \
                         aggregation, procedures, and parameterized queries",
                    )
                    .with_mime_type("text/plain"),
            },
            Annotated {
                annotations: Some(resource_priority(0.8)),
                raw: RawResource::new("selene://agents", "agents")
                    .with_description(
                        "Active agent sessions for multi-agent coordination. \
                         Subscribe for real-time updates when agents register, \
                         heartbeat, or deregister.",
                    )
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
        let templates = vec![
            Annotated {
                annotations: Some(resource_priority(0.7)),
                raw: RawResourceTemplate::new("selene://schemas/{label}", "schema")
                    .with_description("Schema definition for a specific label")
                    .with_mime_type("application/json"),
            },
            Annotated {
                annotations: Some(resource_priority(0.8)),
                raw: RawResourceTemplate::new("selene://agents/{project}", "agents-by-project")
                    .with_description(
                        "Active agent sessions filtered by project. \
                         Subscribe for real-time updates on project-scoped agent activity.",
                    )
                    .with_mime_type("application/json"),
            },
        ];

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

        let (content, mime) = match uri.as_str() {
            "selene://health" => {
                let resp = ops::health::health(&self.state);
                (
                    serde_json::to_string_pretty(&resp).unwrap_or_default(),
                    "application/json",
                )
            }
            "selene://stats" => {
                let stats = ops::graph_stats::graph_stats(&self.state, &auth);
                (
                    serde_json::to_string_pretty(&serde_json::json!({
                        "node_count": stats.node_count,
                        "edge_count": stats.edge_count,
                        "node_labels": stats.node_labels,
                        "edge_labels": stats.edge_labels,
                    }))
                    .unwrap_or_default(),
                    "application/json",
                )
            }
            "selene://schemas" => {
                let node_schemas =
                    ops::schema::list_node_schemas(&self.state, &auth).map_err(op_err)?;
                let edge_schemas =
                    ops::schema::list_edge_schemas(&self.state, &auth).map_err(op_err)?;
                (
                    serde_json::to_string_pretty(&serde_json::json!({
                        "node_schemas": node_schemas.iter().map(|s| &*s.label).collect::<Vec<_>>(),
                        "edge_schemas": edge_schemas.iter().map(|s| &*s.label).collect::<Vec<_>>(),
                    }))
                    .unwrap_or_default(),
                    "application/json",
                )
            }
            "selene://info" => {
                let info = ops::info::server_info(&self.state);
                (
                    serde_json::to_string_pretty(&info).unwrap_or_default(),
                    "application/json",
                )
            }
            "selene://gql-examples" => (GQL_EXAMPLES.to_string(), "text/plain"),
            "selene://agents" => {
                let query = "MATCH (a:__AgentSession) \
                             FILTER a.status = 'active' OR a.status = 'stale' OR a.status = 'working_locally' \
                             RETURN a.agent_id AS agent_id, a.project AS project, \
                             a.status AS status, a.working_on AS working_on, \
                             a.heartbeat_at AS heartbeat_at \
                             ORDER BY a.heartbeat_at DESC";
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
                (
                    result.data_json.unwrap_or_else(|| "[]".into()),
                    "application/json",
                )
            }
            _ if uri.starts_with("selene://agents/") => {
                let project = &uri["selene://agents/".len()..];
                let mut params = std::collections::HashMap::new();
                params.insert("project".into(), selene_core::Value::from(project));
                let query = "MATCH (a:__AgentSession {project: $project}) \
                             FILTER a.status = 'active' OR a.status = 'stale' OR a.status = 'working_locally' \
                             RETURN a.agent_id AS agent_id, a.project AS project, \
                             a.status AS status, a.working_on AS working_on, \
                             a.heartbeat_at AS heartbeat_at \
                             ORDER BY a.heartbeat_at DESC";
                let result = ops::gql::execute_gql(
                    &self.state,
                    &auth,
                    query,
                    Some(&params),
                    false,
                    false,
                    ops::gql::ResultFormat::Json,
                )
                .map_err(op_err)?;
                (
                    result.data_json.unwrap_or_else(|| "[]".into()),
                    "application/json",
                )
            }
            _ if uri.starts_with("selene://schemas/") => {
                let label = &uri["selene://schemas/".len()..];
                if let Ok(schema) = ops::schema::get_node_schema(&self.state, &auth, label) {
                    (
                        serde_json::to_string_pretty(&serde_json::json!({
                            "type": "node",
                            "schema": schema,
                        }))
                        .unwrap_or_default(),
                        "application/json",
                    )
                } else if let Ok(schema) = ops::schema::get_edge_schema(&self.state, &auth, label) {
                    (
                        serde_json::to_string_pretty(&serde_json::json!({
                            "type": "edge",
                            "schema": schema,
                        }))
                        .unwrap_or_default(),
                        "application/json",
                    )
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
            ResourceContents::text(content, uri.clone()).with_mime_type(mime),
        ]))
    }
}

/// Curated GQL examples for the `selene://gql-examples` resource.
/// Also used by the `text2gql` prompt.
pub(super) const GQL_EXAMPLES: &str = "\
# Selene GQL Examples

## MATCH patterns

# Single node by label
MATCH (s:sensor) RETURN s.name AS name, s.temp AS temp

# Traversal (one hop)
MATCH (b:building)-[:contains]->(f:floor) RETURN b.name AS building, f.name AS floor

# Multi-hop traversal
MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(r:room) \
RETURN b.name AS building, f.name AS floor, r.name AS room

# Variable-length path (1 to 3 hops)
MATCH (a)-[:contains]->{1,3}(b) RETURN a.name AS ancestor, b.name AS descendant

# Filtering
MATCH (s:sensor) FILTER s.temp > 72.0 RETURN s.name AS name, s.temp AS temp

# Multiple filters
MATCH (s:sensor) FILTER s.temp > 68.0 AND s.status = 'active' RETURN s.name AS name

# Optional match
OPTIONAL MATCH (s:sensor)-[:monitors]->(e:equipment) RETURN s.name AS sensor, e.name AS equipment

## Aggregation

# Count by label
MATCH (n) RETURN DISTINCT labels(n) AS labels, count(*) AS count

# Average value
MATCH (s:sensor) RETURN avg(s.temp) AS avg_temp

# Sum with grouping
MATCH (f:floor)-[:contains]->(r:room) RETURN f.name AS floor, count(r) AS room_count

# Multiple aggregates
MATCH (s:sensor) RETURN min(s.temp) AS min_temp, max(s.temp) AS max_temp, avg(s.temp) AS avg_temp

## Write patterns

# Insert a node
INSERT (:sensor {name: 'TempSensor1', temp: 72.5, status: 'active'})

# Insert an edge
MATCH (s:sensor), (r:room) FILTER s.name = 'TempSensor1' AND r.name = 'Room101' \
INSERT (r)-[:contains]->(s)

# Merge (create if not exists)
MERGE (:building {name: 'Main Building'})

# Set properties
MATCH (s:sensor) FILTER s.name = 'TempSensor1' SET s.temp = 73.0, s.updated = TRUE

# Delete a node
MATCH (s:sensor) FILTER s.name = 'OldSensor' DELETE s

# Delete with detach (removes edges too)
MATCH (s:sensor) FILTER s.name = 'OldSensor' DETACH DELETE s

## Procedures

# Schema dump (LLM-friendly overview)
CALL graph.schemaDump() YIELD schema RETURN schema

# Latest time-series value
CALL ts.latest($entityId, $property) YIELD value, timestamp RETURN value, timestamp

# Vector search (requires vector feature)
CALL graph.vectorSearch($queryVector, $k) YIELD nodeId, score RETURN nodeId, score

# Semantic search by text (requires vector feature)
CALL graph.semanticSearch($queryText, $k) YIELD nodeId, score RETURN nodeId, score

## Parameterized queries

# Use $param syntax for safe parameter binding
MATCH (s:sensor) FILTER s.name = $name RETURN s.temp AS temp
# Parameters: {\"name\": \"TempSensor1\"}

MATCH (n) FILTER id(n) = $nodeId RETURN n
# Parameters: {\"nodeId\": 42}

INSERT (:sensor {name: $name, temp: $temp})
# Parameters: {\"name\": \"NewSensor\", \"temp\": 72.5}
";
