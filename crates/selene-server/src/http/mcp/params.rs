//! MCP parameter types for tool input deserialization.

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::Deserialize;

/// Accept both `123` (number) and `"123"` (string) for u64 ID fields.
/// MCP transports may serialize integer IDs as strings depending on the
/// client implementation.
fn deserialize_u64_or_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let v = serde_json::Value::deserialize(deserializer)?;
    match &v {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| D::Error::custom(format!("expected u64, got {v}"))),
        serde_json::Value::String(s) => s
            .parse::<u64>()
            .map_err(|_| D::Error::custom(format!("cannot parse '{s}' as u64"))),
        _ => Err(D::Error::custom(format!(
            "expected number or string, got {v}"
        ))),
    }
}

/// Accept both a single string `"sensor"` and an array `["sensor", "temperature"]`
/// for label fields. MCP clients often pass a single label as a bare string.
fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(deserializer)?;
    match v {
        serde_json::Value::String(s) => Ok(vec![s]),
        serde_json::Value::Array(arr) => arr
            .into_iter()
            .map(|item| match item {
                serde_json::Value::String(s) => Ok(s),
                other => Err(serde::de::Error::custom(format!(
                    "expected string in labels array, got {other}"
                ))),
            })
            .collect(),
        other => Err(serde::de::Error::custom(format!(
            "expected string or array for labels, got {other}"
        ))),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn default_true_opt() -> Option<bool> {
    Some(true)
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct GqlParams {
    /// GQL query text. Example: MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name AS name
    pub(crate) query: String,
    /// Query parameters for parameterized queries (optional).
    #[serde(default)]
    pub(crate) parameters: Option<HashMap<String, serde_json::Value>>,
    /// Query timeout in milliseconds (optional).
    #[serde(default)]
    pub(crate) timeout_ms: Option<u32>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct GqlExplainParams {
    /// GQL query to explain (returns the execution plan without executing).
    pub(crate) query: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct NodeIdParams {
    /// Numeric node ID.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) id: u64,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct NodeEdgesParams {
    /// Numeric node ID.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) id: u64,
    /// Filter by direction: "outgoing", "incoming", or "both" (default).
    #[serde(default)]
    pub(crate) direction: Option<String>,
    /// Filter to specific edge label(s). Omit for all labels.
    #[serde(default)]
    pub(crate) labels: Option<Vec<String>>,
    /// Maximum number of edges to return (default: 1000, max: 10000).
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    /// Number of edges to skip (for pagination).
    #[serde(default)]
    pub(crate) offset: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct CreateNodeParams {
    /// Labels to assign (e.g., ["sensor", "temperature"]). At least one required.
    #[serde(deserialize_with = "deserialize_string_or_vec")]
    pub(crate) labels: Vec<String>,
    /// Key-value properties (e.g., {"unit": "°F", "threshold": 72.5}).
    #[serde(default)]
    pub(crate) properties: HashMap<String, serde_json::Value>,
    /// Parent node ID for containment. Creates a "contains" edge from parent to this node.
    #[serde(default)]
    pub(crate) parent_id: Option<u64>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ModifyNodeParams {
    /// Node ID to modify.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) id: u64,
    /// Properties to set or update.
    #[serde(default)]
    pub(crate) set_properties: HashMap<String, serde_json::Value>,
    /// Property keys to remove.
    #[serde(default)]
    pub(crate) remove_properties: Vec<String>,
    /// Labels to add.
    #[serde(default)]
    pub(crate) add_labels: Vec<String>,
    /// Labels to remove.
    #[serde(default)]
    pub(crate) remove_labels: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ListNodesParams {
    /// Filter by label (e.g., "sensor"). Omit to list all.
    pub(crate) label: Option<String>,
    /// Maximum number of nodes to return (default: 100).
    pub(crate) limit: Option<u64>,
    /// Number of nodes to skip (for pagination).
    #[serde(default)]
    pub(crate) offset: Option<u64>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct EdgeIdParams {
    /// Numeric edge ID.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) id: u64,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct CreateEdgeParams {
    /// Source node ID (the "from" end).
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) source: u64,
    /// Target node ID (the "to" end).
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) target: u64,
    /// Relationship type (e.g., "contains", "feeds", "isPointOf", "monitors").
    pub(crate) label: String,
    /// Key-value properties for the edge.
    #[serde(default)]
    pub(crate) properties: HashMap<String, serde_json::Value>,
    /// If true, return existing edge instead of creating a duplicate when an edge
    /// with the same source, target, and label already exists. Properties on the
    /// existing edge are updated with any new values provided.
    #[serde(default)]
    pub(crate) upsert: Option<bool>,
}

/// Single node entry for batch creation.
#[derive(Deserialize, JsonSchema)]
pub(crate) struct BatchNodeEntry {
    /// Labels to assign.
    #[serde(deserialize_with = "deserialize_string_or_vec")]
    pub(crate) labels: Vec<String>,
    /// Key-value properties.
    #[serde(default)]
    pub(crate) properties: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct BatchCreateNodesParams {
    /// Array of nodes to create. Each entry has labels and optional properties.
    pub(crate) nodes: Vec<BatchNodeEntry>,
}

/// Single edge entry for batch creation.
#[derive(Deserialize, JsonSchema)]
pub(crate) struct BatchEdgeEntry {
    /// Source node ID.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) source: u64,
    /// Target node ID.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) target: u64,
    /// Edge label.
    pub(crate) label: String,
    /// Key-value properties.
    #[serde(default)]
    pub(crate) properties: HashMap<String, serde_json::Value>,
    /// Deduplicate on (source, target, label).
    #[serde(default)]
    pub(crate) upsert: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct BatchCreateEdgesParams {
    /// Array of edges to create. Each entry has source, target, label, and optional properties.
    pub(crate) edges: Vec<BatchEdgeEntry>,
    /// Batch-level upsert default. When true, all edges deduplicate on (source,
    /// target, label) unless overridden by the per-edge `upsert` flag. Default: false.
    #[serde(default)]
    pub(crate) upsert: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ModifyEdgeParams {
    /// Edge ID to modify.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) id: u64,
    /// Properties to set or update.
    #[serde(default)]
    pub(crate) set_properties: HashMap<String, serde_json::Value>,
    /// Property keys to remove.
    #[serde(default)]
    pub(crate) remove_properties: Vec<String>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ListEdgesParams {
    /// Filter by edge label (e.g., "contains"). Omit to list all.
    pub(crate) label: Option<String>,
    /// Maximum number of edges to return (default: 100).
    pub(crate) limit: Option<u64>,
    /// Number of edges to skip (for pagination).
    #[serde(default)]
    pub(crate) offset: Option<u64>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct TsWriteParams {
    /// Time-series samples to write.
    pub(crate) samples: Vec<TsSampleParam>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct TsSampleParam {
    /// Node ID the sample belongs to.
    pub(crate) entity_id: u64,
    /// Property name (e.g., "temperature", "humidity").
    pub(crate) property: String,
    /// Timestamp in nanoseconds since Unix epoch.
    pub(crate) timestamp_nanos: i64,
    /// Numeric value.
    pub(crate) value: f64,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct TsQueryParams {
    /// Node ID to query.
    pub(crate) entity_id: u64,
    /// Property name to query.
    pub(crate) property: String,
    /// Start timestamp (nanos). Omit for earliest.
    #[serde(default)]
    pub(crate) start: Option<i64>,
    /// End timestamp (nanos). Omit for latest.
    #[serde(default)]
    pub(crate) end: Option<i64>,
    /// Maximum number of samples to return (default: 1000). Only used when aggregation is "raw".
    #[serde(default)]
    pub(crate) limit: Option<u64>,
    /// Aggregation bucket duration: "5m", "15m", "1h", "1d", "auto", or "raw" (default).
    /// "auto" selects based on time range: <4h=raw, <24h=5m, <7d=15m, <30d=1h, else=1d.
    #[serde(default)]
    pub(crate) aggregation: Option<String>,
    /// Aggregate function: "avg" (default), "min", "max", "sum", "count".
    /// Only used when aggregation is not "raw".
    #[serde(default)]
    pub(crate) function: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct GraphSliceParams {
    /// Slice type: "full", "labels", "containment", or "traverse".
    #[serde(default = "default_slice_type")]
    pub(crate) slice_type: String,
    /// For "labels" slice: which labels to include. For "traverse": edge labels to follow.
    #[serde(default)]
    pub(crate) labels: Option<Vec<String>>,
    /// For "containment"/"traverse" slice: root node ID.
    #[serde(default)]
    pub(crate) root_id: Option<u64>,
    /// For "containment"/"traverse" slice: maximum traversal depth.
    #[serde(default)]
    pub(crate) max_depth: Option<u32>,
    /// For "traverse" slice: "outgoing" (default), "incoming", or "both".
    #[serde(default)]
    pub(crate) direction: Option<String>,
    /// Pagination: max nodes to return.
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    /// Pagination: nodes to skip.
    #[serde(default)]
    pub(crate) offset: Option<usize>,
}

pub(crate) fn default_slice_type() -> String {
    "full".into()
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct SchemaLabelParams {
    /// Schema label to look up (e.g., "temperature_sensor").
    pub(crate) label: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ImportPackParams {
    /// Schema pack content in TOML or JSON format. Format is auto-detected.
    pub(crate) content: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct CreateSchemaParams {
    /// Type label (e.g., "smart_thermostat"). Lowercase, underscores for spaces.
    pub(crate) label: String,
    /// Parent type to inherit from (e.g., "equipment", "point", "sensor").
    #[serde(default)]
    pub(crate) extends: Option<String>,
    /// Human-readable description.
    #[serde(default)]
    pub(crate) description: Option<String>,
    /// Fields using shorthand syntax. Key is field name, value is type spec.
    /// Examples: "string!" (required string), "float = 72.5" (float with default),
    /// "string = '°F'" (string with default), "bool" (optional bool).
    /// Types: string, int, float, bool, timestamp, bytes, list, any
    #[serde(default)]
    pub(crate) fields: HashMap<String, String>,
    /// Valid edge labels for this type.
    #[serde(default)]
    pub(crate) edges: Vec<String>,
    /// Application-defined annotations (e.g., {"brick": "Temperature_Sensor"}).
    #[serde(default)]
    pub(crate) annotations: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct CreateEdgeSchemaParams {
    /// Edge type label (e.g., "feeds", "contains", "monitors"). Lowercase, underscores for spaces.
    pub(crate) label: String,
    /// Human-readable description.
    #[serde(default)]
    pub(crate) description: Option<String>,
    /// Fields using shorthand syntax. Key is field name, value is type spec.
    /// Examples: "string!" (required string), "float = 72.5" (float with default).
    /// Types: string, int, float, bool, timestamp, bytes, list, any
    #[serde(default)]
    pub(crate) fields: HashMap<String, String>,
    /// Restrict source nodes to these labels (empty = any label allowed).
    #[serde(default)]
    pub(crate) source_labels: Vec<String>,
    /// Restrict target nodes to these labels (empty = any label allowed).
    #[serde(default)]
    pub(crate) target_labels: Vec<String>,
    /// Minimum outgoing edges of this type per source node. When set, nodes
    /// with matching source labels are warned at commit time if they have fewer
    /// than this many outgoing edges of this type.
    #[serde(default)]
    pub(crate) min_out_degree: Option<u32>,
    /// Minimum incoming edges of this type per target node.
    #[serde(default)]
    pub(crate) min_in_degree: Option<u32>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct RFExportParams {
    /// Optional label filter. If set, only exports nodes with this label.
    #[serde(default)]
    pub(crate) label: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct SemanticSearchParams {
    /// Natural language query text (e.g., "supply air temperature sensor").
    pub(crate) query_text: String,
    /// Maximum number of results to return.
    pub(crate) k: i64,
    /// Optional label filter (e.g., "sensor"). Omit to search all nodes.
    #[serde(default)]
    pub(crate) label: Option<String>,
    /// If true, include full node properties (name, labels, all properties)
    /// with each result. Saves follow-up get_node calls. Default: false.
    #[serde(default)]
    pub(crate) include_properties: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct SimilarNodesParams {
    /// Reference node ID to find similar nodes for.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) node_id: u64,
    /// Vector property name to compare (e.g., "embedding").
    pub(crate) property: String,
    /// Maximum number of results to return.
    pub(crate) k: i64,
}

pub(crate) fn default_mcp_csv_type() -> String {
    "nodes".into()
}

pub(crate) fn default_mcp_csv_delimiter() -> String {
    ",".into()
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct McpCsvImportParams {
    /// CSV data to import.
    pub(crate) content: String,
    /// Import type: "nodes" (default) or "edges".
    #[serde(default = "default_mcp_csv_type")]
    pub(crate) csv_type: String,
    /// Label to apply to all imported nodes (required for node import, ignored for edges).
    #[serde(default)]
    pub(crate) label: Option<String>,
    /// Field delimiter (default: ",").
    #[serde(default = "default_mcp_csv_delimiter")]
    pub(crate) delimiter: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct McpCsvExportParams {
    /// Export type: "nodes" (default) or "edges".
    #[serde(default = "default_mcp_csv_type")]
    pub(crate) csv_type: String,
    /// Optional label filter. For nodes: filter by node label. For edges: filter by edge label.
    #[serde(default)]
    pub(crate) label: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct UpdateSchemaParams {
    /// Schema label to update.
    pub(crate) label: String,
    /// Parent type to inherit from. Set to null to remove parent.
    #[serde(default)]
    pub(crate) extends: Option<String>,
    /// Human-readable description.
    #[serde(default)]
    pub(crate) description: Option<String>,
    /// Fields using shorthand syntax (replaces existing fields).
    /// Examples: "string!" (required string), "float = 72.5" (float with default).
    #[serde(default)]
    pub(crate) fields: HashMap<String, String>,
    /// Valid edge labels for this type.
    #[serde(default)]
    pub(crate) edges: Vec<String>,
    /// Application-defined annotations.
    #[serde(default)]
    pub(crate) annotations: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct SparqlQueryParams {
    /// SPARQL query to execute against the graph.
    pub(crate) query: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ParseCheckParams {
    /// The GQL query to parse and check.
    pub(crate) query: String,
}

// ── AI / GraphRAG ────────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub(crate) struct BuildCommunitiesParams {
    /// Minimum community size to persist. Communities smaller than this are skipped. Default: 2.
    #[serde(default)]
    pub(crate) min_community_size: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct GraphRagSearchParams {
    /// Natural language query text.
    pub(crate) query: String,
    /// Number of vector search results (default: 10).
    #[serde(default)]
    pub(crate) k: Option<i64>,
    /// BFS expansion depth (default: 2).
    #[serde(default)]
    pub(crate) max_hops: Option<i64>,
    /// Search mode: "local" (default), "global", or "hybrid".
    #[serde(default)]
    pub(crate) mode: Option<String>,
}

// ── AI / Agent Memory ───────────────���───────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub(crate) struct RememberParams {
    /// Memory namespace (isolates memories by agent or context).
    pub(crate) namespace: String,
    /// The content to remember.
    pub(crate) content: String,
    /// Memory type classification (default: "fact"). Examples: "fact", "preference", "event".
    #[serde(default = "default_memory_type")]
    pub(crate) memory_type: String,
    /// Expiry timestamp in milliseconds since epoch. 0 or omit for no expiry.
    #[serde(default)]
    pub(crate) valid_until: Option<i64>,
    /// Entity names mentioned in this memory. Creates __Entity nodes and __MENTIONS edges.
    #[serde(default)]
    pub(crate) entities: Option<Vec<String>>,
}

fn default_memory_type() -> String {
    "fact".into()
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct RecallParams {
    /// Memory namespace to search.
    pub(crate) namespace: String,
    /// Natural language query text for semantic search.
    pub(crate) query: String,
    /// Maximum number of results (default: 10).
    #[serde(default)]
    pub(crate) k: Option<i64>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ForgetParams {
    /// Memory namespace to delete from.
    pub(crate) namespace: String,
    /// Specific memory node ID to delete.
    #[serde(default)]
    pub(crate) node_id: Option<u64>,
    /// Content substring to match for deletion.
    #[serde(default)]
    pub(crate) query: Option<String>,
}

// ── Resolve + Related ────────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ResolveParams {
    /// The identifier to resolve. Can be a numeric ID, exact name, or
    /// natural language description.
    pub(crate) identifier: String,
    /// Optional label hint to narrow resolution (e.g., "equipment", "zone").
    #[serde(default)]
    pub(crate) label: Option<String>,
    /// If true, include the containment path (parent chain). Default: true.
    #[serde(default = "default_true_opt")]
    pub(crate) include_path: Option<bool>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct RelatedParams {
    /// Numeric node ID.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) id: u64,
    /// Filter to specific edge label(s). Omit for all labels.
    #[serde(default)]
    pub(crate) edge_labels: Option<Vec<String>>,
    /// Filter by direction: "outgoing", "incoming", or "both" (default).
    #[serde(default)]
    pub(crate) direction: Option<String>,
    /// Maximum number of neighbors to return (default: 25).
    #[serde(default)]
    pub(crate) neighbor_limit: Option<usize>,
}

// ── Trace (training data) ────────────────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub(crate) struct LogTraceParams {
    /// Session identifier for grouping related traces.
    pub(crate) session_id: String,
    /// Turn number within the session.
    pub(crate) turn: i64,
    /// Name of the tool that was called.
    pub(crate) tool_name: String,
    /// JSON string of tool parameters.
    pub(crate) tool_params: String,
    /// Compact summary of the tool result.
    pub(crate) tool_result_summary: String,
    /// What the agent said after this tool call.
    #[serde(default)]
    pub(crate) agent_response: Option<String>,
    /// Feedback: "approved", "rejected", "corrected", or "none" (default).
    #[serde(default)]
    pub(crate) feedback: Option<String>,
    /// If feedback is "corrected", the correct answer.
    #[serde(default)]
    pub(crate) correction: Option<String>,
    /// Which model generated this trace.
    #[serde(default)]
    pub(crate) model_id: Option<String>,
    /// Tool execution time in milliseconds.
    #[serde(default)]
    pub(crate) latency_ms: Option<i64>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ExportTracesParams {
    /// Filter by session ID.
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    /// Filter by tool name.
    #[serde(default)]
    pub(crate) tool_name: Option<String>,
    /// Filter by feedback type.
    #[serde(default)]
    pub(crate) feedback: Option<String>,
    /// Filter by model ID.
    #[serde(default)]
    pub(crate) model_id: Option<String>,
    /// Start timestamp (ms). Omit for earliest.
    #[serde(default)]
    pub(crate) start_ms: Option<i64>,
    /// End timestamp (ms). Omit for latest.
    #[serde(default)]
    pub(crate) end_ms: Option<i64>,
    /// Maximum traces to return (default: 1000, max: 10000).
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    /// Output format: "jsonl" (default) or "json".
    #[serde(default)]
    pub(crate) format: Option<String>,
}

// ── Proposals (human-in-the-loop) ───────────────────────────────────

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ProposeActionParams {
    /// Human-readable description of the proposed action.
    pub(crate) description: String,
    /// The GQL query to execute if approved.
    pub(crate) query: String,
    /// Category for grouping proposals (e.g., "setpoint_change", "schedule").
    #[serde(default)]
    pub(crate) category: Option<String>,
    /// Priority: "low", "normal" (default), "high".
    #[serde(default)]
    pub(crate) priority: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ProposalIdParams {
    /// Numeric proposal node ID.
    #[serde(deserialize_with = "deserialize_u64_or_string")]
    pub(crate) proposal_id: u64,
    /// Optional reason for the action.
    #[serde(default)]
    pub(crate) reason: Option<String>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ListProposalsParams {
    /// Filter by status: "pending", "approved", "executed", "rejected", "expired". Omit for all.
    #[serde(default)]
    pub(crate) status: Option<String>,
    /// Maximum proposals to return (default: 50).
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ConfigureMemoryParams {
    /// Memory namespace to configure.
    pub(crate) namespace: String,
    /// Maximum number of memories before eviction (0 = unlimited, default: 1000).
    #[serde(default)]
    pub(crate) max_memories: Option<i64>,
    /// Default time-to-live in milliseconds for new memories (0 = no expiry).
    #[serde(default)]
    pub(crate) default_ttl_ms: Option<i64>,
    /// Eviction policy: "clock" (default), "oldest", or "lowest_confidence".
    #[serde(default)]
    pub(crate) eviction_policy: Option<String>,
}

// ── Principal management params ──────────────────────────────────────

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct GetPrincipalParams {
    /// The identity of the principal to retrieve.
    pub(crate) identity: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct CreatePrincipalParams {
    /// Unique identity string for the new principal.
    pub(crate) identity: String,
    /// Role: admin, service, operator, reader, or device.
    pub(crate) role: String,
    /// Optional password. If omitted, the principal has no credential (OAuth-only).
    #[serde(default)]
    pub(crate) password: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct UpdatePrincipalParams {
    /// Identity of the principal to update.
    pub(crate) identity: String,
    /// New role (admin, service, operator, reader, device). Leave unset to keep current.
    #[serde(default)]
    pub(crate) role: Option<String>,
    /// Set to true/false to enable/disable. Leave unset to keep current.
    #[serde(default)]
    pub(crate) enabled: Option<bool>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct DisablePrincipalParams {
    /// Identity of the principal to disable.
    pub(crate) identity: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct RotateCredentialParams {
    /// Identity of the principal whose credential to rotate.
    pub(crate) identity: String,
    /// The new password.
    pub(crate) new_password: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_node_accepts_array_labels() {
        let json = serde_json::json!({
            "labels": ["sensor", "temperature"],
            "properties": {}
        });
        let params: CreateNodeParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.labels, vec!["sensor", "temperature"]);
    }

    #[test]
    fn create_node_accepts_single_string_label() {
        let json = serde_json::json!({
            "labels": "sensor",
            "properties": {}
        });
        let params: CreateNodeParams = serde_json::from_value(json).unwrap();
        assert_eq!(params.labels, vec!["sensor"]);
    }

    #[test]
    fn batch_node_entry_accepts_single_string_label() {
        let json = serde_json::json!({
            "labels": "equipment"
        });
        let entry: BatchNodeEntry = serde_json::from_value(json).unwrap();
        assert_eq!(entry.labels, vec!["equipment"]);
    }
}
