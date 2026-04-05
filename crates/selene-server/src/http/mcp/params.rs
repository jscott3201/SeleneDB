//! MCP parameter types for tool input deserialization.

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::Deserialize;

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
    pub(crate) id: u64,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct NodeEdgesParams {
    /// Numeric node ID.
    pub(crate) id: u64,
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
    pub(crate) id: u64,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct CreateEdgeParams {
    /// Source node ID (the "from" end).
    pub(crate) source: u64,
    /// Target node ID (the "to" end).
    pub(crate) target: u64,
    /// Relationship type (e.g., "contains", "feeds", "isPointOf", "monitors").
    pub(crate) label: String,
    /// Key-value properties for the edge.
    #[serde(default)]
    pub(crate) properties: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ModifyEdgeParams {
    /// Edge ID to modify.
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
    /// Maximum number of samples to return (default: 1000).
    #[serde(default)]
    pub(crate) limit: Option<u64>,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct GraphSliceParams {
    /// Slice type: "full", "labels", or "containment".
    #[serde(default = "default_slice_type")]
    pub(crate) slice_type: String,
    /// For "labels" slice: which labels to include.
    #[serde(default)]
    pub(crate) labels: Option<Vec<String>>,
    /// For "containment" slice: root node ID.
    #[serde(default)]
    pub(crate) root_id: Option<u64>,
    /// For "containment" slice: maximum traversal depth.
    #[serde(default)]
    pub(crate) max_depth: Option<u32>,
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
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct SimilarNodesParams {
    /// Reference node ID to find similar nodes for.
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

#[cfg(feature = "ai")]
#[derive(Deserialize, JsonSchema)]
pub(crate) struct BuildCommunitiesParams {
    /// Minimum community size to persist. Communities smaller than this are skipped. Default: 2.
    #[serde(default)]
    pub(crate) min_community_size: Option<usize>,
}

#[cfg(feature = "ai")]
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

#[cfg(feature = "ai")]
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

#[cfg(feature = "ai")]
fn default_memory_type() -> String {
    "fact".into()
}

#[cfg(feature = "ai")]
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

#[cfg(feature = "ai")]
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

#[cfg(feature = "ai")]
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
    /// Eviction policy: "clock" (default). Reserved for future policies.
    #[serde(default)]
    pub(crate) eviction_policy: Option<String>,
}
