# MCP Integration Guide

## Overview

The Model Context Protocol (MCP) is an open standard that enables AI agents and LLM-based tools
to interact with external services through a structured tool interface. Selene exposes its full
graph, time-series, and schema APIs as MCP tools, resources, and prompts, allowing AI assistants
to query, mutate, and manage the property graph through natural language.

MCP is served over Streamable HTTP at the `/mcp` endpoint (rmcp 1.3, JSON-RPC 2.0). Any
MCP-compatible client (Claude Desktop, Cursor, custom agents) can connect without a dedicated SDK.

Selene declares three MCP capabilities:
- **Tools** (36): actions that query, mutate, import/export data
- **Resources** (5): read-only data agents can inspect without a tool call round-trip
- **Prompts** (3): guided workflow templates for common agent tasks

## Enabling MCP

### Development mode

In development mode, MCP is available without authentication:

```toml
dev_mode = true

[mcp]
enabled = true
```

### Production mode

In production, MCP supports two authentication methods:

1. **OAuth 2.1** (recommended) for interactive clients (Claude Desktop, Cursor) and headless agents. See [OAuth Authentication](#oauth-authentication) below.
2. **API key** for simpler deployments where a shared secret is acceptable.

#### OAuth 2.1 (recommended)

Enable OAuth by providing a signing key for token generation:

```toml
[mcp]
enabled = true
signing_key = "base64-encoded-32-byte-key"
```

Generate a signing key with `openssl rand -base64 32`. Clients register via
`POST /oauth/register` and then obtain tokens through the standard OAuth 2.1
authorization code or client credentials flow. See
[OAuth Authentication](#oauth-authentication) for the full walkthrough.

#### API key (simple alternative)

For deployments that do not need per-client tokens, configure a shared API key:

```toml
[mcp]
enabled = true
api_key = "your-secret-api-key"
```

MCP clients must include the key in every HTTP request:

```
Authorization: Bearer your-secret-api-key
```

Requests without a valid key receive HTTP 401. The key is validated with
constant-time comparison to prevent timing attacks.

#### Endpoint availability

When the server starts with MCP enabled, the endpoint is available at:

```
http://<host>:8080/mcp
```

If MCP is enabled but neither `signing_key` nor `api_key` is configured (and
`dev_mode` is false), the endpoint is not mounted and logs a message at startup.

## OAuth Authentication

Selene implements OAuth 2.1 with PKCE for the MCP endpoint. This provides
per-client token management, scoped access, and token rotation without sharing
a static secret.

### Overview

Two grant types are supported:

- **Authorization code with PKCE** for interactive clients such as Claude Desktop
  and Cursor. The client opens a browser for user consent, receives an
  authorization code, and exchanges it for tokens.
- **Client credentials** for headless agents and automated pipelines. The client
  authenticates directly with its `client_id` and `client_secret`.

All OAuth endpoints are served under the HTTP listener alongside `/mcp`:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/oauth/register` | POST | Dynamic client registration |
| `/oauth/authorize` | GET | Authorization code flow (browser redirect) |
| `/oauth/token` | POST | Token exchange and refresh |

### Client registration

Before using either flow, register a client:

```bash
curl -X POST http://localhost:8080/oauth/register \
  -H "Content-Type: application/json" \
  -d '{
    "client_name": "my-agent",
    "redirect_uris": ["http://localhost:9876/callback"],
    "grant_types": ["authorization_code"],
    "response_types": ["code"]
  }'
```

The response contains the `client_id` and, for confidential clients, a
`client_secret`. Store these securely.

For headless agents, register with the `client_credentials` grant type:

```bash
curl -X POST http://localhost:8080/oauth/register \
  -H "Content-Type: application/json" \
  -d '{
    "client_name": "headless-pipeline",
    "grant_types": ["client_credentials"]
  }'
```

If `require_approval = true` is set in the server config, newly registered
clients remain inactive until an administrator approves them.

### Authorization code flow (Claude Desktop / Cursor)

Interactive MCP clients handle this flow automatically. The sequence is:

1. The client generates a PKCE `code_verifier` and derives the `code_challenge`.
2. The client opens `GET /oauth/authorize?response_type=code&client_id=...&code_challenge=...&code_challenge_method=S256&redirect_uri=...` in the browser.
3. The user approves the request. The server redirects to the `redirect_uri` with a `code` parameter.
4. The client exchanges the code for tokens via `POST /oauth/token` with `grant_type=authorization_code`, the `code`, and the `code_verifier`.
5. The server returns an `access_token` and a `refresh_token`.

MCP clients that support OAuth (Claude Desktop, Cursor) perform these steps
without manual configuration. Point them at the `/mcp` endpoint and the
discovery metadata at `/.well-known/oauth-authorization-server` guides the flow.

### Client credentials flow (headless agents)

For agents that run without user interaction, exchange the client credentials
directly for an access token:

```bash
curl -X POST http://localhost:8080/oauth/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=CLIENT_ID&client_secret=CLIENT_SECRET"
```

The response contains an `access_token` (and optionally a `refresh_token`).
Include it in subsequent MCP requests:

```
Authorization: Bearer <access_token>
```

### Token refresh

When an access token expires, use the refresh token to obtain a new pair:

```bash
curl -X POST http://localhost:8080/oauth/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=refresh_token&client_id=CLIENT_ID&refresh_token=REFRESH_TOKEN"
```

The server returns a new `access_token` and a rotated `refresh_token`. The
previous refresh token is invalidated.

### Configuration reference

OAuth behavior is controlled by fields in the `[mcp]` TOML section:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `signing_key` | string | (none) | Base64-encoded 32-byte key for signing JWT tokens. Required to enable OAuth. Generate with `openssl rand -base64 32` |
| `require_approval` | bool | `false` | When `true`, newly registered clients must be approved before they can obtain tokens |
| `access_token_ttl_secs` | integer | `3600` | Access token lifetime in seconds (default: 1 hour) |
| `refresh_token_ttl_secs` | integer | `604800` | Refresh token lifetime in seconds (default: 7 days) |

See the [Configuration guide](../operations/configuration.md#mcp) for the full
`[mcp]` table reference.

## Resources

Resources provide read-only graph state that agents can inspect without a tool call.

| URI | Description |
|-----|-------------|
| `selene://health` | Server health: uptime, node/edge counts, status |
| `selene://stats` | Graph statistics with per-label node and edge counts |
| `selene://schemas` | All registered node and edge schema labels |
| `selene://info` | Server metadata: version, profile, dev mode, feature flags |
| `selene://schemas/{label}` | Schema definition for a specific label (template) |

## Prompts

Prompt templates provide guided workflows for common agent tasks.

| Prompt | Arguments | Description |
|--------|-----------|-------------|
| `explore-graph` | none | Returns live health, stats, schema summary, and suggested next steps |
| `query-helper` | `intent: string` | Provides schema context and GQL syntax notes for a natural language query |
| `import-guide` | `format: string` | Step-by-step import instructions for csv, json, or toml |

## Tool Reference

Selene exposes 36 MCP tools organized into ten categories. Each tool accepts a JSON
object of parameters and returns structured text results.

### GQL (2 tools)

GQL is the primary query and mutation interface. These two tools cover all read and
write operations against the property graph.

| Tool | Description |
|------|-------------|
| `gql_query` | Execute a GQL query or mutation against the property graph. Returns GQLSTATUS, JSON results, and mutation summaries. |
| `gql_explain` | Show the execution plan for a GQL query without executing it. Useful for understanding optimizer decisions. |

**gql_query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | yes | GQL query text |

**gql_explain parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | yes | GQL query to explain |

Example queries:

```
MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name AS name
MATCH (b:building)-[:contains]->(f:floor) RETURN b.name AS building, f.name AS floor
INSERT (:sensor {name: "NewSensor", temp: 72.5})
MATCH (s:sensor) FILTER s.temp > 72 SET s.alert = TRUE
```

### Node CRUD (6 tools)

| Tool | Description |
|------|-------------|
| `get_node` | Get a node by its numeric ID. Returns labels, properties, timestamps, and version. |
| `create_node` | Create a new node with labels and optional properties. Schema defaults are applied automatically. |
| `modify_node` | Modify a node: set/remove properties, add/remove labels. Only specified changes are applied. |
| `delete_node` | Delete a node and all its connected edges. Irreversible. |
| `list_nodes` | List nodes, optionally filtered by label. Supports limit/offset pagination. |
| `node_edges` | Get all edges connected to a node (both incoming and outgoing). |

**get_node / delete_node / node_edges parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | integer | yes | Numeric node ID |

**create_node parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `labels` | string[] | yes | Labels to assign (e.g., `["sensor", "temperature"]`) |
| `properties` | object | no | Key-value properties (e.g., `{"unit": "°F", "threshold": 72.5}`) |
| `parent_id` | integer | no | Parent node ID. Creates a `contains` edge from parent to this node. |

**modify_node parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | integer | yes | Node ID to modify |
| `set_properties` | object | no | Properties to set or update |
| `remove_properties` | string[] | no | Property keys to remove |
| `add_labels` | string[] | no | Labels to add |
| `remove_labels` | string[] | no | Labels to remove |

**list_nodes parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `label` | string | no | Filter by label (e.g., `"sensor"`). Omit to list all. |
| `limit` | integer | no | Maximum nodes to return (default: 100) |
| `offset` | integer | no | Number of nodes to skip for pagination |

### Edge CRUD (5 tools)

| Tool | Description |
|------|-------------|
| `get_edge` | Get an edge by its numeric ID. Returns source, target, label, and properties. |
| `create_edge` | Create a directed edge between two nodes. |
| `modify_edge` | Modify an edge's properties. Set new properties or remove existing ones. |
| `delete_edge` | Delete an edge by ID. Irreversible. |
| `list_edges` | List edges, optionally filtered by label. Supports limit/offset pagination. |

**get_edge / delete_edge parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | integer | yes | Numeric edge ID |

**create_edge parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source` | integer | yes | Source node ID (the "from" end) |
| `target` | integer | yes | Target node ID (the "to" end) |
| `label` | string | yes | Relationship type (e.g., `"contains"`, `"feeds"`, `"isPointOf"`, `"monitors"`) |
| `properties` | object | no | Key-value properties for the edge |

**modify_edge parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | integer | yes | Edge ID to modify |
| `set_properties` | object | no | Properties to set or update |
| `remove_properties` | string[] | no | Property keys to remove |

**list_edges parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `label` | string | no | Filter by label (e.g., `"contains"`). Omit to list all. |
| `limit` | integer | no | Maximum edges to return (default: 100) |
| `offset` | integer | no | Number of edges to skip for pagination |

### Time-Series (2 tools)

| Tool | Description |
|------|-------------|
| `ts_write` | Write time-series samples. The referenced entity must exist in the graph. |
| `ts_query` | Query time-series samples for a specific node and property. Returns timestamp/value pairs. |

**ts_write parameters:**

The `samples` array contains one or more sample objects:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `samples` | array | yes | Array of sample objects |

Each sample object:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `entity_id` | integer | yes | Node ID the sample belongs to |
| `property` | string | yes | Property name (e.g., `"temperature"`, `"humidity"`) |
| `timestamp_nanos` | integer | yes | Timestamp in nanoseconds since Unix epoch |
| `value` | float | yes | Numeric value |

**ts_query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `entity_id` | integer | yes | Node ID to query |
| `property` | string | yes | Property name to query |
| `start` | integer | no | Start timestamp in nanoseconds. Omit for earliest. |
| `end` | integer | no | End timestamp in nanoseconds. Omit for latest. |

### Graph (1 tool)

| Tool | Description |
|------|-------------|
| `graph_slice` | Get a snapshot of the graph with filtering and pagination. |

**graph_slice parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `slice_type` | string | no | `"full"` (default), `"labels"`, or `"containment"` |
| `labels` | string[] | no | For `"labels"` slice: which labels to include |
| `root_id` | integer | no | For `"containment"` slice: root node ID |
| `max_depth` | integer | no | For `"containment"` slice: maximum traversal depth |
| `limit` | integer | no | Maximum nodes to return |
| `offset` | integer | no | Nodes to skip for pagination |

For result sets with 200 or fewer nodes, the full node and edge data is included.
Larger results return a summary with counts and a note to use pagination.

### Health (1 tool)

| Tool | Description |
|------|-------------|
| `health` | Check server health. Returns uptime, node/edge counts, and status. No parameters. |

### React Flow (2 tools)

These tools provide interoperability with the [React Flow](https://reactflow.dev)
graph visualization library. The export format uses the standard React Flow JSON
structure with `nodes` and `edges` arrays.

| Tool | Description |
|------|-------------|
| `export_reactflow` | Export the graph in React Flow format. Optionally filter by label. |
| `import_reactflow` | Import a React Flow graph. Node `type` maps to Selene label, `data` maps to properties. Returns an ID mapping from React Flow IDs to Selene IDs. |

**export_reactflow parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `label` | string | no | Filter export to nodes with this label |

**import_reactflow parameters:**

The input is a React Flow graph object with `nodes` and `edges` arrays following the
standard React Flow JSON schema.

### Schema (6 tools)

| Tool | Description |
|------|-------------|
| `list_schemas` | List all registered node and edge schemas with label, description, property count, and parent. |
| `get_schema` | Get the full definition of a schema by label. Tries node schemas first, then edge schemas. |
| `create_schema` | Create a new node type schema using field shorthand syntax. |
| `delete_schema` | Delete a node schema by label. |
| `export_schemas` | Export all registered schemas as compact JSON for backup or migration. |
| `import_schema_pack` | Import a schema pack from compact JSON or TOML. Auto-detects format. |

**get_schema / delete_schema parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `label` | string | yes | Schema label to look up (e.g., `"temperature_sensor"`) |

**create_schema parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `label` | string | yes | Type label (e.g., `"smart_thermostat"`) |
| `extends` | string | no | Parent type to inherit from (e.g., `"equipment"`) |
| `description` | string | no | Human-readable description |
| `fields` | object | no | Field definitions using shorthand syntax (see below) |
| `edges` | string[] | no | Valid edge labels for this type |
| `annotations` | object | no | Application-defined annotations |

Field shorthand syntax for the `fields` object:

| Shorthand | Meaning |
|-----------|---------|
| `"string"` | Optional string |
| `"string!"` | Required string |
| `"float = 72.5"` | Optional float with default 72.5 |
| `"string = '°F'"` | Optional string with default "°F" |
| `"bool = true"` | Optional bool with default true |
| `"int = 60"` | Optional int with default 60 |

**import_schema_pack parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `toml` | string | yes | TOML or JSON content of the schema pack to import. Format is auto-detected. |

### Vector Search (2 tools)

These tools require the `vector` feature and a loaded embedding model. Without the
model, calls return an error.

| Tool | Description |
|------|-------------|
| `semantic_search` | Search the graph using natural language. Embeds the query text, finds similar nodes, and returns them with their containment path. |
| `similar_nodes` | Find nodes most similar to a given node based on vector embeddings, ranked by cosine similarity. |

**semantic_search parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query_text` | string | yes | Natural language query (e.g., `"supply air temperature sensor"`) |
| `k` | integer | yes | Maximum number of results to return |
| `label` | string | no | Filter to nodes with this label |

**similar_nodes parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `node_id` | integer | yes | Reference node ID to find similar nodes for |
| `property` | string | yes | Vector property name to compare (e.g., `"embedding"`) |
| `k` | integer | yes | Maximum number of results to return |

## Client Configuration

### Claude Desktop

Add the following to your Claude Desktop MCP configuration file
(`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS,
`%APPDATA%\Claude\claude_desktop_config.json` on Windows):

```json
{
  "mcpServers": {
    "selene": {
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

The server must be running with `dev_mode = true` and `mcp.enabled = true` before
Claude Desktop connects.

### Other MCP Clients

Any client that supports Streamable HTTP transport can connect by pointing to the
`/mcp` endpoint. The server advertises its capabilities through the standard MCP
`initialize` handshake, including tool names, descriptions, JSON Schema parameter
definitions, resource URIs, and prompt templates.

### New in v1: update_schema, export_rdf, sparql_query

| Tool | Description |
|------|-------------|
| `update_schema` | Update an existing node schema. Fields are replaced entirely (not merged). |
| `export_rdf` | Export the graph as RDF (Turtle). Routes through GQL `CALL graph.exportRdf()`. Requires `rdf` feature. |
| `sparql_query` | Execute a SPARQL query. Routes through GQL `CALL graph.sparql()`. Requires `rdf-sparql` feature. |
