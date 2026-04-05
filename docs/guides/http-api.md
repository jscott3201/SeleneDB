# HTTP API Reference

Selene exposes a JSON-over-HTTP API for graph operations, time-series data, GQL queries, schema management, and more. All transports (HTTP, QUIC, MCP) share the same ops layer, so behavior is identical regardless of how you connect.

The HTTP server listens on port **8080** by default (configurable via `[http] listen_addr`). In dev mode, the server starts without TLS and with permissive defaults. In production, Selene requires either TLS termination through a reverse proxy (`[http] allow_plaintext = true`) or disabling HTTP entirely in favor of QUIC.

## Authentication

All endpoints except `GET /` and `GET /health` require authentication. Credentials are sent as a Bearer token in the `Authorization` header:

```
Authorization: Bearer <identity>:<secret>
```

The token is split on the first colon -- `identity` identifies the principal, and `secret` is verified against the stored credential (argon2-hashed).

**Dev mode:** When `dev_mode = true`, requests without an `Authorization` header fall back to a built-in admin context. This lets you use curl without credentials during development.

**Rate limiting:** Failed authentication attempts are tracked per identity. After 5 consecutive failures, exponential backoff kicks in (2^n seconds, capped at 300 seconds). Records expire after 10 minutes of inactivity. Successful authentication clears the failure count.

## Middleware

Every request passes through a standard middleware stack applied in the following order:

| Layer | Default | Notes |
|---|---|---|
| Concurrency limit | 128 | Maximum concurrent requests across all routes |
| CORS | Permissive in dev mode | Production: configure via `[http] cors_origins` |
| Tracing | Enabled | HTTP request/response logging via `tracing` |
| Timeout | 60 seconds | Returns `408 Request Timeout` if exceeded |
| Body limit | 16 MB | Global default; some routes override this (see per-route notes) |

## Endpoints

### Root and Health

#### GET /

Returns an API index with all available endpoints and usage hints. No authentication required.

```bash
curl http://localhost:8080/
```

**Response (200):**

```json
{
  "name": "Selene",
  "description": "Lightweight in-memory property graph runtime — domain-agnostic",
  "endpoints": { "..." },
  "notes": { "..." }
}
```

---

#### GET /health

Returns server health and operational status. No authentication required, but the response varies based on whether a valid token is provided.

**Unauthenticated response (200):** Minimal -- suitable for load balancer probes.

```json
{
  "status": "ok",
  "uptime_secs": 3600
}
```

**Authenticated response (200):** Full operational details.

```json
{
  "status": "ok",
  "node_count": 4200,
  "edge_count": 8100,
  "uptime_secs": 3600,
  "dev_mode": true,
  "role": "primary",
  "primary": null,
  "lag_sequences": null
}
```

Replicas include `"role": "replica"`, `"primary": "10.0.1.5:4510"`, and `"lag_sequences": 12`.

```bash
# Minimal (no auth)
curl http://localhost:8080/health

# Full (with auth)
curl -H "Authorization: Bearer admin:secret" http://localhost:8080/health
```

---

### Node CRUD

All node endpoints require authentication. Mutation endpoints (`POST`, `PUT`, `DELETE`) return `405 Method Not Allowed` on read-only replicas.

#### GET /nodes

List nodes with optional filtering and pagination.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `label` | string | -- | Filter by label |
| `limit` | integer | 1000 | Max results (capped at 10,000) |
| `offset` | integer | 0 | Skip first N results |

```bash
curl -H "Authorization: Bearer admin:secret" \
  "http://localhost:8080/nodes?label=sensor&limit=50"
```

**Response (200):**

```json
{
  "nodes": [
    {
      "id": 1,
      "labels": ["sensor"],
      "properties": {
        "name": "temp-sensor-01",
        "unit": "celsius",
        "floor": 3
      },
      "created_at": 1711670400000000000,
      "updated_at": 1711670400000000000,
      "version": 1
    }
  ],
  "total": 142
}
```

---

#### POST /nodes

Create a new node. Optionally set `parent_id` to auto-create a `contains` edge from the parent.

**Request body:**

```json
{
  "labels": ["sensor"],
  "properties": {
    "name": "temp-sensor-02",
    "unit": "celsius",
    "floor": 5
  },
  "parent_id": 10
}
```

```bash
curl -X POST http://localhost:8080/nodes \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"labels":["sensor"],"properties":{"name":"temp-sensor-02","unit":"celsius","floor":5}}'
```

**Response (201):** The created node (same shape as GET).

---

#### GET /nodes/{id}

Retrieve a single node by ID.

```bash
curl -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/nodes/1
```

**Response (200):**

```json
{
  "id": 1,
  "labels": ["sensor"],
  "properties": {
    "name": "temp-sensor-01",
    "unit": "celsius",
    "floor": 3
  },
  "created_at": 1711670400000000000,
  "updated_at": 1711670400000000000,
  "version": 1
}
```

**Response (404):** `{"error": "node 999 not found"}`

---

#### PUT /nodes/{id}

Modify an existing node. All fields are optional -- include only what you want to change.

**Request body:**

```json
{
  "set_properties": {"floor": 6, "zone": "north"},
  "remove_properties": ["old_field"],
  "add_labels": ["active"],
  "remove_labels": ["offline"]
}
```

```bash
curl -X PUT http://localhost:8080/nodes/1 \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"set_properties":{"floor":6},"add_labels":["active"]}'
```

**Response (200):** The updated node.

---

#### DELETE /nodes/{id}

Delete a node and all edges connected to it.

```bash
curl -X DELETE -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/nodes/1
```

**Response:** `204 No Content`

---

#### GET /nodes/{id}/edges

List all edges connected to a node (both incoming and outgoing).

```bash
curl -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/nodes/1/edges
```

**Response (200):**

```json
{
  "node_id": 1,
  "edges": [
    {
      "id": 100,
      "source": 1,
      "target": 10,
      "label": "feeds",
      "properties": {},
      "created_at": 1711670400000000000
    }
  ],
  "total": 3
}
```

---

### Edge CRUD

All edge endpoints require authentication. Mutation endpoints return `405` on replicas.

#### GET /edges

List edges with optional filtering and pagination. Uses the same query parameters as `GET /nodes`.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `label` | string | -- | Filter by edge label |
| `limit` | integer | 1000 | Max results (capped at 10,000) |
| `offset` | integer | 0 | Skip first N results |

```bash
curl -H "Authorization: Bearer admin:secret" \
  "http://localhost:8080/edges?label=feeds&limit=100"
```

**Response (200):**

```json
{
  "edges": [
    {
      "id": 100,
      "source": 1,
      "target": 10,
      "label": "feeds",
      "properties": {"protocol": "modbus"},
      "created_at": 1711670400000000000
    }
  ],
  "total": 87
}
```

---

#### POST /edges

Create a new edge between two existing nodes.

**Request body:**

```json
{
  "source": 1,
  "target": 10,
  "label": "feeds",
  "properties": {"protocol": "modbus"}
}
```

```bash
curl -X POST http://localhost:8080/edges \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"source":1,"target":10,"label":"feeds","properties":{"protocol":"modbus"}}'
```

**Response (201):** The created edge.

---

#### GET /edges/{id}

Retrieve a single edge by ID.

```bash
curl -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/edges/100
```

**Response (200):** Edge object (same shape as list results).

---

#### PUT /edges/{id}

Modify edge properties.

**Request body:**

```json
{
  "set_properties": {"protocol": "bacnet"},
  "remove_properties": ["old_field"]
}
```

```bash
curl -X PUT http://localhost:8080/edges/100 \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"set_properties":{"protocol":"bacnet"}}'
```

**Response (200):** The updated edge.

---

#### DELETE /edges/{id}

Delete an edge.

```bash
curl -X DELETE -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/edges/100
```

**Response:** `204 No Content`

---

### Time-Series

Selene has a built-in multi-tier time-series engine. The HTTP API writes samples and queries ranges.

#### POST /ts/write

Write one or more time-series samples. Body limit: **2 MB**.

**Request body:**

```json
{
  "samples": [
    {
      "entity_id": 1,
      "property": "temperature",
      "timestamp_nanos": 1711670400000000000,
      "value": 22.5
    },
    {
      "entity_id": 1,
      "property": "temperature",
      "timestamp_nanos": 1711670401000000000,
      "value": 22.7
    }
  ]
}
```

All timestamps are **nanoseconds since Unix epoch**.

```bash
curl -X POST http://localhost:8080/ts/write \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"samples":[{"entity_id":1,"property":"temperature","timestamp_nanos":1711670400000000000,"value":22.5}]}'
```

**Response (200):**

```json
{"written": 1}
```

---

#### GET /ts/{entity_id}/{property}

Query time-series data for a specific entity and property.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `start` | integer (nanos) | 0 | Range start (inclusive) |
| `end` | integer (nanos) | max | Range end (inclusive) |
| `limit` | integer | -- | Max samples to return |

```bash
curl -H "Authorization: Bearer admin:secret" \
  "http://localhost:8080/ts/1/temperature?start=1711670400000000000&end=1711670500000000000&limit=100"
```

**Response (200):** Array of time-series samples.

---

### GQL

GQL (ISO 39075) is the primary query and mutation interface for Selene. The HTTP endpoint accepts a query string and optional parameters.

#### POST /gql

Execute a GQL query or mutation. Body limit: **1 MB**.

**Request body:**

```json
{
  "query": "MATCH (s:sensor) FILTER s.temperature > 20 RETURN s.name, s.temperature",
  "parameters": {"threshold": 20},
  "explain": false,
  "profile": false,
  "timeout_ms": 5000
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `query` | string | yes | GQL query or mutation |
| `parameters` | object | no | Named parameters for the query |
| `explain` | boolean | no | Return the execution plan without running the query |
| `profile` | boolean | no | Run the query and return the execution plan with timing |
| `timeout_ms` | integer | no | Query timeout in milliseconds |

Read-only queries bypass the mutation batcher for lower latency. Mutations and DDL statements are serialized through the batcher for write ordering.

```bash
curl -X POST http://localhost:8080/gql \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (s:sensor) FILTER s.temperature > 20 RETURN s.name AS name, s.temperature AS temp"}'
```

**Response (200):**

```json
{
  "status": "00000",
  "message": "Success",
  "row_count": 3,
  "data": [
    {"name": "temp-sensor-01", "temp": 22.5},
    {"name": "temp-sensor-02", "temp": 23.1},
    {"name": "temp-sensor-03", "temp": 21.8}
  ]
}
```

The `status` field follows the GQLSTATUS convention: `"00000"` indicates success. Errors return `"XX000"` with a descriptive `message`.

**Mutation response:** When the query modifies the graph, the response includes a `mutations` summary:

```json
{
  "status": "00000",
  "message": "Success",
  "row_count": 0,
  "data": [],
  "mutations": {
    "nodes_created": 1,
    "nodes_deleted": 0,
    "edges_created": 0,
    "edges_deleted": 0,
    "properties_set": 2,
    "properties_removed": 0
  }
}
```

**Explain response:** When `explain` or `profile` is true, the response includes a `plan` field with the execution plan.

---

### Graph Operations

#### POST /graph/slice

Retrieve a subset of the graph. Three slice types are available:

| Slice Type | Required Fields | Description |
|---|---|---|
| `full` | -- | All nodes and edges (paginated) |
| `labels` | `labels` | Nodes matching any of the given labels, plus connecting edges |
| `containment` | `root_id` | Containment tree rooted at a node, following `contains` edges |

**Request body examples:**

```json
{"slice_type": "full", "limit": 100, "offset": 0}
```

```json
{"slice_type": "labels", "labels": ["sensor", "equipment"]}
```

```json
{"slice_type": "containment", "root_id": 1, "max_depth": 5}
```

```bash
curl -X POST http://localhost:8080/graph/slice \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"slice_type":"labels","labels":["sensor"]}'
```

**Response (200):**

```json
{
  "nodes": [{"id": 1, "labels": ["sensor"], "properties": {}, "...": "..."}],
  "edges": [{"id": 100, "source": 1, "target": 10, "label": "feeds", "...": "..."}],
  "total_nodes": 142,
  "total_edges": 87
}
```

---

#### GET /graph/stats

Returns aggregate statistics about the graph: node and edge counts broken down by label.

```bash
curl -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/graph/stats
```

**Response (200):**

```json
{
  "node_count": 4200,
  "edge_count": 8100,
  "node_labels": {
    "sensor": 142,
    "equipment": 85,
    "building": 3,
    "floor": 12
  },
  "edge_labels": {
    "feeds": 87,
    "contains": 240,
    "monitors": 56
  }
}
```

---

#### GET /graph/reactflow

Export the graph (or a filtered subset) as React Flow format for visualization.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `label` | string | -- | Filter to nodes with this label |

```bash
curl -H "Authorization: Bearer admin:secret" \
  "http://localhost:8080/graph/reactflow?label=sensor"
```

**Response (200):**

```json
{
  "nodes": [
    {"id": "1", "data": {"label": "sensor"}, "position": {"x": 0, "y": 0}}
  ],
  "edges": [
    {"id": "e100", "source": "1", "target": "10", "label": "feeds"}
  ]
}
```

---

#### POST /graph/reactflow

Import a React Flow graph, creating nodes and edges. Returns a mapping of React Flow IDs to Selene IDs.

**Request body:**

```json
{
  "nodes": [
    {"id": "rf-1", "data": {"label": "sensor", "properties": {"name": "temp-01"}}}
  ],
  "edges": [
    {"id": "rf-e1", "source": "rf-1", "target": "rf-2", "label": "feeds"}
  ]
}
```

```bash
curl -X POST http://localhost:8080/graph/reactflow \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"nodes":[{"id":"rf-1","data":{"label":"sensor"}}],"edges":[]}'
```

**Response (201):**

```json
{
  "nodes_created": 1,
  "edges_created": 0,
  "id_map": {"rf-1": 42}
}
```

---

### Schema Management

Schemas define validation rules for node and edge properties. They are optional -- nodes and edges can exist without schemas.

#### GET /schemas

List all registered node and edge schemas.

```bash
curl -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/schemas
```

**Response (200):**

```json
{
  "node_schemas": [
    {
      "label": "sensor",
      "description": "A physical sensor device",
      "properties": [
        {"name": "unit", "value_type": "String", "required": true, "default": null, "description": "Unit of measure", "indexed": false}
      ],
      "valid_edge_labels": ["feeds", "monitors"],
      "annotations": {}
    }
  ],
  "edge_schemas": []
}
```

---

#### POST /schemas/nodes

Register or replace a node schema. If a schema with the same label already exists, it is replaced (subject to schema versioning compatibility checks).

**Request body:**

```json
{
  "label": "sensor",
  "description": "A physical sensor device",
  "properties": [
    {
      "name": "unit",
      "value_type": "String",
      "required": true,
      "default": null,
      "description": "Unit of measure",
      "indexed": false
    },
    {
      "name": "floor",
      "value_type": "Integer",
      "required": false,
      "default": null,
      "description": "Floor number",
      "indexed": true
    }
  ],
  "valid_edge_labels": ["feeds", "monitors"],
  "annotations": {}
}
```

```bash
curl -X POST http://localhost:8080/schemas/nodes \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"label":"sensor","properties":[{"name":"unit","value_type":"String","required":true}]}'
```

**Response (201):** The registered schema.

---

#### POST /schemas/edges

Register or replace an edge schema. Same structure as node schemas, but for edges.

```bash
curl -X POST http://localhost:8080/schemas/edges \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/json" \
  -d '{"label":"feeds","properties":[{"name":"protocol","value_type":"String","required":false}]}'
```

**Response (201):** The registered schema.

---

#### GET /schemas/nodes/{label}

Retrieve a specific node schema by label.

```bash
curl -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/schemas/nodes/sensor
```

**Response (200):** The schema object.

**Response (404):** `{"error": "node_schema sensor not found"}`

---

#### DELETE /schemas/nodes/{label}

Remove a node schema. Existing nodes with that label are not affected.

```bash
curl -X DELETE -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/schemas/nodes/sensor
```

**Response:** `204 No Content`

---

#### GET /schemas/edges/{label}

Retrieve a specific edge schema by label.

```bash
curl -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/schemas/edges/feeds
```

---

#### DELETE /schemas/edges/{label}

Remove an edge schema.

```bash
curl -X DELETE -H "Authorization: Bearer admin:secret" \
  http://localhost:8080/schemas/edges/feeds
```

**Response:** `204 No Content`

---

#### POST /schemas/import

Import a schema pack (compact TOML format). The body is the raw TOML text, not JSON.

```bash
curl -X POST http://localhost:8080/schemas/import \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: text/plain" \
  --data-binary @building-pack.toml
```

**Response (201):**

```json
{
  "pack": "building",
  "node_schemas_registered": 5,
  "node_schemas_skipped": 0,
  "edge_schemas_registered": 3,
  "edge_schemas_skipped": 0
}
```

---

### CSV Import/Export

#### POST /import/csv

Import nodes or edges from CSV data. Body limit: **4 MB**. The CSV data is sent as the raw request body (not JSON).

**Node import query parameters:**

| Query Param | Type | Required | Description |
|---|---|---|---|
| `type` | string | no | `"nodes"` (default) or `"edges"` |
| `label` | string | yes (nodes) | Label to apply to all imported nodes |
| `delimiter` | string | no | Column delimiter (default: `,`) |
| `parent_id_column` | string | no | Column name for parent node ID (auto-creates `contains` edges) |

**Edge import query parameters:**

| Query Param | Type | Required | Description |
|---|---|---|---|
| `type` | string | yes | Must be `"edges"` |
| `source_column` | string | no | Source node ID column (default: `source_id`) |
| `target_column` | string | no | Target node ID column (default: `target_id`) |
| `label_column` | string | no | Edge label column (default: `label`) |
| `delimiter` | string | no | Column delimiter (default: `,`) |

```bash
# Import nodes
curl -X POST "http://localhost:8080/import/csv?label=sensor" \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: text/csv" \
  --data-binary @sensors.csv

# Import edges
curl -X POST "http://localhost:8080/import/csv?type=edges" \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: text/csv" \
  --data-binary @relationships.csv
```

---

#### GET /export/csv

Export nodes as CSV.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `label` | string | -- | Filter to nodes with this label |

```bash
curl -H "Authorization: Bearer admin:secret" \
  "http://localhost:8080/export/csv?label=sensor" -o sensors.csv
```

**Response (200):** `Content-Type: text/csv; charset=utf-8`

---

### RDF (Feature-Gated)

These endpoints require the `rdf` feature flag (`--features rdf`). They provide RDF import/export using the property-graph-to-RDF mapping configured via `[rdf] namespace`.

#### GET /graph/rdf

Export the graph as RDF.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `format` | string | `turtle` | Output format: `turtle`, `ntriples`, or `nquads` |
| `graphs` | string | -- | Set to `all` to include ontology triples |

```bash
curl -H "Authorization: Bearer admin:secret" \
  "http://localhost:8080/graph/rdf?format=turtle&graphs=all"
```

**Response (200):** RDF serialization with the appropriate content type (e.g., `text/turtle`).

---

#### POST /graph/rdf

Import RDF data into the graph. Body limit: **4 MB**.

| Query Param | Type | Default | Description |
|---|---|---|---|
| `format` | string | `turtle` | Input format: `turtle`, `ntriples`, or `nquads` |
| `graph` | string | -- | Target graph name (e.g., `ontology` for TBox triples) |

```bash
curl -X POST "http://localhost:8080/graph/rdf?format=turtle&graph=ontology" \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: text/turtle" \
  --data-binary @building-ontology.ttl
```

**Response (200):**

```json
{
  "nodesCreated": 12,
  "edgesCreated": 8,
  "labelsAdded": 15,
  "propertiesSet": 24,
  "ontologyTriplesLoaded": 340
}
```

---

### SPARQL (Feature-Gated)

These endpoints require the `rdf-sparql` feature flag (`--features rdf-sparql`). SPARQL queries run against a zero-duplication adapter that routes triple patterns to the in-memory property graph.

#### GET /sparql

Execute a SPARQL query via query parameter.

| Query Param | Type | Required | Description |
|---|---|---|---|
| `query` | string | yes | SPARQL query string |
| `format` | string | no | Result format: `json` (default), `xml`, `csv`, `tsv` |

```bash
curl -H "Authorization: Bearer admin:secret" \
  --get --data-urlencode "query=SELECT ?s ?temp WHERE { ?s <urn:selene:temperature> ?temp }" \
  "http://localhost:8080/sparql?format=json"
```

**Response (200):** SPARQL Results in the requested format (e.g., `application/sparql-results+json`).

---

#### POST /sparql

Execute a SPARQL query via request body. Body limit: **1 MB**. The body is the raw SPARQL query string (not JSON).

| Query Param | Type | Required | Description |
|---|---|---|---|
| `format` | string | no | Result format: `json` (default), `xml`, `csv`, `tsv` |

```bash
curl -X POST "http://localhost:8080/sparql?format=json" \
  -H "Authorization: Bearer admin:secret" \
  -H "Content-Type: application/sparql-query" \
  -d "SELECT ?s ?name WHERE { ?s <urn:selene:name> ?name } LIMIT 10"
```

---

### WebSocket

#### GET /ws/subscribe

Upgrade to a WebSocket connection for real-time graph change notifications. Maximum 100 concurrent WebSocket subscriptions.

**Connection flow:**

1. Connect with a standard WebSocket upgrade request (authentication required).
2. Within 10 seconds, send a JSON filter message. If no filter is sent, the subscription receives all changes.
3. Receive JSON change events as they occur.

**Filter message (client to server):**

```json
{
  "labels": ["sensor", "equipment"],
  "edge_types": ["feeds"]
}
```

Both fields are optional. When set, only changes matching those labels or edge types are delivered. Changes that do not carry label information (such as property updates) pass through regardless of the label filter, because label membership cannot be determined from the change event alone.

**Change event (server to client):**

```json
{
  "sequence": 42,
  "changes": [
    {
      "type": "node_created",
      "entity_id": 15
    },
    {
      "type": "edge_created",
      "entity_id": 200,
      "label": "feeds"
    }
  ]
}
```

**Change types:**

| Type | Entity | Fields |
|---|---|---|
| `node_created` | node | `entity_id` |
| `node_deleted` | node | `entity_id`, `labels` |
| `node_updated` | node | `entity_id`, optionally `label` |
| `edge_created` | edge | `entity_id`, `label` |
| `edge_deleted` | edge | `entity_id`, `label` |
| `edge_updated` | edge | `entity_id` |

The auth scope is refreshed every 60 seconds to reflect any permission changes.

```bash
# Using websocat
websocat "ws://localhost:8080/ws/subscribe" \
  -H "Authorization: Bearer admin:secret" \
  --text '{"labels":["sensor"]}'
```

---

### Metrics

#### GET /metrics

Returns Prometheus-format metrics. If a `metrics_token` is configured in `[http]`, a separate Bearer token is required (distinct from the main auth system). Without a configured token, the endpoint is open.

```bash
# With metrics token configured
curl -H "Authorization: Bearer my-metrics-token" \
  http://localhost:8080/metrics

# Without metrics token (open access)
curl http://localhost:8080/metrics
```

**Response (200):** `Content-Type: text/plain; version=0.0.4; charset=utf-8`

```
selene_graph_nodes_total 4200
selene_graph_edges_total 8100
selene_gql_queries_total{status="success"} 15432
...
```

---

### MCP (Dev Mode Only)

#### ANY /mcp

Model Context Protocol endpoint, mounted only when `[mcp] enabled = true` and `dev_mode = true`. In production, the MCP route is not registered at all. Accepts any HTTP method, as the MCP Streamable HTTP transport manages sessions internally.

```bash
# MCP is typically used by AI tools, not curl, but for testing:
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"initialize","id":1,"params":{}}'
```

---

## Error Format

All error responses use a consistent JSON structure:

```json
{
  "error": "node 999 not found"
}
```

Some errors include an additional `hint` field:

```json
{
  "error": "POST /unknown not found",
  "hint": "GET / for API index"
}
```

**HTTP status codes by error type:**

| Status | Condition |
|---|---|
| 400 | Invalid request or query syntax error |
| 401 | Missing or invalid credentials |
| 403 | Access denied (valid credentials, insufficient permissions) |
| 404 | Entity not found |
| 405 | Mutation attempted on a read-only replica |
| 408 | Request timeout (60s exceeded) |
| 409 | Conflict (concurrent modification) |
| 422 | Schema validation failure |
| 429 | Rate-limited (too many failed auth attempts) |
| 500 | Internal server error |
| 503 | Resources exhausted or max WebSocket connections reached |

Unmatched routes return `404` with a hint directing to the API index.

## Replica Behavior

When Selene runs as a read-only replica (`--replica-of <addr>`), all mutation endpoints return `405 Method Not Allowed` with the body `{"error": "read-only replica"}`. This applies to:

- `POST /nodes`, `PUT /nodes/{id}`, `DELETE /nodes/{id}`
- `POST /edges`, `PUT /edges/{id}`, `DELETE /edges/{id}`
- `POST /ts/write`
- `POST /gql` (for mutations and DDL only -- read-only GQL queries still work)
- `POST /graph/reactflow`
- `POST /schemas/nodes`, `POST /schemas/edges`, `DELETE /schemas/nodes/{label}`, `DELETE /schemas/edges/{label}`, `POST /schemas/import`
- `POST /import/csv`
- `POST /graph/rdf`

Read endpoints (`GET`) work normally on replicas.

## Mutation Batching

All write operations are serialized through a mutation batcher. This guarantees strict ordering of concurrent writes and ensures WAL consistency. The batcher is transparent to API callers -- requests block until the write completes and return the result normally.

Read-only GQL queries bypass the batcher entirely for lower latency.

## Properties and JSON

Send property values as plain JSON:

```json
{"name": "HQ Building", "floor": 3, "active": true, "rating": 4.5}
```

Selene maps JSON types to its internal value types: strings, integers, floats, booleans, and null. Nested JSON objects are stored as JSON strings -- use flat key-value pairs for best results.

When a node schema with `dictionary: true` on a property is active, string values written to that property are automatically interned for memory efficiency. This is transparent to the API.
