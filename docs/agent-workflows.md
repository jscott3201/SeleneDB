# Agent Workflows

SeleneDB is purpose-built as the persistence layer for AI agents. This guide covers end-to-end workflow patterns — from connecting your first agent to building production pipelines that compound knowledge across sessions.

Every pattern here uses SeleneDB's MCP server (64 tools, one endpoint). Whether your agent runs in Claude Desktop, Claude Code, Cursor, Copilot, or a custom orchestrator, the workflow is the same.

---

## Core Concepts

### The Agent Persistence Problem

AI agents are stateless by default. Every session starts from zero — re-reading files, re-establishing context, re-learning decisions. This burns tokens, wastes time, and produces inconsistent results.

SeleneDB solves this by giving agents a living knowledge graph that persists across sessions:

- **Relationships** between entities (graph) — "which modules depend on this crate?"
- **Similarity** between concepts (vector search) — "find nodes related to authentication"
- **Temporal patterns** in data (time-series) — "what was the temperature trend this week?"
- **Memory** across sessions (agent memory) — "what did we decide about the API design?"
- **Community structure** (GraphRAG) — "summarize what this cluster of nodes is about"

All accessible through a single MCP connection. No multi-database orchestration.

### Progressive Disclosure

SeleneDB's MCP tools are designed around **progressive disclosure** — agents start with compact overviews and drill down only when needed. This minimizes context window consumption:

| Pattern | Instead of | Use | Token savings |
|---------|-----------|-----|---------------|
| Schema exploration | Full schema dump (20 KB) | `schema_dump` compact mode (2.5 KB) | ~85% |
| Node lookup | Writing GQL to find a node | `resolve` with natural language | ~70% |
| Node + connections | `get_node` + `node_edges` (2 calls) | `related` (1 call) | ~50% |
| Multi-signal search | Vector + graph + community (3 calls) | `graphrag_search` (1 call) | ~65% |

These savings compound. In production use across 30+ sessions, we've measured:
- **80-85% token reduction** on context re-establishment
- **95% savings** on decision lookups
- **85-90% savings** on session continuity

---

## Connecting Your Agent

### MCP Configuration

Add SeleneDB to your MCP client configuration:

```json
{
  "mcpServers": {
    "selenedb": {
      "type": "streamableHttp",
      "url": "http://localhost:8080/mcp"
    }
  }
}
```

For production with OAuth 2.1:

```json
{
  "mcpServers": {
    "selenedb": {
      "type": "streamableHttp",
      "url": "https://your-host:8080/mcp",
      "headers": {
        "Authorization": "Bearer <token>"
      }
    }
  }
}
```

For API key authentication:

```json
{
  "mcpServers": {
    "selenedb": {
      "type": "streamableHttp",
      "url": "https://your-host:8080/mcp",
      "headers": {
        "X-API-Key": "<your-api-key>"
      }
    }
  }
}
```

See the [MCP Integration Guide](guides/mcp.md) for full authentication details including OAuth 2.1 PKCE flows.

### First Connection Test

Once connected, verify with:

```
health → {"status": "ok", "node_count": 0, "edge_count": 0, ...}
```

Then explore the schema:

```
schema_dump → compact summary of all node/edge types
```

### MCP Resources

SeleneDB exposes read-only resources that agents can inspect without a tool call round-trip:

| URI | Description |
|-----|-------------|
| `selene://health` | Server health, node/edge counts, embedding status |
| `selene://stats` | Per-label breakdowns of node and edge counts |
| `selene://schemas` | All registered schemas |
| `selene://schemas/{label}` | Individual schema by label |
| `selene://info` | Version, runtime profile, feature flags |
| `selene://gql-examples` | Curated GQL query examples |

### MCP Prompts

Guided workflow templates for common agent tasks:

- **`explore-graph`** — Structured exploration plan for unfamiliar graphs
- **`query-helper`** — Natural language → GQL with schema context
- **`text2gql`** — LLM-optimized GQL generation using `graph.schemaDump()`
- **`import-guide`** — Format-specific guidance for CSV, JSON, and TOML imports

---

## Session Lifecycle

### Session Start Pattern

Every productive agent session starts the same way — orient, then act:

**1. Check graph health**

```
health → node_count, edge_count, uptime, embedding status
```

**2. Load schema context**

```
schema_dump(compact: true) → 2.5 KB type summary
```

This gives the agent a mental model of what exists. At 2.5 KB instead of 20 KB, it fits comfortably in any context window.

**3. Recall relevant memories**

```
recall(namespace: "project-x", query: "current sprint goals")
→ ranked memories by semantic similarity
```

**4. Query active work**

```gql
MATCH (w:work_item) WHERE w.status = 'open' RETURN w.title, w.priority
```

**5. Check recent changes**

```gql
MATCH (s:session_summary) RETURN s.summary ORDER BY s.created_at DESC LIMIT 3
```

This five-step pattern costs ~500-800 tokens total — replacing the 10-20K tokens agents typically spend re-reading files and re-establishing context.

### Session End Pattern

Before ending a session, persist what was learned:

**1. Store a session summary**

```
remember(namespace: "project-x", content: "Implemented JWT auth module. Decided on bcrypt for password hashing. Deferred rate limiting to next sprint.", entities: ["jwt-auth", "bcrypt", "rate-limiting"])
```

**2. Update work item status**

```gql
MATCH (w:work_item {title: "JWT auth module"}) SET w.status = 'done'
```

**3. Record decisions**

```
remember(namespace: "project-x", content: "Chose bcrypt over argon2 because bcrypt is better supported in our target deployment environment (Alpine containers).", memory_type: "decision")
```

---

## Agent Memory

SeleneDB provides built-in agent memory with vector embeddings, automatic eviction, and namespace isolation. This is not a simple key-value store — it's semantic memory that agents can query with natural language.

### Namespaces

Memories are isolated by namespace. Use namespaces to separate concerns:

- `"project-alpha"` — project-specific knowledge
- `"conventions"` — coding standards and preferences
- `"architecture"` — design decisions and rationale
- `"agent-smith"` — per-agent memory in multi-agent systems

### Remember

Store a memory with optional entity links and expiry:

```
remember(
  namespace: "project-x",
  content: "Use PostgreSQL for the user service. MySQL for the legacy integration.",
  memory_type: "decision",
  entities: ["postgresql", "mysql", "user-service"]
)
```

Each memory is automatically embedded as a vector for semantic search. Entity names create `__Entity` nodes and `__MENTIONS` edges, building a knowledge graph of what the agent knows.

Memory types help with organization:
- `"fact"` (default) — objective information
- `"preference"` — user or project preferences
- `"event"` — something that happened
- `"decision"` — a choice with rationale

### Recall

Query memories by semantic similarity:

```
recall(
  namespace: "project-x",
  query: "which database should I use for the user service?"
)
→ Returns memories ranked by relevance, with scores
```

Frequently recalled memories are retained longer during eviction — the system learns what matters to the agent.

### Forget

Remove specific memories when they become stale:

```
forget(namespace: "project-x", query: "postgresql user service")
```

Or by specific node ID:

```
forget(namespace: "project-x", node_id: 42)
```

### Memory Configuration

Control capacity, eviction, and expiry per namespace:

```
configure_memory(
  namespace: "project-x",
  max_memories: 500,
  eviction_policy: "lowest_confidence",
  default_ttl_ms: 604800000  // 7 days
)
```

Eviction policies:
- `"clock"` (default) — CLOCK algorithm, balances recency and frequency
- `"oldest"` — evict least recently created
- `"lowest_confidence"` — evict least frequently recalled

---

## Graph Knowledge Patterns

### Entity-Relationship Modeling

Use the graph to model domain knowledge. Agents build this incrementally:

```gql
-- Create entities
INSERT (:module {name: "auth", language: "rust", status: "active"})
INSERT (:module {name: "api", language: "rust", status: "active"})

-- Create relationships
MATCH (a:module {name: "auth"}) MATCH (b:module {name: "api"})
INSERT (b)-[:depends_on]->(a)
```

Query relationships naturally:

```gql
-- What depends on auth?
MATCH (m:module)-[:depends_on]->(:module {name: "auth"})
RETURN m.name

-- Full dependency chain
MATCH (a:module)-[:depends_on*1..3]->(b:module)
RETURN a.name AS consumer, b.name AS dependency
```

### Convention Storage

Store coding conventions, preferences, and best practices as graph nodes:

```gql
INSERT (:convention {
  name: "Error handling",
  scope: "rust",
  severity: "critical",
  description: "Use thiserror for library crates, anyhow for binary crates",
  rationale: "Consistent error handling across the workspace"
})
```

Query conventions contextually:

```gql
MATCH (c:convention)
WHERE c.scope = 'rust' AND c.severity = 'critical'
RETURN c.name, c.description
```

### Decision Records

Persist architectural decisions with rationale — so agents never re-debate settled questions:

```gql
INSERT (:decision {
  name: "Authentication strategy",
  summary: "JWT with refresh tokens",
  rationale: "Stateless auth fits our microservice architecture. Refresh tokens mitigate short JWT lifetimes.",
  status: "accepted",
  created_at: datetime()
})
```

---

## Vector Search & GraphRAG

### Semantic Search

Find nodes by meaning, not just exact matches:

```
semantic_search(
  query_text: "temperature monitoring in building systems",
  k: 10,
  label: "sensor"
)
```

Returns nodes ranked by cosine similarity with their containment path (e.g., building > floor > zone > sensor).

### GraphRAG: Unified Retrieval

GraphRAG combines three retrieval signals in a single call:

```
graphrag_search(
  query: "How does the authentication flow work?",
  mode: "local",
  k: 10,
  max_hops: 2
)
```

**Modes:**

| Mode | Signals | Best for |
|------|---------|----------|
| `local` (default) | Vector similarity + BFS graph traversal + community context | Most queries — finds relevant nodes and their neighborhoods |
| `global` | Community summary embeddings only | Broad "summarize everything about X" questions |
| `hybrid` | Both local and global, merged | Maximum recall when you need comprehensive coverage |

Each result includes:
- **Score** — relevance ranking
- **Provenance** — which signal found it (vector, traversal, community)
- **Context snippets** — key properties for immediate use
- **Traversal depth** — how many hops from the initial match

### Building Community Structure

Before using `global` or `hybrid` mode, build and enrich communities:

```
build_communities(min_community_size: 2)
→ Runs Louvain community detection, creates __CommunitySummary nodes

enrich_communities()
→ Adds vector embeddings to community summaries for search
```

Communities group related nodes automatically — modules that depend on each other, sensors in the same zone, decisions about the same topic. Agents can then ask broad questions and get structured answers.

### Find Similar Nodes

Given a reference node, find semantically similar ones:

```
similar_nodes(node_id: 42, property: "embedding", k: 5)
```

Useful for finding related entities, detecting duplicates, or suggesting connections.

---

## Batch Operations

When agents need to create many entities at once — importing findings, bootstrapping a knowledge base, or ingesting external data — batch operations minimize round-trips.

### Batch Node Creation

```
batch_create_nodes(nodes: [
  { labels: ["finding"], properties: { title: "SQL injection in login", severity: "critical" } },
  { labels: ["finding"], properties: { title: "Missing rate limiting", severity: "high" } },
  { labels: ["finding"], properties: { title: "Verbose error messages", severity: "medium" } }
])
→ Returns array of created node IDs
```

### Batch Edge Creation

```
batch_create_edges(edges: [
  { source: 1, target: 10, label: "affects" },
  { source: 2, target: 10, label: "affects" },
  { source: 3, target: 11, label: "affects" }
], upsert: true)
```

The `upsert` flag deduplicates on (source, target, label) — safe to call repeatedly.

### Batch Ingest (Nodes + Edges)

Create nodes and connect them to existing nodes in one call:

```
batch_ingest(entries: [
  {
    labels: ["security_concern"],
    properties: { summary: "Unvalidated input in API gateway", severity: "high" },
    connect_to: [{ node_id: 42, label: "affects" }],
    connect_from: [{ node_id: 1, label: "discovered_by" }]
  }
])
```

### CSV Import/Export

For bulk data exchange:

```
csv_import(
  content: "name,severity,status\nAuth bypass,critical,open\nXSS in search,high,open",
  label: "vulnerability"
)

csv_export(label: "vulnerability")
→ Returns CSV with id column + all properties
```

---

## Proposal Workflow

For multi-agent systems or human-in-the-loop workflows, SeleneDB provides a proposal mechanism. Agents propose actions; humans (or other agents) approve or reject them.

### Propose an Action

```
propose_action(
  description: "Increase temperature setpoint to 74°F for Zone A",
  query: "MATCH (s:setpoint {zone: 'A'}) SET s.value = 74",
  category: "setpoint_change",
  priority: "high"
)
→ Creates a __Proposal node with "pending" status
```

### Review and Execute

```
list_proposals(status: "pending")
→ Shows all pending proposals with descriptions and queries

approve_proposal(proposal_id: 123, reason: "Approved per comfort request")
execute_proposal(proposal_id: 123)
→ Runs the stored GQL query

reject_proposal(proposal_id: 456, reason: "Outside operating hours")
```

Proposals auto-expire after 24 hours. This prevents stale actions from being executed accidentally.

---

## Resolution Workflows

### Tracking Findings

When agents discover issues (security concerns, code smells, bugs), track them in the graph and resolve them systematically:

```gql
INSERT (:finding {
  title: "Hardcoded credentials in config",
  severity: "critical",
  status: "open",
  file: "src/config.rs",
  line: 42
})
```

### Bulk Resolution

After fixing issues, mark them resolved in one call:

```
mark_fixed(
  node_ids: [10, 11, 12],
  commit_sha: "abc123",
  note: "Moved all credentials to environment variables"
)
```

This updates the status to "fixed" and records the commit for traceability.

---

## Interaction Tracing

SeleneDB can capture agent interaction traces for training data collection and performance analysis.

### Logging Traces

```
log_trace(
  session_id: "session-001",
  turn: 1,
  tool_name: "gql_query",
  tool_params: "{\"query\": \"MATCH (n) RETURN count(n)\"}",
  tool_result_summary: "42 nodes found",
  model_id: "claude-sonnet-4",
  latency_ms: 150,
  feedback: "approved"
)
```

### Exporting for Fine-Tuning

Export traces in JSONL format compatible with TRL, Axolotl, and Unsloth pipelines:

```
export_traces(
  session_id: "session-001",
  format: "jsonl",
  feedback: "approved"
)
```

Filter by model, tool, time range, or feedback type to curate training datasets.

---

## Multi-Agent Patterns

### Namespace Isolation

In multi-agent systems, each agent gets its own memory namespace:

```
-- Agent A (code reviewer)
remember(namespace: "reviewer", content: "Found 3 critical issues in auth module")

-- Agent B (test writer)
remember(namespace: "test-writer", content: "Generated 15 test cases for auth module")

-- Orchestrator reads both
recall(namespace: "reviewer", query: "auth module issues")
recall(namespace: "test-writer", query: "auth module tests")
```

### Shared Knowledge Graph

While memory is namespaced, the graph is shared. Agents collaborate by reading and writing to common entity types:

```gql
-- Code reviewer creates findings
INSERT (:finding {title: "Missing null check", agent: "reviewer", status: "open"})

-- Test writer queries findings to generate tests
MATCH (f:finding) WHERE f.status = 'open' AND f.agent = 'reviewer'
RETURN f.title, f.severity
```

### Principal-Based Access Control

In production, each agent gets its own principal with role-based permissions:

```
create_principal(identity: "code-reviewer", role: "operator")
create_principal(identity: "test-writer", role: "operator")
create_principal(identity: "dashboard", role: "reader")
```

Roles control what each agent can do:
- **reader** — query and search only
- **operator** — read + write (create/modify nodes and edges)
- **admin** — full access including principal management
- **service** — inter-service communication
- **device** — IoT device identity

---

## Edge & IoT Integration

SeleneDB runs on hardware as small as a Raspberry Pi 5. For IoT deployments, agents interact with real-time telemetry alongside the knowledge graph.

### Time-Series Ingestion

Write sensor readings as time-series samples:

```
ts_write(samples: [
  { entity_id: 42, property: "temperature", timestamp_nanos: 1712700000000000000, value: 72.5 },
  { entity_id: 42, property: "humidity", timestamp_nanos: 1712700000000000000, value: 45.2 }
])
```

### Time-Series Queries

Query with automatic aggregation:

```
ts_query(
  entity_id: 42,
  property: "temperature",
  aggregation: "1h",
  function: "avg"
)
→ Hourly average temperatures
```

Aggregation buckets: `5m`, `15m`, `1h`, `1d`, `auto` (picks based on time range).

### Agent-Driven Monitoring

Combine graph relationships with time-series for intelligent monitoring:

```gql
-- Find all sensors in a zone
MATCH (z:zone {name: "Zone A"})-[:contains]->(s:sensor)
RETURN s.name, id(s) AS sensor_id
```

Then query each sensor's telemetry:

```
ts_query(entity_id: <sensor_id>, property: "temperature", aggregation: "auto")
```

Agents can correlate sensor readings with equipment relationships, maintenance history, and operational decisions — all from the same database.

---

## Schema-Driven Workflows

### Schema Packs

Define schemas to enforce data quality as the graph grows:

```
create_schema(
  label: "sensor",
  fields: {
    "name": "string!",
    "unit": "string = '°F'",
    "threshold": "float",
    "active": "bool = true"
  },
  description: "Physical sensor with telemetry"
)
```

Fields use shorthand: `string!` (required), `float = 72.5` (with default), `bool` (optional).

### Edge Schemas

Constrain which node types can connect:

```
create_edge_schema(
  label: "monitors",
  source_labels: ["agent"],
  target_labels: ["sensor", "equipment"],
  fields: { "since": "timestamp" }
)
```

### Schema Inspection

Agents inspect schemas before writing data:

```
schema_dump(compact: true)
→ Type names with property counts and edge connectivity

get_schema(label: "sensor")
→ Full field definitions, types, required flags, defaults
```

---

## Real-World Workflow: Development Assistant

Here's a complete workflow for an AI development assistant using SeleneDB across sessions:

### Session 1: Bootstrap

```
# Orient
health()
schema_dump(compact: true)

# Create project structure
batch_create_nodes(nodes: [
  { labels: ["project"], properties: { name: "MyApp", language: "typescript" } },
  { labels: ["module"], properties: { name: "auth", status: "planned" } },
  { labels: ["module"], properties: { name: "api", status: "planned" } },
  { labels: ["module"], properties: { name: "database", status: "planned" } }
])

# Connect modules to project
batch_create_edges(edges: [
  { source: 2, target: 1, label: "belongs_to" },
  { source: 3, target: 1, label: "belongs_to" },
  { source: 4, target: 1, label: "belongs_to" }
])

# Store initial decisions
remember(namespace: "myapp", content: "Using TypeScript with Express. PostgreSQL for persistence. JWT for auth.")

# End session
remember(namespace: "myapp", content: "Session 1: Created project structure with 3 modules (auth, api, database). All planned, none implemented yet.")
```

### Session 2: Implementation

```
# Orient
recall(namespace: "myapp", query: "what was decided in the last session?")
gql_query("MATCH (m:module) WHERE m.status = 'planned' RETURN m.name")

# Work on auth module
gql_query("MATCH (m:module {name: 'auth'}) SET m.status = 'in_progress'")

# Record implementation decisions as they're made
remember(namespace: "myapp", content: "Auth module uses bcrypt for password hashing, 12 rounds. JWT tokens expire in 15 minutes with 7-day refresh tokens.")

# Track findings
batch_ingest(entries: [
  { labels: ["finding"], properties: { title: "Need rate limiting on login endpoint", severity: "high", status: "open" } }
])

# End session
gql_query("MATCH (m:module {name: 'auth'}) SET m.status = 'done'")
remember(namespace: "myapp", content: "Session 2: Implemented auth module. Uses bcrypt + JWT. Deferred: rate limiting on login endpoint.")
```

### Session 3: Continue seamlessly

```
# Orient — costs ~500 tokens instead of re-reading everything
recall(namespace: "myapp", query: "project status and open items")
gql_query("MATCH (f:finding) WHERE f.status = 'open' RETURN f.title, f.severity")

# Agent immediately knows:
# - Auth is done, API and database are planned
# - Rate limiting is deferred
# - bcrypt with 12 rounds, JWT with 15-min expiry
# No re-reading files. No re-debating decisions.
```

---

## Performance Considerations

### Token Budget

For context-constrained agents, prioritize tools by information density:

| Tool | Typical tokens | Information density |
|------|---------------|-------------------|
| `schema_dump` (compact) | ~600 | Full type landscape |
| `recall` (k=5) | ~300-500 | Relevant memories |
| `resolve` | ~100-200 | Single node details |
| `related` | ~300-500 | Node + all connections |
| `graphrag_search` | ~500-1000 | Multi-signal results |
| `gql_query` | varies | Precise data extraction |

A typical session start (health + schema + recall + work items) costs 800-1500 tokens — well under 2% of most context windows.

### Embedding Performance

- **CPU**: ~50ms per embedding (EmbeddingGemma 300M, 768 dimensions)
- **GPU (CUDA)**: ~5ms per embedding with Tesla T4 or better
- **GGUF quantized**: ~30ms per embedding on CPU with 8-bit quantization

Auto-embed runs as a background task — newly created nodes are embedded automatically based on configured rules. The task uses a changelog subscription with 200ms debounce and content-hash deduplication to avoid redundant work. `__Memory` nodes always have their `content` property auto-embedded. Custom rules can be added via TOML configuration to embed any label + text property combination.

### Query Optimization

The GQL engine includes a 13-rule optimizer with plan caching. Common patterns:

- **Filter pushdown**: Filters are pushed as close to the scan as possible
- **Label-based indexing**: RoaringBitmap indexes for fast label lookups
- **Factorized joins**: WCO (worst-case optimal) joins for complex patterns
- **Plan cache**: Repeated queries skip parsing and planning

---

## Next Steps

- [MCP Integration Guide](guides/mcp.md) — Full tool reference with all 64 tools
- [Vector Search & GraphRAG](guides/vector-search.md) — Deep dive into embeddings and retrieval
- [GQL Reference](guides/gql/) — ISO GQL query language
- [Time-Series](guides/time-series.md) — Telemetry ingestion and aggregation
- [Deployment](operations/deployment.md) — Production deployment patterns
- [Industry Applications](industry-applications.md) — Vertical use cases
