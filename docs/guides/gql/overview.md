# GQL Overview

## What is GQL?

GQL (Graph Query Language) is a declarative query language for property graphs, defined by the ISO/IEC 39075:2024 standard. SeleneDB implements GQL as the sole query and mutation interface. Every transport -- HTTP, QUIC, and MCP -- routes through the same GQL engine. There is no separate SQL or Cypher path.

This design means that a query written for the HTTP API works identically over QUIC or MCP. The engine parses GQL text into an AST, plans it, optimizes it, and executes it against SeleneDB's in-memory graph. Results are returned as Arrow RecordBatches.

```
GQL text --> Parser --> AST --> Planner --> Optimizer --> Executor --> Arrow RecordBatch
```

## The property graph model

SeleneDB stores data as a property graph consisting of nodes and edges.

**Nodes** represent entities. Each node has:

- **id** -- a `u64` identifier, auto-assigned on creation
- **labels** -- a set of zero or more strings (e.g., `sensor`, `building`)
- **properties** -- a key-value map where keys are strings and values are any supported type
- **created_at** -- nanosecond timestamp, set automatically on creation
- **updated_at** -- nanosecond timestamp, updated on each mutation
- **version** -- monotonic counter incremented on each mutation

**Edges** represent relationships between nodes. Each edge has:

- **id** -- a `u64` identifier, auto-assigned on creation
- **source** -- the originating node id
- **target** -- the destination node id
- **label** -- a single string (e.g., `contains`, `monitors`)
- **properties** -- a key-value map, same as nodes
- **created_at** -- nanosecond timestamp, set automatically on creation

Both nodes and edges are first-class entities with their own identity and properties. Edges are directed (source to target), but queries can match them in either direction.

## Pattern matching basics

GQL uses ASCII-art syntax to describe graph patterns. Parentheses `()` denote nodes, square brackets `[]` denote edges, and arrows indicate direction.

Match all nodes:

```gql
MATCH (n) RETURN n.name AS name
```

Match nodes with a specific label:

```gql
MATCH (s:sensor) RETURN s.name AS name
```

Match with inline property filtering:

```gql
MATCH (s:sensor {unit: '°F'}) RETURN s.name AS name
```

Match an edge pattern:

```gql
MATCH (b:building)-[e:contains]->(f:floor) RETURN b.name AS building, f.name AS floor
```

Multi-hop traversal:

```gql
MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(s:sensor)
RETURN b.name AS building, s.name AS sensor
```

## Value types

SeleneDB supports 14 value types. Every type maps to an Arrow data type for columnar output.

| Type | Rust variant | GQL literal syntax | Description |
|------|-------------|-------------------|-------------|
| Null | `Null` | `NULL` | Absent or unknown value |
| Bool | `Bool` | `TRUE`, `FALSE` | Boolean |
| Int | `Int` | `42`, `-7` | Signed 64-bit integer |
| UInt | `UInt` | `42u` | Unsigned 64-bit integer |
| Float | `Float` | `3.14`, `1.0e3` | 64-bit floating point |
| String | `String` | `'hello'` | UTF-8 text (inline up to 22 bytes) |
| Timestamp | `Timestamp` | `TIMESTAMP '2026-03-29T10:00:00Z'` | Nanoseconds since Unix epoch |
| Date | `Date` | `DATE '2026-03-29'` | Calendar date (days since epoch) |
| LocalDateTime | `LocalDateTime` | `LOCAL DATETIME '2026-03-29T10:00:00'` | Datetime without timezone (nanoseconds) |
| Duration | `Duration` | `DURATION 'PT1H30M'` | Duration in nanoseconds |
| Bytes | `Bytes` | (binary, not literal) | Arbitrary byte sequence |
| List | `List` | `[1, 2, 3]` | Ordered collection of values |
| Vector | `Vector` | (via API, not literal) | Dense float array for embeddings |
| InternedStr | `InternedStr` | (same as String) | Interned string for dictionary-encoded properties |

String and InternedStr are interchangeable at the query level. When a property is marked with `DICTIONARY` in its schema definition, string values are automatically promoted to InternedStr on write. Both variants compare equal when their content matches.

Strings use single quotes in GQL. Double quotes are reserved for delimited identifiers (column or property names that contain spaces or conflict with keywords).

## Executing GQL

There are three ways to run a GQL query against SeleneDB.

**CLI** -- the `selene` command communicates with the server over QUIC:

```bash
selene --insecure gql "MATCH (s:sensor) RETURN s.name AS name"
```

**HTTP** -- POST to the `/gql` endpoint with a JSON body:

```bash
curl -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (s:sensor) RETURN s.name AS name"}'
```

**MCP** -- use the `gql_query` tool from any MCP client:

```json
{"tool": "gql_query", "arguments": {"query": "MATCH (s:sensor) RETURN s.name AS name"}}
```

All three methods produce the same results. The CLI uses QUIC (Arrow IPC by default), HTTP returns JSON, and MCP returns JSON. For programmatic clients, the `selene-client` crate provides an async QUIC client with the same capabilities.

## Plan cache

SeleneDB caches parsed GQL statements to avoid re-parsing identical queries. The cache is keyed by query text hash and stores the parsed AST as an `Arc<GqlStatement>`.

**Cache hit latency:** approximately 19 ns (compared to 7 us for a full parse).

**Capacity:** 256 entries with LRU eviction. When the cache is full, the least recently used entry is evicted.

**Invalidation:** the cache clears automatically when the graph's generation counter changes. Schema modifications (CREATE NODE TYPE, DROP NODE TYPE, etc.) increment the generation, ensuring that queries are re-parsed against the updated schema.

**CALL fast path:** standalone `CALL procedure(args) YIELD cols` queries bypass both the cache and the PEG parser entirely. A hand-written parser handles literal arguments (integers, floats, strings) in approximately 500 ns -- roughly 20x faster than the full PEG parse. Queries with variable references or complex expressions fall through to the standard parser.

## Next steps

- [Queries](queries.md) -- MATCH, FILTER, RETURN, aggregation, variable-length paths
- [Mutations](mutations.md) -- INSERT, SET, DELETE, transactions
- [Functions](functions.md) -- 101 scalar functions (math, string, temporal, vector)
- [Procedures](procedures.md) -- 56 CALL procedures (time-series, algorithms, search)
- [Optimizer](optimizer.md) -- EXPLAIN, PROFILE, optimizer rules, index types
