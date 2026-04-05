# GQL Procedures Reference

Selene provides 56 built-in CALL procedures that produce streaming result rows. Each procedure accepts typed arguments and yields named columns that you select with YIELD.

## Syntax

```gql
CALL procedure.name(arg1, arg2) YIELD col1, col2
```

Procedures can appear standalone or compose with MATCH patterns. Use YIELD to select which columns to project into the result set. Columns not listed in YIELD are discarded.

```gql
MATCH (s:sensor)
CALL ts.latest(id(s), 'temperature') YIELD timestamp, value
RETURN s.name, value
```

---

## 1. Time-Series (12 procedures)

Time-series procedures read from Selene's multi-tier storage: hot (in-memory, multi-encoding), warm (pre-computed aggregates), and cold (Parquet on disk). Duration parameters accept nanosecond integers or ISO duration values.

### ts.range

Returns raw samples from the hot tier within an absolute time range.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| start | ZonedDateTime | Yes | Range start (inclusive) |
| end | ZonedDateTime | Yes | Range end (inclusive) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| timestamp | ZonedDateTime | Sample timestamp (UTC) |
| value | Float | Sample value |

```gql
CALL ts.range(42, 'temperature', datetime('2026-03-28T00:00:00Z'), datetime('2026-03-29T00:00:00Z'))
  YIELD timestamp, value
```

### ts.latest

Returns the most recent sample for an entity's property. Returns zero rows if no data exists.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| timestamp | ZonedDateTime | Sample timestamp (UTC) |
| value | Float | Most recent value |

```gql
MATCH (s:sensor {name: 'AHU-1-SAT'})
CALL ts.latest(id(s), 'temperature') YIELD value
RETURN s.name, value
```

### ts.aggregate

Computes a scalar aggregate over a trailing time window. Supported aggregation functions: `avg`, `sum`, `min`, `max`, `count`.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| duration | ZonedDateTime | Yes | Lookback window (nanoseconds or duration) |
| agg_fn | String | Yes | Aggregation function: avg, sum, min, max, count |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| value | Float | Aggregated result |

```gql
CALL ts.aggregate(42, 'temperature', 3600000000000, 'avg') YIELD value
```

### ts.window

Tumbling window aggregation that returns one row per window. Divides the lookback period into fixed-size windows and applies the aggregation function to each.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| window_size | ZonedDateTime | Yes | Window width (nanoseconds or duration) |
| agg_fn | String | Yes | Aggregation function: avg, sum, min, max, count |
| duration | ZonedDateTime | Yes | Total lookback period |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| window_start | ZonedDateTime | Window start timestamp |
| window_end | ZonedDateTime | Window end timestamp |
| value | Float | Aggregated value for the window |

```gql
CALL ts.window(42, 'temperature', 900000000000, 'avg', 3600000000000)
  YIELD window_start, window_end, value
```

### ts.downsample

Returns pre-computed warm-tier aggregates. Each row represents one downsample window. Requires the warm tier to be enabled.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| start | ZonedDateTime | Yes | Range start |
| end | ZonedDateTime | Yes | Range end |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| window_start | ZonedDateTime | Downsample window start |
| min | Float | Minimum value in the window |
| max | Float | Maximum value in the window |
| avg | Float | Average value in the window |
| count | Int | Number of samples in the window |

```gql
CALL ts.downsample(42, 'temperature', datetime('2026-03-28T00:00:00Z'), datetime('2026-03-29T00:00:00Z'))
  YIELD window_start, min, max, avg, count
```

### ts.history

Returns historical samples from the cold tier (Parquet files on disk). Requires a TsHistoryProvider registered at server startup.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| start | ZonedDateTime | Yes | Range start |
| end | ZonedDateTime | Yes | Range end |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| timestamp | ZonedDateTime | Sample timestamp (UTC) |
| value | Float | Sample value |

```gql
CALL ts.history(42, 'temperature', datetime('2026-01-01T00:00:00Z'), datetime('2026-02-01T00:00:00Z'))
  YIELD timestamp, value
```

### ts.fullRange

Queries both hot and cold tiers, merges results by timestamp, and deduplicates. When the same timestamp appears in both tiers, the hot-tier value takes precedence.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| start | ZonedDateTime | Yes | Range start |
| end | ZonedDateTime | Yes | Range end |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| timestamp | ZonedDateTime | Sample timestamp (UTC) |
| value | Float | Sample value |

```gql
CALL ts.fullRange(42, 'temperature', datetime('2026-01-01T00:00:00Z'), datetime('2026-03-29T00:00:00Z'))
  YIELD timestamp, value
```

### ts.trends

Returns hourly aggregates from the hierarchical warm tier. Designed for month-scale dashboard visualizations where per-sample resolution is unnecessary.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| start | ZonedDateTime | Yes | Range start |
| end | ZonedDateTime | Yes | Range end |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| window_start | ZonedDateTime | Hourly window start |
| min | Float | Minimum value |
| max | Float | Maximum value |
| avg | Float | Average value |
| count | Int | Sample count |

```gql
CALL ts.trends(42, 'temperature', datetime('2026-02-01T00:00:00Z'), datetime('2026-03-01T00:00:00Z'))
  YIELD window_start, avg
```

### ts.percentile

Returns a percentile value from the warm tier's DDSketch accumulators. Pre-computed quantiles (p50, p90, p95, p99) are snapshotted per window. The procedure returns the closest pre-computed quantile, weighted by sample count across all windows in the requested duration.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| quantile | Float | Yes | Quantile between 0.0 and 1.0 (e.g. 0.95 for p95) |
| duration | ZonedDateTime | Yes | Lookback duration |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| value | Float | Estimated percentile value |

```gql
CALL ts.percentile(42, 'temperature', 0.95, 86400000000000) YIELD value
```

### ts.scopedAggregate

Traverses the graph from a root node via BFS, collects all descendant nodes, and aggregates their time-series data for a given property. Useful for building-level or zone-level rollups.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| rootNodeId | Int | Yes | Starting node for BFS traversal |
| maxHops | Int | Yes | Maximum BFS depth |
| property | String | Yes | Time-series property name |
| aggFn | String | Yes | Aggregation function: avg, sum, min, max, count |
| duration | ZonedDateTime | Yes | Lookback duration |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| value | Float | Aggregated result across all descendant nodes |
| nodeCount | Int | Number of descendant nodes with data |
| sampleCount | Int | Total number of samples aggregated |

```gql
CALL ts.scopedAggregate(1, 3, 'temperature', 'avg', 3600000000000)
  YIELD value, nodeCount, sampleCount
```

### ts.anomalies

Detects anomalies using Z-score analysis. Compares recent hot-tier samples against warm-tier mean and standard deviation. Returns samples where the absolute Z-score exceeds the threshold.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_id | Int | Yes | Node ID of the entity |
| property | String | Yes | Time-series property name |
| threshold | Float | Yes | Z-score threshold (must be positive) |
| duration | ZonedDateTime | Yes | Lookback duration |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| timestamp | ZonedDateTime | Timestamp of the anomalous sample |
| value | Float | Sample value |
| z_score | Float | Z-score (positive or negative deviation) |

```gql
CALL ts.anomalies(42, 'temperature', 2.0, 3600000000000) YIELD timestamp, value, z_score
```

### ts.peerAnomalies

Graph-aware peer comparison using BFS traversal. Collects the latest reading from each peer node in the neighborhood, computes population statistics, and flags nodes that deviate beyond the threshold. Results are sorted by absolute Z-score descending.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| nodeId | Int | Yes | Starting node for BFS |
| property | String | Yes | Time-series property name |
| maxHops | Int | Yes | Maximum BFS depth |
| threshold | Float | Yes | Z-score threshold (must be positive) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID of the anomalous peer |
| value | Float | Latest reading for that peer |
| z_score | Float | Z-score relative to peer population |

```gql
CALL ts.peerAnomalies(1, 'temperature', 2, 1.5) YIELD nodeId, value, z_score
```

---

## 2. Schema Introspection (7 procedures)

Schema introspection procedures examine the graph's structure, registered schemas, and constraints.

### graph.labels

Returns all distinct node labels present in the graph.

**Parameters:** None.

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| label | String | A node label |

```gql
CALL graph.labels() YIELD label
```

### graph.edge_types

Returns all distinct edge types (labels) present in the graph.

**Parameters:** None.

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| type | String | An edge type |

```gql
CALL graph.edge_types() YIELD type
```

### graph.node_count

Returns the total number of nodes in the graph.

**Parameters:** None.

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| count | Int | Total node count |

```gql
CALL graph.node_count() YIELD count
```

### graph.edge_count

Returns the total number of edges in the graph.

**Parameters:** None.

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| count | Int | Total edge count |

```gql
CALL graph.edge_count() YIELD count
```

### graph.schema

Returns the registered schema details for a given label, including property definitions, types, and constraints.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| label | String | Yes | Node label to inspect |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| property | String | Property name |
| valueType | String | Value type (Int, Float, String, etc.) |
| required | Bool | Whether the property is required |
| unique | Bool | Whether the property has a uniqueness constraint |
| indexed | Bool | Whether the property is indexed |
| immutable | Bool | Whether the property is immutable after creation |
| constraints | String | Additional constraints (min, max, pattern, enum) |

```gql
CALL graph.schema('sensor') YIELD property, valueType, required, indexed
```

### graph.constraints

Returns all constraints across all registered schemas, including uniqueness, immutability, range, enum, pattern, node key, and cardinality constraints.

**Parameters:** None.

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| label | String | Schema label (node or edge) |
| constraintType | String | Type: unique, immutable, range, enum, pattern, node_key, cardinality |
| properties | String | Affected property or properties |
| description | String | Human-readable constraint description |

```gql
CALL graph.constraints() YIELD label, constraintType, description
```

### graph.discoverSchema

Infers schema from existing graph data by scanning all nodes, grouping by label, and analyzing property types, null rates, and uniqueness. Useful for exploring unschematized graphs.

**Parameters:** None.

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| label | String | Node label |
| property | String | Property name |
| inferredType | String | Majority value type observed |
| nullRate | Float | Fraction of nodes missing this property (0.0 to 1.0) |
| uniqueRate | Float | Fraction of distinct values among non-null values |
| sampleSize | Int | Number of nodes with this label |

```gql
CALL graph.discoverSchema() YIELD label, property, inferredType, nullRate
```

---

## 3. Graph Algorithms (18 procedures)

Graph algorithm procedures operate on named projections. A projection is a filtered, in-memory snapshot of the graph that includes only the specified node and edge labels. If a projection with the given name does not exist when an algorithm procedure is called, a default projection containing all nodes and edges is created automatically.

### graph.project

Creates a named graph projection. Filters nodes and edges by label and optionally sets a weight property for path-finding algorithms.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| name | String | Yes | Projection name |
| nodeLabels | List(String) | No | Node labels to include (empty = all) |
| edgeLabels | List(String) | No | Edge labels to include (empty = all) |
| weightProp | String | No | Edge property to use as weight |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| name | String | Projection name |
| nodeCount | Int | Number of nodes in the projection |
| edgeCount | Int | Number of edges in the projection |

```gql
CALL graph.project('hvac', ['equipment', 'sensor'], ['feeds', 'contains'], 'distance')
  YIELD name, nodeCount, edgeCount
```

### graph.drop

Drops a named graph projection and frees its memory.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| name | String | Yes | Projection name to drop |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| dropped | Bool | True if the projection existed and was dropped |

```gql
CALL graph.drop('hvac') YIELD dropped
```

### graph.listProjections

Lists all currently registered graph projections.

**Parameters:** None.

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| name | String | Projection name |
| nodeCount | Int | Number of nodes |
| edgeCount | Int | Number of edges |

```gql
CALL graph.listProjections() YIELD name, nodeCount, edgeCount
```

### graph.wcc

Computes weakly connected components using Union-Find. Returns a component ID for each node.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID |
| componentId | Int | Component ID |

```gql
CALL graph.wcc('hvac') YIELD nodeId, componentId
```

### graph.scc

Computes strongly connected components using Tarjan's algorithm. Returns a component ID for each node.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID |
| componentId | Int | Component ID |

```gql
CALL graph.scc('hvac') YIELD nodeId, componentId
```

### graph.topoSort

Computes a topological ordering of nodes. Returns an error if the graph contains a cycle.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID |
| position | Int | Position in topological order (0-based) |

```gql
CALL graph.topoSort('hvac') YIELD nodeId, position
```

### graph.articulationPoints

Finds all articulation points (cut vertices) in the graph. These are nodes whose removal would disconnect the graph.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID of the articulation point |

```gql
CALL graph.articulationPoints('hvac') YIELD nodeId
```

### graph.bridges

Finds all bridge edges in the graph. These are edges whose removal would disconnect the graph.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| sourceId | Int | Source node ID |
| targetId | Int | Target node ID |

```gql
CALL graph.bridges('hvac') YIELD sourceId, targetId
```

### graph.validate

Validates the graph structure and returns any issues found (orphaned nodes, self-loops, isolated components, etc.).

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| severity | String | Issue severity |
| issue | String | Issue type |
| nodeId | Int | Affected node ID (null if not node-specific) |
| message | String | Human-readable description |

```gql
CALL graph.validate('hvac') YIELD severity, issue, message
```

### graph.isAncestor

Tests whether one node is an ancestor of another in the containment hierarchy. Builds a ContainmentIndex internally.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| ancestor | Int | Yes | Potential ancestor node ID |
| descendant | Int | Yes | Potential descendant node ID |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| result | Bool | True if ancestor is an ancestor of descendant |

```gql
CALL graph.isAncestor(1, 42) YIELD result
```

### graph.shortestPath

Finds the shortest path between two nodes using Dijkstra's algorithm. Uses edge weights from the projection's weight property if configured.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |
| from | Int | Yes | Source node ID |
| to | Int | Yes | Target node ID |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID in the path |
| cost | Float | Total path cost |
| index | Int | Position in the path (0-based) |

```gql
CALL graph.shortestPath('hvac', 1, 42) YIELD nodeId, cost, index
```

### graph.sssp

Single-source shortest paths from a source node to all reachable nodes using Dijkstra's algorithm.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |
| source | Int | Yes | Source node ID |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Destination node ID |
| distance | Float | Shortest distance from source |

```gql
CALL graph.sssp('hvac', 1) YIELD nodeId, distance
```

### graph.apsp

All-pairs shortest paths. Computes shortest distances between all node pairs. Capped at 1000 nodes to prevent excessive computation.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| source | Int | Source node ID |
| target | Int | Target node ID |
| distance | Float | Shortest distance |

```gql
CALL graph.apsp('hvac') YIELD source, target, distance
```

### graph.pagerank

Computes PageRank scores with configurable damping factor and iteration limit.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |
| damping | Float | No | Damping factor (default: 0.85) |
| maxIter | Int | No | Maximum iterations (default: 20) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID |
| score | Float | PageRank score |

```gql
CALL graph.pagerank('hvac', 0.85, 20) YIELD nodeId, score
```

### graph.betweenness

Computes betweenness centrality scores. Optionally samples a subset of source nodes for faster approximation on large graphs.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |
| sampleSize | Int | No | Number of source nodes to sample (null = exact) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID |
| score | Float | Betweenness centrality score |

```gql
CALL graph.betweenness('hvac', 100) YIELD nodeId, score
```

### graph.labelPropagation

Community detection using label propagation. Each node adopts the most common label among its neighbors. Runs until convergence or the iteration limit.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |
| maxIter | Int | No | Maximum iterations (default: 10) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID |
| communityId | Int | Assigned community ID |

```gql
CALL graph.labelPropagation('hvac', 10) YIELD nodeId, communityId
```

### graph.louvain

Community detection using the Louvain method. Optimizes modularity through hierarchical community merging. Returns the community assignment and hierarchy level for each node.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID |
| communityId | Int | Community ID |
| level | Int | Hierarchy level |

```gql
CALL graph.louvain('hvac') YIELD nodeId, communityId, level
```

### graph.triangleCount

Counts the number of triangles each node participates in. Useful for measuring local clustering.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| projection | String | Yes | Projection name |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | Int | Node ID |
| count | Int | Number of triangles |

```gql
CALL graph.triangleCount('hvac') YIELD nodeId, count
```

---

## 4. Vector Search (4 procedures)

Vector search procedures perform brute-force top-k cosine similarity search over vector properties stored on nodes. The `graph.semanticSearch` procedure requires the `vector` feature flag for automatic text embedding.

### graph.vectorSearch

Top-k cosine similarity search over nodes with a given label and vector property.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| label | String | Yes | Node label to search |
| property | String | Yes | Vector property name |
| queryVector | Vector | Yes | Query vector |
| k | Int | Yes | Number of results to return (max 10,000) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | UInt | Node ID |
| score | Float | Cosine similarity score |

```gql
CALL graph.vectorSearch('sensor', 'embedding', $queryVec, 10) YIELD nodeId, score
```

### graph.similarNodes

Finds the k nodes most similar to a given node's vector property. Automatically infers the label from the reference node and excludes the reference node from results.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| nodeId | UInt | Yes | Reference node ID |
| property | String | Yes | Vector property name |
| k | Int | Yes | Number of results (max 10,000) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | UInt | Similar node ID |
| score | Float | Cosine similarity score |

```gql
CALL graph.similarNodes(42, 'embedding', 10) YIELD nodeId, score
```

### graph.scopedVectorSearch

Vector search restricted to the BFS neighborhood of a root node. First collects candidate node IDs via BFS, then runs top-k cosine search over only those candidates. Efficient for localized queries such as "find similar sensors on this floor."

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| rootNodeId | UInt | Yes | Root node for BFS |
| maxHops | Int | Yes | Maximum BFS depth (capped at 20) |
| property | String | Yes | Vector property name |
| queryVector | Vector | Yes | Query vector |
| k | Int | Yes | Number of results (max 10,000) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | UInt | Node ID |
| score | Float | Cosine similarity score |

```gql
CALL graph.scopedVectorSearch(1, 3, 'embedding', $queryVec, 10) YIELD nodeId, score
```

### graph.semanticSearch

Combines text embedding and vector search with containment path traversal. Embeds the query text using the configured model, searches for similar nodes, and walks up the containment hierarchy for each result to build a path string. Requires `--features vector`.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| queryText | String | Yes | Natural language query |
| k | Int | Yes | Number of results (max 10,000) |
| label | String | No | Optional label filter |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | UInt | Node ID |
| score | Float | Cosine similarity score |
| path | String | Containment path (e.g. "Building > Floor 3 > Room 301 > SAT-1") |

```gql
CALL graph.semanticSearch('supply air temperature sensor', 10, 'sensor') YIELD nodeId, score, path
```

---

## 5. Full-Text Search (2 procedures)

Full-text search procedures use BM25-ranked retrieval via the search provider. Requires `--features search` and searchable properties defined in the schema. The `graph.hybridSearch` procedure additionally requires `--features vector`.

### graph.textSearch

BM25-ranked text search on a specific (label, property) pair.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| label | String | Yes | Node label |
| property | String | Yes | Searchable property name |
| query | String | Yes | Search query text |
| k | Int | Yes | Maximum results |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | UInt | Node ID |
| score | Float | BM25 relevance score |

```gql
CALL graph.textSearch('sensor', 'name', 'supply air temperature', 10) YIELD nodeId, score
```

### graph.hybridSearch

Combines BM25 text search and cosine vector search using reciprocal rank fusion (RRF). Searches across all searchable properties for the label, embeds the query text for vector search, and fuses both ranked lists. Requires both `search` and `vector` features.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| label | String | Yes | Node label |
| query | String | Yes | Search query text |
| k | Int | Yes | Maximum results |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodeId | UInt | Node ID |
| score | Float | Fused RRF score |

```gql
CALL graph.hybridSearch('sensor', 'supply air temperature', 10) YIELD nodeId, score
```

---

## 6. Change History (4 procedures)

Change history procedures query the temporal version store. They require a HistoryProvider registered at server startup (available when the server is running with temporal versioning enabled).

### graph.history

Returns the change history for a specific entity. Optional start and end times filter the results.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| nodeId | Int | Yes | Node ID |
| startTime | String | No | ISO 8601 timestamp or nanoseconds |
| endTime | String | No | ISO 8601 timestamp or nanoseconds |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| change_type | String | Type of change (e.g. create, update, delete) |
| key | String | Property key affected (null for node-level changes) |
| old_value | String | Previous value (null for inserts) |
| new_value | String | New value (null for deletes) |
| timestamp | Int | Timestamp in nanoseconds |

```gql
CALL graph.history(42) YIELD change_type, key, new_value, timestamp
```

### graph.changes

Returns recent changes for all entities with a given label within a time window. Duration supports suffixes: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| label | String | Yes | Node label to filter by |
| duration | String | Yes | Lookback window (e.g. "1h", "30m", "7d") |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| node_id | UInt | Node ID |
| change_type | String | Type of change |
| key | String | Property key affected |
| old_value | String | Previous value |
| new_value | String | New value |
| timestamp | Int | Timestamp in nanoseconds |

```gql
CALL graph.changes('sensor', '1h') YIELD node_id, change_type, key, new_value
```

### graph.propertyAt

Returns the value of a property at a specific point in time. Queries the temporal version store to reconstruct historical state.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| nodeId | Int | Yes | Node ID |
| property | String | Yes | Property name |
| timestamp | String | Yes | ISO 8601 timestamp or nanoseconds |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| value | String | Property value at the given time (null if not found) |

```gql
CALL graph.propertyAt(42, 'status', '2026-03-28T03:00:00Z') YIELD value
```

### graph.propertyHistory

Returns all recorded versions of a property. Each row represents a version with the value and the timestamp when it was superseded by a newer value. Optional start and end times filter the results.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| nodeId | Int | Yes | Node ID |
| property | String | Yes | Property name |
| startTime | String | No | ISO 8601 timestamp or nanoseconds |
| endTime | String | No | ISO 8601 timestamp or nanoseconds |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| value | String | Property value |
| superseded_at | Int | Timestamp when this version was replaced (nanoseconds) |

```gql
CALL graph.propertyHistory(42, 'status') YIELD value, superseded_at
```

---

## 7. RDF/SPARQL (3 procedures)

RDF procedures provide interoperability with RDF data models and SPARQL queries. They require the `rdf` feature flag. The `graph.sparql` procedure additionally requires the `rdf-sparql` feature.

### graph.exportRdf

Exports the graph as RDF in the specified serialization format. Optionally includes ontology quads when using N-Quads format.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| format | String | Yes | Serialization format: turtle, ntriples, nquads |
| includeAllGraphs | Bool | No | Include ontology quads in N-Quads output (default: false) |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| data | String | Serialized RDF content |
| format | String | Format used for serialization |

```gql
CALL graph.exportRdf('turtle') YIELD data
```

### graph.importRdf

Imports RDF data into the graph and optionally into the ontology store. Returns statistics about the import operation.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| data | String | Yes | RDF content to import |
| format | String | Yes | Input format: turtle, ntriples, nquads |
| targetGraph | String | No | Target graph -- use "ontology" to route to ontology store |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| nodesCreated | Int | Number of nodes created |
| edgesCreated | Int | Number of edges created |
| labelsAdded | Int | Number of labels added |
| propertiesSet | Int | Number of properties set |
| ontologyTriplesLoaded | Int | Number of ontology triples loaded |

```gql
CALL graph.importRdf('@prefix : <http://example.org/> . :sensor1 a :Sensor .', 'turtle')
  YIELD nodesCreated, edgesCreated
```

### graph.sparql

Executes a SPARQL query against the graph viewed as an RDF dataset. Returns results as a JSON string. Requires `--features rdf-sparql`.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| query | String | Yes | SPARQL query string |

**YIELD columns:**

| Column | Type | Description |
|--------|------|-------------|
| results | String | JSON-serialized SPARQL results |

```gql
CALL graph.sparql('SELECT ?s WHERE { ?s a <selene:type/Sensor> }') YIELD results
```
