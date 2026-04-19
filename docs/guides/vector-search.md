# Vector Search & GraphRAG

SeleneDB provides a mutable HNSW vector index, top-k cosine search, BM25+vector
hybrid retrieval, and GraphRAG with community context — all over a single graph
engine. **Vectors are BYO:** applications embed text with their own model
(Aether's `aether-memory`, an OpenAI/Cohere endpoint, a local EmbeddingGemma —
anything) and pass pre-computed vectors as GQL parameters.

Removing the embedding backend from the DB keeps the layering clean: SeleneDB
stores, indexes, and searches vectors; the application owns embedding strategy,
model lifecycle, and tier policy.

## Architecture

| Tier | Procedure | Input | What it does |
|------|-----------|-------|--------------|
| **Raw vectors** | `graph.vectorSearch`, `graph.similarNodes` | `$vec` | Cosine similarity over stored vectors. |
| **Semantic search** | `graph.semanticSearch` | `$vec` | Vector search + containment-path enrichment. |
| **Scoped search** | `graph.scopedVectorSearch`, `graph.scopedSemanticSearch` | `$vec` | Restricts the scan to the BFS neighborhood of a root node. |
| **Hybrid search** | `graph.hybridSearch` | `queryText` + `$vec` | BM25 lexical + vector cosine via reciprocal rank fusion. |
| **GraphRAG** | `graphrag.search` | `$vec` | Vector similarity + BFS graph expansion + community context. Modes: `local`, `global`, `hybrid`. |
| **Community search** | `graph.communitySearch` | `$vec` | Vector top-k plus Louvain community context per hit. |

**Index:** HNSW (Hierarchical Navigable Small World). Sub-millisecond approximate
nearest neighbor search with >95% recall. Lock-free reads (~1ns) via `ArcSwap`,
incremental inserts via `RwLock`. Brute-force cosine scan is the fallback when
HNSW is not built.

All vector features are always compiled. No feature flags — toggle services via
`selene.toml` or environment variables.

## Storing Vectors

Write any f32 vector onto any property:

```sql
-- Single node
MATCH (s:sensor) FILTER id(s) = $id
SET s.embedding = $vec

-- Bulk seed from a parameter map (e.g. after client-side batch embedding)
UNWIND $rows AS row
MATCH (s:sensor) FILTER id(s) = row.id
SET s.embedding = row.vec
```

SeleneDB accepts any vector dimension; the first inserted vector fixes the
dimension for that property / HNSW namespace. Mismatched-dimension inserts are
skipped with a warning.

## Search Procedures

### graph.vectorSearch — Top-k Cosine Search

Brute-force top-k cosine similarity over nodes with a given label and vector
property.

```sql
CALL graph.vectorSearch('sensor', 'embedding', $queryVec, 10)
YIELD nodeId, score
```

| Parameter | Type | Description |
|-----------|------|-------------|
| label | STRING | Node label to search within |
| property | STRING | Vector property name |
| queryVector | VECTOR | Pre-computed query vector |
| k | INT | Number of results (max 10,000) |

**Yields:** `nodeId` (UINT), `score` (FLOAT)

HNSW fast path kicks in automatically when an index exists for the label's
namespace.

### graph.similarNodes — Find Similar Nodes

Find the k nodes most similar to a reference node's vector property. Label is
inferred from the reference node.

```sql
CALL graph.similarNodes(42, 'embedding', 10)
YIELD nodeId, score
```

### graph.scopedVectorSearch — Neighborhood-Scoped Search

Restricts vector search to the BFS neighborhood of a root node. Efficient for
"find similar sensors on this floor" queries.

```sql
CALL graph.scopedVectorSearch(1, 3, 'embedding', $queryVec, 10)
YIELD nodeId, score
```

### graph.semanticSearch — Vector Search + Containment Path

Top-k cosine on the `embedding` property with containment-path enrichment for
each hit.

```sql
CALL graph.semanticSearch($queryVec, 10)
YIELD node_id, score, path

-- With a label filter:
CALL graph.semanticSearch($queryVec, 10, 'sensor')
YIELD node_id, score, path
```

The `path` column contains the containment hierarchy for each result (e.g.
`"Building-1 > Floor-3 > Room-301 > SAT-1"`), walked from `contains` /
`has_sensor` / `supplies` edges.

### graph.scopedSemanticSearch — BFS + Vector Search

Same as `scopedVectorSearch` but targets the `embedding` property by default.

```sql
CALL graph.scopedSemanticSearch($rootId, 3, $queryVec, 10)
YIELD node_id, score
```

## Hybrid Search

### graph.hybridSearch — BM25 + Vector via RRF

Combines BM25 text search with cosine vector search using reciprocal rank
fusion. Better recall than either alone — keyword matches catch exact terms
while vector search captures semantic similarity.

```sql
CALL graph.hybridSearch('sensor', 'supply air temperature', $queryVec, 10)
YIELD node_id, score
```

| Parameter | Type | Description |
|-----------|------|-------------|
| label | STRING | Node label to search within |
| queryText | STRING | Lexical query for BM25 |
| queryVector | VECTOR | Pre-computed query embedding for cosine |
| k | INT | Number of results |

The procedure over-fetches 2× candidates from each source, then fuses rankings
with RRF (k=60) and returns the top-k.

## Similarity Functions

Two scalar functions are available for direct vector comparison in expressions:

```sql
MATCH (a:sensor), (b:sensor)
WHERE id(a) <> id(b)
RETURN a.name, b.name, cosine_similarity(a.embedding, b.embedding) AS sim
ORDER BY sim DESC LIMIT 5
```

| Function | Description |
|----------|-------------|
| `cosine_similarity(v1, v2)` | Cosine similarity (range: -1.0 to 1.0) |
| `euclidean_distance(v1, v2)` | Euclidean (L2) distance |

## GraphRAG — Graph-Augmented Retrieval

GraphRAG combines vector similarity with graph structure and community
intelligence. Instead of returning isolated nodes, it returns contextually rich
results with relationship paths and community-level summaries.

### graphrag.search — Unified Retrieval

```sql
CALL graphrag.search($queryVec, 10, 2, 'local')
YIELD node_id, score, source, context, depth
```

| Parameter | Type | Description |
|-----------|------|-------------|
| queryVector | VECTOR | Pre-computed query embedding |
| k | INT | Number of vector results (max 10,000) |
| maxHops | INT | BFS expansion depth (max 10) |
| mode | STRING | `"local"` (default), `"global"`, or `"hybrid"` |

**Yields:** `node_id` (INT), `score` (FLOAT), `source` (STRING),
`context` (STRING), `depth` (INT)

### Search Modes

**Local** (default): Vector search → BFS graph expansion → optional community
context. Best for specific questions with known entity types.

**Global**: Vector search over `__CommunitySummary` embeddings. Returns
community-level profiles rather than individual nodes. Best for broad
questions like "what systems are in this building?" — requires community
embeddings to be populated.

**Hybrid**: Runs both and merges results with provenance tags. Best coverage
at the cost of latency. Falls back to `local` when community embeddings are
absent.

### Community Context

Build and enrich communities on the client side:

```sql
-- 1. Detect communities (creates __CommunitySummary nodes)
CALL graph.buildCommunities(2) YIELD community_id, node_count

-- 2. Populate each summary with a pre-computed embedding (client embeds the
-- label_distribution + key_entities text in its own process, then writes
-- back):
MATCH (c:__CommunitySummary)
SET c.embedding = $communityVec
```

The `build_communities` MCP tool runs Louvain and persists the structural
profiles; the `graphrag_search` MCP tool accepts a pre-computed
`query_vector: number[]` for retrieval.

## Vector Quantization (PolarQuant)

SeleneDB includes built-in vector quantization based on the **PolarQuant**
algorithm (TurboQuant Stage 1). Compresses HNSW vector storage by 4–10× while
maintaining >99% recall, making large vector indexes practical on edge devices.

### How It Works

PolarQuant is a data-oblivious scalar quantization scheme — no training data
or codebook computation required:

1. **Haar-random rotation** — Each vector is multiplied by a deterministic
   orthogonal matrix (seeded), spreading information across all coordinates
   and making per-coordinate quantization more uniform.
2. **Lloyd-Max quantization** — Each rotated coordinate is quantized using
   optimal boundaries for Gaussian N(0, 1/√d) distributions. Supports 3, 4,
   or 8 bits per coordinate.
3. **Bit-packing** — Quantized codes are packed into bytes for compact storage.

The quantizer is fully deterministic from (seed, dimension, bits) — the same
configuration always produces the same encoding. No calibration dataset needed.

### Compression Ratios

For 768-dimensional embeddings:

| Bit Width | Bytes/Vector | vs f32 (3,072 B) | 100K Vectors | 1M Vectors |
|-----------|-------------|-------------------|--------------|------------|
| f32 (default) | 3,072 | 1× | 293 MB | 2.9 GB |
| **8-bit** | 768 | **4×** | 73 MB | 732 MB |
| **4-bit** (default) | 384 | **8×** | 37 MB | 366 MB |
| **3-bit** | 288 | **10.7×** | 27 MB | 275 MB |

### Configuration

Enable quantization in `selene.toml`:

```toml
[vector]
hnsw_quantize = true           # Enable PolarQuant (default: false)
hnsw_quantize_bits = 4         # Bit width: 3, 4, or 8 (default: 4)
hnsw_quantize_rescore = false  # Re-rank top-k with f32 vectors (default: false)
```

**Recommended settings:**
- **General use:** `bits = 4` — best balance of compression (8×) and recall (>99%)
- **Maximum recall:** `bits = 8, rescore = true` — near-lossless with 4× compression
- **Maximum compression:** `bits = 3` — 10.7× compression, ~95-98% recall

### Search Architecture

Quantized search uses an **asymmetric distance** strategy that preserves
accuracy:

- **Upper HNSW layers** (greedy navigation): Full f32 cosine similarity.
  These layers are sparse — few comparisons, accuracy is critical for graph
  navigation.
- **Layer 0** (beam search): Asymmetric dot product between the f32 query
  (rotated once) and quantized codes. Where 95%+ of distance computations
  happen — biggest speedup here.
- **Optional rescore**: When `rescore = true`, the top-ef candidates from
  quantized search are re-ranked using full f32 cosine similarity before
  returning the final top-k.

The query is never quantized — only stored vectors are compressed. The rotated
query is computed once per search (O(d²)), then each candidate comparison is
O(d) with the packed codes.

### Monitoring

```sql
CALL vector.quantizationStats()
YIELD namespace, method, bits, vector_count, quantized_bytes, f32_bytes, compression_ratio, rescore
```

The `quantization_stats` MCP tool exposes the same information.

### Persistence

Quantized vectors are automatically included in binary snapshots. The
`QuantizedStorage` (rotation matrix, quantizer parameters, packed codes)
serializes alongside the HNSW graph — no migration or configuration needed.
