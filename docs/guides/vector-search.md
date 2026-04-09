# Vector Search & GraphRAG

SeleneDB provides an integrated retrieval stack — from raw vector similarity to full GraphRAG — that runs inside the database process with zero external dependencies. Embeddings, vector indexes, graph traversal, and community-aware retrieval compose through GQL procedures, so agents get answers in one call instead of orchestrating three separate systems.

## Architecture

The retrieval pipeline has four tiers, each building on the one below:

| Tier | Procedure | What it does |
|------|-----------|--------------|
| **Raw vectors** | `graph.vectorSearch`, `graph.similarNodes` | Cosine similarity over stored vectors. Works with any embedding source. |
| **Semantic search** | `graph.semanticSearch` | Embeds text → vector search → containment path resolution. One-call natural language lookup. |
| **Scoped search** | `graph.scopedVectorSearch` | Restricts vector search to BFS neighborhood of a root node. Localized queries like "find similar sensors on this floor." |
| **GraphRAG** | `graphrag.search` | Vector similarity + BFS graph expansion + community context. Three modes: `local`, `global`, `hybrid`. |

**Embedding models:**
- **EmbeddingGemma** (default) — 24-layer bidirectional transformer via candle. GGUF quantized variants supported. GPU-accelerated on CUDA and Metal.
- **all-MiniLM-L6-v2** — Lightweight 384-dimensional sentence transformer. Good for resource-constrained edge deployments.
- **Remote endpoint** — HTTP-based embedding via `[vector] endpoint` config for shared inference services.

**Vector indexing:**
- **HNSW** (Hierarchical Navigable Small World) — Sub-millisecond approximate nearest neighbor search with >95% recall. Lock-free reads (~1ns) via `ArcSwap`, incremental inserts via `RwLock`. Replaces brute-force O(N) scans for 10K–100K+ vectors.
- **Brute-force fallback** — Cosine scan with O(N log k) heap selection when HNSW is not built.

All vector features are always compiled and available. No feature flags needed — configure via `selene.toml` or environment variables.

## Setup

### Model Configuration

Set the model path in `selene.toml` or via environment variable:

```toml
[vector]
model_path = "data/models/embedding-gemma"
```

```bash
export SELENE_MODEL_PATH=data/models/embedding-gemma
```

For the lighter MiniLM model:

```bash
./scripts/fetch-model.sh
# Downloads to data/models/all-MiniLM-L6-v2/ (~22 MB)
```

The model loads on the first `embed()` call and is cached for the server lifetime. GPU acceleration is automatic — the engine selects CUDA > Metal > CPU based on available hardware.

### GPU Acceleration

SeleneDB's embedding engine runs on NVIDIA GPUs (CUDA) and Apple Silicon (Metal) with zero configuration. The `select_device()` function auto-detects available hardware:

| Hardware | Backend | Performance |
|----------|---------|-------------|
| NVIDIA GPU (T4, A100, etc.) | CUDA | 10-50x faster than CPU |
| Apple M1/M2/M3 | Metal | 5-20x faster than CPU |
| Any CPU | CPU (candle) | Baseline |

For CUDA deployments, build with the `cuda` feature and use `Dockerfile.gpu`:

```bash
cargo build -p selene-server --release --features cuda,dev-tls
```

## Generating Embeddings

The `embed()` scalar function converts text to a 384-dimensional vector:

```sql
RETURN embed('supply air temperature sensor') AS vec
```

The input is limited to 8 KiB of text. The BERT tokenizer truncates to 512 tokens internally.

### Storing Embeddings on Nodes

Generate and store embeddings for existing nodes:

```sql
MATCH (n:sensor)
SET n.embedding = embed(n.name)
```

This iterates over all `sensor` nodes, embeds each node's `name` property, and stores the resulting vector on the `embedding` property.

## Search Procedures

### graph.vectorSearch -- Top-k Cosine Search

Brute-force top-k cosine similarity search over nodes with a given label and vector property.

```sql
CALL graph.vectorSearch('sensor', 'embedding', embed('air temperature'), 10)
YIELD nodeId, score
```

| Parameter | Type | Description |
|-----------|------|-------------|
| label | STRING | Node label to search within |
| property | STRING | Vector property name |
| queryVector | VECTOR | Query vector (e.g. from `embed()`) |
| k | INT | Number of results to return (max 10,000) |

**Yields:** `nodeId` (UINT), `score` (FLOAT)

Results are sorted by cosine similarity in descending order. Nodes without the specified property or with vectors of mismatched dimensions are skipped.

### graph.similarNodes -- Find Similar Nodes

Find the k nodes most similar to a given node's vector property. The label is inferred from the reference node.

```sql
CALL graph.similarNodes(42, 'embedding', 10)
YIELD nodeId, score
```

| Parameter | Type | Description |
|-----------|------|-------------|
| nodeId | UINT | Reference node ID |
| property | STRING | Vector property name |
| k | INT | Number of results to return (max 10,000) |

**Yields:** `nodeId` (UINT), `score` (FLOAT)

The reference node is excluded from results. Nodes are filtered to those sharing the same first label as the reference node.

### graph.scopedVectorSearch -- Neighborhood-Scoped Search

Restricts vector search to the BFS neighborhood of a root node. This is efficient for localized queries such as "find similar sensors on this floor."

```sql
CALL graph.scopedVectorSearch(1, 3, 'embedding', embed('supply air temp'), 10)
YIELD nodeId, score
```

| Parameter | Type | Description |
|-----------|------|-------------|
| rootNodeId | UINT | Root node for BFS traversal |
| maxHops | INT | BFS depth limit (max 20) |
| property | STRING | Vector property name |
| queryVector | VECTOR | Query vector |
| k | INT | Number of results to return (max 10,000) |

**Yields:** `nodeId` (UINT), `score` (FLOAT)

### graph.semanticSearch -- Text-to-Node Search with Containment Path

Combines `embed()` with vector search and containment-path traversal. Pass natural-language text directly -- the procedure embeds it internally.

```sql
CALL graph.semanticSearch('supply air temperature sensor', 10)
YIELD nodeId, score, path
```

An optional third argument filters by label:

```sql
CALL graph.semanticSearch('supply air temperature sensor', 10, 'sensor')
YIELD nodeId, score, path
```

| Parameter | Type | Description |
|-----------|------|-------------|
| queryText | STRING | Natural-language search text |
| k | INT | Number of results (max 10,000) |
| label | STRING | (Optional) label filter |

**Yields:** `nodeId` (UINT), `score` (FLOAT), `path` (STRING)

The `path` column contains the containment hierarchy for each result (e.g. `"Building-1 > Floor-3 > Room-301 > SAT-1"`), constructed by walking up the containment edges from the matched node.

## Full-Text Search

### graph.textSearch -- BM25 Ranked Search

BM25-ranked full-text search over a specific label and property. Requires a schema with `searchable = true` on the target property.

```sql
CALL graph.textSearch('sensor', 'name', 'supply air temperature', 10)
YIELD nodeId, score
```

| Parameter | Type | Description |
|-----------|------|-------------|
| label | STRING | Node label to search within |
| property | STRING | Text property to search |
| query | STRING | Search query text |
| k | INT | Number of results to return |

**Yields:** `nodeId` (UINT), `score` (FLOAT)

## Hybrid Search

### graph.hybridSearch -- BM25 + Vector Fusion

Combines BM25 text search with cosine vector search using reciprocal rank fusion (RRF). This gives better recall than either approach alone -- keyword matches catch exact terms while vector search captures semantic similarity.

```sql
CALL graph.hybridSearch('sensor', 'supply air temperature', 10)
YIELD nodeId, score
```

| Parameter | Type | Description |
|-----------|------|-------------|
| label | STRING | Node label to search within |
| query | STRING | Search query text |
| k | INT | Number of results to return |

**Yields:** `nodeId` (UINT), `score` (FLOAT)

The procedure over-fetches 2x candidates from each source, then fuses rankings with RRF (k=60) and returns the top k results. The BM25 search covers all searchable properties for the given label. The vector search uses the `embedding` property.

## Similarity Functions

Two scalar functions are available for direct vector comparison in expressions:

```sql
MATCH (a:sensor), (b:sensor)
WHERE a.id <> b.id
RETURN a.name, b.name, cosine_similarity(a.embedding, b.embedding) AS sim
ORDER BY sim DESC LIMIT 5
```

| Function | Description |
|----------|-------------|
| `cosine_similarity(v1, v2)` | Cosine similarity between two vectors (range: -1.0 to 1.0) |
| `euclidean_distance(v1, v2)` | Euclidean (L2) distance between two vectors |

## Auto-Embed Configuration

The `[vector.auto_embed]` section in `selene.toml` defines rules for automatic embedding generation. When a text property changes on a node matching a rule, a background task generates an embedding and stores it on the specified property.

```toml
[vector]
model_path = "data/models/all-MiniLM-L6-v2"

[[vector.auto_embed]]
label = "sensor"
text_property = "name"
embedding_property = "embedding"

[[vector.auto_embed]]
label = "equipment"
text_property = "description"
embedding_property = "embedding"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| label | STRING | (required) | Node label to match |
| text_property | STRING | (required) | Source text property |
| embedding_property | STRING | "embedding" | Target property for the vector |

## Remote Embedding Endpoint

As an alternative to local candle inference, you can configure a remote embedding endpoint. When set, `embed()` calls this HTTP endpoint instead of the local model.

```toml
[vector]
endpoint = "http://hub:8090/v1/embeddings"
```

This is useful for deployments where the edge device lacks the resources for local inference or where a shared embedding service is preferred.

## GraphRAG — Graph-Augmented Retrieval

GraphRAG elevates basic vector search by combining it with graph structure and community intelligence. Instead of returning isolated documents, it returns contextually rich results that include relationship paths, neighbor context, and community-level summaries.

### graphrag.search — Unified Retrieval

```sql
CALL graphrag.search('what sensors monitor the chilled water loop?', 10, 2, 'local')
YIELD node_id, score, source, context, depth
```

| Parameter | Type | Description |
|-----------|------|-------------|
| queryText | STRING | Natural language query |
| k | INT | Number of vector results (max 10,000) |
| maxHops | INT | BFS expansion depth (max 10) |
| mode | STRING | `"local"` (default), `"global"`, or `"hybrid"` |

**Yields:** `node_id` (INT), `score` (FLOAT), `source` (STRING), `context` (STRING), `depth` (INT)

### Search Modes

**Local mode** (default): Embeds the query → finds top-k similar nodes → expands each via BFS to discover graph neighbors → adds community context if available. Best for specific questions with known entity types.

**Global mode**: Searches over `__CommunitySummary` embedding vectors. Returns community-level profiles rather than individual nodes. Best for broad questions like "what systems are in this building?"

**Hybrid mode**: Runs both local and global, merges results with provenance tags. Best coverage at the cost of latency.

### Building Community Context

Community detection enables the global and hybrid search modes:

```sql
-- Step 1: Run Louvain community detection
CALL graph.buildCommunities(2)
YIELD community_id, node_count

-- Step 2: Add embeddings to community summaries
CALL graph.enrichCommunities()
YIELD enriched, skipped
```

Communities are stored as `__CommunitySummary` nodes with structural profiles (label distribution, key entities, node count). The `enrichCommunities` step generates vector embeddings from these profiles.

### MCP Tools for GraphRAG

The MCP layer exposes GraphRAG through dedicated tools:

- **`graphrag_search`** — Full GraphRAG retrieval with mode selection
- **`semantic_search`** — Simpler text-to-node search with containment paths
- **`similar_nodes`** — Find nodes similar to a reference node
- **`build_communities`** — Run Louvain detection
- **`enrich_communities`** — Add embeddings to community summaries

These tools handle embedding, search, traversal, and result formatting in a single call — agents never need to construct multi-step retrieval pipelines manually.
