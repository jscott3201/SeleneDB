# Selene v3 Benchmark Suite

## Running Benchmarks

```bash
# All benchmarks (safe defaults: 20 samples, 2s measurement, max 10K scale)
cargo bench --workspace

# Specific crate
cargo bench -p selene-gql

# Specific benchmark group
cargo bench -p selene-gql -- gql_predicates

# Quick mode (~45s, 2 scales)
SELENE_BENCH_PROFILE=quick cargo bench -p selene-gql

# Full scale (includes 50K) — needs 16+ GB RAM
SELENE_BENCH_LARGE=1 cargo bench --workspace

# Save baseline for comparison
cargo bench -- --save-baseline main

# Compare against baseline
cargo bench -- --baseline main
```

## Resource Limits

Benchmarks are configured for safe operation on constrained hardware (8 GB RAM):
- **20 samples** per benchmark (vs criterion default of 100)
- **500ms warm-up** (vs 3s default)
- **2s measurement** (vs 5s default)
- **Max 10K scale** by default (set `SELENE_BENCH_LARGE=1` for 50K)
- **4 parallel jobs** for release builds (via `.cargo/config.toml`)

## Benchmark Organization

| Crate | Benchmark | Groups | Benchmarks | What it measures |
|-------|-----------|--------|------------|-----------------|
| selene-gql | `gql_bench` | 13 | ~55 | GQL engine: parsing, pattern matching, pipeline, predicates, advanced pipeline, e2e, mutations, transactions, caching, options, subqueries, phase5 (cyclic/RPQ), vector search |
| selene-graph | `graph_bench` | 5 | 8 | Node/edge CRUD, index lookups, BFS traversal, concurrent reads, mutation commit/rollback |
| selene-algorithms | `algo_bench` | 5 | 21 | Projection build, structural (WCC/SCC/topo), pathfinding (Dijkstra/SSSP/APSP), centrality (PageRank/betweenness), community (Louvain/LP/triangles) |
| selene-query | `query_bench` | 1 | 15 | SQL full scan, filter pushdown, JSON access, schema tables, TS queries, Arrow IPC/JSON serialization, DSL |
| selene-persist | `persist_bench` | 2 | 7 | WAL append/batch/replay/size, snapshot write/read/size |
| selene-ts | `ts_bench` | 2 | 8 | Hot tier append/batch/range/snapshot/eviction, Parquet flush/read/size |
| selene-wire | `wire_bench` | 4 | 11 | Frame encode/decode, postcard/JSON serialization, zstd compression, datagram codec |

### GQL Benchmark Groups Detail

| Group | Benchmarks | Covers |
|-------|------------|--------|
| `gql_parse` | 10 | Simple/complex/var-length parsing, Phase 0 syntax (LIKE, BETWEEN, DIFFERENT EDGES, UNWIND, type DDL) |
| `gql_label_scan` | 2×scales | RoaringBitmap label scan (all sensors, all nodes) |
| `gql_expand` | 2×scales | Fixed-length edge traversal (1-hop, 2-hop) |
| `gql_var_expand` | 2×scales | Variable-length patterns (depth 1-3, TRAIL 1-5) |
| `gql_filter` | 2×scales | Property comparison (range, equality) with pushdown |
| `gql_sort` | 2×scales | ORDER BY full sort, ORDER BY + LIMIT (TopK pushdown) |
| `gql_aggregation` | 3×scales | COUNT, AVG, GROUP BY with LET binding |
| `gql_predicates` | 4×scales | LIKE (prefix, contains), NOT LIKE, BETWEEN |
| `gql_advanced_pipeline` | 4×scales | WITH clause, HAVING, UNWIND, NULLS FIRST |
| `gql_e2e` | 4×scales | Multi-stage realistic queries |
| `gql_mutations` | 4 | INSERT, INSERT+read, SET property, DETACH DELETE |
| `gql_transactions` | 2 | Multi-statement explicit transactions, rollback |
| `gql_plan_cache` | 2 | Cache miss (parse), cache hit |
| `gql_options` | 2 | Strict vs default coercion mode |
| `gql_subqueries` | 2×scales | COUNT correlated subquery, DIFFERENT EDGES match mode |
| `gql_phase5` | 4×scales | CycleJoin (HVAC), RPQ containment, RPQ alternation, COLLECT |
| `gql_vector` | 3×scales | cosine_similarity scalar, vectorSearch top-10, vectorSearch top-50 |

## Scale Variants

Every benchmark runs at multiple scales to reveal O(n) characteristics:

- **Graph/GQL:** 200, 1K, 10K (target node counts via reference building)
- **Vector:** 1K, 10K (dedicated vector graphs with 384-dim embeddings)
- **Time-series:** 100, 1K, 5K, 10K samples
- **Persistence:** 100, 1K, 10K entries
- **Wire:** 64B, 256B, 1KB, 4KB, 16KB messages

## Performance Targets

v3 targets (should match or exceed v2, which used RocksDB):

| Metric | v2 Actual | v3 Target | Rationale |
|--------|-----------|-----------|-----------|
| Node create | 5 us | <1 us | In-memory dense Vec insert |
| Node get by ID | 1 us | <100 ns | Dense Vec index lookup |
| Property update | 2 us | <500 ns | In-memory mutation |
| BFS depth=1 | 9 us | <5 us | Direct adjacency list |
| TS append | 226K/sec | >500K/sec | Ring buffer push_back |
| Recovery (10K nodes) | 4.1s | <50 ms | Binary snapshot recovery |
| GQL parse simple | — | <5 us | Pest PEG parser |
| GQL plan cache hit | — | <200 ns | Hash lookup + generation check |
| GQL label scan (10K) | — | <300 us | RoaringBitmap intersection |
| Vector search (10K, k=10) | — | <6 ms | Brute-force cosine scan |

## Conventions

- Each function tests ONE operation at ONE scale
- Use `criterion_group!` for related benchmarks
- Name format: `bench_{operation}` with `BenchmarkId::from_parameter(scale)` or `BenchmarkId::new(name, scale)`
- Use `Throughput::Elements` or `Throughput::Bytes` where applicable
- Use `std::hint::black_box()` to prevent dead code elimination
- Graph setup uses `build_scaled_graph(target_nodes)` from `selene_testing`
- Vector setup uses `build_vector_graph(n)` defined in `gql_bench.rs`
- Profiles: `SELENE_BENCH_PROFILE=quick|full|stress`
