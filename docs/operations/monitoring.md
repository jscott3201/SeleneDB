# Monitoring

Selene exposes Prometheus metrics, a health endpoint, and WebSocket change subscriptions for real-time observability.

## Prometheus Metrics

**Endpoint:** `GET /metrics`

Returns metrics in Prometheus text exposition format. A background task updates graph and system statistics every 10 seconds.

### Authentication

When `metrics_token` is set in the `[http]` config section (or via the `SELENE_METRICS_TOKEN` environment variable), the endpoint requires a matching `Authorization: Bearer <token>` header. The comparison uses constant-time byte equality to prevent timing side-channels.

Without a configured token (dev mode only), the endpoint is unauthenticated.

### Metrics Tiers

Selene exposes two tiers of metrics, selected by the runtime profile:

| Profile | Tier |
|---------|------|
| `edge` | Basic |
| `cloud` | Full |
| `standalone` | Full |

Override with the `metrics` field in `selene.toml` (`basic` or `full`).

### Basic Tier Metrics

These 12 metrics are always available:

| Metric | Type | Description |
|--------|------|-------------|
| `selene_query_total` | Counter (labels: `status`) | Total GQL queries executed, labeled `ok` or `error` |
| `selene_query_duration_seconds` | Histogram | GQL query duration (buckets: 1ms to 10s) |
| `selene_active_queries` | Gauge | Currently executing queries |
| `selene_graph_nodes` | Gauge | Total nodes in the graph |
| `selene_graph_edges` | Gauge | Total edges in the graph |
| `selene_graph_generation` | Gauge | Graph mutation generation counter |
| `selene_memory_used_bytes` | Gauge | Estimated graph memory usage |
| `selene_memory_budget_bytes` | Gauge | Configured memory budget |
| `selene_memory_pressure` | Gauge | 1 when memory usage exceeds the soft limit, 0 otherwise |
| `selene_wal_entries` | Gauge | Current WAL entry count |
| `selene_snapshot_duration_seconds` | Histogram | Snapshot write duration (buckets: 10ms to 5s) |
| `selene_snapshot_total` | Counter (labels: `status`) | Total snapshots taken, labeled `ok` or `error` |

### Full Tier Metrics

These 6 additional metrics are available at the full tier:

| Metric | Type | Description |
|--------|------|-------------|
| `selene_replica_lag_sequences` | Gauge | Replica lag in changelog sequences (0 = caught up) |
| `selene_changelog_buffer_entries` | Gauge | Current changelog buffer entry count |
| `selene_changelog_buffer_capacity` | Gauge | Configured changelog buffer capacity |
| `selene_auth_failures_total` | Counter | Total authentication failures |
| `selene_services_active` | Gauge (labels: `service`) | Active services (1 per registered service) |
| `selene_wal_bytes` | Gauge | WAL file size in bytes |

### Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: selene
    scrape_interval: 15s
    bearer_token: "<your-metrics-token>"
    static_configs:
      - targets: ["selene-host:8080"]
```

## Health Endpoint

**Endpoint:** `GET /health`

Returns server health status. The response varies based on authentication:

### Authenticated Response

When the caller provides a valid authentication token, the response includes full operational details:

```json
{
  "status": "ok",
  "node_count": 10500,
  "edge_count": 23000,
  "uptime_secs": 3600,
  "dev_mode": false,
  "role": "primary",
  "primary": null,
  "lag_sequences": null
}
```

For replicas, the `role` field is `"replica"`, `primary` contains the primary's address, and `lag_sequences` reports the replication lag.

### Unauthenticated Response

Unauthenticated callers (load balancers, Kubernetes probes) receive a minimal response:

```json
{
  "status": "ok",
  "uptime_secs": 3600
}
```

### Docker HEALTHCHECK

```yaml
healthcheck:
  test: ["/selene", "health", "--http"]
  interval: 15s
  timeout: 5s
  start_period: 10s
  retries: 3
```

### Kubernetes Probes

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 15

readinessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

## WebSocket Change Subscription

**Endpoint:** `GET /ws/subscribe`

Provides real-time graph change notifications over WebSocket. Clients connect, send a filter message, and receive JSON change events as mutations occur.

### Connection Flow

1. **Upgrade** -- the client sends a standard WebSocket upgrade request to `/ws/subscribe`. Authentication is required (same as other HTTP endpoints).
2. **Send filter** -- within 10 seconds of connecting, the client sends a JSON filter message. If no filter arrives in time, the server uses an empty filter (all changes).
3. **Receive events** -- the server pushes change events as they occur.

### Filter Message

The client sends a JSON object to select which changes to receive:

```json
{
  "labels": ["sensor", "equipment"],
  "edge_types": ["feeds", "contains"]
}
```

Both fields are optional. Omitting a field means no filtering for that dimension. The label filter applies to changes that carry label information (node deletions, label additions/removals, edge creations/deletions). Property changes pass through regardless of label filter because the change record does not carry the node's labels.

### Change Event Format

The server pushes events in this format:

```json
{
  "sequence": 42,
  "changes": [
    {
      "type": "node_created",
      "entity_id": 1001
    },
    {
      "type": "edge_created",
      "entity_id": 5001,
      "label": "feeds"
    },
    {
      "type": "node_deleted",
      "entity_id": 1002,
      "labels": ["sensor", "temperature"]
    }
  ]
}
```

### Change Types

| Type | Description | Extra Fields |
|------|-------------|-------------|
| `node_created` | A new node was created | -- |
| `node_deleted` | A node was deleted | `labels` (list of the node's labels) |
| `node_updated` | A node's properties or labels changed | `label` (if label add/remove) |
| `edge_created` | A new edge was created | `label` (edge type) |
| `edge_deleted` | An edge was deleted | `label` (edge type) |
| `edge_updated` | An edge's properties changed | -- |

### Limits

- Maximum **100** concurrent WebSocket connections. Connections beyond this limit receive `503 Service Unavailable`.
- Auth scope is refreshed every **60 seconds** to reflect containment hierarchy changes.
- If the subscriber lags behind the changelog broadcast buffer, events are skipped with a warning (no backpressure stall).

## Background Tasks

The server spawns several background tasks at startup. All tasks share `ServerState` via `Arc` and shut down gracefully on SIGINT or SIGTERM.

| Task | Interval | Condition | Description |
|------|----------|-----------|-------------|
| Snapshot | `snapshot_interval_secs` (default 300s) | Skipped on replicas | Writes a binary snapshot and truncates the WAL. Also triggers on WAL entry threshold (`snapshot_max_wal_entries`). |
| TS flush | `flush_interval_minutes` (default 5 min) | Always | Flushes expired hot tier data to Parquet. Evicts idle sensor buffers after `idle_eviction_hours`. |
| TS retention | 1 hour | Always | Deletes expired Parquet directories older than `medium_retention_days`. Runs the export pipeline before deletion if configured. |
| TS compaction | `compact_after_hours` | When `compact_after_hours > 0` | Merges small Parquet files in old date directories. |
| Metrics update | 10 seconds | Always | Updates Prometheus gauges (graph stats, memory, WAL). Prunes the auth rate limiter every ~60 seconds. |
| Search index | Changelog-driven | `search` feature enabled | Watches the changelog and incrementally updates tantivy full-text indexes. Commits every 1 second. |
| Auto-embed | Changelog-driven | `vector` feature + auto_embed rules | Watches for text property changes and generates vector embeddings. 200ms debounce. Content-hash deduplication skips unchanged text. |
| Stats collector | Changelog-driven | Always | Maintains per-label node and edge counts from changelog events. |
| Vector store | Changelog-driven | `vector` feature | Keeps the contiguous vector buffer in sync with graph changes. |
| Version pruning | `prune_interval_hours` (default 1h) | `temporal` feature | Prunes expired property versions older than `retention_days`. |

## Logging

Selene uses the `tracing` framework with `tracing-subscriber`. Log levels are controlled via the `RUST_LOG` environment variable.

### Filter Syntax

The `RUST_LOG` variable accepts comma-separated directives in the format `target=level`:

```bash
# Show info-level logs from selene-server (default)
RUST_LOG=selene_server=info

# Debug logging for both server and GQL engine
RUST_LOG=selene_server=debug,selene_gql=debug

# Trace-level for persistence, info for everything else
RUST_LOG=selene_server=info,selene_persist=trace

# Quiet mode -- warnings only
RUST_LOG=selene_server=warn
```

### Recommended Settings

| Environment | Setting |
|-------------|---------|
| Production | `RUST_LOG=selene_server=info` |
| Debugging queries | `RUST_LOG=selene_server=debug,selene_gql=debug` |
| Debugging persistence | `RUST_LOG=selene_server=info,selene_persist=debug` |
| Debugging replication | `RUST_LOG=selene_server=info,selene_client=debug` |
| Minimal output | `RUST_LOG=selene_server=warn` |
