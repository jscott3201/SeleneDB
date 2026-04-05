# Time-Series Guide

Selene includes a multi-tier time-series engine designed for IoT sensor data. Each tier serves a different access pattern and retention window:

- **Hot tier** -- in-memory compressed blocks. Sub-microsecond reads for recent data (typically the last 24 hours).
- **Warm tier** -- pre-computed minute and hourly aggregates with quantile snapshots. Efficient for dashboards and trend analysis.
- **Cold tier** -- Parquet files on local disk (zstd compression, bloom filters, row-group pushdown). Weeks to months of history.
- **Cloud tier** -- offload to S3, GCS, Azure Blob, or MinIO via the `ObjectStoreExporter` (requires `--features cloud-storage`).

Samples flow automatically from hot to warm as windows finalize, and from hot to cold via background flush tasks. Cloud offload runs on a configurable schedule.

## Writing Samples

### HTTP API

Send samples via `POST /ts/write`. Each sample identifies an entity (by node ID), a property name, a nanosecond-precision Unix timestamp, and a float value.

```bash
curl -X POST http://localhost:8080/ts/write \
  -H "Content-Type: application/json" \
  -d '{
    "samples": [
      {
        "entity_id": 42,
        "property": "temperature",
        "timestamp_nanos": 1711900800000000000,
        "value": 72.3
      },
      {
        "entity_id": 42,
        "property": "humidity",
        "timestamp_nanos": 1711900800000000000,
        "value": 45.1
      }
    ]
  }'
```

The response includes the number of samples written:

```json
{ "written": 2 }
```

`timestamp_nanos` is nanoseconds since the Unix epoch (1970-01-01T00:00:00Z). To convert from seconds, multiply by 1,000,000,000. To convert from milliseconds, multiply by 1,000,000.

### Batch Writes

Multiple samples for different entities and properties can be included in a single request. The server batches writes internally for throughput.

## Querying Samples via GQL

All time-series queries use the `CALL` procedure syntax. Durations are specified as nanosecond integers (for example, 3600000000000 for one hour).

### ts.range -- Raw Samples

Returns individual samples from the hot tier within an absolute time range.

```sql
CALL ts.range(42, 'temperature', 1711900000000000000, 1711903600000000000)
YIELD timestamp, value
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name (e.g. 'temperature') |
| start | INT/DATETIME | Range start (nanos since epoch) |
| end | INT/DATETIME | Range end (nanos since epoch) |

**Yields:** `timestamp` (DATETIME), `value` (FLOAT)

### ts.latest -- Most Recent Sample

Returns the single most recent sample for an entity-property pair. Useful for current-state dashboards.

```sql
CALL ts.latest(42, 'temperature') YIELD timestamp, value
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |

**Yields:** `timestamp` (DATETIME), `value` (FLOAT)

Returns zero rows if no data exists for the given entity and property.

## Aggregation

### ts.aggregate -- Scalar Aggregate

Computes a single aggregate value over a rolling duration window ending at the current time.

```sql
CALL ts.aggregate(42, 'temperature', 3600000000000, 'avg')
YIELD value
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |
| duration | INT/DATETIME | Lookback window in nanoseconds |
| agg_fn | STRING | Aggregation function |

Supported aggregation functions: `avg`, `sum`, `min`, `max`, `count`.

**Yields:** `value` (FLOAT)

### ts.window -- Tumbling Window Aggregation

Divides the lookback duration into fixed-size tumbling windows and returns one row per window.

```sql
CALL ts.window(42, 'temperature', 300000000000, 'avg', 3600000000000)
YIELD window_start, window_end, value
```

This example computes 5-minute averages over the last hour.

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |
| window_size | INT/DATETIME | Window width in nanoseconds |
| agg_fn | STRING | Aggregation function (avg, sum, min, max, count) |
| duration | INT/DATETIME | Total lookback window in nanoseconds |

**Yields:** `window_start` (DATETIME), `window_end` (DATETIME), `value` (FLOAT)

## Warm Tier Procedures

The warm tier stores pre-computed minute-level aggregates. As hot-tier samples cross minute boundaries (default: 60-second tumbling windows), the current window is finalized and its statistics are rolled up into warm-tier aggregates. Warm-tier queries are faster than raw scans because they read pre-aggregated data.

### ts.downsample -- Minute Aggregates

Returns one row per warm-tier window with pre-computed min, max, avg, and count.

```sql
CALL ts.downsample(42, 'temperature', 1711900000000000000, 1711903600000000000)
YIELD window_start, min, max, avg, count
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |
| start | INT/DATETIME | Range start (nanos since epoch) |
| end | INT/DATETIME | Range end (nanos since epoch) |

**Yields:** `window_start` (DATETIME), `min` (FLOAT), `max` (FLOAT), `avg` (FLOAT), `count` (INT)

### ts.trends -- Hourly Aggregates

Returns hourly aggregates from the hierarchical warm tier. Designed for month-scale dashboards where minute-level resolution is too fine.

```sql
CALL ts.trends(42, 'temperature', 1709308800000000000, 1711900800000000000)
YIELD window_start, min, max, avg, count
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |
| start | INT/DATETIME | Range start (nanos since epoch) |
| end | INT/DATETIME | Range end (nanos since epoch) |

**Yields:** `window_start` (DATETIME), `min` (FLOAT), `max` (FLOAT), `avg` (FLOAT), `count` (INT)

### ts.percentile -- Quantile Estimation

Returns a percentile value computed from warm-tier DDSketch accumulators. Pre-computed quantile points (p50, p90, p95, p99) are snapshotted per window. The procedure returns the closest pre-computed quantile, weighted by sample count across all windows in the requested duration.

```sql
CALL ts.percentile(42, 'temperature', 0.95, 3600000000000)
YIELD value
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |
| quantile | FLOAT | Target quantile (0.0 to 1.0, e.g. 0.95 for p95) |
| duration | INT/DATETIME | Lookback window in nanoseconds |

**Yields:** `value` (FLOAT)

Common quantile values: 0.50 (median), 0.90 (p90), 0.95 (p95), 0.99 (p99).

## Cold Tier

### ts.history -- Parquet-Backed Historical Data

Returns samples from the cold tier (Parquet files on local disk). Cold-tier data has been flushed from the hot tier and stored with zstd compression, bloom filters, and row-group pushdown for efficient range scans.

```sql
CALL ts.history(42, 'temperature', 1709308800000000000, 1711900800000000000)
YIELD timestamp, value
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |
| start | INT/DATETIME | Range start (nanos since epoch) |
| end | INT/DATETIME | Range end (nanos since epoch) |

**Yields:** `timestamp` (DATETIME), `value` (FLOAT)

## Cross-Tier Queries

### ts.fullRange -- Merged Hot + Cold

Queries both the hot tier and the cold tier, merges results by timestamp, and deduplicates. When the same timestamp exists in both tiers, the hot-tier value takes precedence.

```sql
CALL ts.fullRange(42, 'temperature', 1709308800000000000, 1711900800000000000)
YIELD timestamp, value
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |
| start | INT/DATETIME | Range start (nanos since epoch) |
| end | INT/DATETIME | Range end (nanos since epoch) |

**Yields:** `timestamp` (DATETIME), `value` (FLOAT)

This is the recommended procedure for queries that span both recent and historical data.

## Anomaly Detection

### ts.anomalies -- Z-Score Anomalies

Compares recent hot-tier samples against warm-tier mean and standard deviation. Returns samples whose absolute Z-score exceeds the given threshold.

```sql
CALL ts.anomalies(42, 'temperature', 2.0, 3600000000000)
YIELD timestamp, value, z_score
```

| Parameter | Type | Description |
|-----------|------|-------------|
| entity_id | INT | Node ID of the entity |
| property | STRING | Property name |
| threshold | FLOAT | Z-score threshold (must be positive, e.g. 2.0) |
| duration | INT/DATETIME | Lookback window in nanoseconds |

**Yields:** `timestamp` (DATETIME), `value` (FLOAT), `z_score` (FLOAT)

A threshold of 2.0 flags readings more than two standard deviations from the mean. For stricter detection, use 3.0.

### ts.peerAnomalies -- Peer Comparison

Compares a node's latest reading against its graph neighborhood. BFS from the target node collects the latest value for each peer with the same property, then flags those that deviate beyond the threshold.

```sql
CALL ts.peerAnomalies(42, 'temperature', 2, 1.5)
YIELD nodeId, value, z_score
```

For example, if four temperature sensors serve the same AHU, `peerAnomalies` identifies any sensor whose current reading is significantly different from its peers.

| Parameter | Type | Description |
|-----------|------|-------------|
| nodeId | INT | Starting node for BFS |
| property | STRING | Property name to compare |
| maxHops | INT | BFS depth limit |
| threshold | FLOAT | Z-score threshold (must be positive) |

**Yields:** `nodeId` (INT), `value` (FLOAT), `z_score` (FLOAT)

Results are sorted by absolute Z-score in descending order. Requires at least two peers with data to produce results.

## Scoped Aggregation

### ts.scopedAggregate -- Graph-Aware Aggregation

Traverses the graph from a root node via BFS, collects all descendant nodes, and aggregates their time-series data for a given property and duration. Useful for building-level or floor-level rollups.

```sql
CALL ts.scopedAggregate(1, 3, 'temperature', 'avg', 3600000000000)
YIELD value, nodeCount, sampleCount
```

This example computes the average temperature across all nodes within 3 hops of node 1 over the last hour.

| Parameter | Type | Description |
|-----------|------|-------------|
| rootNodeId | INT | Root node for BFS traversal |
| maxHops | INT | BFS depth limit |
| property | STRING | Property name to aggregate |
| aggFn | STRING | Aggregation function (avg, sum, min, max, count) |
| duration | INT/DATETIME | Lookback window in nanoseconds |

**Yields:** `value` (FLOAT), `nodeCount` (INT), `sampleCount` (INT)

`nodeCount` reports how many descendant nodes contributed data. `sampleCount` reports the total number of individual samples aggregated.

## HTTP Query Endpoint

The HTTP API provides a direct query endpoint for time-series data:

```
GET /ts/:entity_id/:property?start=<nanos>&end=<nanos>&limit=<n>
```

Both `start` and `end` are optional (defaults: 0 and MAX). `limit` caps the number of returned samples.

```bash
curl "http://localhost:8080/ts/42/temperature?start=1711900000000000000&end=1711903600000000000"
```

## Tier Summary

| Tier | Storage | Retention | Query Procedure | Resolution |
|------|---------|-----------|-----------------|------------|
| Hot | In-memory (Gorilla/RLE/Dictionary) | ~24 hours | ts.range, ts.latest, ts.aggregate, ts.window | Individual samples |
| Warm | In-memory aggregates | Configurable (hours) | ts.downsample, ts.trends, ts.percentile | Minute / hourly |
| Cold | Parquet on disk | Weeks to months | ts.history | Individual samples |
| Cloud | S3 / GCS / Azure / MinIO | Indefinite | (offloaded cold files) | Individual samples |
| Cross-tier | Hot + Cold | Full range | ts.fullRange | Individual samples |
