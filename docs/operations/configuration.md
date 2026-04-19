# Configuration

SeleneDB uses a layered configuration system. Every setting has a sensible default, so the server starts with zero configuration in dev mode and requires only TLS paths for production.

## Configuration priority

Settings are resolved in the following order (highest priority first):

1. **CLI flags** -- `--dev`, `--profile`, `--quic-listen`, `--http-listen`, `--data-dir`
2. **Environment variables** -- `SELENE_*` prefixed variables
3. **TOML config file** -- loaded via `--config path/to/selene.toml`
4. **Profile defaults** -- the active profile (`edge`, `cloud`, or `standalone`) sets defaults for services, memory, and metrics
5. **Built-in defaults** -- hardcoded fallback values

## Config file

Pass a TOML config file at startup:

```bash
selene-server --config /etc/selene/selene.toml /data
```

When no `--config` flag is provided, SeleneDB uses built-in defaults for all settings. The data directory defaults to `/tmp/selene-data` (a warning is logged if this path is used in production mode).

## Runtime profiles

Profiles control default service activation, memory budgets, and metrics granularity. Set the profile via CLI (`--profile`), environment variable (`SELENE_PROFILE`), or TOML (`profile = "edge"`).

| Setting | Edge (default) | Cloud | Standalone |
|---------|---------------|-------|------------|
| **vector** | off | on | on |
| **search** | on | on | on |
| **temporal** | on | on | on |
| **federation** | off | on | off |
| **algorithms** | on | on | on |
| **mcp** | off | off | on |
| **replicas** | off | on | off |
| **memory budget** | 2048 MB | 16384 MB | 4096 MB |
| **metrics tier** | basic | full | full |

The **edge** profile targets constrained devices (RPi 5, 4 GB+). It disables vector embeddings, federation, MCP, and replicas to minimize resource usage.

The **cloud** profile enables federation, replicas, and vector embeddings for a hub node that coordinates multiple edge instances.

The **standalone** profile enables MCP and vector embeddings for single-node development and testing. It disables federation and replicas since there is no mesh to join.

Profile defaults can be overridden individually via TOML or environment variables. For example, running the edge profile with vector embeddings enabled:

```toml
profile = "edge"

[services]
vector = { enabled = true }
```

## TOML sections

### Top-level settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `profile` | string | `"edge"` | Runtime profile: `edge`, `cloud`, or `standalone` |
| `listen_addr` | string | `"0.0.0.0:4510"` | QUIC listener address (UDP) |
| `data_dir` | string | `"/tmp/selene-data"` | Directory for WAL, snapshots, Parquet, and vault files |
| `dev_mode` | bool | `false` | Enable dev mode (self-signed TLS, no Cedar auth) |
| `changelog_capacity` | integer | `100000` | Maximum entries in the in-memory changelog ring buffer |
| `quic_max_connections` | integer | `64` | Maximum concurrent QUIC connections |

### [tls]

TLS configuration for the QUIC listener. Required in production mode; omitted in dev mode (self-signed certs are generated automatically).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `cert_path` | string | (required) | Path to PEM-encoded server certificate chain |
| `key_path` | string | (required) | Path to PEM-encoded server private key |
| `ca_cert_path` | string | (none) | Path to PEM-encoded CA certificate for mTLS client verification |

### [http]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable the HTTP listener |
| `listen_addr` | string | `"0.0.0.0:8080"` | HTTP listen address (TCP) |
| `cors_origins` | list of strings | `[]` | Allowed CORS origins; empty list denies all cross-origin requests |
| `metrics_token` | string | (none) | Bearer token for the `/metrics` endpoint; `None` allows unauthenticated access (dev only) |
| `allow_plaintext` | bool | `false` | Acknowledge plaintext HTTP in production; set to `true` only behind a TLS-terminating reverse proxy |

### [mcp]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable the MCP endpoint (requires HTTP) |
| `api_key` | string | (none) | Static API key for production MCP auth; simpler alternative to OAuth. Generate with `openssl rand -base64 32` |
| `signing_key` | string | (none) | Base64-encoded 32-byte key for signing OAuth JWT tokens. When set, enables the OAuth 2.1 endpoints (`/oauth/register`, `/oauth/authorize`, `/oauth/token`). Generate with `openssl rand -base64 32` |
| `require_approval` | bool | `false` | When `true`, dynamically registered OAuth clients must be approved by an administrator before they can obtain tokens |
| `access_token_ttl_secs` | integer | `3600` | OAuth access token lifetime in seconds (default: 1 hour) |
| `refresh_token_ttl_secs` | integer | `604800` | OAuth refresh token lifetime in seconds (default: 7 days) |

### [performance]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rayon_threads` | integer | `0` | Number of Rayon threads; `0` auto-detects from available cores |
| `query_timeout_ms` | integer | `30000` | Query timeout in milliseconds |
| `max_concurrent_queries` | integer | `64` | Maximum concurrent GQL queries |
| `wal_commit_delay_ms` | integer | `0` | WAL group commit delay in milliseconds; `0` flushes immediately. When greater than zero, mutations are batched for up to this many ms before flushing, reducing fsync calls. Recommended: edge=0 (reliability), gateway=2, cloud=5 |

### [vault]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable the encrypted secure vault |
| `master_key_file` | string | (none) | Path to master key file (base64 or hex encoded, 32 bytes) |
| `vault_path` | string | `{data_dir}/secure.vault` | Vault file path |

### [vector]

SeleneDB is BYO-vector — applications embed text in their own process and
pass pre-computed vectors as GQL parameters. The `[vector]` section tunes
the HNSW index that sits over those stored vectors.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `hnsw_m` | integer | `16` | Max HNSW connections per node per layer |
| `hnsw_m0` | integer | `2*M` | Max HNSW connections at layer 0 |
| `hnsw_ef_construction` | integer | `200` | HNSW build search width |
| `hnsw_ef_search` | integer | `50` | Default HNSW query search width |
| `hnsw_quantize` | bool | `false` | Enable PolarQuant vector quantization |
| `hnsw_quantize_bits` | integer | `4` | Quantization bit width (3, 4, or 8) |
| `hnsw_quantize_rescore` | bool | `false` | Re-rank top-k with exact f32 cosine after quantized search |

### [temporal]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable property version tracking |
| `retention_days` | integer | `90` | Maximum age of archived versions in days |
| `prune_interval_hours` | integer | `1` | How often to prune expired versions (hours) |

### [rdf]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `namespace` | string | `"selene:"` | Base namespace URI for minting RDF URIs from graph entities |
| `materialize_observations` | bool | `false` | Materialize SOSA Observation instances for time-series data |
| `observation_debounce_ms` | integer | `1000` | Debounce interval in ms for observation materialization |

### [persist]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sync_policy` | string or table | `{ periodic = { interval_ms = 100 } }` | WAL sync-to-disk strategy (see below) |
| `snapshot_interval_secs` | integer | `300` | Seconds between automatic snapshot writes (5 minutes) |
| `snapshot_max_wal_entries` | integer | `10000` | Maximum WAL entries before forcing a snapshot |
| `max_snapshots` | integer | `3` | Maximum snapshot files to retain on disk |
| `fsync_parent_dir` | bool | `true` | Fsync the parent directory after snapshot rename; safe to disable on ext4/APFS but keep enabled on power-loss-prone systems |

**Sync policy options:**

| Policy | TOML value | Behavior |
|--------|-----------|----------|
| Every entry | `"every_entry"` | Fsync after every WAL entry. Strongest durability, highest latency |
| Periodic | `{ periodic = { interval_ms = 100 } }` | Fsync at a fixed interval. Small loss window on crash |
| On snapshot | `"on_snapshot"` | Fsync only on snapshot. Fastest, but loses all WAL since last snapshot on crash |

### [ts]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `hot_retention_hours` | integer | `24` | Hours of hot data to keep in memory |
| `medium_retention_days` | integer | `7` | Days of cold data to keep on disk as Parquet |
| `flush_interval_minutes` | integer | `15` | Minutes between flush cycles |
| `max_samples_per_buffer` | integer | `86400` | Hard cap on samples per buffer (1/sec for 24h); set to `0` to disable |
| `idle_eviction_hours` | integer | `48` | Hours of inactivity before evicting a buffer key; `0` disables |
| `hot_memory_budget_mb` | integer | `256` | Total memory budget for the hot tier in megabytes |
| `min_samples_per_buffer` | integer | `60` | Minimum samples to keep per buffer even under memory pressure |
| `flush_pressure_threshold` | float | `0.8` | Memory pressure ratio (0.0--1.0) that triggers early eviction |
| `out_of_order_tolerance_nanos` | integer | `5000000000` | Tolerance for out-of-order samples in nanoseconds (default: 5 seconds) |
| `compact_after_hours` | integer | `24` | Hours before a date directory is eligible for compaction |
| `gorilla_window_minutes` | integer | `30` | Block compression window in minutes; samples in the same window collect in a raw Vec, and a new window seals the active block into a compressed TsBlock |

### [ts.warm_tier]

Optional. When present, enables the warm tier for downsampled aggregate storage.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `downsample_interval_secs` | integer | `60` | Downsample window interval in seconds |
| `retention_hours` | integer | `24` | How long to keep warm tier data in hours |
| `ddsketch_enabled` | bool | `true` | Allocate DDSketch accumulators for streaming quantile estimation (p50/p90/p95/p99); disable to save memory on constrained devices |

### [ts.warm_tier.hourly]

Optional. Provides month-scale hourly aggregates for dashboard queries.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable the hourly warm tier level |
| `retention_days` | integer | `30` | How long to keep hourly aggregates in days |

### [ts.cloud]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | (none) | Cloud storage destination URL (e.g., `s3://bucket/prefix/`). Supports `s3://`, `gs://`, `az://` schemes via the `object_store` crate. `None` disables cloud export. Requires the `cloud-storage` feature flag |
| `node_id` | string | (hostname) | Node identifier for Hive-style partitioning (`node={node_id}/date=.../file.parquet`) |

### [services]

Per-service activation toggles. Profile sets defaults; explicit TOML or environment overrides take precedence.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `vector` | `{ enabled = bool }` | profile-dependent | Vector embedding service |
| `search` | `{ enabled = bool }` | profile-dependent | Full-text search (tantivy) |
| `temporal` | `{ enabled = bool }` | profile-dependent | Property version tracking |
| `federation` | `{ enabled = bool }` | profile-dependent | Federation mesh |
| `algorithms` | `{ enabled = bool }` | profile-dependent | Graph algorithm CALL procedures |
| `mcp` | `{ enabled = bool }` | profile-dependent | Model Context Protocol endpoint |
| `replicas` | `{ enabled = bool }` | profile-dependent | CDC replica support |

### [memory]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `budget_mb` | integer | profile-dependent | Memory budget in megabytes (edge: 2048, cloud: 16384, standalone: 4096) |
| `soft_limit_percent` | integer | `80` | Soft limit as percentage of budget; crossing this threshold triggers warnings and background throttling |

### [node_tls]

TLS for inter-node communication (replicas, federation).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ca_cert` | string | (none) | CA certificate for verifying peer identity |
| `cert` | string | (none) | Local node certificate for mTLS |
| `key` | string | (none) | Local node private key |

### [replica]

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `auth_identity` | string | (none) | Identity string for authenticating to the primary |
| `auth_credentials` | string | (none) | Credential for authenticating to the primary |
| `server_name` | string | (none) | TLS server name override for the primary |

### [federation]

Requires the `federation` feature flag at compile time. Note: federation settings currently use runtime defaults based on the profile. The `[federation]` TOML section is accepted but fields are not yet wired into config loading -- configure federation via `[services] federation.enabled` and environment variables for now.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable federation |
| `node_name` | string | `"selene"` | This node's name in the federation mesh |
| `role` | string | `"building"` | Topology role hint: `aggregator`, `building`, or `device` |
| `bootstrap_peers` | list of strings | `[]` | Peer addresses to connect to on startup |
| `peer_ttl_secs` | integer | `300` | Peer registry entry TTL in seconds |
| `refresh_interval_secs` | integer | `60` | How often to re-pull peer directories in seconds |

### metrics

Top-level field (not a section). Controls metrics collection granularity.

| Value | Description |
|-------|-------------|
| `"basic"` | Lightweight counters (default for edge profile) |
| `"full"` | Full histograms and detailed metrics (default for cloud and standalone profiles) |

## Environment variable reference

All environment variables use the `SELENE_` prefix. Boolean values accept `1`, `true`, or `yes` (case-sensitive).

| Variable | Maps to | Type |
|----------|---------|------|
| `SELENE_PROFILE` | `profile` | string |
| `SELENE_DATA_DIR` | `data_dir` | string |
| `SELENE_DEV_MODE` | `dev_mode` | bool |
| `SELENE_QUIC_LISTEN` | `listen_addr` | string |
| `SELENE_HTTP_ENABLED` | `http.enabled` | bool |
| `SELENE_HTTP_LISTEN` | `http.listen_addr` | string |
| `SELENE_MCP_ENABLED` | `mcp.enabled` | bool |
| `SELENE_METRICS_TOKEN` | `http.metrics_token` | string |
| `SELENE_VAULT_ENABLED` | `vault.enabled` | bool |
| `SELENE_VAULT_KEY_FILE` | `vault.master_key_file` | string |
| `SELENE_VAULT_PASSPHRASE` | (passphrase-derived vault key) | string |
| `SELENE_MEMORY_BUDGET_MB` | `memory.budget_mb` | integer |
| `SELENE_SERVICES_VECTOR_ENABLED` | `services.vector.enabled` | bool |
| `SELENE_SERVICES_SEARCH_ENABLED` | `services.search.enabled` | bool |
| `SELENE_SERVICES_TEMPORAL_ENABLED` | `services.temporal.enabled` | bool |
| `SELENE_SERVICES_FEDERATION_ENABLED` | `services.federation.enabled` | bool |
| `SELENE_SERVICES_ALGORITHMS_ENABLED` | `services.algorithms.enabled` | bool |
| `SELENE_SERVICES_MCP_ENABLED` | `services.mcp.enabled` | bool |
| `SELENE_SERVICES_REPLICAS_ENABLED` | `services.replicas.enabled` | bool |
| `RUST_LOG` | (tracing filter) | string |

The `SELENE_VAULT_PASSPHRASE` variable is handled specially: it is read and cleared from the process environment before any threads are spawned, preventing it from leaking via `/proc/PID/environ`.

## Example configs

### Minimal edge deployment

```toml
data_dir = "/data/selene"

[tls]
cert_path = "/etc/selene/certs/server.crt"
key_path = "/etc/selene/certs/server.key"
```

All other settings use edge profile defaults. This is the minimum needed for a production edge node.

### Production cloud hub

```toml
profile = "cloud"
data_dir = "/var/lib/selene"

[tls]
cert_path = "/etc/selene/certs/server.crt"
key_path = "/etc/selene/certs/server.key"
ca_cert_path = "/etc/selene/certs/ca.crt"

[http]
cors_origins = ["https://dashboard.example.com"]
metrics_token = "metrics-secret-token"

[performance]
query_timeout_ms = 60000
max_concurrent_queries = 128
wal_commit_delay_ms = 5

[vault]
enabled = true
master_key_file = "/run/secrets/selene-vault-key"

[persist]
sync_policy = { periodic = { interval_ms = 50 } }
snapshot_interval_secs = 120
max_snapshots = 5

[memory]
budget_mb = 32768

[ts]
hot_retention_hours = 48
medium_retention_days = 30

[ts.warm_tier]
downsample_interval_secs = 60
retention_hours = 168

[ts.warm_tier.hourly]
enabled = true
retention_days = 90

[ts.cloud]
url = "s3://selene-cold-data/ts/"
node_id = "cloud-hub-01"

[federation]
enabled = true
node_name = "cloud-hub"
role = "aggregator"
bootstrap_peers = ["building-a:4510", "building-b:4510"]
```

### Standalone development

```toml
profile = "standalone"
dev_mode = true
data_dir = "/tmp/selene-dev"

[performance]
query_timeout_ms = 10000

[vector]
hnsw_ef_search = 100
```

This enables all services (vector, search, temporal, MCP, algorithms) with dev mode authentication (no credentials required).
