# Deployment

SeleneDB ships as a single statically-linked binary. It runs on bare metal, in Docker containers, or as an embedded library. This guide covers all deployment modes from development to production.

## Docker single-node (production)

The production compose file includes security hardening:

```yaml
services:
  selene:
    build: .
    ports:
      - "4510:4510/udp"   # QUIC
      - "8080:8080"        # HTTP
    volumes:
      - selene-data:/data
    environment:
      - RUST_LOG=selene_server=info
    restart: unless-stopped
    read_only: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    healthcheck:
      test: ["/selene", "health", "--http"]
      interval: 30s
      timeout: 5s
      start_period: 10s
      retries: 3

volumes:
  selene-data:
```

Key hardening measures:

- **read_only** -- the root filesystem is read-only; only the `/data` volume is writable
- **cap_drop: ALL** -- drops all Linux capabilities
- **no-new-privileges** -- prevents privilege escalation via setuid/setgid
- **nonroot user** -- the distroless base image runs as UID 65532

Start the production container:

```bash
docker compose up -d
```

Override profile and service toggles through environment variables:

```yaml
environment:
  - SELENE_PROFILE=cloud
  - SELENE_SERVICES_VECTOR_ENABLED=true
  - SELENE_MEMORY_BUDGET_MB=8192
```

## Docker dev mode

The dev compose file enables debug logging, demo data seeding, and the standalone profile:

```yaml
services:
  selene:
    build: .
    command: ["--dev", "--seed", "/data"]
    ports:
      - "4510:4510/udp"
      - "8080:8080"
    volumes:
      - selene-data:/data
    environment:
      - RUST_LOG=selene_server=debug
      - SELENE_PROFILE=standalone
      - SELENE_SERVICES_MCP_ENABLED=true
    healthcheck:
      test: ["/selene", "health", "--http"]
      interval: 10s
      timeout: 3s
      start_period: 5s
      retries: 3
```

Start with:

```bash
docker compose -f docker-compose.dev.yml up -d
```

The `--seed` flag populates an empty graph with a demo building hierarchy (site, building, floors, zones, equipment, sensors) and 180 time-series samples.

## Building from source

SeleneDB requires Rust 1.94+ and has zero C/C++ dependencies:

```bash
# Standard build (all product features compiled unconditionally)
cargo build --release -p selene-server --features dev-tls

# With GPU acceleration (NVIDIA CUDA)
cargo build --release -p selene-server --features cuda,dev-tls

# With GPU acceleration (Apple Metal)
cargo build --release -p selene-server --features metal,dev-tls

# CLI tool (built separately)
cargo build --release -p selene-cli
```

**Compile-time feature flags:**

| Flag | Purpose |
|------|---------|
| `dev-tls` | Self-signed TLS certificates for development (rcgen) |
| `cuda` | NVIDIA GPU-accelerated embeddings (candle CUDA kernels) |
| `metal` | Apple Silicon GPU-accelerated embeddings (candle Metal) |
| `bench` | Criterion benchmarks |
| `insecure` | Client TLS bypass (testing only) |

All product features — HTTP, MCP, QUIC, vector search, semantic search, GraphRAG, time-series, federation, RDF/SPARQL, agent memory, cloud storage — are always compiled. Enable or disable at runtime via `ServicesConfig` profiles (Edge/Cloud/Standalone) or environment variables.

## Multi-architecture Docker builds

Build for both amd64 and arm64 using Docker BuildKit:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t selene:latest .
```

The Dockerfile uses `rust:alpine` for musl-static builds, producing a binary with zero runtime dependencies.

## Dockerfile anatomy

The build uses a two-stage Dockerfile:

**Stage 1 (builder):** The `rust:alpine` base image compiles a fully static musl binary. BuildKit cache mounts persist the cargo registry and target directory across builds. Server and CLI are built as separate packages because `--features` applies per-package.

**Stage 2 (runtime):** The `gcr.io/distroless/static:nonroot` base image provides CA certificates, timezone data, and a nonroot user (UID 65532). There is no shell and no package manager, reducing the CVE attack surface to near zero.

The resulting image is approximately 14 MB compressed.

**Ports exposed:**

| Port | Protocol | Service |
|------|----------|---------|
| 4510 | UDP | QUIC |
| 8080 | TCP | HTTP |

## Primary/replica deployment

The replica compose file deploys a primary with a read-only replica:

```yaml
services:
  primary:
    build: .
    command: ["--dev", "--seed", "/data"]
    ports:
      - "4510:4510/udp"
      - "8080:8080"
    volumes:
      - primary-data:/data
    environment:
      - RUST_LOG=selene_server=info
      - SELENE_PROFILE=cloud
    healthcheck:
      test: ["/selene", "health", "--http"]
      interval: 15s
      timeout: 5s
      start_period: 10s
      retries: 3

  replica:
    build: .
    command: ["--dev", "--replica-of", "primary:4510", "/data"]
    ports:
      - "4511:4510/udp"
      - "8081:8080"
    volumes:
      - replica-data:/data
    environment:
      - RUST_LOG=selene_server=info
      - SELENE_PROFILE=edge
    depends_on:
      primary:
        condition: service_healthy
    healthcheck:
      test: ["/selene", "health", "--http"]
      interval: 15s
      timeout: 5s
      start_period: 15s
      retries: 3
```

The replica uses `--replica-of primary:4510` to connect to the primary's QUIC port. It receives a full graph snapshot followed by a live CDC stream. Mutations on the replica are disabled.

In production, remove `--dev` and mount TLS certificates on both primary and replica. Configure `[replica]` and `[node_tls]` sections for authenticated inter-node communication.

## Resource sizing

### RPi 5 / edge (4 GB RAM)

- Profile: `edge`
- Memory budget: 2048 MB (default)
- WAL commit delay: 0 ms (immediate flush for reliability)
- Hot TS retention: 24 hours
- Services: search, temporal, algorithms enabled; vector, federation, MCP, replicas disabled

### Cloud hub (16+ GB RAM)

- Profile: `cloud`
- Memory budget: 16384 MB (default) or higher
- WAL commit delay: 5 ms (batched for throughput)
- Hot TS retention: 48+ hours
- Services: all enabled including federation, replicas, vector

## Server CLI reference

```
selene-server [OPTIONS] [DATA_DIR]
```

| Flag | Description |
|------|-------------|
| `--config <path>` | Path to TOML config file |
| `--dev` | Enable dev mode (self-signed TLS, no authentication) |
| `--seed` | Seed demo data on startup if the graph is empty |
| `--profile <name>` | Runtime profile: `edge`, `cloud`, or `standalone`. Overrides `SELENE_PROFILE` and TOML |
| `--replica-of <host:port>` | Start as a read-only replica of the given primary |
| `--show-config` | Print effective configuration and exit |
| `--quic-listen <addr>` | QUIC listen address override (e.g., `0.0.0.0:4510`) |
| `--http-listen <addr>` | HTTP listen address override (e.g., `0.0.0.0:8080`) |
| `--data-dir <path>` | Data directory override |

The positional argument is the data directory (equivalent to `--data-dir`).

## Health checks

SeleneDB exposes a health endpoint at `GET /health` on the HTTP listener. The CLI tool wraps this for Docker and Kubernetes probes:

```bash
/selene health --http
```

**Docker healthcheck:**

```yaml
healthcheck:
  test: ["/selene", "health", "--http"]
  interval: 30s
  timeout: 5s
  start_period: 10s
  retries: 3
```

**Kubernetes liveness probe:**

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30
```

**Kubernetes readiness probe:**

```yaml
readinessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Graceful shutdown

SeleneDB handles SIGINT and SIGTERM for graceful shutdown. The shutdown sequence:

1. Cancel background tasks (snapshot, TS flush, retention, compaction)
2. Write a final snapshot to disk
3. Wait for background tasks to complete
4. Exit (listeners are dropped implicitly)

Docker's default stop timeout (10 seconds) is sufficient for most deployments. For large graphs, consider increasing it:

```yaml
stop_grace_period: 30s
```

## Production checklist

- [ ] TLS certificates configured (`[tls]` section or mounted into container)
- [ ] `dev_mode = false` (the default; do not set `--dev`)
- [ ] Data directory on persistent storage (not `/tmp`)
- [ ] Memory budget sized for expected graph + time-series volume
- [ ] Metrics token set for `/metrics` endpoint (`[http] metrics_token` or `SELENE_METRICS_TOKEN`)
- [ ] CORS origins restricted to known dashboards
- [ ] Vault enabled with a securely stored master key (not a dev key)
- [ ] WAL sync policy appropriate for durability requirements
- [ ] Snapshot interval and retention set for recovery time objectives
- [ ] Log level set to `info` or `warn` (not `debug` or `trace`)
- [ ] Docker security hardening applied (`read_only`, `cap_drop: ALL`, `no-new-privileges`)
- [ ] Health checks configured for container orchestrator
- [ ] Backup strategy for data directory (WAL + snapshots + Parquet)
