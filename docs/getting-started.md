# Getting Started

Selene is a lightweight, in-memory property graph database designed for IoT, smart buildings, and any domain that needs a live graph of connected entities with real-time state. Written in pure Rust with zero C/C++ dependencies, Selene ships as a single binary that exposes QUIC, HTTP, and MCP transports in one process.

## Prerequisites

- **Rust 1.94+** (install via [rustup](https://rustup.rs/))
- No C compiler or system libraries required

Docker users need only Docker (no Rust toolchain required).

## Build from Source

Clone the repository and build the server with HTTP and MCP features enabled:

```bash
git clone https://github.com/yourorg/selene.git
cd selene
cargo build --release -p selene-server --features http,mcp
```

The server binary is at `target/release/selene-server`. The CLI binary can be built separately:

```bash
cargo build --release -p selene-cli
```

## Docker Quickstart

The fastest way to run Selene is with Docker Compose. The dev compose file starts the server in dev mode with demo data pre-loaded:

```bash
docker compose -f docker-compose.dev.yml up -d
```

This starts Selene with:

- QUIC on port 4510 (UDP)
- HTTP on port 8080 (TCP)
- MCP enabled
- Dev mode (self-signed TLS, no auth)
- Demo data seeded on startup

To build the Docker image directly:

```bash
docker build -t selene .
```

The resulting image uses a distroless base and is roughly 14 MB compressed.

## Dev Mode with Demo Data

During development, the `--dev` and `--seed` flags simplify startup. Dev mode generates self-signed TLS certificates and disables authentication. The `--seed` flag populates the graph with sample building data (sensors, equipment, spaces, and relationships) when the graph is empty.

```bash
cargo run -p selene-server --features http,mcp -- --dev --seed
```

Expected output:

```
WARN  selene_server > === DEV MODE ACTIVE -- no authentication, self-signed TLS ===
INFO  selene_server > seeding demo data...
INFO  selene_server > QUIC listener started addr=0.0.0.0:4510
INFO  selene_server > HTTP listener started addr=0.0.0.0:8080
```

The server is now running with a pre-built graph you can query immediately.

## Verify the Server

Check that the server is healthy with a simple curl request:

```bash
curl -s http://localhost:8080/health | jq
```

Expected output:

```json
{
  "status": "ok",
  "uptime_secs": 5
}
```

## Your First Query

### Via HTTP

Selene uses GQL (ISO 39075) as its sole query language. Send a query to the HTTP endpoint to count all nodes in the graph:

```bash
curl -s -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n) RETURN count(*) AS cnt"}' | jq
```

Expected output (with demo data seeded):

```json
{
  "status_code": "00000",
  "message": "OK",
  "row_count": 1,
  "data": [{"cnt": 10}]
}
```

### Via the CLI

The CLI connects to the server over QUIC. In dev mode, use the `--insecure` flag to skip TLS certificate verification:

```bash
selene --insecure gql "MATCH (n) RETURN count(*) AS cnt"
```

If you built from source, the binary is at `target/release/selene`:

```bash
./target/release/selene --insecure gql "MATCH (n) RETURN count(*) AS cnt"
```

### Exploring the Graph

With demo data loaded, try querying for sensors and what they monitor:

```bash
curl -s -X POST http://localhost:8080/gql \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (s:temperature_sensor)-[:isPointOf]->(e:equipment) RETURN s.name AS sensor, e.name AS equipment"}' | jq
```

## Server CLI Flags

The server accepts these flags at startup:

| Flag | Description |
|------|-------------|
| `--dev` | Enable dev mode (self-signed TLS, no auth) |
| `--seed` | Seed demo data on startup (if graph is empty) |
| `--config <path>` | Path to TOML config file |
| `--profile <type>` | Runtime profile: `edge`, `cloud`, or `standalone` |
| `--quic-listen <addr>` | QUIC listen address override |
| `--http-listen <addr>` | HTTP listen address override |
| `--data-dir <path>` | Data directory override |
| `--show-config` | Print effective config and exit |
| `--replica-of <addr>` | Start as a read-only replica of the given primary |

## Feature Flags

All features are opt-in at compile time:

| Feature | Description |
|---------|-------------|
| `http` | HTTP transport (axum) |
| `mcp` | Model Context Protocol server |
| `federation` | GQL-native federation mesh |
| `vector` | Vector embeddings (candle) |
| `search` | Full-text search (tantivy) |
| `temporal` | Temporal graph features |
| `cloud-storage` | Cloud offload (S3/GCS/Azure) |
| `rdf` | RDF import/export |
| `rdf-sparql` | SPARQL query support |

Build with multiple features:

```bash
cargo build --release -p selene-server --features http,mcp,federation,search
```

## Next Steps

- [GQL Query Guide](guides/gql/overview.md) -- learn the query language
- [HTTP API Reference](guides/http-api.md) -- full HTTP endpoint documentation
- [Configuration](operations/configuration.md) -- TOML config, profiles, and environment variables
