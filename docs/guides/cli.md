# CLI Guide

## Overview

The `selene` CLI connects to a running Selene server over QUIC and provides subcommands
for health checks, node and edge operations, time-series writes and queries, GQL execution,
an interactive REPL shell, configuration tuning, key generation, and graph slicing.

```
selene [connection flags] <subcommand> [options]
```

## Connection Flags

All connection flags are optional and apply to every subcommand except `health --http`,
`keygen`, and `tune`, which run locally without a server connection.

| Flag | Default | Description |
|------|---------|-------------|
| `--server` | `127.0.0.1:4510` | Server address (host:port) |
| `--server-name` | `localhost` | Server name for TLS certificate verification |
| `--insecure` | off | Skip TLS certificate verification (dev mode only) |
| `--tls-ca` | none | Path to PEM-encoded CA certificate for server verification |
| `--tls-cert` | none | Path to PEM-encoded client certificate for mTLS |
| `--tls-key` | none | Path to PEM-encoded client private key for mTLS |
| `--auth-type` | none | Authentication type: `dev`, `token`, or `psk` |
| `--auth-identity` | none | Principal identity (username) |
| `--auth-secret` | none | Authentication credential (password or token) |

### TLS Behavior

Selene is secure by default. You must provide one of:

- `--insecure` -- disables TLS verification entirely (development only)
- `--tls-ca <path>` -- specifies the CA certificate to verify the server

For mutual TLS (mTLS), provide both `--tls-cert` and `--tls-key` in addition to
`--tls-ca`.

### Connection Examples

```bash
# Dev mode (insecure, no TLS verification)
selene --insecure health

# Production with CA certificate
selene --tls-ca /etc/selene/ca.pem gql "MATCH (n) RETURN count(*) AS cnt"

# mTLS with authentication
selene --tls-ca ca.pem --tls-cert client.pem --tls-key client-key.pem \
  --auth-type token --auth-identity admin --auth-secret $TOKEN \
  node list --label sensor
```

## Subcommands

### health

Check server health. By default, uses the QUIC connection. The `--http` flag probes
the HTTP health endpoint instead, which is useful for Docker and Kubernetes health
checks that do not require TLS setup.

| Flag | Default | Description |
|------|---------|-------------|
| `--http` | off | Use the HTTP health endpoint instead of QUIC |
| `--http-addr` | `127.0.0.1:8080` | HTTP address to probe |

```bash
# QUIC health check (requires TLS)
selene --insecure health

# HTTP health check (no TLS needed)
selene health --http
selene health --http --http-addr 192.168.1.10:8080
```

### node

Node operations. All node subcommands translate to GQL queries internally.

#### node create

Create a new node with labels and properties.

| Flag | Default | Description |
|------|---------|-------------|
| `--labels` | (required) | Comma-separated labels |
| `--props` | `{}` | JSON properties |

```bash
selene --insecure node create --labels sensor,temperature \
  --props '{"name": "Zone-1 Temp", "unit": "°F"}'
```

#### node get

Get a node by ID.

```bash
selene --insecure node get 42
```

#### node list

List nodes, optionally filtered by label.

| Flag | Default | Description |
|------|---------|-------------|
| `--label` | none | Filter by label |
| `--limit` | 100 | Maximum results |

```bash
selene --insecure node list --label sensor --limit 50
```

#### node modify

Modify a node's properties and labels.

| Flag | Default | Description |
|------|---------|-------------|
| `--set` | `{}` | JSON properties to set or update |
| `--remove-props` | "" | Comma-separated property keys to remove |
| `--add-labels` | "" | Comma-separated labels to add |
| `--remove-labels` | "" | Comma-separated labels to remove |

```bash
selene --insecure node modify 42 \
  --set '{"status": "active", "firmware": "2.1"}' \
  --add-labels commissioned \
  --remove-props legacy_id
```

#### node delete

Delete a node and all its connected edges (DETACH DELETE).

```bash
selene --insecure node delete 42
```

### edge

Edge operations.

#### edge create

Create a directed edge between two nodes.

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | (required) | Source node ID |
| `--target` | (required) | Target node ID |
| `--label` | (required) | Edge label |
| `--props` | `{}` | JSON properties |

```bash
selene --insecure edge create --source 1 --target 42 \
  --label isPointOf --props '{"installed": "2026-01-15"}'
```

#### edge get

Get an edge by ID.

```bash
selene --insecure edge get 7
```

#### edge list

List edges, optionally filtered by label.

| Flag | Default | Description |
|------|---------|-------------|
| `--label` | none | Filter by label |
| `--limit` | 100 | Maximum results |

```bash
selene --insecure edge list --label contains --limit 200
```

#### edge delete

Delete an edge by ID.

```bash
selene --insecure edge delete 7
```

### ts

Time-series operations.

#### ts write

Write a time-series sample. The timestamp is set to the current time automatically.

| Positional | Description |
|------------|-------------|
| `entity_id` | Node ID the sample belongs to |
| `property` | Property name (e.g., `temperature`) |
| `value` | Numeric value |

```bash
selene --insecure ts write 42 temperature 72.5
selene --insecure ts write 42 humidity 45.0
```

#### ts query

Query time-series samples for a node and property.

| Positional | Description |
|------------|-------------|
| `entity_id` | Node ID to query |
| `property` | Property name to query |

| Flag | Default | Description |
|------|---------|-------------|
| `--start` | 0 | Start timestamp in nanoseconds |
| `--end` | i64::MAX | End timestamp in nanoseconds |
| `--limit` | none | Maximum number of results |

```bash
selene --insecure ts query 42 temperature
selene --insecure ts query 42 temperature --start 1711929600000000000 --limit 100
```

### gql

Execute a GQL query. This is the primary query interface and supports reads, mutations,
aggregations, and graph traversals.

| Positional | Description |
|------------|-------------|
| `query` | GQL query string (optional if `--file` is provided) |

| Flag | Default | Description |
|------|---------|-------------|
| `--explain` | off | Show the execution plan without executing |
| `--profile` | off | Show the execution plan with per-operator timing |
| `--file` | none | Execute GQL from a file instead of an argument |
| `--format` | `json` | Output format: `table`, `json`, or `csv` |

```bash
# Simple query
selene --insecure gql "MATCH (s:sensor) RETURN s.name AS name, s.unit AS unit"

# Aggregation
selene --insecure gql "MATCH (s:sensor) RETURN count(*) AS total"

# Graph traversal
selene --insecure gql \
  "MATCH (b:building)-[:contains]->(f:floor)-[:contains]->(z:zone) \
   RETURN b.name AS building, f.name AS floor, z.name AS zone"

# Mutation
selene --insecure gql "INSERT (:sensor {name: 'AHU-1 SAT', unit: '°F'})"

# Explain plan
selene --insecure gql --explain \
  "MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name AS name"

# Profile with timing
selene --insecure gql --profile \
  "MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name AS name"

# Execute from file
selene --insecure gql --file queries/building-report.gql --format table

# CSV output for piping
selene --insecure gql --format csv \
  "MATCH (s:sensor) RETURN s.name AS name, s.unit AS unit" > sensors.csv
```

### shell

Interactive GQL REPL with command history. History is persisted to `~/.selene_history`
between sessions.

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `table` | Default output format: `table`, `json`, or `csv` |

```bash
selene --insecure shell
selene --insecure shell --format json
```

Inside the shell, dot-commands control behavior:

| Command | Description |
|---------|-------------|
| `.help` | Show available commands |
| `.explain on` | Toggle EXPLAIN mode on (all queries show their plan) |
| `.explain off` | Toggle EXPLAIN mode off |
| `.format table` | Switch output to table format |
| `.format json` | Switch output to JSON format |
| `.format csv` | Switch output to CSV format |
| `.clear` | Clear the screen |
| `.exit` | Exit the shell (also `.quit` or Ctrl-D) |

Example session:

```
$ selene --insecure shell
Selene GQL Shell - type .help for commands, .exit to quit
selene> MATCH (n) RETURN count(*) AS cnt
┌─────┐
│ cnt │
├─────┤
│ 847 │
└─────┘
selene> .explain on
EXPLAIN mode ON
selene [explain]> MATCH (s:sensor) FILTER s.temp > 72 RETURN s.name
LabelScan(sensor) -> Filter(s.temp > 72) -> Project(s.name)
selene [explain]> .explain off
EXPLAIN mode OFF
selene> .exit
```

### tune

Analyze the system and recommend Selene configuration values based on available
hardware. The output includes a ready-to-paste TOML configuration block.

| Flag | Default | Description |
|------|---------|-------------|
| `--profile` | `balanced` | Resource profile: `conservative`, `balanced`, or `greedy` |
| `--scope` | `dedicated` | Deployment scope: `dedicated` (Selene owns the machine) or `embedded` (shared resources) |

Profile characteristics:

| Profile | CPU Fraction | Memory Fraction | Query Timeout |
|---------|-------------|-----------------|---------------|
| `conservative` | 25% | 25% | 15s |
| `balanced` | 50% | 50% | 30s |
| `greedy` | 90% | 80% | 120s |

The `embedded` scope halves the CPU and memory fractions, suitable for running alongside
other services on the same machine.

```bash
selene tune
selene tune --profile greedy --scope dedicated
selene tune --profile conservative --scope embedded
```

### keygen

Generate a 256-bit master key for the secure vault (XChaCha20-Poly1305 encryption).
This subcommand runs locally and does not require a server connection.

| Flag | Default | Description |
|------|---------|-------------|
| `--output` | stdout | Write key to a file instead of printing. File permissions are set to 0600 on Unix. |
| `--format` | `base64` | Output format: `base64` or `hex` |

```bash
# Print base64-encoded key
selene keygen

# Write to file (permissions set to 0600)
selene keygen --output /etc/selene/master.key

# Hex format
selene keygen --format hex
```

### slice

Get a graph slice -- a filtered snapshot of nodes and edges.

| Flag | Default | Description |
|------|---------|-------------|
| `--slice-type` | `full` | Slice type: `full`, `labels`, or `containment` |
| `--labels` | none | Comma-separated labels for label-based slice |
| `--root` | none | Root node ID for containment slice |
| `--depth` | none | Maximum depth for containment slice |

```bash
# Full graph dump
selene --insecure slice

# Nodes with specific labels and their connecting edges
selene --insecure slice --slice-type labels --labels sensor,equipment

# Containment subtree from building node
selene --insecure slice --slice-type containment --root 1 --depth 4
```

## Output Formats

Three output formats are available for the `gql` and `shell` subcommands:

| Format | Description |
|--------|-------------|
| `table` | Formatted ASCII table. Default for `shell`. |
| `json` | JSON array of result objects. Default for `gql`. |
| `csv` | Comma-separated values. Suitable for piping to other tools. |

All other subcommands (`node`, `edge`, `ts`, `slice`, `health`) output JSON.
