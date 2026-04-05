# Replication and Federation

Selene supports two forms of multi-node deployment: CDC replicas for read scaling and high availability, and federation mesh for distributed graph queries across peers.

## CDC Replicas

A Selene node can start as a read-only replica of another node (the primary). The replica maintains a synchronized copy of the primary's graph state and serves read queries locally.

### Starting a Replica

Pass the `--replica-of` flag with the primary's QUIC address:

```bash
selene-server --dev --replica-of 192.168.1.10:4510
```

The replica connects to the primary over QUIC using the same TLS and authentication configuration as any other client connection. In dev mode, the replica uses self-signed certificates automatically. In production, configure the `[node_tls]` and `[replica]` sections.

### Replication Protocol

The replica follows a subscribe-before-snapshot pattern to avoid missing changes during the initial sync:

1. **Connect** -- establish a QUIC connection to the primary.
2. **Subscribe to changelog** -- open a changelog stream at sequence 0. Incoming events buffer in memory while the snapshot transfers.
3. **Request snapshot** -- ask the primary for a full binary snapshot (the same postcard+zstd format used for on-disk snapshots).
4. **Load snapshot** -- deserialize the snapshot and replace the local graph state, including nodes, edges, schemas, and triggers.
5. **Replay buffered entries** -- apply any changelog entries with sequence numbers greater than the snapshot's sequence.
6. **Live consumption** -- continue consuming changelog entries in real time. Each entry is acknowledged back to the primary.

If the changelog stream reports sync loss (the primary's buffer has advanced past what the replica has consumed), the replica re-syncs by repeating the full snapshot transfer.

### Automatic Reconnect

If the connection to the primary drops, the replica waits 5 seconds and reconnects. This cycle repeats until the connection succeeds or the server shuts down. Reconnection always starts from step 1 (full snapshot), so the replica self-heals after any network interruption.

### Read-Only Enforcement

A replica rejects all mutation requests:

- **HTTP API** -- mutations return `405 Method Not Allowed`.
- **QUIC** -- mutation messages return an `INVALID_REQUEST` error with the message "read-only replica".
- **GQL** -- `INSERT`, `SET`, and `DELETE` statements are blocked before execution.
- **Background tasks** -- the snapshot task is skipped on replicas since they have no local WAL.

### Health Monitoring

The `/health` endpoint reports the node's role and replication lag:

```json
{
  "status": "ok",
  "node_count": 10500,
  "edge_count": 23000,
  "uptime_secs": 3600,
  "dev_mode": false,
  "role": "replica",
  "primary": "192.168.1.10:4510",
  "lag_sequences": 0
}
```

The `lag_sequences` field indicates how far behind the replica is. A value of `0` means the replica is consuming events in real time. During the initial snapshot transfer, `lag_sequences` is set to `u64::MAX` until the snapshot loads.

### Configuration Reference

The `[replica]` and `[node_tls]` sections control replica authentication and TLS:

| Field | Section | Description |
|-------|---------|-------------|
| `auth_identity` | `[replica]` | Identity string for authenticating to the primary |
| `auth_credentials` | `[replica]` | Credential for authenticating to the primary |
| `server_name` | `[replica]` | TLS server name override for the primary |
| `ca_cert` | `[node_tls]` | CA certificate for verifying peer identity |
| `cert` | `[node_tls]` | Local node certificate for mTLS |
| `key` | `[node_tls]` | Local node private key |

### Docker Replica Setup

The repository includes `docker-compose.replica.yml` for running a primary + replica pair:

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
```

Start both containers:

```bash
docker compose -f docker-compose.replica.yml up -d
```

The replica waits for the primary's healthcheck to pass before starting replication.

## Federation Mesh

Federation allows multiple Selene nodes to form a query mesh. A GQL query on one node can transparently execute against a remote peer's graph using the `USE <graph>` directive. This feature requires the `federation` compile-time flag.

```bash
cargo run -p selene-server --features federation,http
```

### Graph Name Resolution

When a GQL statement includes a `USE <name>` prefix, the graph resolver checks three locations in order:

1. **Vault** -- if the name is `secure` (case-insensitive), the query runs against the encrypted vault graph.
2. **Local catalog** -- named graphs created locally via the API.
3. **Remote peers** -- the federation peer registry. If a peer with a matching name exists, the query is forwarded over QUIC.

If none match, the query returns an error.

### Peer Configuration

Configure federation in the `[federation]` section of `selene.toml`:

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable federation |
| `node_name` | `"selene"` | This node's name in the mesh (used for `USE <name>` routing) |
| `role` | `"building"` | Role hint: `aggregator`, `building`, or `device` |
| `bootstrap_peers` | `[]` | QUIC addresses of peers to connect to on startup |
| `peer_ttl_secs` | `300` | Time-to-live for peer registry entries (seconds) |
| `refresh_interval_secs` | `60` | How often to prune stale peers (seconds) |

Example configuration:

```toml
[federation]
enabled = true
node_name = "building_a"
role = "building"
bootstrap_peers = ["192.168.1.20:4510", "192.168.1.30:4510"]
peer_ttl_secs = 300
refresh_interval_secs = 60
```

### Bloom Filter Label Routing

During the federation handshake, each peer computes a Bloom filter from its schema labels and property keys, then exchanges it with connected peers. Before forwarding a query, the coordinator checks the Bloom filter to determine whether a peer might have data matching the query's labels. Peers that definitely lack matching data are skipped, reducing unnecessary network round trips.

The Bloom filter uses double hashing and is tuned for approximately 10,000 items at a 1% false positive rate (roughly 12 KB per filter).

### Query Forwarding

When a query targets a remote peer, the local node:

1. Establishes (or reuses) a QUIC connection to the peer.
2. Serializes the GQL query into a `FederationGqlRequest`.
3. Sends it over the QUIC connection.
4. The remote peer executes the query against its local default graph.
5. Results return as Arrow IPC (binary, default) or JSON.

The forwarding is transparent to the caller -- results look identical to local query results.

### Peer Lifecycle

- **Bootstrap** -- on startup, the node connects to all `bootstrap_peers` and exchanges registration messages. Each peer shares its name, address, schema labels, role, and Bloom filter.
- **Peer directory** -- after registering, the node pulls the remote peer's full peer list, enabling transitive discovery.
- **Pruning** -- a background task runs every `refresh_interval_secs` and removes peers not seen within `peer_ttl_secs`. Connections to pruned peers are dropped.
- **Reconnect** -- `get_or_connect()` establishes a new connection on demand if a previously known peer's connection was lost.

### Profile Defaults

Federation is enabled by default in the `cloud` profile and disabled in `edge` and `standalone`:

| Profile | Federation |
|---------|-----------|
| `edge` | disabled |
| `cloud` | enabled |
| `standalone` | disabled |

Override with the `[services]` section or the `SELENE_SERVICES_FEDERATION_ENABLED` environment variable.
