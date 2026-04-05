# Security

Selene is secure by default. Production mode requires TLS certificates, enforces Cedar authorization, and rejects plaintext HTTP. Dev mode relaxes these constraints for local development only.

## TLS

Selene uses QUIC (which mandates TLS 1.3) for its primary transport and supports optional HTTPS for the HTTP API. The ALPN protocol identifier is `selene/1`.

### Server certificate setup

Generate a self-signed CA and server certificate for testing:

```bash
# Generate CA key and certificate
openssl ecparam -genkey -name prime256v1 -out ca.key
openssl req -new -x509 -key ca.key -out ca.crt -days 365 \
    -subj "/CN=Selene CA"

# Generate server key and CSR
openssl ecparam -genkey -name prime256v1 -out server.key
openssl req -new -key server.key -out server.csr \
    -subj "/CN=selene.local"

# Sign server certificate with CA
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -days 365
```

Configure the server to use these certificates:

```toml
[tls]
cert_path = "/etc/selene/certs/server.crt"
key_path = "/etc/selene/certs/server.key"
```

### Mutual TLS (mTLS)

For environments requiring client certificate verification, set the `ca_cert_path` to the CA certificate that signed client certificates:

```toml
[tls]
cert_path = "/etc/selene/certs/server.crt"
key_path = "/etc/selene/certs/server.key"
ca_cert_path = "/etc/selene/certs/ca.crt"
```

When `ca_cert_path` is set, QUIC clients must present a certificate signed by the specified CA. Connections without a valid client certificate are rejected at the TLS layer.

Generate a client certificate for mTLS:

```bash
openssl ecparam -genkey -name prime256v1 -out client.key
openssl req -new -key client.key -out client.csr \
    -subj "/CN=selene-client"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out client.crt -days 365
```

### Inter-node TLS

Replicas and federation peers use the `[node_tls]` section for mutual authentication:

```toml
[node_tls]
ca_cert = "/etc/selene/certs/ca.crt"
cert = "/etc/selene/certs/node.crt"
key = "/etc/selene/certs/node.key"
```

### Dev mode TLS

In dev mode (`--dev` flag or `dev_mode = true`), Selene generates a self-signed certificate for `localhost` at startup. This certificate is ephemeral and changes on every restart. Dev mode is logged with a prominent warning and should never be used in production.

## Authentication

Selene supports three authentication types, selected during the connection handshake.

### Bearer token format

HTTP requests authenticate via the `Authorization` header:

```
Authorization: Bearer <identity>:<secret>
```

The token is split on the first colon into an identity and a secret. The identity locates a principal node in the graph; the secret is verified against the principal's stored argon2id hash.

### Auth types

| Type | Transport | When used |
|------|-----------|-----------|
| `dev` | QUIC | Dev mode only. Accepts any identity without credential verification. An empty identity or `"admin"` grants admin context |
| `token` | HTTP, QUIC | Production. Looks up principal by identity, verifies credential against argon2id hash |
| `psk` | QUIC | Pre-shared key. Same flow as `token` (identity lookup + credential verification) |

In dev mode with no `Authorization` header present, HTTP requests fall back to an admin context automatically.

### Principal nodes

Principals are graph nodes with the label `principal` and the following properties:

| Property | Type | Description |
|----------|------|-------------|
| `identity` | string | Unique identifier used in authentication |
| `role` | string | One of: `admin`, `service`, `operator`, `reader`, `device` |
| `credential_hash` | string | Argon2id hash in PHC string format |
| `enabled` | bool | Must be `true` for authentication to succeed |

Credentials are never stored in plaintext. The `credential_hash` property contains a PHC-format string (`$argon2id$...`) produced by the argon2 crate with random salts. Verification uses constant-time comparison.

## Cedar authorization

Selene uses [Cedar](https://www.cedarpolicy.com/) for fine-grained authorization. Cedar policies define which roles can perform which actions on which resources.

### How it works

1. On first startup, Selene writes a default Cedar schema (`schema.cedarschema`) and default policies (`default.cedar`) to the `{data_dir}/policies/` directory
2. On connection, the principal's role and scope are resolved from the graph
3. Each operation maps to a Cedar action (see table below)
4. The Cedar engine evaluates the request against all loaded policies
5. Admin principals bypass Cedar evaluation entirely (global scope)
6. Non-admin principals are scope-checked first (fast bitmap test), then Cedar-evaluated

### Cedar entity model

Selene maps its authorization model to Cedar entities:

- **Principal:** `Selene::Principal` with a `role` attribute
- **Action:** `Selene::Action` with names like `"entity:read"` or `"gql:query"`
- **Resource:** `Selene::Node` representing the target node

### Actions

| Cedar action | Description |
|-------------|-------------|
| `entity:read` | Read node/edge properties |
| `entity:create` | Create nodes or edges |
| `entity:modify` | Modify node/edge properties |
| `entity:delete` | Delete nodes or edges |
| `ts:write` | Write time-series samples |
| `ts:read` | Read time-series data |
| `gql:query` | Execute GQL read queries |
| `gql:mutate` | Execute GQL mutations |
| `changelog:subscribe` | Subscribe to the CDC changelog |
| `principal:manage` | Create, modify, or delete principal nodes |
| `policy:manage` | Modify Cedar policies |
| `federation:manage` | Manage federation peers |

### Default role permissions

The default policies implement a five-role model:

| Role | Permissions |
|------|------------|
| **admin** | All actions on all resources (global scope, bypasses Cedar) |
| **service** | Scoped CRUD, GQL query/mutate, TS read/write, changelog subscribe |
| **operator** | Scoped CRUD, GQL query/mutate, TS read/write |
| **reader** | Scoped entity read, GQL query, TS read |
| **device** | TS write only (scoped to own entity) |

### Scope resolution

Non-admin principals are scoped to a subtree of the graph's containment hierarchy. Scope is determined by `scoped_to` edges from the principal node to one or more root nodes. The scope bitmap includes all root nodes and their descendants reachable via `contains` edges.

For example, a principal with a `scoped_to` edge pointing to a building node can access that building and all floors, zones, and equipment within it, but cannot access other buildings or the parent site.

Scope is resolved as a RoaringBitmap at connection time. Checking scope is O(1) per node.

### Custom policies

Add custom `.cedar` files to `{data_dir}/policies/`. Selene loads all `.cedar` files from this directory at startup. The default policies are written only if they do not already exist, so they can be modified freely.

Example custom policy granting a specific principal read access to federation management:

```cedar
permit(
    principal == Selene::Principal::"500",
    action == Selene::Action::"federation:manage",
    resource
);
```

## Rate limiting

The HTTP authentication layer includes brute-force protection with per-identity exponential backoff.

| Parameter | Value |
|-----------|-------|
| Attempts before backoff | 5 |
| Backoff formula | 2^(failures - 5) seconds |
| Maximum backoff | 300 seconds (5 minutes) |
| Record expiry | 600 seconds (10 minutes of inactivity) |
| Maximum tracked identities | 10,000 |

After 5 failed authentication attempts for a given identity, subsequent attempts receive an HTTP 429 (Too Many Requests) response with the remaining wait time in the response body. The backoff doubles with each additional failure up to the 5-minute maximum. Records are cleared on successful authentication and pruned periodically when they expire.

The rate limiter caps tracked identities at 10,000 entries. If the map reaches capacity, expired entries are pruned first. If still at capacity, new failure records are dropped rather than growing unbounded.

## Secure vault

The vault is an encrypted, isolated graph that stores sensitive data: principal nodes, API keys, Cedar policies, server configuration overrides, and audit logs. It is separate from the main graph with no cross-graph joins.

### Envelope encryption

The vault uses a two-layer encryption scheme:

1. **Master key (KEK)** -- a 256-bit key that wraps the data encryption key. Provided via key file or derived from a passphrase using Argon2id
2. **Data encryption key (DEK)** -- a random 256-bit key that encrypts the vault payload

Both layers use XChaCha20-Poly1305 (AEAD). The DEK is wrapped (encrypted) by the master key and stored in the vault file header. The vault payload (serialized graph data) is encrypted by the DEK with authenticated associated data (magic bytes + version + timestamp).

This design enables master key rotation without re-encrypting the entire vault -- only the DEK wrapper changes.

### Master key management

The master key is resolved in priority order:

1. `SELENE_VAULT_PASSPHRASE` environment variable -- derived via Argon2id (64 MB memory, 3 iterations, 1 thread). The variable is read and cleared from the process environment before any threads are spawned
2. `SELENE_VAULT_KEY_FILE` environment variable -- path to a key file
3. `vault.master_key_file` config setting -- path to a key file
4. Dev mode fallback -- an all-zero key (logged with a warning)

If none of the above are available in production mode, the vault refuses to start.

**Key file format:** The key file must contain exactly 32 bytes encoded as base64 (44 characters) or hex (64 characters). The `base64:` prefix is optional.

Generate a key file:

```bash
openssl rand 32 | base64 > /run/secrets/selene-vault-key
chmod 600 /run/secrets/selene-vault-key
```

**Passphrase derivation:** When using `SELENE_VAULT_PASSPHRASE`, the key is derived using Argon2id with a random 16-byte salt. The salt is stored in the vault file header so the same key can be derived on reopens. For new vaults, a fresh salt is generated.

### Key rotation

**Master key rotation** re-wraps the DEK with a new master key. The vault payload is not re-encrypted (the DEK remains the same). After rotation, the old master key can no longer open the vault.

**DEK rotation** generates a new random DEK and re-encrypts the entire vault payload. Use this for periodic rotation of the data-layer encryption.

### Vault file format

The vault file (`secure.vault`) uses an atomic write strategy (write to temp file, fsync, rename). On Unix systems, file permissions are set to `0600` (owner read/write only). The file format includes:

- 4-byte magic (`SVLT`)
- 2-byte version (currently 1)
- Key source indicator (raw or passphrase-derived)
- Argon2 salt (16 bytes, zero-filled for raw keys)
- Wrapped DEK (nonce + ciphertext)
- Payload timestamp, nonce, and encrypted graph data

### Audit logging

Every vault mutation creates a tamper-resistant audit entry inside the encrypted vault. Audit nodes carry the `audit_log` label with properties for principal identity, action, details, and timestamp. Because audit entries live inside the encrypted vault, an attacker without the master key cannot read or forge them.

## Metrics endpoint security

The `/metrics` endpoint supports a separate bearer token configured via `http.metrics_token` or `SELENE_METRICS_TOKEN`. When a token is configured, requests to `/metrics` must include:

```
Authorization: Bearer <metrics-token>
```

The token comparison uses constant-time equality to prevent timing attacks. When no token is configured (dev mode), the metrics endpoint is unauthenticated.

## Production security checklist

- [ ] Dev mode is disabled (`dev_mode = false` is the default)
- [ ] TLS certificates are from a trusted CA (not self-signed)
- [ ] mTLS is enabled if QUIC clients are known (`ca_cert_path` set)
- [ ] All principal nodes have strong credential hashes (argon2id)
- [ ] No principals use the `admin` role unless strictly necessary
- [ ] Cedar policies are reviewed and locked down for the deployment's role model
- [ ] Vault is enabled with a securely stored master key (file on tmpfs or secrets manager)
- [ ] `SELENE_VAULT_PASSPHRASE` is passed via a secrets manager, not hardcoded
- [ ] Metrics endpoint has a dedicated token (`http.metrics_token`)
- [ ] CORS origins are restricted to known dashboards (`http.cors_origins`)
- [ ] Plaintext HTTP is disabled unless behind a TLS-terminating reverse proxy
- [ ] Docker container runs with `read_only`, `cap_drop: ALL`, and `no-new-privileges`
- [ ] Data directory permissions restrict access to the Selene process user
- [ ] Vault file permissions are `0600` (set automatically on Unix)
- [ ] Log level is `info` or `warn` (debug/trace may log sensitive data)
- [ ] Inter-node TLS is configured for replicas and federation peers (`[node_tls]`)
