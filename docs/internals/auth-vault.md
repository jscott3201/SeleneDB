# Auth and Vault Internals

This document describes the authorization system and encrypted vault in `selene-server`. Cedar policies control what each principal can do. The vault provides encrypted storage for sensitive data (principals, API keys, Cedar policies) in a separate, isolated graph.

**Key source files:**

- `crates/selene-server/src/auth/mod.rs` -- Roles and module structure
- `crates/selene-server/src/auth/engine.rs` -- Cedar authorization engine
- `crates/selene-server/src/auth/credential.rs` -- Argon2id credential hashing
- `crates/selene-server/src/auth/handshake.rs` -- Connection authentication and AuthContext
- `crates/selene-server/src/auth/policies.rs` -- Default Cedar policy loading
- `crates/selene-server/src/auth/projection.rs` -- Scope resolution via containment hierarchy
- `crates/selene-server/src/http/auth.rs` -- HTTP Bearer token extraction and rate limiting
- `crates/selene-server/src/vault/mod.rs` -- VaultHandle, master key resolution
- `crates/selene-server/src/vault/crypto.rs` -- Envelope encryption primitives
- `crates/selene-server/src/vault/storage.rs` -- Vault file format and I/O

## Auth Architecture

SeleneDB separates authentication (who are you?) from authorization (what can you do?):

- **Authentication** is pluggable, supporting dev mode, token-based, and pre-shared key (PSK) types. Principals are nodes in the graph with an `identity` property.
- **Authorization** uses Cedar (Amazon's policy language) to evaluate permit/forbid decisions against a typed entity model.

The flow for every authenticated request:

```
Request arrives
      |
      v
  [Authentication]  -- validate credentials, resolve principal node
      |
      v
  [AuthContext]     -- principal_node_id, role, scope bitmap
      |
      v
  [Authorization]   -- Cedar policy evaluation (principal, action, resource)
      |
      v
  Operation proceeds or is rejected
```

## Cedar Authorization

### Policy Language

Cedar is a declarative policy language created by Amazon. Policies are `permit` or `forbid` rules with conditions on `principal`, `action`, and `resource`. SeleneDB maps its domain concepts to Cedar's entity model:

| Cedar concept | SeleneDB mapping |
|---------------|----------------|
| Principal | `Selene::Principal` entity with the principal's node ID |
| Action | `Selene::Action` entity with a namespaced name (e.g., `entity:read`) |
| Resource | `Selene::Node` entity with the target node's ID |

### Actions

The `Action` enum defines the full set of authorized operations:

| Action | Cedar name | Description |
|--------|------------|-------------|
| EntityRead | `entity:read` | Read a node or edge |
| EntityCreate | `entity:create` | Create a node or edge |
| EntityModify | `entity:modify` | Modify properties or labels |
| EntityDelete | `entity:delete` | Delete a node or edge |
| TsWrite | `ts:write` | Write time-series samples |
| TsRead | `ts:read` | Read time-series data |
| GqlQuery | `gql:query` | Execute a GQL read query |
| GqlMutate | `gql:mutate` | Execute a GQL mutation |
| ChangelogSubscribe | `changelog:subscribe` | Subscribe to change feeds |
| PrincipalManage | `principal:manage` | Create or modify principals |
| PolicyManage | `policy:manage` | Manage Cedar policies |
| FederationManage | `federation:manage` | Manage federation peers |

### Evaluation Flow

`AuthEngine::authorize()` follows this path:

1. **Admin bypass**: If `auth.role == Role::Admin`, return true immediately. Admins have global scope and skip all policy checks.
2. **Scope check**: Verify the resource node ID is in the principal's scope bitmap (`RoaringBitmap`). This is a fast-path rejection that avoids Cedar evaluation entirely.
3. **Cedar evaluation**: Construct a Cedar `Request` with the principal UID, action UID, and resource UID. Build `Entities` containing the principal (with a `role` attribute) and the resource. Call `Authorizer::is_authorized()` against the loaded `PolicySet`. Return true if `Decision::Allow`.

For scopeless actions (like health checks), `authorize_action()` skips the scope check and evaluates Cedar with `NodeId(0)` as a placeholder resource.

### Default Policies

On first load, `AuthEngine::load()` calls `ensure_defaults()` to write default policy files if the policy directory is empty. The default policies define role-based access:

| Role | Permissions |
|------|-------------|
| Admin | All actions (global scope, bypasses Cedar) |
| Service | All entity and TS operations, GQL query/mutate, changelog subscribe |
| Operator | Entity read/create/modify, TS read/write, GQL query/mutate |
| Reader | Entity read, TS read, GQL query |
| Device | TS write only |

### Scope Resolution

Non-admin principals have a scope -- a `RoaringBitmap` of node IDs they can access. The scope is resolved by `AuthEngine::resolve_scope()`, which:

1. Finds the principal's `scoped_to` edges in the graph (pointing to containment root nodes).
2. Walks the containment hierarchy from those roots, collecting all reachable node IDs into a bitmap.

Admin principals return `None` for scope (global access), which the auth checks interpret as "can access everything."

The scope bitmap is cached in the `AuthContext` along with a `scope_generation` counter. When the graph's containment structure changes, the scope may need to be re-resolved.

## Credentials

### Argon2id Hashing

Credentials are never stored in plaintext. The `credential.rs` module provides two functions:

- `hash_credential(secret)` -- Hash a plaintext secret using Argon2id with a random salt. Returns a PHC-format string (`$argon2id$v=19$m=19456,t=2,p=1$...`) that encodes the algorithm parameters, salt, and hash.
- `verify_credential(secret, hash_str)` -- Verify a plaintext secret against a stored Argon2id hash using constant-time comparison to prevent timing attacks.

Each call to `hash_credential` generates a unique salt, so hashing the same secret twice produces different output. Both hashes verify against the original secret.

### Authentication Types

The handshake module (`handshake.rs`) supports three authentication types:

| Type | Usage | Verification |
|------|-------|-------------|
| `dev` | Development only | Identity lookup; empty or "admin" returns admin context |
| `token` | Production | Identity lookup + Argon2id credential verification |
| `psk` | Production | Same as token (pre-shared key treated as a credential) |

For `token` and `psk` authentication, the flow is:

1. Find the principal node by `identity` property (must have the `principal` label).
2. Check `enabled == true` on the node.
3. Verify the supplied credential against the node's `credential_hash` property.
4. Extract the `role` property and parse it into a `Role` enum.
5. Resolve the scope bitmap from the containment hierarchy.

### HTTP Bearer Tokens

HTTP authentication (`http/auth.rs`) uses `Authorization: Bearer <identity>:<secret>` headers. The `HttpAuth` axum extractor:

1. Parses the Bearer token into `identity` and `secret` at the colon delimiter.
2. Checks rate limiting before attempting authentication.
3. Calls `handshake::authenticate()` with `auth_type = "token"`.
4. On success, clears the failure record. On failure, records the attempt.

In dev mode, missing `Authorization` headers fall back to admin context. In production, missing headers return 401.

The `OptionalHttpAuth` extractor provides tiered responses -- endpoints like `/health` serve minimal information without auth and full details with auth.

## Rate Limiting

The `AuthRateLimiter` provides per-identity brute-force protection for HTTP authentication.

### Backoff Curve

After `MAX_FAILURES_BEFORE_BACKOFF` (5) failed attempts for a given identity, exponential backoff activates:

| Failures | Backoff |
|----------|---------|
| 1--4 | None |
| 5 | 2^0 = 1 second |
| 6 | 2^1 = 2 seconds |
| 7 | 2^2 = 4 seconds |
| 8 | 2^3 = 8 seconds |
| ... | ... |
| N (N >= 5) | 2^(N-5) seconds, capped at 300 seconds |

The formula is `2^(count - MAX_FAILURES_BEFORE_BACKOFF)` seconds, capped at 300 seconds (5 minutes). Backoff activates at the 5th failure (not the 6th) because the check is `count >= MAX_FAILURES_BEFORE_BACKOFF`.

### Memory Safety

The failure map is capped at 10,000 tracked identities. When the map reaches capacity:

1. Expired records (older than `RECORD_EXPIRY_SECS` = 600 seconds) are pruned.
2. If still at capacity after pruning, the new failure record is dropped (the identity is not tracked rather than growing unbounded).

Successful authentication clears the identity's failure record immediately. The `prune_expired()` method is called periodically by the background task loop.

## Vault

The vault provides encrypted, isolated storage for sensitive data. It is architecturally separate from the main graph -- no cross-graph joins are possible.

### What the Vault Stores

The vault graph is a standard `SharedGraph` accessible via the same GQL execution path as the main graph. By convention, it stores:

- **Principal nodes** -- identity, role, credential hashes, enabled status
- **API key nodes** -- token values for programmatic access
- **Cedar policies** -- policy definitions loaded from disk or managed via API
- **Default admin** -- a bootstrap `principal` node with `identity: "admin"` and `role: "admin"`, created on first vault initialization

The `USE vault` prefix in GQL routes queries to the vault graph via the graph resolver.

### Envelope Encryption

The vault uses two-layer envelope encryption:

```
Passphrase/Key File
        |
        v  (Argon2id derivation or raw bytes)
   [Master Key (KEK)]  -- 256-bit, wraps DEK
        |
        v  (XChaCha20-Poly1305 AEAD)
   [Data Encryption Key (DEK)]  -- 256-bit random, encrypts payload
        |
        v  (XChaCha20-Poly1305 AEAD with AAD)
   [Vault Payload]  -- serialized graph data
```

**Why envelope encryption?** Master key rotation only re-wraps the DEK (fast, 32-byte encryption). DEK rotation re-encrypts the payload (slower, proportional to vault size). In typical operations, master key rotation is far more common than full re-encryption.

### XChaCha20-Poly1305 AEAD

All encryption uses XChaCha20-Poly1305 from the `chacha20poly1305` crate:

- **XChaCha20**: Stream cipher with a 24-byte nonce (extended nonce avoids nonce-reuse concerns with random generation).
- **Poly1305**: MAC providing authentication. Tampered ciphertext or AAD is rejected on decryption.
- **AEAD**: Authenticated Encryption with Associated Data. The vault header fields (magic, version, timestamp) are bound as AAD, so modifying them without the key causes decryption failure.

### Key Material Safety

Both `MasterKey` and `DataKey` implement `Zeroize` on drop, scrubbing key material from memory when no longer needed. Intermediate plaintext (graph bytes after serialization, plaintext after DEK unwrap) is explicitly zeroized before being dropped.

### Vault File Format

The vault file (`secure.vault`) has a fixed 132-byte header followed by the encrypted payload:

```
Offset  Size    Field
------  ------  ---------------------------------------------------
0       4       Magic: "SVLT"
4       2       Version: 1 (u16 LE)
6       1       Key source type: 0 = raw key, 1 = passphrase-derived
7       1       Reserved (0x00)
8       16      Argon2 salt (zero-filled for raw key source)
24      24      DEK nonce (XChaCha20-Poly1305)
48      48      Encrypted DEK (32-byte key + 16-byte Poly1305 tag)
96      8       Payload write timestamp (i64 LE, nanos since epoch)
104     24      Payload nonce (XChaCha20-Poly1305)
128     4       Payload length (u32 LE)
132     N       Encrypted payload (postcard graph data + Poly1305 tag)
```

The payload is a postcard-serialized `VaultPayload` containing nodes, edges, schemas, and ID counters -- the same structure as a graph snapshot but without WAL or section-based layout. Admin operations are rare enough that atomic full-file writes (no WAL needed) are sufficient.

### Atomic Writes

Vault writes use the same atomic pattern as snapshots:

1. Write to `secure.vault.tmp`.
2. `sync_all()`.
3. `fs::rename()` to `secure.vault`.
4. Set file permissions to 0600 on Unix (owner-only read/write).

### Key Management

#### Resolution Priority

The `resolve_master_key()` function resolves the master key from multiple sources in priority order:

1. **Environment passphrase** (`SELENE_VAULT_PASSPHRASE`) -- Read and cleared from the process environment in `main()` before any threads spawn, preventing leakage via `/proc/PID/environ`. Derived via Argon2id with a 16-byte salt.
2. **Key file** (`SELENE_VAULT_KEY_FILE` env var or `vault.master_key_file` in config) -- A file containing 32 bytes encoded as base64 (44 characters) or hex (64 characters). File contents are zeroized after reading.
3. **Dev key** (dev mode only) -- All-zero 32-byte key. Same code path as production (no special casing in the crypto layer), but provides no security. A warning is logged.

If none of these sources are available and dev mode is off, vault initialization fails with an error.

#### Passphrase Derivation

When using a passphrase, the master key is derived via Argon2id with these parameters:

- **Memory**: 64 MB (`m=65536`)
- **Iterations**: 3 (`t=3`)
- **Parallelism**: 1 (`p=1`)
- **Output**: 32 bytes

These parameters produce approximately 500ms derivation time on an RPi 5. The 16-byte salt is generated randomly on first vault creation and stored in the vault file header. On reopens, the salt is read from the existing vault header to ensure the same key is derived.

#### Key Rotation

Two rotation operations are supported:

- **Master key rotation** (`rotate_master_key`): Re-wraps the existing DEK with a new master key. The payload is not re-encrypted. Fast (encrypts 32 bytes).
- **DEK rotation** (`rotate_data_key`): Generates a new random DEK and re-encrypts the entire payload. The new DEK is wrapped with the current master key. Slower (re-serializes and re-encrypts the full graph).

Both operations flush the vault to disk atomically after completion.

### VaultHandle API

The `VaultHandle` provides the public API for vault operations:

- `open_or_create(path, master, key_source, salt)` -- Opens an existing vault or creates a new one with a default admin principal.
- `flush(master)` -- Persists the current in-memory graph to the encrypted file.
- `rotate_master_key(old_master, new_master)` -- Re-wraps DEK with new master key.
- `rotate_data_key(master)` -- Generates new DEK and re-encrypts payload.
- `graph` -- The vault's `SharedGraph`, accessible for GQL queries and mutations.

The vault graph is mutated via the same `MutationBuilder` API used for the main graph. After mutations, `flush()` must be called to persist changes. This is handled automatically by the ops layer after vault-modifying operations.

### VaultService

The `VaultService` bundles the `VaultHandle` and `MasterKey` together as a registered service in the `ServiceRegistry`. This keeps the vault accessible throughout the server without global state, and ensures the master key is available for flush operations without re-deriving it.
