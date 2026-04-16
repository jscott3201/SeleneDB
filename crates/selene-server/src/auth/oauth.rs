//! OAuth 2.1 token service for MCP authentication.
//!
//! Provides JWT creation, validation, refresh token management, and a
//! deny-list for revoked tokens. This module is transport-agnostic: it
//! contains no HTTP or QUIC awareness.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use parking_lot::RwLock;
use selene_core::{NodeId, Value};
use selene_graph::SharedGraph;

use super::engine::AuthEngine;
use super::handshake::{AuthContext, AuthError};

/// Maximum number of outstanding refresh tokens. Prevents memory exhaustion
/// from leaked or abandoned token pairs.
const MAX_REFRESH_TOKENS: usize = 10_000;

/// Maximum number of entries in the revocation deny-list. Prevents unbounded
/// growth from rapid issue-then-revoke cycles between prune intervals.
const MAX_DENY_LIST: usize = 50_000;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors produced by the OAuth token service.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    #[error("token expired")]
    TokenExpired,
    #[error("token revoked")]
    TokenRevoked,
    #[error("invalid token: {0}")]
    InvalidToken(String),
    #[error("principal error: {0}")]
    PrincipalError(#[from] AuthError),
    #[error("refresh token invalid or expired")]
    InvalidRefreshToken,
    #[error("refresh token store full (max {0} entries)")]
    RefreshStoreFull(usize),
}

// ---------------------------------------------------------------------------
// JWT claims
// ---------------------------------------------------------------------------

/// Claims embedded in MCP access tokens.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct McpTokenClaims {
    /// Principal identity (`client_id`).
    pub sub: String,
    /// Cedar role name.
    pub role: String,
    /// Unique token identifier (used for revocation).
    pub jti: String,
    /// Expiry as a Unix timestamp (seconds).
    pub exp: u64,
    /// Issued-at as a Unix timestamp (seconds).
    pub iat: u64,
}

// ---------------------------------------------------------------------------
// Refresh record
// ---------------------------------------------------------------------------

/// Bookkeeping for a single outstanding refresh token.
#[derive(Debug)]
pub(crate) struct RefreshRecord {
    /// Principal identity (the `sub` claim).
    pub(crate) principal: String,
    /// Role granted at issue time. Used by `refresh_standalone` to reissue
    /// without consulting the graph. `refresh` (graph-backed) still reads
    /// the live role from the graph so role changes take effect immediately.
    pub(crate) role: String,
    /// Unix timestamp (seconds) at which the refresh token expires.
    pub(crate) expires_at: u64,
}

// ---------------------------------------------------------------------------
// Token service
// ---------------------------------------------------------------------------

/// Stateful service for issuing, validating, refreshing, and revoking
/// MCP OAuth 2.1 access tokens.
pub struct OAuthTokenService {
    /// Pre-computed HMAC-SHA256 encoding key.
    encoding_key: EncodingKey,
    /// Pre-computed HMAC-SHA256 decoding key.
    decoding_key: DecodingKey,
    /// Pre-built JWT validation rules (avoids per-request allocation).
    validation: Validation,
    /// Lifetime of an access token.
    access_ttl: Duration,
    /// Lifetime of a refresh token.
    refresh_ttl: Duration,
    /// Outstanding refresh tokens keyed by the opaque token string.
    refresh_store: RwLock<HashMap<String, RefreshRecord>>,
    /// Revoked token IDs mapped to their original expiry. Entries older
    /// than `now` are safe to prune because the corresponding JWTs have
    /// already expired.
    deny_list: RwLock<HashMap<String, u64>>,
    /// Whether the deny list has been modified since the last persistence.
    deny_dirty: AtomicBool,
    /// Cache: principal identity to (NodeId, graph generation).
    /// Avoids O(P) label scan on every validation. Invalidated when graph
    /// generation changes.
    principal_cache: RwLock<HashMap<String, (NodeId, u64)>>,
}

impl OAuthTokenService {
    /// Create a new token service.
    ///
    /// * `signing_secret` - HMAC-SHA256 key material (minimum 32 bytes
    ///   recommended).
    /// * `access_ttl`  - how long an access JWT is valid.
    /// * `refresh_ttl` - how long a refresh token is valid.
    pub fn new(signing_secret: &[u8], access_ttl: Duration, refresh_ttl: Duration) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        validation.set_required_spec_claims(&["sub", "role", "exp", "iat", "jti"]);
        validation.algorithms = vec![Algorithm::HS256];

        Self {
            encoding_key: EncodingKey::from_secret(signing_secret),
            decoding_key: DecodingKey::from_secret(signing_secret),
            validation,
            access_ttl,
            refresh_ttl,
            refresh_store: RwLock::new(HashMap::new()),
            deny_list: RwLock::new(HashMap::new()),
            deny_dirty: AtomicBool::new(false),
            principal_cache: RwLock::new(HashMap::new()),
        }
    }

    // -- Issue ---------------------------------------------------------------

    /// Issue a new (access JWT, refresh token) pair.
    ///
    /// Returns `Err(RefreshStoreFull)` if the refresh store has reached its
    /// capacity limit.
    pub fn issue(&self, principal: &str, role: &str) -> Result<(String, String), OAuthError> {
        let now = now_secs();
        let jti = uuid_v4();

        let claims = McpTokenClaims {
            sub: principal.to_string(),
            role: role.to_string(),
            jti,
            exp: now + self.access_ttl.as_secs(),
            iat: now,
        };

        let jwt = encode(&Header::default(), &claims, &self.encoding_key)
            .map_err(|e| OAuthError::InvalidToken(format!("JWT encode error: {e}")))?;

        // Generate an opaque refresh token and atomically check capacity +
        // insert under a single write lock to avoid a TOCTOU race.
        let refresh_token = uuid_v4();
        {
            let mut store = self.refresh_store.write();
            if store.len() >= MAX_REFRESH_TOKENS {
                return Err(OAuthError::RefreshStoreFull(MAX_REFRESH_TOKENS));
            }
            store.insert(
                refresh_token.clone(),
                RefreshRecord {
                    principal: principal.to_string(),
                    role: role.to_string(),
                    expires_at: now + self.refresh_ttl.as_secs(),
                },
            );
        }

        Ok((jwt, refresh_token))
    }

    // -- Validate ------------------------------------------------------------

    /// Validate an access token and return a full `AuthContext`.
    ///
    /// Performs: signature verification, expiry check (via `jsonwebtoken`),
    /// deny-list lookup, principal graph lookup, and scope resolution.
    pub fn validate(&self, token: &str, graph: &SharedGraph) -> Result<AuthContext, OAuthError> {
        // 1+2. Decode and verify HMAC signature + expiry (pre-built Validation).
        let token_data = decode::<McpTokenClaims>(token, &self.decoding_key, &self.validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => OAuthError::TokenExpired,
                _ => OAuthError::InvalidToken(e.to_string()),
            })?;

        let claims = token_data.claims;

        // 3. Deny-list check.
        {
            let deny = self.deny_list.read();
            if deny.contains_key(&claims.jti) {
                return Err(OAuthError::TokenRevoked);
            }
        }

        // 4. Look up principal in graph (cache-accelerated).
        let current_gen = graph.containment_generation();

        // Try cache first to avoid O(P) label scan.
        let cached = self.principal_cache.read().get(&claims.sub).copied();
        if let Some((cached_id, cached_gen)) = cached
            && cached_gen == current_gen
        {
            // Cache hit: verify the node is still enabled before using it.
            let still_enabled = graph.read(|g| {
                g.get_node(cached_id).is_some_and(|n| {
                    n.property("enabled")
                        .is_some_and(|v| matches!(v, Value::Bool(true)))
                })
            });
            if still_enabled {
                return graph.read(|g| build_auth_context(g, cached_id, &claims.sub, current_gen));
            }
            // Enabled check failed: evict stale entry, fall through to full scan.
            self.principal_cache.write().remove(&claims.sub);
        }

        // Cache miss or stale generation: full label scan.
        graph.read(|g| {
            let principal_id = find_enabled_principal(g, &claims.sub)?;
            self.principal_cache
                .write()
                .insert(claims.sub.clone(), (principal_id, current_gen));
            build_auth_context(g, principal_id, &claims.sub, current_gen)
        })
    }

    /// Validate a JWT without a graph lookup.
    ///
    /// Performs signature verification, expiry check, and deny-list lookup,
    /// but does NOT verify the principal exists in the graph. Returns the
    /// decoded claims. Use this when the graph is not accessible (e.g.,
    /// Aether server validating tokens independently of SeleneDB).
    pub fn validate_standalone(&self, token: &str) -> Result<McpTokenClaims, OAuthError> {
        let token_data = decode::<McpTokenClaims>(token, &self.decoding_key, &self.validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => OAuthError::TokenExpired,
                _ => OAuthError::InvalidToken(e.to_string()),
            })?;

        let claims = token_data.claims;

        // Deny-list check.
        {
            let deny = self.deny_list.read();
            if deny.contains_key(&claims.jti) {
                return Err(OAuthError::TokenRevoked);
            }
        }

        Ok(claims)
    }

    // -- Refresh -------------------------------------------------------------

    /// Exchange a refresh token for a new (access JWT, refresh token) pair.
    ///
    /// The incoming refresh token is consumed (single-use). The principal is
    /// re-validated against the graph to ensure it is still enabled.
    pub fn refresh(
        &self,
        refresh_token: &str,
        graph: &SharedGraph,
    ) -> Result<(String, String), OAuthError> {
        // 1. Remove from store (single-use).
        let record = {
            let mut store = self.refresh_store.write();
            store
                .remove(refresh_token)
                .ok_or(OAuthError::InvalidRefreshToken)?
        };

        // 2. Check expiry.
        if now_secs() >= record.expires_at {
            return Err(OAuthError::InvalidRefreshToken);
        }

        // 3. Re-validate principal and read current role from graph.
        let role_str = graph.read(|g| -> Result<String, OAuthError> {
            let principal_id = find_enabled_principal(g, &record.principal)?;
            let node = g
                .get_node(principal_id)
                .ok_or_else(|| AuthError::PrincipalNotFound(record.principal.clone()))?;
            let role_str = node
                .property("role")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .ok_or_else(|| AuthError::MissingRole(record.principal.clone()))?;
            Ok(role_str)
        })?;

        // 4. Issue new pair with current role.
        self.issue(&record.principal, &role_str)
    }

    /// Exchange a refresh token for a new pair without graph lookup.
    ///
    /// Like [`refresh`] but skips principal re-validation against the graph.
    /// Use this when the graph is not accessible (e.g., Aether server
    /// operating independently of SeleneDB's internal graph).
    pub fn refresh_standalone(&self, refresh_token: &str) -> Result<(String, String), OAuthError> {
        let record = {
            let mut store = self.refresh_store.write();
            store
                .remove(refresh_token)
                .ok_or(OAuthError::InvalidRefreshToken)?
        };

        if now_secs() >= record.expires_at {
            return Err(OAuthError::InvalidRefreshToken);
        }

        // Re-issue with the originally-granted role. The role was captured
        // at `issue()` time; refresh_standalone cannot consult the graph to
        // pick up post-issue role changes, so stored role is authoritative.
        self.issue(&record.principal, &record.role)
    }

    // -- Revoke / prune ------------------------------------------------------

    /// Revoke an access token by its raw JWT string.
    ///
    /// Decodes the token (allowing expired tokens — you can revoke a token
    /// that has already expired to ensure it stays denied after a restart
    /// when deny list is persisted). Extracts the `jti` and `exp` claims
    /// and adds them to the deny list.
    pub fn revoke_token(&self, token: &str) -> Result<(), OAuthError> {
        // Decode allowing expired tokens (we still want to revoke them)
        let mut relaxed = self.validation.clone();
        relaxed.validate_exp = false;
        let token_data = decode::<McpTokenClaims>(token, &self.decoding_key, &relaxed)
            .map_err(|e| OAuthError::InvalidToken(e.to_string()))?;
        self.revoke(&token_data.claims.jti, token_data.claims.exp);
        Ok(())
    }

    /// Add a token ID to the deny-list. The `original_exp` timestamp allows
    /// automatic pruning once the token would have expired anyway.
    pub fn revoke(&self, jti: &str, original_exp: u64) {
        let mut deny = self.deny_list.write();
        if deny.len() >= MAX_DENY_LIST {
            let now = now_secs();
            deny.retain(|_, exp| *exp >= now);
        }
        if deny.len() >= MAX_DENY_LIST {
            tracing::warn!(
                jti,
                "deny-list at capacity after pruning; skipping revocation"
            );
            return;
        }
        deny.insert(jti.to_string(), original_exp);
        self.deny_dirty.store(true, Ordering::Release);
    }

    /// Remove expired refresh tokens and deny-list entries whose original
    /// expiry has passed. Also clears the principal cache so stale entries
    /// repopulate on the next validation.
    pub fn prune_expired(&self) {
        let now = now_secs();

        {
            let mut store = self.refresh_store.write();
            store.retain(|_, rec| rec.expires_at > now);
        }
        {
            let mut deny = self.deny_list.write();
            deny.retain(|_, &mut exp| exp >= now);
        }
        // Clear principal cache; entries repopulate on next validate().
        self.principal_cache.write().clear();
    }

    // -- State persistence ----------------------------------------------------

    /// Check whether the deny list has been modified since the last snapshot.
    pub(crate) fn deny_list_dirty(&self) -> bool {
        self.deny_dirty.load(Ordering::Acquire)
    }

    /// Snapshot the current deny list for vault persistence.
    ///
    /// Returns `(unused, deny_list_entries)` where each deny entry is
    /// `(jti, expires_at)`. The first element is empty (reserved for future
    /// refresh token persistence if a non-hashed approach is adopted).
    ///
    /// Clears the dirty flag — the next snapshot will only persist if new
    /// revocations have been added since this call.
    pub(crate) fn snapshot_state(&self) -> ((), Vec<(String, u64)>) {
        let denied: Vec<(String, u64)> = self
            .deny_list
            .read()
            .iter()
            .map(|(jti, exp)| (jti.clone(), *exp))
            .collect();

        self.deny_dirty.store(false, Ordering::Release);
        ((), denied)
    }

    /// Load persisted deny list entries from the vault into the in-memory store.
    ///
    /// Only the deny list is persisted across restarts (security-critical —
    /// prevents revoked tokens from being reused). Refresh tokens are ephemeral
    /// and reset on restart; clients re-authenticate via OAuth, and their
    /// access tokens remain valid via the persisted signing key.
    pub(crate) fn load_deny_list(&self, entries: Vec<(String, u64)>) {
        let now = now_secs();
        let mut deny = self.deny_list.write();
        for (jti, exp) in entries {
            if exp >= now {
                deny.insert(jti, exp);
            }
        }
        tracing::info!(count = deny.len(), "loaded deny list from vault");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build an `AuthContext` from a known-valid principal node.
///
/// Reads the live role from the graph (not the JWT claim) so that role
/// changes take effect immediately, then resolves the scope bitmap.
fn build_auth_context(
    g: &selene_graph::SeleneGraph,
    principal_id: NodeId,
    identity: &str,
    scope_gen: u64,
) -> Result<AuthContext, OAuthError> {
    let node = g
        .get_node(principal_id)
        .ok_or_else(|| AuthError::PrincipalNotFound(identity.to_string()))?;
    let role_str = node
        .property("role")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .ok_or_else(|| AuthError::MissingRole(identity.to_string()))?;
    let role: super::Role = role_str
        .parse()
        .map_err(|_| AuthError::InvalidRole(role_str))?;
    let scope = AuthEngine::resolve_scope(g, principal_id, role).unwrap_or_default();

    Ok(AuthContext {
        principal_node_id: principal_id,
        role,
        scope,
        scope_generation: scope_gen,
    })
}

/// Find a principal node by `identity` property and verify it is enabled.
fn find_enabled_principal(
    g: &selene_graph::SeleneGraph,
    identity: &str,
) -> Result<NodeId, OAuthError> {
    let principal_id = super::handshake::find_principal_by_identity(g, identity)?;

    let node = g
        .get_node(principal_id)
        .ok_or_else(|| AuthError::PrincipalNotFound(identity.to_string()))?;

    let enabled = node
        .property("enabled")
        .is_some_and(|v| matches!(v, Value::Bool(true)));
    if !enabled {
        return Err(AuthError::PrincipalDisabled(identity.to_string()).into());
    }

    Ok(principal_id)
}

/// Generate a version-4 (random) UUID without an external crate.
///
/// Produces the standard 8-4-4-4-12 hex format with the version and variant
/// bits set per RFC 9562.
pub(crate) fn uuid_v4() -> String {
    use rand::RngExt;

    let mut bytes = [0u8; 16];
    rand::rng().fill(&mut bytes[..]);

    // Set version (4) and variant (RFC 9562: 10xx).
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

/// Current Unix timestamp in seconds.
pub(crate) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_service() -> OAuthTokenService {
        OAuthTokenService::new(
            b"test-secret-key-at-least-32-bytes!",
            Duration::from_secs(300),
            Duration::from_secs(600),
        )
    }

    fn test_service_with_ttl(access_secs: u64, refresh_secs: u64) -> OAuthTokenService {
        OAuthTokenService::new(
            b"test-secret-key-at-least-32-bytes!",
            Duration::from_secs(access_secs),
            Duration::from_secs(refresh_secs),
        )
    }

    #[test]
    fn issue_and_validate_claims() {
        let svc = test_service();
        let (jwt, refresh) = svc.issue("alice", "operator").unwrap();

        // Both tokens are non-empty.
        assert!(!jwt.is_empty());
        assert!(!refresh.is_empty());

        // Decode the JWT directly and inspect claims.
        let validation = Validation::new(Algorithm::HS256);
        let data = decode::<McpTokenClaims>(
            &jwt,
            &DecodingKey::from_secret(b"test-secret-key-at-least-32-bytes!"),
            &validation,
        )
        .unwrap();

        assert_eq!(data.claims.sub, "alice");
        assert_eq!(data.claims.role, "operator");
        assert!(data.claims.exp > data.claims.iat);
        assert!(!data.claims.jti.is_empty());
        // jti should be a valid UUID v4 format (8-4-4-4-12).
        assert_eq!(data.claims.jti.len(), 36);
        assert_eq!(data.claims.jti.chars().filter(|&c| c == '-').count(), 4);
    }

    #[test]
    fn expired_token_rejected() {
        let secret = b"test-secret-key-at-least-32-bytes!";

        // Manually encode a token with exp in the past.
        let claims = McpTokenClaims {
            sub: "bob".to_string(),
            role: "reader".to_string(),
            jti: uuid_v4(),
            exp: 1, // Unix timestamp 1 (1970-01-01) -- long expired
            iat: 0,
        };
        let jwt = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap();

        // Decoding should fail with ExpiredSignature (leeway = 0).
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        let result = decode::<McpTokenClaims>(&jwt, &DecodingKey::from_secret(secret), &validation);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err().kind(),
            jsonwebtoken::errors::ErrorKind::ExpiredSignature
        ));
    }

    #[test]
    fn revoked_token_in_deny_list() {
        let svc = test_service();
        let (jwt, _) = svc.issue("carol", "admin").unwrap();

        // Extract the jti from the JWT (skip verification, test only).
        let data = jsonwebtoken::dangerous::insecure_decode::<McpTokenClaims>(&jwt).unwrap();
        let jti = data.claims.jti;
        let exp = data.claims.exp;

        svc.revoke(&jti, exp);

        let deny = svc.deny_list.read();
        assert!(deny.contains_key(&jti));
        assert_eq!(*deny.get(&jti).unwrap(), exp);
    }

    #[test]
    fn refresh_token_single_use() {
        let svc = test_service();
        let (_, refresh) = svc.issue("dave", "operator").unwrap();

        // The refresh token should be present in the store.
        assert!(svc.refresh_store.read().contains_key(&refresh));

        // Remove it (simulating consumption).
        let removed = svc.refresh_store.write().remove(&refresh);
        assert!(removed.is_some());

        // Now it should be gone.
        assert!(!svc.refresh_store.read().contains_key(&refresh));
    }

    #[test]
    fn prune_removes_expired() {
        // Both TTLs set to 0 so everything expires immediately.
        let svc = test_service_with_ttl(0, 0);
        let _ = svc.issue("eve", "reader").unwrap();

        // Add an expired deny-list entry (exp in the past).
        svc.deny_list.write().insert("old-jti".to_string(), 0);

        // Before prune: stores are non-empty.
        assert!(!svc.refresh_store.read().is_empty());
        assert!(!svc.deny_list.read().is_empty());

        svc.prune_expired();

        assert!(svc.refresh_store.read().is_empty());
        assert!(svc.deny_list.read().is_empty());
    }

    #[test]
    fn refresh_store_bounded() {
        let svc = test_service();

        // Fill the refresh store to capacity.
        for i in 0..MAX_REFRESH_TOKENS {
            svc.issue(&format!("user-{i}"), "reader").unwrap();
        }

        assert_eq!(svc.refresh_store.read().len(), MAX_REFRESH_TOKENS);

        // The next issue should fail.
        let result = svc.issue("one-too-many", "reader");
        assert!(matches!(result, Err(OAuthError::RefreshStoreFull(10_000))));
    }

    #[test]
    fn uuid_v4_format() {
        let id = uuid_v4();
        assert_eq!(id.len(), 36);
        // Check version nibble (position 14 in the string is the first hex char of byte 6).
        assert_eq!(&id[14..15], "4");
        // Check variant nibble (position 19): must be 8, 9, a, or b.
        let variant = &id[19..20];
        assert!(
            variant == "8" || variant == "9" || variant == "a" || variant == "b",
            "variant nibble was {variant}"
        );
        // Dashes at positions 8, 13, 18, 23.
        assert_eq!(&id[8..9], "-");
        assert_eq!(&id[13..14], "-");
        assert_eq!(&id[18..19], "-");
        assert_eq!(&id[23..24], "-");
    }

    #[test]
    fn revoke_token_adds_to_deny_list() {
        let svc = test_service();
        let (jwt, _) = svc.issue("frank", "operator").unwrap();

        // Revoke by raw JWT string
        svc.revoke_token(&jwt).unwrap();

        // Verify the jti is in the deny list
        let data = jsonwebtoken::dangerous::insecure_decode::<McpTokenClaims>(&jwt).unwrap();
        let deny = svc.deny_list.read();
        assert!(deny.contains_key(&data.claims.jti));
    }

    #[test]
    fn refresh_standalone_preserves_original_role() {
        let svc = test_service();

        for role in ["reader", "device", "operator", "admin", "service"] {
            let (_, refresh) = svc.issue("alice", role).unwrap();
            let (jwt, _) = svc.refresh_standalone(&refresh).unwrap();

            let data = jsonwebtoken::dangerous::insecure_decode::<McpTokenClaims>(&jwt).unwrap();
            assert_eq!(
                data.claims.role, role,
                "refresh_standalone must reissue with the original role"
            );
            assert_eq!(data.claims.sub, "alice");
        }
    }

    #[test]
    fn revoke_token_rejects_invalid_jwt() {
        let svc = test_service();
        let result = svc.revoke_token("not-a-valid-jwt");
        assert!(matches!(result, Err(OAuthError::InvalidToken(_))));
    }
}
