//! API-key management — named, revocable bearer tokens stored in the vault.
//!
//! API keys are issued in the format `selk_<prefix>.<secret>` where:
//! - `prefix` is 12 random alphanumeric chars — non-secret, indexable for lookup
//! - `secret` is 32 random alphanumeric chars — secret, stored only as an argon2id hash
//!
//! On verification we parse the token, look up the single `api_key` node with
//! the matching prefix, then argon2-verify the secret. This avoids scanning
//! every row (argon2 is intentionally ~100 ms per verify).
//!
//! Admin-only: all issue/list/revoke operations require `Action::PrincipalManage`.
//! Verification itself is not admin-gated — the verified token produces an
//! `AuthContext` for the principal identity the key was issued to.

use rand::RngExt;
use rand::distr::Alphanumeric;
use serde::Serialize;

use crate::auth::Role;
use crate::auth::credential::{hash_credential, verify_credential};
use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use crate::vault::VaultService;

use super::OpError;

/// Prefix length (non-secret, indexable).
pub const PREFIX_LEN: usize = 12;
/// Secret length (stored only as argon2id hash).
pub const SECRET_LEN: usize = 32;
/// Token scheme tag — makes leaked tokens grep-able (cf. `ghp_`, `gho_`).
pub const TOKEN_PREFIX: &str = "selk_";

/// Public view of an API key — safe to return to callers.
///
/// Deliberately omits `hash`. The raw token is only ever returned once, at
/// issuance, via [`CreateApiKeyResult`].
#[derive(Debug, Serialize)]
pub struct ApiKeyDto {
    pub id: u64,
    pub name: String,
    pub identity: String,
    pub prefix: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub scopes: Option<Vec<String>>,
    pub enabled: bool,
}

/// Returned from [`create_api_key`] — includes the one-time plaintext token.
///
/// Callers must display `plaintext_token` to the user immediately; it cannot
/// be recovered afterwards because only the argon2id hash is persisted.
#[derive(Debug, Serialize)]
pub struct CreateApiKeyResult {
    pub key: ApiKeyDto,
    pub plaintext_token: String,
}

// ── Helpers ──────────────────────────────────────────────────────────

fn require_principal_manage(state: &ServerState, auth: &AuthContext) -> Result<(), OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::PrincipalManage)
    {
        return Err(OpError::AuthDenied);
    }
    Ok(())
}

fn vault_service(state: &ServerState) -> Result<&VaultService, OpError> {
    state
        .services
        .get::<VaultService>()
        .ok_or_else(|| OpError::Internal("vault not available".into()))
}

fn now_nanos() -> i64 {
    selene_core::entity::now_nanos()
}

fn random_alphanumeric(len: usize) -> String {
    rand::rng()
        .sample_iter(Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn string_prop(node: &selene_graph::NodeRef<'_>, key: &str) -> Option<String> {
    node.property(key)
        .and_then(|v| v.as_str())
        .map(str::to_owned)
}

fn list_prop_strings(node: &selene_graph::NodeRef<'_>, key: &str) -> Option<Vec<String>> {
    match node.property(key)? {
        selene_core::Value::List(items) => Some(
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect(),
        ),
        _ => None,
    }
}

fn node_to_dto(g: &selene_graph::SeleneGraph, nid: selene_core::NodeId) -> Option<ApiKeyDto> {
    let node = g.get_node(nid)?;
    Some(ApiKeyDto {
        id: nid.0,
        name: string_prop(&node, "name").unwrap_or_default(),
        identity: string_prop(&node, "identity").unwrap_or_default(),
        prefix: string_prop(&node, "prefix").unwrap_or_default(),
        created_at: node
            .property("created_at")
            .and_then(|v| match v {
                selene_core::Value::Int(i) => Some(*i),
                selene_core::Value::Timestamp(t) => Some(*t),
                _ => None,
            })
            .unwrap_or(0),
        expires_at: node.property("expires_at").and_then(|v| match v {
            selene_core::Value::Int(i) => Some(*i),
            selene_core::Value::Timestamp(t) => Some(*t),
            _ => None,
        }),
        scopes: list_prop_strings(&node, "scopes"),
        enabled: node
            .property("enabled")
            .is_some_and(|v| matches!(v, selene_core::Value::Bool(true))),
    })
}

fn find_by_prefix(g: &selene_graph::SeleneGraph, prefix: &str) -> Option<selene_core::NodeId> {
    g.nodes_by_label("api_key").find(|&nid| {
        g.get_node(nid).is_some_and(|n| {
            n.property("prefix")
                .and_then(|v| v.as_str())
                .is_some_and(|p| p == prefix)
        })
    })
}

fn find_by_id(g: &selene_graph::SeleneGraph, id: u64) -> Option<selene_core::NodeId> {
    let nid = selene_core::NodeId(id);
    // Label check guards against caller passing an arbitrary node ID.
    let node = g.get_node(nid)?;
    if node.labels.iter().any(|l| l.as_str() == "api_key") {
        Some(nid)
    } else {
        None
    }
}

// ── Operations ───────────────────────────────────────────────────────

/// Create and persist a new API key for `identity`.
///
/// Returns the DTO plus the plaintext token (one-time). The caller is
/// responsible for ensuring the principal named by `identity` exists; we
/// accept any identity string so keys can be pre-provisioned.
pub fn create_api_key(
    state: &ServerState,
    auth: &AuthContext,
    name: &str,
    identity: &str,
    ttl_days: Option<u32>,
    scopes: Option<Vec<String>>,
) -> Result<CreateApiKeyResult, OpError> {
    require_principal_manage(state, auth)?;

    if name.trim().is_empty() {
        return Err(OpError::InvalidRequest(
            "api key name cannot be empty".into(),
        ));
    }
    if identity.trim().is_empty() {
        return Err(OpError::InvalidRequest(
            "api key identity cannot be empty".into(),
        ));
    }

    let vs = vault_service(state)?;

    // Generate prefix. Retry on the astronomically unlikely collision rather
    // than proceeding with a duplicate (would break the prefix→row lookup).
    // If every attempt clashes, bail rather than persisting an ambiguous key.
    const PREFIX_ATTEMPTS: usize = 5;
    let mut prefix = String::new();
    let mut found_unique = false;
    for _ in 0..PREFIX_ATTEMPTS {
        prefix = random_alphanumeric(PREFIX_LEN);
        let clash = vs
            .handle
            .graph
            .read(|g| find_by_prefix(g, &prefix).is_some());
        if !clash {
            found_unique = true;
            break;
        }
    }
    if !found_unique {
        return Err(OpError::Internal(
            "could not generate a unique api key prefix".into(),
        ));
    }

    let secret = random_alphanumeric(SECRET_LEN);
    let plaintext_token = format!("{TOKEN_PREFIX}{prefix}.{secret}");

    let hash = hash_credential(&secret)
        .map_err(|e| OpError::Internal(format!("api key hash failed: {e}")))?;

    let created_at = now_nanos();
    let expires_at =
        ttl_days.map(|days| created_at.saturating_add(i64::from(days) * 86_400_000_000_000));

    let mut props = vec![
        (
            selene_core::IStr::new("name"),
            selene_core::Value::str(name),
        ),
        (
            selene_core::IStr::new("identity"),
            selene_core::Value::str(identity),
        ),
        (
            selene_core::IStr::new("prefix"),
            selene_core::Value::str(&prefix),
        ),
        (
            selene_core::IStr::new("hash"),
            selene_core::Value::str(&hash),
        ),
        (
            selene_core::IStr::new("created_at"),
            selene_core::Value::Int(created_at),
        ),
        (
            selene_core::IStr::new("enabled"),
            selene_core::Value::Bool(true),
        ),
    ];
    if let Some(exp) = expires_at {
        props.push((
            selene_core::IStr::new("expires_at"),
            selene_core::Value::Int(exp),
        ));
    }
    if let Some(list) = scopes.as_ref() {
        let items: Vec<selene_core::Value> =
            list.iter().map(|s| selene_core::Value::str(s)).collect();
        props.push((
            selene_core::IStr::new("scopes"),
            selene_core::Value::List(std::sync::Arc::from(items)),
        ));
    }

    let (nid, _changes) = vs
        .handle
        .graph
        .write(|m| {
            m.create_node(
                selene_core::LabelSet::from_strs(&["api_key"]),
                selene_core::PropertyMap::from_pairs(props),
            )
        })
        .map_err(|e| OpError::Internal(format!("vault write failed: {e}")))?;

    vs.handle
        .flush(&vs.master_key)
        .map_err(|e| OpError::Internal(format!("vault flush failed: {e}")))?;

    let dto = vs
        .handle
        .graph
        .read(|g| node_to_dto(g, nid))
        .ok_or_else(|| OpError::Internal("api key created but not readable".into()))?;

    Ok(CreateApiKeyResult {
        key: dto,
        plaintext_token,
    })
}

/// List API keys. When `identity` is `Some`, filters to keys issued to that
/// principal.
pub fn list_api_keys(
    state: &ServerState,
    auth: &AuthContext,
    identity: Option<&str>,
) -> Result<Vec<ApiKeyDto>, OpError> {
    require_principal_manage(state, auth)?;
    let vs = vault_service(state)?;

    let keys = vs.handle.graph.read(|g| {
        g.nodes_by_label("api_key")
            .filter_map(|nid| node_to_dto(g, nid))
            .filter(|dto| identity.is_none_or(|id| dto.identity == id))
            .collect()
    });

    Ok(keys)
}

/// Revoke (disable) an API key by node ID. Idempotent.
pub fn revoke_api_key(
    state: &ServerState,
    auth: &AuthContext,
    key_id: u64,
) -> Result<ApiKeyDto, OpError> {
    require_principal_manage(state, auth)?;
    let vs = vault_service(state)?;

    let nid = vs
        .handle
        .graph
        .read(|g| find_by_id(g, key_id))
        .ok_or_else(|| OpError::InvalidRequest(format!("api key {key_id} not found")))?;

    vs.handle
        .graph
        .write(|m| {
            m.set_property(
                nid,
                selene_core::IStr::new("enabled"),
                selene_core::Value::Bool(false),
            )
        })
        .map_err(|e| OpError::Internal(format!("vault write failed: {e}")))?;

    vs.handle
        .flush(&vs.master_key)
        .map_err(|e| OpError::Internal(format!("vault flush failed: {e}")))?;

    vs.handle
        .graph
        .read(|g| node_to_dto(g, nid))
        .ok_or_else(|| OpError::Internal("api key revoked but not readable".into()))
}

/// Verify a bearer token and return the authenticated principal's role.
///
/// Steps:
/// 1. Parse `selk_<prefix>.<secret>` — reject any other format.
/// 2. Look up the api_key node by prefix (single index probe).
/// 3. Check enabled + not-expired.
/// 4. Argon2id-verify the secret against the stored hash.
/// 5. Resolve the owning principal's role from the vault.
///
/// Returns `(identity, role)`. Callers are expected to build an `AuthContext`
/// from this (the scope bitmap belongs to the main graph, not the vault).
pub fn verify_api_key(state: &ServerState, token: &str) -> Result<(String, Role), OpError> {
    let Some(rest) = token.strip_prefix(TOKEN_PREFIX) else {
        return Err(OpError::AuthDenied);
    };
    let Some((prefix, secret)) = rest.split_once('.') else {
        return Err(OpError::AuthDenied);
    };
    if prefix.len() != PREFIX_LEN || secret.len() != SECRET_LEN {
        return Err(OpError::AuthDenied);
    }

    let vs = vault_service(state)?;

    // Read-only lookup of the row. Returns (identity, hash, enabled, expires_at).
    let row = vs.handle.graph.read(|g| {
        let nid = find_by_prefix(g, prefix)?;
        let n = g.get_node(nid)?;
        Some((
            string_prop(&n, "identity").unwrap_or_default(),
            string_prop(&n, "hash").unwrap_or_default(),
            n.property("enabled")
                .is_some_and(|v| matches!(v, selene_core::Value::Bool(true))),
            n.property("expires_at").and_then(|v| match v {
                selene_core::Value::Int(i) => Some(*i),
                selene_core::Value::Timestamp(t) => Some(*t),
                _ => None,
            }),
        ))
    });

    let (identity, hash, enabled, expires_at) = row.ok_or(OpError::AuthDenied)?;

    if !enabled {
        return Err(OpError::AuthDenied);
    }
    if let Some(exp) = expires_at
        && exp <= now_nanos()
    {
        return Err(OpError::AuthDenied);
    }

    let ok = verify_credential(secret, &hash).map_err(|_| OpError::AuthDenied)?;
    if !ok {
        return Err(OpError::AuthDenied);
    }

    // Resolve role from the principal with matching identity.
    let role_str = vs
        .handle
        .graph
        .read(|g| {
            g.nodes_by_label("principal").find_map(|nid| {
                let n = g.get_node(nid)?;
                if n.property("identity").and_then(|v| v.as_str()) == Some(identity.as_str()) {
                    string_prop(&n, "role")
                } else {
                    None
                }
            })
        })
        .ok_or(OpError::AuthDenied)?;

    let role: Role = role_str.parse().map_err(|_| OpError::AuthDenied)?;
    Ok((identity, role))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::ServerState;

    async fn test_state_with_vault() -> (ServerState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServerState::for_testing(dir.path()).await;

        let master = crate::vault::crypto::MasterKey::dev_key();
        let vault_path = dir.path().join("secure.vault");
        let (handle, _) = crate::vault::VaultHandle::open_or_create(
            vault_path,
            &master,
            crate::vault::KeySource::Raw,
            [0u8; 16],
        )
        .unwrap();
        state.services.register(crate::vault::VaultService::new(
            std::sync::Arc::new(handle),
            std::sync::Arc::new(master),
        ));
        (state, dir)
    }

    fn admin_ctx() -> AuthContext {
        AuthContext::dev_admin()
    }

    #[test]
    fn random_alphanumeric_length() {
        let s = random_alphanumeric(12);
        assert_eq!(s.len(), 12);
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[tokio::test]
    async fn create_returns_plaintext_and_hides_hash() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = admin_ctx();
        let result = create_api_key(&state, &auth, "ci-bot", "admin", None, None).unwrap();
        assert!(result.plaintext_token.starts_with("selk_"));
        let rest = result.plaintext_token.trim_start_matches("selk_");
        let (p, s) = rest.split_once('.').unwrap();
        assert_eq!(p.len(), PREFIX_LEN);
        assert_eq!(s.len(), SECRET_LEN);
        assert_eq!(result.key.prefix, p);
        assert_eq!(result.key.identity, "admin");
        assert!(result.key.enabled);
        let json = serde_json::to_string(&result.key).unwrap();
        assert!(!json.contains("hash"));
    }

    #[tokio::test]
    async fn verify_roundtrip() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = admin_ctx();
        let result = create_api_key(&state, &auth, "bot", "admin", None, None).unwrap();
        let (identity, role) = verify_api_key(&state, &result.plaintext_token).unwrap();
        assert_eq!(identity, "admin");
        assert_eq!(role, Role::Admin);
    }

    #[tokio::test]
    async fn verify_rejects_malformed() {
        let (state, _dir) = test_state_with_vault().await;
        assert!(matches!(
            verify_api_key(&state, "not-a-token"),
            Err(OpError::AuthDenied)
        ));
        assert!(matches!(
            verify_api_key(&state, "selk_short.secret"),
            Err(OpError::AuthDenied)
        ));
    }

    #[tokio::test]
    async fn verify_rejects_revoked() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = admin_ctx();
        let result = create_api_key(&state, &auth, "bot", "admin", None, None).unwrap();
        revoke_api_key(&state, &auth, result.key.id).unwrap();
        assert!(matches!(
            verify_api_key(&state, &result.plaintext_token),
            Err(OpError::AuthDenied)
        ));
    }

    #[tokio::test]
    async fn verify_rejects_expired() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = admin_ctx();
        let result = create_api_key(&state, &auth, "bot", "admin", None, None).unwrap();
        let vs = vault_service(&state).unwrap();
        let nid = selene_core::NodeId(result.key.id);
        vs.handle
            .graph
            .write(|m| {
                m.set_property(
                    nid,
                    selene_core::IStr::new("expires_at"),
                    selene_core::Value::Int(1),
                )
            })
            .unwrap();
        vs.handle.flush(&vs.master_key).unwrap();
        assert!(matches!(
            verify_api_key(&state, &result.plaintext_token),
            Err(OpError::AuthDenied)
        ));
    }

    #[tokio::test]
    async fn list_filters_by_identity() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = admin_ctx();
        create_api_key(&state, &auth, "a", "admin", None, None).unwrap();
        create_api_key(&state, &auth, "b", "admin", None, None).unwrap();
        crate::ops::principals::create_principal(&state, &auth, "svc", "service", None).unwrap();
        create_api_key(&state, &auth, "c", "svc", None, None).unwrap();

        let all = list_api_keys(&state, &auth, None).unwrap();
        assert_eq!(all.len(), 3);
        let only_admin = list_api_keys(&state, &auth, Some("admin")).unwrap();
        assert_eq!(only_admin.len(), 2);
        let only_svc = list_api_keys(&state, &auth, Some("svc")).unwrap();
        assert_eq!(only_svc.len(), 1);
    }

    #[tokio::test]
    async fn empty_name_rejected() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = admin_ctx();
        let err = create_api_key(&state, &auth, "   ", "admin", None, None).unwrap_err();
        assert!(matches!(err, OpError::InvalidRequest(_)));
    }
}
