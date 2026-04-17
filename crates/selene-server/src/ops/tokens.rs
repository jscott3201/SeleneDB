//! OAuth token revocation operations.
//!
//! Thin ops wrappers around the OAuth service's deny-list primitives.
//! Authorization: `Action::PrincipalManage` (admin-only). Revocation is a
//! privileged action — any caller that can revoke tokens can effectively
//! log out arbitrary principals.

use serde::Serialize;

use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use crate::http::mcp::oauth::OAuthService;

use super::OpError;

/// A single deny-list entry.
#[derive(Debug, Serialize)]
pub struct RevokedTokenDto {
    /// JWT `jti` claim of the revoked token.
    pub jti: String,
    /// Original expiry as a Unix timestamp (seconds). After this point the
    /// entry is pruned since the token is already expired.
    pub expires_at: u64,
}

/// Result of a revoke call.
#[derive(Debug, Serialize)]
pub struct RevokeTokenResult {
    pub jti: String,
    pub expires_at: u64,
}

/// Result of an unrevoke call.
#[derive(Debug, Serialize)]
pub struct UnrevokeTokenResult {
    pub jti: String,
    /// `true` if the entry existed and was removed. `false` if the jti was
    /// not on the deny-list.
    pub removed: bool,
}

fn require_admin(state: &ServerState, auth: &AuthContext) -> Result<(), OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::PrincipalManage)
    {
        return Err(OpError::AuthDenied);
    }
    Ok(())
}

fn oauth_service(state: &ServerState) -> Result<&OAuthService, OpError> {
    state
        .services
        .get::<OAuthService>()
        .ok_or_else(|| OpError::Internal("OAuth service not available".into()))
}

/// Revoke an access token by its raw JWT string. The deny-list entry
/// persists until the token's original expiry (when it would have become
/// invalid anyway).
pub fn revoke_token(
    state: &ServerState,
    auth: &AuthContext,
    token: &str,
) -> Result<RevokeTokenResult, OpError> {
    require_admin(state, auth)?;
    let oauth = oauth_service(state)?;

    // The service decodes the token (allowing expired), inserts into the
    // deny-list, and hands back the jti + original expiry. Passing those
    // through keeps the response deterministic — even for an already-expired
    // token that `list_revoked` would have filtered out, and even when other
    // entries with later expiries already live on the deny-list.
    let (jti, expires_at) = oauth
        .token_service
        .revoke_token(token)
        .map_err(|e| OpError::InvalidRequest(format!("revoke failed: {e}")))?;

    Ok(RevokeTokenResult { jti, expires_at })
}

/// List current deny-list entries. Expired entries are filtered out.
pub fn list_revoked_tokens(
    state: &ServerState,
    auth: &AuthContext,
) -> Result<Vec<RevokedTokenDto>, OpError> {
    require_admin(state, auth)?;
    let oauth = oauth_service(state)?;

    Ok(oauth
        .token_service
        .list_revoked()
        .into_iter()
        .map(|(jti, expires_at)| RevokedTokenDto { jti, expires_at })
        .collect())
}

/// Remove a jti from the deny-list, reinstating any still-unexpired token
/// that carried it. Idempotent — returns `removed=false` if the jti is not
/// currently on the deny-list.
pub fn unrevoke_token(
    state: &ServerState,
    auth: &AuthContext,
    jti: &str,
) -> Result<UnrevokeTokenResult, OpError> {
    require_admin(state, auth)?;
    let oauth = oauth_service(state)?;

    let removed = oauth.token_service.unrevoke(jti);
    Ok(UnrevokeTokenResult {
        jti: jti.to_owned(),
        removed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::ServerState;

    async fn test_state() -> (ServerState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let state = ServerState::for_testing(dir.path()).await;
        (state, dir)
    }

    #[tokio::test]
    async fn revoke_adds_to_deny_list() {
        let (state, _dir) = test_state().await;
        let auth = AuthContext::dev_admin();
        let oauth = state
            .services
            .get::<OAuthService>()
            .expect("oauth service registered in for_testing");
        let (jwt, _refresh) = oauth.token_service.issue("admin", "admin").unwrap();

        let result = revoke_token(&state, &auth, &jwt).unwrap();
        assert!(!result.jti.is_empty());
        assert!(result.expires_at > 0);

        let listed = list_revoked_tokens(&state, &auth).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].jti, result.jti);
    }

    #[tokio::test]
    async fn unrevoke_removes_entry() {
        let (state, _dir) = test_state().await;
        let auth = AuthContext::dev_admin();
        let oauth = state.services.get::<OAuthService>().unwrap();
        let (jwt, _) = oauth.token_service.issue("admin", "admin").unwrap();

        let revoked = revoke_token(&state, &auth, &jwt).unwrap();
        assert_eq!(list_revoked_tokens(&state, &auth).unwrap().len(), 1);

        let unrevoke = unrevoke_token(&state, &auth, &revoked.jti).unwrap();
        assert!(unrevoke.removed);
        assert!(list_revoked_tokens(&state, &auth).unwrap().is_empty());

        // Second call is idempotent.
        let second = unrevoke_token(&state, &auth, &revoked.jti).unwrap();
        assert!(!second.removed);
    }

    #[tokio::test]
    async fn revoke_rejects_malformed_token() {
        let (state, _dir) = test_state().await;
        let auth = AuthContext::dev_admin();
        let err = revoke_token(&state, &auth, "not-a-jwt").unwrap_err();
        assert!(matches!(err, OpError::InvalidRequest(_)));
    }
}
