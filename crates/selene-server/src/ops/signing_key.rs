//! OAuth signing-key rotation.
//!
//! Rotation generates a new 32-byte HMAC-SHA256 secret, persists it in the
//! vault (replacing the current `oauth_signing_key` entry), and installs it
//! on the running `OAuthTokenService`. The previous key is kept in an
//! in-memory retired-key ring for `retire_for_secs` so access tokens signed
//! under it remain valid during the grace period.
//!
//! Retired keys are *not* persisted across restart. On restart, the server
//! loads only the current key; any access token signed under a previously
//! retired key becomes invalid and clients must exchange their refresh token
//! (which is opaque and key-agnostic) for a new pair.

use std::time::SystemTime;

use serde::Serialize;

use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use crate::http::mcp::oauth::OAuthService;
use crate::vault::VaultService;

use super::OpError;

/// Default grace period for retired keys (24 hours). Matches the design
/// intent: any access token issued under the prior key will have expired
/// (access_ttl defaults to 15 min), so 24h is a comfortable ceiling for
/// even a generous access_ttl.
const DEFAULT_RETIRE_SECONDS: u64 = 24 * 60 * 60;

/// Return shape for the rotate tool.
#[derive(Debug, Serialize)]
pub struct RotateSigningKeyResult {
    /// Unix seconds (server clock) at which the rotation was performed.
    pub rotated_at: u64,
    /// Unix seconds at which the previous key's grace period expires.
    pub previous_key_valid_until: u64,
    /// Retired keys are not persisted across restart — this flag makes the
    /// behavior explicit in the tool output.
    pub retired_keys_in_memory_only: bool,
}

/// Rotate the OAuth signing key.
///
/// `retire_for_secs` is the number of seconds the *previous* key should
/// remain accepted for decoding existing access tokens. When `None`,
/// defaults to 24 hours.
pub fn rotate_signing_key(
    state: &ServerState,
    auth: &AuthContext,
    retire_for_secs: Option<u64>,
) -> Result<RotateSigningKeyResult, OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::PrincipalManage)
    {
        return Err(OpError::AuthDenied);
    }

    let vault = state
        .services
        .get::<VaultService>()
        .ok_or_else(|| OpError::Internal("vault not available".into()))?;
    let oauth = state
        .services
        .get::<OAuthService>()
        .ok_or_else(|| OpError::Internal("OAuth service not available".into()))?;

    // Persist and return the new key bytes.
    let new_key = vault
        .handle
        .rotate_signing_key(&vault.master_key)
        .map_err(|e| OpError::Internal(format!("vault signing-key rotate failed: {e}")))?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let retire_for = retire_for_secs.unwrap_or(DEFAULT_RETIRE_SECONDS);
    let retire_until = now.saturating_add(retire_for);

    // Swap the live service: new encoding key active, prior decoding key
    // moves into the retired ring until `retire_until`.
    oauth
        .token_service
        .rotate_signing_key(&new_key, retire_until);

    tracing::info!(
        retire_for,
        retire_until,
        "OAuth signing key rotated; previous key retained in retired ring"
    );

    Ok(RotateSigningKeyResult {
        rotated_at: now,
        previous_key_valid_until: retire_until,
        retired_keys_in_memory_only: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::ServerState;

    async fn test_state_with_oauth_and_vault() -> (ServerState, tempfile::TempDir) {
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

    #[tokio::test]
    async fn rotate_installs_new_key_and_keeps_prior_token_valid() {
        let (state, _dir) = test_state_with_oauth_and_vault().await;
        let oauth = state
            .services
            .get::<OAuthService>()
            .expect("oauth service registered in for_testing");

        // Issue a token under the current (pre-rotation) key.
        let (access_before, _refresh) = oauth.token_service.issue("admin", "admin").unwrap();
        // Standalone decode works with the original key.
        assert!(
            oauth
                .token_service
                .validate_standalone(&access_before)
                .is_ok()
        );

        let auth = AuthContext::dev_admin();
        let result = rotate_signing_key(&state, &auth, Some(3600)).unwrap();
        assert!(result.retired_keys_in_memory_only);
        assert!(result.previous_key_valid_until > result.rotated_at);

        // Pre-rotation token still decodes thanks to the retired-key ring.
        assert!(
            oauth
                .token_service
                .validate_standalone(&access_before)
                .is_ok()
        );

        // New tokens issued under the rotated key also validate.
        let (access_after, _) = oauth.token_service.issue("admin", "admin").unwrap();
        assert_ne!(access_before, access_after);
        assert!(
            oauth
                .token_service
                .validate_standalone(&access_after)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rotate_with_zero_grace_invalidates_prior_tokens() {
        let (state, _dir) = test_state_with_oauth_and_vault().await;
        let oauth = state.services.get::<OAuthService>().unwrap();
        let (access_before, _) = oauth.token_service.issue("admin", "admin").unwrap();

        let auth = AuthContext::dev_admin();
        // retire_for=0 means the prior key's grace window closes immediately.
        rotate_signing_key(&state, &auth, Some(0)).unwrap();

        // Prior token fails signature verification now.
        let res = oauth.token_service.validate_standalone(&access_before);
        assert!(res.is_err());
    }
}
