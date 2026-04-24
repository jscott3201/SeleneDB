//! HTTP authentication — Bearer token extraction and verification.
//!
//! Token format: `identity:secret` in the `Authorization: Bearer <token>` header.
//! In dev mode, missing headers fall back to admin context.
//! Rate limiting: per-identity exponential backoff after failed attempts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::FromRequestParts;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Json, Response};
use parking_lot::Mutex;

use crate::auth::handshake::{self, AuthContext};
use crate::bootstrap::ServerState;

/// Per-identity failure tracking for brute-force protection.
struct FailureRecord {
    count: u32,
    last_attempt: Instant,
}

/// Global auth rate limiter (shared across all HTTP requests).
///
/// Tracks failed authentication attempts per identity. After 5 failures,
/// applies exponential backoff (2^(failures-5) seconds, max 300s).
/// Records expire after 10 minutes of no attempts.
pub struct AuthRateLimiter {
    failures: Mutex<HashMap<String, FailureRecord>>,
}

const MAX_FAILURES_BEFORE_BACKOFF: u32 = 5;
const MAX_BACKOFF_SECS: u64 = 300; // 5 minutes
const RECORD_EXPIRY_SECS: u64 = 600; // 10 minutes

impl AuthRateLimiter {
    pub fn new() -> Self {
        Self {
            failures: Mutex::new(HashMap::new()),
        }
    }

    /// Check if an identity is currently rate-limited. Returns the wait time if so.
    pub fn check(&self, identity: &str) -> Option<u64> {
        let failures = self.failures.lock();
        let record = failures.get(identity)?;

        if record.count < MAX_FAILURES_BEFORE_BACKOFF {
            return None;
        }

        let exponent = record
            .count
            .saturating_sub(MAX_FAILURES_BEFORE_BACKOFF)
            .min(20);
        let backoff_secs = 2u64.pow(exponent).min(MAX_BACKOFF_SECS);
        let elapsed = record.last_attempt.elapsed().as_secs();

        if elapsed < backoff_secs {
            Some(backoff_secs - elapsed)
        } else {
            None
        }
    }

    /// Record a failed authentication attempt.
    pub fn record_failure(&self, identity: &str) {
        let mut failures = self.failures.lock();

        // Cap the failure map to prevent memory exhaustion from unique identities.
        // Prune expired entries first; if still over capacity, skip recording.
        const MAX_TRACKED_IDENTITIES: usize = 10_000;
        if failures.len() >= MAX_TRACKED_IDENTITIES {
            failures
                .retain(|_, record| record.last_attempt.elapsed().as_secs() < RECORD_EXPIRY_SECS);
        }
        if failures.len() >= MAX_TRACKED_IDENTITIES {
            return; // at capacity — drop this record rather than growing unbounded
        }

        let record = failures
            .entry(identity.to_string())
            .or_insert(FailureRecord {
                count: 0,
                last_attempt: Instant::now(),
            });
        record.count = record.count.saturating_add(1);
        record.last_attempt = Instant::now();

        if record.count == MAX_FAILURES_BEFORE_BACKOFF {
            tracing::warn!(
                identity,
                "auth rate limit activated after {MAX_FAILURES_BEFORE_BACKOFF} failures"
            );
        }
    }

    /// Clear failure count on successful authentication.
    pub fn record_success(&self, identity: &str) {
        self.failures.lock().remove(identity);
    }

    /// Prune expired records (call periodically).
    pub fn prune_expired(&self) {
        let mut failures = self.failures.lock();
        failures.retain(|_, record| record.last_attempt.elapsed().as_secs() < RECORD_EXPIRY_SECS);
    }
}

impl Default for AuthRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Axum extractor that resolves an `AuthContext` from the Bearer token.
pub struct HttpAuth(pub AuthContext);

impl<S> FromRequestParts<S> for HttpAuth
where
    S: Send + Sync,
{
    type Rejection = AuthRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Get ServerState from extensions (set by middleware)
        let state = parts
            .extensions
            .get::<Arc<ServerState>>()
            .ok_or(AuthRejection::InternalError("missing server state"))?;

        // Extract Authorization header
        let auth_header = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok());

        match auth_header {
            Some(header) if header.starts_with("Bearer ") => {
                let token = &header[7..];
                // Split token as identity:secret
                let (identity, secret) = token.split_once(':').ok_or(
                    AuthRejection::InvalidToken("token must be in 'identity:secret' format"),
                )?;

                // Check rate limit before attempting authentication
                if let Some(wait_secs) = state.auth_rate_limiter.check(identity) {
                    return Err(AuthRejection::RateLimited(wait_secs));
                }

                let vault_graph = match crate::auth::vault_graph_for_auth(&state) {
                    Ok(g) => g,
                    Err(e) => {
                        tracing::warn!(identity = %identity, error = %e, "auth failed: vault unavailable");
                        return Err(AuthRejection::AuthFailed(
                            "authentication unavailable".to_string(),
                        ));
                    }
                };
                match handshake::authenticate(
                    vault_graph,
                    &state.graph,
                    "token",
                    identity,
                    secret,
                    state.config.dev_mode,
                ) {
                    Ok(auth) => {
                        state.auth_rate_limiter.record_success(identity);
                        Ok(HttpAuth(auth))
                    }
                    Err(e) => {
                        state.auth_rate_limiter.record_failure(identity);
                        tracing::info!(identity = %identity, error = %e, "auth failed");
                        Err(AuthRejection::AuthFailed("invalid credentials".to_string()))
                    }
                }
            }
            _ => {
                // No auth header
                if state.config.dev_mode {
                    // Dev mode fallback: admin access
                    Ok(HttpAuth(AuthContext::dev_admin()))
                } else {
                    Err(AuthRejection::MissingToken)
                }
            }
        }
    }
}

/// Axum extractor that optionally resolves an `AuthContext` from the Bearer token.
///
/// Unlike `HttpAuth`, this does **not** reject requests without auth headers
/// in production -- it returns `None` instead, allowing endpoints to serve
/// tiered responses. In dev mode, missing headers still fall back to admin
/// context (matching `HttpAuth` behavior). If an `Authorization` header *is*
/// present but invalid, the request is rejected with 401 regardless of mode.
///
/// Used for endpoints that serve different response tiers based on
/// authentication (e.g., `/health`).
pub struct OptionalHttpAuth(pub Option<AuthContext>);

impl<S> FromRequestParts<S> for OptionalHttpAuth
where
    S: Send + Sync,
{
    type Rejection = AuthRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let has_auth_header = parts.headers.contains_key("authorization");
        if has_auth_header {
            // Header is present -- delegate to HttpAuth which validates the token.
            // If the token is invalid, this propagates the rejection (401/429/etc).
            let HttpAuth(auth) = HttpAuth::from_request_parts(parts, state).await?;
            return Ok(OptionalHttpAuth(Some(auth)));
        }

        // No Authorization header present.
        // In dev mode, fall back to admin context (consistent with HttpAuth).
        // In production, return None so the endpoint can serve a minimal response.
        let server_state = parts
            .extensions
            .get::<Arc<ServerState>>()
            .ok_or(AuthRejection::InternalError("missing server state"))?;

        if server_state.config.dev_mode {
            Ok(OptionalHttpAuth(Some(AuthContext::dev_admin())))
        } else {
            Ok(OptionalHttpAuth(None))
        }
    }
}

/// Auth rejection reasons.
pub enum AuthRejection {
    MissingToken,
    InvalidToken(&'static str),
    AuthFailed(String),
    RateLimited(u64),
    InternalError(&'static str),
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AuthRejection::MissingToken => (
                StatusCode::UNAUTHORIZED,
                "missing Authorization: Bearer <identity:secret> header".into(),
            ),
            AuthRejection::InvalidToken(msg) => (StatusCode::BAD_REQUEST, msg.into()),
            AuthRejection::AuthFailed(msg) => (
                StatusCode::UNAUTHORIZED,
                format!("authentication failed: {msg}"),
            ),
            AuthRejection::RateLimited(wait_secs) => (
                StatusCode::TOO_MANY_REQUESTS,
                format!("too many failed attempts, retry in {wait_secs}s"),
            ),
            AuthRejection::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.into()),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}
