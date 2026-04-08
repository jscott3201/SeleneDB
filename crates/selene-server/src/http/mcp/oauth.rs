//! OAuth 2.1 HTTP endpoints for MCP authentication.
//!
//! Implements the authorization code flow with PKCE (RFC 7636) and
//! client credentials grant for machine-to-machine authentication.
//! All endpoints are public (they *are* the auth flow).
//!
//! Endpoints:
//! - `GET  /.well-known/oauth-authorization-server` -- server metadata (RFC 8414)
//! - `POST /oauth/register`   -- dynamic client registration
//! - `GET  /oauth/authorize`  -- authorization code request
//! - `POST /oauth/approve`    -- consent form submission
//! - `POST /oauth/token`      -- token exchange

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use axum::Extension;
use axum::Form;
use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::auth::credential;
use crate::auth::handshake;
use crate::auth::oauth::{OAuthTokenService, now_secs, uuid_v4};
use crate::bootstrap::ServerState;
use crate::service_registry::{Service, ServiceHealth};

// ---------------------------------------------------------------------------
// OAuth service wrapper (for ServiceRegistry)
// ---------------------------------------------------------------------------

/// Wrapper that registers `OAuthTokenService` in the `ServiceRegistry`.
///
/// Registered in `bootstrap()` when `config.mcp.enabled` is true. The token
/// endpoint looks it up via `state.services.get::<OAuthService>()`.
pub struct OAuthService {
    pub token_service: Arc<OAuthTokenService>,
}

impl OAuthService {
    pub fn new(token_service: Arc<OAuthTokenService>) -> Self {
        Self { token_service }
    }
}

impl Service for OAuthService {
    fn name(&self) -> &'static str {
        "oauth"
    }

    fn health(&self) -> ServiceHealth {
        ServiceHealth::Healthy
    }
}

// ---------------------------------------------------------------------------
// Auth code store
// ---------------------------------------------------------------------------

/// Record for a pending authorization code.
#[derive(Debug, Clone)]
struct AuthCodeRecord {
    client_id: String,
    redirect_uri: String,
    scope: String,
    code_challenge: String,
    expires_at: u64,
}

/// Record for a pending CSRF nonce (generated at authorize time,
/// verified at approve time). Short-lived: 10-minute TTL.
#[derive(Debug, Clone)]
struct CsrfRecord {
    expires_at: u64,
}

/// Maximum number of pending authorization codes before the store rejects new entries.
const MAX_AUTH_CODES: usize = 5_000;

/// Maximum number of pending CSRF nonces.
const MAX_CSRF_NONCES: usize = 5_000;

/// Shared store for pending authorization codes and CSRF nonces. Codes have a
/// 5-minute TTL, CSRF nonces have a 10-minute TTL, and both are consumed on
/// first use.
#[derive(Clone, Default)]
pub struct AuthCodeStore {
    codes: Arc<RwLock<HashMap<String, AuthCodeRecord>>>,
    csrf_nonces: Arc<RwLock<HashMap<String, CsrfRecord>>>,
}

impl AuthCodeStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Remove all expired authorization codes and CSRF nonces.
    pub fn prune_expired(&self) {
        let now = now_secs();
        self.codes.write().retain(|_, r| r.expires_at > now);
        self.csrf_nonces.write().retain(|_, r| r.expires_at > now);
    }

    /// Generate and store a CSRF nonce with a 10-minute TTL.
    /// Returns `None` if the store is at capacity.
    fn issue_csrf_nonce(&self) -> Option<String> {
        let now = now_secs();
        let mut nonces = self.csrf_nonces.write();
        nonces.retain(|_, r| r.expires_at > now);
        if nonces.len() >= MAX_CSRF_NONCES {
            return None;
        }
        let nonce = uuid_v4();
        nonces.insert(
            nonce.clone(),
            CsrfRecord {
                expires_at: now + 600, // 10 minutes
            },
        );
        Some(nonce)
    }

    /// Verify and consume a CSRF nonce. Returns `true` if valid.
    fn verify_csrf_nonce(&self, nonce: &str) -> bool {
        let mut nonces = self.csrf_nonces.write();
        let Some(record) = nonces.remove(nonce) else {
            return false;
        };
        record.expires_at > now_secs()
    }
}

impl Service for AuthCodeStore {
    fn name(&self) -> &'static str {
        "auth_code_store"
    }

    fn health(&self) -> ServiceHealth {
        ServiceHealth::Healthy
    }
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

/// Standard OAuth error response body (RFC 6749 section 5.2).
#[derive(Serialize)]
struct OAuthErrorBody {
    error: String,
    error_description: String,
}

fn oauth_error(status: StatusCode, error: &str, description: &str) -> Response {
    (
        status,
        Json(OAuthErrorBody {
            error: error.to_string(),
            error_description: description.to_string(),
        }),
    )
        .into_response()
}

/// Build a redirect response carrying an OAuth error in query parameters.
fn error_redirect(
    redirect_uri: &str,
    error: &str,
    description: &str,
    state: Option<&str>,
) -> Response {
    let encoded_desc = percent_encode(description);
    let sep = if redirect_uri.contains('?') { '&' } else { '?' };
    let mut url = format!("{redirect_uri}{sep}error={error}&error_description={encoded_desc}");
    if let Some(s) = state {
        let encoded_state = percent_encode(s);
        let _ = write!(url, "&state={encoded_state}");
    }
    Redirect::to(&url).into_response()
}

/// Minimal percent-encoding for query parameter values. Encodes characters
/// that are unsafe in query strings without pulling in a full URL crate.
fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                let _ = write!(out, "{b:02X}");
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Generate a cryptographically random secret (32 bytes, base64-encoded).
fn generate_secret() -> String {
    use rand::RngExt;
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes[..]);
    BASE64_STANDARD.encode(bytes)
}

/// Escape a string for embedding in a GQL single-quoted literal.
/// GQL uses doubled single quotes (`''`) as the escape for `'`.
fn gql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Validate that a user-supplied string is safe for GQL interpolation.
///
/// Rejects values containing backslashes, null bytes, and control characters
/// that could break out of a GQL single-quoted string literal. The `gql_escape`
/// function handles single quotes, but these characters pose additional risk.
fn validate_gql_safe(value: &str, field_name: &str) -> Result<(), String> {
    for ch in value.chars() {
        if ch == '\\' || ch == '\0' || ch.is_control() {
            return Err(format!(
                "{field_name} contains disallowed characters (backslashes, null bytes, or control characters)"
            ));
        }
    }
    Ok(())
}

/// Issue an authorization code with a 5-minute TTL.
///
/// Prunes expired codes first, then checks capacity before inserting.
/// Returns `Err` if the store is at capacity.
fn issue_auth_code(
    store: &AuthCodeStore,
    client_id: &str,
    redirect_uri: &str,
    scope: &str,
    code_challenge: &str,
) -> Result<String, &'static str> {
    let code = uuid_v4();
    let now = now_secs();
    let record = AuthCodeRecord {
        client_id: client_id.to_string(),
        redirect_uri: redirect_uri.to_string(),
        scope: scope.to_string(),
        code_challenge: code_challenge.to_string(),
        expires_at: now + 300, // 5 minutes
    };
    let mut codes = store.codes.write();
    // Prune expired codes before checking capacity.
    codes.retain(|_, r| r.expires_at > now);
    if codes.len() >= MAX_AUTH_CODES {
        return Err("authorization code store is at capacity");
    }
    codes.insert(code.clone(), record);
    Ok(code)
}

/// Verify a PKCE S256 code challenge against the provided verifier.
///
/// SHA-256 hashes the verifier, base64url-encodes (no padding), and compares
/// with the stored challenge.
fn verify_pkce(code_verifier: &str, code_challenge: &str) -> Result<(), &'static str> {
    use subtle::ConstantTimeEq;
    let mut hasher = Sha256::new();
    hasher.update(code_verifier.as_bytes());
    let digest = hasher.finalize();
    let computed = URL_SAFE_NO_PAD.encode(digest);
    if bool::from(computed.as_bytes().ct_eq(code_challenge.as_bytes())) {
        Ok(())
    } else {
        Err("PKCE verification failed")
    }
}

/// Retrieve the `OAuthTokenService` from `ServerState.services`.
fn get_token_service(state: &ServerState) -> Option<&Arc<OAuthTokenService>> {
    state
        .services
        .get::<OAuthService>()
        .map(|svc| &svc.token_service)
}

// ---------------------------------------------------------------------------
// 1. GET /.well-known/oauth-authorization-server
// ---------------------------------------------------------------------------

/// RFC 8414 authorization server metadata.
#[derive(Serialize)]
pub struct SeleneAuthMetadata {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    revocation_endpoint: String,
    registration_endpoint: String,
    scopes_supported: Vec<String>,
    response_types_supported: Vec<String>,
    code_challenge_methods_supported: Vec<String>,
    grant_types_supported: Vec<String>,
    token_endpoint_auth_methods_supported: Vec<String>,
}

pub async fn oauth_metadata(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let base = state
        .config
        .mcp
        .resolve_public_url(state.config.http.listen_addr, state.config.dev_mode);
    let metadata = SeleneAuthMetadata {
        issuer: base.clone(),
        authorization_endpoint: format!("{base}/oauth/authorize"),
        token_endpoint: format!("{base}/oauth/token"),
        revocation_endpoint: format!("{base}/oauth/revoke"),
        registration_endpoint: format!("{base}/oauth/register"),
        scopes_supported: vec![
            "admin".into(),
            "service".into(),
            "operator".into(),
            "reader".into(),
            "device".into(),
        ],
        response_types_supported: vec!["code".into()],
        code_challenge_methods_supported: vec!["S256".into()],
        grant_types_supported: vec![
            "authorization_code".into(),
            "client_credentials".into(),
            "refresh_token".into(),
        ],
        token_endpoint_auth_methods_supported: vec!["client_secret_post".into()],
    };
    (StatusCode::OK, Json(metadata))
}

// ---------------------------------------------------------------------------
// 2. POST /oauth/register
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RegisterRequest {
    client_name: String,
    redirect_uris: Vec<String>,
    /// Accepted for protocol compatibility; not stored (all grants are allowed).
    #[serde(default)]
    #[allow(dead_code)]
    grant_types: Vec<String>,
    #[serde(default)]
    scope: String,
}

#[derive(Serialize)]
struct RegisterResponse {
    client_id: String,
    client_secret: String,
    client_name: String,
    redirect_uris: Vec<String>,
}

pub async fn oauth_register(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(req): Json<RegisterRequest>,
) -> Response {
    // Gate registration behind a static bearer token when configured.
    if let Some(ref expected) = state.config.mcp.registration_token {
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "));
        match provided {
            Some(token) => {
                use subtle::ConstantTimeEq;
                if !bool::from(token.as_bytes().ct_eq(expected.as_bytes())) {
                    return oauth_error(
                        StatusCode::UNAUTHORIZED,
                        "invalid_token",
                        "invalid registration token",
                    );
                }
            }
            None => {
                return oauth_error(
                    StatusCode::UNAUTHORIZED,
                    "invalid_token",
                    "registration requires Authorization: Bearer <registration_token>",
                );
            }
        }
    }

    // Input validation: reject oversized or malformed values.
    if req.client_name.len() > 256 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "client_name must be 256 characters or fewer",
        );
    }
    if req.redirect_uris.len() != 1 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "exactly one redirect_uri is required",
        );
    }
    for uri in &req.redirect_uris {
        if uri.len() > 2048 {
            return oauth_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "each redirect_uri must be 2048 characters or fewer",
            );
        }
        if !uri.starts_with("https://")
            && !uri.starts_with("http://localhost")
            && !uri.starts_with("http://127.0.0.1")
        {
            return oauth_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "redirect_uri must use https:// (or http://localhost / http://127.0.0.1 for development)",
            );
        }
    }

    // Validate scope: OAuth 2.0 (RFC 6749 §3.3) uses space-delimited scope
    // tokens. Each token must be a recognized role. When multiple roles are
    // requested, the highest-privilege role is selected (SeleneDB uses
    // single-role authorization).
    let scope = if req.scope.is_empty() {
        "reader".to_owned()
    } else {
        let role_priority = |r: &crate::auth::Role| match r {
            crate::auth::Role::Admin => 4,
            crate::auth::Role::Service => 3,
            crate::auth::Role::Operator => 2,
            crate::auth::Role::Reader => 1,
            crate::auth::Role::Device => 0,
        };
        let mut best: Option<crate::auth::Role> = None;
        for token in req.scope.split_whitespace() {
            match token.parse::<crate::auth::Role>() {
                Ok(role) => {
                    if best
                        .as_ref()
                        .is_none_or(|b| role_priority(&role) > role_priority(b))
                    {
                        best = Some(role);
                    }
                }
                Err(_) => {
                    return oauth_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_scope",
                        &format!(
                            "scope must be a valid role (admin/service/operator/reader/device), got '{token}'"
                        ),
                    );
                }
            }
        }
        best.unwrap_or(crate::auth::Role::Reader).to_string()
    };

    let client_id = format!("mcp-{}", uuid_v4());
    let client_secret = generate_secret();

    // Hash the secret for storage.
    let credential_hash = match credential::hash_credential(&client_secret) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, "failed to hash credential during OAuth registration");
            return oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "registration failed",
            );
        }
    };

    // Validate user-supplied values are safe for GQL string interpolation.
    // The client_id and credential_hash are generated by us, and role is
    // validated against the Role enum, so only client_name and redirect_uri
    // need this check.
    if let Err(msg) = validate_gql_safe(&req.client_name, "client_name") {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request", &msg);
    }
    if let Err(msg) = validate_gql_safe(&req.redirect_uris[0], "redirect_uri") {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request", &msg);
    }

    // Build GQL INSERT for the principal node.
    let identity = gql_escape(&client_id);
    let hash_escaped = gql_escape(&credential_hash);
    let role = gql_escape(&scope);
    let name = gql_escape(&req.client_name);
    let uri = gql_escape(&req.redirect_uris[0]);

    let gql = format!(
        "INSERT (:principal {{identity: '{identity}', credential_hash: '{hash_escaped}', \
         role: '{role}', enabled: true, client_name: '{name}', redirect_uri: '{uri}'}})"
    );

    let shared = state.graph.clone();
    let batcher_result = state
        .mutation_batcher
        .submit(move || {
            selene_gql::MutationBuilder::new(&gql)
                .execute(&shared)
                .map(|_| ())
        })
        .await;
    match batcher_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::error!(error = %e, "failed to create principal during OAuth registration");
            return oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "registration failed",
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "mutation batcher error during OAuth registration");
            return oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "registration failed",
            );
        }
    }

    (
        StatusCode::CREATED,
        Json(RegisterResponse {
            client_id,
            client_secret,
            client_name: req.client_name,
            redirect_uris: req.redirect_uris,
        }),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// 3. GET /oauth/authorize
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct AuthorizeParams {
    response_type: Option<String>,
    client_id: Option<String>,
    redirect_uri: Option<String>,
    #[serde(default)]
    scope: String,
    state: Option<String>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
}

pub async fn oauth_authorize(
    State(state): State<Arc<ServerState>>,
    Extension(code_store): Extension<AuthCodeStore>,
    Query(params): Query<AuthorizeParams>,
) -> Response {
    // Validate required parameters.
    let Some(ref response_type) = params.response_type else {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "response_type is required",
        );
    };
    if response_type != "code" {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "unsupported_response_type",
            "only response_type=code is supported",
        );
    }
    let Some(ref client_id) = params.client_id else {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "client_id is required",
        );
    };
    let Some(ref redirect_uri) = params.redirect_uri else {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "redirect_uri is required",
        );
    };
    let Some(ref code_challenge) = params.code_challenge else {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "code_challenge is required (PKCE)",
        );
    };
    let method = params.code_challenge_method.as_deref().unwrap_or("S256");
    if method != "S256" {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "only code_challenge_method=S256 is supported",
        );
    }

    // Validate client exists in graph, is enabled, and redirect_uri matches.
    let valid = state.graph.read(|g| {
        g.nodes_by_label("principal").any(|nid| {
            g.get_node(nid).is_some_and(|n| {
                n.property("identity")
                    .is_some_and(|v| v.as_str() == Some(client_id))
                    && n.property("enabled")
                        .is_some_and(|v| matches!(v, selene_core::Value::Bool(true)))
                    && n.property("redirect_uri")
                        .is_some_and(|v| v.as_str() == Some(redirect_uri))
            })
        })
    });
    if !valid {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_client",
            "client_id not found, disabled, or redirect_uri mismatch",
        );
    }

    let scope = if params.scope.is_empty() {
        "reader".to_string()
    } else {
        params.scope.clone()
    };

    // Auto-approve when configured for headless operation.
    if !state.config.mcp.require_approval {
        let code =
            match issue_auth_code(&code_store, client_id, redirect_uri, &scope, code_challenge) {
                Ok(c) => c,
                Err(msg) => {
                    return oauth_error(StatusCode::SERVICE_UNAVAILABLE, "server_error", msg);
                }
            };
        let mut url = format!("{redirect_uri}?code={code}");
        if let Some(ref s) = params.state {
            let encoded = percent_encode(s);
            let _ = write!(url, "&state={encoded}");
        }
        return Redirect::to(&url).into_response();
    }

    // Generate a CSRF nonce for the consent form.
    let Some(csrf_token) = code_store.issue_csrf_nonce() else {
        return oauth_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            "CSRF nonce store is at capacity",
        );
    };

    // Render consent page (includes CSRF token as hidden field).
    let state_field = params.state.as_deref().unwrap_or("");
    let html = consent_page(
        client_id,
        redirect_uri,
        &scope,
        state_field,
        code_challenge,
        method,
        &csrf_token,
    );
    Html(html).into_response()
}

/// Render a minimal HTML consent page with hidden form fields.
fn consent_page(
    client_id: &str,
    redirect_uri: &str,
    scope: &str,
    state: &str,
    code_challenge: &str,
    code_challenge_method: &str,
    csrf_token: &str,
) -> String {
    // HTML-escape values to prevent injection.
    let esc = |s: &str| -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#x27;")
    };

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Selene - Authorize Application</title>
<style>
  body {{ font-family: system-ui, sans-serif; max-width: 480px; margin: 60px auto; padding: 0 20px; }}
  h1 {{ font-size: 1.4em; }}
  .info {{ background: #f5f5f5; padding: 12px 16px; border-radius: 6px; margin: 16px 0; }}
  .actions {{ display: flex; gap: 12px; margin-top: 24px; }}
  button {{ padding: 10px 24px; border: none; border-radius: 4px; font-size: 1em; cursor: pointer; }}
  .approve {{ background: #2563eb; color: white; }}
  .deny {{ background: #e5e7eb; color: #374151; }}
</style>
</head>
<body>
<h1>Authorize Application</h1>
<div class="info">
  <p><strong>{client_id}</strong> is requesting access with scope <strong>{scope}</strong>.</p>
</div>
<form method="POST" action="/oauth/approve">
  <input type="hidden" name="client_id" value="{ci}">
  <input type="hidden" name="redirect_uri" value="{ru}">
  <input type="hidden" name="scope" value="{sc}">
  <input type="hidden" name="state" value="{st}">
  <input type="hidden" name="code_challenge" value="{cc}">
  <input type="hidden" name="code_challenge_method" value="{ccm}">
  <input type="hidden" name="csrf_token" value="{csrf}">
  <div class="actions">
    <button type="submit" name="approved" value="true" class="approve">Approve</button>
    <button type="submit" name="approved" value="false" class="deny">Deny</button>
  </div>
</form>
</body>
</html>"#,
        client_id = esc(client_id),
        scope = esc(scope),
        ci = esc(client_id),
        ru = esc(redirect_uri),
        sc = esc(scope),
        st = esc(state),
        cc = esc(code_challenge),
        ccm = esc(code_challenge_method),
        csrf = esc(csrf_token),
    )
}

// ---------------------------------------------------------------------------
// 4. POST /oauth/approve
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ApproveForm {
    client_id: String,
    redirect_uri: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    state: String,
    code_challenge: String,
    #[allow(dead_code)] // Deserialized for protocol completeness.
    code_challenge_method: String,
    #[serde(default)]
    approved: String,
    #[serde(default)]
    csrf_token: String,
}

pub async fn oauth_approve(
    State(state): State<Arc<ServerState>>,
    Extension(code_store): Extension<AuthCodeStore>,
    Form(form): Form<ApproveForm>,
) -> Response {
    // Verify CSRF nonce before processing the form.
    if form.csrf_token.is_empty() || !code_store.verify_csrf_nonce(&form.csrf_token) {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "invalid_request",
            "missing or invalid CSRF token",
        );
    }

    if form.approved != "true" {
        return error_redirect(
            &form.redirect_uri,
            "access_denied",
            "the resource owner denied the request",
            if form.state.is_empty() {
                None
            } else {
                Some(&form.state)
            },
        );
    }

    // Re-validate client_id and redirect_uri against the graph.
    let valid = state.graph.read(|g| {
        g.nodes_by_label("principal").any(|nid| {
            g.get_node(nid).is_some_and(|n| {
                n.property("identity")
                    .is_some_and(|v| v.as_str() == Some(&form.client_id))
                    && n.property("enabled")
                        .is_some_and(|v| matches!(v, selene_core::Value::Bool(true)))
                    && n.property("redirect_uri")
                        .is_some_and(|v| v.as_str() == Some(&form.redirect_uri))
            })
        })
    });
    if !valid {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_client",
            "client_id not found, disabled, or redirect_uri mismatch",
        );
    }

    let code = match issue_auth_code(
        &code_store,
        &form.client_id,
        &form.redirect_uri,
        &form.scope,
        &form.code_challenge,
    ) {
        Ok(c) => c,
        Err(msg) => return oauth_error(StatusCode::SERVICE_UNAVAILABLE, "server_error", msg),
    };

    let mut url = format!("{}?code={code}", form.redirect_uri);
    if !form.state.is_empty() {
        let encoded = percent_encode(&form.state);
        let _ = write!(url, "&state={encoded}");
    }
    Redirect::to(&url).into_response()
}

// ---------------------------------------------------------------------------
// 5. POST /oauth/token
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct TokenRequest {
    grant_type: String,
    // authorization_code fields
    #[serde(default)]
    code: String,
    #[serde(default)]
    redirect_uri: String,
    #[serde(default)]
    code_verifier: String,
    // client credentials (also used with authorization_code)
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    client_secret: String,
    // refresh_token grant
    #[serde(default)]
    refresh_token: String,
    // optional scope
    #[serde(default)]
    scope: String,
}

#[derive(Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
    refresh_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
}

pub async fn oauth_token(
    State(state): State<Arc<ServerState>>,
    Extension(code_store): Extension<AuthCodeStore>,
    Form(req): Form<TokenRequest>,
) -> Response {
    let Some(token_svc) = get_token_service(&state) else {
        return oauth_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            "OAuth token service is not initialized",
        );
    };

    match req.grant_type.as_str() {
        "authorization_code" => handle_auth_code_grant(&state, &code_store, token_svc, &req),
        "client_credentials" => handle_client_credentials_grant(&state, token_svc, &req),
        "refresh_token" => handle_refresh_token_grant(&state, token_svc, &req),
        _ => oauth_error(
            StatusCode::BAD_REQUEST,
            "unsupported_grant_type",
            &format!("unsupported grant_type: {}", req.grant_type),
        ),
    }
}

// -- Token revocation (RFC 7009) -------------------------------------------

/// `POST /oauth/revoke` — revoke an access token.
///
/// Accepts `application/x-www-form-urlencoded` with a `token` field containing
/// the JWT access token to revoke. Per RFC 7009, this endpoint always returns
/// 200 OK regardless of whether the token was valid or already revoked.
pub async fn oauth_revoke(
    State(state): State<Arc<ServerState>>,
    Form(req): Form<RevokeRequest>,
) -> Response {
    let Some(token_svc) = get_token_service(&state) else {
        return oauth_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "server_error",
            "OAuth token service is not initialized",
        );
    };

    if req.token.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "token is required",
        );
    }

    // Per RFC 7009 §2.1: the server responds with 200 OK even if the token
    // is invalid, expired, or already revoked — to prevent token scanning.
    if let Err(e) = token_svc.revoke_token(&req.token) {
        tracing::debug!("revoke request for invalid token: {e}");
    }

    (StatusCode::OK, Json(serde_json::json!({"status": "revoked"}))).into_response()
}

#[derive(Deserialize)]
pub(crate) struct RevokeRequest {
    token: String,
}

// -- Grant handlers --------------------------------------------------------

fn handle_auth_code_grant(
    state: &ServerState,
    code_store: &AuthCodeStore,
    token_svc: &Arc<OAuthTokenService>,
    req: &TokenRequest,
) -> Response {
    if req.code.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "code is required",
        );
    }
    if req.code_verifier.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "code_verifier is required",
        );
    }
    if req.client_id.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "client_id is required",
        );
    }

    // Rate-limit check before authentication.
    if let Some(wait) = state.auth_rate_limiter.check(&req.client_id) {
        return oauth_error(
            StatusCode::TOO_MANY_REQUESTS,
            "slow_down",
            &format!("too many failed attempts, retry in {wait}s"),
        );
    }

    // Consume the authorization code (single-use).
    let record = {
        let mut codes = code_store.codes.write();
        codes.remove(&req.code)
    };
    let Some(record) = record else {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
            "authorization code is invalid or already used",
        );
    };

    // Check expiry.
    if now_secs() >= record.expires_at {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
            "authorization code has expired",
        );
    }

    // Validate client_id and redirect_uri match the code.
    if record.client_id != req.client_id {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
            "client_id does not match the authorization code",
        );
    }
    if record.redirect_uri != req.redirect_uri {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_grant",
            "redirect_uri does not match the authorization code",
        );
    }

    // Verify PKCE.
    if let Err(msg) = verify_pkce(&req.code_verifier, &record.code_challenge) {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", msg);
    }

    // Authenticate the client.
    let auth = match handshake::authenticate(
        &state.graph,
        "token",
        &req.client_id,
        &req.client_secret,
        state.config.dev_mode,
    ) {
        Ok(ctx) => {
            state.auth_rate_limiter.record_success(&req.client_id);
            ctx
        }
        Err(e) => {
            state.auth_rate_limiter.record_failure(&req.client_id);
            tracing::warn!(client_id = %req.client_id, error = %e, "OAuth auth_code client authentication failed");
            return oauth_error(
                StatusCode::UNAUTHORIZED,
                "invalid_client",
                "client authentication failed",
            );
        }
    };

    // Issue tokens.
    match token_svc.issue(&req.client_id, auth.role.as_str()) {
        Ok((access_token, refresh_token)) => (
            StatusCode::OK,
            Json(TokenResponse {
                access_token,
                token_type: "Bearer".into(),
                expires_in: state.config.mcp.access_token_ttl_secs,
                refresh_token,
                scope: Some(record.scope),
            }),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to issue OAuth tokens");
            oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "token issuance failed",
            )
        }
    }
}

fn handle_client_credentials_grant(
    state: &ServerState,
    token_svc: &Arc<OAuthTokenService>,
    req: &TokenRequest,
) -> Response {
    if req.client_id.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "client_id is required",
        );
    }
    if req.client_secret.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "client_secret is required",
        );
    }

    // Rate-limit check before authentication.
    if let Some(wait) = state.auth_rate_limiter.check(&req.client_id) {
        return oauth_error(
            StatusCode::TOO_MANY_REQUESTS,
            "slow_down",
            &format!("too many failed attempts, retry in {wait}s"),
        );
    }

    let auth = match handshake::authenticate(
        &state.graph,
        "token",
        &req.client_id,
        &req.client_secret,
        state.config.dev_mode,
    ) {
        Ok(ctx) => {
            state.auth_rate_limiter.record_success(&req.client_id);
            ctx
        }
        Err(e) => {
            state.auth_rate_limiter.record_failure(&req.client_id);
            tracing::warn!(client_id = %req.client_id, error = %e, "OAuth client_credentials authentication failed");
            return oauth_error(
                StatusCode::UNAUTHORIZED,
                "invalid_client",
                "client authentication failed",
            );
        }
    };

    // Validate requested scope against the principal's actual role. If a scope
    // is requested, it must match the principal's role exactly; privilege
    // escalation via scope parameter is not allowed.
    // OAuth 2.0 (RFC 6749 §3.3) scope is space-delimited. Accept if any
    // requested scope matches the principal's authorized role.
    let scope_str = if req.scope.is_empty() {
        auth.role.as_str().to_string()
    } else {
        let mut matched = false;
        for token in req.scope.split_whitespace() {
            match token.parse::<crate::auth::Role>() {
                Ok(role) if role == auth.role => {
                    matched = true;
                    break;
                }
                Ok(_) => {} // valid role but doesn't match — skip
                Err(_) => {
                    return oauth_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_scope",
                        &format!(
                            "scope must be a valid role (admin/service/operator/reader/device), got '{token}'"
                        ),
                    );
                }
            }
        }
        if !matched {
            return oauth_error(
                StatusCode::BAD_REQUEST,
                "invalid_scope",
                "requested scope does not match the client's authorized role",
            );
        }
        auth.role.as_str().to_string()
    };

    match token_svc.issue(&req.client_id, auth.role.as_str()) {
        Ok((access_token, refresh_token)) => (
            StatusCode::OK,
            Json(TokenResponse {
                access_token,
                token_type: "Bearer".into(),
                expires_in: state.config.mcp.access_token_ttl_secs,
                refresh_token,
                scope: Some(scope_str),
            }),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to issue OAuth tokens for client_credentials");
            oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "token issuance failed",
            )
        }
    }
}

fn handle_refresh_token_grant(
    state: &ServerState,
    token_svc: &Arc<OAuthTokenService>,
    req: &TokenRequest,
) -> Response {
    if req.refresh_token.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "refresh_token is required",
        );
    }
    if req.client_id.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "client_id is required",
        );
    }
    if req.client_secret.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "client_secret is required",
        );
    }

    // Rate-limit check before authentication.
    if let Some(wait) = state.auth_rate_limiter.check(&req.client_id) {
        return oauth_error(
            StatusCode::TOO_MANY_REQUESTS,
            "slow_down",
            &format!("too many failed attempts, retry in {wait}s"),
        );
    }

    // Authenticate client before allowing refresh.
    match handshake::authenticate(
        &state.graph,
        "token",
        &req.client_id,
        &req.client_secret,
        state.config.dev_mode,
    ) {
        Ok(_) => {
            state.auth_rate_limiter.record_success(&req.client_id);
        }
        Err(e) => {
            state.auth_rate_limiter.record_failure(&req.client_id);
            tracing::warn!(client_id = %req.client_id, error = %e, "OAuth refresh_token authentication failed");
            return oauth_error(
                StatusCode::UNAUTHORIZED,
                "invalid_client",
                "client authentication failed",
            );
        }
    }

    match token_svc.refresh(&req.refresh_token, &state.graph) {
        Ok((access_token, refresh_token)) => (
            StatusCode::OK,
            Json(TokenResponse {
                access_token,
                token_type: "Bearer".into(),
                expires_in: state.config.mcp.access_token_ttl_secs,
                refresh_token,
                scope: None,
            }),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "OAuth refresh_token grant failed");
            oauth_error(
                StatusCode::BAD_REQUEST,
                "invalid_grant",
                "refresh token is invalid or expired",
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_s256_roundtrip() {
        // RFC 7636 Appendix B example-style test.
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let mut hasher = Sha256::new();
        hasher.update(verifier.as_bytes());
        let digest = hasher.finalize();
        let challenge = URL_SAFE_NO_PAD.encode(digest);

        assert!(verify_pkce(verifier, &challenge).is_ok());
        assert!(verify_pkce("wrong-verifier", &challenge).is_err());
    }

    #[test]
    fn generate_secret_is_base64() {
        let secret = generate_secret();
        assert!(!secret.is_empty());
        // Should decode without error.
        assert!(BASE64_STANDARD.decode(&secret).is_ok());
        // 32 bytes base64 = 44 characters (with padding).
        assert_eq!(secret.len(), 44);
    }

    #[test]
    fn gql_escape_single_quotes() {
        assert_eq!(gql_escape("no quotes"), "no quotes");
        assert_eq!(gql_escape("it's"), "it''s");
        assert_eq!(gql_escape("''"), "''''");
    }

    #[test]
    fn auth_code_store_insert_and_consume() {
        let store = AuthCodeStore::new();
        let code = issue_auth_code(
            &store,
            "cid",
            "https://example.com/cb",
            "reader",
            "challenge123",
        )
        .expect("should issue code");
        assert!(!code.is_empty());

        // Should be present.
        assert!(store.codes.read().contains_key(&code));

        // Consume.
        let record = store.codes.write().remove(&code);
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.client_id, "cid");
        assert_eq!(record.redirect_uri, "https://example.com/cb");
        assert_eq!(record.scope, "reader");
        assert_eq!(record.code_challenge, "challenge123");

        // Consumed: no longer present.
        assert!(!store.codes.read().contains_key(&code));
    }

    #[test]
    fn percent_encode_basic() {
        assert_eq!(percent_encode("hello"), "hello");
        assert_eq!(percent_encode("hello world"), "hello%20world");
        assert_eq!(percent_encode("a=b&c"), "a%3Db%26c");
    }

    #[test]
    fn now_secs_reasonable() {
        let now = now_secs();
        // Should be after 2024-01-01 (1704067200).
        assert!(now > 1_704_067_200);
    }
}
