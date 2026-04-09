//! Embedded HTTP server — axum routes + optional MCP.

pub(crate) mod auth;
pub(crate) mod error;
pub(crate) mod rate_limit;
pub(crate) mod routes;
pub(crate) mod ws;

pub(crate) mod mcp;

use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use tower_http::cors::CorsLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

tokio::task_local! {
    /// Per-request auth context for MCP session creation.
    ///
    /// The MCP auth middleware scopes the validated `AuthContext` into this
    /// task-local before calling `next.run(req)`. The rmcp factory closure
    /// reads it back on the same tokio task, guaranteeing one-to-one mapping
    /// without shared mutable state.
    static MCP_AUTH_CTX: AuthContext;
}

/// Why MCP bearer validation failed — used to build RFC 6750 §3.1 error responses.
enum AuthFailure {
    /// No Authorization header present.
    Missing,
    /// JWT signature valid but `exp` has passed.
    Expired,
    /// JWT is in the deny list (revoked).
    Revoked,
    /// Token is malformed, wrong signature, principal not found, etc.
    Invalid(String),
}

/// Build a 401 Unauthorized JSON response with RFC 6750 error details.
fn unauthorized_response(failure: AuthFailure) -> axum::response::Response {
    let (www_auth, error, description) = match failure {
        AuthFailure::Missing => (
            r#"Bearer error="invalid_request", error_description="missing Authorization header""#.to_owned(),
            "invalid_request",
            "MCP requires Authorization: Bearer <token>",
        ),
        AuthFailure::Expired => (
            r#"Bearer error="invalid_token", error_description="token expired""#.to_owned(),
            "invalid_token",
            "token expired",
        ),
        AuthFailure::Revoked => (
            r#"Bearer error="invalid_token", error_description="token revoked""#.to_owned(),
            "invalid_token",
            "token revoked",
        ),
        AuthFailure::Invalid(ref msg) => (
            format!(r#"Bearer error="invalid_token", error_description="{msg}""#),
            "invalid_token",
            "invalid token",
        ),
    };

    axum::response::Response::builder()
        .status(axum::http::StatusCode::UNAUTHORIZED)
        .header("content-type", "application/json")
        .header("www-authenticate", www_auth)
        .body(axum::body::Body::from(format!(
            r#"{{"error":"{error}","error_description":"{description}"}}"#
        )))
        .unwrap()
}

/// Validate a Bearer token from an MCP request and return the resolved
/// `AuthContext`.
///
/// Tries JWT validation first (via `OAuthTokenService`), then falls back
/// to constant-time comparison against the static API key (which grants
/// admin access).
fn validate_mcp_bearer(
    state: &ServerState,
    expected_key: &str,
    req: &axum::extract::Request,
) -> Result<AuthContext, AuthFailure> {
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let bearer_token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        _ => return Err(AuthFailure::Missing),
    };

    // 1. Try JWT validation via OAuthTokenService (returns per-principal AuthContext).
    if let Some(oauth_svc) = state
        .services
        .get::<crate::http::mcp::oauth::OAuthService>()
    {
        match oauth_svc.token_service.validate(bearer_token, &state.graph) {
            Ok(auth_ctx) => return Ok(auth_ctx),
            Err(crate::auth::oauth::OAuthError::TokenExpired) => {
                return Err(AuthFailure::Expired);
            }
            Err(crate::auth::oauth::OAuthError::TokenRevoked) => {
                return Err(AuthFailure::Revoked);
            }
            Err(_) => {} // fall through to API key check
        }
    }

    // 2. Fall back to static API key (constant-time comparison, grants admin).
    if !expected_key.is_empty() {
        use subtle::ConstantTimeEq;
        if bearer_token
            .as_bytes()
            .ct_eq(expected_key.as_bytes())
            .into()
        {
            return Ok(AuthContext::dev_admin());
        }
    }

    Err(AuthFailure::Invalid("invalid token".into()))
}

/// Build the axum router with all routes.
pub fn router(state: Arc<ServerState>) -> Router {
    let app = Router::new()
        .route("/", get(routes::api_index))
        .route("/health", get(routes::health))
        .route("/ready", get(routes::ready))
        .route("/info", get(routes::server_info))
        .route("/openapi.yaml", get(routes::openapi_spec))
        // Root, health, and info are unauthenticated — all other routes use HttpAuth extractor
        .route("/nodes", get(routes::list_nodes).post(routes::create_node))
        .route(
            "/nodes/{id}",
            get(routes::get_node)
                .put(routes::modify_node)
                .delete(routes::delete_node),
        )
        .route("/nodes/{id}/edges", get(routes::node_edges))
        .route("/edges", get(routes::list_edges).post(routes::create_edge))
        .route(
            "/edges/{id}",
            get(routes::get_edge)
                .put(routes::modify_edge)
                .delete(routes::delete_edge),
        )
        .route("/ts/samples", post(routes::ts_write)
            .layer(DefaultBodyLimit::max(2 * 1024 * 1024))) // 2MB for TS writes
        .route("/ts/write", post(routes::ts_write)
            .layer(DefaultBodyLimit::max(2 * 1024 * 1024))) // deprecated alias
        .route("/ts/{entity_id}/{property}", get(routes::ts_query))
        .route("/gql", post(routes::gql_query)
            .layer(DefaultBodyLimit::max(1024 * 1024))) // 1MB for GQL queries
        .route("/graph/slice", post(routes::graph_slice))
        .route("/graph/stats", get(routes::graph_stats))
        // React Flow interop
        .route("/graph/reactflow", get(routes::reactflow_export).post(routes::reactflow_import))
        // Schema management
        .route("/schemas", get(routes::list_schemas))
        .route("/schemas/nodes", post(routes::register_node_schema))
        .route("/schemas/edges", post(routes::register_edge_schema))
        .route("/schemas/nodes/{label}", get(routes::get_node_schema).put(routes::update_node_schema).delete(routes::delete_node_schema))
        .route("/schemas/edges/{label}", get(routes::get_edge_schema).delete(routes::delete_edge_schema))
        .route("/schemas/import", post(routes::import_schema_pack))
        // CSV import/export
        .route("/graph/csv", post(routes::csv_import).get(routes::csv_export)
            .layer(DefaultBodyLimit::max(4 * 1024 * 1024))) // 4 MB for CSV
        .route("/import/csv", post(routes::csv_import)
            .layer(DefaultBodyLimit::max(4 * 1024 * 1024))) // deprecated alias
        .route("/export/csv", get(routes::csv_export)) // deprecated alias
        .route("/snapshot", get(routes::snapshot_export))
        .route("/subscribe", get(routes::subscribe::subscribe));

    // RDF import/export
    let app = app.route(
        "/graph/rdf",
        get(routes::export_rdf)
            .post(routes::import_rdf)
            .layer(DefaultBodyLimit::max(4 * 1024 * 1024)),
    ); // 4 MB for RDF

    // SPARQL Protocol endpoint
    let app = app.route(
        "/sparql",
        get(routes::sparql_get)
            .post(routes::sparql_post)
            .layer(DefaultBodyLimit::max(1024 * 1024)),
    ); // 1 MB for SPARQL queries

    let app = app
        // WebSocket subscriptions
        .route("/ws/subscribe", get(ws::ws_subscribe))
        // Prometheus metrics (optional bearer token via config.http.metrics_token)
        .route("/metrics", get(routes::prometheus_metrics))
        .with_state(state.clone());

    // Mount MCP when enabled. OAuth provides auth even without a static API key,
    // so MCP is available whenever the config flag is on.
    let app = if state.config.mcp.enabled {
        // OAuth routes (public, no auth -- they *are* the auth flow).
        // Built as a sub-router with the AuthCodeStore extension, then merged.
        // Retrieve the AuthCodeStore from ServiceRegistry (registered at bootstrap).
        // Fall back to a fresh store if MCP was enabled but somehow not registered.
        let auth_code_store = state
            .services
            .get::<mcp::oauth::AuthCodeStore>()
            .cloned()
            .unwrap_or_default();
        let oauth_routes = Router::new()
            .route(
                "/.well-known/oauth-authorization-server",
                get(mcp::oauth::oauth_metadata),
            )
            .route("/oauth/register", post(mcp::oauth::oauth_register))
            .route("/oauth/authorize", get(mcp::oauth::oauth_authorize))
            .route("/oauth/approve", post(mcp::oauth::oauth_approve))
            .route("/oauth/token", post(mcp::oauth::oauth_token))
            .route("/oauth/revoke", post(mcp::oauth::oauth_revoke))
            .layer(axum::Extension(auth_code_store))
            .with_state(state.clone());
        let app = app.merge(oauth_routes);

        // Session manager with configured timeout and keep-alive.
        use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
        let mut session_mgr = LocalSessionManager::default();
        session_mgr.session_config.keep_alive = Some(std::time::Duration::from_secs(
            state.config.mcp.session_timeout_secs,
        ));
        let session_manager = Arc::new(session_mgr);
        let mut sse_config = rmcp::transport::StreamableHttpServerConfig::default();
        sse_config.sse_keep_alive = Some(std::time::Duration::from_secs(30));

        if state.config.dev_mode {
            // Dev mode: no auth middleware, static dev_admin context.
            let mcp_state = state.clone();
            let mcp_service = rmcp::transport::StreamableHttpService::new(
                move || {
                    Ok(mcp::SeleneTools::new(
                        mcp_state.clone(),
                        AuthContext::dev_admin(),
                    ))
                },
                session_manager,
                sse_config,
            );
            app.route("/mcp", axum::routing::any_service(mcp_service))
        } else {
            // Production: require Bearer JWT or static API key.
            //
            // The middleware validates the token and scopes the AuthContext
            // into a tokio task-local. The rmcp factory reads it back on the
            // same task, providing race-free per-request auth without shared
            // mutable state.
            let mcp_state = state.clone();
            let mcp_service = rmcp::transport::StreamableHttpService::new(
                move || {
                    let auth = MCP_AUTH_CTX.try_with(AuthContext::clone).map_err(|_| {
                        tracing::error!(
                            "MCP factory called outside auth scope: \
                             possible middleware bypass or task spawn. Denying access."
                        );
                        std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "MCP session creation failed: missing auth context",
                        )
                    })?;
                    Ok(mcp::SeleneTools::new(mcp_state.clone(), auth))
                },
                session_manager,
                sse_config,
            );

            let api_key = state.config.mcp.api_key.clone().unwrap_or_default();
            let middleware_state = state.clone();
            app.route(
                "/mcp",
                axum::routing::any_service(mcp_service).layer(axum::middleware::from_fn(
                    move |req, next: axum::middleware::Next| {
                        let st = middleware_state.clone();
                        let key = api_key.clone();
                        async move {
                            match validate_mcp_bearer(&st, &key, &req) {
                                Ok(auth) => MCP_AUTH_CTX.scope(auth, next.run(req)).await,
                                Err(failure) => unauthorized_response(failure),
                            }
                        }
                    },
                )),
            )
        }
    } else {
        app
    };

    // Add ServerState as Extension so HttpAuth extractor can access it
    // Add fallback for unmatched routes (returns JSON error instead of empty body)
    let app = app.fallback(routes::fallback_handler);

    app.layer(axum::Extension(state))
}

/// Serve a pre-composed router with standard Selene middleware.
///
/// Embedders compose their own router (merging Selene + app routes),
/// then call this to bind and serve with standard middleware (CORS,
/// body limits, tracing, concurrency).
///
/// Pass a `CancellationToken` for graceful shutdown: in-flight requests
/// drain before the server exits. Pass `None` for no graceful shutdown.
pub async fn serve_router(
    router: Router,
    config: &crate::config::HttpConfig,
    dev_mode: bool,
    shutdown: Option<tokio_util::sync::CancellationToken>,
) -> anyhow::Result<()> {
    let cors = if dev_mode {
        CorsLayer::permissive()
    } else if !config.cors_origins.is_empty() {
        use tower_http::cors::AllowOrigin;
        let origins: Vec<axum::http::HeaderValue> = config
            .cors_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins))
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::PUT,
                axum::http::Method::DELETE,
                axum::http::Method::OPTIONS,
            ])
            .allow_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
            ])
    } else {
        CorsLayer::new()
    };
    let x_request_id = axum::http::HeaderName::from_static("x-request-id");
    let app = router
        .layer(tower_http::request_id::PropagateRequestIdLayer::new(
            x_request_id.clone(),
        ))
        .layer(tower_http::request_id::SetRequestIdLayer::new(
            x_request_id,
            tower_http::request_id::MakeRequestUuid,
        ))
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_CONTENT_TYPE_OPTIONS,
            axum::http::HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_FRAME_OPTIONS,
            axum::http::HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
        ))
        .layer(TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            std::time::Duration::from_secs(60),
        ))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            Arc::new(rate_limit::EndpointRateLimiter::from_config(
                &config.rate_limit,
            )),
            rate_limit::rate_limit_middleware,
        ))
        .layer(tower::limit::ConcurrencyLimitLayer::new(128));

    // HSTS: add Strict-Transport-Security when running behind TLS (not dev, not plaintext).
    let app = if !dev_mode && !config.allow_plaintext {
        app.layer(SetResponseHeaderLayer::if_not_present(
            axum::http::header::STRICT_TRANSPORT_SECURITY,
            axum::http::HeaderValue::from_static("max-age=31536000"),
        ))
    } else {
        app
    };

    let rl = &config.rate_limit;
    tracing::info!(
        read = rl.read_per_sec,
        write = rl.write_per_sec,
        query = rl.query_per_sec,
        data = rl.data_per_sec,
        "per-endpoint rate limits (req/s, 0 = disabled)"
    );
    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tracing::info!(addr = %config.listen_addr, "HTTP listener started");
    let serve = axum::serve(listener, app);
    if let Some(token) = shutdown {
        serve
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;
    } else {
        serve.await?;
    }
    Ok(())
}

/// Start the HTTP listener with Selene's default router.
pub async fn serve(
    state: Arc<ServerState>,
    shutdown: Option<tokio_util::sync::CancellationToken>,
) -> anyhow::Result<()> {
    let dev_mode = state.config.dev_mode;
    if !dev_mode && !state.config.http.allow_plaintext {
        anyhow::bail!(
            "HTTP is configured without TLS in production mode. \
             Set [http] allow_plaintext = true if using a TLS-terminating reverse proxy, \
             or disable HTTP with [http] enabled = false."
        );
    }
    if !dev_mode && state.config.http.allow_plaintext {
        tracing::warn!(
            "HTTP running without TLS (allow_plaintext = true). \
             Ensure a TLS-terminating reverse proxy is in front."
        );
    }
    let config = state.config.http.clone();
    serve_router(router(state), &config, dev_mode, shutdown).await
}
