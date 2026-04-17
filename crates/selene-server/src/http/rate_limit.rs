//! Per-endpoint token bucket rate limiter.
//!
//! Classifies incoming HTTP requests into tiers (read, write, query, data)
//! and applies independent token bucket limits to each. System endpoints
//! (health, ready, info) bypass rate limiting entirely. Excess requests
//! receive 429 Too Many Requests with a `Retry-After` header.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use parking_lot::Mutex;

use crate::config::RateLimitConfig;

/// Rate limit tier for an endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Tier {
    /// Health, ready, info, openapi: never rate-limited.
    System,
    /// GET on nodes, edges, stats, time-series, schemas, reactflow, subscribe.
    Read,
    /// POST/PUT/DELETE on nodes, edges, schemas.
    Write,
    /// GQL, SPARQL, graph slice: potentially expensive query evaluation.
    Query,
    /// CSV/RDF import/export, time-series bulk writes: heavy I/O.
    Data,
    /// Unauthenticated requests on anything other than System endpoints.
    /// Has its own (much tighter) bucket so that a flood of anonymous traffic
    /// cannot exhaust the budget that authenticated clients depend on.
    Anonymous,
}

/// Token bucket: permits `capacity` requests per second, refilling continuously.
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(per_sec: u32) -> Self {
        let capacity = f64::from(per_sec);
        Self {
            capacity,
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns `true` if permitted.
    fn try_acquire(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Seconds until the next token is available (for Retry-After header).
    fn retry_after_secs(&self) -> u64 {
        if self.capacity <= 0.0 {
            return 1;
        }
        let deficit = 1.0 - self.tokens;
        let secs = deficit / self.capacity;
        (secs.ceil() as u64).max(1)
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.capacity).min(self.capacity);
    }
}

/// Per-tier rate limiter constructed from config.
pub struct EndpointRateLimiter {
    read: Mutex<TokenBucket>,
    write: Mutex<TokenBucket>,
    query: Mutex<TokenBucket>,
    data: Mutex<TokenBucket>,
    anonymous: Mutex<TokenBucket>,
}

impl EndpointRateLimiter {
    pub fn from_config(config: &RateLimitConfig) -> Self {
        Self {
            read: Mutex::new(TokenBucket::new(config.read_per_sec)),
            write: Mutex::new(TokenBucket::new(config.write_per_sec)),
            query: Mutex::new(TokenBucket::new(config.query_per_sec)),
            data: Mutex::new(TokenBucket::new(config.data_per_sec)),
            anonymous: Mutex::new(TokenBucket::new(config.anonymous_per_sec)),
        }
    }

    fn try_acquire(&self, tier: Tier) -> Result<(), u64> {
        let bucket = match tier {
            Tier::System => return Ok(()),
            Tier::Read => &self.read,
            Tier::Write => &self.write,
            Tier::Query => &self.query,
            Tier::Data => &self.data,
            Tier::Anonymous => &self.anonymous,
        };
        let mut guard = bucket.lock();
        // Tier disabled (0 req/s in config): always permit.
        if guard.capacity <= 0.0 {
            return Ok(());
        }
        if guard.try_acquire() {
            Ok(())
        } else {
            Err(guard.retry_after_secs())
        }
    }
}

/// Heuristic: does this `Authorization` header value look like a bearer
/// token a downstream extractor might accept?
///
/// Filters out empty values, whitespace-only values, and headers that
/// don't carry the `Bearer ` scheme. Does NOT validate the token — that
/// happens downstream. This is purely a tier-routing decision: callers
/// that pass this check are charged against the per-tier (authenticated)
/// budgets; callers that fail go to [`Tier::Anonymous`].
fn looks_like_bearer(value: &str) -> bool {
    let trimmed = value.trim();
    let Some(rest) = trimmed
        .strip_prefix("Bearer ")
        .or_else(|| trimmed.strip_prefix("bearer "))
    else {
        return false;
    };
    !rest.trim().is_empty()
}

/// Classify a request into a rate limit tier by path, method, and whether
/// the caller presented an `Authorization` header.
///
/// Unauthenticated requests on anything other than a System endpoint go to
/// the [`Tier::Anonymous`] bucket so that a flood of anonymous traffic can't
/// drain the per-tier budget that authenticated clients depend on. Whether
/// the bearer is *valid* is checked downstream by the auth extractor; we
/// only need its presence here to decide which budget to charge.
fn classify(method: &Method, path: &str, authenticated: bool) -> Tier {
    // System endpoints: never rate-limited, even anonymously.
    match path {
        "/" | "/health" | "/ready" | "/info" | "/openapi.yaml" | "/metrics" => {
            return Tier::System;
        }
        _ => {}
    }

    if !authenticated {
        return Tier::Anonymous;
    }

    // Query tier: GQL, SPARQL, graph slice.
    if path == "/gql" || path == "/sparql" || path == "/graph/slice" {
        return Tier::Query;
    }

    // Data tier: CSV, RDF, time-series writes, MCP.
    if path == "/graph/csv"
        || path == "/import/csv"
        || path == "/export/csv"
        || path == "/graph/rdf"
        || path == "/ts/samples"
        || path == "/ts/write"
        || path == "/mcp"
    {
        return Tier::Data;
    }

    // Everything else: read vs. write by HTTP method.
    if *method == Method::GET || *method == Method::HEAD || *method == Method::OPTIONS {
        Tier::Read
    } else {
        Tier::Write
    }
}

/// Axum middleware that applies per-tier rate limiting.
///
/// Insert via `axum::middleware::from_fn_with_state` with an
/// `Arc<EndpointRateLimiter>` state.
pub async fn rate_limit_middleware(
    axum::extract::State(limiter): axum::extract::State<Arc<EndpointRateLimiter>>,
    req: Request,
    next: Next,
) -> Response {
    // We need to decide which budget to charge *before* running the auth
    // extractor (which is downstream in the route handler). A truly robust
    // fix would reorder so auth runs as middleware and the rate limiter
    // reads the resolved principal from request extensions; that's a
    // larger restructuring tracked separately.
    //
    // For now, classify as authenticated only when the header looks like a
    // well-formed `Bearer <token>` with a non-trivial token. This rejects
    // empty values, whitespace, and obvious garbage, so casual probes go
    // to the Anonymous bucket. A motivated attacker can still send a
    // syntactically-valid bearer with a bogus secret and be classified as
    // authenticated, but the auth extractor will reject the request before
    // any expensive work runs and the auth-failure rate limiter
    // (`AuthRateLimiter`) takes over after 5 failed attempts per identity.
    let authenticated = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .is_some_and(looks_like_bearer);
    let tier = classify(req.method(), req.uri().path(), authenticated);

    match limiter.try_acquire(tier) {
        Ok(()) => next.run(req).await,
        Err(retry_after) => {
            tracing::debug!(
                tier = ?tier,
                path = req.uri().path(),
                retry_after,
                "rate limited"
            );
            (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", retry_after.to_string())],
                Json(serde_json::json!({
                    "error": "rate limit exceeded",
                    "retry_after": retry_after
                })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: existing tests use authenticated=true; the unauthenticated
    // path is exercised separately below.
    fn cls(method: &Method, path: &str) -> Tier {
        classify(method, path, true)
    }

    #[test]
    fn classify_system_endpoints() {
        assert_eq!(cls(&Method::GET, "/health"), Tier::System);
        assert_eq!(cls(&Method::GET, "/ready"), Tier::System);
        assert_eq!(cls(&Method::GET, "/info"), Tier::System);
        assert_eq!(cls(&Method::GET, "/"), Tier::System);
    }

    #[test]
    fn classify_query_endpoints() {
        assert_eq!(cls(&Method::POST, "/gql"), Tier::Query);
        assert_eq!(cls(&Method::GET, "/sparql"), Tier::Query);
        assert_eq!(cls(&Method::POST, "/graph/slice"), Tier::Query);
    }

    #[test]
    fn classify_data_endpoints() {
        assert_eq!(cls(&Method::POST, "/graph/csv"), Tier::Data);
        assert_eq!(cls(&Method::POST, "/graph/rdf"), Tier::Data);
        assert_eq!(cls(&Method::POST, "/ts/samples"), Tier::Data);
        assert_eq!(cls(&Method::POST, "/mcp"), Tier::Data);
    }

    #[test]
    fn classify_read_write() {
        assert_eq!(cls(&Method::GET, "/nodes"), Tier::Read);
        assert_eq!(cls(&Method::GET, "/edges/42"), Tier::Read);
        assert_eq!(cls(&Method::POST, "/nodes"), Tier::Write);
        assert_eq!(cls(&Method::PUT, "/nodes/1"), Tier::Write);
        assert_eq!(cls(&Method::DELETE, "/edges/5"), Tier::Write);
    }

    #[test]
    fn unauthenticated_non_system_uses_anonymous_tier() {
        // The whole point of the Anonymous tier: a flood of unauthenticated
        // hits on /gql or /nodes must drain the Anonymous bucket, not the
        // shared Query/Read budgets that authenticated callers depend on.
        assert_eq!(classify(&Method::POST, "/gql", false), Tier::Anonymous);
        assert_eq!(classify(&Method::GET, "/nodes", false), Tier::Anonymous);
        assert_eq!(
            classify(&Method::POST, "/graph/csv", false),
            Tier::Anonymous
        );
        assert_eq!(
            classify(&Method::DELETE, "/edges/5", false),
            Tier::Anonymous
        );
    }

    #[test]
    fn looks_like_bearer_filters_garbage() {
        // Accept: well-formed Bearer values
        assert!(super::looks_like_bearer("Bearer xyz123"));
        assert!(super::looks_like_bearer("bearer xyz123")); // case-insensitive scheme
        assert!(super::looks_like_bearer("  Bearer abc  ")); // leading/trailing space ok
        assert!(super::looks_like_bearer("Bearer principal:secret")); // colon-separated form Selene uses

        // Reject: anything that wouldn't pass downstream auth anyway
        assert!(!super::looks_like_bearer(""));
        assert!(!super::looks_like_bearer("   "));
        assert!(!super::looks_like_bearer("Bearer "));
        assert!(!super::looks_like_bearer("Bearer  "));
        assert!(!super::looks_like_bearer("Basic abc"));
        assert!(!super::looks_like_bearer("xyz123"));
        assert!(!super::looks_like_bearer("BearerNoSpace"));
    }

    #[test]
    fn unauthenticated_system_endpoints_still_bypass() {
        // Unauthenticated is fine for liveness probes and discovery.
        assert_eq!(classify(&Method::GET, "/health", false), Tier::System);
        assert_eq!(classify(&Method::GET, "/openapi.yaml", false), Tier::System);
    }

    #[test]
    fn token_bucket_basic() {
        let mut bucket = TokenBucket::new(10);
        // Fresh bucket should have full capacity.
        for _ in 0..10 {
            assert!(bucket.try_acquire());
        }
        // 11th request should fail.
        assert!(!bucket.try_acquire());
    }

    #[test]
    fn disabled_tier_always_permits() {
        let config = RateLimitConfig {
            read_per_sec: 0,
            write_per_sec: 100,
            query_per_sec: 50,
            data_per_sec: 20,
            anonymous_per_sec: 10,
        };
        let limiter = EndpointRateLimiter::from_config(&config);
        // Read tier disabled: always permits.
        for _ in 0..1000 {
            assert!(limiter.try_acquire(Tier::Read).is_ok());
        }
    }

    #[test]
    fn anonymous_bucket_independent_of_authenticated_tiers() {
        // The whole reason the Anonymous tier exists: draining it must
        // not affect the per-tier (read/write/query/data) budgets.
        let config = RateLimitConfig {
            read_per_sec: 100,
            write_per_sec: 100,
            query_per_sec: 100,
            data_per_sec: 100,
            anonymous_per_sec: 5,
        };
        let limiter = EndpointRateLimiter::from_config(&config);
        // Burn the entire Anonymous budget.
        for _ in 0..5 {
            assert!(limiter.try_acquire(Tier::Anonymous).is_ok());
        }
        assert!(
            limiter.try_acquire(Tier::Anonymous).is_err(),
            "anonymous tier should be exhausted after 5 hits"
        );
        // Authenticated tiers must still have their full budget intact.
        for _ in 0..100 {
            assert!(limiter.try_acquire(Tier::Query).is_ok());
            assert!(limiter.try_acquire(Tier::Read).is_ok());
        }
    }

    #[test]
    fn token_bucket_refills_over_time() {
        let mut bucket = TokenBucket::new(100); // 100 tokens/sec
        // Drain all tokens.
        for _ in 0..100 {
            assert!(bucket.try_acquire());
        }
        assert!(!bucket.try_acquire());
        // Wait 50ms: should refill ~5 tokens (100/sec * 0.05s).
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            bucket.try_acquire(),
            "bucket should have refilled after sleep"
        );
    }

    #[test]
    fn system_tier_always_permits() {
        let config = RateLimitConfig {
            read_per_sec: 1,
            write_per_sec: 1,
            query_per_sec: 1,
            data_per_sec: 1,
            anonymous_per_sec: 1,
        };
        let limiter = EndpointRateLimiter::from_config(&config);
        for _ in 0..1000 {
            assert!(limiter.try_acquire(Tier::System).is_ok());
        }
    }
}
