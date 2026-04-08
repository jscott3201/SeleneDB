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
}

impl EndpointRateLimiter {
    pub fn from_config(config: &RateLimitConfig) -> Self {
        Self {
            read: Mutex::new(TokenBucket::new(config.read_per_sec)),
            write: Mutex::new(TokenBucket::new(config.write_per_sec)),
            query: Mutex::new(TokenBucket::new(config.query_per_sec)),
            data: Mutex::new(TokenBucket::new(config.data_per_sec)),
        }
    }

    fn try_acquire(&self, tier: Tier) -> Result<(), u64> {
        let bucket = match tier {
            Tier::System => return Ok(()),
            Tier::Read => &self.read,
            Tier::Write => &self.write,
            Tier::Query => &self.query,
            Tier::Data => &self.data,
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

/// Classify a request into a rate limit tier by path and method.
fn classify(method: &Method, path: &str) -> Tier {
    // System endpoints: never rate-limited.
    match path {
        "/" | "/health" | "/ready" | "/info" | "/openapi.yaml" | "/metrics" => {
            return Tier::System;
        }
        _ => {}
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
    let tier = classify(req.method(), req.uri().path());

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

    #[test]
    fn classify_system_endpoints() {
        assert_eq!(classify(&Method::GET, "/health"), Tier::System);
        assert_eq!(classify(&Method::GET, "/ready"), Tier::System);
        assert_eq!(classify(&Method::GET, "/info"), Tier::System);
        assert_eq!(classify(&Method::GET, "/"), Tier::System);
    }

    #[test]
    fn classify_query_endpoints() {
        assert_eq!(classify(&Method::POST, "/gql"), Tier::Query);
        assert_eq!(classify(&Method::GET, "/sparql"), Tier::Query);
        assert_eq!(classify(&Method::POST, "/graph/slice"), Tier::Query);
    }

    #[test]
    fn classify_data_endpoints() {
        assert_eq!(classify(&Method::POST, "/graph/csv"), Tier::Data);
        assert_eq!(classify(&Method::POST, "/graph/rdf"), Tier::Data);
        assert_eq!(classify(&Method::POST, "/ts/samples"), Tier::Data);
        assert_eq!(classify(&Method::POST, "/mcp"), Tier::Data);
    }

    #[test]
    fn classify_read_write() {
        assert_eq!(classify(&Method::GET, "/nodes"), Tier::Read);
        assert_eq!(classify(&Method::GET, "/edges/42"), Tier::Read);
        assert_eq!(classify(&Method::POST, "/nodes"), Tier::Write);
        assert_eq!(classify(&Method::PUT, "/nodes/1"), Tier::Write);
        assert_eq!(classify(&Method::DELETE, "/edges/5"), Tier::Write);
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
        };
        let limiter = EndpointRateLimiter::from_config(&config);
        // Read tier disabled: always permits.
        for _ in 0..1000 {
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
        };
        let limiter = EndpointRateLimiter::from_config(&config);
        for _ in 0..1000 {
            assert!(limiter.try_acquire(Tier::System).is_ok());
        }
    }
}
