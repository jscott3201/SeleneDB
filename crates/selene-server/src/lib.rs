#![deny(unsafe_code)]
//! selene-server: QUIC + HTTP server with GQL query engine.
//!
//! Wires together `selene-graph`, `selene-ts`, `selene-persist`, and
//! `selene-gql` into a running service. Handles entity CRUD,
//! time-series, GQL queries, and changelog subscriptions.

pub mod auth;
pub mod bootstrap;
pub mod config;
mod config_sync;
pub mod federation;
pub mod history;
pub mod http;
pub(crate) mod merge_tracker;
pub(crate) mod metrics;
pub(crate) mod mutation_batcher;
pub mod ops;
pub mod quic;
#[cfg(feature = "rdf")]
pub(crate) mod rdf_service;
pub mod replica;
#[cfg(feature = "search")]
pub mod search;
pub mod service_registry;
pub(crate) mod stats_subscriber;
pub(crate) mod subscription;
pub(crate) mod sync;
pub(crate) mod sync_push;
pub(crate) mod sync_task;
pub mod tasks;
pub mod tls;
pub mod ts_history;
pub mod vault;
pub(crate) mod vector_store;
pub mod version_store;
pub(crate) mod view_state;
pub(crate) mod wal_coalescer;

// Public API re-exports
pub use bootstrap::ServerState;
pub use config::SeleneConfig;
pub use ops::init_start_time;
pub use quic::handler;
pub use subscription::{SubscriptionDef, SubscriptionFilter, SyncDirection};
pub use sync::{handle_sync_push, handle_sync_subscribe, validate_sync_subscribe};

// Embedder-friendly re-exports
pub use http::mcp::{CustomMcpTool, CustomToolRegistry};
pub use http::{router as api_router, serve_router};
pub use tasks::{BackgroundTasks, shutdown_snapshot, spawn_background_tasks};
