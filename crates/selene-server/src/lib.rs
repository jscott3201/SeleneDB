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
pub(crate) mod rdf_service;
pub mod replica;
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

/// Run the MCP server over stdin/stdout (stdio transport).
///
/// Blocks until the client disconnects (stdin EOF). Uses `dev_admin`
/// auth context since stdio implies trusted local access.
pub async fn serve_stdio_mcp(
    state: std::sync::Arc<ServerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use rmcp::ServiceExt;
    let tools = http::mcp::SeleneTools::new(state, auth::handshake::AuthContext::dev_admin());
    let transport = rmcp::transport::io::stdio();
    let server = tools.serve(transport).await?;
    let _ = server.waiting().await;
    Ok(())
}
pub use config::SeleneConfig;
pub use ops::init_start_time;
pub use quic::handler;
pub use subscription::{SubscriptionDef, SubscriptionFilter, SyncDirection};
pub use sync::{handle_sync_push, handle_sync_subscribe, validate_sync_subscribe};

// Embedder-friendly re-exports
pub use http::mcp::{CustomMcpTool, CustomToolRegistry};
pub use http::{router as api_router, serve_router};
pub use tasks::{BackgroundTasks, shutdown_snapshot, spawn_background_tasks};
