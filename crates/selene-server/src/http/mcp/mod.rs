//! MCP tool server for Selene -- calls ops directly (no QUIC round-trip).
//!
//! Provides AI agents with full access to the Selene property graph,
//! time-series data, GQL queries, schema management, and graph algorithms.

mod completions;
pub(crate) mod format;
pub(crate) mod oauth;
pub(super) mod params;
mod prompts;
mod resources;
mod tools;

use std::collections::HashSet;
use std::sync::Arc;

use rmcp::handler::server::router::prompt::PromptRouter;
use rmcp::handler::server::tool::{ToolCallContext, ToolRouter};
use rmcp::model::{
    CallToolRequestParams, CallToolResult, CompleteRequestParams, CompleteResult, ErrorCode,
    GetPromptRequestParams, GetPromptResult, Implementation, ListPromptsResult, ListToolsResult,
    LoggingLevel, LoggingMessageNotificationParam, PaginatedRequestParams,
    ResourceUpdatedNotificationParam, ServerCapabilities, ServerInfo, SetLevelRequestParams,
    SubscribeRequestParams, UnsubscribeRequestParams,
};
use rmcp::service::{NotificationContext, Peer, RequestContext};
use rmcp::{ErrorData as McpError, RoleServer, prompt_handler};

use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use crate::ops;

// ── Custom Tool Extension Point ─────────────────────────────────────

/// Trait for embedder-provided MCP tools.
///
/// Implement this trait to register custom tools that appear alongside
/// Selene's built-in tools in the MCP tool list. The tool receives the
/// `ServerState` and the session's `AuthContext` for access to the graph,
/// auth engine, and other services.
///
/// # Example
///
/// ```ignore
/// struct MyTool;
///
/// impl CustomMcpTool for MyTool {
///     fn definition(&self) -> rmcp::model::Tool {
///         rmcp::model::Tool {
///             name: "my_custom_tool".into(),
///             description: Some("Does something custom".into()),
///             ..Default::default()
///         }
///     }
///
///     fn call(
///         &self,
///         arguments: Option<serde_json::Map<String, serde_json::Value>>,
///         state: &ServerState,
///         auth: &AuthContext,
///     ) -> Result<CallToolResult, McpError> {
///         Ok(CallToolResult::success(vec![
///             rmcp::model::Content::text("hello from custom tool"),
///         ]))
///     }
/// }
/// ```
pub trait CustomMcpTool: Send + Sync {
    /// The tool definition (name, description, input schema) for `list_tools`.
    fn definition(&self) -> rmcp::model::Tool;

    /// Execute the tool. Called when a client invokes a tool whose name
    /// matches `definition().name` and is not handled by the static router.
    fn call(
        &self,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
        state: &ServerState,
        auth: &AuthContext,
    ) -> Result<CallToolResult, McpError>;
}

/// Registry of embedder-provided custom MCP tools.
///
/// Register tools before creating `ServerState`, then pass the registry
/// to the service registry so the MCP server picks them up at session creation.
#[derive(Default, Clone)]
pub struct CustomToolRegistry {
    tools: Arc<Vec<Box<dyn CustomMcpTool>>>,
}

impl CustomToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a registry from a list of custom tools.
    pub fn with_tools(tools: Vec<Box<dyn CustomMcpTool>>) -> Self {
        Self {
            tools: Arc::new(tools),
        }
    }
}

impl crate::service_registry::Service for CustomToolRegistry {
    fn name(&self) -> &'static str {
        "mcp-custom-tools"
    }

    fn health(&self) -> crate::service_registry::ServiceHealth {
        crate::service_registry::ServiceHealth::Healthy
    }
}

// ── MCP Tool Server ──────────────────────────────────────────────────

#[derive(Clone)]
pub struct SeleneTools {
    pub(crate) state: Arc<ServerState>,
    /// Auth context for this MCP session (resolved at session creation).
    pub(crate) auth: AuthContext,
    /// Tool dispatch router (built once per session by the `#[tool_router]` macro).
    tool_router: ToolRouter<Self>,
    /// Prompt dispatch router.
    prompt_router: PromptRouter<Self>,
    /// Current logging level set by the client via `logging/setLevel`.
    log_level: Arc<tokio::sync::Mutex<LoggingLevel>>,
    /// Peer handle for sending notifications (logging, progress, resource updates).
    /// Set during `on_initialized`; None until the MCP handshake completes.
    peer: Arc<tokio::sync::Mutex<Option<Peer<RoleServer>>>>,
    /// URIs the client has subscribed to for update notifications.
    subscribed_uris: Arc<tokio::sync::Mutex<HashSet<String>>>,
    /// Embedder-registered custom tools (checked after the static router).
    custom_tools: Arc<Vec<Box<dyn CustomMcpTool>>>,
}

/// Return the auth context for the current MCP session.
///
/// Currently infallible (auth is validated at transport level), but
/// returns `Result` for forward compatibility with per-principal scope
/// checking, token expiry, and rate limiting.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn mcp_auth(tools: &SeleneTools) -> Result<AuthContext, McpError> {
    Ok(tools.auth.clone())
}

pub(crate) fn op_err(e: ops::OpError) -> McpError {
    let code = match &e {
        ops::OpError::NotFound { .. } => ErrorCode::INVALID_PARAMS,
        ops::OpError::AuthDenied => ErrorCode::INVALID_REQUEST,
        ops::OpError::InvalidRequest(_) => ErrorCode::INVALID_PARAMS,
        ops::OpError::SchemaViolation(_) => ErrorCode::INVALID_PARAMS,
        ops::OpError::QueryError(_) | ops::OpError::Internal(_) => ErrorCode::INTERNAL_ERROR,
        ops::OpError::ReadOnly => ErrorCode::INVALID_REQUEST,
        ops::OpError::Conflict(_) => ErrorCode::INVALID_REQUEST,
        ops::OpError::ResourcesExhausted(_) => ErrorCode::INTERNAL_ERROR,
    };
    McpError {
        code,
        message: e.to_string().into(),
        data: None,
    }
}

pub(crate) fn reject_replica(state: &ServerState) -> Result<(), McpError> {
    if state.replica.is_replica {
        Err(op_err(ops::OpError::ReadOnly))
    } else {
        Ok(())
    }
}

impl SeleneTools {
    pub fn new(state: Arc<ServerState>, auth: AuthContext) -> Self {
        let custom_tools = state
            .services
            .get::<CustomToolRegistry>()
            .map(|r| Arc::clone(&r.tools))
            .unwrap_or_default();
        Self {
            state,
            auth,
            tool_router: Self::build_tool_router(),
            prompt_router: Self::build_prompt_router(),
            log_level: Arc::new(tokio::sync::Mutex::new(LoggingLevel::Warning)),
            peer: Arc::new(tokio::sync::Mutex::new(None)),
            subscribed_uris: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            custom_tools,
        }
    }

    /// Send a log message to the MCP client if the level meets the threshold.
    #[allow(dead_code)]
    pub(crate) async fn send_log(&self, level: LoggingLevel, logger: &str, message: &str) {
        let threshold = *self.log_level.lock().await;
        if (level as u8) >= (threshold as u8)
            && let Some(peer) = self.peer.lock().await.as_ref()
        {
            let _ = peer
                .notify_logging_message(LoggingMessageNotificationParam {
                    level,
                    logger: Some(logger.to_string()),
                    data: serde_json::Value::String(message.to_string()),
                })
                .await;
        }
    }

    /// Submit a write operation through the mutation batcher.
    /// Handles replica rejection, Arc cloning, auth, batcher submission, and error mapping.
    pub(crate) async fn mutate<F, T>(&self, f: F) -> Result<T, McpError>
    where
        F: FnOnce(&ServerState, &AuthContext) -> Result<T, ops::OpError> + Send + 'static,
        T: Send + 'static,
    {
        reject_replica(&self.state)?;
        let auth = mcp_auth(self)?;
        let st = Arc::clone(&self.state);
        self.state
            .mutation_batcher
            .submit(move || f(&st, &auth))
            .await
            .map_err(|e| op_err(ops::OpError::Internal(e.to_string())))?
            .map_err(op_err)
    }

    /// Submit a pre-prepared closure through the mutation batcher.
    /// Caller handles auth and data preparation; this just submits + maps errors.
    pub(crate) async fn submit_mut<F, T>(&self, f: F) -> Result<T, McpError>
    where
        F: FnOnce() -> Result<T, ops::OpError> + Send + 'static,
        T: Send + 'static,
    {
        self.state
            .mutation_batcher
            .submit(f)
            .await
            .map_err(|e| op_err(ops::OpError::Internal(e.to_string())))?
            .map_err(op_err)
    }
}

// Manual `call_tool` implementation (instead of `#[tool_handler]`) to add
// cancellation support via `tokio::select!` on the request's cancellation token.
//
// The `ServerHandler` trait defines methods returning `impl Future<...>` rather
// than `async fn`, so we must match that signature (clippy::manual_async_fn).
#[allow(clippy::manual_async_fn)]
#[prompt_handler]
impl rmcp::ServerHandler for SeleneTools {
    fn get_info(&self) -> ServerInfo {
        let capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .enable_resources_subscribe()
            .enable_prompts()
            .enable_logging()
            .enable_completions()
            .build();

        ServerInfo::new(capabilities)
            .with_server_info(
                Implementation::new("selene", env!("CARGO_PKG_VERSION"))
                    .with_title("Selene Property Graph".to_string())
                    .with_description(
                        "Lightweight in-memory property graph runtime with time-series, \
                         GQL queries, schema validation, and graph algorithms.",
                    ),
            )
            .with_instructions(
                "Selene is a lightweight, domain-agnostic in-memory property graph runtime. \
                 \n\nCore concepts:\
                 \n- Nodes have labels (categories) and properties (key-value pairs)\
                 \n- Edges are directed relationships between nodes\
                 \n- Time-series: numeric readings with entity_id, property, timestamp, value\
                 \n- Schemas define expected property types, defaults, and validation rules\
                 \n\nQuery with GQL (use the 'gql_query' tool).\
                 \nExample: MATCH (s:sensor) RETURN s.name AS name\
                 \nGraph navigation: use node_edges to explore a node's relationships\
                 \n\nResources: use selene://schemas, selene://stats, selene://health for read-only data.",
            )
    }

    // ── Tool dispatch with cancellation ─────────────────────────────

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        async move {
            let ct = context.ct.clone();
            let tool_name = request.name.clone();
            let tool_args = request.arguments.clone();
            let tool_context = ToolCallContext::new(self, request, context);
            tokio::select! {
                result = self.tool_router.call(tool_context) => {
                    match result {
                        // Static router handled it.
                        Ok(r) => Ok(r),
                        // Static router returned "tool not found": try custom tools.
                        Err(e) if e.code == ErrorCode::INVALID_PARAMS => {
                            if let Some(custom) = self.custom_tools.iter().find(|t| t.definition().name == tool_name) {
                                custom.call(tool_args, &self.state, &self.auth)
                            } else {
                                Err(e)
                            }
                        }
                        Err(e) => Err(e),
                    }
                },
                _ = ct.cancelled() => Err(McpError {
                    code: ErrorCode::INTERNAL_ERROR,
                    message: "request cancelled by client".into(),
                    data: None,
                }),
            }
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        let mut tools = self.tool_router.list_all();
        tools.extend(self.custom_tools.iter().map(|t| t.definition()));
        std::future::ready(Ok(ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
        }))
    }

    // ── Logging capability ──────────────────────────────────────────

    fn set_level(
        &self,
        request: SetLevelRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<(), McpError>> + Send + '_ {
        async move {
            *self.log_level.lock().await = request.level;
            Ok(())
        }
    }

    // ── Completions ──────────────────────────────────────────────────

    fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CompleteResult, McpError>> + Send + '_ {
        self.handle_complete(request, context)
    }

    // ── Resource subscriptions ─────────────────────────────────────

    fn subscribe(
        &self,
        request: SubscribeRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<(), McpError>> + Send + '_ {
        async move {
            self.subscribed_uris
                .lock()
                .await
                .insert(request.uri.clone());
            tracing::debug!(uri = %request.uri, "MCP client subscribed to resource");
            Ok(())
        }
    }

    fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<(), McpError>> + Send + '_ {
        async move {
            self.subscribed_uris.lock().await.remove(&request.uri);
            tracing::debug!(uri = %request.uri, "MCP client unsubscribed from resource");
            Ok(())
        }
    }

    // ── Lifecycle: capture peer for notifications ───────────────────

    fn on_initialized(
        &self,
        context: NotificationContext<RoleServer>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {
            let peer = context.peer.clone();
            *self.peer.lock().await = Some(context.peer);
            tracing::debug!("MCP client initialized, peer captured for notifications");

            // Spawn a background task that watches for graph changes and
            // notifies the client about subscribed resource updates.
            let subscribed = Arc::clone(&self.subscribed_uris);
            let mut rx = self.state.persistence.changelog_notify.subscribe();

            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(_seq) => {
                            let uris: Vec<String> =
                                { subscribed.lock().await.iter().cloned().collect() };
                            for uri in uris {
                                let param = ResourceUpdatedNotificationParam::new(uri);
                                if peer.notify_resource_updated(param).await.is_err() {
                                    // Peer disconnected; stop the task.
                                    tracing::debug!(
                                        "MCP peer disconnected, stopping subscription notifier"
                                    );
                                    return;
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::debug!(
                                "Changelog broadcast closed, stopping subscription notifier"
                            );
                            return;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                skipped = n,
                                "MCP subscription notifier lagged, some updates may be missed"
                            );
                            // Continue processing; the next recv will catch up.
                        }
                    }
                }
            });
        }
    }

    // ── Resources ───────────────────────────────────────────────────

    fn list_resources(
        &self,
        request: Option<rmcp::model::PaginatedRequestParams>,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> impl std::future::Future<Output = Result<rmcp::model::ListResourcesResult, McpError>> + Send + '_
    {
        self.handle_list_resources(request, context)
    }

    fn list_resource_templates(
        &self,
        request: Option<rmcp::model::PaginatedRequestParams>,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> impl std::future::Future<
        Output = Result<rmcp::model::ListResourceTemplatesResult, McpError>,
    > + Send
    + '_ {
        self.handle_list_resource_templates(request, context)
    }

    fn read_resource(
        &self,
        request: rmcp::model::ReadResourceRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> impl std::future::Future<Output = Result<rmcp::model::ReadResourceResult, McpError>> + Send + '_
    {
        self.handle_read_resource(request, context)
    }
}
