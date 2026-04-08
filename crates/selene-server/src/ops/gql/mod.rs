//! GQL execution bridge -- connects transports to selene-gql.
//!
//! Handles auth scope, read vs mutation dispatch, EXPLAIN/PROFILE,
//! error mapping to GQLSTATUS codes, and result format conversion.

mod ddl;
mod format;
mod routing;

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

use super::{OpError, persist_or_die};
use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use selene_core::Value;

use ddl::{
    execute_ddl, is_ddl_statement, is_graph_state_ddl, sync_search_indexes_after_ddl,
    sync_view_state_after_ddl,
};
use format::{batches_to_ipc, batches_to_json};
use routing::{
    execute_local_graph_query, execute_remote_query, execute_vault_query, parse_use_prefix,
};

/// Default query timeout in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Result format for GQL output.
#[derive(Clone, Copy)]
pub enum ResultFormat {
    Json,
    ArrowIpc,
}

/// GQL execution result -- transport-agnostic.
pub struct GqlQueryResult {
    /// 5-digit GQLSTATUS code (e.g. "00000", "42601").
    pub status_code: String,
    /// Human-readable status message.
    pub message: String,
    /// JSON data (populated when format=Json).
    pub data_json: Option<String>,
    /// Arrow IPC bytes (populated when format=ArrowIpc).
    pub data_arrow: Option<Vec<u8>>,
    /// Number of result rows.
    pub row_count: u64,
    /// Mutation statistics (if any).
    pub mutations: Option<MutationStatsResult>,
    /// Execution plan text (populated when explain/profile=true).
    pub plan: Option<String>,
}

/// Mutation statistics for the response.
pub struct MutationStatsResult {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub edges_created: usize,
    pub edges_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
}

/// Execute a GQL query through the ops layer.
///
/// Handles: auth checks, mutation detection, scope filtering,
/// EXPLAIN/PROFILE, error -> GQLSTATUS mapping, and result formatting.
pub fn execute_gql(
    state: &ServerState,
    auth: &AuthContext,
    query: &str,
    parameters: Option<&HashMap<String, Value>>,
    explain: bool,
    profile: bool,
    format: ResultFormat,
) -> Result<GqlQueryResult, OpError> {
    execute_gql_with_timeout(
        state, auth, query, parameters, explain, profile, format, None,
    )
}

/// Execute a GQL query with an optional per-query timeout.
///
/// Timeout is advisory -- checks elapsed time before and after execution but does
/// not cancel in-flight queries. MAX_BINDINGS is the primary resource limit.
#[allow(clippy::too_many_arguments)]
pub fn execute_gql_with_timeout(
    state: &ServerState,
    auth: &AuthContext,
    query: &str,
    parameters: Option<&HashMap<String, Value>>,
    explain: bool,
    profile: bool,
    format: ResultFormat,
    timeout_ms: Option<u32>,
) -> Result<GqlQueryResult, OpError> {
    let start = Instant::now();
    let deadline_ms = timeout_ms.map_or(DEFAULT_TIMEOUT_MS, u64::from);

    // Check for "USE <graph>" prefix -- route to vault, local graph, or remote peer
    if let Some((graph_name, remaining)) = parse_use_prefix(query) {
        let catalog = state
            .services
            .get::<crate::service_registry::GraphCatalogService>()
            .ok_or_else(|| OpError::Internal("GraphCatalogService not registered".into()))?;
        let resolver = super::graph_resolver::GraphResolver::new(
            &catalog.catalog,
            state.services.get::<crate::vault::VaultService>().is_some(),
            state
                .services
                .get::<crate::federation::FederationService>()
                .map(|svc| svc.registry.as_ref()),
        );
        return match resolver.resolve(graph_name) {
            Ok(super::graph_resolver::ResolvedGraph::Vault) => Ok(execute_vault_query(
                state,
                auth,
                remaining,
                format,
                start,
                deadline_ms,
            )),
            Ok(super::graph_resolver::ResolvedGraph::Local(graph)) => {
                Ok(execute_local_graph_query(
                    state,
                    auth,
                    remaining,
                    &graph,
                    format,
                    start,
                    deadline_ms,
                ))
            }
            Ok(super::graph_resolver::ResolvedGraph::Remote { peer_name }) => execute_remote_query(
                state,
                auth,
                remaining,
                &peer_name,
                format,
                start,
                deadline_ms,
            ),
            Ok(super::graph_resolver::ResolvedGraph::Default) => {
                unreachable!("GraphResolver never returns Default")
            }
            Err(msg) => Ok(error_result("XX000", &msg)),
        };
    }

    // Parse once (via plan cache), shared by auth check, explain, and execution.
    // Cache hit returns Arc::clone (~1 ns) instead of deep AST clone (~100 ns).
    let generation = state.graph.read(|g| g.generation());
    let stmt = match state.plan_cache.get_or_parse(query, generation) {
        Ok(s) => s,
        Err(e) => {
            audit_log(auth, query, "parse_error", 0, start.elapsed());
            return Ok(map_gql_error(&e));
        }
    };

    // DDL dispatch (admin-only)
    if is_ddl_statement(&stmt) {
        if !auth.is_admin() {
            audit_log(auth, query, "ddl_denied", 0, start.elapsed());
            return Ok(error_result("42501", "DDL requires Admin role"));
        }
        let result = execute_ddl(state, &stmt);
        audit_log(auth, query, "ddl", 0, start.elapsed());
        return result;
    }

    // Graph-state DDL (admin-only, requires SharedGraph write)
    if is_graph_state_ddl(&stmt) {
        if !auth.is_admin() {
            audit_log(auth, query, "ddl_denied", 0, start.elapsed());
            return Ok(error_result("42501", "DDL requires Admin role"));
        }
        crate::metrics::query_start();
        let result = execute_mutation(state, auth, query, None);
        crate::metrics::query_end();

        // Sync ViewStateStore after materialized view DDL succeeds.
        if result.is_ok() {
            sync_view_state_after_ddl(state, &stmt);
            sync_search_indexes_after_ddl(state, &stmt);
        }

        let elapsed = start.elapsed();
        return match result {
            Ok(gql_result) => {
                audit_log(auth, query, "ddl", gql_result.row_count(), elapsed);
                Ok(format_result(gql_result, format))
            }
            Err(ref gql_err) => {
                audit_log(auth, query, &format!("error:{gql_err}"), 0, elapsed);
                Ok(map_gql_error(gql_err))
            }
        };
    }

    // Auth check based on statement type
    let is_mutation = matches!(
        *stmt,
        selene_gql::GqlStatement::Mutate(_)
            | selene_gql::GqlStatement::StartTransaction
            | selene_gql::GqlStatement::Commit
            | selene_gql::GqlStatement::Rollback
    );

    let action = if is_mutation {
        Action::GqlMutate
    } else {
        Action::GqlQuery
    };
    if !auth.is_admin() && !state.auth_engine.authorize_action(auth, action) {
        audit_log(auth, query, "auth_denied", 0, start.elapsed());
        return Ok(error_result("42501", "insufficient privilege"));
    }

    // EXPLAIN: return plan text without executing
    if explain {
        let result = execute_explain(state, &stmt, profile);
        audit_log(auth, query, "explain", 0, start.elapsed());
        return result;
    }

    // Build GQL parameter map from core::Value parameters
    let gql_params = parameters.map(|p| {
        p.iter()
            .map(|(k, v)| (selene_core::IStr::new(k), selene_gql::GqlValue::from(v)))
            .collect::<selene_gql::ParameterMap>()
    });

    // Timeout check before execution
    if start.elapsed().as_millis() as u64 > deadline_ms {
        audit_log(auth, query, "timeout", 0, start.elapsed());
        return Ok(error_result(
            "57014",
            "query cancelled: timeout before execution",
        ));
    }

    // Execute -- pass parsed stmt to read path (avoids re-parsing)
    crate::metrics::query_start();
    let result = if is_mutation {
        execute_mutation(state, auth, query, gql_params.as_ref())
    } else {
        execute_read(state, auth, &stmt, gql_params.as_ref())
    };
    crate::metrics::query_end();

    let elapsed = start.elapsed();

    // Timeout check after execution
    if elapsed.as_millis() as u64 > deadline_ms {
        audit_log(auth, query, "timeout", 0, elapsed);
        return Ok(error_result(
            "57014",
            &format!(
                "query exceeded timeout: {}ms > {}ms",
                elapsed.as_millis(),
                deadline_ms
            ),
        ));
    }

    match result {
        Ok(ref gql_result) => {
            let rows = gql_result.row_count();
            audit_log(auth, query, "ok", rows, elapsed);
            crate::metrics::record_query(elapsed, true);
            Ok(format_result(result.unwrap(), format))
        }
        Err(ref gql_err) => {
            audit_log(auth, query, &format!("error:{gql_err}"), 0, elapsed);
            crate::metrics::record_query(elapsed, false);
            Ok(map_gql_error(gql_err))
        }
    }
}

// ── Shared helpers (visible to submodules) ──────────────────────────────

/// Emit a structured audit log for each query execution.
pub(super) fn audit_log(
    auth: &AuthContext,
    query: &str,
    status: &str,
    rows: usize,
    duration: std::time::Duration,
) {
    let q = if query.len() > 200 {
        &query[..query.floor_char_boundary(200)]
    } else {
        query
    };
    tracing::info!(
        principal = auth.principal_node_id.0,
        role = ?auth.role,
        query = q,
        status = status,
        rows = rows,
        duration_us = duration.as_micros() as u64,
        "gql_query"
    );
}

/// Execute a read-only query against a graph snapshot using a pre-parsed statement.
fn execute_read(
    state: &ServerState,
    auth: &AuthContext,
    stmt: &selene_gql::GqlStatement,
    parameters: Option<&selene_gql::ParameterMap>,
) -> Result<selene_gql::GqlResult, selene_gql::GqlError> {
    let snapshot = state.graph.load_snapshot();
    let scope = if auth.is_admin() {
        None
    } else {
        Some(&auth.scope)
    };
    // Use cached CSR (rebuilds only when graph generation changes).
    let csr = crate::bootstrap::get_or_build_csr(&state.csr_cache, &snapshot);
    let registry = selene_gql::runtime::procedures::ProcedureRegistry::with_builtins_and_catalog(
        state.projection_catalog.clone(),
    );
    let mut qb = selene_gql::QueryBuilder::from_statement(stmt, &snapshot)
        .with_hot_tier(&state.hot_tier)
        .with_procedures(&registry)
        .with_csr(&csr);
    if let Some(s) = scope {
        qb = qb.with_scope(s);
    }
    if let Some(p) = parameters {
        qb = qb.with_parameters(p);
    }
    catch_unwind(AssertUnwindSafe(|| qb.execute()))
        .unwrap_or_else(|panic| {
            let msg = panic_message(&panic);
            tracing::error!(message = %msg, "query execution panicked");
            Err(selene_gql::GqlError::Internal {
                message: format!("query execution panicked: {msg}"),
            })
        })
}

/// Execute a mutation via SharedGraph, persisting changes to WAL + changelog.
fn execute_mutation(
    state: &ServerState,
    auth: &AuthContext,
    query: &str,
    parameters: Option<&selene_gql::ParameterMap>,
) -> Result<selene_gql::GqlResult, selene_gql::GqlError> {
    if state.replica.is_replica {
        return Err(selene_gql::GqlError::Internal {
            message: "this is a read-only replica".into(),
        });
    }
    let scope = if auth.is_admin() {
        None
    } else {
        Some(&auth.scope)
    };
    // MutationBuilder re-parses internally (SharedGraph::write borrow semantics)
    let mut mb = selene_gql::MutationBuilder::new(query).with_hot_tier(&state.hot_tier);
    if let Some(s) = scope {
        mb = mb.with_scope(s);
    }
    if let Some(p) = parameters {
        mb = mb.with_parameters(p);
    }
    let graph = &state.graph;
    let result = catch_unwind(AssertUnwindSafe(|| mb.execute(graph)))
        .unwrap_or_else(|panic| {
            let msg = panic_message(&panic);
            tracing::error!(message = %msg, "mutation execution panicked");
            Err(selene_gql::GqlError::Internal {
                message: format!("mutation execution panicked: {msg}"),
            })
        })?;

    // Persist changes to WAL + changelog + version archive
    if !result.changes.is_empty() {
        persist_or_die(state, &result.changes);
    }

    Ok(result)
}

/// Format the execution plan as text without executing.
fn execute_explain(
    state: &ServerState,
    stmt: &selene_gql::GqlStatement,
    _profile: bool,
) -> Result<GqlQueryResult, OpError> {
    let snapshot = state.graph.load_snapshot();

    let plan_text = match stmt {
        selene_gql::GqlStatement::Query(pipeline) => {
            let plan =
                selene_gql::plan_query(pipeline, &snapshot).map_err(|e| map_gql_error_to_op(&e))?;
            selene_gql::format_plan(&plan)
        }
        selene_gql::GqlStatement::Mutate(mp) => {
            let plan =
                selene_gql::plan_mutation(mp, &snapshot).map_err(|e| map_gql_error_to_op(&e))?;
            selene_gql::format_plan(&plan)
        }
        _ => "Transaction control statement — no plan".to_string(),
    };

    Ok(GqlQueryResult {
        status_code: "00000".to_string(),
        message: "plan".to_string(),
        data_json: None,
        data_arrow: None,
        row_count: 0,
        mutations: None,
        plan: Some(plan_text),
    })
}

/// Format a successful GqlResult into the transport result.
pub(super) fn format_result(result: selene_gql::GqlResult, format: ResultFormat) -> GqlQueryResult {
    let status_code = result.status.code.code();

    let row_count = result.row_count() as u64;

    let ms = &result.mutations;
    let has_mutations = ms.nodes_created > 0
        || ms.nodes_deleted > 0
        || ms.edges_created > 0
        || ms.edges_deleted > 0
        || ms.properties_set > 0
        || ms.properties_removed > 0;
    let mutations = if has_mutations {
        Some(MutationStatsResult {
            nodes_created: ms.nodes_created,
            nodes_deleted: ms.nodes_deleted,
            edges_created: ms.edges_created,
            edges_deleted: ms.edges_deleted,
            properties_set: ms.properties_set,
            properties_removed: ms.properties_removed,
        })
    } else {
        None
    };

    let (data_json, data_arrow) = match format {
        ResultFormat::Json => {
            let json = batches_to_json(&result.batches, &result.schema);
            (Some(json), None)
        }
        ResultFormat::ArrowIpc => {
            let ipc = batches_to_ipc(&result.batches, &result.schema);
            (None, Some(ipc))
        }
    };

    GqlQueryResult {
        status_code: status_code.to_string(),
        message: result.status.message,
        data_json,
        data_arrow,
        row_count,
        mutations,
        plan: None,
    }
}

/// Map a GqlError to a GQLSTATUS result (no OpError -- errors are data).
pub(super) fn map_gql_error(err: &selene_gql::GqlError) -> GqlQueryResult {
    use selene_gql::GqlError;

    let (code, message) = match err {
        GqlError::Parse { message, .. } => ("42601", format!("syntax error: {message}")),
        GqlError::Type { message } => ("42804", format!("type mismatch: {message}")),
        GqlError::AuthDenied => ("42501", "insufficient privilege".to_string()),
        GqlError::ResourcesExhausted { message } => ("54000", format!("limit exceeded: {message}")),
        GqlError::UnknownProcedure { name } => ("42883", format!("unknown procedure: {name}")),
        GqlError::InvalidArgument { message } => ("42602", format!("invalid argument: {message}")),
        GqlError::SchemaViolation { message } => ("42S01", format!("schema violation: {message}")),
        GqlError::NotFound { entity, id } => ("02000", format!("{entity} {id} not found")),
        _ => ("XX000", format!("internal error: {err}")),
    };

    error_result(code, &message)
}

/// Map a GqlError to an OpError (for cases where we need to propagate).
fn map_gql_error_to_op(err: &selene_gql::GqlError) -> OpError {
    OpError::QueryError(err.to_string())
}

/// Create an error-status GqlQueryResult.
pub(super) fn error_result(code: &str, message: &str) -> GqlQueryResult {
    GqlQueryResult {
        status_code: code.to_string(),
        message: message.to_string(),
        data_json: Some("[]".to_string()),
        data_arrow: None,
        row_count: 0,
        mutations: None,
        plan: None,
    }
}

/// Extract a human-readable message from a panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::ServerState;

    async fn test_state() -> ServerState {
        let dir = tempfile::tempdir().unwrap();
        ServerState::for_testing(dir.path()).await
    }

    #[tokio::test]
    async fn read_query_returns_success() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let result = execute_gql(
            &state,
            &auth,
            "MATCH (n) RETURN count(*) AS total",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "00000");
        assert!(result.data_json.is_some());
    }

    #[tokio::test]
    async fn mutation_returns_stats() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let result = execute_gql(
            &state,
            &auth,
            "INSERT (:sensor {name: 'Test'})",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "00000");
        assert!(result.mutations.is_some());
        assert_eq!(result.mutations.as_ref().unwrap().nodes_created, 1);
    }

    #[tokio::test]
    async fn explain_returns_plan_text() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let result = execute_gql(
            &state,
            &auth,
            "MATCH (s:sensor) RETURN s.name AS name",
            None,
            true,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "00000");
        assert!(result.plan.is_some());
        assert!(result.plan.as_ref().unwrap().contains("LabelScan"));
    }

    #[tokio::test]
    async fn syntax_error_returns_42601() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let result = execute_gql(
            &state,
            &auth,
            "INVALID QUERY",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "42601");
    }

    #[tokio::test]
    async fn empty_result_returns_02000() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        let result = execute_gql(
            &state,
            &auth,
            "MATCH (s:nonexistent) RETURN s.name AS name",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "02000");
    }

    #[tokio::test]
    async fn query_with_parameters() {
        let state = test_state().await;
        let auth = AuthContext::dev_admin();
        // Insert a node first
        execute_gql(
            &state,
            &auth,
            "INSERT (:sensor {name: 'ParamTest', temp: 72.5})",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        // Query with $param
        let mut params = HashMap::new();
        params.insert("threshold".into(), Value::Float(70.0));
        let result = execute_gql(
            &state,
            &auth,
            "MATCH (s:sensor) FILTER s.temp > $threshold RETURN s.name AS name",
            Some(&params),
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "00000");
        assert_eq!(result.row_count, 1);
    }

    // ── Vault tests ──────────────────────────────────────────────────

    async fn test_state_with_vault() -> (ServerState, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServerState::for_testing(dir.path()).await;

        // Create a vault with dev key
        let master = crate::vault::crypto::MasterKey::dev_key();
        let vault_path = dir.path().join("secure.vault");
        let handle = crate::vault::VaultHandle::open_or_create(
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

    #[test]
    fn parse_use_prefix_general() {
        use routing::parse_use_prefix;

        // Secure graph
        let (name, rest) = parse_use_prefix("USE secure MATCH (n) RETURN n").unwrap();
        assert_eq!(name, "secure");
        assert_eq!(rest, "MATCH (n) RETURN n");

        // With semicolon
        let (name, rest) = parse_use_prefix("USE secure; MATCH (n:principal) RETURN n").unwrap();
        assert_eq!(name, "secure");
        assert_eq!(rest, "MATCH (n:principal) RETURN n");

        // Case insensitive USE keyword
        let (name, _) = parse_use_prefix("use secure MATCH (n) RETURN n").unwrap();
        assert_eq!(name, "secure");

        // Leading whitespace
        let (name, _) = parse_use_prefix("  USE SECURE MATCH (n) RETURN n").unwrap();
        assert_eq!(name, "SECURE");

        // Non-secure graph name
        let (name, rest) = parse_use_prefix("USE building_a MATCH (n) RETURN n").unwrap();
        assert_eq!(name, "building_a");
        assert_eq!(rest, "MATCH (n) RETURN n");

        // No USE prefix
        assert!(parse_use_prefix("MATCH (n) RETURN n").is_none());

        // USE without following query
        assert!(parse_use_prefix("USE secure").is_none());
    }

    #[tokio::test]
    async fn vault_read_default_admin() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = AuthContext::dev_admin();
        let result = execute_gql(
            &state,
            &auth,
            "USE secure MATCH (n:principal) RETURN n.identity AS identity",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "00000");
        assert_eq!(result.row_count, 1);
        assert!(result.data_json.as_ref().unwrap().contains("admin"));
    }

    #[tokio::test]
    async fn vault_mutation_and_read() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = AuthContext::dev_admin();

        // Insert into vault
        let result = execute_gql(
            &state,
            &auth,
            "USE secure; INSERT (:api_key {token: 'test-key-123'})",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "00000");

        // Read back
        let result = execute_gql(
            &state,
            &auth,
            "USE secure MATCH (k:api_key) RETURN k.token AS token",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "00000");
        assert_eq!(result.row_count, 1);
        assert!(result.data_json.as_ref().unwrap().contains("test-key-123"));
    }

    #[tokio::test]
    async fn vault_non_admin_denied() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = AuthContext {
            principal_node_id: selene_core::NodeId(99),
            role: crate::auth::Role::Reader,
            scope: roaring::RoaringBitmap::new(),
            scope_generation: 0,
        };
        let result = execute_gql(
            &state,
            &auth,
            "USE secure MATCH (n) RETURN n",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "42501");
        assert!(result.message.contains("Admin"));
    }

    #[tokio::test]
    async fn vault_not_configured() {
        let state = test_state().await; // no vault
        let auth = AuthContext::dev_admin();
        let result = execute_gql(
            &state,
            &auth,
            "USE secure MATCH (n) RETURN n",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "XX000");
        assert!(result.message.contains("not available"));
    }

    #[tokio::test]
    async fn vault_audit_log_created() {
        let (state, _dir) = test_state_with_vault().await;
        let auth = AuthContext::dev_admin();

        // Insert something to trigger audit
        execute_gql(
            &state,
            &auth,
            "USE secure; INSERT (:config {key: 'test'})",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();

        // Check audit log exists
        let result = execute_gql(
            &state,
            &auth,
            "USE secure MATCH (a:audit_log) RETURN count(*) AS cnt",
            None,
            false,
            false,
            ResultFormat::Json,
        )
        .unwrap();
        assert_eq!(result.status_code, "00000");
        // Should have at least 1 audit entry
        assert!(result.data_json.as_ref().unwrap().contains('1'));
    }
}
