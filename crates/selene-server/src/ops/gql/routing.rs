//! Graph routing: USE prefix parsing, vault/local/remote query dispatch.

use std::time::Instant;

use super::{GqlQueryResult, ResultFormat, audit_log, error_result, format_result, map_gql_error};
use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use crate::ops::OpError;

/// Parse a `USE <graph>;` prefix from a query, returning `(graph_name, remaining_query)`.
/// Returns `None` if the query does not start with `USE`.
///
/// Examples:
/// - `"USE secure; MATCH ..."` -> `Some(("secure", "MATCH ..."))`
/// - `"USE building_a MATCH ..."` -> `Some(("building_a", "MATCH ..."))`
/// - `"MATCH ..."` -> `None`
pub(super) fn parse_use_prefix(query: &str) -> Option<(&str, &str)> {
    let trimmed = query.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.len() < 5 || !bytes[..3].eq_ignore_ascii_case(b"USE") || bytes[3] != b' ' {
        return None;
    }
    let rest = trimmed[4..].trim_start();
    // Graph name ends at semicolon or whitespace
    let name_end = rest.find(|c: char| c == ';' || c.is_whitespace())?;
    let name = &rest[..name_end];
    if name.is_empty() {
        return None;
    }
    let remainder = rest[name_end..].trim_start();
    let remainder = remainder
        .strip_prefix(';')
        .unwrap_or(remainder)
        .trim_start();
    if remainder.is_empty() {
        return None; // USE without a following query
    }
    Some((name, remainder))
}

/// Execute a query against the vault graph.
pub(super) fn execute_vault_query(
    state: &ServerState,
    auth: &AuthContext,
    query: &str,
    format: ResultFormat,
    start: Instant,
    deadline_ms: u64,
) -> GqlQueryResult {
    // Admin-only access
    if !auth.is_admin() {
        audit_log(auth, query, "vault_denied", 0, start.elapsed());
        return error_result("42501", "vault access requires Admin role");
    }

    let Some(vault_svc) = state.services.get::<crate::vault::VaultService>() else {
        return error_result(
            "XX000",
            "secure vault not available — enable vault in config",
        );
    };
    let vault = &vault_svc.handle;

    // Parse the vault query
    let stmt = match selene_gql::parse_statement(query) {
        Ok(s) => s,
        Err(e) => {
            audit_log(auth, query, "vault_parse_error", 0, start.elapsed());
            return map_gql_error(&e);
        }
    };

    let is_mutation = matches!(
        stmt,
        selene_gql::GqlStatement::Mutate(_)
            | selene_gql::GqlStatement::StartTransaction
            | selene_gql::GqlStatement::Commit
            | selene_gql::GqlStatement::Rollback
    );

    // Timeout check
    if start.elapsed().as_millis() as u64 > deadline_ms {
        return error_result("57014", "query cancelled: timeout");
    }

    // Execute against vault graph
    let result = if is_mutation {
        // Mutation: execute via SharedGraph::write
        let gql_result = selene_gql::MutationBuilder::new(query).execute(&vault.graph);

        if gql_result.is_ok() {
            // Write audit log before flush so it's included in the encrypted payload
            let audit_details = if query.len() > 200 {
                &query[..query.floor_char_boundary(200)]
            } else {
                query
            };
            crate::vault::audit::log_audit(
                &vault.graph,
                &format!("{}", auth.principal_node_id.0),
                "gql_mutate",
                audit_details,
            );

            // Flush vault to disk
            {
                if let Err(e) = vault.flush(&vault_svc.master_key) {
                    tracing::error!("vault flush failed: {e}");
                    return error_result("XX000", &format!("vault flush failed: {e}"));
                }
            }
        }

        gql_result
    } else {
        // Read: snapshot-based query
        let snapshot = vault.graph.load_snapshot();
        let registry = selene_gql::runtime::procedures::ProcedureRegistry::with_builtins();
        selene_gql::QueryBuilder::from_statement(&stmt, &snapshot)
            .with_procedures(&registry)
            .execute()
    };

    let elapsed = start.elapsed();
    audit_log(auth, query, "vault_ok", 0, elapsed);

    match result {
        Ok(gql_result) => format_result(gql_result, format),
        Err(ref gql_err) => map_gql_error(gql_err),
    }
}

/// Execute a query against a local named graph (from GraphCatalog).
pub(super) fn execute_local_graph_query(
    state: &ServerState,
    auth: &AuthContext,
    query: &str,
    graph: &selene_graph::SharedGraph,
    format: ResultFormat,
    start: Instant,
    deadline_ms: u64,
) -> GqlQueryResult {
    let stmt = match selene_gql::parse_statement(query) {
        Ok(s) => s,
        Err(e) => {
            audit_log(auth, query, "local_graph_parse_error", 0, start.elapsed());
            return map_gql_error(&e);
        }
    };

    // DDL requires admin -- same check as the default graph path
    if super::ddl::is_ddl_statement(&stmt) {
        if !auth.is_admin() {
            audit_log(auth, query, "local_graph_ddl_denied", 0, start.elapsed());
            return error_result("42501", "DDL requires Admin role");
        }
        // DDL on named graphs not supported -- schemas belong to the default graph
        return error_result("0A000", "DDL not supported on named graphs");
    }

    let is_mutation = matches!(
        stmt,
        selene_gql::GqlStatement::Mutate(_)
            | selene_gql::GqlStatement::StartTransaction
            | selene_gql::GqlStatement::Commit
            | selene_gql::GqlStatement::Rollback
    );

    // Auth check -- same as default graph path
    let action = if is_mutation {
        Action::GqlMutate
    } else {
        Action::GqlQuery
    };
    if !auth.is_admin() && !state.auth_engine.authorize_action(auth, action) {
        audit_log(auth, query, "local_graph_auth_denied", 0, start.elapsed());
        return error_result("42501", "insufficient privilege");
    }

    // Reserved-label reservation: named local graphs share mutation semantics
    // with the default graph, so the same reservation applies here. Vault
    // access routes through `execute_vault_query` and is exempt.
    if let selene_gql::GqlStatement::Mutate(ref pipeline) = stmt
        && let Err(e) = crate::auth::reserved::reject_reserved_in_mutation(pipeline)
    {
        audit_log(
            auth,
            query,
            "local_graph_reserved_label",
            0,
            start.elapsed(),
        );
        return error_result("42501", &e.to_string());
    }

    if start.elapsed().as_millis() as u64 > deadline_ms {
        return error_result("57014", "query cancelled: timeout");
    }

    let result = if is_mutation {
        // Mutations go through the SharedGraph write path
        let scope = if auth.is_admin() {
            None
        } else {
            Some(&auth.scope)
        };
        {
            let mut mb = selene_gql::MutationBuilder::new(query);
            if let Some(s) = scope {
                mb = mb.with_scope(s);
            }
            mb.execute(graph)
        }
    } else {
        let snapshot = graph.load_snapshot();
        let scope = if auth.is_admin() {
            None
        } else {
            Some(&auth.scope)
        };
        let registry = selene_gql::runtime::procedures::ProcedureRegistry::with_builtins();
        let mut qb =
            selene_gql::QueryBuilder::from_statement(&stmt, &snapshot).with_procedures(&registry);
        if let Some(s) = scope {
            qb = qb.with_scope(s);
        }
        qb.execute()
    };

    let elapsed = start.elapsed();
    audit_log(auth, query, "local_graph_ok", 0, elapsed);

    match result {
        Ok(gql_result) => format_result(gql_result, format),
        Err(ref gql_err) => map_gql_error(gql_err),
    }
}

/// Execute a GQL query on a remote peer via federation forwarding.
pub(super) fn execute_remote_query(
    state: &ServerState,
    auth: &AuthContext,
    query: &str,
    peer_name: &str,
    format: ResultFormat,
    start: Instant,
    _deadline_ms: u64,
) -> Result<GqlQueryResult, OpError> {
    let fed_svc = state
        .services
        .get::<crate::federation::FederationService>()
        .ok_or_else(|| OpError::Internal("federation not enabled".into()))?;
    let manager = &fed_svc.manager;

    // Get existing connection (don't block the sync context with async connect)
    let client = manager
        .get_connection(peer_name)
        .ok_or_else(|| OpError::Internal(format!("peer '{peer_name}' not connected")))?;

    let json_format = matches!(format, ResultFormat::Json);
    let forwarded_scope = {
        let mut buf = Vec::new();
        auth.scope.serialize_into(&mut buf).ok();
        Some(buf)
    };

    let req = selene_wire::dto::federation::FederationGqlRequest {
        query: query.to_string(),
        json_format,
        forwarded_scope,
    };

    // Block on the async call -- we're already in a sync ops context
    let resp: selene_wire::dto::federation::FederationGqlResponse =
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { client.federation_gql(req).await })
        })
        .map_err(|e| OpError::Internal(format!("federation query to {peer_name}: {e}")))?;

    let elapsed = start.elapsed();
    audit_log(
        auth,
        query,
        &format!("remote:{peer_name}"),
        resp.row_count as usize,
        elapsed,
    );

    if let Some(err) = resp.error {
        return Ok(error_result(&err.code, &err.message));
    }

    Ok(GqlQueryResult {
        status_code: resp.status_code,
        message: resp.message,
        data_json: resp.json_result,
        data_arrow: resp.ipc_bytes,
        row_count: resp.row_count,
        mutations: None,
        plan: None,
    })
}
