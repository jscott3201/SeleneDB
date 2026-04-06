//! DDL statement classification and execution (admin-only, returns message strings).

use super::{GqlQueryResult, error_result};
use crate::bootstrap::ServerState;
use crate::ops::OpError;

/// Check if a statement is a DDL handled by execute_ddl (admin-only, returns message strings).
pub(super) fn is_ddl_statement(stmt: &selene_gql::GqlStatement) -> bool {
    use selene_gql::GqlStatement;
    matches!(
        stmt,
        GqlStatement::CreateGraph { .. }
            | GqlStatement::DropGraph { .. }
            | GqlStatement::CreateIndex { .. }
            | GqlStatement::DropIndex { .. }
            | GqlStatement::CreateUser { .. }
            | GqlStatement::DropUser { .. }
            | GqlStatement::CreateRole { .. }
            | GqlStatement::DropRole { .. }
            | GqlStatement::GrantRole { .. }
            | GqlStatement::RevokeRole { .. }
            | GqlStatement::CreateProcedure { .. }
            | GqlStatement::DropProcedure { .. }
    )
}

/// Sync ViewStateStore after a materialized view CREATE or DROP succeeds.
///
/// The executor registers/removes the definition in ViewRegistry (inside
/// SeleneGraph), but aggregate state lives in the server-layer ViewStateStore.
/// This hook bridges the two layers.
pub(super) fn sync_view_state_after_ddl(state: &ServerState, stmt: &selene_gql::GqlStatement) {
    use selene_gql::GqlStatement;
    let Some(svc) = state.services.get::<crate::view_state::ViewStateService>() else {
        return;
    };
    match stmt {
        GqlStatement::CreateMaterializedView { name, .. } => {
            let upper = name.as_str().to_uppercase();
            state.graph.read(|g| {
                if let Some(def) = g.view_registry().get(&upper) {
                    svc.store.register_view(def, g);
                    tracing::debug!(view = %upper, "materialized view state initialized");
                }
            });
        }
        GqlStatement::DropMaterializedView { name, .. } => {
            let upper = name.as_str().to_uppercase();
            svc.store.remove_view(&upper);
            tracing::debug!(view = %upper, "materialized view state removed");
        }
        _ => {}
    }
}

/// Check if a statement is graph-state DDL requiring SharedGraph write access (admin-only).
pub(super) fn is_graph_state_ddl(stmt: &selene_gql::GqlStatement) -> bool {
    use selene_gql::GqlStatement;
    matches!(
        stmt,
        GqlStatement::CreateTrigger(_)
            | GqlStatement::DropTrigger(_)
            | GqlStatement::ShowTriggers
            | GqlStatement::CreateNodeType { .. }
            | GqlStatement::DropNodeType { .. }
            | GqlStatement::ShowNodeTypes
            | GqlStatement::CreateEdgeType { .. }
            | GqlStatement::DropEdgeType { .. }
            | GqlStatement::ShowEdgeTypes
            | GqlStatement::CreateMaterializedView { .. }
            | GqlStatement::DropMaterializedView { .. }
            | GqlStatement::ShowMaterializedViews
    )
}

pub(super) fn execute_ddl(
    state: &ServerState,
    stmt: &selene_gql::GqlStatement,
) -> Result<GqlQueryResult, OpError> {
    use selene_gql::GqlStatement;
    let message = match stmt {
        GqlStatement::CreateGraph {
            name,
            if_not_exists,
            or_replace,
        } => {
            let cat_svc = state
                .services
                .get::<crate::service_registry::GraphCatalogService>()
                .ok_or_else(|| OpError::Internal("GraphCatalogService not registered".into()))?;
            let mut catalog = cat_svc.catalog.lock();
            if *or_replace {
                catalog
                    .create_or_replace_graph(name)
                    .map_err(|e| OpError::QueryError(e.to_string()))?;
                format!("Graph '{name}' created (or replaced)")
            } else if *if_not_exists {
                if catalog.get_graph(name).is_none() {
                    catalog
                        .create_graph(name)
                        .map_err(|e| OpError::QueryError(e.to_string()))?;
                }
                format!("Graph '{name}' created")
            } else {
                catalog
                    .create_graph(name)
                    .map_err(|e| OpError::QueryError(e.to_string()))?;
                format!("Graph '{name}' created")
            }
        }
        GqlStatement::DropGraph { name, if_exists } => {
            let cat_svc = state
                .services
                .get::<crate::service_registry::GraphCatalogService>()
                .ok_or_else(|| OpError::Internal("GraphCatalogService not registered".into()))?;
            let mut catalog = cat_svc.catalog.lock();
            if *if_exists {
                catalog.drop_graph_if_exists(name);
                format!("Graph '{name}' dropped")
            } else {
                catalog
                    .drop_graph(name)
                    .map_err(|e| OpError::QueryError(e.to_string()))?;
                format!("Graph '{name}' dropped")
            }
        }
        GqlStatement::CreateIndex {
            name,
            label,
            properties,
            ..
        } => {
            let props_str = properties.join(", ");
            tracing::warn!(index = name, label = label, properties = %props_str, "CREATE INDEX parsed but not yet persisted — index is parse-only stub");
            format!(
                "Index '{name}' created on :{label}({props_str}) (parse-only — not yet persisted)"
            )
        }
        GqlStatement::DropIndex { name, .. } => {
            tracing::warn!(index = name, "DROP INDEX parsed but not yet persisted");
            format!("Index '{name}' dropped (parse-only)")
        }
        GqlStatement::CreateUser { username, role, .. } => {
            // Never log the password
            let role_str = role.as_deref().unwrap_or("Reader");
            tracing::warn!(
                username = username,
                role = role_str,
                "CREATE USER parsed but not yet persisted — requires vault"
            );
            format!(
                "User '{username}' created with role '{role_str}' (parse-only — requires vault)"
            )
        }
        GqlStatement::DropUser { username, .. } => {
            tracing::warn!(
                username = username,
                "DROP USER parsed but not yet persisted"
            );
            format!("User '{username}' dropped (parse-only)")
        }
        GqlStatement::CreateRole { name, .. } => {
            tracing::warn!(
                role = name,
                "CREATE ROLE parsed but not yet persisted — requires vault"
            );
            format!("Role '{name}' created (parse-only — requires vault)")
        }
        GqlStatement::DropRole { name, .. } => {
            tracing::warn!(role = name, "DROP ROLE parsed but not yet persisted");
            format!("Role '{name}' dropped (parse-only)")
        }
        GqlStatement::GrantRole { role, username } => {
            tracing::warn!(
                role = role,
                username = username,
                "GRANT ROLE parsed but not yet persisted"
            );
            format!("Role '{role}' granted to '{username}' (parse-only)")
        }
        GqlStatement::RevokeRole { role, username } => {
            tracing::warn!(
                role = role,
                username = username,
                "REVOKE ROLE parsed but not yet persisted"
            );
            format!("Role '{role}' revoked from '{username}' (parse-only)")
        }
        GqlStatement::CreateProcedure { name, .. } => {
            tracing::warn!(
                procedure = name,
                "CREATE PROCEDURE parsed but not yet persisted"
            );
            format!("Procedure '{name}' created (parse-only)")
        }
        GqlStatement::DropProcedure { name, .. } => {
            tracing::warn!(
                procedure = name,
                "DROP PROCEDURE parsed but not yet persisted"
            );
            format!("Procedure '{name}' dropped (parse-only)")
        }
        _ => return Ok(error_result("XX000", "unknown DDL statement")),
    };

    Ok(GqlQueryResult {
        status_code: "0A000".to_string(),
        message: format!("{message} -- feature not yet implemented"),
        data_json: Some("[]".to_string()),
        data_arrow: None,
        row_count: 0,
        mutations: None,
        plan: None,
    })
}
