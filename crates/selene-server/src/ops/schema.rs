//! Schema management operations.

use selene_core::schema::{EdgeSchema, NodeSchema, SchemaPack};

use super::OpError;
use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

/// List all registered node schemas.
pub fn list_node_schemas(
    state: &ServerState,
    auth: &AuthContext,
) -> Result<Vec<NodeSchema>, OpError> {
    if !state.auth_engine.authorize_action(auth, Action::EntityRead) {
        return Err(OpError::AuthDenied);
    }
    let schemas = state
        .graph
        .read(|g| g.schema().all_node_schemas().cloned().collect());
    Ok(schemas)
}

/// List all registered edge schemas.
pub fn list_edge_schemas(
    state: &ServerState,
    auth: &AuthContext,
) -> Result<Vec<EdgeSchema>, OpError> {
    if !state.auth_engine.authorize_action(auth, Action::EntityRead) {
        return Err(OpError::AuthDenied);
    }
    let schemas = state
        .graph
        .read(|g| g.schema().all_edge_schemas().cloned().collect());
    Ok(schemas)
}

/// Get a specific node schema by label.
pub fn get_node_schema(
    state: &ServerState,
    auth: &AuthContext,
    label: &str,
) -> Result<NodeSchema, OpError> {
    if !state.auth_engine.authorize_action(auth, Action::EntityRead) {
        return Err(OpError::AuthDenied);
    }
    state
        .graph
        .read(|g| g.schema().node_schema(label).cloned())
        .ok_or(OpError::NotFound {
            entity: "node_schema",
            id: 0,
        })
}

/// Get a specific edge schema by label.
pub fn get_edge_schema(
    state: &ServerState,
    auth: &AuthContext,
    label: &str,
) -> Result<EdgeSchema, OpError> {
    if !state.auth_engine.authorize_action(auth, Action::EntityRead) {
        return Err(OpError::AuthDenied);
    }
    state
        .graph
        .read(|g| g.schema().edge_schema(label).cloned())
        .ok_or(OpError::NotFound {
            entity: "edge_schema",
            id: 0,
        })
}

/// Register a node schema. Rejects if a schema with the same label already exists.
/// Use `register_node_schema_force` to overwrite.
pub fn register_node_schema(
    state: &ServerState,
    auth: &AuthContext,
    schema: NodeSchema,
) -> Result<(), OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::EntityCreate)
    {
        return Err(OpError::AuthDenied);
    }
    let label = schema.label.to_string();
    // Cycle check + registration under a single write lock
    {
        let mut guard = state.graph.inner().write();
        if let Some(ref parent) = schema.parent
            && guard.schema().has_inheritance_cycle(&schema.label, parent)
        {
            return Err(OpError::InvalidRequest(format!(
                "inheritance cycle: '{label}' → ... → '{label}'"
            )));
        }
        let is_new = guard
            .schema_mut()
            .register_node_schema_if_new(schema)
            .map_err(|e| OpError::InvalidRequest(e.to_string()))?;
        if !is_new {
            return Err(OpError::InvalidRequest(format!(
                "node schema '{label}' already exists -- use force=true to overwrite or delete it first"
            )));
        }
        guard.build_property_indexes();
        guard.build_composite_indexes();
    }
    state.graph.publish_snapshot();
    tracing::info!(label, "node schema registered");
    Ok(())
}

/// Register a node schema, replacing any existing one with the same label.
pub fn register_node_schema_force(
    state: &ServerState,
    auth: &AuthContext,
    schema: NodeSchema,
) -> Result<bool, OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::EntityCreate)
    {
        return Err(OpError::AuthDenied);
    }
    let label = schema.label.to_string();
    // Cycle check + registration under a single write lock
    let replaced = {
        let mut guard = state.graph.inner().write();
        if let Some(ref parent) = schema.parent
            && guard.schema().has_inheritance_cycle(&schema.label, parent)
        {
            return Err(OpError::InvalidRequest(format!(
                "inheritance cycle: '{label}' → ... → '{label}'"
            )));
        }
        let replaced = guard
            .schema_mut()
            .register_node_schema(schema)
            .map_err(|e| OpError::InvalidRequest(e.to_string()))?;
        guard.build_property_indexes();
        guard.build_composite_indexes();
        replaced
    };
    state.graph.publish_snapshot();
    if replaced {
        tracing::info!(label, "node schema replaced");
    } else {
        tracing::info!(label, "node schema registered");
    }
    Ok(replaced)
}

/// Register an edge schema. Rejects if already exists.
pub fn register_edge_schema(
    state: &ServerState,
    auth: &AuthContext,
    schema: EdgeSchema,
) -> Result<(), OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::EntityCreate)
    {
        return Err(OpError::AuthDenied);
    }
    let label = schema.label.to_string();
    let is_new = state
        .graph
        .inner()
        .write()
        .schema_mut()
        .register_edge_schema_if_new(schema)
        .map_err(|e| OpError::InvalidRequest(e.to_string()))?;
    if !is_new {
        return Err(OpError::InvalidRequest(format!(
            "edge schema '{label}' already exists"
        )));
    }
    state.graph.publish_snapshot();
    tracing::info!(label, "edge schema registered");
    Ok(())
}

/// Unregister a node schema by label.
pub fn unregister_node_schema(
    state: &ServerState,
    auth: &AuthContext,
    label: &str,
) -> Result<(), OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::EntityDelete)
    {
        return Err(OpError::AuthDenied);
    }
    let removed = state
        .graph
        .inner()
        .write()
        .schema_mut()
        .unregister_node_schema(label);
    if removed.is_some() {
        state.graph.publish_snapshot();
        tracing::info!(label, "node schema unregistered");
        Ok(())
    } else {
        Err(OpError::NotFound {
            entity: "node_schema",
            id: 0,
        })
    }
}

/// Unregister an edge schema by label.
pub fn unregister_edge_schema(
    state: &ServerState,
    auth: &AuthContext,
    label: &str,
) -> Result<(), OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::EntityDelete)
    {
        return Err(OpError::AuthDenied);
    }
    let removed = state
        .graph
        .inner()
        .write()
        .schema_mut()
        .unregister_edge_schema(label);
    if removed.is_some() {
        state.graph.publish_snapshot();
        tracing::info!(label, "edge schema unregistered");
        Ok(())
    } else {
        Err(OpError::NotFound {
            entity: "edge_schema",
            id: 0,
        })
    }
}

/// Import a schema pack (registers all node and edge schemas).
///
/// Skips schemas that already exist (no overwrite). Returns how many
/// were actually registered vs skipped.
pub fn import_pack(
    state: &ServerState,
    auth: &AuthContext,
    pack: SchemaPack,
) -> Result<ImportResult, OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::EntityCreate)
    {
        return Err(OpError::AuthDenied);
    }
    let pack_name = pack.name.clone();
    let mut nodes_registered = 0usize;
    let mut nodes_skipped = 0usize;
    let mut edges_registered = 0usize;
    let mut edges_skipped = 0usize;

    {
        let mut graph = state.graph.inner().write();
        let schema = graph.schema_mut();
        for ns in pack.nodes {
            // Check for inheritance cycles before registering.
            if let Some(ref parent) = ns.parent
                && schema.has_inheritance_cycle(&ns.label, parent)
            {
                tracing::warn!(
                    label = &*ns.label,
                    parent = &**parent,
                    "skipped: inheritance cycle"
                );
                nodes_skipped += 1;
                continue;
            }
            match schema.register_node_schema_if_new(ns) {
                Ok(true) => nodes_registered += 1,
                Ok(false) => nodes_skipped += 1,
                Err(e) => {
                    tracing::warn!(error = %e, "schema compat error during pack import, skipping");
                    nodes_skipped += 1;
                }
            }
        }
        for es in pack.edges {
            match schema.register_edge_schema_if_new(es) {
                Ok(true) => edges_registered += 1,
                Ok(false) => edges_skipped += 1,
                Err(e) => {
                    tracing::warn!(error = %e, "schema compat error during pack import, skipping");
                    edges_skipped += 1;
                }
            }
        }
        if nodes_registered > 0 {
            graph.build_property_indexes();
            graph.build_composite_indexes();
        }
    } // write lock dropped
    state.graph.publish_snapshot();

    tracing::info!(
        pack = pack_name,
        nodes_registered,
        nodes_skipped,
        edges_registered,
        edges_skipped,
        "schema pack imported"
    );

    Ok(ImportResult {
        pack_name,
        node_schemas_registered: nodes_registered,
        node_schemas_skipped: nodes_skipped,
        edge_schemas_registered: edges_registered,
        edge_schemas_skipped: edges_skipped,
    })
}

/// Result of a schema pack import.
pub struct ImportResult {
    pub pack_name: String,
    pub node_schemas_registered: usize,
    pub node_schemas_skipped: usize,
    pub edge_schemas_registered: usize,
    pub edge_schemas_skipped: usize,
}
