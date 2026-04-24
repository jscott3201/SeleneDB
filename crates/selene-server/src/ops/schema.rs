//! Schema management operations.

use selene_core::schema::{EdgeSchema, NodeSchema, SchemaPack};

use super::OpError;
use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

/// Persist a schema mutation through the WAL.
///
/// Since 1.3.0, schema mutations are first-class `Change::SchemaMutation`
/// records that flow through the same WAL coalescer as node and edge
/// mutations. `persist_or_die` blocks until the WAL append is confirmed,
/// so when this function returns the mutation is durable — no more full
/// `take_snapshot` per schema write, and no more `schema_persist_pending`
/// flag to track partial failures. The function is infallible for the
/// same reason `persist_or_die` is: a WAL append failure aborts the
/// process rather than being surfaced to the caller. Recovery replays
/// these records via `change_applier::apply_schema_mutation`.
fn persist_schema_change(state: &ServerState, op: selene_core::changeset::SchemaMutation) {
    super::persist_or_die(state, &[selene_core::changeset::Change::SchemaMutation(op)]);
}

/// Outcome of an idempotent schema registration. Lets callers distinguish
/// a fresh create from a no-op on an already-equal schema, and from a
/// conflict (same label, different shape) without throwing — the last case
/// is still a soft error but structured enough for an agent to recover from.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaRegisterOutcome {
    /// Schema did not exist; has been registered.
    Created,
    /// Schema already exists and is byte-equal to the proposed one.
    AlreadyExistsEqual,
}

/// Apply the same inheritance resolution that
/// [`selene_graph::SchemaValidator::register_node_schema`] performs at
/// registration time: prepend parent properties (child wins on name
/// collisions) and inherit `valid_edge_labels` when the child leaves
/// them empty. This lets the idempotent-create equality check compare
/// the proposal against the stored schema on equal footing — without
/// this, any proposal with a `parent` would look different from what's
/// stored and trigger a spurious "different shape" conflict.
fn resolve_node_schema_inheritance(
    schema_reader: &selene_graph::SchemaValidator,
    schema: &NodeSchema,
) -> NodeSchema {
    let mut resolved = schema.clone();
    if let Some(ref parent_label) = resolved.parent
        && let Some(parent) = schema_reader.node_schema(parent_label)
    {
        let child_names: std::collections::HashSet<&str> = resolved
            .properties
            .iter()
            .map(|p| p.name.as_ref())
            .collect();
        let mut merged: Vec<_> = parent
            .properties
            .iter()
            .filter(|p| !child_names.contains(p.name.as_ref()))
            .cloned()
            .collect();
        merged.append(&mut resolved.properties);
        resolved.properties = merged;
        if resolved.valid_edge_labels.is_empty() {
            resolved.valid_edge_labels = parent.valid_edge_labels.clone();
        }
    }
    resolved
}

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

/// Register a node schema idempotently.
///
/// Agent tool flows frequently pre-call `create_schema` as a defensive
/// step before writing nodes; rejecting that call with an error teaches
/// models to treat a benign pre-flight as a failure and retry with
/// increasingly wrong args. This routine collapses the benign case into
/// [`SchemaRegisterOutcome::AlreadyExistsEqual`] when the proposed shape
/// is byte-identical to the registered one. Genuinely conflicting
/// proposals still return [`OpError::InvalidRequest`], pointing the
/// caller at `update_schema` for explicit overwrites.
///
/// Use `register_node_schema_force` when you want unconditional
/// overwrite (no equality check).
pub fn register_node_schema(
    state: &ServerState,
    auth: &AuthContext,
    schema: NodeSchema,
) -> Result<SchemaRegisterOutcome, OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::EntityCreate)
    {
        return Err(OpError::AuthDenied);
    }
    let label = schema.label.to_string();
    let schema_for_wal = schema.clone();
    let outcome = {
        let mut guard = state.graph.inner().write();
        if let Some(ref parent) = schema.parent
            && guard.schema().has_inheritance_cycle(&schema.label, parent)
        {
            return Err(OpError::InvalidRequest(format!(
                "inheritance cycle: '{label}' → ... → '{label}'"
            )));
        }
        // Structural-equality check against any existing schema with the
        // same label. Resolve inheritance on the proposal first so the
        // comparison matches like-for-like — the stored schema has had
        // parent properties merged in at registration time, so a raw
        // proposal with a `parent` field would otherwise look different
        // from its own registered form.
        if let Some(existing) = guard.schema().node_schema(&schema.label) {
            let resolved_proposal = resolve_node_schema_inheritance(guard.schema(), &schema);
            let a = serde_json::to_value(existing).map_err(|e| {
                OpError::Internal(format!(
                    "failed to serialize existing node schema '{label}' for comparison: {e}"
                ))
            })?;
            let b = serde_json::to_value(&resolved_proposal).map_err(|e| {
                OpError::Internal(format!(
                    "failed to serialize proposed node schema '{label}' for comparison: {e}"
                ))
            })?;
            if a == b {
                tracing::debug!(label, "create_schema no-op: proposed shape equals existing");
                return Ok(SchemaRegisterOutcome::AlreadyExistsEqual);
            }
            return Err(OpError::InvalidRequest(format!(
                "node schema '{label}' already exists with a different shape -- \
                 call update_schema to change it, or delete_schema first"
            )));
        }
        let is_new = guard
            .schema_mut()
            .register_node_schema_if_new(schema)
            .map_err(|e| OpError::InvalidRequest(e.to_string()))?;
        debug_assert!(
            is_new,
            "schema_mut reported duplicate after equality branch"
        );
        guard.build_property_indexes();
        guard.build_composite_indexes();
        SchemaRegisterOutcome::Created
    };
    state.graph.publish_snapshot();
    if matches!(outcome, SchemaRegisterOutcome::Created) {
        persist_schema_change(
            state,
            selene_core::changeset::SchemaMutation::RegisterNode(Box::new(schema_for_wal)),
        );
    }
    tracing::info!(label, "node schema registered");
    Ok(outcome)
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
    let schema_for_wal = schema.clone();
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
    persist_schema_change(
        state,
        selene_core::changeset::SchemaMutation::RegisterNodeForce(Box::new(schema_for_wal)),
    );
    if replaced {
        tracing::info!(label, "node schema replaced");
    } else {
        tracing::info!(label, "node schema registered");
    }
    Ok(replaced)
}

/// Register an edge schema idempotently. Mirrors the policy in
/// [`register_node_schema`]: byte-equal duplicates return
/// [`SchemaRegisterOutcome::AlreadyExistsEqual`], a different-shape
/// duplicate errors and points at `update_schema`.
pub fn register_edge_schema(
    state: &ServerState,
    auth: &AuthContext,
    schema: EdgeSchema,
) -> Result<SchemaRegisterOutcome, OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::EntityCreate)
    {
        return Err(OpError::AuthDenied);
    }
    let label = schema.label.to_string();
    let schema_for_wal = schema.clone();
    let outcome = {
        let mut guard = state.graph.inner().write();
        if let Some(existing) = guard.schema().edge_schema(&schema.label) {
            // Edge schemas don't inherit, so no resolution step needed.
            let a = serde_json::to_value(existing).map_err(|e| {
                OpError::Internal(format!(
                    "failed to serialize existing edge schema '{label}' for comparison: {e}"
                ))
            })?;
            let b = serde_json::to_value(&schema).map_err(|e| {
                OpError::Internal(format!(
                    "failed to serialize proposed edge schema '{label}' for comparison: {e}"
                ))
            })?;
            if a == b {
                tracing::debug!(
                    label,
                    "create_schema no-op: edge proposed shape equals existing"
                );
                return Ok(SchemaRegisterOutcome::AlreadyExistsEqual);
            }
            // There is no `update_edge_schema` MCP tool; point operators
            // at the delete + recreate path instead of a dead-end hint.
            return Err(OpError::InvalidRequest(format!(
                "edge schema '{label}' already exists with a different shape -- \
                 use delete_edge_schema and create_edge_schema to replace it"
            )));
        }
        let is_new = guard
            .schema_mut()
            .register_edge_schema_if_new(schema)
            .map_err(|e| OpError::InvalidRequest(e.to_string()))?;
        debug_assert!(
            is_new,
            "schema_mut reported duplicate after equality branch"
        );
        SchemaRegisterOutcome::Created
    };
    state.graph.publish_snapshot();
    if matches!(outcome, SchemaRegisterOutcome::Created) {
        persist_schema_change(
            state,
            selene_core::changeset::SchemaMutation::RegisterEdge(Box::new(schema_for_wal)),
        );
    }
    tracing::info!(label, "edge schema registered");
    Ok(outcome)
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
        persist_schema_change(
            state,
            selene_core::changeset::SchemaMutation::UnregisterNode(selene_core::IStr::new(label)),
        );
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
        persist_schema_change(
            state,
            selene_core::changeset::SchemaMutation::UnregisterEdge(selene_core::IStr::new(label)),
        );
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
    // Accumulate per-successful-register WAL records so we can persist
    // them after dropping the write lock. Each entry matches exactly
    // one schema that was added by this call (pre-existing equal ones
    // produce no record).
    let mut wal_records: Vec<selene_core::changeset::SchemaMutation> = Vec::new();

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
            let ns_for_wal = ns.clone();
            match schema.register_node_schema_if_new(ns) {
                Ok(true) => {
                    nodes_registered += 1;
                    wal_records.push(selene_core::changeset::SchemaMutation::RegisterNode(
                        Box::new(ns_for_wal),
                    ));
                }
                Ok(false) => nodes_skipped += 1,
                Err(e) => {
                    tracing::warn!(error = %e, "schema compat error during pack import, skipping");
                    nodes_skipped += 1;
                }
            }
        }
        for es in pack.edges {
            let es_for_wal = es.clone();
            match schema.register_edge_schema_if_new(es) {
                Ok(true) => {
                    edges_registered += 1;
                    wal_records.push(selene_core::changeset::SchemaMutation::RegisterEdge(
                        Box::new(es_for_wal),
                    ));
                }
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
    if !wal_records.is_empty() {
        // persist_or_die blocks on WAL append; the pack is durable on
        // return. Bundle all records into one changeset for efficiency.
        let changes: Vec<selene_core::changeset::Change> = wal_records
            .into_iter()
            .map(selene_core::changeset::Change::SchemaMutation)
            .collect();
        super::persist_or_die(state, &changes);
    }

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
