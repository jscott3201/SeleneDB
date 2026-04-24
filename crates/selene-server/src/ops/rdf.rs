//! RDF/SPARQL ops — authz + scope + durability for the RDF surfaces.
//!
//! Pre-1.3.0, the HTTP handlers for `/graph/rdf` and `/sparql` accepted an
//! `HttpAuth` extractor and then discarded it with `let _ = auth.0`. Every
//! authenticated principal could read the whole graph via RDF export or
//! SPARQL query, and SPARQL Update could mutate through any principal
//! regardless of scope or role and bypassed `persist_or_die` entirely
//! (see Selene_Bug_v1 findings #3, #4, #5).
//!
//! This module re-routes the RDF/SPARQL surfaces through the same
//! authorization + scope + persistence model as the CRUD and GQL paths:
//!
//! - Reads (`rdf_export`, `sparql_query`) require [`Action::EntityRead`].
//!   Non-admin principals see a scope-filtered view of the graph:
//!   `selene_rdf::{graph_to_quads_scoped, execute_sparql_scoped}` drop any
//!   quad whose subject or object references an out-of-scope node before
//!   the serializer or SPARQL evaluator sees it. Ontology-graph quads
//!   (schema/types) are shared metadata and are not filtered.
//!
//! - Writes (`rdf_import`, `sparql_update`) require
//!   [`Action::GqlMutate`] **and** admin role. Scoped import and
//!   scoped SPARQL Update require triple-level mutation scoping that is
//!   out of scope for 1.3.0; narrowing to admin-only closes the
//!   immediate escalation surface without shipping an incomplete scoped
//!   design. Both writes route through the mutation batcher, reject on
//!   replica, and emit their full changeset through [`persist_or_die`]
//!   so WAL, changelog, version store, and subscribers see the
//!   mutations.

use std::sync::Arc;

use selene_core::changeset::Change;
use selene_graph::SharedGraph;

use super::{OpError, persist_or_die};
use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

/// Summary result of a SPARQL Update execution (mirrors
/// `selene_rdf::update::UpdateResult`).
#[derive(Debug, Default, serde::Serialize)]
pub struct SparqlUpdateResult {
    pub nodes_created: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub edges_created: usize,
    pub edges_deleted: usize,
}

/// Summary result of an RDF import (mirrors `selene_rdf::import::ImportResult`).
#[derive(Debug, Default, serde::Serialize)]
pub struct RdfImportResult {
    pub nodes_created: usize,
    pub edges_created: usize,
    pub labels_added: usize,
    pub properties_set: usize,
    pub ontology_triples_loaded: usize,
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn require_action(state: &ServerState, auth: &AuthContext, action: Action) -> Result<(), OpError> {
    if !state.auth_engine.authorize_action(auth, action) {
        return Err(OpError::AuthDenied);
    }
    Ok(())
}

fn reject_if_replica(state: &ServerState) -> Result<(), OpError> {
    if state.replica.is_replica {
        Err(OpError::ReadOnly)
    } else {
        Ok(())
    }
}

/// Translate an `RdfError` into an `OpError` with the right classification.
///
/// Client-caused failures (syntax, oversize body, unknown format / namespace)
/// surface as 4xx via `InvalidRequest` / `ResourcesExhausted`; only genuinely
/// unexpected failures flow through `Internal` (HTTP 500). The previous path
/// bucketed everything through `GraphError::Other` + `graph_err` which
/// turned every error into 500, hiding client-side problems.
fn map_rdf_error(e: selene_rdf::RdfError) -> OpError {
    use selene_rdf::RdfError;
    match e {
        RdfError::Parse(msg) => OpError::InvalidRequest(format!("RDF parse error: {msg}")),
        RdfError::Namespace(msg) => OpError::InvalidRequest(format!("RDF namespace: {msg}")),
        RdfError::Unsupported(msg) => OpError::InvalidRequest(format!("RDF unsupported: {msg}")),
        RdfError::TooManyQuads(limit) => {
            OpError::ResourcesExhausted(format!("RDF import exceeds limit of {limit} quads"))
        }
        RdfError::Graph(ge) => super::graph_err(ge),
        RdfError::Serialize(msg) => OpError::Internal(format!("RDF serialize: {msg}")),
    }
}

/// Translate a SPARQL Update `UpdateError` into an `OpError` with the
/// appropriate classification. Parse / unsupported / invalid-quad errors are
/// client-caused; `NodeNotFound` is a 404; `ScopeDenied` is 403; `Graph`
/// falls through the shared graph-error mapper.
fn map_sparql_update_error(e: selene_rdf::update::UpdateError) -> OpError {
    use selene_rdf::update::UpdateError;
    match e {
        UpdateError::Parse(msg) => OpError::QueryError(format!("SPARQL parse error: {msg}")),
        UpdateError::Unsupported(msg) => {
            OpError::InvalidRequest(format!("SPARQL unsupported: {msg}"))
        }
        UpdateError::InvalidQuad(msg) => {
            OpError::InvalidRequest(format!("SPARQL invalid quad: {msg}"))
        }
        UpdateError::NodeNotFound(id) => OpError::NotFound { entity: "node", id },
        UpdateError::ScopeDenied(msg) => OpError::Forbidden(msg),
        UpdateError::Graph(ge) => super::graph_err(ge),
    }
}

/// RDF import is admin-only in 1.3.0. Scoped import would require the
/// caller to bind each new node under an explicit in-scope parent; the
/// RDF-import format does not express that binding, so the library
/// rejects node creation for scoped principals and the majority of real
/// imports would fail. Until a parent-binding mechanism is added, the
/// ops layer short-circuits and returns `Forbidden` instead.
fn require_admin_for_rdf_import(auth: &AuthContext) -> Result<(), OpError> {
    if !auth.is_admin() {
        return Err(OpError::Forbidden(
            "RDF import requires admin role in 1.3.0 — scoped import is tracked as \
             follow-up work (needs a parent-in-scope binding per imported subject)"
                .into(),
        ));
    }
    Ok(())
}

// ── Read ops ────────────────────────────────────────────────────────────

/// Export the property graph as RDF, filtered to the caller's scope.
///
/// Admins see the full graph. Non-admins see only nodes in their scope
/// bitmap plus edges whose endpoints are both in scope. Ontology triples
/// (schema metadata in the `urn:selene:ontology` named graph) are
/// unfiltered — they describe types, not instance data.
pub fn rdf_export(
    state: &ServerState,
    auth: &AuthContext,
    format: selene_rdf::RdfFormat,
    include_all_graphs: bool,
) -> Result<Vec<u8>, OpError> {
    require_action(state, auth, Action::EntityRead)?;

    let auth = super::refresh_scope_if_stale(state, auth);
    let ns = &state.rdf_namespace;
    let snap = state.graph.load_snapshot();
    let ontology_ref = state.rdf_ontology.as_ref().map(|o| o.read());

    let scope = if auth.is_admin() {
        None
    } else {
        Some(&auth.scope)
    };

    selene_rdf::export::export_graph_scoped(
        &snap,
        ns,
        format,
        ontology_ref.as_deref(),
        include_all_graphs,
        scope,
    )
    .map_err(|e| OpError::Internal(format!("RDF export: {e}")))
}

/// Execute a SPARQL SELECT / ASK / CONSTRUCT / DESCRIBE query, filtered
/// to the caller's scope. Returns (serialized bytes, content type).
pub fn sparql_query(
    state: &ServerState,
    auth: &AuthContext,
    query_str: &str,
    format: selene_rdf::sparql::SparqlResultFormat,
) -> Result<(Vec<u8>, &'static str), OpError> {
    require_action(state, auth, Action::EntityRead)?;

    let auth = super::refresh_scope_if_stale(state, auth);
    let ns = &state.rdf_namespace;
    let snap = state.graph.load_snapshot();
    let csr = crate::bootstrap::get_or_build_csr(&state.csr_cache, &snap);
    let ontology_ref = state.rdf_ontology.as_ref().map(|o| o.read());

    let scope = if auth.is_admin() {
        None
    } else {
        Some(&auth.scope)
    };

    selene_rdf::sparql::execute_sparql_scoped(
        &snap,
        &csr,
        ns,
        ontology_ref.as_deref(),
        query_str,
        format,
        scope,
    )
    .map_err(|e| OpError::QueryError(e.to_string()))
}

// ── Write ops ───────────────────────────────────────────────────────────

/// Import RDF data into the main graph. Admin-only in 1.3.0; routes through
/// the mutation batcher and emits the resulting changeset via
/// `persist_or_die` so WAL, changelog, and version store stay consistent
/// with every other mutation surface.
pub async fn rdf_import(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    format: selene_rdf::RdfFormat,
    target_graph: Option<String>,
    body: axum::body::Bytes,
) -> Result<RdfImportResult, OpError> {
    reject_if_replica(state)?;
    require_action(state, auth, Action::GqlMutate)?;
    require_admin_for_rdf_import(auth)?;

    let ontology_arc = state
        .rdf_ontology
        .as_ref()
        .ok_or_else(|| OpError::Internal("RDF ontology store not initialized".into()))?
        .clone();

    let ns = state.rdf_namespace.clone();
    let graph: SharedGraph = state.graph.clone();
    let st = Arc::clone(state);

    // The mutation batcher serializes all writes. import_rdf_with_changes
    // drives its own SharedGraph::write and returns the property-graph
    // changeset so we can hand it to persist_or_die below, matching the
    // persistence contract of every other mutation surface.
    let (import_result, changes) = st
        .mutation_batcher
        .submit(
            move || -> Result<(selene_rdf::RdfImportResult, Vec<Change>), OpError> {
                let mut ontology = ontology_arc.write();
                selene_rdf::import::import_rdf_with_changes(
                    &body,
                    format,
                    target_graph.as_deref(),
                    &graph,
                    &ns,
                    &mut ontology,
                )
                .map_err(map_rdf_error)
            },
        )
        .await
        .map_err(super::graph_err)??;

    persist_or_die(state, &changes);

    Ok(RdfImportResult {
        nodes_created: import_result.nodes_created,
        edges_created: import_result.edges_created,
        labels_added: import_result.labels_added,
        properties_set: import_result.properties_set,
        ontology_triples_loaded: import_result.ontology_triples_loaded,
    })
}

/// Execute a SPARQL Update (INSERT DATA / DELETE DATA / DELETE-INSERT-WHERE).
///
/// Scoped principals are supported in 1.3.0: each mutation is checked
/// against their scope bitmap, so a scoped writer can update properties /
/// labels / edges on existing in-scope nodes. Creating new top-level
/// nodes remains admin-only because SPARQL Update's INSERT DATA cannot
/// express a parent-in-scope binding for the newly-minted node. `CLEAR` /
/// `DROP` on the default graph likewise require admin (they delete every
/// node, including out-of-scope ones).
///
/// Replica-rejected; routed through the mutation batcher; the captured
/// `Vec<Change>` is handed to `persist_or_die` so WAL / changelog /
/// version store observe the writes with the same durability posture as
/// GQL mutations — closing the 1.3.0 follow-up flagged in PR #74.
pub async fn sparql_update(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    update_str: &str,
) -> Result<SparqlUpdateResult, OpError> {
    reject_if_replica(state)?;
    require_action(state, auth, Action::GqlMutate)?;

    let auth_ctx = super::refresh_scope_if_stale(state, auth);
    let ns = state.rdf_namespace.clone();
    let update_owned = update_str.to_owned();
    let graph: SharedGraph = state.graph.clone();
    let st = Arc::clone(state);

    // Per-quad scope enforcement happens inside the batcher closure,
    // where the executor owns the write lock. Admin: no scope arg;
    // scoped: pass the bitmap by clone so the closure can outlive the
    // AuthContext borrow.
    let scope_clone = if auth_ctx.is_admin() {
        None
    } else {
        Some(auth_ctx.scope.clone())
    };

    let (result, changes) = st
        .mutation_batcher
        .submit(
            move || -> Result<(selene_rdf::update::UpdateResult, Vec<Change>), OpError> {
                let write_scope = match scope_clone.as_ref() {
                    Some(bm) => selene_rdf::update::WriteScope::scoped(bm),
                    None => selene_rdf::update::WriteScope::admin(),
                };
                selene_rdf::update::execute_update_shared(&graph, &ns, &update_owned, write_scope)
                    .map_err(map_sparql_update_error)
            },
        )
        .await
        .map_err(super::graph_err)??;

    // `Vec<Change>` is now populated by `apply_insert_data` /
    // `apply_delete_data` / `apply_delete_insert` committing through
    // `TrackedMutation::commit`. persist_or_die drives it through the
    // WAL coalescer + version store; the snapshot is already published
    // inside execute_update_shared.
    persist_or_die(state, &changes);

    Ok(SparqlUpdateResult {
        nodes_created: result.nodes_created,
        properties_set: result.properties_set,
        properties_removed: result.properties_removed,
        labels_added: result.labels_added,
        labels_removed: result.labels_removed,
        edges_created: result.edges_created,
        edges_deleted: result.edges_deleted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Role;
    use crate::bootstrap::ServerState;

    async fn test_state() -> (Arc<ServerState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let state = ServerState::for_testing(dir.path()).await;
        (Arc::new(state), dir)
    }

    fn reader_auth() -> AuthContext {
        AuthContext {
            principal_node_id: selene_core::NodeId(999),
            role: Role::Reader,
            scope: roaring::RoaringBitmap::new(),
            scope_generation: 0,
        }
    }

    fn operator_auth(scope_ids: &[u64]) -> AuthContext {
        let mut scope = roaring::RoaringBitmap::new();
        for id in scope_ids {
            scope.insert(*id as u32);
        }
        AuthContext {
            principal_node_id: selene_core::NodeId(999),
            role: Role::Operator,
            scope,
            scope_generation: 0,
        }
    }

    #[tokio::test]
    async fn rdf_import_requires_admin() {
        // Closes finding 11022: a non-admin principal must not be able to
        // import RDF. The original handler dropped auth entirely; this
        // asserts the positive control.
        let (state, _dir) = test_state().await;
        let auth = operator_auth(&[1, 2]);
        let result = rdf_import(
            &state,
            &auth,
            selene_rdf::RdfFormat::Turtle,
            None,
            axum::body::Bytes::from_static(b""),
        )
        .await;
        assert!(
            matches!(result, Err(OpError::Forbidden(_))),
            "operator-role import should be forbidden, got {result:?}"
        );
    }

    #[tokio::test]
    async fn sparql_update_scoped_allowed_on_in_scope_node() {
        // 1.3.0 follow-up to finding 11021: SPARQL Update is no longer
        // admin-only — a scoped principal can update properties on
        // nodes inside their bitmap. Seed a node, grant its id to a
        // scoped operator, observe the property write succeeds.
        let (state, _dir) = test_state().await;
        let shared = state.graph.clone();
        let (node_id, _) = shared
            .write(|m| {
                m.create_node(
                    selene_core::LabelSet::from_strs(&["Sensor"]),
                    selene_core::PropertyMap::from_pairs(vec![(
                        selene_core::IStr::new("unit"),
                        selene_core::Value::str("degC"),
                    )]),
                )
            })
            .unwrap();

        let auth = operator_auth(&[node_id.0]);
        let update = format!(
            "PREFIX sel: <selene:/> \
             INSERT DATA {{ <selene:/node/{}> <selene:/prop/status> \"ok\" }}",
            node_id.0
        );
        let result = sparql_update(&state, &auth, &update).await;
        assert!(
            result.is_ok(),
            "scoped in-bitmap property write should succeed, got {result:?}"
        );
        assert_eq!(result.unwrap().properties_set, 1);
    }

    #[tokio::test]
    async fn sparql_update_scoped_rejects_out_of_scope_node() {
        // Complementary to the success case: a write to a node outside
        // the scope bitmap returns Forbidden (mapped from
        // UpdateError::ScopeDenied). Without this, a scoped principal
        // could write anywhere the SPARQL update reached.
        let (state, _dir) = test_state().await;
        let shared = state.graph.clone();
        let ((in_scope, out_of_scope), _) = shared
            .write(|m| {
                let a = m.create_node(
                    selene_core::LabelSet::from_strs(&["Sensor"]),
                    selene_core::PropertyMap::new(),
                )?;
                let b = m.create_node(
                    selene_core::LabelSet::from_strs(&["Sensor"]),
                    selene_core::PropertyMap::new(),
                )?;
                Ok((a, b))
            })
            .unwrap();

        let auth = operator_auth(&[in_scope.0]);
        let update = format!(
            "INSERT DATA {{ <selene:/node/{}> <selene:/prop/unit> \"forbidden\" }}",
            out_of_scope.0
        );
        let result = sparql_update(&state, &auth, &update).await;
        assert!(
            matches!(result, Err(OpError::Forbidden(ref msg)) if msg.contains("scope")),
            "scoped write outside the bitmap should return Forbidden, got {result:?}"
        );
    }

    #[tokio::test]
    async fn sparql_update_rejects_node_creation_for_scoped_caller() {
        // INSERT DATA targeting a subject URI that does not resolve to
        // an existing node would implicitly create one — which requires
        // a parent-in-scope binding that SPARQL Update cannot express.
        // Scoped principals get Forbidden; admins would succeed.
        let (state, _dir) = test_state().await;
        let auth = operator_auth(&[1, 2]);
        let update = "INSERT DATA { <selene:/node/9999> <selene:/prop/x> \"y\" }";
        let result = sparql_update(&state, &auth, update).await;
        assert!(
            matches!(result, Err(OpError::Forbidden(ref msg)) if msg.contains("create")),
            "scoped INSERT creating a new node should be forbidden, got {result:?}"
        );
    }

    #[tokio::test]
    async fn sparql_update_admin_captures_wal_changes() {
        // Closes the durability caveat from PR #72: Vec<Change> flows
        // through execute_update_shared → persist_or_die. The easiest
        // structural proof is that the post-update graph generation
        // advances, indicating the mutation batcher observed writes.
        let (state, _dir) = test_state().await;
        let shared = state.graph.clone();
        let (node_id, _) = shared
            .write(|m| {
                m.create_node(
                    selene_core::LabelSet::from_strs(&["Sensor"]),
                    selene_core::PropertyMap::new(),
                )
            })
            .unwrap();

        let gen_before = state.graph.read(|g| g.generation());
        let admin = AuthContext::dev_admin();
        let update = format!(
            "INSERT DATA {{ <selene:/node/{}> <selene:/prop/unit> \"degC\" }}",
            node_id.0
        );
        sparql_update(&state, &admin, &update).await.unwrap();
        let gen_after = state.graph.read(|g| g.generation());
        assert!(
            gen_after > gen_before,
            "SPARQL Update must advance the graph generation (pre={gen_before}, post={gen_after})"
        );
    }

    #[tokio::test]
    async fn rdf_export_filters_scope_for_reader() {
        // Closes finding 11020: a scoped reader must not see nodes outside
        // its scope bitmap via RDF export.
        let (state, _dir) = test_state().await;

        // Seed two nodes: id 1 is "public" (in scope), id 2 is "secret" (out).
        let shared = state.graph.clone();
        let ((public_id, secret_id), _) = shared
            .write(|m| {
                let a = m.create_node(
                    selene_core::LabelSet::from_strs(&["Public"]),
                    selene_core::PropertyMap::from_pairs(vec![(
                        selene_core::IStr::new("name"),
                        selene_core::Value::str("alpha"),
                    )]),
                )?;
                let b = m.create_node(
                    selene_core::LabelSet::from_strs(&["Secret"]),
                    selene_core::PropertyMap::from_pairs(vec![(
                        selene_core::IStr::new("name"),
                        selene_core::Value::str("beta"),
                    )]),
                )?;
                Ok((a, b))
            })
            .unwrap();

        let auth = operator_auth(&[public_id.0]);
        let bytes = rdf_export(&state, &auth, selene_rdf::RdfFormat::Turtle, false)
            .expect("scoped export should succeed");
        let turtle = String::from_utf8(bytes).expect("turtle is utf-8");

        assert!(
            turtle.contains("alpha"),
            "in-scope node should appear: {turtle}"
        );
        assert!(
            !turtle.contains("beta"),
            "out-of-scope node must not leak: {turtle}"
        );
        // Sanity: admin sees both.
        let admin = AuthContext::dev_admin();
        let admin_bytes = rdf_export(&state, &admin, selene_rdf::RdfFormat::Turtle, false).unwrap();
        let admin_turtle = String::from_utf8(admin_bytes).unwrap();
        assert!(admin_turtle.contains("alpha") && admin_turtle.contains("beta"));

        // And reader with empty scope sees nothing on either side.
        let zero_scope = reader_auth();
        let empty_bytes =
            rdf_export(&state, &zero_scope, selene_rdf::RdfFormat::Turtle, false).unwrap();
        let empty_turtle = String::from_utf8(empty_bytes).unwrap();
        assert!(
            !empty_turtle.contains("alpha") && !empty_turtle.contains("beta"),
            "empty-scope reader must see no instance data: {empty_turtle}"
        );

        let _ = secret_id; // used only via scope membership above
    }

    #[tokio::test]
    async fn sparql_query_filters_scope_for_reader() {
        // Closes finding 11020 for SPARQL: a scoped principal's SELECT must
        // not observe out-of-scope nodes through the SPARQL endpoint.
        let (state, _dir) = test_state().await;
        let shared = state.graph.clone();
        let ((public_id, _secret_id), _) = shared
            .write(|m| {
                let a = m.create_node(
                    selene_core::LabelSet::from_strs(&["Sensor"]),
                    selene_core::PropertyMap::from_pairs(vec![(
                        selene_core::IStr::new("unit"),
                        selene_core::Value::str("degC"),
                    )]),
                )?;
                let b = m.create_node(
                    selene_core::LabelSet::from_strs(&["Sensor"]),
                    selene_core::PropertyMap::from_pairs(vec![(
                        selene_core::IStr::new("unit"),
                        selene_core::Value::str("hidden"),
                    )]),
                )?;
                Ok((a, b))
            })
            .unwrap();

        let auth = operator_auth(&[public_id.0]);
        // `RdfNamespace::new("selene:")` normalizes to "selene:/" (adds
        // trailing slash), so the concrete URIs are `selene:/type/<Label>`
        // and `selene:/prop/<key>`.
        let query = "SELECT ?u WHERE { ?s a <selene:/type/Sensor> . ?s <selene:/prop/unit> ?u }";
        let (bytes, _ct) = sparql_query(
            &state,
            &auth,
            query,
            selene_rdf::sparql::SparqlResultFormat::Json,
        )
        .expect("scoped SPARQL query should succeed");
        let body = String::from_utf8(bytes).unwrap();

        // Admin sees both values; scoped reader must only see "degC".
        let admin = AuthContext::dev_admin();
        let (admin_bytes, _) = sparql_query(
            &state,
            &admin,
            query,
            selene_rdf::sparql::SparqlResultFormat::Json,
        )
        .unwrap();
        let admin_body = String::from_utf8(admin_bytes).unwrap();
        assert!(admin_body.contains("degC") && admin_body.contains("hidden"));
        assert!(
            !body.contains("hidden"),
            "scoped query must not leak hidden value: {body}"
        );
    }
}
