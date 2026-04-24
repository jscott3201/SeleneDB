//! Principal management operations — admin-only CRUD for vault principals.
//!
//! All operations require `Action::PrincipalManage` authorization and operate
//! on the vault graph (encrypted, isolated from the main graph).

use serde::Serialize;

use crate::auth::engine::Action;
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;
use crate::vault::VaultService;

use super::OpError;

/// Principal data transfer object — safe to expose (no credential hash).
#[derive(Debug, Serialize)]
pub struct PrincipalDto {
    pub identity: String,
    pub role: String,
    pub enabled: bool,
    pub has_credential: bool,
    /// Main-graph node IDs that define this principal's scope root(s).
    /// Empty for admins (global scope) or for principals with no scope
    /// configured (which resolves to an empty access bitmap).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope_root_ids: Vec<u64>,
}

// ── Helpers ──────────────────────────────────────────────────────────

fn require_principal_manage(state: &ServerState, auth: &AuthContext) -> Result<(), OpError> {
    if !state
        .auth_engine
        .authorize_action(auth, Action::PrincipalManage)
    {
        return Err(OpError::AuthDenied);
    }
    Ok(())
}

fn vault_service(state: &ServerState) -> Result<&VaultService, OpError> {
    state
        .services
        .get::<VaultService>()
        .ok_or_else(|| OpError::Internal("vault not available".into()))
}

fn node_to_dto(g: &selene_graph::SeleneGraph, nid: selene_core::NodeId) -> Option<PrincipalDto> {
    let node = g.get_node(nid)?;
    let scope_root_ids = match node.property("scope_root_ids") {
        Some(selene_core::Value::List(items)) => items
            .iter()
            .filter_map(|v| match v {
                selene_core::Value::Int(i) if *i >= 0 => Some(*i as u64),
                selene_core::Value::UInt(u) => Some(*u),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    Some(PrincipalDto {
        identity: node
            .property("identity")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned(),
        role: node
            .property("role")
            .and_then(|v| v.as_str())
            .unwrap_or("reader")
            .to_owned(),
        enabled: node
            .property("enabled")
            .is_some_and(|v| matches!(v, selene_core::Value::Bool(true))),
        has_credential: node.property("credential_hash").is_some(),
        scope_root_ids,
    })
}

/// Encode a list of main-graph NodeId values into the property representation
/// used on the vault principal. Stored as `Value::List` of `Value::Int`.
fn encode_scope_root_ids(ids: &[u64]) -> selene_core::Value {
    use std::sync::Arc;
    let values: Arc<[selene_core::Value]> = ids
        .iter()
        .map(|id| selene_core::Value::Int(*id as i64))
        .collect::<Vec<_>>()
        .into();
    selene_core::Value::List(values)
}

/// Verify that every declared scope root exists in the main graph. Rejects
/// at configuration time so we fail loudly rather than silently granting an
/// empty scope when a root is mistyped or has been deleted.
fn verify_scope_roots_exist(state: &ServerState, scope_root_ids: &[u64]) -> Result<(), OpError> {
    if scope_root_ids.is_empty() {
        return Ok(());
    }
    let missing: Vec<u64> = state.graph.read(|g| {
        scope_root_ids
            .iter()
            .copied()
            .filter(|id| !g.contains_node(selene_core::NodeId(*id)))
            .collect()
    });
    if !missing.is_empty() {
        return Err(OpError::InvalidRequest(format!(
            "scope_root_ids contains unknown node id(s): {missing:?}"
        )));
    }
    Ok(())
}

// ── Operations ───────────────────────────────────────────────────────

/// List all principals in the vault.
pub fn list_principals(
    state: &ServerState,
    auth: &AuthContext,
) -> Result<Vec<PrincipalDto>, OpError> {
    require_principal_manage(state, auth)?;
    let vs = vault_service(state)?;

    let principals = vs.handle.graph.read(|g| {
        g.nodes_by_label("principal")
            .filter_map(|nid| node_to_dto(g, nid))
            .collect()
    });

    Ok(principals)
}

/// Get a single principal by identity.
pub fn get_principal(
    state: &ServerState,
    auth: &AuthContext,
    identity: &str,
) -> Result<PrincipalDto, OpError> {
    require_principal_manage(state, auth)?;
    let vs = vault_service(state)?;

    vs.handle
        .graph
        .read(|g| {
            g.nodes_by_label("principal")
                .find(|&nid| {
                    g.get_node(nid).is_some_and(|n| {
                        n.property("identity")
                            .is_some_and(|v| v.as_str() == Some(identity))
                    })
                })
                .and_then(|nid| node_to_dto(g, nid))
        })
        .ok_or_else(|| OpError::InvalidRequest(format!("principal '{identity}' not found")))
}

/// Create a new principal with the given identity, role, optional password,
/// and optional scope roots.
///
/// `scope_root_ids` lists main-graph NodeId values that bound the principal's
/// authority; the scope bitmap is the union of `[:contains]*` descendants
/// beneath each root. Admins should pass `&[]` (admins get global scope by
/// role, not by containment). Scope roots are validated at call time — an
/// unknown id is an `InvalidRequest`, not a silent empty-scope creation.
pub fn create_principal(
    state: &ServerState,
    auth: &AuthContext,
    identity: &str,
    role: &str,
    password: Option<&str>,
    scope_root_ids: &[u64],
) -> Result<PrincipalDto, OpError> {
    require_principal_manage(state, auth)?;

    // Validate role
    role.parse::<crate::auth::Role>()
        .map_err(OpError::InvalidRequest)?;

    verify_scope_roots_exist(state, scope_root_ids)?;

    let vs = vault_service(state)?;

    // Check for duplicate
    let exists = vs.handle.graph.read(|g| {
        g.nodes_by_label("principal").any(|nid| {
            g.get_node(nid).is_some_and(|n| {
                n.property("identity")
                    .is_some_and(|v| v.as_str() == Some(identity))
            })
        })
    });
    if exists {
        return Err(OpError::Conflict(format!(
            "principal '{identity}' already exists"
        )));
    }

    // Build properties
    let mut props = vec![
        (
            selene_core::IStr::new("identity"),
            selene_core::Value::str(identity),
        ),
        (
            selene_core::IStr::new("role"),
            selene_core::Value::str(role),
        ),
        (
            selene_core::IStr::new("enabled"),
            selene_core::Value::Bool(true),
        ),
    ];

    if let Some(pw) = password {
        let hash = crate::auth::credential::hash_credential(pw)
            .map_err(|e| OpError::Internal(format!("credential hash failed: {e}")))?;
        props.push((
            selene_core::IStr::new("credential_hash"),
            selene_core::Value::str(&hash),
        ));
    }

    if !scope_root_ids.is_empty() {
        props.push((
            selene_core::IStr::new("scope_root_ids"),
            encode_scope_root_ids(scope_root_ids),
        ));
    }

    vs.handle
        .graph
        .write(|m| {
            m.create_node(
                selene_core::LabelSet::from_strs(&["principal"]),
                selene_core::PropertyMap::from_pairs(props),
            )
        })
        .map_err(|e| OpError::Internal(format!("vault write failed: {e}")))?;

    vs.handle
        .flush(&vs.master_key)
        .map_err(|e| OpError::Internal(format!("vault flush failed: {e}")))?;

    get_principal(state, auth, identity)
}

/// Update a principal's role, enabled status, and/or scope roots.
///
/// `scope_root_ids` replaces the stored value entirely when supplied.
/// Passing `Some(&[])` clears the scope (the principal sees nothing until
/// new roots are set); pass `None` to leave the existing roots untouched.
pub fn update_principal(
    state: &ServerState,
    auth: &AuthContext,
    identity: &str,
    role: Option<&str>,
    enabled: Option<bool>,
    scope_root_ids: Option<&[u64]>,
) -> Result<PrincipalDto, OpError> {
    require_principal_manage(state, auth)?;

    if let Some(r) = role {
        r.parse::<crate::auth::Role>()
            .map_err(OpError::InvalidRequest)?;
    }

    if let Some(ids) = scope_root_ids {
        verify_scope_roots_exist(state, ids)?;
    }

    let vs = vault_service(state)?;

    let node_id = vs
        .handle
        .graph
        .read(|g| {
            g.nodes_by_label("principal").find(|&nid| {
                g.get_node(nid).is_some_and(|n| {
                    n.property("identity")
                        .is_some_and(|v| v.as_str() == Some(identity))
                })
            })
        })
        .ok_or_else(|| OpError::InvalidRequest(format!("principal '{identity}' not found")))?;

    vs.handle
        .graph
        .write(|m| {
            if let Some(r) = role {
                m.set_property(
                    node_id,
                    selene_core::IStr::new("role"),
                    selene_core::Value::str(r),
                )?;
            }
            if let Some(en) = enabled {
                m.set_property(
                    node_id,
                    selene_core::IStr::new("enabled"),
                    selene_core::Value::Bool(en),
                )?;
            }
            if let Some(ids) = scope_root_ids {
                if ids.is_empty() {
                    m.remove_property(node_id, "scope_root_ids")?;
                } else {
                    m.set_property(
                        node_id,
                        selene_core::IStr::new("scope_root_ids"),
                        encode_scope_root_ids(ids),
                    )?;
                }
            }
            Ok(())
        })
        .map_err(|e| OpError::Internal(format!("vault write failed: {e}")))?;

    vs.handle
        .flush(&vs.master_key)
        .map_err(|e| OpError::Internal(format!("vault flush failed: {e}")))?;

    get_principal(state, auth, identity)
}

/// Disable a principal (set enabled = false).
pub fn disable_principal(
    state: &ServerState,
    auth: &AuthContext,
    identity: &str,
) -> Result<PrincipalDto, OpError> {
    update_principal(state, auth, identity, None, Some(false), None)
}

/// Create an OAuth-registered client principal in the vault.
///
/// This is the registration entry point for OAuth 2.1 dynamic client
/// registration (RFC 7591). It bypasses the `PrincipalManage` auth check
/// because OAuth registration is gated at the HTTP layer by the
/// `registration_token` setting — there is no authenticated principal to
/// check against during client bootstrap.
///
/// OAuth-specific properties (`client_name`, `redirect_uri`) are persisted
/// alongside the standard principal shape so the OAuth authorization flow
/// can retrieve them without a separate lookup table.
///
/// Scope roots are intentionally not exposed here — dynamically-registered
/// clients have no scope by default; an admin must attach scope roots via
/// `update_principal` after registration (or grant an admin-scoped role).
/// Role capping (e.g. refusing `admin`) is enforced by the HTTP caller so
/// this function can also service future internal registrations that may
/// legitimately create service-role clients.
pub fn oauth_register_principal(
    state: &ServerState,
    identity: &str,
    role: &str,
    password: &str,
    client_name: &str,
    redirect_uri: &str,
) -> Result<(), OpError> {
    role.parse::<crate::auth::Role>()
        .map_err(OpError::InvalidRequest)?;

    let vs = vault_service(state)?;

    let exists = vs.handle.graph.read(|g| {
        g.nodes_by_label("principal").any(|nid| {
            g.get_node(nid).is_some_and(|n| {
                n.property("identity")
                    .is_some_and(|v| v.as_str() == Some(identity))
            })
        })
    });
    if exists {
        return Err(OpError::Conflict(format!(
            "principal '{identity}' already exists"
        )));
    }

    let hash = crate::auth::credential::hash_credential(password)
        .map_err(|e| OpError::Internal(format!("credential hash failed: {e}")))?;

    let props = vec![
        (
            selene_core::IStr::new("identity"),
            selene_core::Value::str(identity),
        ),
        (
            selene_core::IStr::new("role"),
            selene_core::Value::str(role),
        ),
        (
            selene_core::IStr::new("enabled"),
            selene_core::Value::Bool(true),
        ),
        (
            selene_core::IStr::new("credential_hash"),
            selene_core::Value::str(&hash),
        ),
        (
            selene_core::IStr::new("client_name"),
            selene_core::Value::str(client_name),
        ),
        (
            selene_core::IStr::new("redirect_uri"),
            selene_core::Value::str(redirect_uri),
        ),
    ];

    vs.handle
        .graph
        .write(|m| {
            m.create_node(
                selene_core::LabelSet::from_strs(&["principal"]),
                selene_core::PropertyMap::from_pairs(props),
            )
        })
        .map_err(|e| OpError::Internal(format!("vault write failed: {e}")))?;

    vs.handle
        .flush(&vs.master_key)
        .map_err(|e| OpError::Internal(format!("vault flush failed: {e}")))?;

    Ok(())
}

/// Rotate a principal's credential (set a new password).
pub fn rotate_credential(
    state: &ServerState,
    auth: &AuthContext,
    identity: &str,
    new_password: &str,
) -> Result<(), OpError> {
    require_principal_manage(state, auth)?;
    let vs = vault_service(state)?;

    let node_id = vs
        .handle
        .graph
        .read(|g| {
            g.nodes_by_label("principal").find(|&nid| {
                g.get_node(nid).is_some_and(|n| {
                    n.property("identity")
                        .is_some_and(|v| v.as_str() == Some(identity))
                })
            })
        })
        .ok_or_else(|| OpError::InvalidRequest(format!("principal '{identity}' not found")))?;

    let hash = crate::auth::credential::hash_credential(new_password)
        .map_err(|e| OpError::Internal(format!("credential hash failed: {e}")))?;

    vs.handle
        .graph
        .write(|m| {
            m.set_property(
                node_id,
                selene_core::IStr::new("credential_hash"),
                selene_core::Value::str(&hash),
            )
        })
        .map_err(|e| OpError::Internal(format!("vault write failed: {e}")))?;

    vs.handle
        .flush(&vs.master_key)
        .map_err(|e| OpError::Internal(format!("vault flush failed: {e}")))?;

    Ok(())
}
