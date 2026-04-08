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
    })
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

/// Create a new principal with the given identity, role, and optional password.
pub fn create_principal(
    state: &ServerState,
    auth: &AuthContext,
    identity: &str,
    role: &str,
    password: Option<&str>,
) -> Result<PrincipalDto, OpError> {
    require_principal_manage(state, auth)?;

    // Validate role
    role.parse::<crate::auth::Role>()
        .map_err(OpError::InvalidRequest)?;

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
        (selene_core::IStr::new("identity"), selene_core::Value::str(identity)),
        (selene_core::IStr::new("role"), selene_core::Value::str(role)),
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

/// Update a principal's role and/or enabled status.
pub fn update_principal(
    state: &ServerState,
    auth: &AuthContext,
    identity: &str,
    role: Option<&str>,
    enabled: Option<bool>,
) -> Result<PrincipalDto, OpError> {
    require_principal_manage(state, auth)?;

    if let Some(r) = role {
        r.parse::<crate::auth::Role>()
            .map_err(OpError::InvalidRequest)?;
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
                m.set_property(node_id, selene_core::IStr::new("role"), selene_core::Value::str(r))?;
            }
            if let Some(en) = enabled {
                m.set_property(
                    node_id,
                    selene_core::IStr::new("enabled"),
                    selene_core::Value::Bool(en),
                )?;
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
    update_principal(state, auth, identity, None, Some(false))
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
