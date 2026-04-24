//! Connection handshake — authenticates a connection against the vault.
//!
//! Since 1.3.0 all principals live in the vault graph; the main graph holds
//! only the containment tree used for scope expansion. `authenticate` takes
//! both graph handles and never consults the main graph for identity data.

use roaring::RoaringBitmap;
use selene_core::{NodeId, Value};
use selene_graph::SharedGraph;

use super::Role;
use super::engine::AuthEngine;

/// Per-connection authorization context, established at handshake.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// The principal's node ID in the vault graph.
    pub principal_node_id: NodeId,
    /// The principal's role.
    pub role: Role,
    /// Bitmap of main-graph node IDs this principal can access.
    /// Empty for admin (global access checked separately).
    pub scope: RoaringBitmap,
    /// Main-graph containment generation at scope resolution time.
    pub scope_generation: u64,
}

impl AuthContext {
    /// Create a dev-mode admin context (no authentication required).
    pub fn dev_admin() -> Self {
        Self {
            principal_node_id: NodeId(0),
            role: Role::Admin,
            scope: RoaringBitmap::new(),
            scope_generation: 0,
        }
    }

    /// Check if this context has global (admin) scope.
    pub fn is_admin(&self) -> bool {
        self.role == Role::Admin
    }

    /// Check if a node ID is within this principal's scope.
    pub fn in_scope(&self, node_id: NodeId) -> bool {
        self.role == Role::Admin || self.scope.contains(node_id.0 as u32)
    }
}

/// Authenticate a connection using the handshake credentials.
///
/// Principals are resolved from the vault graph; scope is expanded against
/// the main graph. Both arguments are required — passing the main graph in
/// place of the vault (as the pre-1.3.0 code did) would silently deny every
/// login, because post-unification the main graph contains no `:principal`
/// nodes by design.
pub fn authenticate(
    vault_graph: &SharedGraph,
    main_graph: &SharedGraph,
    auth_type: &str,
    identity: &str,
    credentials: &str,
    dev_mode: bool,
) -> Result<AuthContext, AuthError> {
    match auth_type {
        "dev" => {
            if !dev_mode {
                return Err(AuthError::UnsupportedAuthType(
                    "dev auth_type is not allowed in production mode".into(),
                ));
            }
            authenticate_dev(vault_graph, main_graph, identity)
        }
        "token" | "psk" => {
            authenticate_by_credential(vault_graph, main_graph, identity, credentials)
        }
        other => Err(AuthError::UnsupportedAuthType(other.to_string())),
    }
}

fn authenticate_dev(
    vault_graph: &SharedGraph,
    main_graph: &SharedGraph,
    identity: &str,
) -> Result<AuthContext, AuthError> {
    // In dev mode, if identity is empty or "admin", return admin context
    if identity.is_empty() || identity == "admin" {
        return Ok(AuthContext::dev_admin());
    }

    let scope_gen = main_graph.containment_generation();
    let principal_id = vault_graph.read(|g| find_principal_by_identity(g, identity))?;

    let role = vault_graph.read(|g| {
        let node = g
            .get_node(principal_id)
            .ok_or_else(|| AuthError::PrincipalNotFound(identity.to_string()))?;
        extract_role(node, identity)
    })?;

    let scope = resolve_scope_two_graphs(vault_graph, main_graph, principal_id, role);

    Ok(AuthContext {
        principal_node_id: principal_id,
        role,
        scope,
        scope_generation: scope_gen,
    })
}

/// Find a principal node in the vault graph by its `identity` property.
pub(super) fn find_principal_by_identity(
    g: &selene_graph::SeleneGraph,
    identity: &str,
) -> Result<NodeId, AuthError> {
    g.nodes_by_label("principal")
        .find(|&node_id| {
            g.get_node(node_id).is_some_and(|n| {
                n.property("identity")
                    .is_some_and(|v| v.as_str() == Some(identity))
            })
        })
        .ok_or_else(|| AuthError::PrincipalNotFound(identity.to_string()))
}

/// Extract and parse the role from a principal node.
fn extract_role(node: selene_graph::NodeRef<'_>, identity: &str) -> Result<Role, AuthError> {
    let role_str = node
        .property("role")
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .ok_or_else(|| AuthError::MissingRole(identity.to_string()))?;

    role_str
        .parse()
        .map_err(|_| AuthError::InvalidRole(role_str))
}

/// Orchestrate scope resolution across the two graphs under read locks.
pub(crate) fn resolve_scope_two_graphs(
    vault_graph: &SharedGraph,
    main_graph: &SharedGraph,
    principal_id: NodeId,
    role: Role,
) -> RoaringBitmap {
    vault_graph.read(|vg| {
        main_graph.read(|mg| {
            AuthEngine::resolve_scope(vg, mg, principal_id, role).unwrap_or_default()
        })
    })
}

/// Authenticate by identity (vault lookup) + credential (argon2id verify).
///
/// 1. Find principal node in vault by `identity`.
/// 2. Check `enabled == true`.
/// 3. Verify `credentials` against `credential_hash`.
/// 4. Extract role; expand scope against the main graph.
fn authenticate_by_credential(
    vault_graph: &SharedGraph,
    main_graph: &SharedGraph,
    identity: &str,
    credentials: &str,
) -> Result<AuthContext, AuthError> {
    let scope_gen = main_graph.containment_generation();

    let (principal_id, role, credential_hash, enabled) = vault_graph.read(|g| {
        let principal_id = find_principal_by_identity(g, identity)?;
        let node = g
            .get_node(principal_id)
            .ok_or_else(|| AuthError::PrincipalNotFound(identity.to_string()))?;

        let enabled = node
            .property("enabled")
            .is_some_and(|v| matches!(v, Value::Bool(true)));

        let credential_hash = node
            .property("credential_hash")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .ok_or_else(|| AuthError::CredentialNotConfigured(identity.to_string()))?;

        let role = extract_role(node, identity)?;

        Ok::<_, AuthError>((principal_id, role, credential_hash, enabled))
    })?;

    if !enabled {
        return Err(AuthError::PrincipalDisabled(identity.to_string()));
    }

    let valid = super::credential::verify_credential(credentials, &credential_hash)
        .map_err(|e| AuthError::CredentialVerifyFailed(e.to_string()))?;

    if !valid {
        return Err(AuthError::InvalidCredential(identity.to_string()));
    }

    let scope = resolve_scope_two_graphs(vault_graph, main_graph, principal_id, role);

    Ok(AuthContext {
        principal_node_id: principal_id,
        role,
        scope,
        scope_generation: scope_gen,
    })
}

/// Authentication errors.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("principal not found: {0}")]
    PrincipalNotFound(String),
    #[error("principal disabled: {0}")]
    PrincipalDisabled(String),
    #[error("missing role on principal: {0}")]
    MissingRole(String),
    #[error("invalid role: {0}")]
    InvalidRole(String),
    #[error("credential not configured for principal: {0}")]
    CredentialNotConfigured(String),
    #[error("invalid credential for principal: {0}")]
    InvalidCredential(String),
    #[error("credential verification failed: {0}")]
    CredentialVerifyFailed(String),
    #[error("unsupported auth type: {0}")]
    UnsupportedAuthType(String),
    #[error("vault unavailable: {0}")]
    VaultUnavailable(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_admin_context() {
        let ctx = AuthContext::dev_admin();
        assert!(ctx.is_admin());
        assert!(ctx.in_scope(NodeId(999))); // admin sees everything
    }

    #[test]
    fn scoped_context() {
        let mut scope = RoaringBitmap::new();
        scope.insert(1);
        scope.insert(2);

        let ctx = AuthContext {
            principal_node_id: NodeId(100),
            role: Role::Operator,
            scope,
            scope_generation: 0,
        };

        assert!(!ctx.is_admin());
        assert!(ctx.in_scope(NodeId(1)));
        assert!(ctx.in_scope(NodeId(2)));
        assert!(!ctx.in_scope(NodeId(3)));
    }
}
