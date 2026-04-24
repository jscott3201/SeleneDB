//! Cedar-based authorization for Selene.
//!
//! Principals are nodes in the graph with `scoped_to` edges defining their
//! containment scope. Cedar policies on disk define role→action mappings.
//! Query-level enforcement is handled via scope bitmaps passed to GQL execution.

pub mod credential;
pub(crate) mod engine;
pub mod handshake;
pub mod oauth;
pub(crate) mod policies;
pub(crate) mod projection;
pub(crate) mod reserved;

pub use credential::{CredentialError, hash_credential, verify_credential};
pub use engine::AuthEngine;
pub use handshake::AuthContext;
pub use oauth::{OAuthError, OAuthTokenService};

/// Borrow the vault's graph for principal lookup during authentication.
///
/// Since 1.3.0 all non-admin authentication resolves principals out of the
/// vault graph (see `auth::reserved` for the escalation path this closes).
/// Returning `VaultUnavailable` keeps the caller fail-closed: a production
/// deployment without a vault cannot authenticate non-admin users at all.
pub(crate) fn vault_graph_for_auth(
    state: &crate::bootstrap::ServerState,
) -> Result<&selene_graph::SharedGraph, handshake::AuthError> {
    state
        .services
        .get::<crate::vault::VaultService>()
        .map(|svc| &svc.handle.graph)
        .ok_or_else(|| {
            handshake::AuthError::VaultUnavailable(
                "vault not configured; non-admin authentication requires a vault".into(),
            )
        })
}

/// Roles recognized by the default Cedar policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Role {
    Admin,
    Service,
    Operator,
    Reader,
    Device,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Service => "service",
            Self::Operator => "operator",
            Self::Reader => "reader",
            Self::Device => "device",
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Role {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "admin" => Ok(Self::Admin),
            "service" => Ok(Self::Service),
            "operator" => Ok(Self::Operator),
            "reader" => Ok(Self::Reader),
            "device" => Ok(Self::Device),
            other => Err(format!("unknown role: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_round_trip() {
        for role in [
            Role::Admin,
            Role::Service,
            Role::Operator,
            Role::Reader,
            Role::Device,
        ] {
            let s = role.as_str();
            let parsed: Role = s.parse().unwrap();
            assert_eq!(parsed, role);
        }
    }

    #[test]
    fn invalid_role() {
        let result: Result<Role, _> = "superadmin".parse();
        assert!(result.is_err());
    }
}
