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
