//! Federation-specific error types.

/// Errors specific to federation operations.
#[derive(Debug, thiserror::Error)]
pub enum FederationError {
    #[error("peer not found: {0}")]
    PeerNotFound(String),

    #[error("connection failed to {peer}: {reason}")]
    ConnectionFailed { peer: String, reason: String },

    #[error("registration rejected: {0}")]
    RegistrationRejected(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("client error: {0}")]
    Client(String),
}
