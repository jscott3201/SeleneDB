//! Vault error types.

use thiserror::Error;

/// Errors from vault operations.
#[derive(Debug, Error)]
pub enum VaultError {
    #[error("vault I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("encryption/decryption failed: {0}")]
    Crypto(String),

    #[error("invalid vault format: {0}")]
    InvalidFormat(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("key derivation error: {0}")]
    KeyDerivation(String),

    #[error("vault not available: {0}")]
    NotAvailable(String),
}

impl From<chacha20poly1305::aead::Error> for VaultError {
    fn from(_e: chacha20poly1305::aead::Error) -> Self {
        // Don't leak cipher internals in error messages
        VaultError::Crypto("AEAD authentication failed".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 1 ─────────────────────────────────────────────────────────────────
    #[test]
    fn vault_error_io_display() {
        let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = VaultError::Io(inner);
        let msg = err.to_string();
        assert!(msg.contains("vault I/O error"), "got: {msg}");
        assert!(msg.contains("file missing"), "got: {msg}");
    }

    // 2 ─────────────────────────────────────────────────────────────────
    #[test]
    fn vault_error_crypto_display() {
        let err = VaultError::Crypto("bad ciphertext".into());
        let msg = err.to_string();
        assert!(msg.contains("encryption/decryption failed"), "got: {msg}");
        assert!(msg.contains("bad ciphertext"), "got: {msg}");
    }

    // 3 ─────────────────────────────────────────────────────────────────
    #[test]
    fn vault_error_invalid_format_display() {
        let err = VaultError::InvalidFormat("wrong magic bytes".into());
        let msg = err.to_string();
        assert!(msg.contains("invalid vault format"), "got: {msg}");
        assert!(msg.contains("wrong magic bytes"), "got: {msg}");
    }

    // 4 ─────────────────────────────────────────────────────────────────
    #[test]
    fn vault_error_serialization_display() {
        let err = VaultError::Serialization("postcard failed".into());
        let msg = err.to_string();
        assert!(msg.contains("serialization error"), "got: {msg}");
    }

    // 5 ─────────────────────────────────────────────────────────────────
    #[test]
    fn vault_error_key_derivation_display() {
        let err = VaultError::KeyDerivation("argon2 params invalid".into());
        let msg = err.to_string();
        assert!(msg.contains("key derivation error"), "got: {msg}");
    }

    // 6 ─────────────────────────────────────────────────────────────────
    #[test]
    fn vault_error_not_available_display() {
        let err = VaultError::NotAvailable("no master key".into());
        let msg = err.to_string();
        assert!(msg.contains("vault not available"), "got: {msg}");
        assert!(msg.contains("no master key"), "got: {msg}");
    }

    // 7 ─────────────────────────────────────────────────────────────────
    /// From<io::Error> conversion produces VaultError::Io.
    #[test]
    fn vault_error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let vault_err: VaultError = io_err.into();
        assert!(matches!(vault_err, VaultError::Io(_)));
    }

    // 8 ─────────────────────────────────────────────────────────────────
    /// From<aead::Error> hides cipher internals.
    #[test]
    fn vault_error_from_aead_hides_internals() {
        let aead_err = chacha20poly1305::aead::Error;
        let vault_err: VaultError = aead_err.into();
        let msg = vault_err.to_string();
        assert!(
            msg.contains("AEAD authentication failed"),
            "must use generic message: {msg}"
        );
        // The error message must NOT leak the original error's details
        // beyond the fixed string.
    }
}
