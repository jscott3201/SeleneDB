//! Credential hashing and verification using argon2id.
//!
//! Credentials are never stored in plaintext. Principal nodes carry a
//! `credential_hash` property containing an argon2id hash string
//! (algorithm + salt + hash in PHC string format).

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};

/// Hash a plaintext credential using argon2id.
///
/// Returns a PHC-format string containing the algorithm parameters,
/// salt, and hash. This string is stored in the principal node's
/// `credential_hash` property.
pub fn hash_credential(secret: &str) -> Result<String, CredentialError> {
    let salt = SaltString::generate(&mut rand_core::OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|e| CredentialError::HashFailed(e.to_string()))?;
    Ok(hash.to_string())
}

/// Verify a plaintext credential against an argon2id hash.
///
/// Uses constant-time comparison to prevent timing attacks.
/// Returns `Ok(true)` if the credential matches, `Ok(false)` if not.
pub fn verify_credential(secret: &str, hash_str: &str) -> Result<bool, CredentialError> {
    let parsed_hash =
        PasswordHash::new(hash_str).map_err(|e| CredentialError::InvalidHash(e.to_string()))?;

    match Argon2::default().verify_password(secret.as_bytes(), &parsed_hash) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::Password) => Ok(false),
        Err(e) => Err(CredentialError::VerifyFailed(e.to_string())),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("failed to hash credential: {0}")]
    HashFailed(String),
    #[error("invalid hash format: {0}")]
    InvalidHash(String),
    #[error("verification failed: {0}")]
    VerifyFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_roundtrip() {
        let secret = "my-secret-token-123";
        let hash = hash_credential(secret).unwrap();

        // Hash should be a PHC string starting with $argon2id$
        assert!(hash.starts_with("$argon2id$"));

        // Correct secret verifies
        assert!(verify_credential(secret, &hash).unwrap());

        // Wrong secret fails
        assert!(!verify_credential("wrong-secret", &hash).unwrap());
    }

    #[test]
    fn different_hashes_for_same_secret() {
        let secret = "same-secret";
        let hash1 = hash_credential(secret).unwrap();
        let hash2 = hash_credential(secret).unwrap();

        // Different salts produce different hashes
        assert_ne!(hash1, hash2);

        // Both verify
        assert!(verify_credential(secret, &hash1).unwrap());
        assert!(verify_credential(secret, &hash2).unwrap());
    }

    #[test]
    fn invalid_hash_string() {
        let result = verify_credential("secret", "not-a-valid-hash");
        assert!(result.is_err());
    }
}
