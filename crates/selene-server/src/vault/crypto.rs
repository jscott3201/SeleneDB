//! Vault encryption primitives — envelope encryption with XChaCha20-Poly1305.
//!
//! Master key (KEK) wraps a random data encryption key (DEK).
//! DEK encrypts the vault payload. Key rotation re-wraps DEK without
//! re-encrypting data.

use chacha20poly1305::XChaCha20Poly1305;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use zeroize::Zeroize;

use super::error::VaultError;

/// 256-bit master key (Key Encryption Key).
/// Zeroized on drop to prevent key material lingering in memory.
#[derive(Zeroize)]
#[zeroize(drop)]
pub struct MasterKey([u8; 32]);

/// 256-bit data encryption key (wrapped by master key).
/// Zeroized on drop.
#[derive(Zeroize)]
#[zeroize(drop)]
pub struct DataKey([u8; 32]);

impl MasterKey {
    /// Load from raw 32 bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Derive from passphrase using Argon2id.
    /// Parameters: 64 MB memory, 3 iterations, 1 thread — ~500ms on RPi 5.
    pub fn from_passphrase(passphrase: &str, salt: &[u8; 16]) -> Result<Self, VaultError> {
        use argon2::{Algorithm, Argon2, Params, Version};
        let params = Params::new(65536, 3, 1, Some(32))
            .map_err(|e| VaultError::KeyDerivation(format!("argon2 params: {e}")))?;
        let mut key = [0u8; 32];
        Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
            .hash_password_into(passphrase.as_bytes(), salt, &mut key)
            .map_err(|e| VaultError::KeyDerivation(format!("argon2: {e}")))?;
        Ok(Self(key))
    }

    /// Dev mode: well-known all-zero key. Same code path as production.
    pub fn dev_key() -> Self {
        Self([0u8; 32])
    }

    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl DataKey {
    /// Generate a random 256-bit DEK.
    pub fn generate() -> Self {
        use rand::RngExt;
        let mut key = [0u8; 32];
        rand::rng().fill(&mut key[..]);
        Self(key)
    }

    /// Encrypt (wrap) this DEK using the master key.
    /// Returns (ciphertext, nonce).
    pub fn wrap(&self, master: &MasterKey) -> Result<(Vec<u8>, [u8; 24]), VaultError> {
        let cipher = XChaCha20Poly1305::new(master.0.as_ref().into());
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ciphertext = cipher.encrypt(&nonce, self.0.as_ref())?;
        let mut nonce_bytes = [0u8; 24];
        nonce_bytes.copy_from_slice(nonce.as_slice());
        Ok((ciphertext, nonce_bytes))
    }

    /// Decrypt (unwrap) a DEK using the master key.
    pub fn unwrap(
        encrypted: &[u8],
        nonce: &[u8; 24],
        master: &MasterKey,
    ) -> Result<Self, VaultError> {
        let cipher = XChaCha20Poly1305::new(master.0.as_ref().into());
        let mut plaintext = cipher.decrypt(nonce.as_ref().into(), encrypted)?;
        if plaintext.len() != 32 {
            plaintext.zeroize();
            return Err(VaultError::Crypto("unwrapped DEK is not 32 bytes".into()));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&plaintext);
        plaintext.zeroize(); // scrub plaintext before dropping
        Ok(Self(key))
    }
}

/// Encrypt a serialized payload with the DEK and associated data.
/// Returns (ciphertext_with_tag, nonce).
pub fn encrypt_payload(
    data: &[u8],
    dek: &DataKey,
    aad: &[u8],
) -> Result<(Vec<u8>, [u8; 24]), VaultError> {
    let cipher = XChaCha20Poly1305::new(dek.0.as_ref().into());
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let payload = Payload { msg: data, aad };
    let ciphertext = cipher.encrypt(&nonce, payload)?;
    let mut nonce_bytes = [0u8; 24];
    nonce_bytes.copy_from_slice(nonce.as_slice());
    Ok((ciphertext, nonce_bytes))
}

/// Decrypt a vault payload with the DEK and associated data.
pub fn decrypt_payload(
    ciphertext: &[u8],
    nonce: &[u8; 24],
    dek: &DataKey,
    aad: &[u8],
) -> Result<Vec<u8>, VaultError> {
    let cipher = XChaCha20Poly1305::new(dek.0.as_ref().into());
    let payload = Payload {
        msg: ciphertext,
        aad,
    };
    cipher
        .decrypt(nonce.as_ref().into(), payload)
        .map_err(VaultError::from)
}

/// Generate a random 16-byte salt for Argon2id.
pub fn generate_salt() -> [u8; 16] {
    use rand::RngExt;
    let mut salt = [0u8; 16];
    rand::rng().fill(&mut salt[..]);
    salt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn master_key_round_trip() {
        let mk = MasterKey::from_bytes([42u8; 32]);
        assert_eq!(mk.as_bytes(), &[42u8; 32]);
    }

    #[test]
    fn dek_wrap_unwrap() {
        let master = MasterKey::from_bytes([1u8; 32]);
        let dek = DataKey::generate();
        let original_bytes: [u8; 32] = dek.0;

        let (encrypted, nonce) = dek.wrap(&master).unwrap();
        assert_ne!(&encrypted[..32], &original_bytes[..]);

        let restored = DataKey::unwrap(&encrypted, &nonce, &master).unwrap();
        assert_eq!(restored.0, original_bytes);
    }

    #[test]
    fn wrong_master_key_fails() {
        let master1 = MasterKey::from_bytes([1u8; 32]);
        let master2 = MasterKey::from_bytes([2u8; 32]);
        let dek = DataKey::generate();

        let (encrypted, nonce) = dek.wrap(&master1).unwrap();
        let result = DataKey::unwrap(&encrypted, &nonce, &master2);
        assert!(result.is_err());
    }

    #[test]
    fn payload_encrypt_decrypt() {
        let dek = DataKey::generate();
        let data = b"sensitive vault data";
        let aad = b"SVLT\x01\x00";

        let (ciphertext, nonce) = encrypt_payload(data, &dek, aad).unwrap();
        let plaintext = decrypt_payload(&ciphertext, &nonce, &dek, aad).unwrap();
        assert_eq!(plaintext, data);
    }

    #[test]
    fn tampered_aad_fails() {
        let dek = DataKey::generate();
        let data = b"sensitive vault data";
        let aad = b"SVLT\x01\x00";

        let (ciphertext, nonce) = encrypt_payload(data, &dek, aad).unwrap();
        let bad_aad = b"SVLT\x02\x00";
        let result = decrypt_payload(&ciphertext, &nonce, &dek, bad_aad);
        assert!(result.is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let dek = DataKey::generate();
        let data = b"sensitive vault data";
        let aad = b"SVLT\x01\x00";

        let (mut ciphertext, nonce) = encrypt_payload(data, &dek, aad).unwrap();
        ciphertext[0] ^= 0xFF;
        let result = decrypt_payload(&ciphertext, &nonce, &dek, aad);
        assert!(result.is_err());
    }

    #[test]
    fn passphrase_derivation() {
        let salt = generate_salt();
        let mk1 = MasterKey::from_passphrase("test-passphrase", &salt).unwrap();
        let mk2 = MasterKey::from_passphrase("test-passphrase", &salt).unwrap();
        assert_eq!(mk1.0, mk2.0); // same passphrase + salt = same key

        let mk3 = MasterKey::from_passphrase("different", &salt).unwrap();
        assert_ne!(mk1.0, mk3.0);
    }

    #[test]
    fn dev_key_is_zeros() {
        let dk = MasterKey::dev_key();
        assert_eq!(dk.0, [0u8; 32]);
    }
}
