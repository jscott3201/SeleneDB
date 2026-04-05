//! Encrypted Secure Vault — isolated, encrypted graph for sensitive data.
//!
//! The vault stores principal nodes, API keys, Cedar policies, server config
//! overrides, and audit logs in an encrypted file (`secure.vault`). Hard
//! isolation from the main graph — no cross-graph joins.
//!
//! Architecture:
//! - Master key (KEK) wraps a random data encryption key (DEK)
//! - DEK encrypts the vault payload (XChaCha20-Poly1305 AEAD)
//! - Vault graph is a standard `SharedGraph` — same GQL execution path
//! - Persisted via atomic write on each mutation (no WAL needed — admin ops are rare)

pub mod audit;
pub mod crypto;
pub mod error;
pub mod storage;

use std::path::PathBuf;

use selene_core::{IStr, LabelSet, PropertyMap, Value};
use selene_graph::{SeleneGraph, SharedGraph};

use zeroize::Zeroize;

use self::crypto::{DataKey, MasterKey};
use self::error::VaultError;

/// Key source type stored in the vault header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KeySource {
    Raw = 0,
    Passphrase = 1,
}

/// Handle to the secure vault — holds the in-memory graph and persistence state.
pub struct VaultHandle {
    /// The vault graph, accessible via the same SharedGraph API as the main graph.
    pub graph: SharedGraph,
    /// Persistence state (behind Arc for shared access from flush operations).
    persist: parking_lot::Mutex<VaultPersist>,
}

/// Vault persistence state — encryption keys and file path.
struct VaultPersist {
    path: PathBuf,
    dek: DataKey,
    key_source: KeySource,
    argon2_salt: [u8; 16],
}

impl VaultHandle {
    /// Open an existing vault or create a new one with default admin principal.
    pub fn open_or_create(
        path: PathBuf,
        master: &MasterKey,
        key_source: KeySource,
        argon2_salt: [u8; 16],
    ) -> Result<Self, VaultError> {
        if path.exists() {
            let (graph, dek, ks, salt) = storage::read_vault(&path, master)?;
            tracing::info!(nodes = graph.node_count(), "secure vault opened");
            Ok(Self {
                graph: SharedGraph::new(graph),
                persist: parking_lot::Mutex::new(VaultPersist {
                    path,
                    dek,
                    key_source: ks,
                    argon2_salt: salt,
                }),
            })
        } else {
            let dek = DataKey::generate();
            let mut graph = SeleneGraph::new();
            seed_default_admin(&mut graph);
            tracing::info!("secure vault created with default admin principal");

            // Flush immediately to create the file
            storage::write_vault(&path, &graph, &dek, master, key_source, &argon2_salt)?;

            Ok(Self {
                graph: SharedGraph::new(graph),
                persist: parking_lot::Mutex::new(VaultPersist {
                    path,
                    dek,
                    key_source,
                    argon2_salt,
                }),
            })
        }
    }

    /// Flush the current vault state to disk.
    /// Call after mutations to persist changes.
    pub fn flush(&self, master: &MasterKey) -> Result<(), VaultError> {
        let snapshot = self.graph.load_snapshot();
        let persist = self.persist.lock();
        storage::write_vault(
            &persist.path,
            &snapshot,
            &persist.dek,
            master,
            persist.key_source,
            &persist.argon2_salt,
        )
    }

    /// Rotate the master key (re-wraps DEK with new key, payload unchanged).
    pub fn rotate_master_key(
        &self,
        _old_master: &MasterKey,
        new_master: &MasterKey,
    ) -> Result<(), VaultError> {
        self.flush(new_master)
    }

    /// Rotate the DEK (generates new DEK, re-encrypts payload).
    pub fn rotate_data_key(&self, master: &MasterKey) -> Result<(), VaultError> {
        let mut persist = self.persist.lock();
        persist.dek = DataKey::generate();
        let snapshot = self.graph.load_snapshot();
        storage::write_vault(
            &persist.path,
            &snapshot,
            &persist.dek,
            master,
            persist.key_source,
            &persist.argon2_salt,
        )
    }

    /// Get the vault file path.
    pub fn path(&self) -> PathBuf {
        self.persist.lock().path.clone()
    }

    /// Get node count in the vault graph.
    pub fn node_count(&self) -> usize {
        self.graph.load_snapshot().node_count()
    }
}

/// Seed the default admin principal in a fresh vault.
fn seed_default_admin(graph: &mut SeleneGraph) {
    let mut m = graph.mutate();
    m.create_node(
        LabelSet::from_strs(&["principal"]),
        PropertyMap::from_pairs(vec![
            (IStr::new("identity"), Value::str("admin")),
            (IStr::new("role"), Value::str("admin")),
        ]),
    )
    .unwrap();
    m.commit(0).unwrap();
}

/// Resolve the master key from config sources.
/// Priority: env passphrase > env key file > config key file > dev key.
///
/// `vault_path` should point to an existing vault file (if any). When using
/// passphrase-derived keys, the Argon2 salt is read from the existing vault
/// header to ensure the same key is derived on reopens. For new vaults,
/// a fresh salt is generated.
///
/// `env_passphrase` is the value of `SELENE_VAULT_PASSPHRASE`, read and cleared
/// from the process environment in `main()` before any threads are spawned.
/// This prevents the passphrase from leaking via `/proc/PID/environ`.
pub fn resolve_master_key(
    key_file: Option<&std::path::Path>,
    dev_mode: bool,
    vault_path: Option<&std::path::Path>,
    env_passphrase: Option<String>,
) -> Result<(MasterKey, KeySource, [u8; 16]), VaultError> {
    // 1. Check pre-read SELENE_VAULT_PASSPHRASE (cleared from env in main())
    if let Some(mut passphrase) = env_passphrase {
        // Reuse existing vault's salt to derive the same key; generate fresh for new vaults
        let salt = match vault_path {
            Some(p) if p.exists() => storage::read_vault_salt(p)?,
            _ => crypto::generate_salt(),
        };
        let mk = MasterKey::from_passphrase(&passphrase, &salt)?;
        passphrase.zeroize();
        return Ok((mk, KeySource::Passphrase, salt));
    }

    // 2. Check SELENE_VAULT_KEY_FILE env var or config key file
    let key_path = std::env::var("SELENE_VAULT_KEY_FILE")
        .ok()
        .map(PathBuf::from)
        .or_else(|| key_file.map(|p| p.to_path_buf()));

    if let Some(path) = key_path {
        let mut contents = std::fs::read_to_string(&path).map_err(|e| {
            VaultError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("master key file {}: {e}", path.display()),
            ))
        })?;
        let key_bytes = parse_key_file(contents.trim())?;
        // Zeroize file contents before dropping
        contents.as_mut_str().zeroize();
        return Ok((MasterKey::from_bytes(key_bytes), KeySource::Raw, [0u8; 16]));
    }

    // 3. Dev mode fallback
    if dev_mode {
        tracing::warn!("vault using dev key (all zeros) — NOT FOR PRODUCTION");
        return Ok((MasterKey::dev_key(), KeySource::Raw, [0u8; 16]));
    }

    Err(VaultError::NotAvailable(
        "no master key configured (set SELENE_VAULT_KEY_FILE, SELENE_VAULT_PASSPHRASE, or vault.master_key_file in config)".into(),
    ))
}

/// Parse a key file — supports base64-encoded or raw hex.
fn parse_key_file(content: &str) -> Result<[u8; 32], VaultError> {
    // Try base64 first (44 chars for 32 bytes)
    if let Some(b64) = content.strip_prefix("base64:") {
        return decode_base64(b64);
    }

    // Try raw base64 (no prefix)
    if content.len() == 44
        && content
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
    {
        return decode_base64(content);
    }

    // Try hex (64 chars for 32 bytes)
    if content.len() == 64 && content.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut key = [0u8; 32];
        for i in 0..32 {
            key[i] = u8::from_str_radix(&content[i * 2..i * 2 + 2], 16)
                .map_err(|_| VaultError::InvalidFormat("invalid hex in key file".into()))?;
        }
        return Ok(key);
    }

    Err(VaultError::InvalidFormat(
        "key file must be base64 (44 chars) or hex (64 chars)".into(),
    ))
}

fn decode_base64(input: &str) -> Result<[u8; 32], VaultError> {
    // Simple base64 decoder (no external dependency)
    let table: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input = input.trim_end_matches('=');
    let mut bytes = Vec::with_capacity(32);

    let mut buf = 0u32;
    let mut bits = 0u32;
    for ch in input.bytes() {
        let val = table
            .iter()
            .position(|&b| b == ch)
            .ok_or_else(|| VaultError::InvalidFormat("invalid base64 character".into()))?;
        buf = (buf << 6) | val as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }

    if bytes.len() != 32 {
        return Err(VaultError::InvalidFormat(format!(
            "decoded key is {} bytes, expected 32",
            bytes.len()
        )));
    }

    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

/// Encode 32 bytes as base64 for key file output.
pub fn encode_base64(data: &[u8; 32]) -> String {
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(44);
    let mut i = 0;
    while i < 30 {
        let n = u32::from(data[i]) << 16 | u32::from(data[i + 1]) << 8 | u32::from(data[i + 2]);
        result.push(table[((n >> 18) & 0x3F) as usize] as char);
        result.push(table[((n >> 12) & 0x3F) as usize] as char);
        result.push(table[((n >> 6) & 0x3F) as usize] as char);
        result.push(table[(n & 0x3F) as usize] as char);
        i += 3;
    }
    // Last 2 bytes
    let n = u32::from(data[30]) << 16 | u32::from(data[31]) << 8;
    result.push(table[((n >> 18) & 0x3F) as usize] as char);
    result.push(table[((n >> 12) & 0x3F) as usize] as char);
    result.push(table[((n >> 6) & 0x3F) as usize] as char);
    result.push('=');
    result
}

// ── Service wrapper ──────────────────────────────────────────────────

/// Vault as a registered service in the ServiceRegistry.
/// Bundles the VaultHandle and MasterKey together — they're always created
/// and used as a pair.
pub struct VaultService {
    pub handle: std::sync::Arc<VaultHandle>,
    pub master_key: std::sync::Arc<crypto::MasterKey>,
}

impl VaultService {
    pub fn new(
        handle: std::sync::Arc<VaultHandle>,
        master_key: std::sync::Arc<crypto::MasterKey>,
    ) -> Self {
        Self { handle, master_key }
    }
}

impl crate::service_registry::Service for VaultService {
    fn name(&self) -> &'static str {
        "vault"
    }

    fn health(&self) -> crate::service_registry::ServiceHealth {
        crate::service_registry::ServiceHealth::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vault_create_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let vault_path = dir.path().join("secure.vault");
        let master = MasterKey::dev_key();

        // Create
        let handle =
            VaultHandle::open_or_create(vault_path.clone(), &master, KeySource::Raw, [0u8; 16])
                .unwrap();
        assert_eq!(handle.node_count(), 1); // default admin

        // Mutate
        selene_gql::MutationBuilder::new("INSERT (:api_key {token: 'secret-456'})")
            .execute(&handle.graph)
            .unwrap();
        handle.flush(&master).unwrap();
        assert_eq!(handle.node_count(), 2);

        drop(handle);

        // Reopen
        let handle2 =
            VaultHandle::open_or_create(vault_path, &master, KeySource::Raw, [0u8; 16]).unwrap();
        assert_eq!(handle2.node_count(), 2);
    }

    #[test]
    fn vault_key_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let vault_path = dir.path().join("secure.vault");
        let old_master = MasterKey::from_bytes([1u8; 32]);
        let new_master = MasterKey::from_bytes([2u8; 32]);

        let handle =
            VaultHandle::open_or_create(vault_path.clone(), &old_master, KeySource::Raw, [0u8; 16])
                .unwrap();

        // Rotate master key
        handle.rotate_master_key(&old_master, &new_master).unwrap();

        drop(handle);

        // Old key should fail
        let result =
            VaultHandle::open_or_create(vault_path.clone(), &old_master, KeySource::Raw, [0u8; 16]);
        assert!(result.is_err());

        // New key should work
        let handle2 =
            VaultHandle::open_or_create(vault_path, &new_master, KeySource::Raw, [0u8; 16])
                .unwrap();
        assert_eq!(handle2.node_count(), 1);
    }

    #[test]
    fn base64_round_trip() {
        let key = [42u8; 32];
        let encoded = encode_base64(&key);
        let decoded = decode_base64(&encoded).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn hex_key_parse() {
        let hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        let key = parse_key_file(hex).unwrap();
        assert_eq!(key[0], 0x01);
        assert_eq!(key[31], 0x20);
    }

    #[test]
    #[allow(unsafe_code)]
    fn resolve_dev_key() {
        // Clear env vars that might interfere
        // SAFETY: test-only, single-threaded test context
        unsafe {
            std::env::remove_var("SELENE_VAULT_PASSPHRASE");
            std::env::remove_var("SELENE_VAULT_KEY_FILE");
        }

        let (mk, ks, _salt) = resolve_master_key(None, true, None, None).unwrap();
        assert!(matches!(ks, KeySource::Raw));
        assert_eq!(mk.as_bytes(), &[0u8; 32]);
    }

    #[test]
    #[allow(unsafe_code)]
    fn resolve_no_key_fails() {
        // SAFETY: test-only, single-threaded test context
        unsafe {
            std::env::remove_var("SELENE_VAULT_PASSPHRASE");
            std::env::remove_var("SELENE_VAULT_KEY_FILE");
        }

        let result = resolve_master_key(None, false, None, None);
        assert!(result.is_err());
    }
}
