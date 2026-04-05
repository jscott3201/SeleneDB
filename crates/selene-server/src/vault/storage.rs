//! Vault file I/O — read and write encrypted `.vault` files.
//!
//! Format (v1):
//! ```text
//! Offset  Size    Field
//! ──────  ──────  ──────────────────────────────────────────
//! 0       4       Magic: "SVLT"
//! 4       2       Version: 1 (u16 LE)
//! 6       1       Key source type: 0=raw, 1=passphrase-derived
//! 7       1       Reserved (0x00)
//! 8       16      Argon2 salt (zero-filled for raw key)
//! 24      24      DEK nonce
//! 48      48      Encrypted DEK (32-byte key + 16-byte Poly1305 tag)
//! 96      8       Payload write timestamp (i64 LE, nanos since epoch)
//! 104     24      Payload nonce
//! 128     4       Payload length (u32 LE)
//! 132     N       Encrypted payload (postcard graph data + Poly1305 tag)
//! ```

use std::fs;
use std::io::Write;
use std::path::Path;

use selene_core::now_nanos;
use selene_graph::SeleneGraph;
use selene_persist::snapshot::{SnapshotEdge, SnapshotNode, SnapshotSchemas};
use zeroize::Zeroize;

use super::KeySource;
use super::crypto::{self, DataKey, MasterKey};
use super::error::VaultError;

const VAULT_MAGIC: &[u8; 4] = b"SVLT";
const VAULT_VERSION: u16 = 1;
const HEADER_SIZE: usize = 132;

/// Serializable vault payload — same structure as snapshot but simpler.
#[derive(serde::Serialize, serde::Deserialize)]
struct VaultPayload {
    nodes: Vec<SnapshotNode>,
    edges: Vec<SnapshotEdge>,
    next_node_id: u64,
    next_edge_id: u64,
    schemas: SnapshotSchemas,
}

/// Serialize a SeleneGraph to postcard bytes for vault encryption.
fn serialize_graph(graph: &SeleneGraph) -> Result<Vec<u8>, VaultError> {
    let nodes: Vec<SnapshotNode> = graph
        .all_node_ids()
        .filter_map(|id| graph.get_node(id))
        .map(|nr| SnapshotNode {
            id: nr.id.0,
            labels: nr.labels.iter().map(|l| l.as_str().to_string()).collect(),
            properties: nr
                .properties
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.clone()))
                .collect(),
            created_at: nr.created_at,
            updated_at: nr.updated_at,
            version: nr.version,
        })
        .collect();

    let edges: Vec<SnapshotEdge> = graph
        .all_edge_ids()
        .filter_map(|id| graph.get_edge(id))
        .map(|er| SnapshotEdge {
            id: er.id.0,
            source: er.source.0,
            target: er.target.0,
            label: er.label.as_str().to_string(),
            properties: er
                .properties
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.clone()))
                .collect(),
            created_at: er.created_at,
        })
        .collect();

    let schemas = SnapshotSchemas {
        node_schemas: graph.schema().all_node_schemas().cloned().collect(),
        edge_schemas: graph.schema().all_edge_schemas().cloned().collect(),
    };

    let payload = VaultPayload {
        nodes,
        edges,
        next_node_id: graph.next_node_id(),
        next_edge_id: graph.next_edge_id(),
        schemas,
    };

    postcard::to_allocvec(&payload).map_err(|e| VaultError::Serialization(format!("{e}")))
}

/// Deserialize a SeleneGraph from postcard bytes.
fn deserialize_graph(data: &[u8]) -> Result<SeleneGraph, VaultError> {
    let payload: VaultPayload =
        postcard::from_bytes(data).map_err(|e| VaultError::Serialization(format!("{e}")))?;

    let mut graph = SeleneGraph::new();

    let nodes: Vec<selene_core::Node> =
        payload.nodes.into_iter().map(|sn| sn.into_node()).collect();
    let edges: Vec<selene_core::Edge> =
        payload.edges.into_iter().map(|se| se.into_edge()).collect();

    graph.load_nodes(nodes);
    graph.load_edges(edges);
    graph
        .set_next_ids(payload.next_node_id, payload.next_edge_id)
        .map_err(|e| VaultError::Crypto(e.to_string()))?;

    if !payload.schemas.node_schemas.is_empty() || !payload.schemas.edge_schemas.is_empty() {
        graph
            .schema_mut()
            .import(payload.schemas.node_schemas, payload.schemas.edge_schemas);
    }

    Ok(graph)
}

/// Write a vault file to disk atomically (temp + rename).
pub fn write_vault(
    path: &Path,
    graph: &SeleneGraph,
    dek: &DataKey,
    master: &MasterKey,
    key_source: KeySource,
    argon2_salt: &[u8; 16],
) -> Result<(), VaultError> {
    // 1. Serialize graph
    let mut graph_bytes = serialize_graph(graph)?;

    // 2. Build AAD: magic || version || timestamp
    let timestamp = now_nanos();
    let mut aad = Vec::with_capacity(14);
    aad.extend_from_slice(VAULT_MAGIC);
    aad.extend_from_slice(&VAULT_VERSION.to_le_bytes());
    aad.extend_from_slice(&timestamp.to_le_bytes());

    // 3. Encrypt payload
    let (encrypted_payload, payload_nonce) = crypto::encrypt_payload(&graph_bytes, dek, &aad)?;
    graph_bytes.zeroize(); // H2: scrub plaintext graph data from memory

    // 4. Guard payload length against u32 overflow (M4)
    if encrypted_payload.len() > u32::MAX as usize {
        return Err(VaultError::Serialization(
            "encrypted payload exceeds 4 GiB limit".into(),
        ));
    }

    // 5. Wrap DEK with master key
    let (encrypted_dek, dek_nonce) = dek.wrap(master)?;

    // 6. Write to temp file (L2: append .tmp to full filename)
    let tmp_name = format!(
        "{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy()
    );
    let tmp_path = path.with_file_name(tmp_name);
    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(VAULT_MAGIC)?;
        file.write_all(&VAULT_VERSION.to_le_bytes())?;
        file.write_all(&[key_source as u8, 0u8])?;
        file.write_all(argon2_salt)?;
        file.write_all(&dek_nonce)?;
        file.write_all(&encrypted_dek)?;
        file.write_all(&timestamp.to_le_bytes())?;
        file.write_all(&payload_nonce)?;
        file.write_all(&(encrypted_payload.len() as u32).to_le_bytes())?;
        file.write_all(&encrypted_payload)?;
        file.sync_all()?;
    }

    // 7. Atomic rename
    fs::rename(&tmp_path, path)?;

    // 8. Restrict file permissions (M3: vault file should not be world-readable)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = fs::set_permissions(path, fs::Permissions::from_mode(0o600)) {
            tracing::warn!("failed to set vault file permissions to 0600: {e}");
        }
    }

    Ok(())
}

/// Read just the Argon2 salt from an existing vault file header.
/// Used to derive the correct passphrase-based key on reopens.
pub fn read_vault_salt(path: &Path) -> Result<[u8; 16], VaultError> {
    use std::io::Read;
    let mut f = fs::File::open(path)?;
    let mut header = [0u8; 24];
    f.read_exact(&mut header)
        .map_err(|_| VaultError::InvalidFormat("vault file too short for header".into()))?;
    if &header[0..4] != VAULT_MAGIC {
        return Err(VaultError::InvalidFormat("bad magic".into()));
    }
    Ok(header[8..24].try_into().unwrap())
}

/// Read and decrypt a vault file, returning the graph and DEK.
pub fn read_vault(
    path: &Path,
    master: &MasterKey,
) -> Result<(SeleneGraph, DataKey, KeySource, [u8; 16]), VaultError> {
    let data = fs::read(path)?;

    if data.len() < HEADER_SIZE {
        return Err(VaultError::InvalidFormat("file too short".into()));
    }

    // 1. Validate magic and version
    if &data[0..4] != VAULT_MAGIC {
        return Err(VaultError::InvalidFormat("bad magic".into()));
    }
    let version = u16::from_le_bytes([data[4], data[5]]);
    if version != VAULT_VERSION {
        return Err(VaultError::InvalidFormat(format!(
            "unsupported version {version}"
        )));
    }

    // 2. Parse header
    let key_source_byte = data[6];
    let key_source = match key_source_byte {
        0 => KeySource::Raw,
        1 => KeySource::Passphrase,
        _ => {
            return Err(VaultError::InvalidFormat(format!(
                "unknown key source {key_source_byte}"
            )));
        }
    };
    let argon2_salt: [u8; 16] = data[8..24].try_into().unwrap();
    let dek_nonce: [u8; 24] = data[24..48].try_into().unwrap();
    let encrypted_dek = &data[48..96];
    let timestamp = i64::from_le_bytes(data[96..104].try_into().unwrap());
    let payload_nonce: [u8; 24] = data[104..128].try_into().unwrap();
    let payload_len = u32::from_le_bytes(data[128..132].try_into().unwrap()) as usize;

    if data.len() < HEADER_SIZE + payload_len {
        return Err(VaultError::InvalidFormat("file truncated".into()));
    }
    let encrypted_payload = &data[132..132 + payload_len];

    // 3. Unwrap DEK
    let dek = DataKey::unwrap(encrypted_dek, &dek_nonce, master)?;

    // 4. Build AAD and decrypt payload
    let mut aad = Vec::with_capacity(14);
    aad.extend_from_slice(VAULT_MAGIC);
    aad.extend_from_slice(&version.to_le_bytes());
    aad.extend_from_slice(&timestamp.to_le_bytes());

    let graph_bytes = crypto::decrypt_payload(encrypted_payload, &payload_nonce, &dek, &aad)?;

    // 5. Deserialize graph
    let graph = deserialize_graph(&graph_bytes)?;

    Ok((graph, dek, key_source, argon2_salt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, PropertyMap, Value};
    use selene_graph::SeleneGraph;

    fn make_test_graph() -> SeleneGraph {
        let mut graph = SeleneGraph::new();
        let mut m = graph.mutate();
        m.create_node(
            LabelSet::from_strs(&["principal"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("identity"), Value::str("admin")),
                (IStr::new("role"), Value::str("Admin")),
            ]),
        )
        .unwrap();
        m.create_node(
            LabelSet::from_strs(&["api_key"]),
            PropertyMap::from_pairs(vec![(IStr::new("token"), Value::str("secret-123"))]),
        )
        .unwrap();
        m.commit(0).unwrap();
        graph
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let graph = make_test_graph();
        let bytes = serialize_graph(&graph).unwrap();
        let restored = deserialize_graph(&bytes).unwrap();

        assert_eq!(restored.node_count(), 2);
        assert_eq!(restored.next_node_id(), graph.next_node_id());
        assert_eq!(restored.next_edge_id(), graph.next_edge_id());
    }

    #[test]
    fn vault_write_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let vault_path = dir.path().join("test.vault");
        let master = MasterKey::dev_key();
        let dek = DataKey::generate();
        let graph = make_test_graph();
        let salt = [0u8; 16];

        write_vault(&vault_path, &graph, &dek, &master, KeySource::Raw, &salt).unwrap();
        assert!(vault_path.exists());

        let (restored, _dek, ks, _salt) = read_vault(&vault_path, &master).unwrap();
        assert_eq!(restored.node_count(), 2);
        assert!(matches!(ks, KeySource::Raw));
    }

    #[test]
    fn wrong_key_fails_to_read() {
        let dir = tempfile::tempdir().unwrap();
        let vault_path = dir.path().join("test.vault");
        let master = MasterKey::from_bytes([1u8; 32]);
        let dek = DataKey::generate();
        let graph = make_test_graph();
        let salt = [0u8; 16];

        write_vault(&vault_path, &graph, &dek, &master, KeySource::Raw, &salt).unwrap();

        let wrong_key = MasterKey::from_bytes([2u8; 32]);
        let result = read_vault(&vault_path, &wrong_key);
        assert!(result.is_err());
    }

    #[test]
    fn atomic_write_no_partial() {
        let dir = tempfile::tempdir().unwrap();
        let vault_path = dir.path().join("test.vault");
        let master = MasterKey::dev_key();
        let dek = DataKey::generate();
        let graph = make_test_graph();
        let salt = [0u8; 16];

        write_vault(&vault_path, &graph, &dek, &master, KeySource::Raw, &salt).unwrap();

        // Temp file should be gone
        assert!(!dir.path().join("test.vault.tmp").exists());
        assert!(vault_path.exists());
    }
}
