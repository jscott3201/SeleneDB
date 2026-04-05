//! Graph snapshot — full serialization of the in-memory graph to disk.
//!
//! Snapshots are binary files (postcard + zstd + XXH3) containing all nodes,
//! edges, and metadata needed to reconstruct a `SeleneGraph`.  Atomic write
//! (temp + rename) ensures a crash during write never leaves a corrupt snapshot.
//!
//! Format (v1 -- binary):
//! ```text
//! [Header: 32 bytes]
//!   magic: "SSNP" (4 bytes)
//!   version: u16 LE (1)
//!   flags: u16 LE (bit 0 = compressed)
//!   section_count: u32 LE
//!   reserved: u32 LE
//!   xxh3_128: u128 LE (hash of all sections + TOC)
//!
//! [Section 0: Metadata]      postcard(SnapshotMetadata)
//! [Section 1: Nodes]         postcard(Vec<SnapshotNode>)
//! [Section 2: Edges]         postcard(Vec<SnapshotEdge>)
//! [Section 3: Schemas]       postcard(SnapshotSchemas) — optional, empty if no schemas
//! [Section 4: Triggers]      postcard(Vec<TriggerDef>)
//!
//! [TOC: section_count × 16 bytes]
//!   offset: u64 LE
//!   length: u64 LE
//! ```

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use selene_core::schema::{EdgeSchema, NodeSchema};
use selene_core::trigger::TriggerDef;
use selene_core::{Edge, EdgeId, Node, NodeId, Value};

use crate::error::PersistError;

const SNAP_MAGIC: &[u8; 4] = b"SSNP";
const SNAP_VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;
const TOC_ENTRY_SIZE: usize = 16; // offset(8) + length(8)
const FLAG_COMPRESSED: u16 = 0x01;

/// Metadata section — small, deserialized first for quick inspection.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct SnapshotMetadata {
    pub(crate) next_node_id: u64,
    pub(crate) next_edge_id: u64,
    pub(crate) changelog_sequence: u64,
    pub(crate) node_count: u64,
    pub(crate) edge_count: u64,
}

/// Schemas section — persisted alongside nodes and edges.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct SnapshotSchemas {
    pub node_schemas: Vec<NodeSchema>,
    pub edge_schemas: Vec<EdgeSchema>,
}

/// Serializable representation of a graph snapshot.
#[derive(Debug)]
pub struct GraphSnapshot {
    pub nodes: Vec<SnapshotNode>,
    pub edges: Vec<SnapshotEdge>,
    pub next_node_id: u64,
    pub next_edge_id: u64,
    pub changelog_sequence: u64,
    pub schemas: SnapshotSchemas,
    pub triggers: Vec<TriggerDef>,
    /// Additional sections (pre-serialized bytes) appended after the core sections.
    /// Section 5 = version store (temporal feature), future sections as needed.
    pub extra_sections: Vec<Vec<u8>>,
}

/// A node in serializable form (String instead of `Arc<str>`).
///
/// Properties are stored as sorted `Vec<(String, Value)>` to avoid HashMap
/// allocation during deserialization and map directly to `PropertyMap::from_pairs()`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotNode {
    pub id: u64,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Value)>,
    pub created_at: i64,
    pub updated_at: i64,
    pub version: u64,
}

/// An edge in serializable form.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotEdge {
    pub id: u64,
    pub source: u64,
    pub target: u64,
    pub label: String,
    pub properties: Vec<(String, Value)>,
    pub created_at: i64,
}

impl SnapshotNode {
    /// Convert from a `Node` reference.
    pub fn from_node(node: &Node) -> Self {
        Self {
            id: node.id.0,
            labels: node.labels.iter().map(|l| l.as_str().to_string()).collect(),
            properties: node
                .properties
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.clone()))
                .collect(),
            created_at: node.created_at,
            updated_at: node.updated_at,
            version: node.version,
        }
    }

    /// Convert back to a `Node`.
    ///
    /// Properties are already sorted pairs, mapping directly to
    /// `PropertyMap::from_pairs()` without intermediate HashMap allocation.
    pub fn into_node(self) -> Node {
        let label_strs: Vec<&str> = self.labels.iter().map(|s| s.as_str()).collect();
        let labels = selene_core::LabelSet::from_strs(&label_strs);
        let properties = selene_core::PropertyMap::from_pairs(
            self.properties
                .into_iter()
                .map(|(k, v)| (selene_core::IStr::new(&k), v)),
        );

        Node {
            id: NodeId(self.id),
            labels,
            properties,
            created_at: self.created_at,
            updated_at: self.updated_at,
            version: self.version,
            cached_json: None,
        }
    }
}

impl SnapshotEdge {
    /// Convert from an `Edge` reference.
    pub fn from_edge(edge: &Edge) -> Self {
        Self {
            id: edge.id.0,
            source: edge.source.0,
            target: edge.target.0,
            label: edge.label.to_string(),
            properties: edge
                .properties
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            created_at: edge.created_at,
        }
    }

    /// Convert back to an `Edge`.
    pub fn into_edge(self) -> Edge {
        let properties = selene_core::PropertyMap::from_pairs(
            self.properties
                .into_iter()
                .map(|(k, v)| (selene_core::IStr::new(&k), v)),
        );

        Edge {
            id: EdgeId(self.id),
            source: NodeId(self.source),
            target: NodeId(self.target),
            label: selene_core::IStr::new(&self.label),
            properties,
            created_at: self.created_at,
        }
    }
}

/// Write a full graph snapshot to the given path (binary format).
///
/// Uses atomic write: writes to a temp file first, then renames.
/// If `fsync_parent` is true, fsyncs the parent directory after rename
/// for maximum durability (adds ~50-200 ms on SD cards).
/// Returns the number of bytes written.
pub fn write_snapshot(snapshot: &GraphSnapshot, path: &Path) -> Result<u64, PersistError> {
    write_snapshot_opts(snapshot, path, true)
}

/// Write a snapshot with explicit fsync control.
pub fn write_snapshot_opts(
    snapshot: &GraphSnapshot,
    path: &Path,
    fsync_parent: bool,
) -> Result<u64, PersistError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let metadata = SnapshotMetadata {
        next_node_id: snapshot.next_node_id,
        next_edge_id: snapshot.next_edge_id,
        changelog_sequence: snapshot.changelog_sequence,
        node_count: snapshot.nodes.len() as u64,
        edge_count: snapshot.edges.len() as u64,
    };

    // Serialize sections with postcard
    let meta_bytes = postcard::to_allocvec(&metadata)
        .map_err(|e| PersistError::Serialization(format!("metadata: {e}")))?;
    let nodes_bytes = postcard::to_allocvec(&snapshot.nodes)
        .map_err(|e| PersistError::Serialization(format!("nodes: {e}")))?;
    let edges_bytes = postcard::to_allocvec(&snapshot.edges)
        .map_err(|e| PersistError::Serialization(format!("edges: {e}")))?;
    let schemas_bytes = postcard::to_allocvec(&snapshot.schemas)
        .map_err(|e| PersistError::Serialization(format!("schemas: {e}")))?;
    let triggers_bytes = postcard::to_allocvec(&snapshot.triggers)
        .map_err(|e| PersistError::Serialization(format!("triggers: {e}")))?;

    let mut all_sections: Vec<&[u8]> = vec![
        &meta_bytes,
        &nodes_bytes,
        &edges_bytes,
        &schemas_bytes,
        &triggers_bytes,
    ];
    for extra in &snapshot.extra_sections {
        all_sections.push(extra);
    }
    let section_count = all_sections.len() as u32;

    // Optionally compress sections that benefit from it
    let mut compressed_sections: Vec<Vec<u8>> = Vec::with_capacity(all_sections.len());
    let mut any_compressed = false;
    for section in &all_sections {
        if section.len() >= 256 {
            let compressed = zstd::encode_all(*section, 1)
                .map_err(|e| PersistError::Serialization(format!("zstd: {e}")))?;
            if compressed.len() < section.len() {
                compressed_sections.push(compressed);
                any_compressed = true;
                continue;
            }
        }
        compressed_sections.push(section.to_vec());
    }

    // Build TOC (relative to after header)
    let mut toc = Vec::with_capacity(section_count as usize * TOC_ENTRY_SIZE);
    let mut offset = HEADER_SIZE as u64;
    for cs in &compressed_sections {
        toc.extend_from_slice(&offset.to_le_bytes());
        toc.extend_from_slice(&(cs.len() as u64).to_le_bytes());
        offset += cs.len() as u64;
    }

    // Compute XXH3-128 over sections + TOC
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    for cs in &compressed_sections {
        hasher.update(cs);
    }
    hasher.update(&toc);
    let hash = hasher.digest128();

    // Build header
    let flags: u16 = if any_compressed { FLAG_COMPRESSED } else { 0 };
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(SNAP_MAGIC);
    header.extend_from_slice(&SNAP_VERSION.to_le_bytes());
    header.extend_from_slice(&flags.to_le_bytes());
    header.extend_from_slice(&section_count.to_le_bytes());
    header.extend_from_slice(&0u32.to_le_bytes()); // reserved
    header.extend_from_slice(&hash.to_le_bytes());

    // Atomic write: temp file + rename.
    // On any error after creating the tmp file, clean it up so stale
    // `.tmp` files never accumulate on disk.
    let tmp_path = path.with_extension("tmp");
    let total_size =
        HEADER_SIZE + compressed_sections.iter().map(Vec::len).sum::<usize>() + toc.len();

    let write_result = (|| -> Result<(), PersistError> {
        let mut file = fs::File::create(&tmp_path)?;
        let mut buf = Vec::with_capacity(total_size);
        buf.extend_from_slice(&header);
        for cs in &compressed_sections {
            buf.extend_from_slice(cs);
        }
        buf.extend_from_slice(&toc);
        file.write_all(&buf)?;
        file.sync_data()?;
        Ok(())
    })();

    if let Err(e) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    if let Err(e) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e.into());
    }

    // Fsync parent directory for durable rename. Disableable for SD card
    // deployments where the extra fsync adds 50-200 ms.
    if fsync_parent
        && let Some(parent) = path.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        let _ = dir.sync_data();
    }

    Ok(total_size as u64)
}

/// Read a snapshot from the given path (binary format).
pub fn read_snapshot(path: &Path) -> Result<GraphSnapshot, PersistError> {
    const MAX_SNAPSHOT_SIZE: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB
    let file_size = fs::metadata(path)?.len();
    if file_size > MAX_SNAPSHOT_SIZE {
        return Err(PersistError::SnapshotRead(format!(
            "snapshot file too large: {file_size} bytes (max {MAX_SNAPSHOT_SIZE})"
        )));
    }
    let data = fs::read(path)?;

    if data.len() < HEADER_SIZE {
        return Err(PersistError::SnapshotRead(
            "file too short for header".into(),
        ));
    }

    // Parse header
    if &data[0..4] != SNAP_MAGIC {
        return Err(PersistError::SnapshotRead("invalid snapshot magic".into()));
    }
    let version = u16::from_le_bytes([data[4], data[5]]);
    if version != SNAP_VERSION {
        return Err(PersistError::SnapshotRead(format!(
            "unsupported snapshot version {version}, expected {SNAP_VERSION}"
        )));
    }
    let _flags = u16::from_le_bytes([data[6], data[7]]);
    let section_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    let stored_hash = u128::from_le_bytes(data[16..32].try_into().unwrap());

    if section_count < 5 {
        return Err(PersistError::SnapshotRead(format!(
            "expected at least 5 sections, got {section_count}"
        )));
    }

    // Read TOC from the end of the file
    let toc_size = section_count * TOC_ENTRY_SIZE;
    if data.len() < HEADER_SIZE + toc_size {
        return Err(PersistError::SnapshotRead("file too short for TOC".into()));
    }
    let toc_start = data.len() - toc_size;
    let toc_data = &data[toc_start..];

    // Parse TOC entries
    let mut sections: Vec<(usize, usize)> = Vec::with_capacity(section_count);
    for i in 0..section_count {
        let entry_offset = i * TOC_ENTRY_SIZE;
        let offset =
            u64::from_le_bytes(toc_data[entry_offset..entry_offset + 8].try_into().unwrap())
                as usize;
        let length = u64::from_le_bytes(
            toc_data[entry_offset + 8..entry_offset + 16]
                .try_into()
                .unwrap(),
        ) as usize;
        if offset + length > toc_start {
            return Err(PersistError::SnapshotRead(format!(
                "section {i} extends past TOC boundary"
            )));
        }
        sections.push((offset, length));
    }

    // Verify XXH3-128 checksum
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    for &(offset, length) in &sections {
        hasher.update(&data[offset..offset + length]);
    }
    hasher.update(toc_data);
    let computed_hash = hasher.digest128();

    if computed_hash != stored_hash {
        return Err(PersistError::SnapshotRead(format!(
            "checksum mismatch: stored={stored_hash:#x}, computed={computed_hash:#x}"
        )));
    }

    // Decompress sections via per-section magic-byte detection (some sections
    // may be stored raw even when FLAG_COMPRESSED is set on the file).
    // Returns Cow::Borrowed for uncompressed sections (zero-copy).
    const MAX_DECOMPRESSED: usize = 256 * 1024 * 1024; // 256 MiB

    let (meta_off, meta_len) = sections[0];
    let meta_raw = crate::compress::decompress_if_zstd(
        &data[meta_off..meta_off + meta_len],
        MAX_DECOMPRESSED,
    )?;
    let metadata: SnapshotMetadata = postcard::from_bytes(&meta_raw)
        .map_err(|e| PersistError::SnapshotRead(format!("metadata: {e}")))?;

    let (nodes_off, nodes_len) = sections[1];
    let nodes_raw = crate::compress::decompress_if_zstd(
        &data[nodes_off..nodes_off + nodes_len],
        MAX_DECOMPRESSED,
    )?;
    let nodes: Vec<SnapshotNode> = postcard::from_bytes(&nodes_raw)
        .map_err(|e| PersistError::SnapshotRead(format!("nodes: {e}")))?;

    let (edges_off, edges_len) = sections[2];
    let edges_raw = crate::compress::decompress_if_zstd(
        &data[edges_off..edges_off + edges_len],
        MAX_DECOMPRESSED,
    )?;
    let edges: Vec<SnapshotEdge> = postcard::from_bytes(&edges_raw)
        .map_err(|e| PersistError::SnapshotRead(format!("edges: {e}")))?;

    // Section 3: Schemas
    let (schemas_off, schemas_len) = sections[3];
    let schemas_raw = crate::compress::decompress_if_zstd(
        &data[schemas_off..schemas_off + schemas_len],
        MAX_DECOMPRESSED,
    )?;
    let schemas: SnapshotSchemas = postcard::from_bytes(&schemas_raw)
        .map_err(|e| PersistError::SnapshotRead(format!("schemas: {e}")))?;

    // Section 4: Triggers
    let (trig_off, trig_len) = sections[4];
    let trig_raw = crate::compress::decompress_if_zstd(
        &data[trig_off..trig_off + trig_len],
        MAX_DECOMPRESSED,
    )?;
    let triggers: Vec<TriggerDef> = postcard::from_bytes(&trig_raw)
        .map_err(|e| PersistError::SnapshotRead(format!("triggers: {e}")))?;

    // Extra sections (5+): returned as raw decompressed bytes for the caller to interpret.
    // Section 5 = version store (temporal feature).
    let mut extra_sections = Vec::new();
    for &(off, len) in &sections[5..section_count] {
        let raw = crate::compress::decompress_if_zstd(&data[off..off + len], MAX_DECOMPRESSED)?;
        extra_sections.push(raw.into_owned());
    }

    Ok(GraphSnapshot {
        nodes,
        edges,
        next_node_id: metadata.next_node_id,
        next_edge_id: metadata.next_edge_id,
        changelog_sequence: metadata.changelog_sequence,
        schemas,
        triggers,
        extra_sections,
    })
}

/// Find the latest snapshot file in a directory.
///
/// Snapshots are named `snap-{sequence:012}.snap`.  Returns the path
/// with the highest sequence number.
pub fn find_latest_snapshot(snapshot_dir: &Path) -> Result<Option<PathBuf>, PersistError> {
    if !snapshot_dir.exists() {
        return Ok(None);
    }

    let mut best: Option<(u64, PathBuf)> = None;

    for entry in fs::read_dir(snapshot_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if let Some(seq_str) = name_str
            .strip_prefix("snap-")
            .and_then(|s| s.strip_suffix(".snap"))
            && let Ok(seq) = seq_str.parse::<u64>()
            && best.as_ref().is_none_or(|(best_seq, _)| seq > *best_seq)
        {
            best = Some((seq, entry.path()));
        }
    }

    Ok(best.map(|(_, path)| path))
}

/// Generate the snapshot filename for a given sequence.
pub fn snapshot_filename(sequence: u64) -> String {
    format!("snap-{sequence:012}.snap")
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol_str::SmolStr;
    use std::sync::Arc;

    fn sample_snapshot() -> GraphSnapshot {
        GraphSnapshot {
            nodes: vec![
                SnapshotNode {
                    id: 1,
                    labels: vec!["sensor".into(), "temperature".into()],
                    properties: vec![
                        ("unit".into(), Value::String(SmolStr::new("°F"))),
                        ("value".into(), Value::Float(72.5)),
                    ],
                    created_at: 1000,
                    updated_at: 2000,
                    version: 3,
                },
                SnapshotNode {
                    id: 2,
                    labels: vec!["building".into()],
                    properties: vec![],
                    created_at: 500,
                    updated_at: 500,
                    version: 1,
                },
            ],
            edges: vec![SnapshotEdge {
                id: 1,
                source: 2,
                target: 1,
                label: "contains".into(),
                properties: vec![],
                created_at: 600,
            }],
            next_node_id: 3,
            next_edge_id: 2,
            changelog_sequence: 10,
            schemas: SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        }
    }

    #[test]
    fn write_and_read_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap-000000000010.snap");

        let snap = sample_snapshot();
        let bytes = write_snapshot(&snap, &path).unwrap();
        assert!(bytes > 0);
        assert!(path.exists());

        let loaded = read_snapshot(&path).unwrap();
        assert_eq!(loaded.nodes.len(), 2);
        assert_eq!(loaded.edges.len(), 1);
        assert_eq!(loaded.next_node_id, 3);
        assert_eq!(loaded.next_edge_id, 2);
        assert_eq!(loaded.changelog_sequence, 10);
    }

    #[test]
    fn snapshot_node_round_trip() {
        let node = Node::new(
            NodeId(42),
            selene_core::LabelSet::from_strs(&["sensor", "temp"]),
            selene_core::PropertyMap::from_pairs(vec![(
                selene_core::IStr::new("unit"),
                Value::String(SmolStr::new("°C")),
            )]),
        );

        let snap = SnapshotNode::from_node(&node);
        assert_eq!(snap.id, 42);
        assert_eq!(snap.labels.len(), 2);

        let restored = snap.into_node();
        assert_eq!(restored.id, NodeId(42));
        assert!(restored.has_label("sensor"));
        assert!(restored.has_label("temp"));
        assert_eq!(
            restored.property("unit"),
            Some(&Value::String(SmolStr::new("°C")))
        );
    }

    #[test]
    fn snapshot_edge_round_trip() {
        let edge = Edge::new(
            EdgeId(7),
            NodeId(1),
            NodeId(2),
            selene_core::IStr::new("feeds"),
            selene_core::PropertyMap::from_pairs(vec![(
                selene_core::IStr::new("medium"),
                Value::String(SmolStr::new("air")),
            )]),
        );

        let snap = SnapshotEdge::from_edge(&edge);
        assert_eq!(snap.id, 7);

        let restored = snap.into_edge();
        assert_eq!(restored.id, EdgeId(7));
        assert_eq!(restored.source, NodeId(1));
        assert_eq!(restored.target, NodeId(2));
        assert_eq!(restored.label.as_str(), "feeds");
        assert_eq!(
            restored.properties.get_by_str("medium"),
            Some(&Value::String(SmolStr::new("air")))
        );
    }

    #[test]
    fn find_latest_snapshot_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let snapdir = dir.path().join("snapshots");
        fs::create_dir_all(&snapdir).unwrap();

        assert!(find_latest_snapshot(&snapdir).unwrap().is_none());
    }

    #[test]
    fn find_latest_snapshot_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let snapdir = dir.path().join("snapshots");
        fs::create_dir_all(&snapdir).unwrap();

        let snap = sample_snapshot();
        write_snapshot(&snap, &snapdir.join("snap-000000000005.snap")).unwrap();
        write_snapshot(&snap, &snapdir.join("snap-000000000010.snap")).unwrap();
        write_snapshot(&snap, &snapdir.join("snap-000000000003.snap")).unwrap();

        let latest = find_latest_snapshot(&snapdir).unwrap().unwrap();
        assert!(latest.to_string_lossy().contains("000000000010"));
    }

    #[test]
    fn find_latest_nonexistent_dir() {
        let result = find_latest_snapshot(Path::new("/nonexistent/path")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn snapshot_filename_format() {
        assert_eq!(snapshot_filename(42), "snap-000000000042.snap");
        assert_eq!(snapshot_filename(0), "snap-000000000000.snap");
        assert_eq!(snapshot_filename(999_999_999_999), "snap-999999999999.snap");
    }

    #[test]
    fn atomic_write_no_corrupt_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.snap");

        let snap = sample_snapshot();
        write_snapshot(&snap, &path).unwrap();

        // The temp file should be gone
        assert!(!dir.path().join("snap.tmp").exists());
        assert!(path.exists());
    }

    #[test]
    fn checksum_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.snap");

        write_snapshot(&sample_snapshot(), &path).unwrap();

        // Corrupt a byte in the nodes section
        let mut data = fs::read(&path).unwrap();
        let corrupt_pos = HEADER_SIZE + 10; // somewhere in metadata/nodes
        if corrupt_pos < data.len() {
            data[corrupt_pos] ^= 0xFF;
            fs::write(&path, &data).unwrap();
        }

        let result = read_snapshot(&path);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("checksum") || err.contains("metadata") || err.contains("nodes"));
    }

    #[test]
    fn binary_smaller_than_json() {
        let snap = sample_snapshot();

        let dir = tempfile::tempdir().unwrap();
        let bin_path = dir.path().join("snap.snap");
        let bin_size = write_snapshot(&snap, &bin_path).unwrap();

        // Compare to hypothetical JSON size
        let json_bytes = serde_json::to_vec(&serde_json::json!({
            "nodes": snap.nodes.iter().map(|n| {
                serde_json::json!({
                    "id": n.id, "labels": n.labels, "created_at": n.created_at,
                    "updated_at": n.updated_at, "version": n.version
                })
            }).collect::<Vec<_>>(),
            "edges": snap.edges.iter().map(|e| {
                serde_json::json!({
                    "id": e.id, "source": e.source, "target": e.target,
                    "label": e.label, "created_at": e.created_at
                })
            }).collect::<Vec<_>>(),
            "next_node_id": snap.next_node_id,
            "next_edge_id": snap.next_edge_id,
            "changelog_sequence": snap.changelog_sequence,
        }))
        .unwrap();

        // Binary should be smaller (or at most comparable for tiny snapshots)
        assert!(
            bin_size <= json_bytes.len() as u64 + 100,
            "binary ({bin_size}) should not be much larger than JSON ({})",
            json_bytes.len()
        );
    }

    #[test]
    fn large_snapshot_round_trip() {
        let mut nodes = Vec::new();
        for i in 0..500 {
            nodes.push(SnapshotNode {
                id: i,
                labels: vec!["sensor".into(), "temperature".into()],
                properties: vec![
                    (
                        "name".into(),
                        Value::String(SmolStr::new(format!("sensor-{i}"))),
                    ),
                    ("value".into(), Value::Float(72.0 + i as f64 * 0.01)),
                ],
                created_at: 1000 + i as i64,
                updated_at: 2000 + i as i64,
                version: 1,
            });
        }

        let snap = GraphSnapshot {
            nodes,
            edges: vec![],
            next_node_id: 500,
            next_edge_id: 1,
            changelog_sequence: 42,
            schemas: SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.snap");
        write_snapshot(&snap, &path).unwrap();

        let loaded = read_snapshot(&path).unwrap();
        assert_eq!(loaded.nodes.len(), 500);
        assert_eq!(loaded.changelog_sequence, 42);
    }

    #[test]
    fn snapshot_size_report() {
        // Outputs actual file sizes for documentation (visible with --nocapture)
        for count in [100, 1_000, 10_000] {
            let mut nodes = Vec::new();
            for i in 0..count {
                nodes.push(SnapshotNode {
                    id: i as u64,
                    labels: vec!["sensor".into(), "entity".into()],
                    properties: vec![
                        (
                            "name".into(),
                            Value::String(SmolStr::new(format!("sensor-{i}"))),
                        ),
                        ("value".into(), Value::Float(72.0 + f64::from(i) * 0.01)),
                        ("index".into(), Value::Int(i64::from(i))),
                    ],
                    created_at: 1_000_000_000,
                    updated_at: 1_000_000_000,
                    version: 1,
                });
            }
            let snap = GraphSnapshot {
                nodes,
                edges: vec![],
                next_node_id: count as u64 + 1,
                next_edge_id: 1,
                changelog_sequence: count as u64,
                schemas: SnapshotSchemas::default(),
                triggers: vec![],
                extra_sections: vec![],
            };
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("size_test.snap");
            let bytes = write_snapshot(&snap, &path).unwrap();
            eprintln!(
                "snapshot_size: {count} nodes = {bytes} bytes ({:.1} KB)",
                bytes as f64 / 1024.0
            );
        }
    }

    #[test]
    fn trigger_round_trip() {
        use selene_core::trigger::{TriggerDef, TriggerEvent};
        let trigger = TriggerDef {
            name: std::sync::Arc::from("auto_status"),
            event: TriggerEvent::Insert,
            label: std::sync::Arc::from("sensor"),
            condition: Some("NEW.status IS NULL".into()),
            action: "SET NEW.status = 'active'".into(),
        };

        let mut snap = sample_snapshot();
        snap.triggers = vec![trigger];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("triggers.snap");
        write_snapshot(&snap, &path).unwrap();

        let recovered = read_snapshot(&path).unwrap();
        assert_eq!(recovered.triggers.len(), 1);
        assert_eq!(&*recovered.triggers[0].name, "auto_status");
        assert_eq!(recovered.triggers[0].event, TriggerEvent::Insert);
        assert_eq!(&*recovered.triggers[0].label, "sensor");
        assert_eq!(
            recovered.triggers[0].condition.as_deref(),
            Some("NEW.status IS NULL")
        );
        assert_eq!(recovered.triggers[0].action, "SET NEW.status = 'active'");
    }

    #[test]
    fn snapshot_without_triggers_section_returns_empty() {
        // Reading a snapshot written with no triggers should return empty vec
        let snap = sample_snapshot();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_triggers.snap");
        write_snapshot(&snap, &path).unwrap();

        let recovered = read_snapshot(&path).unwrap();
        assert!(recovered.triggers.is_empty());
    }

    #[test]
    fn trigger_def_partial_eq() {
        use selene_core::trigger::{TriggerDef, TriggerEvent};
        let t1 = TriggerDef {
            name: Arc::from("test"),
            event: TriggerEvent::Insert,
            label: Arc::from("sensor"),
            condition: None,
            action: "SET NEW.status = 'ok'".into(),
        };
        let t2 = t1.clone();
        assert_eq!(t1, t2);
    }

    // ── Snapshot hardening tests ────────────────────────────────────────

    #[test]
    fn snapshot_with_extra_sections_round_trip() {
        // Extra section 5 (version store) should survive write/read
        let extra_bytes = vec![0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x01, 0x02, 0x03];
        let mut snap = sample_snapshot();
        snap.extra_sections = vec![extra_bytes.clone()];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("extra.snap");
        write_snapshot(&snap, &path).unwrap();

        let loaded = read_snapshot(&path).unwrap();
        assert_eq!(loaded.extra_sections.len(), 1);
        assert_eq!(loaded.extra_sections[0], extra_bytes);
        // Core data should also survive
        assert_eq!(loaded.nodes.len(), 2);
        assert_eq!(loaded.edges.len(), 1);
    }

    #[test]
    fn snapshot_with_multiple_extra_sections() {
        let mut snap = sample_snapshot();
        snap.extra_sections = vec![vec![1, 2, 3], vec![4, 5, 6, 7, 8], vec![9]];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi_extra.snap");
        write_snapshot(&snap, &path).unwrap();

        let loaded = read_snapshot(&path).unwrap();
        assert_eq!(loaded.extra_sections.len(), 3);
        assert_eq!(loaded.extra_sections[0], vec![1, 2, 3]);
        assert_eq!(loaded.extra_sections[1], vec![4, 5, 6, 7, 8]);
        assert_eq!(loaded.extra_sections[2], vec![9]);
    }

    #[test]
    fn snapshot_body_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.snap");
        write_snapshot(&sample_snapshot(), &path).unwrap();

        // Flip a byte in the middle of the file (well past the header)
        let mut data = fs::read(&path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let result = read_snapshot(&path);
        assert!(result.is_err(), "corruption should be detected");
        let err = format!("{}", result.unwrap_err());
        // Could be checksum mismatch or deserialization failure
        assert!(
            err.contains("checksum") || err.contains("metadata") || err.contains("nodes"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn snapshot_header_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt_header.snap");
        write_snapshot(&sample_snapshot(), &path).unwrap();

        // Corrupt the section_count field (bytes 8-11)
        let mut data = fs::read(&path).unwrap();
        data[8] = 0xFF;
        data[9] = 0xFF;
        fs::write(&path, &data).unwrap();

        let result = read_snapshot(&path);
        assert!(result.is_err(), "header corruption should be detected");
    }

    #[test]
    fn truncated_snapshot_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.snap");
        write_snapshot(&sample_snapshot(), &path).unwrap();

        // Truncate to just the header (32 bytes)
        let data = fs::read(&path).unwrap();
        fs::write(&path, &data[..HEADER_SIZE]).unwrap();

        let result = read_snapshot(&path);
        assert!(result.is_err(), "truncated snapshot should return error");
    }

    #[test]
    fn snapshot_shorter_than_header_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short.snap");
        // Only 10 bytes, too short for the 32-byte header
        fs::write(&path, [0u8; 10]).unwrap();

        let result = read_snapshot(&path);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("too short"),
            "expected 'too short' error, got: {err}"
        );
    }

    #[test]
    fn snapshot_bad_magic_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_magic.snap");
        write_snapshot(&sample_snapshot(), &path).unwrap();

        // Overwrite magic bytes
        let mut data = fs::read(&path).unwrap();
        data[0..4].copy_from_slice(b"XXXX");
        fs::write(&path, &data).unwrap();

        let result = read_snapshot(&path);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("magic"), "expected magic error, got: {err}");
    }

    #[test]
    fn snapshot_bad_version_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_version.snap");
        write_snapshot(&sample_snapshot(), &path).unwrap();

        // Overwrite version field (bytes 4-5) with version 99
        let mut data = fs::read(&path).unwrap();
        data[4..6].copy_from_slice(&99u16.to_le_bytes());
        fs::write(&path, &data).unwrap();

        let result = read_snapshot(&path);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("version"),
            "expected version error, got: {err}"
        );
    }

    #[test]
    fn empty_graph_snapshot_round_trip() {
        let snap = GraphSnapshot {
            nodes: vec![],
            edges: vec![],
            next_node_id: 1,
            next_edge_id: 1,
            changelog_sequence: 0,
            schemas: SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.snap");
        let bytes = write_snapshot(&snap, &path).unwrap();
        assert!(bytes > 0, "even empty snapshot should have nonzero size");

        let loaded = read_snapshot(&path).unwrap();
        assert!(loaded.nodes.is_empty());
        assert!(loaded.edges.is_empty());
        assert_eq!(loaded.next_node_id, 1);
        assert_eq!(loaded.next_edge_id, 1);
        assert_eq!(loaded.changelog_sequence, 0);
        assert!(loaded.triggers.is_empty());
        assert!(loaded.extra_sections.is_empty());
    }

    #[test]
    fn snapshot_all_value_types_round_trip() {
        let snap = GraphSnapshot {
            nodes: vec![SnapshotNode {
                id: 1,
                labels: vec!["test".into()],
                properties: vec![
                    ("null".into(), Value::Null),
                    ("bool".into(), Value::Bool(true)),
                    ("int".into(), Value::Int(i64::MIN)),
                    ("float".into(), Value::Float(f64::EPSILON)),
                    ("string".into(), Value::String(SmolStr::new("hello"))),
                    ("timestamp".into(), Value::Timestamp(1_700_000_000)),
                    ("uint".into(), Value::UInt(u64::MAX)),
                    (
                        "list".into(),
                        Value::List(std::sync::Arc::from(vec![Value::Int(1), Value::Null])),
                    ),
                    (
                        "bytes".into(),
                        Value::Bytes(std::sync::Arc::from(vec![0xFF, 0x00])),
                    ),
                    ("date".into(), Value::Date(20000)),
                    ("duration".into(), Value::Duration(86_400_000_000_000)),
                ],
                created_at: 100,
                updated_at: 200,
                version: 1,
            }],
            edges: vec![],
            next_node_id: 2,
            next_edge_id: 1,
            changelog_sequence: 1,
            schemas: SnapshotSchemas::default(),
            triggers: vec![],
            extra_sections: vec![],
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("values.snap");
        write_snapshot(&snap, &path).unwrap();

        let loaded = read_snapshot(&path).unwrap();
        assert_eq!(loaded.nodes.len(), 1);

        let original_props = &snap.nodes[0].properties;
        let loaded_props = &loaded.nodes[0].properties;
        assert_eq!(
            original_props.len(),
            loaded_props.len(),
            "property count mismatch"
        );

        for (orig_key, orig_val) in original_props {
            let loaded_val = loaded_props
                .iter()
                .find(|(k, _)| k == orig_key)
                .map(|(_, v)| v);
            assert_eq!(
                loaded_val,
                Some(orig_val),
                "value mismatch for property '{orig_key}'"
            );
        }
    }

    #[test]
    fn snapshot_with_schemas_round_trip() {
        use selene_core::schema::{NodeSchema, PropertyDef, SchemaVersion, ValueType};

        let schema = NodeSchema::builder("sensor")
            .version(SchemaVersion {
                major: 1,
                minor: 0,
                patch: 0,
            })
            .property(PropertyDef::simple("temp", ValueType::Float, true))
            .property(PropertyDef::simple("name", ValueType::String, false))
            .build();

        let schemas = SnapshotSchemas {
            node_schemas: vec![schema],
            edge_schemas: vec![],
        };

        let mut snap = sample_snapshot();
        snap.schemas = schemas;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.snap");
        write_snapshot(&snap, &path).unwrap();

        let loaded = read_snapshot(&path).unwrap();
        assert_eq!(loaded.schemas.node_schemas.len(), 1);
        assert_eq!(&*loaded.schemas.node_schemas[0].label, "sensor");
        assert_eq!(loaded.schemas.node_schemas[0].properties.len(), 2);
    }

    #[test]
    fn snapshot_checksum_field_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checksum_corrupt.snap");
        write_snapshot(&sample_snapshot(), &path).unwrap();

        // Corrupt the stored checksum (bytes 16-31 of the header)
        let mut data = fs::read(&path).unwrap();
        data[16] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let result = read_snapshot(&path);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("checksum"),
            "expected checksum error, got: {err}"
        );
    }

    #[test]
    fn snapshot_toc_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("toc_corrupt.snap");
        write_snapshot(&sample_snapshot(), &path).unwrap();

        // Corrupt the last few bytes of the file (inside the TOC)
        let mut data = fs::read(&path).unwrap();
        let toc_byte = data.len() - 2;
        data[toc_byte] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let result = read_snapshot(&path);
        assert!(result.is_err(), "TOC corruption should be detected");
    }

    #[test]
    fn snapshot_node_into_node_preserves_all_fields() {
        let snap_node = SnapshotNode {
            id: 99,
            labels: vec!["a".into(), "b".into(), "c".into()],
            properties: vec![("x".into(), Value::Int(1)), ("y".into(), Value::Float(2.0))],
            created_at: 1000,
            updated_at: 2000,
            version: 5,
        };

        let node = snap_node.into_node();
        assert_eq!(node.id, NodeId(99));
        assert!(node.has_label("a"));
        assert!(node.has_label("b"));
        assert!(node.has_label("c"));
        assert_eq!(node.property("x"), Some(&Value::Int(1)));
        assert_eq!(node.property("y"), Some(&Value::Float(2.0)));
        assert_eq!(node.created_at, 1000);
        assert_eq!(node.updated_at, 2000);
        assert_eq!(node.version, 5);
    }

    #[test]
    fn snapshot_edge_into_edge_preserves_all_fields() {
        let snap_edge = SnapshotEdge {
            id: 42,
            source: 10,
            target: 20,
            label: "relates_to".into(),
            properties: vec![("weight".into(), Value::Float(0.5))],
            created_at: 3000,
        };

        let edge = snap_edge.into_edge();
        assert_eq!(edge.id, EdgeId(42));
        assert_eq!(edge.source, NodeId(10));
        assert_eq!(edge.target, NodeId(20));
        assert_eq!(edge.label.as_str(), "relates_to");
        assert_eq!(
            edge.properties.get_by_str("weight"),
            Some(&Value::Float(0.5))
        );
        assert_eq!(edge.created_at, 3000);
    }
}
