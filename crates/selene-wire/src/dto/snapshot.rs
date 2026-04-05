//! Snapshot transfer DTOs for primary-to-replica graph snapshot streaming.

use serde::{Deserialize, Serialize};

/// Request: replica asks primary for a binary snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// Which graph to snapshot (empty = default graph).
    pub graph_name: String,
}

/// Response: a chunk of the binary snapshot stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotChunk {
    /// Index of this chunk (0-based).
    pub chunk_index: u32,
    /// Snapshot data for this chunk.
    pub data: Vec<u8>,
    /// True if this is the final chunk.
    pub is_last: bool,
    /// Changelog sequence at snapshot time. Only set on the final chunk.
    /// Replica uses this to know where to start changelog replay.
    pub snapshot_sequence: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};

    #[test]
    fn snapshot_request_round_trip() {
        let req = SnapshotRequest {
            graph_name: "default".into(),
        };
        let bytes = serialize_payload(&req, WireFlags::empty()).unwrap();
        let decoded: SnapshotRequest = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.graph_name, "default");
    }

    #[test]
    fn snapshot_chunk_round_trip() {
        let chunk = SnapshotChunk {
            chunk_index: 0,
            data: vec![1, 2, 3, 4],
            is_last: false,
            snapshot_sequence: None,
        };
        let bytes = serialize_payload(&chunk, WireFlags::empty()).unwrap();
        let decoded: SnapshotChunk = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.chunk_index, 0);
        assert!(!decoded.is_last);
        assert!(decoded.snapshot_sequence.is_none());
    }

    #[test]
    fn snapshot_final_chunk_with_sequence() {
        let chunk = SnapshotChunk {
            chunk_index: 5,
            data: vec![],
            is_last: true,
            snapshot_sequence: Some(42),
        };
        let bytes = serialize_payload(&chunk, WireFlags::empty()).unwrap();
        let decoded: SnapshotChunk = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert!(decoded.is_last);
        assert_eq!(decoded.snapshot_sequence, Some(42));
    }
}
