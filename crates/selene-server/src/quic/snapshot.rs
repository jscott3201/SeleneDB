//! Snapshot streaming handler — sends binary snapshot to replica over QUIC.

use std::sync::Arc;

use bytes::Bytes;
use quinn::SendStream;
use selene_persist::snapshot::{
    GraphSnapshot, SnapshotEdge, SnapshotNode, SnapshotSchemas, write_snapshot_opts,
};
use selene_wire::WireFlags;
use selene_wire::dto::snapshot::SnapshotChunk;
use selene_wire::frame::Frame;
use selene_wire::msg_type::MsgType;
use selene_wire::serialize::serialize_payload;

use crate::bootstrap::ServerState;

/// Maximum chunk size for snapshot transfer (64 KB).
const CHUNK_SIZE: usize = 64 * 1024;

/// Stream a binary snapshot to the replica.
///
/// 1. Capture changelog sequence FIRST (guarantees sequence <= snapshot state)
/// 2. Load ArcSwap snapshot (lock-free)
/// 3. Write snapshot to temp file, read back as bytes
/// 4. Stream in CHUNK_SIZE pieces
/// 5. Final chunk carries snapshot_sequence
pub async fn stream_snapshot(
    state: Arc<ServerState>,
    send: &mut SendStream,
    flags: WireFlags,
) -> anyhow::Result<()> {
    // Capture sequence BEFORE snapshot — guarantees sequence <= snapshot state.
    let sequence = state.persistence.changelog.lock().current_sequence();
    let snap = state.graph.load_snapshot();

    tracing::info!(
        nodes = snap.node_count(),
        edges = snap.edge_count(),
        sequence,
        "streaming snapshot to replica"
    );

    // Build GraphSnapshot — same pattern as tasks.rs::take_snapshot()
    let (raw_nodes, raw_edges, next_node, next_edge, schemas, triggers) = {
        let nodes: Vec<selene_core::Node> = snap
            .all_node_ids()
            .filter_map(|id| snap.get_node(id).map(|n| n.to_owned_node()))
            .collect();
        let edges: Vec<selene_core::Edge> = snap
            .all_edge_ids()
            .filter_map(|id| snap.get_edge(id).map(|e| e.to_owned_edge()))
            .collect();
        let (node_schemas, edge_schemas) = snap.schema().export();
        let triggers = snap.trigger_registry().to_vec();
        (
            nodes,
            edges,
            snap.next_node_id(),
            snap.next_edge_id(),
            SnapshotSchemas {
                node_schemas,
                edge_schemas,
            },
            triggers,
        )
    };

    let graph_snapshot = GraphSnapshot {
        nodes: raw_nodes.iter().map(SnapshotNode::from_node).collect(),
        edges: raw_edges.iter().map(SnapshotEdge::from_edge).collect(),
        next_node_id: next_node,
        next_edge_id: next_edge,
        changelog_sequence: sequence,
        schemas,
        triggers,
        extra_sections: vec![],
    };

    // Write snapshot to temp file, then read back as bytes.
    // write_snapshot_opts() handles compression + integrity (zstd + XXH3).
    let tmp_path = state.config.data_dir.join("replica_snapshot.tmp");
    write_snapshot_opts(&graph_snapshot, &tmp_path, false)?;
    let snapshot_bytes = std::fs::read(&tmp_path)?;
    let _ = std::fs::remove_file(&tmp_path);

    // Stream chunks
    let total_chunks = snapshot_bytes.len().div_ceil(CHUNK_SIZE).max(1);

    for (i, chunk_data) in snapshot_bytes.chunks(CHUNK_SIZE).enumerate() {
        let is_last = i == total_chunks - 1;
        let chunk = SnapshotChunk {
            chunk_index: i as u32,
            data: chunk_data.to_vec(),
            is_last,
            snapshot_sequence: if is_last { Some(sequence) } else { None },
        };

        let payload = serialize_payload(&chunk, flags)
            .map_err(|e| anyhow::anyhow!("snapshot chunk serialize: {e}"))?;
        let frame = Frame {
            msg_type: MsgType::SnapshotChunk,
            flags,
            payload: Bytes::from(payload),
        };
        send.write_all(&frame.encode()).await?;
    }

    // Handle empty graph — send a single empty final chunk
    if snapshot_bytes.is_empty() {
        let chunk = SnapshotChunk {
            chunk_index: 0,
            data: vec![],
            is_last: true,
            snapshot_sequence: Some(sequence),
        };
        let payload = serialize_payload(&chunk, flags)
            .map_err(|e| anyhow::anyhow!("snapshot chunk serialize: {e}"))?;
        let frame = Frame {
            msg_type: MsgType::SnapshotChunk,
            flags,
            payload: Bytes::from(payload),
        };
        send.write_all(&frame.encode()).await?;
    }

    tracing::info!(
        chunks = total_chunks,
        bytes = snapshot_bytes.len(),
        "snapshot stream complete"
    );

    Ok(())
}
