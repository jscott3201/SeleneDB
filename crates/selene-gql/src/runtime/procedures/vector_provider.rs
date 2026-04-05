//! VectorProvider trait: decouples vector storage from the GQL engine.
//!
//! The VectorStore in selene-server implements this trait. The GQL engine
//! uses it via OnceLock for contiguous vector scanning during brute-force
//! search. Falls back to PropertyMap access when no provider is registered.

use std::sync::{Arc, OnceLock};

use selene_core::{IStr, NodeId};

/// Provider for contiguous vector data during brute-force search.
///
/// The implementation holds its internal lock for the duration of `scan_vectors`,
/// so borrowed slices do not escape the callback.
pub trait VectorProvider: Send + Sync {
    /// Scan all vectors for the given nodes and property, calling `f` for each match.
    ///
    /// - `node_ids`: iterator of candidate node IDs (from label scan)
    /// - `key`: the property key (e.g., "embedding")
    /// - `query_dim`: expected vector dimension (mismatches are skipped)
    /// - `f`: callback receiving (node_id, vector_data, is_normalized)
    fn scan_vectors(
        &self,
        node_ids: &mut dyn Iterator<Item = NodeId>,
        key: &IStr,
        query_dim: usize,
        f: &mut dyn FnMut(NodeId, &[f32], bool),
    );

    /// Score vectors internally using optimized storage format (e.g., int8).
    ///
    /// Returns `true` if scoring was performed (caller should NOT re-score).
    /// Returns `false` if not supported (caller falls back to `scan_vectors` + manual scoring).
    ///
    /// The callback receives `(node_id, cosine_similarity_score)` without raw vector data.
    fn scan_with_scores(
        &self,
        _node_ids: &mut dyn Iterator<Item = NodeId>,
        _key: &IStr,
        _query_vec: &[f32],
        _f: &mut dyn FnMut(NodeId, f32),
    ) -> bool {
        false // Default: not supported
    }
}

static VECTOR_PROVIDER: OnceLock<Arc<dyn VectorProvider>> = OnceLock::new();

/// Set the vector provider. Called once at server startup.
pub fn set_vector_provider(provider: Arc<dyn VectorProvider>) {
    let _ = VECTOR_PROVIDER.set(provider);
}

/// Get the registered vector provider (if any).
pub fn get_vector_provider() -> Option<&'static Arc<dyn VectorProvider>> {
    VECTOR_PROVIDER.get()
}
