//! Embedding provider trait and task-specific prompt selection.
//!
//! All embedding operations go through `EmbeddingProvider`. Implementations
//! are `Send + Sync` for use in the static `OnceLock` cache. The `namespace`
//! parameter on each method prepares the interface for future multi-namespace
//! embedding support; current implementations ignore it.

use crate::types::error::GqlError;

/// Task-specific prompt selection for embedding models that support it.
///
/// Models like EmbeddingGemma use prompt prefixes to optimize embeddings
/// for different downstream tasks. BERT-family models ignore this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EmbeddingTask {
    /// Retrieval/search queries.
    Retrieval,
    /// Semantic similarity comparison.
    SemanticSimilarity,
    /// Text classification.
    Classification,
    /// Clustering.
    Clustering,
    /// Document storage (titles, descriptions, memory entries).
    Document,
    /// No task prefix (backward compatible default).
    Raw,
}

/// Trait for embedding providers.
///
/// Implementations must be `Send + Sync` for use in the global static cache.
/// The `namespace` parameter is reserved for future multi-namespace support;
/// pass `None` to use the default namespace.
pub trait EmbeddingProvider: Send + Sync {
    /// Embed text into a vector using the default task (`Retrieval`).
    fn embed(&self, text: &str, namespace: Option<&str>) -> Result<Vec<f32>, GqlError>;

    /// Embed text with an explicit task selection.
    fn embed_with_task(
        &self,
        text: &str,
        task: EmbeddingTask,
        namespace: Option<&str>,
    ) -> Result<Vec<f32>, GqlError>;

    /// Output vector dimensions for the given namespace.
    fn dimensions(&self, namespace: Option<&str>) -> usize;

    /// Model identifier (e.g., "all-MiniLM-L6-v2", "embeddinggemma-300m").
    fn model_id(&self) -> &'static str;

    /// Maximum input text length in bytes.
    fn max_input_bytes(&self) -> usize;
}
