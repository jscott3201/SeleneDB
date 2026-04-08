//! Embedding engine: pluggable provider architecture for vector embeddings.
//!
//! The embedding layer loads the EmbeddingGemma provider on first call and
//! caches it for the server lifetime.
//!
//! Public API:
//! - [`embed_text`]: Embed text using the default task (backward compatible).
//! - [`embed_text_with_task`]: Embed text with an explicit task selection.
//! - [`embedding_dimensions`]: Query the output vector dimensions.
//! - [`set_model_path`]: Configure model directory (legacy, use [`set_model_config`]).
//! - [`set_model_config`]: Configure model name, path, and dimensions.

pub mod gemma;
pub(crate) mod gemma_encoder;
pub mod provider;
pub(crate) mod quantized_gemma_encoder;

pub use provider::{EmbeddingProvider, EmbeddingTask};

use std::path::PathBuf;
use std::sync::OnceLock;

use candle_core::Tensor;

use crate::runtime::eval::EvalContext;
use crate::runtime::functions::ScalarFunction;
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

// ── Tensor utilities (used by GemmaProvider) ────────────────────────────

/// Mean pooling over token dimension (dim=1).
///
/// Averages all token embeddings into a single sentence embedding.
/// Shape: `[1, seq_len, hidden_size]` -> `[hidden_size]`
pub(crate) fn mean_pool(embeddings: &Tensor) -> Result<Tensor, GqlError> {
    let seq_len = embeddings
        .dim(1)
        .map_err(|e| GqlError::internal(format!("dim: {e}")))?;
    let pooled = (embeddings
        .sum(1)
        .map_err(|e| GqlError::internal(format!("sum: {e}")))?
        / (seq_len as f64))
        .map_err(|e| GqlError::internal(format!("div: {e}")))?;

    pooled
        .squeeze(0)
        .map_err(|e| GqlError::internal(format!("squeeze: {e}")))
}

/// L2 normalize a tensor to unit length.
///
/// Produces a unit vector suitable for cosine similarity via dot product.
pub(crate) fn l2_normalize(tensor: &Tensor) -> Result<Tensor, GqlError> {
    let norm = tensor
        .sqr()
        .map_err(|e| GqlError::internal(format!("sqr: {e}")))?
        .sum_all()
        .map_err(|e| GqlError::internal(format!("sum_all: {e}")))?
        .sqrt()
        .map_err(|e| GqlError::internal(format!("sqrt: {e}")))?;

    tensor
        .broadcast_div(&norm)
        .map_err(|e| GqlError::internal(format!("normalize: {e}")))
}

/// Model name set at server startup. Defaults to "embeddinggemma".
static MODEL_NAME: OnceLock<String> = OnceLock::new();

/// Model path set once at server startup. Falls back to SELENE_MODEL_PATH env var.
static MODEL_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Target output dimensions for MRL-capable models. Defaults to native dimensions.
static MODEL_DIMS: OnceLock<usize> = OnceLock::new();

/// Cached embedding provider, initialized on first `embed_text()` call.
static PROVIDER: OnceLock<Result<Box<dyn EmbeddingProvider>, String>> = OnceLock::new();

/// Set the model directory path. Call once at server startup from config.
///
/// Legacy API for backward compatibility. Prefer [`set_model_config`].
pub fn set_model_path(path: PathBuf) {
    let _ = MODEL_PATH.set(path);
}

/// Configure the embedding model. Call once at server startup.
///
/// - `name`: Model name (default `"embeddinggemma"`).
/// - `path`: Path to the model directory.
/// - `dimensions`: Target output dimensions (768, 512, 256, or 128).
pub fn set_model_config(name: String, path: PathBuf, dimensions: Option<usize>) {
    let _ = MODEL_NAME.set(name);
    let _ = MODEL_PATH.set(path);
    if let Some(dims) = dimensions {
        let _ = MODEL_DIMS.set(dims);
    }
}

/// Resolve the model path from static, env var, or default.
fn resolve_model_path() -> PathBuf {
    if let Some(path) = MODEL_PATH.get() {
        return path.clone();
    }
    if let Ok(path) = std::env::var("SELENE_MODEL_PATH") {
        return PathBuf::from(path);
    }
    PathBuf::from("data/models/embeddinggemma-300m")
}

/// Build the embedding provider (EmbeddingGemma).
fn build_provider() -> Result<Box<dyn EmbeddingProvider>, String> {
    let path = resolve_model_path();
    let dims = MODEL_DIMS.get().copied().unwrap_or(768);

    tracing::info!(path = %path.display(), dims, "loading EmbeddingGemma...");

    gemma::GemmaProvider::load(&path, dims)
        .map(|p| Box::new(p) as Box<dyn EmbeddingProvider>)
        .map_err(|e| e.to_string())
}

/// Get or initialize the embedding provider.
///
/// The provider is cached in a static `OnceLock`. If the first load fails,
/// the error is cached permanently. A server restart is required to retry.
fn get_provider() -> Result<&'static dyn EmbeddingProvider, GqlError> {
    let result = PROVIDER.get_or_init(|| match build_provider() {
        Ok(provider) => Ok(provider),
        Err(e) => {
            tracing::error!(error = %e, "embedding provider load failed (restart required to retry)");
            Err(e)
        }
    });
    match result {
        Ok(provider) => Ok(provider.as_ref()),
        Err(msg) => Err(GqlError::InvalidArgument {
            message: format!("embedding engine unavailable (restart to retry): {msg}"),
        }),
    }
}

/// Embedding model status for health reporting.
///
/// Returns the current state of the embedding provider without triggering
/// initialization. Safe to call from health checks.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EmbeddingStatus {
    /// Whether the embedding provider is loaded and operational.
    pub loaded: bool,
    /// Model identifier (e.g., "embeddinggemma-300m"). Null if not loaded.
    pub model_id: Option<String>,
    /// Output vector dimensions. Null if not loaded.
    pub dimensions: Option<usize>,
    /// Configured model path.
    pub model_path: String,
    /// Error message if the provider failed to load.
    pub error: Option<String>,
}

/// Query the embedding provider status without triggering initialization.
pub fn embedding_status() -> EmbeddingStatus {
    let model_path = resolve_model_path().display().to_string();

    match PROVIDER.get() {
        Some(Ok(provider)) => EmbeddingStatus {
            loaded: true,
            model_id: Some(provider.model_id().to_string()),
            dimensions: Some(provider.dimensions(None)),
            model_path,
            error: None,
        },
        Some(Err(e)) => EmbeddingStatus {
            loaded: false,
            model_id: None,
            dimensions: None,
            model_path,
            error: Some(e.clone()),
        },
        None => EmbeddingStatus {
            loaded: false,
            model_id: None,
            dimensions: None,
            model_path,
            error: None,
        },
    }
}

/// Generate an embedding for text using the default task (Retrieval).
///
/// This is the backward-compatible public API used by procedures and tools.
pub fn embed_text(text: &str) -> Result<Vec<f32>, GqlError> {
    get_provider()?.embed(text, None)
}

/// Generate an embedding with an explicit task selection.
///
/// EmbeddingGemma uses task-specific prompt prefixes to optimize embeddings
/// for different downstream tasks (retrieval, clustering, etc.).
pub fn embed_text_with_task(text: &str, task: EmbeddingTask) -> Result<Vec<f32>, GqlError> {
    get_provider()?.embed_with_task(text, task, None)
}

/// Query the output vector dimensions of the current provider.
pub fn embedding_dimensions() -> Result<usize, GqlError> {
    Ok(get_provider()?.dimensions(None))
}

/// GQL scalar function: `embed('text') -> Vector`
pub struct EmbedFunction;

impl ScalarFunction for EmbedFunction {
    fn name(&self) -> &'static str {
        "embed"
    }

    fn description(&self) -> &'static str {
        "Generate a vector embedding from text"
    }

    fn invoke(&self, args: &[GqlValue], _ctx: &EvalContext<'_>) -> Result<GqlValue, GqlError> {
        let text = match args.first() {
            Some(GqlValue::Null) | None => return Ok(GqlValue::Null),
            Some(GqlValue::String(s)) => s.as_str(),
            Some(other) => {
                return Err(GqlError::type_error(format!(
                    "embed() requires a STRING argument, got {}",
                    other.gql_type()
                )));
            }
        };

        let provider = get_provider()?;
        let vec = provider.embed(text, None)?;
        Ok(GqlValue::Vector(std::sync::Arc::from(vec)))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// Helper: skip test if EmbeddingGemma model files aren't present.
    fn require_model() -> PathBuf {
        // Navigate from crate root to workspace root
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent() // crates/
            .and_then(|p| p.parent()) // workspace root
            .expect("workspace root")
            .to_path_buf();
        let path = workspace_root.join("data/models/embeddinggemma-300m");
        if !path.join("model.safetensors").exists() {
            eprintln!(
                "SKIP: EmbeddingGemma not found at {} -- run scripts/fetch-embeddinggemma.sh",
                path.display()
            );
            std::process::exit(0);
        }
        path
    }

    #[test]
    fn embed_returns_768_dims() {
        let path = require_model();
        let provider = gemma::GemmaProvider::load(&path, 768).unwrap();
        let vec = provider.embed("hello world", None).unwrap();
        assert_eq!(vec.len(), 768);
    }

    #[test]
    fn embed_is_deterministic() {
        let path = require_model();
        let provider = gemma::GemmaProvider::load(&path, 768).unwrap();
        let v1 = provider.embed("temperature sensor", None).unwrap();
        let v2 = provider.embed("temperature sensor", None).unwrap();
        assert_eq!(v1, v2);
    }

    #[test]
    fn similar_text_high_cosine() {
        let path = require_model();
        let provider = gemma::GemmaProvider::load(&path, 768).unwrap();
        let v1 = provider.embed("temperature sensor", None).unwrap();
        let v2 = provider.embed("temp sensor", None).unwrap();
        let sim = cosine_sim(&v1, &v2);
        assert!(sim > 0.7, "expected similarity > 0.7, got {sim}");
    }

    #[test]
    fn dissimilar_text_lower_cosine() {
        let path = require_model();
        let provider = gemma::GemmaProvider::load(&path, 768).unwrap();
        let v1 = provider
            .embed("temperature sensor in HVAC system", None)
            .unwrap();
        let v2 = provider
            .embed("the quick brown fox jumps over the lazy dog", None)
            .unwrap();
        let similar = provider
            .embed("temp sensor for heating ventilation", None)
            .unwrap();
        let sim_different = cosine_sim(&v1, &v2);
        let sim_similar = cosine_sim(&v1, &similar);
        assert!(
            sim_similar > sim_different,
            "similar ({sim_similar}) should be > dissimilar ({sim_different})"
        );
    }

    #[test]
    fn embed_empty_string() {
        let path = require_model();
        let provider = gemma::GemmaProvider::load(&path, 768).unwrap();
        let vec = provider.embed("", None).unwrap();
        assert_eq!(vec.len(), 768);
    }

    #[test]
    fn embed_missing_model_returns_error() {
        let result = gemma::GemmaProvider::load(Path::new("/nonexistent/path"), 768);
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("not found") || err.contains("fetch-embeddinggemma"));
    }

    #[test]
    fn embed_is_unit_normalized() {
        let path = require_model();
        let provider = gemma::GemmaProvider::load(&path, 768).unwrap();
        let vec = provider.embed("temperature sensor", None).unwrap();
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 0.01,
            "expected L2 norm ~1.0, got {norm}"
        );
    }

    #[test]
    fn embed_mrl_truncation_256() {
        let path = require_model();
        let provider = gemma::GemmaProvider::load(&path, 256).unwrap();
        let vec = provider.embed("hello world", None).unwrap();
        assert_eq!(vec.len(), 256);
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 0.01,
            "MRL truncated vector should be re-normalized, got norm={norm}"
        );
    }

    #[test]
    fn embed_task_prompts_differ() {
        let path = require_model();
        let provider = gemma::GemmaProvider::load(&path, 768).unwrap();
        let v_retrieval = provider
            .embed_with_task("temperature sensor", EmbeddingTask::Retrieval, None)
            .unwrap();
        let v_clustering = provider
            .embed_with_task("temperature sensor", EmbeddingTask::Clustering, None)
            .unwrap();
        // Different task prompts should produce different embeddings
        assert_ne!(v_retrieval, v_clustering);
    }

    fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
        let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
        let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if mag_a == 0.0 || mag_b == 0.0 {
            0.0
        } else {
            dot / (mag_a * mag_b)
        }
    }
}
