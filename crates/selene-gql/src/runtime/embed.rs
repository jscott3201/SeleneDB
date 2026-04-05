//! Embedding engine: local candle inference for `embed()` GQL function.
//!
//! Feature-gated behind `--features vector`. The embedding engine loads a
//! BERT-family model (default: all-MiniLM-L6-v2) from disk on first call
//! and caches it for the server lifetime.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config as BertConfig};
use tokenizers::Tokenizer;

use crate::runtime::eval::EvalContext;
use crate::runtime::functions::ScalarFunction;
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

/// Model path set once at server startup. Falls back to SELENE_MODEL_PATH env var.
static MODEL_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Cached embedding engine, initialized on first `embed()` call.
static ENGINE: OnceLock<Result<EmbeddingEngine, String>> = OnceLock::new();

/// Set the model directory path. Call once at server startup from config.
pub fn set_model_path(path: PathBuf) {
    let _ = MODEL_PATH.set(path);
}

/// Resolve the model path from static, env var, or default.
fn resolve_model_path() -> PathBuf {
    if let Some(path) = MODEL_PATH.get() {
        return path.clone();
    }
    if let Ok(path) = std::env::var("SELENE_MODEL_PATH") {
        return PathBuf::from(path);
    }
    PathBuf::from("data/models/all-MiniLM-L6-v2")
}

/// Get or initialize the embedding engine.
///
/// The engine is cached in a static `OnceLock`. If the first load fails,
/// the error is cached permanently. A server restart is required to retry.
fn get_engine() -> Result<&'static EmbeddingEngine, GqlError> {
    let result = ENGINE.get_or_init(|| {
        let path = resolve_model_path();
        tracing::info!(model_path = %path.display(), "loading embedding model...");
        match EmbeddingEngine::load(&path) {
            Ok(engine) => Ok(engine),
            Err(e) => {
                tracing::error!(model_path = %path.display(), error = %e, "embedding model load failed (restart required to retry)");
                Err(e.to_string())
            }
        }
    });
    match result {
        Ok(engine) => Ok(engine),
        Err(msg) => Err(GqlError::InvalidArgument {
            message: format!("embedding engine unavailable (restart to retry): {msg}"),
        }),
    }
}

/// Generate an embedding for text using the cached engine.
/// Public API for other modules (e.g., semanticSearch procedure).
pub fn embed_text(text: &str) -> Result<Vec<f32>, GqlError> {
    get_engine()?.embed(text)
}

/// BERT-based sentence embedding engine.
pub struct EmbeddingEngine {
    model: BertModel,
    tokenizer: Tokenizer,
}

impl EmbeddingEngine {
    /// Load model from a directory containing model.safetensors, tokenizer.json, config.json.
    #[allow(unsafe_code)] // candle mmap requires unsafe for memory-mapped safetensors
    pub fn load(model_dir: &Path) -> Result<Self, GqlError> {
        let config_path = model_dir.join("config.json");
        let tokenizer_path = model_dir.join("tokenizer.json");
        let weights_path = model_dir.join("model.safetensors");

        // Check files exist with clear error messages
        if !config_path.exists() {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "embedding model not found at {} -- run scripts/fetch-model.sh",
                    model_dir.display()
                ),
            });
        }
        if !weights_path.exists() {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "model.safetensors not found in {} -- run scripts/fetch-model.sh",
                    model_dir.display()
                ),
            });
        }

        // Load config
        let config_str =
            std::fs::read_to_string(&config_path).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to read {}: {e}", config_path.display()),
            })?;
        let config: BertConfig =
            serde_json::from_str(&config_str).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to parse config.json: {e}"),
            })?;

        // Load tokenizer
        let tokenizer =
            Tokenizer::from_file(&tokenizer_path).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to load tokenizer: {e}"),
            })?;

        // Load model weights
        let device = Device::Cpu;
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&[weights_path], candle_core::DType::F32, &device)
                .map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to load model weights: {e}"),
            })?
        };

        let model = BertModel::load(vb, &config).map_err(|e| GqlError::InvalidArgument {
            message: format!("failed to build BERT model: {e}"),
        })?;

        tracing::info!(
            model_dir = %model_dir.display(),
            hidden_size = config.hidden_size,
            num_layers = config.num_hidden_layers,
            "embedding engine loaded"
        );

        Ok(Self { model, tokenizer })
    }

    /// Maximum input text length in bytes (8 KiB).
    /// BERT tokenizers have a 512-token limit, but a pathologically long input
    /// can consume excessive memory during tokenization before truncation.
    const MAX_INPUT_BYTES: usize = 8192;

    /// Generate a normalized embedding for a text string.
    pub fn embed(&self, text: &str) -> Result<Vec<f32>, GqlError> {
        if text.len() > Self::MAX_INPUT_BYTES {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "embed() input too long ({} bytes, max {})",
                    text.len(),
                    Self::MAX_INPUT_BYTES
                ),
            });
        }

        let device = Device::Cpu;

        // Tokenize with truncation
        let encoding =
            self.tokenizer
                .encode(text, true)
                .map_err(|e| GqlError::InvalidArgument {
                    message: format!("tokenization failed: {e}"),
                })?;

        let token_ids = encoding.get_ids();
        let type_ids = encoding.get_type_ids();

        let tokens = Tensor::new(token_ids, &device)
            .map_err(|e| GqlError::internal(format!("tensor creation: {e}")))?
            .unsqueeze(0)
            .map_err(|e| GqlError::internal(format!("unsqueeze: {e}")))?;

        let type_ids_tensor = Tensor::new(type_ids, &device)
            .map_err(|e| GqlError::internal(format!("type_ids tensor: {e}")))?
            .unsqueeze(0)
            .map_err(|e| GqlError::internal(format!("unsqueeze: {e}")))?;

        // Forward pass
        let embeddings = self
            .model
            .forward(&tokens, &type_ids_tensor, None)
            .map_err(|e| GqlError::internal(format!("BERT forward pass: {e}")))?;

        // Mean pooling over token dimension (dim=1), excluding padding
        // Shape: [1, seq_len, hidden_size] → [1, hidden_size] → [hidden_size]
        let seq_len = embeddings
            .dim(1)
            .map_err(|e| GqlError::internal(format!("dim: {e}")))?;
        let pooled = (embeddings
            .sum(1)
            .map_err(|e| GqlError::internal(format!("sum: {e}")))?
            / (seq_len as f64))
            .map_err(|e| GqlError::internal(format!("div: {e}")))?;

        let pooled = pooled
            .squeeze(0)
            .map_err(|e| GqlError::internal(format!("squeeze: {e}")))?;

        // L2 normalize
        let norm = pooled
            .sqr()
            .map_err(|e| GqlError::internal(format!("sqr: {e}")))?
            .sum_all()
            .map_err(|e| GqlError::internal(format!("sum_all: {e}")))?
            .sqrt()
            .map_err(|e| GqlError::internal(format!("sqrt: {e}")))?;

        let normalized = pooled
            .broadcast_div(&norm)
            .map_err(|e| GqlError::internal(format!("normalize: {e}")))?;

        // Extract f32 vec
        let vec: Vec<f32> = normalized
            .to_vec1()
            .map_err(|e| GqlError::internal(format!("to_vec1: {e}")))?;

        Ok(vec)
    }
}

/// GQL scalar function: `embed('text') → Vector(384)`
pub struct EmbedFunction;

impl ScalarFunction for EmbedFunction {
    fn name(&self) -> &'static str {
        "embed"
    }

    fn description(&self) -> &'static str {
        "Generate a vector embedding from text (384 dimensions)"
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

        let engine = get_engine()?;
        let vec = engine.embed(text)?;
        Ok(GqlValue::Vector(std::sync::Arc::from(vec)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: skip test if model files aren't present.
    fn require_model() -> PathBuf {
        let path = resolve_model_path();
        if !path.join("model.safetensors").exists() {
            eprintln!(
                "SKIP: model not found at {} -- run scripts/fetch-model.sh",
                path.display()
            );
            std::process::exit(0);
        }
        path
    }

    #[test]
    fn embed_returns_384_dims() {
        let path = require_model();
        let engine = EmbeddingEngine::load(&path).unwrap();
        let vec = engine.embed("hello world").unwrap();
        assert_eq!(vec.len(), 384);
    }

    #[test]
    fn embed_is_deterministic() {
        let path = require_model();
        let engine = EmbeddingEngine::load(&path).unwrap();
        let v1 = engine.embed("temperature sensor").unwrap();
        let v2 = engine.embed("temperature sensor").unwrap();
        assert_eq!(v1, v2);
    }

    #[test]
    fn similar_text_high_cosine() {
        let path = require_model();
        let engine = EmbeddingEngine::load(&path).unwrap();
        let v1 = engine.embed("temperature sensor").unwrap();
        let v2 = engine.embed("temp sensor").unwrap();
        let sim = cosine_sim(&v1, &v2);
        assert!(sim > 0.7, "expected similarity > 0.7, got {sim}");
    }

    #[test]
    fn dissimilar_text_lower_cosine() {
        let path = require_model();
        let engine = EmbeddingEngine::load(&path).unwrap();
        let v1 = engine.embed("temperature sensor in HVAC system").unwrap();
        let v2 = engine
            .embed("the quick brown fox jumps over the lazy dog")
            .unwrap();
        let similar = engine.embed("temp sensor for heating ventilation").unwrap();
        let sim_different = cosine_sim(&v1, &v2);
        let sim_similar = cosine_sim(&v1, &similar);
        // Similar text should score higher than dissimilar text
        assert!(
            sim_similar > sim_different,
            "similar ({sim_similar}) should be > dissimilar ({sim_different})"
        );
    }

    #[test]
    fn embed_empty_string() {
        let path = require_model();
        let engine = EmbeddingEngine::load(&path).unwrap();
        let vec = engine.embed("").unwrap();
        assert_eq!(vec.len(), 384);
    }

    #[test]
    fn embed_missing_model_returns_error() {
        let result = EmbeddingEngine::load(Path::new("/nonexistent/path"));
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("not found") || err.contains("fetch-model"));
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
