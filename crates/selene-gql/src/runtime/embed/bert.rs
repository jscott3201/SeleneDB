//! BERT-based embedding provider (all-MiniLM-L6-v2).
//!
//! This is the default embedding provider. It loads a BERT model from disk
//! via candle and produces 384-dimensional normalized embeddings. Task prompts
//! are ignored (BERT has no task-specific prompt support).

use std::path::Path;

use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config as BertConfig};
use tokenizers::Tokenizer;

use super::provider::{EmbeddingProvider, EmbeddingTask};
use crate::types::error::GqlError;

/// Maximum input text length in bytes (8 KiB).
/// BERT tokenizers have a 512-token limit, but a pathologically long input
/// can consume excessive memory during tokenization before truncation.
const MAX_INPUT_BYTES: usize = 8192;

/// Mean pooling over token dimension (dim=1).
///
/// Averages all token embeddings into a single sentence embedding.
/// Shape: `[1, seq_len, hidden_size]` -> `[hidden_size]`
pub(super) fn mean_pool(embeddings: &Tensor) -> Result<Tensor, GqlError> {
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
pub(super) fn l2_normalize(tensor: &Tensor) -> Result<Tensor, GqlError> {
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

/// BERT-based sentence embedding provider.
///
/// Loads all-MiniLM-L6-v2 (or compatible BERT model) from a directory
/// containing `model.safetensors`, `tokenizer.json`, and `config.json`.
/// Produces 384-dimensional L2-normalized embeddings via mean pooling.
pub struct BertProvider {
    model: BertModel,
    tokenizer: Tokenizer,
}

impl BertProvider {
    /// Load model from a directory containing model.safetensors, tokenizer.json, config.json.
    #[allow(unsafe_code)] // candle mmap requires unsafe for memory-mapped safetensors
    pub fn load(model_dir: &Path) -> Result<Self, GqlError> {
        let config_path = model_dir.join("config.json");
        let tokenizer_path = model_dir.join("tokenizer.json");
        let weights_path = model_dir.join("model.safetensors");

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

        let config_str =
            std::fs::read_to_string(&config_path).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to read {}: {e}", config_path.display()),
            })?;
        let config: BertConfig =
            serde_json::from_str(&config_str).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to parse config.json: {e}"),
            })?;

        let tokenizer =
            Tokenizer::from_file(&tokenizer_path).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to load tokenizer: {e}"),
            })?;

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
            "BERT embedding provider loaded"
        );

        Ok(Self { model, tokenizer })
    }

    /// Run the BERT forward pass with mean pooling and L2 normalization.
    fn embed_inner(&self, text: &str) -> Result<Vec<f32>, GqlError> {
        if text.len() > MAX_INPUT_BYTES {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "embed() input too long ({} bytes, max {MAX_INPUT_BYTES})",
                    text.len(),
                ),
            });
        }

        let device = Device::Cpu;

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

        let embeddings = self
            .model
            .forward(&tokens, &type_ids_tensor, None)
            .map_err(|e| GqlError::internal(format!("BERT forward pass: {e}")))?;

        let pooled = mean_pool(&embeddings)?;
        let normalized = l2_normalize(&pooled)?;

        normalized
            .to_vec1()
            .map_err(|e| GqlError::internal(format!("to_vec1: {e}")))
    }
}

impl EmbeddingProvider for BertProvider {
    fn embed(&self, text: &str, _namespace: Option<&str>) -> Result<Vec<f32>, GqlError> {
        self.embed_inner(text)
    }

    fn embed_with_task(
        &self,
        text: &str,
        _task: EmbeddingTask,
        _namespace: Option<&str>,
    ) -> Result<Vec<f32>, GqlError> {
        // BERT has no task-specific prompts; all tasks produce the same embedding.
        self.embed_inner(text)
    }

    fn dimensions(&self, _namespace: Option<&str>) -> usize {
        384
    }

    fn model_id(&self) -> &'static str {
        "all-MiniLM-L6-v2"
    }

    fn max_input_bytes(&self) -> usize {
        MAX_INPUT_BYTES
    }
}
