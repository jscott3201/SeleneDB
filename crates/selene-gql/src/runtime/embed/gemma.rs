//! EmbeddingGemma provider: 308M parameter embedding model from Google.
//!
//! Processing pipeline:
//! 1. Tokenize with task-specific prompt prefix
//! 2. Encoder forward (24-layer bidirectional Gemma 3 backbone) -> `[1, seq_len, 768]`
//! 3. Mean pooling -> `[768]`
//! 4. Dense projection 1 (768 -> 3072) -> `[3072]`
//! 5. Dense projection 2 (3072 -> 768) -> `[768]`
//! 6. L2 normalize -> `[768]` unit vector
//! 7. MRL truncation to target dims -> `[target_dims]` re-normalized unit vector

use std::path::Path;

use candle_core::{DType, Device, Tensor};
use candle_nn::{Linear, VarBuilder};
use tokenizers::Tokenizer;

use super::gemma_encoder::{EmbeddingGemmaConfig, EmbeddingGemmaEncoder};
use super::provider::{EmbeddingProvider, EmbeddingTask};
use super::{l2_normalize, mean_pool};
use crate::types::error::GqlError;

/// Valid MRL truncation dimensions for EmbeddingGemma.
const VALID_MRL_DIMS: &[usize] = &[128, 256, 512, 768];

/// Maximum input text length in bytes.
/// EmbeddingGemma supports 2048 tokens, but we bound pre-tokenization input.
const MAX_INPUT_BYTES: usize = 16384;

/// EmbeddingGemma embedding provider.
///
/// Loads the EmbeddingGemma-300M model (Gemma 3 encoder backbone + Dense
/// projection layers) and produces L2-normalized embeddings with optional
/// MRL (Matryoshka Representation Learning) dimension truncation.
pub struct GemmaProvider {
    encoder: EmbeddingGemmaEncoder,
    tokenizer: Tokenizer,
    dense1: Linear, // 768 -> 3072
    dense2: Linear, // 3072 -> 768
    target_dims: usize,
    device: Device,
}

impl GemmaProvider {
    /// Load EmbeddingGemma from a model directory.
    ///
    /// Expected directory structure:
    /// - `config.json` (EmbeddingGemma config)
    /// - `tokenizer.json` (Gemma 3 tokenizer, 262K vocab)
    /// - `model.safetensors` (backbone weights)
    /// - `2_Dense/model.safetensors` (projection 768 -> 3072)
    /// - `3_Dense/model.safetensors` (projection 3072 -> 768)
    #[allow(unsafe_code)] // candle mmap requires unsafe for memory-mapped safetensors
    pub fn load(model_dir: &Path, target_dims: usize) -> Result<Self, GqlError> {
        // Validate target dimensions
        if !VALID_MRL_DIMS.contains(&target_dims) {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "invalid embedding dimensions {target_dims}, must be one of {VALID_MRL_DIMS:?}"
                ),
            });
        }

        // Check required files
        let config_path = model_dir.join("config.json");
        let tokenizer_path = model_dir.join("tokenizer.json");
        let weights_path = model_dir.join("model.safetensors");
        let dense1_path = model_dir.join("2_Dense/model.safetensors");
        let dense2_path = model_dir.join("3_Dense/model.safetensors");

        for (path, desc) in [
            (&config_path, "config.json"),
            (&tokenizer_path, "tokenizer.json"),
            (&weights_path, "model.safetensors"),
            (&dense1_path, "2_Dense/model.safetensors"),
            (&dense2_path, "3_Dense/model.safetensors"),
        ] {
            if !path.exists() {
                return Err(GqlError::InvalidArgument {
                    message: format!(
                        "{desc} not found in {} -- run scripts/fetch-embeddinggemma.sh",
                        model_dir.display()
                    ),
                });
            }
        }

        // Load config
        let config_str =
            std::fs::read_to_string(&config_path).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to read config.json: {e}"),
            })?;
        let config: EmbeddingGemmaConfig =
            serde_json::from_str(&config_str).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to parse EmbeddingGemma config.json: {e}"),
            })?;

        // Load tokenizer
        let tokenizer =
            Tokenizer::from_file(&tokenizer_path).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to load tokenizer: {e}"),
            })?;

        let device = select_device();
        // Load backbone weights (bf16 -> f32)
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&[weights_path], DType::F32, &device).map_err(
                |e| GqlError::InvalidArgument {
                    message: format!("failed to load backbone weights: {e}"),
                },
            )?
        };

        let encoder =
            EmbeddingGemmaEncoder::load(&config, vb).map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to build EmbeddingGemma encoder: {e}"),
            })?;

        // Load Dense projection layers from separate safetensors
        // 2_Dense: 768 -> 3072 (weight shape: [3072, 768])
        let dense1 = Self::load_dense_layer(&dense1_path, 3072, 768, &device)?;
        // 3_Dense: 3072 -> 768 (weight shape: [768, 3072])
        let dense2 = Self::load_dense_layer(&dense2_path, 768, 3072, &device)?;

        let device_name = if device.is_metal() { "Metal" } else { "CPU" };
        tracing::info!(
            model_dir = %model_dir.display(),
            hidden_size = config.hidden_size,
            num_layers = config.num_hidden_layers,
            target_dims = target_dims,
            device = device_name,
            "EmbeddingGemma provider loaded"
        );

        Ok(Self {
            encoder,
            tokenizer,
            dense1,
            dense2,
            target_dims,
            device,
        })
    }

    /// Load a single Dense projection layer from a safetensors file.
    ///
    /// Each file contains a `linear.weight` tensor (no bias).
    /// 2_Dense: shape (3072, 768), 3_Dense: shape (768, 3072).
    #[allow(unsafe_code)]
    fn load_dense_layer(
        path: &Path,
        out_dim: usize,
        in_dim: usize,
        device: &Device,
    ) -> Result<Linear, GqlError> {
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(&[path.to_path_buf()], DType::F32, device).map_err(
                |e| GqlError::InvalidArgument {
                    message: format!("failed to load dense weights from {}: {e}", path.display()),
                },
            )?
        };

        let weight = vb
            .pp("linear")
            .get((out_dim, in_dim), "weight")
            .map_err(|e| GqlError::InvalidArgument {
                message: format!("failed to load linear.weight from {}: {e}", path.display()),
            })?;

        Ok(Linear::new(weight, None))
    }

    /// Prepend task-specific prompt prefix for EmbeddingGemma.
    fn format_with_task(task: EmbeddingTask, text: &str) -> String {
        match task {
            EmbeddingTask::Retrieval => format!("task: search result | query: {text}"),
            EmbeddingTask::SemanticSimilarity => {
                format!("task: sentence similarity | query: {text}")
            }
            EmbeddingTask::Classification => format!("task: classification | query: {text}"),
            EmbeddingTask::Clustering => format!("task: clustering | query: {text}"),
            EmbeddingTask::Document => format!("title: none | text: {text}"),
            EmbeddingTask::Raw => text.to_string(),
        }
    }

    /// Core embedding pipeline: tokenize -> encode -> pool -> project -> normalize -> truncate.
    fn embed_inner(&self, text: &str, task: EmbeddingTask) -> Result<Vec<f32>, GqlError> {
        if text.len() > MAX_INPUT_BYTES {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "embed() input too long ({} bytes, max {MAX_INPUT_BYTES})",
                    text.len(),
                ),
            });
        }

        let prompted = Self::format_with_task(task, text);
        let device = &self.device;

        // Tokenize
        let encoding = self
            .tokenizer
            .encode(prompted.as_str(), true)
            .map_err(|e| GqlError::InvalidArgument {
                message: format!("tokenization failed: {e}"),
            })?;

        let token_ids = encoding.get_ids();
        let tokens = Tensor::new(token_ids, device)
            .map_err(|e| GqlError::internal(format!("tensor creation: {e}")))?
            .unsqueeze(0)
            .map_err(|e| GqlError::internal(format!("unsqueeze: {e}")))?;

        // Encoder forward -> [1, seq_len, 768]
        let hidden_states = self
            .encoder
            .forward(&tokens)
            .map_err(|e| GqlError::internal(format!("encoder forward: {e}")))?;

        // Mean pooling -> [768]
        let pooled = mean_pool(&hidden_states)?;

        // Dense projections need 2D input: [768] -> [1, 768]
        let projected = pooled
            .unsqueeze(0)
            .map_err(|e| GqlError::internal(format!("unsqueeze for dense: {e}")))?;
        let projected = projected
            .apply(&self.dense1)
            .map_err(|e| GqlError::internal(format!("dense1: {e}")))?;
        let projected = projected
            .apply(&self.dense2)
            .map_err(|e| GqlError::internal(format!("dense2: {e}")))?;
        // Back to 1D: [1, 768] -> [768]
        let projected = projected
            .squeeze(0)
            .map_err(|e| GqlError::internal(format!("squeeze after dense: {e}")))?;

        // L2 normalize the full 768-dim vector
        let normalized = l2_normalize(&projected)?;

        // MRL truncation: slice to target_dims, then re-normalize
        let output = if self.target_dims < 768 {
            let truncated = normalized
                .narrow(0, 0, self.target_dims)
                .map_err(|e| GqlError::internal(format!("MRL truncation: {e}")))?;
            l2_normalize(&truncated)?
        } else {
            normalized
        };

        output
            .to_vec1()
            .map_err(|e| GqlError::internal(format!("to_vec1: {e}")))
    }
}

impl EmbeddingProvider for GemmaProvider {
    fn embed(&self, text: &str, _namespace: Option<&str>) -> Result<Vec<f32>, GqlError> {
        self.embed_inner(text, EmbeddingTask::Retrieval)
    }

    fn embed_with_task(
        &self,
        text: &str,
        task: EmbeddingTask,
        _namespace: Option<&str>,
    ) -> Result<Vec<f32>, GqlError> {
        self.embed_inner(text, task)
    }

    fn dimensions(&self, _namespace: Option<&str>) -> usize {
        self.target_dims
    }

    fn model_id(&self) -> &'static str {
        "embeddinggemma-300m"
    }

    fn max_input_bytes(&self) -> usize {
        MAX_INPUT_BYTES
    }
}

/// Select the compute device for embedding inference.
///
/// Metal GPU is available when compiled with `--features metal` and enabled
/// at runtime via `SELENE_METAL=1`. Defaults to CPU because candle 0.10's
/// Metal backend lacks a rotary-emb kernel required by the Gemma 3 encoder.
/// Enable when a future candle release adds the missing kernel.
fn select_device() -> Device {
    #[cfg(feature = "metal")]
    if std::env::var("SELENE_METAL").is_ok_and(|v| v == "1") {
        match Device::new_metal(0) {
            Ok(device) => {
                tracing::info!("EmbeddingGemma using Metal GPU acceleration");
                return device;
            }
            Err(e) => {
                tracing::warn!("Metal requested but not available, falling back to CPU: {e}");
            }
        }
    }
    Device::Cpu
}
