//! Quantized EmbeddingGemma encoder for GGUF model files.
//!
//! Mirrors `gemma_encoder.rs` (24-layer bidirectional Gemma 3 backbone) but
//! uses candle's quantized types (`QMatMul`, `quantized_nn::RmsNorm`) for
//! 85-92% memory reduction (2.4 GB f32 to 200-350 MB quantized).
//!
//! GGUF tensor naming follows the llama.cpp convention used by ggml-org
//! conversions of EmbeddingGemma-300M.
//!
//! Key differences from the standard encoder:
//! - `QMatMul` replaces `Linear` for all projection layers
//! - `quantized_nn::RmsNorm` does NOT apply the Gemma +1.0 weight offset
//!   (the GGUF converter bakes the offset into the stored weights)
//! - Weights loaded from a single GGUF file instead of safetensors

use candle_core::quantized::QTensor;
use candle_core::quantized::gguf_file;
use candle_core::{DType, Device, Module, Result, Tensor};
use candle_transformers::quantized_nn::RmsNorm;

use super::gemma_encoder::EmbeddingGemmaConfig;

// ── QMatMul wrapper ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct QMatMul {
    inner: candle_core::quantized::QMatMul,
}

impl QMatMul {
    fn from_qtensor(qtensor: QTensor) -> Result<Self> {
        let inner = candle_core::quantized::QMatMul::from_qtensor(qtensor)?;
        Ok(Self { inner })
    }

    fn forward(&self, xs: &Tensor) -> Result<Tensor> {
        self.inner.forward(xs)
    }
}

// ── Rotary Embedding ───────────────────────────────────────────────────
// Identical to the standard encoder (uses computed Tensors, not weights).

#[derive(Debug, Clone)]
struct RotaryEmbedding {
    sin: Tensor,
    cos: Tensor,
}

impl RotaryEmbedding {
    fn new(
        dtype: DType,
        cfg: &EmbeddingGemmaConfig,
        dev: &Device,
        is_sliding: bool,
    ) -> Result<Self> {
        let dim = cfg.head_dim;
        let max_seq_len = cfg.max_position_embeddings;
        let rope_freq = if is_sliding {
            cfg.rope_local_base_freq
        } else {
            cfg.rope_theta
        };
        let inv_freq: Vec<_> = (0..dim)
            .step_by(2)
            .map(|i| 1f32 / rope_freq.powf(i as f64 / dim as f64) as f32)
            .collect();
        let inv_freq_len = inv_freq.len();
        let inv_freq = Tensor::from_vec(inv_freq, (1, inv_freq_len), dev)?.to_dtype(dtype)?;
        let t = Tensor::arange(0u32, max_seq_len as u32, dev)?
            .to_dtype(dtype)?
            .reshape((max_seq_len, 1))?;
        let freqs = t.matmul(&inv_freq)?;
        Ok(Self {
            sin: freqs.sin()?,
            cos: freqs.cos()?,
        })
    }

    fn apply_rotary_emb_qkv(&self, q: &Tensor, k: &Tensor) -> Result<(Tensor, Tensor)> {
        let (_b_sz, _h, seq_len, _n_embd) = q.dims4()?;
        let cos = self.cos.narrow(0, 0, seq_len)?;
        let sin = self.sin.narrow(0, 0, seq_len)?;
        let q_embed = candle_nn::rotary_emb::rope(&q.contiguous()?, &cos, &sin)?;
        let k_embed = candle_nn::rotary_emb::rope(&k.contiguous()?, &cos, &sin)?;
        Ok((q_embed, k_embed))
    }
}

// ── Quantized MLP ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
struct QuantizedMlp {
    gate_proj: QMatMul,
    up_proj: QMatMul,
    down_proj: QMatMul,
}

impl QuantizedMlp {
    fn forward(&self, xs: &Tensor) -> Result<Tensor> {
        let gate = candle_nn::ops::silu(&self.gate_proj.forward(xs)?)?;
        let up = self.up_proj.forward(xs)?;
        self.down_proj.forward(&(gate * up)?)
    }
}

// ── Quantized Encoder Attention ────────────────────────────────────────

#[derive(Debug, Clone)]
struct QuantizedEncoderAttention {
    q_proj: QMatMul,
    k_proj: QMatMul,
    v_proj: QMatMul,
    o_proj: QMatMul,
    q_norm: RmsNorm,
    k_norm: RmsNorm,
    num_heads: usize,
    num_kv_heads: usize,
    num_kv_groups: usize,
    head_dim: usize,
    attn_logit_softcapping: Option<f64>,
    rotary_emb: std::sync::Arc<RotaryEmbedding>,
}

impl QuantizedEncoderAttention {
    fn forward(&self, xs: &Tensor, attention_mask: Option<&Tensor>) -> Result<Tensor> {
        let (b_sz, q_len, _) = xs.dims3()?;

        let query_states = self.q_proj.forward(xs)?;
        let key_states = self.k_proj.forward(xs)?;
        let value_states = self.v_proj.forward(xs)?;

        let query_states = query_states
            .reshape((b_sz, q_len, self.num_heads, self.head_dim))?
            .transpose(1, 2)?;
        let key_states = key_states
            .reshape((b_sz, q_len, self.num_kv_heads, self.head_dim))?
            .transpose(1, 2)?;
        let value_states = value_states
            .reshape((b_sz, q_len, self.num_kv_heads, self.head_dim))?
            .transpose(1, 2)?;

        let query_states = self.q_norm.forward(&query_states.contiguous()?)?;
        let key_states = self.k_norm.forward(&key_states.contiguous()?)?;

        let (query_states, key_states) = self
            .rotary_emb
            .apply_rotary_emb_qkv(&query_states, &key_states)?;

        let key_states =
            candle_transformers::utils::repeat_kv(key_states, self.num_kv_groups)?.contiguous()?;
        let value_states = candle_transformers::utils::repeat_kv(value_states, self.num_kv_groups)?
            .contiguous()?;

        let scale = 1f64 / f64::sqrt(self.head_dim as f64);
        let attn_weights = (query_states.matmul(&key_states.transpose(2, 3)?)? * scale)?;

        let attn_weights = match self.attn_logit_softcapping {
            None => attn_weights,
            Some(sc) => ((attn_weights / sc)?.tanh()? * sc)?,
        };

        let attn_weights = match attention_mask {
            None => attn_weights,
            Some(mask) => attn_weights.broadcast_add(mask)?,
        };
        let attn_weights = candle_nn::ops::softmax_last_dim(&attn_weights)?;

        let output = attn_weights
            .matmul(&value_states)?
            .transpose(1, 2)?
            .reshape((b_sz, q_len, ()))?;
        self.o_proj.forward(&output)
    }
}

// ── Quantized Encoder Layer ────────────────────────────────────────────

#[derive(Debug, Clone)]
struct QuantizedEncoderLayer {
    self_attn: QuantizedEncoderAttention,
    mlp: QuantizedMlp,
    input_layernorm: RmsNorm,
    pre_feedforward_layernorm: RmsNorm,
    post_feedforward_layernorm: RmsNorm,
    post_attention_layernorm: RmsNorm,
    is_sliding: bool,
}

impl QuantizedEncoderLayer {
    fn forward(&self, xs: &Tensor, attention_mask: Option<&Tensor>) -> Result<Tensor> {
        let residual = xs;
        let xs = self.input_layernorm.forward(xs)?;
        let xs = self.self_attn.forward(&xs, attention_mask)?;
        let xs = self.post_attention_layernorm.forward(&xs)?;
        let xs = (xs + residual)?;
        let residual = &xs;
        let xs = self.pre_feedforward_layernorm.forward(&xs)?;
        let xs = self.mlp.forward(&xs)?;
        let xs = self.post_feedforward_layernorm.forward(&xs)?;
        residual + xs
    }
}

// ── Bidirectional Attention Mask ───────────────────────────────────────

fn prepare_bidirectional_sliding_mask(
    b_size: usize,
    seq_len: usize,
    sliding_window: usize,
    dtype: DType,
    device: &Device,
) -> Result<Tensor> {
    let mask: Vec<_> = (0..seq_len)
        .flat_map(|i| {
            (0..seq_len).map(move |j| {
                let dist = i.abs_diff(j);
                if dist > sliding_window {
                    f32::NEG_INFINITY
                } else {
                    0.
                }
            })
        })
        .collect();
    let mask = Tensor::from_slice(&mask, (seq_len, seq_len), device)?;
    mask.expand((b_size, 1, seq_len, seq_len))?.to_dtype(dtype)
}

// ── Top-Level Quantized Encoder ────────────────────────────────────────

/// Quantized EmbeddingGemma encoder loaded from a GGUF file.
///
/// Functionally identical to `EmbeddingGemmaEncoder` but uses quantized
/// weight representations for significantly reduced memory usage.
#[derive(Debug, Clone)]
pub(super) struct QuantizedEmbeddingGemmaEncoder {
    embed_tokens: candle_nn::Embedding,
    layers: Vec<QuantizedEncoderLayer>,
    norm: RmsNorm,
    hidden_size: usize,
    sliding_window: usize,
    device: Device,
}

impl QuantizedEmbeddingGemmaEncoder {
    /// Load from a GGUF file.
    pub fn from_gguf<R: std::io::Seek + std::io::Read>(
        ct: &gguf_file::Content,
        reader: &mut R,
        cfg: &EmbeddingGemmaConfig,
        device: &Device,
    ) -> Result<Self> {
        let rms_norm_eps = cfg.rms_norm_eps;

        // Embedding layer (dequantized to f32 for embedding lookup)
        let tok_embeddings = ct.tensor(reader, "token_embd.weight", device)?;
        let tok_embeddings = tok_embeddings.dequantize(device)?;
        let embed_tokens = candle_nn::Embedding::new(tok_embeddings, cfg.hidden_size);

        // Final norm
        let norm = RmsNorm::from_qtensor(
            ct.tensor(reader, "output_norm.weight", device)?,
            rms_norm_eps,
        )?;

        // Encoder layers
        let mut layers = Vec::with_capacity(cfg.num_hidden_layers);
        for layer_idx in 0..cfg.num_hidden_layers {
            let is_sliding = cfg.is_sliding_layer(layer_idx);
            let prefix = format!("blk.{layer_idx}");

            // Rotary embedding (computed, not loaded)
            let rotary_emb =
                std::sync::Arc::new(RotaryEmbedding::new(DType::F32, cfg, device, is_sliding)?);

            // Attention projections
            let q_proj = QMatMul::from_qtensor(ct.tensor(
                reader,
                &format!("{prefix}.attn_q.weight"),
                device,
            )?)?;
            let k_proj = QMatMul::from_qtensor(ct.tensor(
                reader,
                &format!("{prefix}.attn_k.weight"),
                device,
            )?)?;
            let v_proj = QMatMul::from_qtensor(ct.tensor(
                reader,
                &format!("{prefix}.attn_v.weight"),
                device,
            )?)?;
            let o_proj = QMatMul::from_qtensor(ct.tensor(
                reader,
                &format!("{prefix}.attn_output.weight"),
                device,
            )?)?;

            // Attention norms
            let q_norm = RmsNorm::from_qtensor(
                ct.tensor(reader, &format!("{prefix}.attn_q_norm.weight"), device)?,
                rms_norm_eps,
            )?;
            let k_norm = RmsNorm::from_qtensor(
                ct.tensor(reader, &format!("{prefix}.attn_k_norm.weight"), device)?,
                rms_norm_eps,
            )?;

            let self_attn = QuantizedEncoderAttention {
                q_proj,
                k_proj,
                v_proj,
                o_proj,
                q_norm,
                k_norm,
                num_heads: cfg.num_attention_heads,
                num_kv_heads: cfg.num_key_value_heads,
                num_kv_groups: cfg.num_attention_heads / cfg.num_key_value_heads,
                head_dim: cfg.head_dim,
                attn_logit_softcapping: cfg.attn_logit_softcapping,
                rotary_emb,
            };

            // MLP projections
            let gate_proj = QMatMul::from_qtensor(ct.tensor(
                reader,
                &format!("{prefix}.ffn_gate.weight"),
                device,
            )?)?;
            let up_proj = QMatMul::from_qtensor(ct.tensor(
                reader,
                &format!("{prefix}.ffn_up.weight"),
                device,
            )?)?;
            let down_proj = QMatMul::from_qtensor(ct.tensor(
                reader,
                &format!("{prefix}.ffn_down.weight"),
                device,
            )?)?;
            let mlp = QuantizedMlp {
                gate_proj,
                up_proj,
                down_proj,
            };

            // Layer norms
            let input_layernorm = RmsNorm::from_qtensor(
                ct.tensor(reader, &format!("{prefix}.attn_norm.weight"), device)?,
                rms_norm_eps,
            )?;
            let post_attention_layernorm = RmsNorm::from_qtensor(
                ct.tensor(
                    reader,
                    &format!("{prefix}.post_attention_norm.weight"),
                    device,
                )?,
                rms_norm_eps,
            )?;
            let pre_feedforward_layernorm = RmsNorm::from_qtensor(
                ct.tensor(reader, &format!("{prefix}.ffn_norm.weight"), device)?,
                rms_norm_eps,
            )?;
            let post_feedforward_layernorm = RmsNorm::from_qtensor(
                ct.tensor(reader, &format!("{prefix}.post_ffw_norm.weight"), device)?,
                rms_norm_eps,
            )?;

            layers.push(QuantizedEncoderLayer {
                self_attn,
                mlp,
                input_layernorm,
                pre_feedforward_layernorm,
                post_feedforward_layernorm,
                post_attention_layernorm,
                is_sliding,
            });
        }

        Ok(Self {
            embed_tokens,
            layers,
            norm,
            hidden_size: cfg.hidden_size,
            sliding_window: cfg.sliding_window,
            device: device.clone(),
        })
    }

    /// Forward pass returning full hidden states `[batch, seq_len, hidden_size]`.
    pub fn forward(&self, input_ids: &Tensor) -> Result<Tensor> {
        let (b_size, seq_len) = input_ids.dims2()?;

        let mut xs = (self.embed_tokens.forward(input_ids)? * (self.hidden_size as f64).sqrt())?;

        let sliding_mask = if seq_len > 1 {
            Some(prepare_bidirectional_sliding_mask(
                b_size,
                seq_len,
                self.sliding_window,
                DType::F32,
                &self.device,
            )?)
        } else {
            None
        };

        for layer in &self.layers {
            let mask = if layer.is_sliding {
                sliding_mask.as_ref()
            } else {
                None
            };
            xs = layer.forward(&xs, mask)?;
        }

        self.norm.forward(&xs)
    }
}
