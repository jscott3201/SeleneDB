//! EmbeddingGemma candle-native encoder.
//!
//! Forked from `candle_transformers::models::gemma3` (decoder) with these
//! modifications for bi-directional encoding:
//!
//! - KV cache removed (encoder processes full sequence in one pass)
//! - Causal attention mask replaced with bidirectional sliding window mask
//! - LM head and logit softcapping removed
//! - Forward returns full hidden states for mean pooling (not last-token logits)
//! - All forward methods are `&self` (stateless, no mutation)
//!
//! Architecture: 24-layer Gemma 3 backbone with bidirectional attention.
//! 20 sliding-window layers (512-token window) + 4 full-attention layers,
//! in a repeating pattern of 5 sliding + 1 full.

use candle_core::{D, DType, Device, Module, Result, Tensor};
use candle_nn::{Activation, Linear, VarBuilder, linear_b as linear};

// ── Config ──────────────────────────────────────────────────────────────

#[derive(serde::Deserialize, Debug, Clone)]
pub(super) struct EmbeddingGemmaConfig {
    pub attention_bias: bool,
    pub head_dim: usize,
    pub hidden_activation: Activation,
    pub hidden_size: usize,
    pub intermediate_size: usize,
    pub num_attention_heads: usize,
    pub num_hidden_layers: usize,
    pub num_key_value_heads: usize,
    pub rms_norm_eps: f64,
    pub rope_theta: f64,
    #[serde(default = "default_rope_local")]
    pub rope_local_base_freq: f64,
    pub vocab_size: usize,
    #[serde(default)]
    pub attn_logit_softcapping: Option<f64>,
    #[allow(dead_code)] // used only during deserialization validation
    pub query_pre_attn_scalar: usize,
    #[serde(default = "default_sliding_window")]
    pub sliding_window: usize,
    /// Every Nth layer uses full attention; others use sliding window.
    /// Candle convention: `sliding_window_pattern`. HuggingFace config.json
    /// may use `_sliding_window_pattern` (underscore prefix).
    #[serde(
        default = "default_sliding_window_pattern",
        alias = "_sliding_window_pattern"
    )]
    pub sliding_window_pattern: usize,
    pub max_position_embeddings: usize,
    #[serde(default)]
    #[allow(dead_code)] // deserialized for config validation
    pub use_bidirectional_attention: bool,
    /// Explicit per-layer attention type. When present, overrides
    /// `sliding_window_pattern` derivation.
    #[serde(default)]
    pub layer_types: Option<Vec<String>>,
}

fn default_rope_local() -> f64 {
    10_000.0
}
fn default_sliding_window() -> usize {
    512
}
fn default_sliding_window_pattern() -> usize {
    6
}

impl EmbeddingGemmaConfig {
    /// Whether layer `idx` uses sliding-window attention.
    fn is_sliding_layer(&self, idx: usize) -> bool {
        if let Some(ref types) = self.layer_types {
            types.get(idx).is_none_or(|t| t != "full_attention")
        } else if self.sliding_window_pattern > 0 {
            !(idx + 1).is_multiple_of(self.sliding_window_pattern)
        } else {
            false
        }
    }
}

// ── RmsNorm ─────────────────────────────────────────────────────────────
// Gemma-specific: weight offset by +1.0

#[derive(Debug, Clone)]
struct RmsNorm {
    weight: Tensor,
    eps: f64,
}

impl RmsNorm {
    fn new(dim: usize, eps: f64, vb: VarBuilder) -> Result<Self> {
        let weight = vb.get(dim, "weight")?;
        Ok(Self { weight, eps })
    }
}

impl Module for RmsNorm {
    fn forward(&self, x: &Tensor) -> Result<Tensor> {
        let x_dtype = x.dtype();
        let internal_dtype = match x_dtype {
            DType::F16 | DType::BF16 => DType::F32,
            d => d,
        };
        let hidden_size = x.dim(D::Minus1)?;
        let x = x.to_dtype(internal_dtype)?;
        let norm_x = (x.sqr()?.sum_keepdim(D::Minus1)? / hidden_size as f64)?;
        let x_normed = x.broadcast_div(&(norm_x + self.eps)?.sqrt()?)?;
        x_normed
            .to_dtype(x_dtype)?
            .broadcast_mul(&(&self.weight + 1.0)?)
    }
}

// ── Rotary Embedding ────────────────────────────────────────────────────

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

    /// Apply rotary embeddings. Encoder always starts at position 0.
    fn apply_rotary_emb_qkv(&self, q: &Tensor, k: &Tensor) -> Result<(Tensor, Tensor)> {
        let (_b_sz, _h, seq_len, _n_embd) = q.dims4()?;
        let cos = self.cos.narrow(0, 0, seq_len)?;
        let sin = self.sin.narrow(0, 0, seq_len)?;
        let q_embed = candle_nn::rotary_emb::rope(&q.contiguous()?, &cos, &sin)?;
        let k_embed = candle_nn::rotary_emb::rope(&k.contiguous()?, &cos, &sin)?;
        Ok((q_embed, k_embed))
    }
}

// ── MLP ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(clippy::upper_case_acronyms)]
struct MLP {
    gate_proj: Linear,
    up_proj: Linear,
    down_proj: Linear,
    act_fn: Activation,
}

impl MLP {
    fn new(cfg: &EmbeddingGemmaConfig, vb: VarBuilder) -> Result<Self> {
        let hidden_sz = cfg.hidden_size;
        let intermediate_sz = cfg.intermediate_size;
        let gate_proj = linear(hidden_sz, intermediate_sz, false, vb.pp("gate_proj"))?;
        let up_proj = linear(hidden_sz, intermediate_sz, false, vb.pp("up_proj"))?;
        let down_proj = linear(intermediate_sz, hidden_sz, false, vb.pp("down_proj"))?;
        Ok(Self {
            gate_proj,
            up_proj,
            down_proj,
            act_fn: cfg.hidden_activation,
        })
    }
}

impl Module for MLP {
    fn forward(&self, xs: &Tensor) -> Result<Tensor> {
        let lhs = xs.apply(&self.gate_proj)?.apply(&self.act_fn)?;
        let rhs = xs.apply(&self.up_proj)?;
        (lhs * rhs)?.apply(&self.down_proj)
    }
}

// ── Encoder Attention ───────────────────────────────────────────────────
// No KV cache (stateless). No flash-attn.

#[derive(Debug, Clone)]
struct EncoderAttention {
    q_proj: Linear,
    k_proj: Linear,
    v_proj: Linear,
    o_proj: Linear,
    q_norm: RmsNorm,
    k_norm: RmsNorm,
    num_heads: usize,
    num_kv_heads: usize,
    num_kv_groups: usize,
    head_dim: usize,
    attn_logit_softcapping: Option<f64>,
    rotary_emb: std::sync::Arc<RotaryEmbedding>,
}

impl EncoderAttention {
    fn new(
        rotary_emb: std::sync::Arc<RotaryEmbedding>,
        cfg: &EmbeddingGemmaConfig,
        vb: VarBuilder,
    ) -> Result<Self> {
        let hidden_sz = cfg.hidden_size;
        let num_heads = cfg.num_attention_heads;
        let num_kv_heads = cfg.num_key_value_heads;
        let num_kv_groups = num_heads / num_kv_heads;
        let head_dim = cfg.head_dim;
        let bias = cfg.attention_bias;
        let q_proj = linear(hidden_sz, num_heads * head_dim, bias, vb.pp("q_proj"))?;
        let k_proj = linear(hidden_sz, num_kv_heads * head_dim, bias, vb.pp("k_proj"))?;
        let v_proj = linear(hidden_sz, num_kv_heads * head_dim, bias, vb.pp("v_proj"))?;
        let o_proj = linear(num_heads * head_dim, hidden_sz, bias, vb.pp("o_proj"))?;
        let q_norm = RmsNorm::new(head_dim, cfg.rms_norm_eps, vb.pp("q_norm"))?;
        let k_norm = RmsNorm::new(head_dim, cfg.rms_norm_eps, vb.pp("k_norm"))?;
        Ok(Self {
            q_proj,
            k_proj,
            v_proj,
            o_proj,
            q_norm,
            k_norm,
            num_heads,
            num_kv_heads,
            num_kv_groups,
            head_dim,
            attn_logit_softcapping: cfg.attn_logit_softcapping,
            rotary_emb,
        })
    }

    /// Stateless forward pass (no KV cache).
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

        let query_states = self.q_norm.forward(&query_states)?;
        let key_states = self.k_norm.forward(&key_states)?;

        let (query_states, key_states) = self
            .rotary_emb
            .apply_rotary_emb_qkv(&query_states, &key_states)?;

        // GQA: expand KV heads to match query heads
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

        attn_weights
            .matmul(&value_states)?
            .transpose(1, 2)?
            .reshape((b_sz, q_len, ()))?
            .apply(&self.o_proj)
    }
}

// ── Encoder Layer ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct EncoderLayer {
    self_attn: EncoderAttention,
    mlp: MLP,
    input_layernorm: RmsNorm,
    pre_feedforward_layernorm: RmsNorm,
    post_feedforward_layernorm: RmsNorm,
    post_attention_layernorm: RmsNorm,
    is_sliding: bool,
}

impl EncoderLayer {
    fn new(cfg: &EmbeddingGemmaConfig, vb: VarBuilder, is_sliding: bool) -> Result<Self> {
        let rotary_emb = std::sync::Arc::new(RotaryEmbedding::new(
            vb.dtype(),
            cfg,
            vb.device(),
            is_sliding,
        )?);
        let self_attn = EncoderAttention::new(rotary_emb, cfg, vb.pp("self_attn"))?;
        let mlp = MLP::new(cfg, vb.pp("mlp"))?;
        let input_layernorm =
            RmsNorm::new(cfg.hidden_size, cfg.rms_norm_eps, vb.pp("input_layernorm"))?;
        let pre_feedforward_layernorm = RmsNorm::new(
            cfg.hidden_size,
            cfg.rms_norm_eps,
            vb.pp("pre_feedforward_layernorm"),
        )?;
        let post_feedforward_layernorm = RmsNorm::new(
            cfg.hidden_size,
            cfg.rms_norm_eps,
            vb.pp("post_feedforward_layernorm"),
        )?;
        let post_attention_layernorm = RmsNorm::new(
            cfg.hidden_size,
            cfg.rms_norm_eps,
            vb.pp("post_attention_layernorm"),
        )?;
        Ok(Self {
            self_attn,
            mlp,
            input_layernorm,
            pre_feedforward_layernorm,
            post_feedforward_layernorm,
            post_attention_layernorm,
            is_sliding,
        })
    }

    fn forward(&self, xs: &Tensor, attention_mask: Option<&Tensor>) -> Result<Tensor> {
        let residual = xs;
        let xs = self.input_layernorm.forward(xs)?;
        let xs = self.self_attn.forward(&xs, attention_mask)?;
        let xs = xs.apply(&self.post_attention_layernorm)?;
        let xs = (xs + residual)?;
        let residual = &xs;
        let xs = xs.apply(&self.pre_feedforward_layernorm)?;
        let xs = xs.apply(&self.mlp)?;
        let xs = xs.apply(&self.post_feedforward_layernorm)?;
        residual + xs
    }
}

// ── Bidirectional Attention Masks ───────────────────────────────────────

/// Bidirectional sliding-window mask (no causal constraint).
///
/// Token `i` can attend to token `j` only if `|i - j| <= sliding_window`.
/// This is the key difference from the decoder: no `i < j` causal triangle.
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

// ── Top-Level Encoder ───────────────────────────────────────────────────

/// EmbeddingGemma encoder: 24-layer bidirectional transformer.
///
/// Produces full hidden states `[batch, seq_len, hidden_size]` for downstream
/// mean pooling. No LM head, no logit softcapping.
#[derive(Debug, Clone)]
pub(super) struct EmbeddingGemmaEncoder {
    embed_tokens: candle_nn::Embedding,
    layers: Vec<EncoderLayer>,
    norm: RmsNorm,
    hidden_size: usize,
    sliding_window: usize,
    dtype: DType,
    device: Device,
}

impl EmbeddingGemmaEncoder {
    pub fn load(cfg: &EmbeddingGemmaConfig, vb: VarBuilder) -> Result<Self> {
        // EmbeddingGemma sentence-transformers packaging omits the "model."
        // prefix. Try with prefix first (standard Gemma), fall back to no prefix.
        let vb_m = if vb.contains_tensor("model.embed_tokens.weight") {
            vb.pp("model")
        } else {
            vb.clone()
        };
        let embed_tokens =
            candle_nn::embedding(cfg.vocab_size, cfg.hidden_size, vb_m.pp("embed_tokens"))?;
        let mut layers = Vec::with_capacity(cfg.num_hidden_layers);
        let vb_l = vb_m.pp("layers");
        for layer_idx in 0..cfg.num_hidden_layers {
            let is_sliding = cfg.is_sliding_layer(layer_idx);
            let layer = EncoderLayer::new(cfg, vb_l.pp(layer_idx), is_sliding)?;
            layers.push(layer);
        }
        let norm = RmsNorm::new(cfg.hidden_size, cfg.rms_norm_eps, vb_m.pp("norm"))?;
        Ok(Self {
            embed_tokens,
            layers,
            norm,
            hidden_size: cfg.hidden_size,
            sliding_window: cfg.sliding_window,
            dtype: vb.dtype(),
            device: vb.device().clone(),
        })
    }

    /// Forward pass returning full hidden states `[batch, seq_len, hidden_size]`.
    pub fn forward(&self, input_ids: &Tensor) -> Result<Tensor> {
        let (b_size, seq_len) = input_ids.dims2()?;

        // Embed + scale by sqrt(hidden_size) (same as decoder)
        let mut xs = (self.embed_tokens.forward(input_ids)? * (self.hidden_size as f64).sqrt())?;

        // Pre-compute sliding mask once for all sliding layers.
        // Full-attention layers get None (bidirectional, no masking).
        let sliding_mask = if seq_len > 1 {
            Some(prepare_bidirectional_sliding_mask(
                b_size,
                seq_len,
                self.sliding_window,
                self.dtype,
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

        // Apply final norm to ALL positions (not just last token)
        self.norm.forward(&xs)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that the EmbeddingGemma config.json deserializes correctly.
    #[test]
    fn config_deserializes() {
        let json = r#"{
            "_sliding_window_pattern": 6,
            "architectures": ["Gemma3TextModel"],
            "attention_bias": false,
            "attention_dropout": 0.0,
            "attn_logit_softcapping": null,
            "bos_token_id": 2,
            "dtype": "float32",
            "eos_token_id": 1,
            "final_logit_softcapping": null,
            "head_dim": 256,
            "hidden_activation": "gelu_pytorch_tanh",
            "hidden_size": 768,
            "initializer_range": 0.02,
            "intermediate_size": 1152,
            "layer_types": [
                "sliding_attention","sliding_attention","sliding_attention",
                "sliding_attention","sliding_attention","full_attention",
                "sliding_attention","sliding_attention","sliding_attention",
                "sliding_attention","sliding_attention","full_attention",
                "sliding_attention","sliding_attention","sliding_attention",
                "sliding_attention","sliding_attention","full_attention",
                "sliding_attention","sliding_attention","sliding_attention",
                "sliding_attention","sliding_attention","full_attention"
            ],
            "max_position_embeddings": 2048,
            "model_type": "gemma3_text",
            "num_attention_heads": 3,
            "num_hidden_layers": 24,
            "num_key_value_heads": 1,
            "pad_token_id": 0,
            "query_pre_attn_scalar": 256,
            "rms_norm_eps": 1e-06,
            "rope_local_base_freq": 10000.0,
            "rope_scaling": null,
            "rope_theta": 1000000.0,
            "sliding_window": 512,
            "transformers_version": "4.57.0.dev0",
            "use_bidirectional_attention": true,
            "use_cache": true,
            "vocab_size": 262144
        }"#;

        let cfg: EmbeddingGemmaConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.num_attention_heads, 3);
        assert_eq!(cfg.num_key_value_heads, 1);
        assert_eq!(cfg.hidden_size, 768);
        assert_eq!(cfg.intermediate_size, 1152);
        assert_eq!(cfg.num_hidden_layers, 24);
        assert_eq!(cfg.head_dim, 256);
        assert_eq!(cfg.sliding_window, 512);
        assert_eq!(cfg.sliding_window_pattern, 6);
        assert!(cfg.use_bidirectional_attention);
        assert_eq!(cfg.max_position_embeddings, 2048);

        // Verify layer type derivation matches explicit layer_types
        assert!(cfg.is_sliding_layer(0)); // sliding
        assert!(cfg.is_sliding_layer(4)); // sliding
        assert!(!cfg.is_sliding_layer(5)); // full
        assert!(cfg.is_sliding_layer(6)); // sliding
        assert!(!cfg.is_sliding_layer(11)); // full
        assert!(!cfg.is_sliding_layer(23)); // full (last layer)
    }

    /// Verify the bidirectional sliding mask has correct shape and values.
    #[test]
    fn bidirectional_sliding_mask_shape_and_values() {
        let device = Device::Cpu;
        let mask = prepare_bidirectional_sliding_mask(1, 6, 2, DType::F32, &device).unwrap();
        assert_eq!(mask.dims(), &[1, 1, 6, 6]);

        let data: Vec<Vec<f32>> = (0..6)
            .map(|i| {
                mask.get(0)
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .get(i)
                    .unwrap()
                    .to_vec1::<f32>()
                    .unwrap()
            })
            .collect();

        // Token 0 can attend to 0,1,2 (distance <= 2) but not 3,4,5
        assert_eq!(data[0][0], 0.0);
        assert_eq!(data[0][2], 0.0);
        assert!(data[0][3].is_infinite() && data[0][3] < 0.0);

        // Token 3 can attend to 1,2,3,4,5 but not 0
        assert!(data[3][0].is_infinite() && data[3][0] < 0.0);
        assert_eq!(data[3][1], 0.0);
        assert_eq!(data[3][5], 0.0);

        // Symmetry: mask[i][j] == mask[j][i] (bidirectional)
        assert_eq!(data[1][4], data[4][1]);
    }
}
