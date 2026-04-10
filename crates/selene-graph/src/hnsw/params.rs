//! HNSW index configuration parameters.

use super::quantize::QuantizationConfig;

/// Configuration for the HNSW index.
#[derive(Debug, Clone)]
pub struct HnswParams {
    /// Max bidirectional connections per node per layer (default: 16).
    pub m: usize,
    /// Max connections at layer 0, the densest layer (default: 2*M = 32).
    pub m0: usize,
    /// Search width during index construction (default: 200).
    pub ef_construction: usize,
    /// Default search width during query (default: 50, overridable per-query).
    pub ef_search: usize,
    /// Layer assignment probability factor: 1.0 / ln(M).
    pub level_factor: f64,
    /// Optional PolarQuant vector quantization. When `Some`, vectors are
    /// quantized after build/insert for compressed asymmetric search.
    pub quantization: Option<QuantizationConfig>,
}

impl HnswParams {
    pub fn new(m: usize) -> Self {
        Self {
            m,
            m0: m * 2,
            ef_construction: 200,
            ef_search: 50,
            level_factor: 1.0 / (m as f64).ln(),
            quantization: None,
        }
    }

    pub fn max_neighbors(&self, layer: u8) -> usize {
        if layer == 0 { self.m0 } else { self.m }
    }
}

impl Default for HnswParams {
    fn default() -> Self {
        Self::new(16)
    }
}

impl HnswParams {
    /// Create params with quantization enabled.
    pub fn with_quantization(mut self, config: QuantizationConfig) -> Self {
        self.quantization = Some(config);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_params() {
        let p = HnswParams::default();
        assert_eq!(p.m, 16);
        assert_eq!(p.m0, 32);
        assert_eq!(p.ef_construction, 200);
        assert_eq!(p.ef_search, 50);
    }

    #[test]
    fn max_neighbors_layer_0_uses_m0() {
        let p = HnswParams::default();
        assert_eq!(p.max_neighbors(0), p.m0);
        assert_eq!(p.max_neighbors(1), p.m);
        assert_eq!(p.max_neighbors(5), p.m);
    }

    #[test]
    fn level_factor_scales_with_m() {
        let p8 = HnswParams::new(8);
        let p16 = HnswParams::new(16);
        let p32 = HnswParams::new(32);

        // level_factor = 1 / ln(m); larger m => smaller level_factor
        assert!(p8.level_factor > p16.level_factor);
        assert!(p16.level_factor > p32.level_factor);

        // Verify the formula: level_factor * ln(m) == 1.0
        assert!((p8.level_factor * (8_f64).ln() - 1.0).abs() < 1e-10);
        assert!((p16.level_factor * (16_f64).ln() - 1.0).abs() < 1e-10);
        assert!((p32.level_factor * (32_f64).ln() - 1.0).abs() < 1e-10);
    }
}
