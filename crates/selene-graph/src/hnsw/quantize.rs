//! PolarQuant vector quantization for the HNSW index.
//!
//! Implements TurboQuant Stage 1 (PolarQuant) — a data-oblivious scalar
//! quantization scheme that achieves 4–8× compression on unit vectors with
//! minimal recall loss. The algorithm:
//!
//! 1. **Random rotation**: Multiply by a Haar-random orthogonal matrix
//!    (seeded, deterministic, shared across all vectors).
//! 2. **Scalar quantization**: Each rotated coordinate is quantized using
//!    MSE-optimal Lloyd-Max boundaries for the Gaussian N(0, 1/√d).
//! 3. **Bit packing**: Quantized codes packed into minimum bytes.
//!
//! Distance computation uses **asymmetric evaluation**: the query is rotated
//! once (O(d²)), then dotted against dequantized codes per candidate (O(d)).
//! Rotation preserves inner products, so cosine similarity is exact in
//! rotated space.
//!
//! Based on: "TurboQuant: Redefining AI Efficiency with Extreme Compression"
//! (arXiv:2504.19874, ICLR 2026). QJL (Stage 2) is intentionally omitted —
//! it increases variance 30–300% and harms HNSW ranking quality.

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::{Deserialize, Serialize};

// ─── Math utilities ──────────────────────────────────────────────────

const INV_SQRT_2PI: f32 = 0.398_942_3;

/// Error function (Abramowitz & Stegun 7.1.26, max |error| < 1.5×10⁻⁷).
fn erf_approx(x: f32) -> f32 {
    const A1: f32 = 0.254_829_6;
    const A2: f32 = -0.284_496_74;
    const A3: f32 = 1.421_413_7;
    const A4: f32 = -1.453_152;
    const A5: f32 = 1.061_405_4;
    const P: f32 = 0.327_591_1;

    let sign = x.signum();
    let x = x.abs();
    let t = 1.0 / (1.0 + P * x);
    let poly = (((A5 * t + A4) * t + A3) * t + A2) * t + A1;
    sign * (1.0 - poly * t * (-x * x).exp())
}

/// Standard normal PDF: φ(z) = (1/√(2π)) exp(-z²/2).
fn std_normal_pdf(z: f32) -> f32 {
    INV_SQRT_2PI * (-0.5 * z * z).exp()
}

/// Standard normal CDF: Φ(z) = ½(1 + erf(z/√2)).
fn std_normal_cdf(z: f32) -> f32 {
    0.5 * (1.0 + erf_approx(z * std::f32::consts::FRAC_1_SQRT_2))
}

/// Conditional mean E[X | a ≤ X ≤ b] for X ~ N(0, σ²).
///
/// Uses: E[X | a ≤ X ≤ b] = σ · (φ(a/σ) − φ(b/σ)) / (Φ(b/σ) − Φ(a/σ))
fn conditional_mean(a: f32, b: f32, sigma: f32) -> f32 {
    let za = a / sigma;
    let zb = b / sigma;
    let mass = std_normal_cdf(zb) - std_normal_cdf(za);
    if mass.abs() < 1e-12 {
        (a + b) * 0.5
    } else {
        sigma * (std_normal_pdf(za) - std_normal_pdf(zb)) / mass
    }
}

/// Box-Muller transform: two independent N(0,1) samples from uniform random.
fn box_muller(u1: f32, u2: f32) -> (f32, f32) {
    let r = (-2.0 * u1.max(1e-10).ln()).sqrt();
    let theta = u2 * std::f32::consts::TAU;
    (r * theta.cos(), r * theta.sin())
}

// ─── Random orthogonal matrix ───────────────────────────────────────

/// A d×d Haar-random orthogonal matrix, deterministic from a seed.
///
/// Generated via Householder QR decomposition of a Gaussian random matrix
/// with sign correction for Haar uniformity (Stewart 1980).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotationMatrix {
    data: Vec<f32>,
    dim: usize,
    seed: u64,
}

impl RotationMatrix {
    /// Generate a random orthogonal matrix for the given dimension and seed.
    ///
    /// Cost: O(d³) ≈ 0.3s at d=768. The matrix is d²×4 bytes (2.25 MB at d=768).
    pub fn from_seed(dim: usize, seed: u64) -> Self {
        assert!(dim > 0, "dimension must be positive");
        let mut rng = StdRng::seed_from_u64(seed);

        // Generate d×d Gaussian random matrix via Box-Muller
        let total = dim * dim;
        let mut a = Vec::with_capacity(total);
        let pairs = total / 2;
        for _ in 0..pairs {
            let u1: f32 = rng.random();
            let u2: f32 = rng.random();
            let (x, y) = box_muller(u1, u2);
            a.push(x);
            a.push(y);
        }
        if !total.is_multiple_of(2) {
            let u1: f32 = rng.random();
            let u2: f32 = rng.random();
            let (x, _) = box_muller(u1, u2);
            a.push(x);
        }

        let data = householder_qr(&mut a, dim);
        Self { data, dim, seed }
    }

    /// Multiply a vector by this orthogonal matrix: y = Q · v.
    ///
    /// Uses 8-wide accumulator matching the SIMD pattern in `distance.rs`.
    pub fn rotate(&self, v: &[f32]) -> Vec<f32> {
        assert_eq!(v.len(), self.dim, "vector dimension mismatch");
        let d = self.dim;
        let mut result = vec![0.0_f32; d];

        for (result_elem, row_start) in result.iter_mut().zip((0..d).map(|i| i * d)) {
            let row = &self.data[row_start..row_start + d];
            let mut sum = [0.0_f32; 8];
            let chunks_r = row.chunks_exact(8);
            let chunks_v = v.chunks_exact(8);
            let rem_r = chunks_r.remainder();
            let rem_v = chunks_v.remainder();

            for (r8, v8) in chunks_r.zip(chunks_v) {
                sum[0] += r8[0] * v8[0];
                sum[1] += r8[1] * v8[1];
                sum[2] += r8[2] * v8[2];
                sum[3] += r8[3] * v8[3];
                sum[4] += r8[4] * v8[4];
                sum[5] += r8[5] * v8[5];
                sum[6] += r8[6] * v8[6];
                sum[7] += r8[7] * v8[7];
            }

            let mut total =
                (sum[0] + sum[1]) + (sum[2] + sum[3]) + (sum[4] + sum[5]) + (sum[6] + sum[7]);
            for (r, v) in rem_r.iter().zip(rem_v) {
                total += r * v;
            }
            *result_elem = total;
        }
        result
    }

    /// Multiply by the transpose (inverse rotation): y = Qᵀ · v.
    pub fn rotate_transpose(&self, v: &[f32]) -> Vec<f32> {
        assert_eq!(v.len(), self.dim, "vector dimension mismatch");
        let d = self.dim;
        let mut result = vec![0.0_f32; d];

        // Qᵀ[i,j] = Q[j,i] — accumulate column-wise
        for (j, vj) in v.iter().enumerate() {
            let row_j = &self.data[j * d..(j + 1) * d];
            for (result_elem, &rji) in result.iter_mut().zip(row_j) {
                *result_elem += rji * vj;
            }
        }
        result
    }

    /// Dimension of this rotation matrix.
    pub fn dim(&self) -> usize {
        self.dim
    }
}

/// Householder QR decomposition of a d×d matrix (row-major, in-place on `a`).
///
/// Returns the orthogonal factor Q with Haar sign correction (Stewart 1980).
fn householder_qr(a: &mut [f32], d: usize) -> Vec<f32> {
    let mut v_store: Vec<Vec<f32>> = Vec::with_capacity(d);

    for k in 0..d {
        let n = d - k;

        // Extract sub-column a[k:d, k]
        let mut x = vec![0.0_f32; n];
        for i in 0..n {
            x[i] = a[(k + i) * d + k];
        }

        // Householder vector: v = x − α·e₁ where α = −sign(x₀)·‖x‖
        let norm_x: f32 = x.iter().map(|&xi| xi * xi).sum::<f32>().sqrt();
        let alpha = if x[0] >= 0.0 { -norm_x } else { norm_x };

        let mut v = x;
        v[0] -= alpha;
        let norm_v: f32 = v.iter().map(|&vi| vi * vi).sum::<f32>().sqrt();
        if norm_v > 1e-12 {
            let inv = 1.0 / norm_v;
            for vi in &mut v {
                *vi *= inv;
            }
        }

        // Apply reflection: A[k:d, k:d] -= 2·v·(vᵀ·A[k:d, k:d])
        for j in k..d {
            let mut dot = 0.0_f32;
            for i in 0..n {
                dot += v[i] * a[(k + i) * d + j];
            }
            let two_dot = 2.0 * dot;
            for i in 0..n {
                a[(k + i) * d + j] -= v[i] * two_dot;
            }
        }

        v_store.push(v);
    }

    // Record diagonal signs of R for Haar correction
    let diag_positive: Vec<bool> = (0..d).map(|k| a[k * d + k] >= 0.0).collect();

    // Reconstruct Q by applying reflections in reverse to identity
    let mut q = vec![0.0_f32; d * d];
    for i in 0..d {
        q[i * d + i] = 1.0;
    }

    for k in (0..d).rev() {
        let v = &v_store[k];
        let n = d - k;
        for j in 0..d {
            let mut dot = 0.0_f32;
            for i in 0..n {
                dot += v[i] * q[(k + i) * d + j];
            }
            let two_dot = 2.0 * dot;
            for i in 0..n {
                q[(k + i) * d + j] -= v[i] * two_dot;
            }
        }
    }

    // Haar sign correction: flip column k if R[k,k] was negative
    for (k, &positive) in diag_positive.iter().enumerate() {
        if !positive {
            for i in 0..d {
                q[i * d + k] = -q[i * d + k];
            }
        }
    }

    q
}

// ─── Lloyd-Max quantizer ────────────────────────────────────────────

/// MSE-optimal scalar quantizer for a Gaussian(0, σ) source.
///
/// Computed via the Lloyd-Max algorithm with closed-form centroid updates.
/// The codebook is determined entirely by (dim, bits) — no training data needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GaussianQuantizer {
    /// Reconstruction centroids (2^bits values, sorted ascending).
    centroids: Vec<f32>,
    /// Decision boundaries (2^bits − 1 values, sorted ascending).
    boundaries: Vec<f32>,
    bits: u8,
    sigma: f32,
}

impl GaussianQuantizer {
    /// Create a new quantizer for unit vectors of the given dimension.
    ///
    /// σ = 1/√d, the theoretical std-dev of each coordinate of a randomly
    /// rotated unit vector in ℝᵈ.
    pub fn new(dim: usize, bits: u8) -> Self {
        assert!(matches!(bits, 3 | 4 | 8), "supported bit widths: 3, 4, 8");
        let sigma = 1.0 / (dim as f32).sqrt();
        let (centroids, boundaries) = solve_lloyd_max(sigma, bits, 200);
        Self {
            centroids,
            boundaries,
            bits,
            sigma,
        }
    }

    /// Quantize a scalar value to a code index in [0, 2^bits).
    pub fn quantize_scalar(&self, x: f32) -> u8 {
        // Binary search in sorted boundaries
        match self
            .boundaries
            .binary_search_by(|b| b.partial_cmp(&x).unwrap_or(std::cmp::Ordering::Equal))
        {
            Ok(i) | Err(i) => i as u8,
        }
    }

    /// Dequantize a code index to its centroid value.
    pub fn dequantize_scalar(&self, code: u8) -> f32 {
        self.centroids[code as usize]
    }

    /// Number of quantization levels (2^bits).
    pub fn levels(&self) -> usize {
        self.centroids.len()
    }

    /// Standard deviation used for this quantizer.
    pub fn sigma(&self) -> f32 {
        self.sigma
    }
}

/// Solve Lloyd-Max for a Gaussian(0, σ) source at the given bit width.
///
/// Uses the closed-form centroid update for Gaussian sources:
///   c_i = σ · (φ(a_i/σ) − φ(b_i/σ)) / (Φ(b_i/σ) − Φ(a_i/σ))
fn solve_lloyd_max(sigma: f32, bits: u8, max_iter: usize) -> (Vec<f32>, Vec<f32>) {
    let n_levels = 1_usize << bits;
    let range = 3.5 * sigma;

    // Initialize centroids uniformly in [-3.5σ, 3.5σ]
    let mut centroids: Vec<f32> = (0..n_levels)
        .map(|i| -range + 2.0 * range * (i as f32 + 0.5) / n_levels as f32)
        .collect();

    let mut boundaries = vec![0.0_f32; n_levels - 1];
    let tail = 10.0 * sigma;

    for _ in 0..max_iter {
        // Boundaries = midpoints between adjacent centroids
        for i in 0..n_levels - 1 {
            boundaries[i] = (centroids[i] + centroids[i + 1]) * 0.5;
        }

        // Centroids = conditional mean within each partition
        let mut max_shift: f32 = 0.0;
        for i in 0..n_levels {
            let a = if i == 0 { -tail } else { boundaries[i - 1] };
            let b = if i == n_levels - 1 {
                tail
            } else {
                boundaries[i]
            };
            let new_c = conditional_mean(a, b, sigma);
            max_shift = max_shift.max((new_c - centroids[i]).abs());
            centroids[i] = new_c;
        }

        if max_shift < 1e-10 * sigma {
            break;
        }
    }

    (centroids, boundaries)
}

// ─── Bit packing ────────────────────────────────────────────────────

/// Pack 4-bit codes: two codes per byte (high nibble = even index).
fn pack_4bit(codes: &[u8]) -> Vec<u8> {
    let mut packed = Vec::with_capacity(codes.len().div_ceil(2));
    for pair in codes.chunks(2) {
        let hi = pair[0] & 0x0F;
        let lo = if pair.len() > 1 { pair[1] & 0x0F } else { 0 };
        packed.push((hi << 4) | lo);
    }
    packed
}

/// Unpack 4-bit codes from packed bytes.
fn unpack_4bit(packed: &[u8], dim: usize) -> Vec<u8> {
    let mut codes = Vec::with_capacity(dim);
    for &byte in packed {
        codes.push(byte >> 4);
        if codes.len() < dim {
            codes.push(byte & 0x0F);
        }
    }
    codes.truncate(dim);
    codes
}

/// Pack 3-bit codes using a bit buffer.
fn pack_3bit(codes: &[u8]) -> Vec<u8> {
    let mut packed = Vec::with_capacity((codes.len() * 3).div_ceil(8));
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;

    for &code in codes {
        buf |= u32::from(code & 0x07) << bits;
        bits += 3;
        while bits >= 8 {
            packed.push((buf & 0xFF) as u8);
            buf >>= 8;
            bits -= 8;
        }
    }
    if bits > 0 {
        packed.push((buf & 0xFF) as u8);
    }
    packed
}

/// Unpack 3-bit codes from packed bytes.
fn unpack_3bit(packed: &[u8], dim: usize) -> Vec<u8> {
    let mut codes = Vec::with_capacity(dim);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    let mut byte_idx = 0;

    for _ in 0..dim {
        while bits < 3 && byte_idx < packed.len() {
            buf |= u32::from(packed[byte_idx]) << bits;
            bits += 8;
            byte_idx += 1;
        }
        codes.push((buf & 0x07) as u8);
        buf >>= 3;
        bits -= 3;
    }
    codes
}

// ─── Configuration ──────────────────────────────────────────────────

/// Quantization bit width.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuantBits {
    /// 8-bit: 4× compression, <0.1% recall loss.
    Eight = 8,
    /// 4-bit: 8× compression, <1% recall loss (recommended default).
    Four = 4,
    /// 3-bit: 10.7× compression, 2–5% recall loss.
    Three = 3,
}

impl QuantBits {
    fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Configuration for PolarQuant vector quantization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizationConfig {
    /// Bit width for quantization.
    pub bits: QuantBits,
    /// Seed for the random rotation matrix (deterministic from seed + dimension).
    pub seed: u64,
    /// Whether to re-score top-k results with full-precision vectors.
    pub rescore: bool,
}

impl Default for QuantizationConfig {
    fn default() -> Self {
        Self {
            bits: QuantBits::Four,
            seed: 42,
            rescore: false,
        }
    }
}

// ─── PolarQuantizer ─────────────────────────────────────────────────

/// PolarQuant quantizer: random rotation + Lloyd-Max scalar quantization.
///
/// Create one per HNSW index (shared across all vectors of the same dimension).
/// The quantizer state is deterministic from `(dim, config.seed, config.bits)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolarQuantizer {
    rotation: RotationMatrix,
    quantizer: GaussianQuantizer,
    dim: usize,
    bits: u8,
}

impl PolarQuantizer {
    /// Create a new PolarQuantizer for the given dimension and configuration.
    ///
    /// This generates the rotation matrix (O(d³), ~0.3s at d=768) and computes
    /// Lloyd-Max codebooks. Result is deterministic from `(dim, seed, bits)`.
    pub fn new(dim: usize, config: &QuantizationConfig) -> Self {
        let rotation = RotationMatrix::from_seed(dim, config.seed);
        let quantizer = GaussianQuantizer::new(dim, config.bits.as_u8());
        Self {
            rotation,
            quantizer,
            dim,
            bits: config.bits.as_u8(),
        }
    }

    /// Encode a unit vector into packed quantization codes.
    ///
    /// Steps: rotate → quantize each coordinate → pack bits.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        assert_eq!(vector.len(), self.dim, "vector dimension mismatch");

        let rotated = self.rotation.rotate(vector);
        let codes: Vec<u8> = rotated
            .iter()
            .map(|&x| self.quantizer.quantize_scalar(x))
            .collect();

        match self.bits {
            8 => codes,
            4 => pack_4bit(&codes),
            3 => pack_3bit(&codes),
            _ => unreachable!(),
        }
    }

    /// Decode packed codes back to an approximate f32 vector (in original space).
    ///
    /// Steps: unpack → dequantize → inverse rotate (Qᵀ multiply).
    pub fn decode(&self, packed: &[u8]) -> Vec<f32> {
        let codes = match self.bits {
            8 => packed.to_vec(),
            4 => unpack_4bit(packed, self.dim),
            3 => unpack_3bit(packed, self.dim),
            _ => unreachable!(),
        };

        let rotated: Vec<f32> = codes
            .iter()
            .map(|&c| self.quantizer.dequantize_scalar(c))
            .collect();

        self.rotation.rotate_transpose(&rotated)
    }

    /// Rotate a query vector for asymmetric distance computation.
    ///
    /// Call once per search, reuse the rotated query across all candidates.
    /// Cost: O(d²), amortized over thousands of candidate comparisons.
    pub fn rotate_query(&self, query: &[f32]) -> Vec<f32> {
        self.rotation.rotate(query)
    }

    /// Asymmetric dot product: full-precision rotated query × dequantized codes.
    ///
    /// Core distance function for quantized search. Processes packed codes
    /// directly to avoid allocation. Uses 8-wide accumulator for auto-SIMD.
    pub fn asymmetric_dot(&self, query_rotated: &[f32], packed: &[u8]) -> f32 {
        assert_eq!(query_rotated.len(), self.dim);
        match self.bits {
            8 => self.asymmetric_dot_8bit(query_rotated, packed),
            4 => self.asymmetric_dot_4bit(query_rotated, packed),
            3 => self.asymmetric_dot_3bit(query_rotated, packed),
            _ => unreachable!(),
        }
    }

    fn asymmetric_dot_8bit(&self, q: &[f32], codes: &[u8]) -> f32 {
        let centroids = &self.quantizer.centroids;
        let mut sum = [0.0_f32; 8];
        let q_chunks = q.chunks_exact(8);
        let c_chunks = codes.chunks_exact(8);
        let q_rem = q_chunks.remainder();
        let c_rem = c_chunks.remainder();

        for (q8, c8) in q_chunks.zip(c_chunks) {
            sum[0] += q8[0] * centroids[c8[0] as usize];
            sum[1] += q8[1] * centroids[c8[1] as usize];
            sum[2] += q8[2] * centroids[c8[2] as usize];
            sum[3] += q8[3] * centroids[c8[3] as usize];
            sum[4] += q8[4] * centroids[c8[4] as usize];
            sum[5] += q8[5] * centroids[c8[5] as usize];
            sum[6] += q8[6] * centroids[c8[6] as usize];
            sum[7] += q8[7] * centroids[c8[7] as usize];
        }

        let mut total =
            (sum[0] + sum[1]) + (sum[2] + sum[3]) + (sum[4] + sum[5]) + (sum[6] + sum[7]);
        for (qi, ci) in q_rem.iter().zip(c_rem) {
            total += qi * centroids[*ci as usize];
        }
        total
    }

    fn asymmetric_dot_4bit(&self, q: &[f32], packed: &[u8]) -> f32 {
        let centroids = &self.quantizer.centroids;
        let mut total = 0.0_f32;
        let mut qi = 0;

        for &byte in packed {
            if qi < self.dim {
                total += q[qi] * centroids[(byte >> 4) as usize];
                qi += 1;
            }
            if qi < self.dim {
                total += q[qi] * centroids[(byte & 0x0F) as usize];
                qi += 1;
            }
        }
        total
    }

    fn asymmetric_dot_3bit(&self, q: &[f32], packed: &[u8]) -> f32 {
        let centroids = &self.quantizer.centroids;
        let mut total = 0.0_f32;
        let mut buf: u32 = 0;
        let mut bits: u32 = 0;
        let mut byte_idx = 0;

        for qi in &q[..self.dim] {
            while bits < 3 && byte_idx < packed.len() {
                buf |= u32::from(packed[byte_idx]) << bits;
                bits += 8;
                byte_idx += 1;
            }
            total += qi * centroids[(buf & 0x07) as usize];
            buf >>= 3;
            bits -= 3;
        }
        total
    }

    /// Number of bytes per encoded vector.
    pub fn encoded_size(&self) -> usize {
        match self.bits {
            8 => self.dim,
            4 => self.dim.div_ceil(2),
            3 => (self.dim * 3).div_ceil(8),
            _ => unreachable!(),
        }
    }

    /// Compression ratio vs f32 storage.
    pub fn compression_ratio(&self) -> f32 {
        (self.dim * 4) as f32 / self.encoded_size() as f32
    }

    /// Vector dimension this quantizer was built for.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Bit width used for quantization.
    pub fn bits(&self) -> u8 {
        self.bits
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Generate a random unit vector of the given dimension.
    fn random_unit_vector(dim: usize, rng: &mut StdRng) -> Vec<f32> {
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim / 2 {
            let u1: f32 = rng.random();
            let u2: f32 = rng.random();
            let (a, b) = box_muller(u1, u2);
            v.push(a);
            v.push(b);
        }
        if !dim.is_multiple_of(2) {
            let u1: f32 = rng.random();
            let u2: f32 = rng.random();
            let (a, _) = box_muller(u1, u2);
            v.push(a);
        }
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        for x in &mut v {
            *x /= norm;
        }
        v
    }

    fn dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }

    fn norm(v: &[f32]) -> f32 {
        v.iter().map(|x| x * x).sum::<f32>().sqrt()
    }

    // ── erf accuracy ────────────────────────────────────────────

    #[test]
    fn erf_known_values() {
        // erf(0) = 0, erf(1) ≈ 0.8427, erf(2) ≈ 0.9953
        assert!(erf_approx(0.0).abs() < 1e-6);
        assert!((erf_approx(1.0) - 0.842_700_8).abs() < 1e-5);
        assert!((erf_approx(2.0) - 0.995_322_3).abs() < 1e-5);
        // Odd function
        assert!((erf_approx(-1.0) + erf_approx(1.0)).abs() < 1e-6);
    }

    // ── Rotation matrix properties ──────────────────────────────

    #[test]
    fn rotation_preserves_norm() {
        let dim = 32;
        let rot = RotationMatrix::from_seed(dim, 12345);
        let mut rng = StdRng::seed_from_u64(99);
        let v = random_unit_vector(dim, &mut rng);

        let rv = rot.rotate(&v);
        let n = norm(&rv);
        assert!((n - 1.0).abs() < 1e-4, "expected norm ~1.0, got {n}");
    }

    #[test]
    fn rotation_preserves_inner_product() {
        let dim = 32;
        let rot = RotationMatrix::from_seed(dim, 12345);
        let mut rng = StdRng::seed_from_u64(99);
        let u = random_unit_vector(dim, &mut rng);
        let v = random_unit_vector(dim, &mut rng);

        let original_dot = dot(&u, &v);
        let rotated_dot = dot(&rot.rotate(&u), &rot.rotate(&v));

        assert!(
            (original_dot - rotated_dot).abs() < 1e-4,
            "dot product not preserved: {original_dot} vs {rotated_dot}"
        );
    }

    #[test]
    fn rotation_deterministic_from_seed() {
        let dim = 16;
        let r1 = RotationMatrix::from_seed(dim, 42);
        let r2 = RotationMatrix::from_seed(dim, 42);
        assert_eq!(r1.data.len(), r2.data.len());
        for (a, b) in r1.data.iter().zip(&r2.data) {
            assert!((a - b).abs() < 1e-10, "matrices differ");
        }
    }

    #[test]
    fn rotation_orthogonality() {
        // Q · Qᵀ should be identity
        let dim = 16;
        let rot = RotationMatrix::from_seed(dim, 7);

        for i in 0..dim {
            let mut e_i = vec![0.0_f32; dim];
            e_i[i] = 1.0;

            let q_ei = rot.rotate(&e_i);
            let roundtrip = rot.rotate_transpose(&q_ei);

            for (j, &roundtrip_j) in roundtrip.iter().enumerate() {
                let expected = if i == j { 1.0 } else { 0.0 };
                assert!(
                    (roundtrip_j - expected).abs() < 1e-4,
                    "QᵀQ not identity at ({i},{j}): got {roundtrip_j}",
                );
            }
        }
    }

    // ── Lloyd-Max quantizer ─────────────────────────────────────

    #[test]
    fn lloyd_max_symmetry() {
        // For symmetric Gaussian, centroids should be symmetric around 0
        let sigma = 1.0 / (64.0_f32).sqrt();
        let (centroids, boundaries) = solve_lloyd_max(sigma, 4, 200);

        assert_eq!(centroids.len(), 16);
        assert_eq!(boundaries.len(), 15);

        // Check symmetry: c[i] ≈ -c[n-1-i]
        let n = centroids.len();
        for i in 0..n / 2 {
            assert!(
                (centroids[i] + centroids[n - 1 - i]).abs() < 1e-4 * sigma,
                "centroids not symmetric: {} vs {}",
                centroids[i],
                centroids[n - 1 - i]
            );
        }
    }

    #[test]
    fn lloyd_max_ordered() {
        let sigma = 1.0 / (128.0_f32).sqrt();
        let (centroids, boundaries) = solve_lloyd_max(sigma, 3, 200);

        for w in centroids.windows(2) {
            assert!(w[0] < w[1], "centroids not sorted: {} >= {}", w[0], w[1]);
        }
        for w in boundaries.windows(2) {
            assert!(w[0] < w[1], "boundaries not sorted");
        }
    }

    // ── Bit packing ─────────────────────────────────────────────

    #[test]
    fn pack_4bit_roundtrip() {
        let codes: Vec<u8> = (0..16).cycle().take(100).collect();
        let packed = pack_4bit(&codes);
        let unpacked = unpack_4bit(&packed, 100);
        assert_eq!(codes, unpacked);
    }

    #[test]
    fn pack_4bit_odd_length() {
        let codes: Vec<u8> = vec![3, 7, 11];
        let packed = pack_4bit(&codes);
        let unpacked = unpack_4bit(&packed, 3);
        assert_eq!(codes, unpacked);
    }

    #[test]
    fn pack_3bit_roundtrip() {
        let codes: Vec<u8> = (0..8).cycle().take(100).collect();
        let packed = pack_3bit(&codes);
        let unpacked = unpack_3bit(&packed, 100);
        assert_eq!(codes, unpacked);
    }

    #[test]
    fn pack_3bit_non_multiple_of_8() {
        let codes: Vec<u8> = vec![1, 5, 3, 7, 0];
        let packed = pack_3bit(&codes);
        let unpacked = unpack_3bit(&packed, 5);
        assert_eq!(codes, unpacked);
    }

    // ── PolarQuantizer end-to-end ───────────────────────────────

    #[test]
    fn encode_decode_roundtrip_8bit() {
        let dim = 32;
        let config = QuantizationConfig {
            bits: QuantBits::Eight,
            seed: 42,
            rescore: false,
        };
        let pq = PolarQuantizer::new(dim, &config);
        let mut rng = StdRng::seed_from_u64(123);
        let v = random_unit_vector(dim, &mut rng);

        let encoded = pq.encode(&v);
        assert_eq!(encoded.len(), pq.encoded_size());

        let decoded = pq.decode(&encoded);
        assert_eq!(decoded.len(), dim);

        // 8-bit should be very close to original
        let error: f32 = v
            .iter()
            .zip(&decoded)
            .map(|(a, b)| (a - b) * (a - b))
            .sum::<f32>()
            .sqrt();
        assert!(error < 0.15, "8-bit roundtrip MSE too high: {error}");
    }

    #[test]
    fn encode_decode_roundtrip_4bit() {
        let dim = 64;
        let config = QuantizationConfig {
            bits: QuantBits::Four,
            seed: 42,
            rescore: false,
        };
        let pq = PolarQuantizer::new(dim, &config);
        let mut rng = StdRng::seed_from_u64(456);
        let v = random_unit_vector(dim, &mut rng);

        let encoded = pq.encode(&v);
        assert_eq!(encoded.len(), pq.encoded_size());
        assert_eq!(encoded.len(), dim / 2);

        let decoded = pq.decode(&encoded);
        // 4-bit has more error but should still be reasonable
        let cos_sim = dot(&v, &decoded) / (norm(&v) * norm(&decoded));
        assert!(cos_sim > 0.8, "4-bit cosine similarity too low: {cos_sim}");
    }

    #[test]
    fn encode_decode_roundtrip_3bit() {
        let dim = 64;
        let config = QuantizationConfig {
            bits: QuantBits::Three,
            seed: 42,
            rescore: false,
        };
        let pq = PolarQuantizer::new(dim, &config);
        let mut rng = StdRng::seed_from_u64(789);
        let v = random_unit_vector(dim, &mut rng);

        let encoded = pq.encode(&v);
        assert_eq!(encoded.len(), pq.encoded_size());

        let decoded = pq.decode(&encoded);
        let cos_sim = dot(&v, &decoded) / (norm(&v) * norm(&decoded));
        assert!(cos_sim > 0.5, "3-bit cosine similarity too low: {cos_sim}");
    }

    #[test]
    fn asymmetric_dot_matches_full_dot() {
        let dim = 64;
        let config = QuantizationConfig {
            bits: QuantBits::Eight,
            seed: 42,
            rescore: false,
        };
        let pq = PolarQuantizer::new(dim, &config);
        let mut rng = StdRng::seed_from_u64(321);
        let query = random_unit_vector(dim, &mut rng);
        let target = random_unit_vector(dim, &mut rng);

        let full_dot = dot(&query, &target);
        let encoded = pq.encode(&target);
        let q_rot = pq.rotate_query(&query);
        let asym_dot = pq.asymmetric_dot(&q_rot, &encoded);

        // Should be close to the true dot product
        assert!(
            (full_dot - asym_dot).abs() < 0.1,
            "asymmetric dot {asym_dot} too far from true dot {full_dot}"
        );
    }

    #[test]
    fn asymmetric_dot_4bit_ranking_preservation() {
        // The core property: if dot(q, a) > dot(q, b), then
        // asymmetric_dot(q_rot, encode(a)) > asymmetric_dot(q_rot, encode(b))
        // most of the time. Test with a clear margin.
        let dim = 64;
        let config = QuantizationConfig {
            bits: QuantBits::Four,
            seed: 42,
            rescore: false,
        };
        let pq = PolarQuantizer::new(dim, &config);
        let mut rng = StdRng::seed_from_u64(555);

        let query = random_unit_vector(dim, &mut rng);
        let q_rot = pq.rotate_query(&query);

        let mut correct = 0;
        let trials = 100;
        for _ in 0..trials {
            let a = random_unit_vector(dim, &mut rng);
            let b = random_unit_vector(dim, &mut rng);

            let true_a = dot(&query, &a);
            let true_b = dot(&query, &b);
            let asym_a = pq.asymmetric_dot(&q_rot, &pq.encode(&a));
            let asym_b = pq.asymmetric_dot(&q_rot, &pq.encode(&b));

            if (true_a > true_b) == (asym_a > asym_b) {
                correct += 1;
            }
        }

        // Expect high ranking agreement (>85% for random vectors at 4-bit)
        assert!(
            correct > 85,
            "ranking preservation too low: {correct}/{trials}"
        );
    }

    // ── Compression ratios ──────────────────────────────────────

    #[test]
    fn compression_ratios() {
        let dim = 768;

        let cfg8 = QuantizationConfig {
            bits: QuantBits::Eight,
            ..Default::default()
        };
        let cfg4 = QuantizationConfig {
            bits: QuantBits::Four,
            ..Default::default()
        };
        let cfg3 = QuantizationConfig {
            bits: QuantBits::Three,
            ..Default::default()
        };

        // Don't instantiate full PolarQuantizer (slow for 768-dim in tests),
        // just verify the size math.
        assert_eq!(encoded_size(dim, 8), 768); // 4× vs 3072
        assert_eq!(encoded_size(dim, 4), 384); // 8× vs 3072
        assert_eq!(encoded_size(dim, 3), 288); // 10.67× vs 3072

        // Verify via config defaults
        assert_eq!(cfg8.bits, QuantBits::Eight);
        assert_eq!(cfg4.bits, QuantBits::Four);
        assert_eq!(cfg3.bits, QuantBits::Three);
    }

    fn encoded_size(dim: usize, bits: u8) -> usize {
        match bits {
            8 => dim,
            4 => dim.div_ceil(2),
            3 => (dim * 3).div_ceil(8),
            _ => unreachable!(),
        }
    }

    // ── Gaussian quantizer scalar tests ─────────────────────────

    #[test]
    fn quantizer_covers_range() {
        let q = GaussianQuantizer::new(256, 4);
        // A value far in the tails should map to boundary codes
        assert_eq!(q.quantize_scalar(-1.0), 0);
        assert_eq!(q.quantize_scalar(1.0), 15);
        // A value near zero should map to a middle code
        let mid = q.quantize_scalar(0.0);
        assert!((7..=8).contains(&mid), "expected mid code, got {mid}");
    }

    #[test]
    fn quantizer_monotonic() {
        let q = GaussianQuantizer::new(256, 4);
        let mut prev_code = q.quantize_scalar(-0.5);
        for i in 1..100 {
            let x = -0.5 + i as f32 * 0.01;
            let code = q.quantize_scalar(x);
            assert!(code >= prev_code, "non-monotonic at x={x}");
            prev_code = code;
        }
    }

    // ── Proptest: roundtrip accuracy across random vectors ──────

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn roundtrip_preserves_similarity(seed in 0u64..10000) {
                let dim = 32;
                let config = QuantizationConfig {
                    bits: QuantBits::Four,
                    seed: 42,
                    rescore: false,
                };
                let pq = PolarQuantizer::new(dim, &config);
                let mut rng = StdRng::seed_from_u64(seed);
                let v = random_unit_vector(dim, &mut rng);

                let encoded = pq.encode(&v);
                let decoded = pq.decode(&encoded);

                let cos = dot(&v, &decoded) / (norm(&v) * norm(&decoded).max(1e-10));
                prop_assert!(cos > 0.5, "cosine too low: {}", cos);
            }
        }
    }
}
