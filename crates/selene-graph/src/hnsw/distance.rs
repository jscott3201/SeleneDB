//! Vector distance and similarity functions for HNSW search.
//!
//! All functions use an 8-wide accumulator pattern that LLVM auto-vectorizes
//! to ARM NEON, x86 SSE/AVX, and WASM SIMD without intrinsics or nightly
//! features. For 384-dim vectors (all-MiniLM-L6-v2): 384/8 = 48 iterations,
//! zero remainder.

/// Dot product of two f32 slices using an 8-wide accumulator.
///
/// Eight independent accumulators break the sequential reduction dependency
/// chain and saturate dual-issue FMA pipelines (M1: 2 NEON FMA/cycle).
/// LLVM auto-vectorizes this pattern to ARM NEON, x86 SSE/AVX, and WASM SIMD.
/// Portable: no intrinsics, no nightly features required.
///
/// Panics in debug builds if `a.len() != b.len()`.
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = [0.0f32; 8];
    let chunks_a = a.chunks_exact(8);
    let chunks_b = b.chunks_exact(8);
    let rem_a = chunks_a.remainder();
    let rem_b = chunks_b.remainder();
    for (a8, b8) in chunks_a.zip(chunks_b) {
        sum[0] += a8[0] * b8[0];
        sum[1] += a8[1] * b8[1];
        sum[2] += a8[2] * b8[2];
        sum[3] += a8[3] * b8[3];
        sum[4] += a8[4] * b8[4];
        sum[5] += a8[5] * b8[5];
        sum[6] += a8[6] * b8[6];
        sum[7] += a8[7] * b8[7];
    }
    // Pairwise summation for better numerical stability
    let mut total = (sum[0] + sum[1]) + (sum[2] + sum[3]) + (sum[4] + sum[5]) + (sum[6] + sum[7]);
    for (x, y) in rem_a.iter().zip(rem_b) {
        total += x * y;
    }
    total
}

/// Cosine similarity between two f32 slices.
///
/// Returns `dot(a, b) / (||a|| * ||b||)`. Returns `0.0` when either vector
/// is zero-length to avoid division by zero. When both vectors are already
/// unit-length, prefer calling `dot_product` directly (saves two sqrt calls).
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product(a, b);
    let mag_a = dot_product(a, a).sqrt();
    let mag_b = dot_product(b, b).sqrt();
    if mag_a == 0.0 || mag_b == 0.0 {
        0.0
    } else {
        dot / (mag_a * mag_b)
    }
}

/// Returns `true` if `v` is approximately unit-length (L2 norm within 1e-5 of 1.0).
///
/// Use this to select the faster `dot_product` path instead of the full
/// `cosine_similarity` computation when vectors are pre-normalized.
pub fn is_unit_vector(v: &[f32]) -> bool {
    let mag_sq = dot_product(v, v);
    (1.0 - mag_sq).abs() < 1e-5
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dot_product_basic() {
        assert!((dot_product(&[1.0, 0.0], &[0.0, 1.0]) - 0.0).abs() < 1e-6);
        assert!((dot_product(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]) - 32.0).abs() < 1e-4);
    }

    #[test]
    fn dot_product_384_dim() {
        let a: Vec<f32> = (0..384).map(|i| (i as f32 * 0.01).sin()).collect();
        let b: Vec<f32> = (0..384).map(|i| (i as f32 * 0.01).cos()).collect();
        let naive: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
        let fast = dot_product(&a, &b);
        assert!((naive - fast).abs() < 1e-3, "naive={naive}, fast={fast}");
    }

    #[test]
    fn cosine_identical_vectors() {
        let v = vec![0.6f32, 0.8]; // unit vector: 0.36 + 0.64 = 1.0
        let sim = cosine_similarity(&v, &v);
        assert!((sim - 1.0).abs() < 1e-6, "expected ~1.0, got {sim}");
    }

    #[test]
    fn cosine_orthogonal_vectors() {
        let a = vec![1.0f32, 0.0];
        let b = vec![0.0f32, 1.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-6, "expected ~0.0, got {sim}");
    }

    #[test]
    fn is_unit_vector_check() {
        assert!(is_unit_vector(&[1.0f32, 0.0]));
        assert!(is_unit_vector(&[0.6f32, 0.8])); // 0.36 + 0.64 = 1.0

        assert!(!is_unit_vector(&[2.0f32, 0.0]));
        assert!(!is_unit_vector(&[0.0f32, 0.0]));

        // L2-normalized 384-dim vector is unit length
        let mut v: Vec<f32> = (0..384).map(|i| (i as f32 * 0.01).sin()).collect();
        let mag = dot_product(&v, &v).sqrt();
        for x in &mut v {
            *x /= mag;
        }
        assert!(is_unit_vector(&v));
    }

    #[test]
    fn zero_vector_cosine() {
        let zero = vec![0.0f32, 0.0];
        let other = vec![1.0f32, 0.0];
        assert_eq!(cosine_similarity(&zero, &other), 0.0);
        assert_eq!(cosine_similarity(&other, &zero), 0.0);
        assert_eq!(cosine_similarity(&zero, &zero), 0.0);
    }
}
