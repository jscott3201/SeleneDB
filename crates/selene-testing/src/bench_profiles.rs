//! Benchmark profile system with env-gated criterion configuration.
//!
//! Set `SELENE_BENCH_PROFILE` to control benchmark depth:
//! - `quick`  - 2 scales, 10 samples, 1s measurement (~45s total)
//! - `full`   - 3 scales, 20 samples, 2s measurement (~3min total, default)
//! - `stress` - 5 scales, 30 samples, 3s measurement (cloud only)

#[cfg(feature = "bench")]
use criterion::Criterion;
use std::time::Duration;

/// Benchmark profile with scales and criterion configuration.
pub struct BenchProfile {
    pub scales: &'static [u64],
    pub sample_size: usize,
    pub warm_up: Duration,
    pub measurement: Duration,
}

impl BenchProfile {
    /// Target node counts for this profile.
    pub fn scales(&self) -> &[u64] {
        self.scales
    }

    /// Build a configured Criterion instance.
    #[cfg(feature = "bench")]
    pub fn into_criterion(self) -> Criterion {
        Criterion::default()
            .sample_size(self.sample_size)
            .warm_up_time(self.warm_up)
            .measurement_time(self.measurement)
    }

    /// Whether to skip expensive algorithms at this scale.
    /// Returns true if `scale >= max_scale`.
    pub fn should_skip_expensive(scale: u64, max_scale: u64) -> bool {
        scale >= max_scale
    }
}

pub const QUICK_SCALES: &[u64] = &[200, 1_000];
pub const FULL_SCALES: &[u64] = &[200, 1_000, 10_000];
pub const STRESS_SCALES: &[u64] = &[200, 1_000, 10_000, 100_000, 250_000];

/// Read profile from `SELENE_BENCH_PROFILE` env var. Defaults to `full`.
pub fn bench_profile() -> BenchProfile {
    let profile = std::env::var("SELENE_BENCH_PROFILE").unwrap_or_else(|_| "full".to_string());

    match profile.as_str() {
        "quick" => BenchProfile {
            scales: QUICK_SCALES,
            sample_size: 10,
            warm_up: Duration::from_millis(300),
            measurement: Duration::from_secs(1),
        },
        "stress" => BenchProfile {
            scales: STRESS_SCALES,
            sample_size: 30,
            warm_up: Duration::from_millis(500),
            measurement: Duration::from_secs(3),
        },
        _ => BenchProfile {
            scales: FULL_SCALES,
            sample_size: 20,
            warm_up: Duration::from_millis(500),
            measurement: Duration::from_secs(2),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_constants_are_ordered() {
        assert!(QUICK_SCALES.len() < FULL_SCALES.len());
        assert!(FULL_SCALES.len() < STRESS_SCALES.len());
        for scales in [QUICK_SCALES, FULL_SCALES, STRESS_SCALES] {
            for w in scales.windows(2) {
                assert!(w[0] < w[1]);
            }
        }
    }

    #[test]
    fn should_skip_expensive_works() {
        assert!(!BenchProfile::should_skip_expensive(1_000, 10_000));
        assert!(BenchProfile::should_skip_expensive(10_000, 10_000));
        assert!(BenchProfile::should_skip_expensive(100_000, 10_000));
    }
}
