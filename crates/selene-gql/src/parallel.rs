//! Rayon parallelism utilities — adaptive threshold based on hardware.

use std::sync::OnceLock;

/// Minimum binding count to trigger parallel execution.
/// Inversely proportional to available cores:
/// - RPi 5 (4 cores): 1024
/// - M5 (10 cores): 409
/// - Graviton (64 cores): 256 (floor)
///
/// Below the threshold, serial execution is used (zero Rayon overhead).
pub fn parallel_threshold() -> usize {
    static THRESHOLD: OnceLock<usize> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        (4096 / cores).max(256)
    })
}
