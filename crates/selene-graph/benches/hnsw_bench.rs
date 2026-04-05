//! Benchmarks for HNSW build and search performance.
//!
//! Benchmark targets (Apple M5, 384-dim, random unit vectors):
//!   - Build 1K:   <50ms
//!   - Build 10K:  <500ms
//!   - Search 10K top-10: <0.5ms
//!
//! Run with: `cargo bench -p selene-graph --bench hnsw_bench`

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use selene_core::NodeId;
use selene_graph::hnsw::{HnswGraph, build, params::HnswParams, search};
use selene_testing::bench_profiles::{BenchProfile, bench_profile};

fn random_unit_vectors(n: usize, dim: usize) -> Vec<(NodeId, Arc<[f32]>)> {
    (0..n)
        .map(|i| {
            let v: Vec<f32> = (0..dim).map(|_| rand::random::<f32>() - 0.5).collect();
            let mag = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            let normalized: Vec<f32> = v.iter().map(|x| x / mag).collect();
            (NodeId(i as u64 + 1), Arc::from(normalized))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Build benchmarks
// ---------------------------------------------------------------------------

fn bench_hnsw_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_build");
    let params = HnswParams::default();

    for &n in &[1_000usize, 10_000] {
        // HNSW build is O(n * M * ef_construction); the 10K case is the expensive
        // benchmark in this group, so skip it in quick/full profiles to stay within
        // the overall benchmark time budget.
        if BenchProfile::should_skip_expensive(n as u64, 10_000) {
            continue;
        }
        let vectors = random_unit_vectors(n, 384);
        group.bench_with_input(BenchmarkId::from_parameter(n), &vectors, |b, vecs| {
            b.iter(|| {
                let g: HnswGraph = build::build(vecs.clone(), &params);
                std::hint::black_box(g.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Search benchmarks
// ---------------------------------------------------------------------------

fn bench_hnsw_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_search");
    let params = HnswParams::default();

    for &n in &[1_000usize, 10_000] {
        let vectors = random_unit_vectors(n, 384);
        let graph = build::build(vectors.clone(), &params);
        // Use the first vector's slice as the query -- avoid cloning via Arc deref.
        let query_vec = Arc::clone(&vectors[0].1);

        group.bench_with_input(
            BenchmarkId::new("top10", n),
            &(graph, query_vec),
            |b, (g, q)| {
                b.iter(|| {
                    let results = search::search(g, q, 10, params.ef_search, None);
                    std::hint::black_box(results.len());
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// HNSW vs brute-force comparison
// ---------------------------------------------------------------------------

fn bench_hnsw_vs_brute_force(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_vs_brute");
    let params = HnswParams::default();

    for &n in &[1_000usize, 10_000] {
        let vectors = random_unit_vectors(n, 384);
        let graph = build::build(vectors.clone(), &params);
        let query_vec = Arc::clone(&vectors[0].1);

        // HNSW search
        group.bench_with_input(
            BenchmarkId::new("hnsw", n),
            &(graph, Arc::clone(&query_vec)),
            |b, (g, q)| {
                b.iter(|| {
                    let results = search::search(g, q, 10, params.ef_search, None);
                    std::hint::black_box(results.len());
                });
            },
        );

        // Brute-force search
        group.bench_with_input(
            BenchmarkId::new("brute_force", n),
            &(vectors.clone(), Arc::clone(&query_vec)),
            |b, (vecs, q)| {
                b.iter(|| {
                    use selene_graph::hnsw::distance::cosine_similarity;
                    let mut scores: Vec<(NodeId, f32)> = vecs
                        .iter()
                        .map(|(id, v)| (*id, cosine_similarity(v, q)))
                        .collect();
                    scores.sort_unstable_by(|a, b| {
                        b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    scores.truncate(10);
                    std::hint::black_box(scores.len());
                });
            },
        );
    }
    group.finish();
}

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

criterion_group! {
    name = hnsw_benches;
    config = profile_criterion();
    targets = bench_hnsw_build, bench_hnsw_search, bench_hnsw_vs_brute_force
}
criterion_main!(hnsw_benches);
