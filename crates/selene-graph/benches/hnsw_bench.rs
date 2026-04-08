//! Benchmarks for HNSW build, search, and incremental insert performance.
//!
//! Benchmark targets (Apple M5, 384-dim, random unit vectors):
//!   - Build 1K:   <50ms
//!   - Build 10K:  <500ms
//!   - Search 10K top-10: <0.5ms
//!   - Incremental insert into 10K: <200us per insert
//!   - Search with 100 pending mutations: <2x baseline
//!   - Snapshot 10K + 100 mutations: <50ms
//!   - Recall@10 with 20% tombstones: >95%
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

// ---------------------------------------------------------------------------
// Incremental insert benchmarks (HnswIndex hybrid model)
// ---------------------------------------------------------------------------

fn bench_hnsw_incremental_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_incremental_insert");
    let params = HnswParams::default();

    // Build a base index with 10K vectors, then measure incremental inserts.
    if BenchProfile::should_skip_expensive(10_000, 100_000) {
        group.finish();
        return;
    }

    let base_vectors = random_unit_vectors(10_000, 384);
    let index = selene_graph::HnswIndex::new(params.clone(), 384);
    index.rebuild(base_vectors);
    index.snapshot();

    for &insert_count in &[1usize, 10, 100] {
        let extra = random_unit_vectors(insert_count, 384);
        // Offset node IDs to avoid collisions with base vectors.
        let extra: Vec<(NodeId, Arc<[f32]>)> = extra
            .into_iter()
            .enumerate()
            .map(|(i, (_, v))| (NodeId(10_001 + i as u64), v))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("inserts_into_10k", insert_count),
            &extra,
            |b, vecs| {
                b.iter(|| {
                    for (nid, vec) in vecs {
                        index.insert(*nid, Arc::clone(vec));
                    }
                    // Clean up: remove inserted nodes so next iteration starts fresh.
                    for (nid, _) in vecs {
                        index.remove(*nid);
                    }
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Search with pending mutations benchmark
// ---------------------------------------------------------------------------

fn bench_hnsw_search_with_pending(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_search_pending");
    let params = HnswParams::default();

    if BenchProfile::should_skip_expensive(10_000, 100_000) {
        group.finish();
        return;
    }

    let base_vectors = random_unit_vectors(10_000, 384);
    let query_vec: Vec<f32> = (0..384).map(|_| rand::random::<f32>() - 0.5).collect();
    let mag = query_vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    let query_vec: Vec<f32> = query_vec.iter().map(|x| x / mag).collect();

    // Baseline: search with no pending mutations (fully snapshotted).
    let index_clean = selene_graph::HnswIndex::new(params.clone(), 384);
    index_clean.rebuild(base_vectors.clone());
    index_clean.snapshot();

    group.bench_function("top10_no_pending", |b| {
        b.iter(|| {
            let results = index_clean.search(&query_vec, 10, None, None);
            std::hint::black_box(results.len());
        });
    });

    // With 100 pending mutations (not yet snapshotted).
    for &pending in &[50usize, 100, 500] {
        if BenchProfile::should_skip_expensive(pending as u64, 1_000) {
            continue;
        }

        let index = selene_graph::HnswIndex::new(params.clone(), 384);
        index.rebuild(base_vectors.clone());
        index.snapshot();

        // Insert pending mutations without snapshotting.
        for i in 0..pending {
            let v: Vec<f32> = (0..384).map(|_| rand::random::<f32>() - 0.5).collect();
            let m = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            let v: Vec<f32> = v.iter().map(|x| x / m).collect();
            index.insert(NodeId(10_001 + i as u64), Arc::from(v));
        }

        group.bench_function(BenchmarkId::new("top10_pending", pending), |b| {
            b.iter(|| {
                let results = index.search(&query_vec, 10, None, None);
                std::hint::black_box(results.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Snapshot overhead benchmark
// ---------------------------------------------------------------------------

fn bench_hnsw_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_snapshot");
    let params = HnswParams::default();

    if BenchProfile::should_skip_expensive(10_000, 100_000) {
        group.finish();
        return;
    }

    let base_vectors = random_unit_vectors(10_000, 384);

    // Snapshot with 100 pending mutations.
    group.bench_function("snapshot_10k_plus_100", |b| {
        b.iter_batched(
            || {
                let index = selene_graph::HnswIndex::new(params.clone(), 384);
                index.rebuild(base_vectors.clone());
                index.snapshot();
                // Add 100 pending mutations.
                for i in 0..100u64 {
                    let v: Vec<f32> = (0..384).map(|_| rand::random::<f32>() - 0.5).collect();
                    let m = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                    let v: Vec<f32> = v.iter().map(|x| x / m).collect();
                    index.insert(NodeId(10_001 + i), Arc::from(v));
                }
                index
            },
            |index| {
                index.snapshot();
                std::hint::black_box(index.len());
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Tombstone recall degradation benchmark
// ---------------------------------------------------------------------------

fn bench_hnsw_tombstone_recall(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_tombstone_recall");
    group.sample_size(10); // Recall measurement, not throughput.
    let params = HnswParams::default();

    if BenchProfile::should_skip_expensive(10_000, 100_000) {
        group.finish();
        return;
    }

    let vectors = random_unit_vectors(10_000, 384);

    // For each tombstone ratio, build index, tombstone nodes, measure recall@10
    // by comparing against brute-force ground truth on the non-tombstoned set.
    for &tombstone_pct in &[10u32, 20] {
        let tombstone_count = (10_000 * tombstone_pct as usize) / 100;

        group.bench_function(
            BenchmarkId::new("recall_at10", format!("{tombstone_pct}pct_tombstoned")),
            |b| {
                b.iter_batched(
                    || {
                        let index = selene_graph::HnswIndex::new(params.clone(), 384);
                        index.rebuild(vectors.clone());
                        index.snapshot();

                        // Tombstone first N nodes.
                        for (nid, _) in vectors.iter().take(tombstone_count) {
                            index.remove(*nid);
                        }

                        // 10 random query vectors.
                        let queries: Vec<Vec<f32>> = (0..10)
                            .map(|_| {
                                let v: Vec<f32> =
                                    (0..384).map(|_| rand::random::<f32>() - 0.5).collect();
                                let m = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                                v.iter().map(|x| x / m).collect()
                            })
                            .collect();

                        // Ground truth: brute-force over non-tombstoned vectors.
                        let alive: Vec<&(NodeId, Arc<[f32]>)> =
                            vectors[tombstone_count..].iter().collect();

                        (
                            index,
                            queries,
                            alive
                                .iter()
                                .map(|&&(id, ref v)| (id, Arc::clone(v)))
                                .collect::<Vec<_>>(),
                        )
                    },
                    |(index, queries, alive)| {
                        let mut total_recall = 0.0f64;
                        for query in &queries {
                            // HNSW search.
                            let hnsw_results = index.search(query, 10, None, None);
                            let hnsw_ids: std::collections::HashSet<u64> =
                                hnsw_results.iter().map(|(id, _)| id.0).collect();

                            // Brute-force ground truth.
                            use selene_graph::hnsw::distance::cosine_similarity;
                            let mut brute: Vec<(NodeId, f32)> = alive
                                .iter()
                                .map(|(id, v)| (*id, cosine_similarity(v, query)))
                                .collect();
                            brute.sort_unstable_by(|a, b| {
                                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            brute.truncate(10);
                            let truth_ids: std::collections::HashSet<u64> =
                                brute.iter().map(|(id, _)| id.0).collect();

                            let overlap = hnsw_ids.intersection(&truth_ids).count();
                            total_recall += overlap as f64 / 10.0;
                        }
                        let avg_recall = total_recall / queries.len() as f64;
                        std::hint::black_box(avg_recall);
                    },
                    criterion::BatchSize::SmallInput,
                );
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
    targets =
        bench_hnsw_build,
        bench_hnsw_search,
        bench_hnsw_vs_brute_force,
        bench_hnsw_incremental_insert,
        bench_hnsw_search_with_pending,
        bench_hnsw_snapshot,
        bench_hnsw_tombstone_recall
}
criterion_main!(hnsw_benches);
