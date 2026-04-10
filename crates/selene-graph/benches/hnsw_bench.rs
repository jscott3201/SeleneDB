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
//!   - Quantized search (4-bit): ~2x faster than f32 with <1% recall loss
//!
//! Run with: `cargo bench -p selene-graph --bench hnsw_bench`

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use selene_core::NodeId;
use selene_graph::hnsw::{HnswGraph, QuantBits, QuantizationConfig, build, params::HnswParams, search};
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

// ===========================================================================
// PolarQuant quantized benchmarks
// ===========================================================================

fn quantized_params(bits: QuantBits, rescore: bool) -> HnswParams {
    HnswParams::default().with_quantization(QuantizationConfig {
        bits,
        seed: 42,
        rescore,
    })
}

// ---------------------------------------------------------------------------
// Quantized build: measures overhead of post-build quantization
// ---------------------------------------------------------------------------

fn bench_hnsw_quantized_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_quantized_build");

    let n = 1_000usize;
    let vectors = random_unit_vectors(n, 384);

    // Baseline: f32 only.
    let params_f32 = HnswParams::default();
    group.bench_with_input(BenchmarkId::new("f32", n), &vectors, |b, vecs| {
        b.iter(|| {
            let g: HnswGraph = build::build(vecs.clone(), &params_f32);
            std::hint::black_box(g.len());
        });
    });

    // Quantized builds at different bit widths.
    for &bits in &[QuantBits::Eight, QuantBits::Four, QuantBits::Three] {
        let params = quantized_params(bits, false);
        let label = format!("q{}bit", bits as u8);
        group.bench_with_input(BenchmarkId::new(label, n), &vectors, |b, vecs| {
            b.iter(|| {
                let g: HnswGraph = build::build(vecs.clone(), &params);
                std::hint::black_box(g.len());
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Quantized search: f32 vs 4-bit vs 4-bit+rescore vs 8-bit
// ---------------------------------------------------------------------------

fn bench_hnsw_quantized_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_quantized_search");

    let n = 10_000usize;
    let vectors = random_unit_vectors(n, 384);
    let query_vec = Arc::clone(&vectors[0].1);

    // f32 baseline.
    let params_f32 = HnswParams::default();
    let graph_f32 = build::build(vectors.clone(), &params_f32);
    group.bench_function(BenchmarkId::new("f32", n), |b| {
        b.iter(|| {
            let results = search::search(&graph_f32, &query_vec, 10, params_f32.ef_search, None);
            std::hint::black_box(results.len());
        });
    });

    // 4-bit quantized (no rescore).
    let params_q4 = quantized_params(QuantBits::Four, false);
    let graph_q4 = build::build(vectors.clone(), &params_q4);
    group.bench_function(BenchmarkId::new("q4bit", n), |b| {
        b.iter(|| {
            let qs = graph_q4.quantized().unwrap();
            let results = search::search_quantized(
                &graph_q4,
                qs,
                &query_vec,
                10,
                params_q4.ef_search,
                false,
                None,
            );
            std::hint::black_box(results.len());
        });
    });

    // 4-bit quantized + rescore.
    let params_q4r = quantized_params(QuantBits::Four, true);
    let graph_q4r = build::build(vectors.clone(), &params_q4r);
    group.bench_function(BenchmarkId::new("q4bit_rescore", n), |b| {
        b.iter(|| {
            let qs = graph_q4r.quantized().unwrap();
            let results = search::search_quantized(
                &graph_q4r,
                qs,
                &query_vec,
                10,
                params_q4r.ef_search,
                true,
                None,
            );
            std::hint::black_box(results.len());
        });
    });

    // 8-bit quantized.
    let params_q8 = quantized_params(QuantBits::Eight, false);
    let graph_q8 = build::build(vectors.clone(), &params_q8);
    group.bench_function(BenchmarkId::new("q8bit", n), |b| {
        b.iter(|| {
            let qs = graph_q8.quantized().unwrap();
            let results = search::search_quantized(
                &graph_q8,
                qs,
                &query_vec,
                10,
                params_q8.ef_search,
                false,
                None,
            );
            std::hint::black_box(results.len());
        });
    });

    // 3-bit quantized.
    let params_q3 = quantized_params(QuantBits::Three, false);
    let graph_q3 = build::build(vectors.clone(), &params_q3);
    group.bench_function(BenchmarkId::new("q3bit", n), |b| {
        b.iter(|| {
            let qs = graph_q3.quantized().unwrap();
            let results = search::search_quantized(
                &graph_q3,
                qs,
                &query_vec,
                10,
                params_q3.ef_search,
                false,
                None,
            );
            std::hint::black_box(results.len());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Quantized recall: measures recall@10 for each bit width vs f32 ground truth
// ---------------------------------------------------------------------------

fn bench_hnsw_quantized_recall(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_quantized_recall");
    group.sample_size(10);

    let n = 5_000usize;
    let vectors = random_unit_vectors(n, 384);

    // Build brute-force ground truth for 20 queries.
    let queries: Vec<Vec<f32>> = (0..20)
        .map(|_| {
            let v: Vec<f32> = (0..384).map(|_| rand::random::<f32>() - 0.5).collect();
            let m = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            v.iter().map(|x| x / m).collect()
        })
        .collect();

    let ground_truth: Vec<Vec<NodeId>> = queries
        .iter()
        .map(|q| {
            use selene_graph::hnsw::distance::cosine_similarity;
            let mut scores: Vec<(NodeId, f32)> = vectors
                .iter()
                .map(|(id, v)| (*id, cosine_similarity(v, q)))
                .collect();
            scores.sort_unstable_by(|a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            scores.truncate(10);
            scores.into_iter().map(|(id, _)| id).collect()
        })
        .collect();

    // f32 HNSW recall.
    let params_f32 = HnswParams::default();
    let graph_f32 = build::build(vectors.clone(), &params_f32);
    group.bench_function("f32_recall", |b| {
        b.iter(|| {
            let mut total = 0.0f64;
            for (q, truth) in queries.iter().zip(&ground_truth) {
                let results = search::search(&graph_f32, q, 10, params_f32.ef_search, None);
                let ids: std::collections::HashSet<u64> = results.iter().map(|(id, _)| id.0).collect();
                let truth_ids: std::collections::HashSet<u64> = truth.iter().map(|id| id.0).collect();
                total += ids.intersection(&truth_ids).count() as f64 / 10.0;
            }
            std::hint::black_box(total / queries.len() as f64)
        });
    });

    // Quantized recall at each bit width.
    for &(bits, label) in &[
        (QuantBits::Eight, "q8bit_recall"),
        (QuantBits::Four, "q4bit_recall"),
        (QuantBits::Three, "q3bit_recall"),
    ] {
        let params = quantized_params(bits, false);
        let graph = build::build(vectors.clone(), &params);
        group.bench_function(label, |b| {
            b.iter(|| {
                let qs = graph.quantized().unwrap();
                let mut total = 0.0f64;
                for (q, truth) in queries.iter().zip(&ground_truth) {
                    let results = search::search_quantized(
                        &graph,
                        qs,
                        q,
                        10,
                        params.ef_search,
                        false,
                        None,
                    );
                    let ids: std::collections::HashSet<u64> = results.iter().map(|(id, _)| id.0).collect();
                    let truth_ids: std::collections::HashSet<u64> = truth.iter().map(|id| id.0).collect();
                    total += ids.intersection(&truth_ids).count() as f64 / 10.0;
                }
                std::hint::black_box(total / queries.len() as f64)
            });
        });
    }

    // 4-bit + rescore recall.
    let params_q4r = quantized_params(QuantBits::Four, true);
    let graph_q4r = build::build(vectors.clone(), &params_q4r);
    group.bench_function("q4bit_rescore_recall", |b| {
        b.iter(|| {
            let qs = graph_q4r.quantized().unwrap();
            let mut total = 0.0f64;
            for (q, truth) in queries.iter().zip(&ground_truth) {
                let results = search::search_quantized(
                    &graph_q4r,
                    qs,
                    q,
                    10,
                    params_q4r.ef_search,
                    true,
                    None,
                );
                let ids: std::collections::HashSet<u64> = results.iter().map(|(id, _)| id.0).collect();
                let truth_ids: std::collections::HashSet<u64> = truth.iter().map(|id| id.0).collect();
                total += ids.intersection(&truth_ids).count() as f64 / 10.0;
            }
            std::hint::black_box(total / queries.len() as f64)
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Memory footprint comparison
// ---------------------------------------------------------------------------

fn bench_hnsw_quantized_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_quantized_memory");
    group.sample_size(10);

    // Build at various sizes, report serialized bytes (proxy for memory).
    for &n in &[1_000usize, 10_000] {
        if BenchProfile::should_skip_expensive(n as u64, 10_000) {
            continue;
        }

        let vectors = random_unit_vectors(n, 384);

        // f32 snapshot size.
        let params_f32 = HnswParams::default();
        let graph_f32 = build::build(vectors.clone(), &params_f32);
        group.bench_function(BenchmarkId::new("f32_bytes", n), |b| {
            b.iter(|| {
                let bytes = graph_f32.to_bytes().unwrap();
                std::hint::black_box(bytes.len())
            });
        });

        // 4-bit snapshot size.
        let params_q4 = quantized_params(QuantBits::Four, false);
        let graph_q4 = build::build(vectors.clone(), &params_q4);
        group.bench_function(BenchmarkId::new("q4bit_bytes", n), |b| {
            b.iter(|| {
                let bytes = graph_q4.to_bytes().unwrap();
                std::hint::black_box(bytes.len())
            });
        });
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
        bench_hnsw_tombstone_recall,
        bench_hnsw_quantized_build,
        bench_hnsw_quantized_search,
        bench_hnsw_quantized_recall,
        bench_hnsw_quantized_memory
}
criterion_main!(hnsw_benches);
