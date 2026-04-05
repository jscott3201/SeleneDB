//! Benchmarks for selene-ts: hot tier append, range query, flush to Parquet,
//! Gorilla compression, and warm tier aggregation.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use selene_core::ValueEncoding;
use selene_core::{IStr, NodeId};
use selene_testing::bench_profiles::bench_profile;
use selene_ts::config::{TsConfig, WarmTierConfig};
use selene_ts::encoding::TsBlock;
use selene_ts::hot::{HotTier, TimeSample};
use selene_ts::warm::WarmTier;

fn profile_scales() -> Vec<u64> {
    bench_profile().scales().to_vec()
}

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

/// Build a hot tier with `n` samples spread across 10 entity/property pairs.
fn build_hot_tier(n: u64) -> Arc<HotTier> {
    let hot = Arc::new(HotTier::new(TsConfig::default()));
    let base_ts: i64 = 1_000_000_000_000;
    let props = ["temp", "humidity", "pressure", "co2", "occupancy"];

    for i in 0..n {
        let node_id = NodeId((i % 10) + 1);
        let property = props[(i % 5) as usize];
        hot.append(
            node_id,
            property,
            TimeSample {
                timestamp_nanos: base_ts + i as i64 * 1_000_000,
                value: 20.0 + (i as f64 * 0.1),
            },
        );
    }
    hot
}

// ── Hot Tier Append ─────────────────────────────────────────────────────

fn bench_hot_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_append");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || HotTier::new(TsConfig::default()),
                |hot| {
                    let base_ts: i64 = 1_000_000_000_000;
                    for i in 0..n {
                        hot.append(
                            NodeId((i % 10) + 1),
                            "temp",
                            TimeSample {
                                timestamp_nanos: base_ts + i as i64 * 1_000_000,
                                value: 72.0 + i as f64 * 0.01,
                            },
                        );
                    }
                },
            );
        });
    }
    group.finish();
}

fn bench_hot_append_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_append_batch");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            let base_ts: i64 = 1_000_000_000_000;
            let samples: Vec<(NodeId, &str, TimeSample)> = (0..n)
                .map(|i| {
                    (
                        NodeId((i % 10) + 1),
                        "temp",
                        TimeSample {
                            timestamp_nanos: base_ts + i as i64 * 1_000_000,
                            value: 72.0 + i as f64 * 0.01,
                        },
                    )
                })
                .collect();
            b.iter(|| {
                let hot = HotTier::new(TsConfig::default());
                hot.append_batch(&samples);
            });
        });
    }
    group.finish();
}

// ── Hot Tier Range Query ────────────────────────────────────────────────

fn bench_hot_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_range_query");
    for &count in &profile_scales() {
        let hot = build_hot_tier(count);
        let base_ts: i64 = 1_000_000_000_000;
        // Query middle 50% of time range
        let start = base_ts + (count as i64 / 4) * 1_000_000;
        let end = base_ts + (count as i64 * 3 / 4) * 1_000_000;

        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _n| {
            b.iter(|| {
                let result = hot.range(NodeId(1), "temp", start, end);
                std::hint::black_box(result.len());
            });
        });
    }
    group.finish();
}

// ── Hot Tier Eviction ──────────────────────────────────────────────────

fn bench_hot_evict_idle(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_evict_idle");
    for &count in &profile_scales() {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    // Build tier with many keys, half "old" and half "recent"
                    let hot = HotTier::new(TsConfig::default());
                    for i in 0..n {
                        let ts = if i % 2 == 0 { 100 } else { 1_000_000_000_000 };
                        hot.append(
                            NodeId(i + 1),
                            "temp",
                            TimeSample {
                                timestamp_nanos: ts,
                                value: 72.0,
                            },
                        );
                    }
                    hot
                },
                |hot| {
                    let evicted = hot.evict_idle(500);
                    std::hint::black_box(evicted);
                },
            );
        });
    }
    group.finish();
}

// ── Flush to Parquet ────────────────────────────────────────────────────

fn bench_flush_to_parquet(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_to_parquet");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let hot = Arc::new(HotTier::new(TsConfig {
                        hot_retention_hours: 0, // everything is "expired"
                        ..TsConfig::default()
                    }));
                    // Append samples with old timestamps so they'll drain
                    for i in 0..n {
                        hot.append(
                            NodeId((i % 10) + 1),
                            "temp",
                            TimeSample {
                                timestamp_nanos: 1_000_000 + i as i64 * 1_000,
                                value: 72.0 + i as f64 * 0.01,
                            },
                        );
                    }
                    let flush =
                        selene_ts::flush::FlushTask::new(Arc::clone(&hot), dir.path().join("ts"));
                    (dir, flush)
                },
                |(_dir, flush)| {
                    let count = flush.flush_once().unwrap();
                    std::hint::black_box(count);
                },
            );
        });
    }
    group.finish();
}

// ── Hot Tier Drain Before ─────────────────────────────────────────────

fn bench_hot_drain_before(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_drain_before");
    for &scale in &profile_scales() {
        let n = scale.min(10_000) as i64;
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter_with_setup(
                || {
                    let hot = Arc::new(HotTier::new(TsConfig::default()));
                    let base = 1_000_000_000_000i64;
                    for i in 0..n {
                        hot.append(
                            NodeId(i as u64 % 10),
                            "temp",
                            TimeSample {
                                timestamp_nanos: base + i * 1_000_000_000,
                                value: 72.0 + i as f64 * 0.01,
                            },
                        );
                    }
                    let cutoff = base + (n / 2) * 1_000_000_000;
                    (hot, cutoff)
                },
                |(hot, cutoff)| {
                    std::hint::black_box(hot.drain_before(cutoff));
                },
            );
        });
    }
    group.finish();
}

// ── Hot Tier Stale-Heap Eviction ──────────────────────────────────────

fn bench_hot_evict_stale_heap(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_evict_stale_heap");
    group.bench_function("evict_with_stale_entries", |b| {
        b.iter_with_setup(
            || {
                let hot = Arc::new(HotTier::new(TsConfig::default()));
                let base = 1_000_000_000_000i64;
                // Write 100 rounds to 10 keys to create 900 stale heap entries
                for round in 0..100i64 {
                    for key in 0..10u64 {
                        hot.append(
                            NodeId(key),
                            "temp",
                            TimeSample {
                                timestamp_nanos: base + (round * 10 + key as i64) * 1_000_000_000,
                                value: 72.0,
                            },
                        );
                    }
                }
                hot
            },
            |hot| {
                // evict_idle takes absolute cutoff timestamp; i64::MAX evicts everything
                std::hint::black_box(hot.evict_idle(i64::MAX));
            },
        );
    });
    group.finish();
}

criterion_group! {
    name = hot;
    config = profile_criterion();
    targets = bench_hot_append, bench_hot_append_batch, bench_hot_range_query, bench_hot_evict_idle,
              bench_hot_drain_before, bench_hot_evict_stale_heap
}
// ── Parquet Read with Predicate ────────────────────────────────────────

fn bench_parquet_read_filtered(c: &mut Criterion) {
    use selene_ts::hot::TsKey;
    use selene_ts::parquet_writer::{read_samples_from_parquet, write_samples_to_parquet};

    let mut group = c.benchmark_group("parquet_read_filtered");
    for &count in &profile_scales() {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("bench.parquet");
                    // Write samples across 10 entities
                    let data: Vec<(TsKey, Vec<TimeSample>)> = (1..=10)
                        .map(|eid| {
                            let samples: Vec<TimeSample> = (0..n / 10)
                                .map(|i| TimeSample {
                                    timestamp_nanos: 1_000_000 + i as i64 * 1_000,
                                    value: 72.0 + i as f64 * 0.01,
                                })
                                .collect();
                            (
                                TsKey {
                                    node_id: NodeId(eid),
                                    property: IStr::new("temp"),
                                },
                                samples,
                            )
                        })
                        .collect();
                    write_samples_to_parquet(&path, &data, None).unwrap();
                    (dir, path)
                },
                |(_dir, path)| {
                    // Filter: single entity, middle 50% time range
                    let results = read_samples_from_parquet(
                        &path,
                        Some(NodeId(5)),
                        None,
                        Some(1_000_000 + (n as i64 / 40) * 1_000),
                        Some(1_000_000 + (n as i64 * 3 / 40) * 1_000),
                    )
                    .unwrap();
                    std::hint::black_box(results.len());
                },
            );
        });
    }
    group.finish();
}

// ── Parquet Write Size Tracking ───────────────────────────────────────

fn bench_parquet_write_size(c: &mut Criterion) {
    use selene_ts::hot::TsKey;
    use selene_ts::parquet_writer::write_samples_to_parquet;

    let mut group = c.benchmark_group("parquet_write_size");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("bench.parquet");
                    let data: Vec<(TsKey, Vec<TimeSample>)> = (1..=10)
                        .map(|eid| {
                            let samples: Vec<TimeSample> = (0..n / 10)
                                .map(|i| TimeSample {
                                    timestamp_nanos: 1_000_000 + i as i64 * 1_000,
                                    value: 72.0 + i as f64 * 0.01,
                                })
                                .collect();
                            (
                                TsKey {
                                    node_id: NodeId(eid),
                                    property: IStr::new("temp"),
                                },
                                samples,
                            )
                        })
                        .collect();
                    (dir, path, data)
                },
                |(_dir, path, data)| {
                    let rows = write_samples_to_parquet(&path, &data, None).unwrap();
                    let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                    // Print size info (visible in criterion output)
                    std::hint::black_box((rows, file_size));
                },
            );
        });
    }
    group.finish();
}

criterion_group! {
    name = flush;
    config = profile_criterion();
    targets = bench_flush_to_parquet, bench_parquet_read_filtered, bench_parquet_write_size
}

// ── Gorilla Compression ──────────────────────────────────────────────

const GORILLA_SAMPLES: usize = 1_800; // 30-min block at 1 Hz

fn gorilla_base_ts() -> i64 {
    1_700_000_000_000_000_000 // realistic epoch nanos
}

fn gorilla_regular_1hz() -> Vec<TimeSample> {
    let base = gorilla_base_ts();
    (0..GORILLA_SAMPLES)
        .map(|i| TimeSample {
            timestamp_nanos: base + i as i64 * 1_000_000_000,
            value: 72.0 + (i as f64 * 0.01).sin() * 2.0,
        })
        .collect()
}

fn gorilla_slowly_drifting() -> Vec<TimeSample> {
    let base = gorilla_base_ts();
    (0..GORILLA_SAMPLES)
        .map(|i| TimeSample {
            timestamp_nanos: base + i as i64 * 1_000_000_000,
            value: 72.0 + i as f64 * 0.001,
        })
        .collect()
}

fn gorilla_binary_sensor() -> Vec<TimeSample> {
    let base = gorilla_base_ts();
    (0..GORILLA_SAMPLES)
        .map(|i| TimeSample {
            timestamp_nanos: base + i as i64 * 1_000_000_000,
            value: 1.0,
        })
        .collect()
}

fn gorilla_random() -> Vec<TimeSample> {
    let base = gorilla_base_ts();
    (0..GORILLA_SAMPLES)
        .map(|i| {
            let mut h = DefaultHasher::new();
            i.hash(&mut h);
            let bits = h.finish();
            TimeSample {
                timestamp_nanos: base + i as i64 * 1_000_000_000,
                // Deterministic pseudo-random, avoiding NaN/Inf
                value: f64::from_bits(bits & 0x7FEF_FFFF_FFFF_FFFF),
            }
        })
        .collect()
}

fn bench_gorilla_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla_encode");
    group.throughput(Throughput::Elements(GORILLA_SAMPLES as u64));

    let regular = gorilla_regular_1hz();
    group.bench_function("regular_1hz", |b| {
        b.iter(|| std::hint::black_box(TsBlock::encode(&regular, ValueEncoding::Gorilla)));
    });

    let drifting = gorilla_slowly_drifting();
    group.bench_function("slowly_drifting", |b| {
        b.iter(|| std::hint::black_box(TsBlock::encode(&drifting, ValueEncoding::Gorilla)));
    });

    let binary = gorilla_binary_sensor();
    group.bench_function("binary_sensor", |b| {
        b.iter(|| std::hint::black_box(TsBlock::encode(&binary, ValueEncoding::Gorilla)));
    });

    group.finish();
}

fn bench_gorilla_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla_decode");
    group.throughput(Throughput::Elements(GORILLA_SAMPLES as u64));

    let regular_block = TsBlock::encode(&gorilla_regular_1hz(), ValueEncoding::Gorilla);
    group.bench_function("regular_1hz", |b| {
        b.iter(|| std::hint::black_box(regular_block.decode_all()));
    });

    let drifting_block = TsBlock::encode(&gorilla_slowly_drifting(), ValueEncoding::Gorilla);
    group.bench_function("slowly_drifting", |b| {
        b.iter(|| std::hint::black_box(drifting_block.decode_all()));
    });

    let binary_block = TsBlock::encode(&gorilla_binary_sensor(), ValueEncoding::Gorilla);
    group.bench_function("binary_sensor", |b| {
        b.iter(|| std::hint::black_box(binary_block.decode_all()));
    });

    group.finish();
}

fn bench_gorilla_decode_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla_decode_range");
    group.throughput(Throughput::Elements(300)); // 5 minutes of samples

    let base = gorilla_base_ts();
    let block = TsBlock::encode(&gorilla_regular_1hz(), ValueEncoding::Gorilla);

    // Middle 5 minutes: samples 750..1050
    let start = base + 750 * 1_000_000_000;
    let end = base + 1049 * 1_000_000_000;

    group.bench_function("middle_5min", |b| {
        b.iter(|| std::hint::black_box(block.decode_range_partial(start, end)));
    });

    group.finish();
}

#[allow(clippy::type_complexity)]
fn bench_gorilla_compression_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("gorilla_compression_ratio");
    let raw_bytes = GORILLA_SAMPLES * 16; // 16 bytes per TimeSample (i64 + f64)

    // Each "benchmark" encodes once and reports the ratio. We use
    // criterion primarily to print the result; throughput is bytes/sample.
    let patterns: &[(&str, fn() -> Vec<TimeSample>)] = &[
        ("regular_1hz", gorilla_regular_1hz),
        ("slowly_drifting", gorilla_slowly_drifting),
        ("binary_sensor", gorilla_binary_sensor),
        ("random", gorilla_random),
    ];

    for &(name, gen_fn) in patterns {
        let samples = gen_fn();
        let block = TsBlock::encode(&samples, ValueEncoding::Gorilla);
        let compressed = block.compressed_size();
        let ratio = raw_bytes as f64 / compressed as f64;
        let bytes_per_sample = compressed as f64 / GORILLA_SAMPLES as f64;

        // Print ratio outside of the timed loop (visible in criterion output)
        eprintln!(
            "  {name}: {compressed} bytes, {bytes_per_sample:.2} bytes/sample, {ratio:.1}x compression"
        );

        group.throughput(Throughput::Elements(GORILLA_SAMPLES as u64));
        group.bench_function(name, |b| {
            b.iter(|| {
                let blk = TsBlock::encode(&samples, ValueEncoding::Gorilla);
                std::hint::black_box(blk.compressed_size())
            });
        });
    }

    group.finish();
}

// ── Warm Tier ────────────────────────────────────────────────────────

fn build_warm_tier() -> WarmTier {
    WarmTier::new(WarmTierConfig {
        downsample_interval_secs: 60,
        retention_hours: 24,
        ddsketch_enabled: true,
        hourly: None,
    })
}

fn bench_warm_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("warm_record");
    let base_ts: i64 = 1_000_000_000_000;

    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter(|| {
                let warm = build_warm_tier();
                for i in 0..n {
                    warm.record(
                        NodeId(i % 10 + 1),
                        IStr::new("temp"),
                        TimeSample {
                            // Spread across multiple 1-minute windows to trigger finalization
                            timestamp_nanos: base_ts + i as i64 * 1_000_000_000,
                            value: 72.0 + (i as f64 * 0.01).sin() * 2.0,
                        },
                    );
                }
            });
        });
    }
    group.finish();
}

fn bench_warm_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("warm_range_query");
    let base_ts: i64 = 1_000_000_000_000;

    for &count in &profile_scales() {
        // Pre-build warm tier with N samples
        let warm = build_warm_tier();
        for i in 0..count {
            warm.record(
                NodeId(i % 10 + 1),
                IStr::new("temp"),
                TimeSample {
                    timestamp_nanos: base_ts + i as i64 * 1_000_000_000,
                    value: 72.0 + (i as f64 * 0.01).sin() * 2.0,
                },
            );
        }

        // Query middle 50% of the time range for node 1
        let start = base_ts + (count as i64 / 4) * 1_000_000_000;
        let end = base_ts + (count as i64 * 3 / 4) * 1_000_000_000;

        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _n| {
            b.iter(|| {
                let result = warm.range(NodeId(1), "temp", start, end);
                std::hint::black_box(result.len());
            });
        });
    }
    group.finish();
}

fn bench_warm_quantile_finalize(c: &mut Criterion) {
    let mut group = c.benchmark_group("warm_quantile_finalize");
    group.throughput(Throughput::Elements(1000));

    // Feed 1000 samples into a single 1-minute window, then measure finalize
    // cost by querying the current (not-yet-finalized) window via all_aggregates().
    group.bench_function("1000_samples", |b| {
        b.iter_with_setup(
            || {
                let warm = build_warm_tier();
                let base_ts: i64 = 1_000_000_000_000;
                // All samples within the same 1-minute window
                for i in 0..1000u64 {
                    warm.record(
                        NodeId(1),
                        IStr::new("temp"),
                        TimeSample {
                            timestamp_nanos: base_ts + i as i64 * 1_000_000, // 1ms apart, same 60s window
                            value: 50.0 + (i as f64 * 0.1).sin() * 30.0,
                        },
                    );
                }
                warm
            },
            |warm| {
                // all_aggregates calls finalize() on the current window
                let aggs = warm.all_aggregates(NodeId(1), "temp");
                std::hint::black_box(aggs);
            },
        );
    });

    group.finish();
}

criterion_group! {
    name = gorilla;
    config = profile_criterion();
    targets = bench_gorilla_encode, bench_gorilla_decode, bench_gorilla_decode_range, bench_gorilla_compression_ratio
}

// ── RLE + Dictionary Encoding ──────────────────────────────────────

fn rle_binary_sensor() -> Vec<TimeSample> {
    let base = gorilla_base_ts();
    (0..GORILLA_SAMPLES)
        .map(|i| TimeSample {
            timestamp_nanos: base + i as i64 * 1_000_000_000,
            value: if (i as i64) < GORILLA_SAMPLES as i64 / 2 {
                1.0
            } else {
                0.0
            },
        })
        .collect()
}

fn dictionary_discrete_sensor() -> Vec<TimeSample> {
    let base = gorilla_base_ts();
    let modes = [0.0, 1.0, 2.0, 3.0];
    (0..GORILLA_SAMPLES)
        .map(|i| TimeSample {
            timestamp_nanos: base + i as i64 * 1_000_000_000,
            value: modes[i % 4],
        })
        .collect()
}

fn bench_rle_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("rle_encode");
    group.throughput(Throughput::Elements(GORILLA_SAMPLES as u64));

    let binary = rle_binary_sensor();
    group.bench_function("binary_sensor", |b| {
        b.iter(|| std::hint::black_box(TsBlock::encode(&binary, ValueEncoding::Rle)));
    });

    let constant = gorilla_binary_sensor(); // all 1.0
    group.bench_function("constant", |b| {
        b.iter(|| std::hint::black_box(TsBlock::encode(&constant, ValueEncoding::Rle)));
    });

    group.finish();
}

fn bench_rle_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("rle_decode");
    group.throughput(Throughput::Elements(GORILLA_SAMPLES as u64));

    let binary_block = TsBlock::encode(&rle_binary_sensor(), ValueEncoding::Rle);
    group.bench_function("binary_sensor", |b| {
        b.iter(|| std::hint::black_box(binary_block.decode_all()));
    });

    let constant_block = TsBlock::encode(&gorilla_binary_sensor(), ValueEncoding::Rle);
    group.bench_function("constant", |b| {
        b.iter(|| std::hint::black_box(constant_block.decode_all()));
    });

    group.finish();
}

fn bench_dictionary_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("dictionary_encode");
    group.throughput(Throughput::Elements(GORILLA_SAMPLES as u64));

    let discrete = dictionary_discrete_sensor();
    group.bench_function("4_state", |b| {
        b.iter(|| std::hint::black_box(TsBlock::encode(&discrete, ValueEncoding::Dictionary)));
    });

    group.finish();
}

fn bench_dictionary_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("dictionary_decode");
    group.throughput(Throughput::Elements(GORILLA_SAMPLES as u64));

    let block = TsBlock::encode(&dictionary_discrete_sensor(), ValueEncoding::Dictionary);
    group.bench_function("4_state", |b| {
        b.iter(|| std::hint::black_box(block.decode_all()));
    });

    group.finish();
}

criterion_group! {
    name = encoding;
    config = profile_criterion();
    targets = bench_rle_encode, bench_rle_decode, bench_dictionary_encode, bench_dictionary_decode
}

criterion_group! {
    name = warm;
    config = profile_criterion();
    targets = bench_warm_record, bench_warm_range_query, bench_warm_quantile_finalize
}
criterion_main!(hot, flush, gorilla, encoding, warm);
