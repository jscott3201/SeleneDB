//! Benchmarks for selene-persist: WAL append/replay, snapshot write/read.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use selene_core::changeset::Change;
use selene_core::{IStr, NodeId, Origin, Value};
use selene_persist::config::SyncPolicy;
use selene_persist::snapshot::{
    GraphSnapshot, SnapshotEdge, SnapshotNode, SnapshotSchemas, read_snapshot, write_snapshot,
};
use selene_persist::wal::Wal;
use selene_testing::bench_profiles::bench_profile;
use smol_str::SmolStr;

fn profile_scales() -> Vec<u64> {
    bench_profile().scales().to_vec()
}

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

/// Generate a realistic batch of changes (NodeCreated + PropertySet).
fn make_changes(count: u64) -> Vec<Change> {
    let mut changes = Vec::with_capacity(count as usize * 3);
    for i in 1..=count {
        changes.push(Change::NodeCreated { node_id: NodeId(i) });
        changes.push(Change::PropertySet {
            node_id: NodeId(i),
            key: IStr::new("index"),
            value: Value::Int(i as i64),
            old_value: None,
        });
        changes.push(Change::LabelAdded {
            node_id: NodeId(i),
            label: IStr::new("sensor"),
        });
    }
    changes
}

/// Build snapshot nodes for benchmarks.
fn make_snapshot_nodes(n: u64) -> Vec<SnapshotNode> {
    (1..=n)
        .map(|i| SnapshotNode {
            id: i,
            labels: vec!["sensor".into(), "entity".into()],
            properties: vec![
                ("index".into(), Value::Int(i as i64)),
                (
                    "name".into(),
                    Value::String(SmolStr::new(format!("sensor-{i}"))),
                ),
            ],
            created_at: 1_000_000_000,
            updated_at: 1_000_000_000,
            version: 1,
        })
        .collect()
}

/// Build a nodes-only snapshot with `n` nodes.
fn make_snapshot(n: u64) -> GraphSnapshot {
    GraphSnapshot {
        nodes: make_snapshot_nodes(n),
        edges: vec![],
        next_node_id: n + 1,
        next_edge_id: 1,
        changelog_sequence: 0,
        schemas: SnapshotSchemas::default(),
        triggers: vec![],
        extra_sections: vec![],
    }
}

/// Build a realistic snapshot with `n` nodes and `n-1` edges (a linear chain).
fn make_realistic_snapshot(n: u64) -> GraphSnapshot {
    let nodes = make_snapshot_nodes(n);
    let edges: Vec<SnapshotEdge> = (0..n.saturating_sub(1))
        .map(|i| SnapshotEdge {
            id: i,
            source: i,
            target: i + 1,
            label: "contains".into(),
            properties: vec![("weight".into(), Value::Float(1.0))],
            created_at: 1_000_000_000,
        })
        .collect();
    GraphSnapshot {
        nodes,
        edges,
        next_node_id: n,
        next_edge_id: n.saturating_sub(1),
        changelog_sequence: 0,
        schemas: SnapshotSchemas::default(),
        triggers: vec![],
        extra_sections: vec![vec![0x01, 0, 0, 0, 0]],
    }
}

// ── WAL Append ──────────────────────────────────────────────────────────

fn bench_wal_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let wal_path = dir.path().join("bench.wal");
                    let wal = Wal::open(&wal_path, SyncPolicy::OnSnapshot).unwrap();
                    let changes = make_changes(1); // single-change batches
                    (dir, wal, changes)
                },
                |(_dir, mut wal, changes)| {
                    for _ in 0..n {
                        wal.append(&changes, 0, Origin::Local).unwrap();
                    }
                },
            );
        });
    }
    group.finish();
}

fn bench_wal_append_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append_batch");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let wal_path = dir.path().join("bench.wal");
                    let wal = Wal::open(&wal_path, SyncPolicy::OnSnapshot).unwrap();
                    let batch: Vec<Vec<_>> = (0..n).map(|_| make_changes(1)).collect();
                    (dir, wal, batch)
                },
                |(_dir, mut wal, batch)| {
                    wal.append_batch(&batch, 0, Origin::Local).unwrap();
                },
            );
        });
    }
    group.finish();
}

fn bench_wal_replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_replay");
    for &count in &profile_scales() {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let wal_path = dir.path().join("bench.wal");
                    let mut wal = Wal::open(&wal_path, SyncPolicy::OnSnapshot).unwrap();
                    let changes = make_changes(1);
                    for _ in 0..n {
                        wal.append(&changes, 0, Origin::Local).unwrap();
                    }
                    drop(wal);
                    (dir, wal_path)
                },
                |(_dir, wal_path)| {
                    let entries = Wal::read_entries_after(&wal_path, 0).unwrap();
                    std::hint::black_box(entries.len());
                },
            );
        });
    }
    group.finish();
}

// ── Snapshot Write/Read ─────────────────────────────────────────────────

fn bench_snapshot_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_write");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        let snapshot = make_snapshot(count);
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("bench.snap");
                    (dir, path)
                },
                |(_dir, path)| {
                    let bytes = write_snapshot(&snapshot, &path).unwrap();
                    std::hint::black_box(bytes);
                },
            );
        });
    }
    group.finish();
}

fn bench_snapshot_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_recovery");
    for &count in &profile_scales() {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("bench.snap");
                    let snapshot = make_snapshot(n);
                    write_snapshot(&snapshot, &path).unwrap();
                    (dir, path)
                },
                |(_dir, path)| {
                    let snap = read_snapshot(&path).unwrap();
                    std::hint::black_box(snap.nodes.len());
                },
            );
        });
    }
    group.finish();
}

// ── Realistic Snapshot (nodes + edges) ─────────────────────────────────

fn bench_snapshot_write_with_edges(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_write_with_edges");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        let snapshot = make_realistic_snapshot(count);
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("bench.snap");
                    (dir, path)
                },
                |(_dir, path)| {
                    let bytes = write_snapshot(&snapshot, &path).unwrap();
                    std::hint::black_box(bytes);
                },
            );
        });
    }
    group.finish();
}

// ── Full Recovery (snapshot + WAL replay) ──────────────────────────────

fn bench_full_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_recovery");
    for &count in &profile_scales() {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let snap_path = dir.path().join("bench.snap");
                    let wal_path = dir.path().join("bench.wal");

                    // Write a snapshot with n/2 nodes+edges
                    let half = n / 2;
                    let snapshot = make_realistic_snapshot(half.max(1));
                    write_snapshot(&snapshot, &snap_path).unwrap();

                    // Write WAL entries for the other half
                    let mut wal = Wal::open(&wal_path, SyncPolicy::OnSnapshot).unwrap();
                    let remaining = n.saturating_sub(half);
                    for _ in 0..remaining {
                        let changes = make_changes(1);
                        wal.append(&changes, 0, Origin::Local).unwrap();
                    }
                    drop(wal);

                    (dir, snap_path, wal_path)
                },
                |(_dir, snap_path, wal_path)| {
                    // Measure the actual recovery path: read snapshot + replay WAL
                    let snap = read_snapshot(&snap_path).unwrap();
                    std::hint::black_box(snap.nodes.len() + snap.edges.len());

                    let entries = Wal::read_entries_after(&wal_path, 0).unwrap();
                    std::hint::black_box(entries.len());
                },
            );
        });
    }
    group.finish();
}

// ── WAL Size Tracking ─────────────────────────────────────────────────

fn bench_wal_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_size");
    for &count in &profile_scales() {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &n| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let wal_path = dir.path().join("bench.wal");
                    (dir, wal_path)
                },
                |(_dir, wal_path)| {
                    let mut wal = Wal::open(&wal_path, SyncPolicy::OnSnapshot).unwrap();
                    let changes = make_changes(1);
                    for _ in 0..n {
                        wal.append(&changes, 0, Origin::Local).unwrap();
                    }
                    let file_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
                    std::hint::black_box(file_size);
                },
            );
        });
    }
    group.finish();
}

criterion_group! {
    name = wal;
    config = profile_criterion();
    targets = bench_wal_append, bench_wal_append_batch, bench_wal_replay, bench_wal_size
}
criterion_group! {
    name = snapshots;
    config = profile_criterion();
    targets = bench_snapshot_write, bench_snapshot_recovery, bench_snapshot_write_with_edges, bench_full_recovery
}
criterion_main!(wal, snapshots);
