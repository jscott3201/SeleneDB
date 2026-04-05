//! Prometheus metrics for Selene server.
//!
//! Exports counters, histograms, and gauges at GET /metrics.
//! Query instrumentation is called from ops/gql.rs on every execution.
//! Graph/connection stats are updated by a periodic background task.
//!
//! Two tiers:
//! - **Basic** (Edge default): 12 metrics — queries, graph, memory, WAL, snapshots.
//! - **Full** (Cloud/Standalone): +6 metrics — replica lag, changelog, WAL bytes, auth, services.

use std::sync::OnceLock;
use std::time::Duration;

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

use crate::bootstrap::ServerState;
use crate::config::MetricsTier;

/// Global metrics registry.
static REGISTRY: OnceLock<MetricsState> = OnceLock::new();

#[allow(dead_code)]
struct MetricsState {
    registry: Registry,
    // ── Basic tier (always present) ──────────────────────────────────
    // Query metrics
    query_count: IntCounterVec,
    query_duration: Histogram,
    active_queries: IntGauge,
    // Graph metrics
    graph_nodes: IntGauge,
    graph_edges: IntGauge,
    graph_generation: IntGauge,
    // Memory metrics
    memory_used_bytes: IntGauge,
    memory_budget_bytes: IntGauge,
    memory_pressure: IntGauge,
    // Persistence metrics
    wal_entries: IntGauge,
    snapshot_duration: Histogram,
    snapshot_total: IntCounterVec,

    // ── Full tier (Cloud/Standalone, None when basic) ────────────────
    replica_lag_sequences: Option<IntGauge>,
    changelog_buffer_entries: Option<IntGauge>,
    changelog_buffer_capacity: Option<IntGauge>,
    auth_failures_total: Option<IntCounter>,
    services_active: Option<IntGaugeVec>,
    wal_bytes: Option<IntGauge>,
}

/// Initialize metrics with the given tier. Must be called once at startup.
pub(crate) fn init(tier: MetricsTier) {
    REGISTRY.get_or_init(|| create_metrics(tier));
}

fn create_metrics(tier: MetricsTier) -> MetricsState {
    let registry = Registry::new();

    // ── Basic tier ───────────────────────────────────────────────────

    let query_count = IntCounterVec::new(
        Opts::new("selene_query_total", "Total GQL queries executed"),
        &["status"],
    )
    .unwrap();

    let query_duration = Histogram::with_opts(
        HistogramOpts::new(
            "selene_query_duration_seconds",
            "GQL query duration in seconds",
        )
        .buckets(vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0,
        ]),
    )
    .unwrap();

    let active_queries =
        IntGauge::new("selene_active_queries", "Currently executing queries").unwrap();
    let graph_nodes = IntGauge::new("selene_graph_nodes", "Total nodes in the graph").unwrap();
    let graph_edges = IntGauge::new("selene_graph_edges", "Total edges in the graph").unwrap();
    let graph_generation = IntGauge::new(
        "selene_graph_generation",
        "Graph mutation generation counter",
    )
    .unwrap();

    let memory_used_bytes =
        IntGauge::new("selene_memory_used_bytes", "Estimated graph memory usage").unwrap();
    let memory_budget_bytes =
        IntGauge::new("selene_memory_budget_bytes", "Configured memory budget").unwrap();
    let memory_pressure = IntGauge::new(
        "selene_memory_pressure",
        "1 when memory usage exceeds soft limit",
    )
    .unwrap();

    let wal_entries = IntGauge::new("selene_wal_entries", "WAL entry count").unwrap();
    let snapshot_duration = Histogram::with_opts(
        HistogramOpts::new(
            "selene_snapshot_duration_seconds",
            "Snapshot write duration",
        )
        .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
    )
    .unwrap();
    let snapshot_total = IntCounterVec::new(
        Opts::new("selene_snapshot_total", "Total snapshots taken"),
        &["status"],
    )
    .unwrap();

    // Register basic tier
    for metric in [
        Box::new(query_count.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(query_duration.clone()),
        Box::new(active_queries.clone()),
        Box::new(graph_nodes.clone()),
        Box::new(graph_edges.clone()),
        Box::new(graph_generation.clone()),
        Box::new(memory_used_bytes.clone()),
        Box::new(memory_budget_bytes.clone()),
        Box::new(memory_pressure.clone()),
        Box::new(wal_entries.clone()),
        Box::new(snapshot_duration.clone()),
        Box::new(snapshot_total.clone()),
    ] {
        registry.register(metric).unwrap();
    }

    // ── Full tier ────────────────────────────────────────────────────

    let (
        replica_lag_sequences,
        changelog_buffer_entries,
        changelog_buffer_capacity,
        auth_failures_total,
        services_active,
        wal_bytes,
    ) = if tier == MetricsTier::Full {
        let replica_lag = IntGauge::new(
            "selene_replica_lag_sequences",
            "Replica lag in changelog sequences",
        )
        .unwrap();
        let cl_entries = IntGauge::new(
            "selene_changelog_buffer_entries",
            "Changelog buffer entries",
        )
        .unwrap();
        let cl_capacity = IntGauge::new(
            "selene_changelog_buffer_capacity",
            "Changelog buffer capacity",
        )
        .unwrap();
        let auth_fail = IntCounter::new(
            "selene_auth_failures_total",
            "Total authentication failures",
        )
        .unwrap();
        let svc_active = IntGaugeVec::new(
            Opts::new("selene_services_active", "Active services"),
            &["service"],
        )
        .unwrap();
        let wal_b = IntGauge::new("selene_wal_bytes", "WAL file size in bytes").unwrap();

        for metric in [
            Box::new(replica_lag.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(cl_entries.clone()),
            Box::new(cl_capacity.clone()),
            Box::new(auth_fail.clone()),
            Box::new(svc_active.clone()),
            Box::new(wal_b.clone()),
        ] {
            registry.register(metric).unwrap();
        }

        (
            Some(replica_lag),
            Some(cl_entries),
            Some(cl_capacity),
            Some(auth_fail),
            Some(svc_active),
            Some(wal_b),
        )
    } else {
        (None, None, None, None, None, None)
    };

    MetricsState {
        registry,
        query_count,
        query_duration,
        active_queries,
        graph_nodes,
        graph_edges,
        graph_generation,
        memory_used_bytes,
        memory_budget_bytes,
        memory_pressure,
        wal_entries,
        snapshot_duration,
        snapshot_total,
        replica_lag_sequences,
        changelog_buffer_entries,
        changelog_buffer_capacity,
        auth_failures_total,
        services_active,
        wal_bytes,
    }
}

fn metrics() -> &'static MetricsState {
    REGISTRY.get_or_init(|| create_metrics(MetricsTier::Basic))
}

// ── Public instrumentation API ───────────────────────────────────────────

/// Record a completed query execution.
pub(crate) fn record_query(duration: Duration, success: bool) {
    let m = metrics();
    let status = if success { "ok" } else { "error" };
    m.query_count.with_label_values(&[status]).inc();
    m.query_duration.observe(duration.as_secs_f64());
}

/// Increment active query count (call before execution).
pub(crate) fn query_start() {
    metrics().active_queries.inc();
}

/// Decrement active query count (call after execution).
pub(crate) fn query_end() {
    metrics().active_queries.dec();
}

/// Record a snapshot completion.
#[allow(dead_code)]
pub(crate) fn record_snapshot(duration: Duration, success: bool) {
    let m = metrics();
    let status = if success { "ok" } else { "error" };
    m.snapshot_total.with_label_values(&[status]).inc();
    m.snapshot_duration.observe(duration.as_secs_f64());
}

/// Record an authentication failure.
#[allow(dead_code)]
pub(crate) fn record_auth_failure() {
    if let Some(ref counter) = metrics().auth_failures_total {
        counter.inc();
    }
}

/// Update graph and system statistics from current state.
pub(crate) fn update_graph_stats(state: &ServerState) {
    let m = metrics();
    state.graph.read(|g| {
        m.graph_nodes.set(g.node_count() as i64);
        m.graph_edges.set(g.edge_count() as i64);
        m.graph_generation.set(g.generation() as i64);

        // Memory metrics
        let used = g.memory_estimate_bytes() as i64;
        m.memory_used_bytes.set(used);
        m.memory_budget_bytes
            .set(state.config.memory.budget_bytes() as i64);
        m.memory_pressure.set(i64::from(
            state.mutation_batcher.memory_budget.in_pressure(),
        ));
    });

    // WAL entry count
    let wal_entries = state.persistence.wal.lock().entry_count();
    m.wal_entries.set(wal_entries as i64);

    // ── Full-tier metrics ────────────────────────────────────────────
    if let Some(ref cl_entries) = m.changelog_buffer_entries {
        let buf = state.persistence.changelog.lock();
        cl_entries.set(buf.len() as i64);
        if let Some(ref cl_cap) = m.changelog_buffer_capacity {
            cl_cap.set(state.config.changelog_capacity as i64);
        }
    }

    // Replica lag
    if let Some(ref lag_gauge) = m.replica_lag_sequences
        && let Some(ref lag) = state.replica.lag
    {
        lag_gauge.set(lag.load(std::sync::atomic::Ordering::Relaxed) as i64);
    }

    // WAL file size
    if let Some(ref wal_b) = m.wal_bytes {
        let wal_path = state.persistence.wal.lock().path().to_path_buf();
        if let Ok(meta) = std::fs::metadata(&wal_path) {
            wal_b.set(meta.len() as i64);
        }
    }

    // Service count
    if let Some(ref svc) = m.services_active {
        for name in state.services.service_names() {
            svc.with_label_values(&[name]).set(1);
        }
    }
}

/// Render all metrics in Prometheus text format.
pub(crate) fn render() -> String {
    let m = metrics();
    let encoder = TextEncoder::new();
    let metric_families = m.registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
