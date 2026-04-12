//! Adaptive mutation batcher — serializes all writes through a single background task.
//!
//! All mutations flow through an MPSC channel to a single executor task.
//! The executor drains all pending mutations and runs them back-to-back,
//! eliminating RwLock contention and tokio scheduling overhead between writes.
//!
//! Under low load: single pending mutation executes immediately.
//! Under high load: mutations drain back-to-back without scheduling gaps.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use selene_graph::SharedGraph;
use tokio::sync::mpsc;
use tracing::Instrument;

/// Type-erased task: captures everything it needs, no arguments.
type TaskFn = Box<dyn FnOnce() + Send>;

/// Memory budget for OOM protection, shared between batcher handle and executor.
pub struct MemoryBudget {
    /// Hard limit in bytes (0 = disabled).
    budget_bytes: u64,
    /// Soft limit in bytes.
    soft_limit_bytes: u64,
    /// True when memory usage exceeds the soft limit.
    pub pressure: AtomicBool,
}

impl MemoryBudget {
    pub fn new(budget_bytes: u64, soft_limit_bytes: u64) -> Self {
        Self {
            budget_bytes,
            soft_limit_bytes,
            pressure: AtomicBool::new(false),
        }
    }

    /// No-op budget (disabled).
    pub fn disabled() -> Self {
        Self::new(0, 0)
    }

    pub fn is_enabled(&self) -> bool {
        self.budget_bytes > 0
    }

    pub fn in_pressure(&self) -> bool {
        self.pressure.load(Ordering::Relaxed)
    }
}

/// Handle to the mutation batcher. Clone is cheap (Arc'd channel sender).
#[derive(Clone)]
pub struct MutationBatcher {
    tx: mpsc::Sender<TaskFn>,
    pub memory_budget: Arc<MemoryBudget>,
}

impl MutationBatcher {
    /// Spawn the batcher background task. Returns the handle.
    pub fn spawn(graph: SharedGraph) -> Self {
        Self::spawn_with_budget(graph, MemoryBudget::disabled())
    }

    /// Spawn with memory budget for OOM protection.
    pub fn spawn_with_budget(graph: SharedGraph, budget: MemoryBudget) -> Self {
        let (tx, rx) = mpsc::channel::<TaskFn>(1024);
        let memory_budget = Arc::new(budget);
        tokio::spawn(
            batch_executor(graph, rx, Arc::clone(&memory_budget))
                .instrument(tracing::info_span!("mutation_batcher")),
        );
        Self { tx, memory_budget }
    }

    /// Submit a mutation that captures its own state (e.g., Arc<ServerState>).
    ///
    /// The closure is executed on the batcher's single-threaded task.
    /// It can call `state.graph.write()` internally -- the batcher ensures
    /// no other mutations are running concurrently, eliminating lock contention.
    pub async fn submit<R: Send + 'static>(
        &self,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> Result<R, selene_graph::error::GraphError> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        let task: TaskFn = Box::new(move || {
            let result = f();
            let _ = result_tx.send(result);
        });

        self.tx.send(task).await.map_err(|_| {
            selene_graph::error::GraphError::Other("mutation batcher shut down".into())
        })?;

        result_rx.await.map_err(|_| {
            selene_graph::error::GraphError::Other("mutation batcher dropped result".into())
        })
    }
}

/// Background task: drain channel, execute mutations.
///
/// OOM protection: before processing each batch, checks graph memory against
/// the configured budget. At soft limit, sets the pressure flag (background
/// tasks check this to throttle). At hard limit, drops all pending mutations
/// (callers receive GraphError via closed oneshot channels).
async fn batch_executor(
    graph: SharedGraph,
    mut rx: mpsc::Receiver<TaskFn>,
    budget: Arc<MemoryBudget>,
) {
    while let Some(first) = rx.recv().await {
        // Adaptive drain: collect all pending entries
        let mut batch = vec![first];
        while let Ok(entry) = rx.try_recv() {
            batch.push(entry);
        }

        // ── OOM check ────────────────────────────────────────────────
        if budget.is_enabled() {
            let used = graph.read(|g| g.memory_estimate_bytes()) as u64;

            // Soft limit: set pressure flag for background task throttling
            let in_pressure = used >= budget.soft_limit_bytes;
            budget.pressure.store(in_pressure, Ordering::Relaxed);
            if in_pressure && used < budget.budget_bytes {
                tracing::warn!(
                    used_mb = used / (1024 * 1024),
                    budget_mb = budget.budget_bytes / (1024 * 1024),
                    "memory soft limit reached"
                );
            }

            // Hard limit: reject all writes (drop batch, callers get RecvError)
            if used >= budget.budget_bytes {
                tracing::error!(
                    used_mb = used / (1024 * 1024),
                    budget_mb = budget.budget_bytes / (1024 * 1024),
                    dropped = batch.len(),
                    "memory budget exceeded -- rejecting writes"
                );
                // Dropping batch drops all oneshot senders, callers receive error
                drop(batch);
                continue;
            }
        }

        let batch_size = batch.len();

        for task in batch {
            task();
        }

        if batch_size > 1 {
            tracing::debug!(batch_size, "mutation batch drained");
        }
    }

    tracing::info!("mutation batcher shut down");
}

#[cfg(test)]
mod tests {
    use super::*;

    // 1 ─────────────────────────────────────────────────────────────────
    #[test]
    fn memory_budget_new_stores_values() {
        let budget = MemoryBudget::new(1_000_000, 800_000);
        assert_eq!(budget.budget_bytes, 1_000_000);
        assert_eq!(budget.soft_limit_bytes, 800_000);
        assert!(
            !budget.in_pressure(),
            "fresh budget must not be in pressure"
        );
    }

    // 2 ─────────────────────────────────────────────────────────────────
    #[test]
    fn memory_budget_disabled_has_zero_budget() {
        let budget = MemoryBudget::disabled();
        assert_eq!(budget.budget_bytes, 0);
        assert_eq!(budget.soft_limit_bytes, 0);
    }

    // 3 ─────────────────────────────────────────────────────────────────
    #[test]
    fn memory_budget_is_enabled_true_when_budget_positive() {
        let budget = MemoryBudget::new(1024, 512);
        assert!(budget.is_enabled());
    }

    // 4 ─────────────────────────────────────────────────────────────────
    #[test]
    fn memory_budget_is_enabled_false_when_disabled() {
        let budget = MemoryBudget::disabled();
        assert!(!budget.is_enabled());
    }

    // 5 ─────────────────────────────────────────────────────────────────
    #[test]
    fn memory_budget_is_enabled_false_when_budget_zero() {
        let budget = MemoryBudget::new(0, 500);
        assert!(!budget.is_enabled());
    }

    // 6 ─────────────────────────────────────────────────────────────────
    #[test]
    fn memory_budget_pressure_toggle() {
        let budget = MemoryBudget::new(1_000_000, 800_000);
        assert!(!budget.in_pressure());

        budget.pressure.store(true, Ordering::Relaxed);
        assert!(budget.in_pressure());

        budget.pressure.store(false, Ordering::Relaxed);
        assert!(!budget.in_pressure());
    }

    // 7 ─────────────────────────────────────────────────────────────────
    /// Boundary: budget_bytes == 1 is still enabled.
    #[test]
    fn memory_budget_minimum_enabled() {
        let budget = MemoryBudget::new(1, 0);
        assert!(budget.is_enabled());
    }

    // 8 ─────────────────────────────────────────────────────────────────
    /// MemoryBudget with u64::MAX does not overflow or panic.
    #[test]
    fn memory_budget_max_values() {
        let budget = MemoryBudget::new(u64::MAX, u64::MAX);
        assert!(budget.is_enabled());
        assert_eq!(budget.budget_bytes, u64::MAX);
        assert_eq!(budget.soft_limit_bytes, u64::MAX);
    }
}
