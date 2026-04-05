//! WAL batch coalescer -- groups concurrent writes into batched fsyncs.
//!
//! When `commit_delay_ms == 0` (default), flushes synchronously (identical to
//! previous behavior). When `> 0`, uses a bounded channel + background flush
//! task to batch multiple mutations into a single `append()` + fsync.
//!
//! The `persist_or_die` guarantee is preserved: every `submit()` blocks until
//! the caller's changes are durable, regardless of mode.

use std::sync::Arc;

use parking_lot::Mutex;
use selene_core::Origin;
use selene_core::changeset::Change;
use selene_persist::Wal;
use uhlc::HLC;

use selene_graph::ChangelogBuffer;

/// Generate an HLC timestamp, or fall back to wall-clock nanos as u64.
fn generate_timestamp(hlc: Option<&Arc<HLC>>) -> (u64, i64) {
    if let Some(hlc) = hlc {
        let ts = hlc.new_timestamp();
        let hlc_u64 = ts.get_time().as_u64();
        let wall_nanos = selene_core::now_nanos();
        (hlc_u64, wall_nanos)
    } else {
        let wall_nanos = selene_core::now_nanos();
        (wall_nanos as u64, wall_nanos)
    }
}

/// Attempt a WAL append with 3 retries and exponential backoff.
/// Aborts the process if all attempts fail (persist_or_die guarantee).
/// The `label` parameter is used in log messages to distinguish callers.
fn wal_append_or_abort(
    wal: &Mutex<Wal>,
    changes: &[Change],
    hlc_ts: u64,
    origin: Origin,
    label: &str,
) {
    const WAL_RETRY_COUNT: u32 = 3;
    let mut last_err = None;

    for attempt in 1..=WAL_RETRY_COUNT {
        match wal.lock().append(changes, hlc_ts, origin) {
            Ok((_wal_seq, _timestamp)) => {
                if attempt > 1 {
                    tracing::warn!(attempt, "WAL {label} succeeded after retry");
                }
                return;
            }
            Err(e) => {
                tracing::error!(attempt, "WAL {label} failed: {e}");
                last_err = Some(e);
                if attempt < WAL_RETRY_COUNT {
                    std::thread::sleep(std::time::Duration::from_millis(10 * u64::from(attempt)));
                }
            }
        }
    }

    tracing::error!(
        "WAL {label} failed after {WAL_RETRY_COUNT} attempts, aborting: {}",
        last_err.map(|e| e.to_string()).unwrap_or_default()
    );
    std::process::abort();
}

/// WAL coalescer with optional group commit.
///
/// - `commit_delay_ms == 0`: synchronous flush (immediate, same as before)
/// - `commit_delay_ms > 0`: async batching via background flush task
pub struct WalCoalescer {
    wal: Arc<Mutex<Wal>>,
    changelog: Arc<Mutex<ChangelogBuffer>>,
    changelog_notify: tokio::sync::broadcast::Sender<u64>,
    /// When Some, submit() sends changes to the background flush task.
    group_tx: Option<tokio::sync::mpsc::Sender<GroupCommitEntry>>,
    /// Hybrid Logical Clock for causal timestamp generation.
    hlc: Option<Arc<HLC>>,
}

/// A pending WAL write waiting for group commit.
struct GroupCommitEntry {
    changes: Vec<Change>,
    origin: Origin,
    done: tokio::sync::oneshot::Sender<()>,
}

impl WalCoalescer {
    /// Create a synchronous coalescer (commit_delay_ms == 0).
    pub fn new(
        wal: Arc<Mutex<Wal>>,
        changelog: Arc<Mutex<ChangelogBuffer>>,
        changelog_notify: tokio::sync::broadcast::Sender<u64>,
    ) -> Self {
        Self {
            wal,
            changelog,
            changelog_notify,
            group_tx: None,
            hlc: None,
        }
    }

    /// Set the HLC for causal timestamp generation.
    pub fn with_hlc(mut self, hlc: Arc<HLC>) -> Self {
        self.hlc = Some(hlc);
        self
    }

    /// Create a group-commit coalescer with a background flush task.
    /// The flush task drains pending writes every `commit_delay_ms`.
    pub fn with_group_commit(
        wal: Arc<Mutex<Wal>>,
        changelog: Arc<Mutex<ChangelogBuffer>>,
        changelog_notify: tokio::sync::broadcast::Sender<u64>,
        commit_delay_ms: u64,
        hlc: Option<Arc<HLC>>,
    ) -> (Self, GroupCommitHandle) {
        let (tx, rx) = tokio::sync::mpsc::channel::<GroupCommitEntry>(1024);

        let flush_wal = Arc::clone(&wal);
        let flush_changelog = Arc::clone(&changelog);
        let flush_notify = changelog_notify.clone();
        let flush_hlc = hlc.clone();
        let delay = std::time::Duration::from_millis(commit_delay_ms);

        let handle = GroupCommitHandle {
            rx,
            wal: flush_wal,
            changelog: flush_changelog,
            changelog_notify: flush_notify,
            delay,
            hlc: flush_hlc,
        };

        let coalescer = Self {
            wal,
            changelog,
            changelog_notify,
            group_tx: Some(tx),
            hlc,
        };
        (coalescer, handle)
    }

    /// Submit changes for persistence with the given origin tag.
    ///
    /// Blocks until changes are durable (either via direct flush or group commit).
    /// Preserves the `persist_or_die` guarantee.
    ///
    /// `origin` indicates whether the changes are locally produced or pulled
    /// from a remote peer. The push task (Phase 5B) uses this tag to avoid
    /// re-sending replicated changes back to the hub.
    pub fn submit(&self, changes: &[Change], origin: Origin) {
        if changes.is_empty() {
            return;
        }

        if let Some(tx) = &self.group_tx {
            // Group commit path: send to background task, wait for confirmation
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let entry = GroupCommitEntry {
                changes: changes.to_vec(),
                origin,
                done: done_tx,
            };
            // Use blocking send since submit() is called from sync context
            if tx.blocking_send(entry).is_err() {
                // Channel closed -- fall back to synchronous flush
                tracing::warn!("group commit channel closed, falling back to sync flush");
                self.flush_batch_sync(changes, origin);
                return;
            }
            // Wait for the flush task to confirm durability
            let _ = done_rx.blocking_recv();
        } else {
            // Synchronous path (commit_delay_ms == 0)
            self.flush_batch_sync(changes, origin);
        }
    }

    /// Write changes to the WAL only, without appending to the changelog.
    ///
    /// Used by the replica pull path, which manages its own changelog
    /// append with the primary's original timestamps.
    pub fn submit_wal_only(&self, changes: &[Change], origin: Origin) {
        if changes.is_empty() {
            return;
        }
        let (hlc_ts, _wall_nanos) = generate_timestamp(self.hlc.as_ref());
        wal_append_or_abort(&self.wal, changes, hlc_ts, origin, "replica pull");
    }

    /// Synchronous flush with retry -- the original behavior.
    fn flush_batch_sync(&self, changes: &[Change], origin: Origin) {
        let (hlc_ts, wall_nanos) = generate_timestamp(self.hlc.as_ref());
        wal_append_or_abort(&self.wal, changes, hlc_ts, origin, "append");
        let seq = self
            .changelog
            .lock()
            .append(changes.to_vec(), wall_nanos, hlc_ts);
        let _ = self.changelog_notify.send(seq);
    }
}

/// Background flush task handle -- must be spawned on a tokio runtime.
pub struct GroupCommitHandle {
    rx: tokio::sync::mpsc::Receiver<GroupCommitEntry>,
    wal: Arc<Mutex<Wal>>,
    changelog: Arc<Mutex<ChangelogBuffer>>,
    changelog_notify: tokio::sync::broadcast::Sender<u64>,
    delay: std::time::Duration,
    hlc: Option<Arc<HLC>>,
}

impl GroupCommitHandle {
    /// Run the background flush loop. Call via `tokio::spawn(handle.run())`.
    pub async fn run(mut self) {
        let mut pending: Vec<GroupCommitEntry> = Vec::new();

        loop {
            // Wait for first entry or channel close
            if let Some(entry) = self.rx.recv().await {
                pending.push(entry);
            } else {
                // Channel closed -- flush remaining and exit
                if !pending.is_empty() {
                    self.flush_pending(&mut pending);
                }
                return;
            }

            // Accumulate more entries for up to `delay` duration
            let deadline = tokio::time::Instant::now() + self.delay;
            loop {
                match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                    Ok(Some(entry)) => pending.push(entry),
                    Ok(None) => {
                        // Channel closed
                        self.flush_pending(&mut pending);
                        return;
                    }
                    Err(_) => break, // Timeout -- flush now
                }
            }

            self.flush_pending(&mut pending);
        }
    }

    /// Flush all pending entries, grouping by origin for separate WAL writes.
    ///
    /// Entries with the same origin are batched into a single WAL append.
    /// When all entries share the same origin (the common case), this
    /// produces exactly one WAL write, identical to the previous behavior.
    fn flush_pending(&self, pending: &mut Vec<GroupCommitEntry>) {
        if pending.is_empty() {
            return;
        }

        let (hlc_ts, wall_nanos) = generate_timestamp(self.hlc.as_ref());

        // Partition changes by origin. In the common case every entry is
        // `Origin::Local` and we get a single batch, preserving the original
        // one-write behavior.
        let mut local_changes: Vec<Change> = Vec::new();
        let mut replicated_changes: Vec<Change> = Vec::new();
        for entry in pending.iter() {
            match entry.origin {
                Origin::Local => local_changes.extend(entry.changes.iter().cloned()),
                Origin::Replicated => replicated_changes.extend(entry.changes.iter().cloned()),
            }
        }

        if !local_changes.is_empty() {
            wal_append_or_abort(
                &self.wal,
                &local_changes,
                hlc_ts,
                Origin::Local,
                "group commit",
            );
        }
        if !replicated_changes.is_empty() {
            wal_append_or_abort(
                &self.wal,
                &replicated_changes,
                hlc_ts,
                Origin::Replicated,
                "group commit",
            );
        }

        // Drain entries, moving changes into the changelog (avoids a second
        // clone) and notifying each caller.
        let mut cl = self.changelog.lock();
        let mut seq = 0u64;
        for entry in pending.drain(..) {
            seq = cl.append(entry.changes, wall_nanos, hlc_ts);
            let _ = entry.done.send(());
        }
        drop(cl);
        let _ = self.changelog_notify.send(seq);
    }
}
