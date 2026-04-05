//! Thread-safe wrapper around [`SeleneGraph`].
//!
//! [`SharedGraph`] uses a hybrid concurrency model:
//! - **Reads** are lock-free via `ArcSwap` -- readers load an atomic `Arc` snapshot
//!   (~1ns) and work with a consistent point-in-time view. No blocking, no contention.
//! - **Writes** use a `RwLock` for exclusive access, preserving `TrackedMutation`'s
//!   in-place mutation and rollback semantics. After commit, a clone of the graph
//!   is published to the `ArcSwap` for future readers.
//!
//! This is Fjall's "SuperVersion" pattern: readers never block writers and vice versa.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use parking_lot::RwLock;
use selene_core::changeset::Change;

use crate::error::GraphError;
use crate::graph::SeleneGraph;
use crate::mutation::TrackedMutation;

/// Check whether any changes involve containment-relevant edge labels
/// (`contains` or `scoped_to`). Used to decide whether the containment
/// generation counter should be incremented for lazy auth scope refresh.
fn changes_affect_containment(changes: &[Change]) -> bool {
    changes.iter().any(|c| match c {
        Change::EdgeCreated { label, .. } => {
            label.as_str() == "contains" || label.as_str() == "scoped_to"
        }
        Change::EdgeDeleted { label, .. } => {
            label.as_str() == "contains" || label.as_str() == "scoped_to"
        }
        _ => false,
    })
}

/// Thread-safe concurrent wrapper around [`SeleneGraph`].
///
/// - **Reads** are lock-free via `ArcSwap` snapshot (~1ns load).
/// - **Writes** use `RwLock` for `TrackedMutation` (in-place + rollback).
/// - After each successful write, a snapshot is published for readers.
/// - `containment_generation` increments on containment edge changes
///   for lazy auth scope refresh.
#[derive(Clone)]
pub struct SharedGraph {
    /// The mutable graph -- writers hold the RwLock's write guard exclusively.
    inner: Arc<RwLock<SeleneGraph>>,
    /// Published read-only snapshot -- updated after each successful write.
    /// Readers load this atomically without any lock.
    snapshot: Arc<ArcSwap<SeleneGraph>>,
    /// Incremented when containment-relevant edges change (contains, scoped_to).
    containment_generation: Arc<AtomicU64>,
}

impl SharedGraph {
    /// Wrap a `SeleneGraph` for concurrent access.
    pub fn new(graph: SeleneGraph) -> Self {
        let snapshot = Arc::new(graph.clone());
        Self {
            inner: Arc::new(RwLock::new(graph)),
            snapshot: Arc::new(ArcSwap::from(snapshot)),
            containment_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Current containment generation counter.
    /// Changes when `contains` or `scoped_to` edges are created/deleted.
    pub fn containment_generation(&self) -> u64 {
        self.containment_generation.load(Ordering::Relaxed)
    }

    /// Lock-free read: load the ArcSwap snapshot and execute `f`.
    ///
    /// Multiple readers run concurrently without blocking. Each reader
    /// sees a consistent snapshot -- writes that happen concurrently are
    /// invisible until the next `read()` call after the write commits.
    pub fn read<R>(&self, f: impl FnOnce(&SeleneGraph) -> R) -> R {
        let guard = self.snapshot.load();
        f(&guard)
    }

    /// Acquire the write lock, create a [`TrackedMutation`], and pass
    /// it to `f`.
    ///
    /// - If `f` returns `Ok(result)`, the mutation is committed, a new
    ///   snapshot is published for readers, and `(result, changes)` is returned.
    /// - If `f` returns `Err`, the mutation is dropped (triggering
    ///   automatic rollback) and the error is propagated.
    ///
    /// Changelog entries receive `hlc_timestamp = 0`. Use
    /// [`write_with_hlc`](Self::write_with_hlc) to stamp entries with a
    /// real HLC value for LWW merge.
    pub fn write<R>(
        &self,
        f: impl FnOnce(&mut TrackedMutation<'_>) -> Result<R, GraphError>,
    ) -> Result<(R, Vec<Change>), GraphError> {
        self.write_with_hlc(0, f)
    }

    /// Like [`write`](Self::write), but stamps changelog entries with the
    /// given HLC timestamp for Last-Writer-Wins conflict resolution during
    /// bidirectional sync.
    pub fn write_with_hlc<R>(
        &self,
        hlc_timestamp: u64,
        f: impl FnOnce(&mut TrackedMutation<'_>) -> Result<R, GraphError>,
    ) -> Result<(R, Vec<Change>), GraphError> {
        let mut guard = self.inner.write();
        let mut mutation = guard.mutate();
        let result = f(&mut mutation)?;
        let changes = mutation.commit(hlc_timestamp)?;

        // Increment containment generation if any edge changes involve
        // containment-relevant labels
        if changes_affect_containment(&changes) {
            self.containment_generation.fetch_add(1, Ordering::Relaxed);
        }

        // Publish new snapshot for lock-free readers.
        // Clone cost: O(N/256) for ChunkedVec columns (Arc pointer copies),
        // O(log N) for imbl index maps (structural sharing).
        self.snapshot.store(Arc::new(guard.clone()));

        Ok((result, changes))
    }

    /// Publish a fresh snapshot from the current mutable graph state.
    ///
    /// Call this after any direct mutation via `inner()` (e.g. schema changes)
    /// that bypasses the `write()` method. Without this, lock-free readers
    /// would see stale data.
    pub fn publish_snapshot(&self) {
        let guard = self.inner.read();
        self.snapshot.store(Arc::new(guard.clone()));
    }

    /// Publish a snapshot from a write guard -- used by MutationBatcher to
    /// avoid re-acquiring the lock. The caller holds the write guard and
    /// passes a clone of the graph. This avoids the deadlock that would
    /// occur if `publish_snapshot()` tried to acquire a read lock while
    /// the write lock is held.
    pub fn publish_snapshot_arc(&self, snapshot: Arc<SeleneGraph>) {
        self.snapshot.store(snapshot);
    }

    /// Check changes for containment-relevant edge modifications and
    /// increment the generation counter if found.
    pub fn check_containment_generation(&self, changes: &[Change]) {
        if changes_affect_containment(changes) {
            self.containment_generation.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Access the inner `Arc<RwLock<SeleneGraph>>` for components that need
    /// mutable access (e.g. schema registration, changelog subscriptions).
    ///
    /// **Important:** After mutating the graph via `inner()`, call
    /// `publish_snapshot()` to make changes visible to lock-free readers.
    pub fn inner(&self) -> &Arc<RwLock<SeleneGraph>> {
        &self.inner
    }

    /// Load a pinned snapshot for long-lived use (GQL transactions, queries).
    ///
    /// Returns an owned `Arc<SeleneGraph>` via `ArcSwap::load_full()`.
    /// Unlike `read()` which uses a short-lived Guard, this Arc can be held
    /// across async boundaries and multiple GQL statement executions.
    pub fn load_snapshot(&self) -> Arc<SeleneGraph> {
        self.snapshot.load_full()
    }

    /// Begin a multi-statement transaction.
    ///
    /// Acquires the write lock and returns a `TransactionHandle` that supports
    /// multiple mutation operations. Each `mutate()` call creates a short-lived
    /// `TrackedMutation`, commits it, and accumulates changes. The graph sees
    /// all prior mutations within the transaction because `TrackedMutation`
    /// operates in-place.
    ///
    /// - `commit()`: publishes a new snapshot and returns accumulated changes for WAL.
    /// - `drop` without commit: changes are already applied in-place (see note below).
    ///
    /// **Single-writer:** The write lock is held for the entire transaction.
    /// Read queries are unaffected (they use the ArcSwap snapshot).
    pub fn begin_transaction(&self) -> TransactionHandle<'_> {
        let guard = self.inner.write();
        // Snapshot graph state before mutations so we can restore on drop.
        // CoW ChunkedVec makes this O(N/256) Arc increments -- near-instant.
        let pre_txn_snapshot = guard.clone();
        TransactionHandle {
            guard,
            shared: self,
            changes: Vec::new(),
            committed: false,
            pre_txn_snapshot: Some(pre_txn_snapshot),
        }
    }
}

/// Handle for a multi-statement transaction.
///
/// Created by `SharedGraph::begin_transaction()`. Holds the write lock
/// for the entire transaction duration. Each `mutate()` call is internally
/// committed to keep the graph consistent for subsequent operations within
/// the same transaction.
///
/// On `commit()`: publishes a new ArcSwap snapshot, increments containment
/// generation if needed, and returns accumulated changes for WAL persistence.
///
/// On drop without commit: restores the graph to its pre-transaction state
/// using a CoW snapshot taken at `begin_transaction()`. This ensures the
/// mutable graph is never left with orphaned mutations from a failed
/// transaction.
pub struct TransactionHandle<'a> {
    guard: parking_lot::RwLockWriteGuard<'a, SeleneGraph>,
    shared: &'a SharedGraph,
    changes: Vec<Change>,
    committed: bool,
    /// Pre-transaction graph snapshot for rollback on drop without commit.
    /// CoW ChunkedVec makes this clone O(N/256) -- near-instant.
    pre_txn_snapshot: Option<SeleneGraph>,
}

impl TransactionHandle<'_> {
    /// Execute a mutation within the transaction.
    ///
    /// Creates a `TrackedMutation`, passes it to the closure, commits it,
    /// and accumulates the changes. The closure sees all prior mutations
    /// because `TrackedMutation` operates on the graph in-place.
    ///
    /// Changelog entries receive `hlc_timestamp = 0`. Use
    /// [`mutate_with_hlc`](Self::mutate_with_hlc) to stamp entries with a
    /// real HLC value.
    pub fn mutate<R>(
        &mut self,
        f: impl FnOnce(&mut TrackedMutation<'_>) -> Result<R, GraphError>,
    ) -> Result<R, GraphError> {
        self.mutate_with_hlc(0, f)
    }

    /// Like [`mutate`](Self::mutate), but stamps changelog entries with
    /// the given HLC timestamp for LWW merge.
    pub fn mutate_with_hlc<R>(
        &mut self,
        hlc_timestamp: u64,
        f: impl FnOnce(&mut TrackedMutation<'_>) -> Result<R, GraphError>,
    ) -> Result<R, GraphError> {
        let mut mutation = self.guard.mutate();
        let result = f(&mut mutation)?;
        let changes = mutation.commit(hlc_timestamp)?;
        self.changes.extend(changes);
        Ok(result)
    }

    /// Execute a mutation and return both the result and the changes produced.
    /// Used by trigger evaluation to know which changes to evaluate for cascades.
    ///
    /// Changelog entries receive `hlc_timestamp = 0`. Use
    /// [`mutate_tracked_with_hlc`](Self::mutate_tracked_with_hlc) to stamp
    /// entries with a real HLC value.
    pub fn mutate_tracked<R>(
        &mut self,
        f: impl FnOnce(&mut TrackedMutation<'_>) -> Result<R, GraphError>,
    ) -> Result<(R, Vec<Change>), GraphError> {
        self.mutate_tracked_with_hlc(0, f)
    }

    /// Like [`mutate_tracked`](Self::mutate_tracked), but stamps changelog
    /// entries with the given HLC timestamp for LWW merge.
    pub fn mutate_tracked_with_hlc<R>(
        &mut self,
        hlc_timestamp: u64,
        f: impl FnOnce(&mut TrackedMutation<'_>) -> Result<R, GraphError>,
    ) -> Result<(R, Vec<Change>), GraphError> {
        let mut mutation = self.guard.mutate();
        let result = f(&mut mutation)?;
        let changes = mutation.commit(hlc_timestamp)?;
        self.changes.extend_from_slice(&changes);
        Ok((result, changes))
    }

    /// Read the current graph state (includes all mutations from this transaction).
    pub fn graph(&self) -> &SeleneGraph {
        &self.guard
    }

    /// Mutable access to the graph (for trigger evaluation within transactions).
    pub fn graph_mut(&mut self) -> &mut SeleneGraph {
        &mut self.guard
    }

    /// Read-only access to accumulated changes (for trigger cascade evaluation).
    pub fn accumulated_changes(&self) -> &[Change] {
        &self.changes
    }

    /// Commit the transaction: publish snapshot, return changes for WAL.
    pub fn commit(mut self) -> Vec<Change> {
        self.committed = true;
        self.pre_txn_snapshot = None; // release CoW snapshot

        // Increment containment generation if any changes involve containment edges
        if changes_affect_containment(&self.changes) {
            self.shared
                .containment_generation
                .fetch_add(1, Ordering::Relaxed);
        }

        // Publish new snapshot for lock-free readers
        self.shared.snapshot.store(Arc::new(self.guard.clone()));

        std::mem::take(&mut self.changes)
    }

    /// Append externally-produced changes (e.g. from trigger evaluation)
    /// into the transaction's accumulated change list so they are included
    /// in the `Vec<Change>` returned by `commit()`.
    pub fn extend_changes(&mut self, extra: Vec<Change>) {
        self.changes.extend(extra);
    }

    /// Number of accumulated changes in this transaction.
    pub fn change_count(&self) -> usize {
        self.changes.len()
    }
}

impl Drop for TransactionHandle<'_> {
    fn drop(&mut self) {
        if !self.committed && !self.changes.is_empty() {
            // Restore graph to pre-transaction state, undoing all in-place mutations.
            if let Some(snapshot) = self.pre_txn_snapshot.take() {
                *self.guard = snapshot;
            }
            tracing::warn!(
                changes = self.changes.len(),
                "transaction dropped without commit -- graph restored to pre-transaction state"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{IStr, LabelSet, NodeId, PropertyMap, Value};

    fn labels(names: &[&str]) -> LabelSet {
        LabelSet::from_strs(names)
    }

    #[test]
    fn read_access() {
        let graph = SeleneGraph::new();
        let shared = SharedGraph::new(graph);
        let count = shared.read(SeleneGraph::node_count);
        assert_eq!(count, 0);
    }

    #[test]
    fn write_creates_and_commits() {
        let shared = SharedGraph::new(SeleneGraph::new());

        let (node_id, changes) = shared
            .write(|m| {
                let id = m.create_node(labels(&["sensor"]), PropertyMap::new())?;
                Ok(id)
            })
            .unwrap();

        assert_eq!(node_id, NodeId(1));
        // NodeCreated + LabelAdded("sensor")
        assert_eq!(changes.len(), 2);

        // Verify the node is visible via lock-free read (snapshot was published)
        let exists = shared.read(|g| g.contains_node(node_id));
        assert!(exists);
    }

    #[test]
    fn write_error_triggers_rollback() {
        let shared = SharedGraph::new(SeleneGraph::new());

        let result = shared.write(|m| {
            m.create_node(labels(&["sensor"]), PropertyMap::new())?;
            m.delete_node(NodeId(999))
        });

        assert!(result.is_err());
        let count = shared.read(SeleneGraph::node_count);
        assert_eq!(count, 0);
    }

    #[test]
    fn multiple_writes_accumulate() {
        let shared = SharedGraph::new(SeleneGraph::new());

        shared
            .write(|m| {
                m.create_node(labels(&["site"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        shared
            .write(|m| {
                m.create_node(labels(&["building"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        let count = shared.read(SeleneGraph::node_count);
        assert_eq!(count, 2);
    }

    #[test]
    fn concurrent_readers() {
        let shared = SharedGraph::new(SeleneGraph::new());

        shared
            .write(|m| {
                for i in 0..100 {
                    let props = PropertyMap::from_pairs(vec![(IStr::new("index"), Value::Int(i))]);
                    m.create_node(labels(&["sensor"]), props)?;
                }
                Ok(())
            })
            .unwrap();

        let shared1 = shared.clone();
        let shared2 = shared.clone();

        let count1 = shared1.read(SeleneGraph::node_count);
        let count2 = shared2.read(SeleneGraph::node_count);

        assert_eq!(count1, 100);
        assert_eq!(count2, 100);
    }

    #[test]
    fn inner_arc_access() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let arc = shared.inner();

        let guard = arc.read();
        assert_eq!(guard.node_count(), 0);
    }

    #[test]
    fn clone_shares_state() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let clone = shared.clone();

        shared
            .write(|m| {
                m.create_node(labels(&["sensor"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        // Clone sees the mutation via the shared ArcSwap snapshot
        let count = clone.read(SeleneGraph::node_count);
        assert_eq!(count, 1);
    }

    #[test]
    fn write_returns_user_value_and_changes() {
        let shared = SharedGraph::new(SeleneGraph::new());

        let (msg, changes) = shared
            .write(|m| {
                m.create_node(labels(&["ahu"]), PropertyMap::new())?;
                Ok("created an AHU")
            })
            .unwrap();

        assert_eq!(msg, "created an AHU");
        assert!(!changes.is_empty());
    }

    #[tokio::test]
    async fn concurrent_readers_async() {
        let shared = SharedGraph::new(SeleneGraph::new());

        shared
            .write(|m| {
                for _ in 0..50 {
                    m.create_node(labels(&["point"]), PropertyMap::new())?;
                }
                Ok(())
            })
            .unwrap();

        let mut handles = vec![];
        for _ in 0..10 {
            let s = shared.clone();
            handles.push(tokio::spawn(async move { s.read(SeleneGraph::node_count) }));
        }

        for h in handles {
            assert_eq!(h.await.unwrap(), 50);
        }
    }

    #[test]
    fn snapshot_is_consistent_point_in_time() {
        let shared = SharedGraph::new(SeleneGraph::new());

        // Write some data
        shared
            .write(|m| {
                m.create_node(labels(&["a"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        // Load snapshot — this is a consistent point-in-time view
        let snapshot = shared.snapshot.load();
        assert_eq!(snapshot.node_count(), 1);

        // Write more data
        shared
            .write(|m| {
                m.create_node(labels(&["b"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        // The old snapshot still sees 1 node (point-in-time consistency)
        assert_eq!(snapshot.node_count(), 1);

        // A new read sees 2 nodes
        assert_eq!(shared.read(SeleneGraph::node_count), 2);
    }

    #[test]
    fn load_snapshot_returns_pinned_arc() {
        let shared = SharedGraph::new(SeleneGraph::new());
        shared
            .write(|m| {
                m.create_node(labels(&["a"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        let snapshot = shared.load_snapshot();
        assert_eq!(snapshot.node_count(), 1);

        // Write more data — pinned snapshot still sees 1
        shared
            .write(|m| {
                m.create_node(labels(&["b"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        assert_eq!(snapshot.node_count(), 1);
        assert_eq!(shared.read(SeleneGraph::node_count), 2);
    }

    #[test]
    fn transaction_mutate_and_commit() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let mut txn = shared.begin_transaction();

        // Create two nodes in separate mutate calls
        txn.mutate(|m| {
            m.create_node(labels(&["sensor"]), PropertyMap::new())?;
            Ok(())
        })
        .unwrap();

        txn.mutate(|m| {
            m.create_node(labels(&["building"]), PropertyMap::new())?;
            Ok(())
        })
        .unwrap();

        // Second mutate sees first mutate's node
        assert_eq!(txn.graph().node_count(), 2);
        // 2 NodeCreated + 2 LabelAdded (initial labels)
        assert_eq!(txn.change_count(), 4);

        // Readers don't see uncommitted changes
        // (can't call shared.read() while txn holds write lock — test via load_snapshot before txn)

        // Commit
        let changes = txn.commit();
        assert_eq!(changes.len(), 4);

        // Now readers see both nodes
        assert_eq!(shared.read(SeleneGraph::node_count), 2);
    }

    #[test]
    fn transaction_drop_without_commit() {
        let shared = SharedGraph::new(SeleneGraph::new());

        {
            let mut txn = shared.begin_transaction();
            txn.mutate(|m| {
                m.create_node(labels(&["sensor"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();
            assert_eq!(txn.graph().node_count(), 1);
            // txn dropped here without commit
        }

        // Node was applied in-place (TrackedMutation committed per-mutate),
        // but the accumulated changes are discarded (not persisted to WAL).
        // The graph state includes the mutation because TrackedMutation is eager.
        // Recovery from snapshot+WAL would restore pre-transaction state.
    }

    #[test]
    fn transaction_mutate_error_rolls_back_that_statement() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let mut txn = shared.begin_transaction();

        // First mutate succeeds
        txn.mutate(|m| {
            m.create_node(labels(&["sensor"]), PropertyMap::new())?;
            Ok(())
        })
        .unwrap();

        // Second mutate fails (delete nonexistent node)
        let result = txn.mutate(|m| m.delete_node(NodeId(999)));
        assert!(result.is_err());

        // First node still exists (only second statement rolled back)
        assert_eq!(txn.graph().node_count(), 1);
        // NodeCreated + LabelAdded("sensor")
        assert_eq!(txn.change_count(), 2);

        // Can still commit the successful work
        let changes = txn.commit();
        assert_eq!(changes.len(), 2);
        assert_eq!(shared.read(SeleneGraph::node_count), 1);
    }

    #[test]
    fn transaction_containment_generation_increments() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let gen_before = shared.containment_generation();

        let mut txn = shared.begin_transaction();
        let n1 = txn
            .mutate(|m| m.create_node(labels(&["building"]), PropertyMap::new()))
            .unwrap();
        let n2 = txn
            .mutate(|m| m.create_node(labels(&["floor"]), PropertyMap::new()))
            .unwrap();
        txn.mutate(|m| {
            m.create_edge(n1, IStr::new("contains"), n2, PropertyMap::new())?;
            Ok(())
        })
        .unwrap();
        txn.commit();

        assert!(shared.containment_generation() > gen_before);
    }

    // ── Concurrency stress tests ──────────────────────────────────────

    #[test]
    fn concurrent_readers_see_consistent_snapshot() {
        // Multiple threads reading simultaneously must all see the same
        // node count from a single atomic snapshot load.
        let shared = SharedGraph::new(SeleneGraph::new());
        shared
            .write(|m| {
                for _ in 0..200 {
                    m.create_node(labels(&["point"]), PropertyMap::new())?;
                }
                Ok(())
            })
            .unwrap();

        let shared = Arc::new(shared);
        let mut handles = vec![];
        for _ in 0..16 {
            let s = Arc::clone(&shared);
            handles.push(std::thread::spawn(move || {
                let count = s.read(SeleneGraph::node_count);
                assert_eq!(count, 200);
                count
            }));
        }
        for h in handles {
            assert_eq!(h.join().unwrap(), 200);
        }
    }

    #[test]
    fn write_during_concurrent_reads_no_corruption() {
        // A writer publishing new snapshots while readers are active
        // must not cause panics, data races, or inconsistent node counts.
        let shared = Arc::new(SharedGraph::new(SeleneGraph::new()));

        // Start with 10 nodes
        shared
            .write(|m| {
                for _ in 0..10 {
                    m.create_node(labels(&["sensor"]), PropertyMap::new())?;
                }
                Ok(())
            })
            .unwrap();

        let mut handles = vec![];

        // Spawn 8 readers that each read 100 times
        for _ in 0..8 {
            let s = Arc::clone(&shared);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let count = s.read(SeleneGraph::node_count);
                    // Count must be >= 10 (initial) and grow monotonically
                    // within a single snapshot, but between reads the count
                    // can jump forward as writes land.
                    assert!(count >= 10, "count {count} should be >= 10");
                }
            }));
        }

        // Spawn 1 writer that adds 50 nodes one at a time
        {
            let s = Arc::clone(&shared);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    s.write(|m| {
                        m.create_node(labels(&["sensor"]), PropertyMap::new())?;
                        Ok(())
                    })
                    .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Final state: 10 + 50 = 60 nodes
        assert_eq!(shared.read(SeleneGraph::node_count), 60);
    }

    #[test]
    fn rapid_write_read_write_cycles() {
        // Alternating write/read cycles must maintain consistency.
        let shared = SharedGraph::new(SeleneGraph::new());

        for i in 1..=50u64 {
            shared
                .write(|m| {
                    m.create_node(labels(&["cycle"]), PropertyMap::new())?;
                    Ok(())
                })
                .unwrap();

            let count = shared.read(SeleneGraph::node_count);
            assert_eq!(
                count, i as usize,
                "after write #{i}, read should see {i} nodes, got {count}"
            );
        }
    }

    #[test]
    fn read_after_write_sees_new_data() {
        // The snapshot published by write() must be visible to the next read().
        let shared = SharedGraph::new(SeleneGraph::new());

        let (node_id, _) = shared
            .write(|m| {
                let id = m.create_node(
                    labels(&["sensor"]),
                    PropertyMap::from_pairs([(IStr::new("name"), Value::str("temp_1"))]),
                )?;
                Ok(id)
            })
            .unwrap();

        // Immediately verify both existence and property content
        shared.read(|g| {
            let node = g.get_node(node_id).expect("node should exist");
            let name = node
                .properties
                .get(IStr::new("name"))
                .expect("property should exist");
            assert_eq!(*name, Value::str("temp_1"));
        });
    }

    #[test]
    fn multiple_sequential_writes_accumulate() {
        // Each write atomically commits, so sequential writes accumulate.
        let shared = SharedGraph::new(SeleneGraph::new());

        for _ in 0..100 {
            shared
                .write(|m| {
                    m.create_node(labels(&["equip"]), PropertyMap::new())?;
                    Ok(())
                })
                .unwrap();
        }

        assert_eq!(shared.read(SeleneGraph::node_count), 100);
    }

    #[test]
    fn snapshot_pinning_reader_holds_old_while_writer_updates() {
        // A reader that pins a snapshot via load_snapshot() must see the
        // graph as it was at pin time, even after subsequent writes.
        let shared = SharedGraph::new(SeleneGraph::new());

        shared
            .write(|m| {
                m.create_node(labels(&["gen1"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        // Pin the current snapshot
        let pinned = shared.load_snapshot();
        assert_eq!(pinned.node_count(), 1);

        // Write more data
        for _ in 0..10 {
            shared
                .write(|m| {
                    m.create_node(labels(&["gen2"]), PropertyMap::new())?;
                    Ok(())
                })
                .unwrap();
        }

        // Pinned snapshot still sees 1 node
        assert_eq!(pinned.node_count(), 1);
        // Current read sees 11
        assert_eq!(shared.read(SeleneGraph::node_count), 11);
    }

    #[test]
    fn concurrent_writers_serialize_correctly() {
        // Two writer threads racing to create nodes. The RwLock serializes
        // them, so the final count must equal the sum of both writers' work.
        let shared = Arc::new(SharedGraph::new(SeleneGraph::new()));

        let s1 = Arc::clone(&shared);
        let s2 = Arc::clone(&shared);

        let h1 = std::thread::spawn(move || {
            for _ in 0..50 {
                s1.write(|m| {
                    m.create_node(labels(&["from_t1"]), PropertyMap::new())?;
                    Ok(())
                })
                .unwrap();
            }
        });

        let h2 = std::thread::spawn(move || {
            for _ in 0..50 {
                s2.write(|m| {
                    m.create_node(labels(&["from_t2"]), PropertyMap::new())?;
                    Ok(())
                })
                .unwrap();
            }
        });

        h1.join().unwrap();
        h2.join().unwrap();

        assert_eq!(shared.read(SeleneGraph::node_count), 100);
    }

    #[test]
    fn clone_shares_writes_across_threads() {
        // SharedGraph::clone() shares the same underlying data.
        // Writes through one clone are visible to reads from the other.
        let shared = SharedGraph::new(SeleneGraph::new());
        let clone = shared.clone();

        let handle = std::thread::spawn(move || {
            clone
                .write(|m| {
                    m.create_node(labels(&["remote"]), PropertyMap::new())?;
                    Ok(())
                })
                .unwrap();
        });
        handle.join().unwrap();

        assert_eq!(shared.read(SeleneGraph::node_count), 1);
    }

    #[test]
    fn transaction_isolation_from_readers() {
        // While a transaction holds the write lock, readers continue to see
        // the pre-transaction snapshot (not uncommitted mutations).
        let shared = SharedGraph::new(SeleneGraph::new());

        // Create initial data visible to all
        shared
            .write(|m| {
                m.create_node(labels(&["existing"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        // Pin snapshot before transaction
        let pre_txn = shared.load_snapshot();
        assert_eq!(pre_txn.node_count(), 1);

        // Begin transaction (holds write lock)
        let mut txn = shared.begin_transaction();
        txn.mutate(|m| {
            m.create_node(labels(&["in_txn"]), PropertyMap::new())?;
            Ok(())
        })
        .unwrap();

        // Pre-transaction snapshot still sees 1 node
        assert_eq!(pre_txn.node_count(), 1);

        // Commit and verify
        txn.commit();
        assert_eq!(shared.read(SeleneGraph::node_count), 2);
    }

    #[test]
    fn transaction_rollback_restores_graph() {
        let shared = SharedGraph::new(SeleneGraph::new());

        // Create initial data
        shared
            .write(|m| {
                m.create_node(labels(&["keep"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        {
            let mut txn = shared.begin_transaction();
            txn.mutate(|m| {
                m.create_node(labels(&["discard"]), PropertyMap::new())?;
                Ok(())
            })
            .unwrap();
            assert_eq!(txn.graph().node_count(), 2);
            // Drop without commit triggers rollback
        }

        // Graph should be restored to pre-transaction state
        let inner = shared.inner().read();
        assert_eq!(inner.node_count(), 1, "rollback should restore to 1 node");
    }

    #[test]
    fn containment_generation_unchanged_for_non_containment_edges() {
        let shared = SharedGraph::new(SeleneGraph::new());
        let gen_before = shared.containment_generation();

        shared
            .write(|m| {
                let a = m.create_node(labels(&["a"]), PropertyMap::new())?;
                let b = m.create_node(labels(&["b"]), PropertyMap::new())?;
                m.create_edge(a, IStr::new("feeds"), b, PropertyMap::new())?;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            shared.containment_generation(),
            gen_before,
            "non-containment edge should not increment generation"
        );
    }

    #[test]
    fn write_with_hlc_records_timestamp() {
        let sg = SharedGraph::new(SeleneGraph::new());
        sg.write_with_hlc(42, |m| {
            m.create_node(LabelSet::from_strs(&["test"]), PropertyMap::new())?;
            Ok(())
        })
        .unwrap();
        let entries = sg.read(|g| g.changelog.since(0).unwrap());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].hlc_timestamp, 42);
    }
}
