//! Background tasks — periodic snapshot, TS flush, TS retention.
//!
//! All tasks run as tokio tasks spawned at server startup.
//! They share `ServerState` via `Arc` and run independently.
//! A `CancellationToken` enables graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use selene_persist::snapshot::{
    GraphSnapshot, SnapshotEdge, SnapshotNode, snapshot_filename, write_snapshot_opts,
};
use selene_ts::flush::FlushTask;
use selene_ts::retention;
use tokio_util::sync::CancellationToken;

use crate::bootstrap::ServerState;

/// Handle for managing background tasks and graceful shutdown.
pub struct BackgroundTasks {
    pub cancel: CancellationToken,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl BackgroundTasks {
    /// Signal all background tasks to stop.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    /// Wait for all background tasks to complete.
    pub async fn wait(self) {
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

/// Spawn all background tasks. Returns a handle for graceful shutdown.
///
/// The caller provides a [`CancellationToken`] so that embedders can wire
/// Selene's background tasks into their own shutdown hierarchy:
///
/// ```ignore
/// let root = CancellationToken::new();
/// let bg = spawn_background_tasks(state, root.child_token());
/// // root.cancel() shuts down Selene + embedder tasks together
/// ```
pub fn spawn_background_tasks(
    state: Arc<ServerState>,
    cancel: CancellationToken,
) -> BackgroundTasks {
    let mut handles = Vec::new();

    // Snapshot task (skip on replicas — no local persistence)
    let snapshot_interval = Duration::from_secs(state.config.persist.snapshot_interval_secs.max(1));
    if !state.replica.is_replica {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        handles.push(tokio::spawn(async move {
            snapshot_loop(s, snapshot_interval, token).await;
        }));
    }

    // TS flush task
    let flush_interval =
        Duration::from_secs(u64::from(state.config.ts.flush_interval_minutes.max(1)) * 60);
    let s = Arc::clone(&state);
    let token = cancel.clone();
    handles.push(tokio::spawn(async move {
        ts_flush_loop(s, flush_interval, token).await;
    }));

    // TS retention task (runs once per hour)
    let retention_days = state.config.ts.medium_retention_days;
    let s = Arc::clone(&state);
    let token = cancel.clone();
    handles.push(tokio::spawn(async move {
        ts_retention_loop(s, retention_days, token).await;
    }));

    // TS compaction task (runs once per compact_after_hours)
    if state.config.ts.compact_after_hours > 0 {
        let compact_interval =
            Duration::from_secs(u64::from(state.config.ts.compact_after_hours) * 3600);
        let s = Arc::clone(&state);
        let token = cancel.clone();
        handles.push(tokio::spawn(async move {
            ts_compact_loop(s, compact_interval, token).await;
        }));
    }

    // Metrics update task (every 10 seconds)
    let s = Arc::clone(&state);
    let token = cancel.clone();
    handles.push(tokio::spawn(async move {
        metrics_update_loop(s, token).await;
    }));

    // Auto-embed task (vector service with configured rules)
    if state
        .services
        .get::<crate::vector_store::VectorStoreService>()
        .is_some()
        && !state.config.vector.auto_embed.is_empty()
    {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        let rules = state.config.vector.auto_embed.clone();
        handles.push(tokio::spawn(async move {
            auto_embed_loop(s, rules, token).await;
        }));
    }

    // Search index updater
    if state
        .services
        .get::<crate::search::SearchIndexService>()
        .is_some()
    {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        handles.push(tokio::spawn(async move {
            search_index_loop(s, token).await;
        }));
    }

    // Stats collector changelog subscriber (always-on)
    if state
        .services
        .get::<crate::stats_subscriber::StatsCollector>()
        .is_some()
    {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        handles.push(tokio::spawn(async move {
            stats_collector_loop(s, token).await;
        }));
    }

    // Materialized view changelog subscriber
    if state
        .services
        .get::<crate::view_state::ViewStateService>()
        .is_some()
    {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        handles.push(tokio::spawn(async move {
            view_state_loop(s, token).await;
        }));
    }

    // Vector store changelog subscriber
    if state
        .services
        .get::<crate::vector_store::VectorStoreService>()
        .is_some()
    {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        handles.push(tokio::spawn(async move {
            vector_store_loop(s, token).await;
        }));
    }

    // Version store pruning (temporal service)
    if state
        .services
        .get::<crate::version_store::VersionStoreService>()
        .is_some()
    {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        let prune_hours = state.config.temporal.prune_interval_hours;
        handles.push(tokio::spawn(async move {
            version_prune_loop(s, prune_hours, token).await;
        }));
    }

    // Bidirectional sync task (edge-to-hub push + hub-to-edge pull)
    if state.config.sync.is_enabled() {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        handles.push(tokio::spawn(async move {
            crate::sync_task::run_sync_loop(s, token).await;
        }));
    }

    // HNSW index rebuild task
    {
        let s = Arc::clone(&state);
        let token = cancel.clone();
        handles.push(tokio::spawn(async move {
            hnsw_rebuild_loop(s, token).await;
        }));
    }

    tracing::info!(
        snapshot_interval_secs = snapshot_interval.as_secs(),
        flush_interval_mins = state.config.ts.flush_interval_minutes,
        retention_days = retention_days,
        "background tasks started"
    );

    BackgroundTasks { cancel, handles }
}

/// Take a final snapshot before shutdown. Called from the signal handler.
pub fn shutdown_snapshot(state: &ServerState) {
    if state.replica.is_replica {
        return;
    }
    let entry_count = state.persistence.wal.lock().entry_count();
    if entry_count > 0 {
        tracing::info!("taking final snapshot before shutdown");
        if let Err(e) = take_snapshot(state) {
            tracing::error!("final snapshot failed: {e}");
        }
    }
}

/// Periodically snapshot the graph and truncate WAL.
/// Also forces a snapshot when WAL entry count exceeds `snapshot_max_wal_entries`.
async fn snapshot_loop(state: Arc<ServerState>, interval: Duration, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(interval);
    // Check WAL size every 10 seconds for threshold-based snapshots
    let mut wal_check = tokio::time::interval(Duration::from_secs(10));
    tick.tick().await;
    wal_check.tick().await;

    let max_entries = state.config.persist.snapshot_max_wal_entries;

    loop {
        let should_snapshot = tokio::select! {
            _ = tick.tick() => true,
            _ = wal_check.tick() => {
                let count = state.persistence.wal.lock().entry_count();
                count >= max_entries
            }
            _ = cancel.cancelled() => {
                tracing::debug!("snapshot task shutting down");
                return;
            }
        };

        if !should_snapshot {
            continue;
        }

        let entry_count = state.persistence.wal.lock().entry_count();
        if entry_count == 0 {
            continue;
        }

        if entry_count >= max_entries {
            tracing::info!(
                entry_count,
                max_entries,
                "WAL threshold exceeded, forcing snapshot"
            );
        }

        if let Err(e) = take_snapshot(&state) {
            tracing::error!("snapshot failed: {e}");
        }
    }
}

/// Snapshot the current graph state and truncate the WAL.
pub fn take_snapshot(state: &ServerState) -> anyhow::Result<()> {
    // Hold WAL lock across graph read + truncate to prevent data loss race.
    // Mutations queue in the WAL coalescer channel until the lock is released.
    let mut wal_guard = state.persistence.wal.lock();
    let seq = wal_guard.next_sequence();

    let (raw_nodes, raw_edges, next_node, next_edge, schemas, triggers) = state.graph.read(|g| {
        let nodes: Vec<selene_core::Node> = g
            .all_node_ids()
            .filter_map(|id| g.get_node(id).map(|n| n.to_owned_node()))
            .collect();
        let edges: Vec<selene_core::Edge> = g
            .all_edge_ids()
            .filter_map(|id| g.get_edge(id).map(|e| e.to_owned_edge()))
            .collect();
        let (node_schemas, edge_schemas) = g.schema().export();
        let triggers = g.trigger_registry().to_vec();
        (
            nodes,
            edges,
            g.next_node_id(),
            g.next_edge_id(),
            selene_persist::snapshot::SnapshotSchemas {
                node_schemas,
                edge_schemas,
            },
            triggers,
        )
    });

    // Serialize extra sections (version store, etc.)
    let mut extra_sections = Vec::new();

    // Tag bytes: 0x01 = version store, 0x02 = RDF ontology.
    // Each extra section is prefixed with a 1-byte tag so that bootstrap
    // can identify sections by tag rather than relying on positional index.
    if let Some(vs_svc) = state
        .services
        .get::<crate::version_store::VersionStoreService>()
    {
        let serializable = vs_svc.store.read().to_serializable();
        match postcard::to_allocvec(&serializable) {
            Ok(bytes) => {
                let mut tagged = Vec::with_capacity(1 + bytes.len());
                tagged.push(0x01);
                tagged.extend_from_slice(&bytes);
                extra_sections.push(tagged);
            }
            Err(e) => tracing::warn!("failed to serialize version store: {e}"),
        }
    }

    // Serialize RDF ontology store as a tagged extra section
    if let Some(ontology_arc) = state.rdf_ontology.as_ref() {
        let ontology = ontology_arc.read();
        if !ontology.is_empty() {
            match ontology.to_nquads() {
                Ok(bytes) => {
                    let mut tagged = Vec::with_capacity(1 + bytes.len());
                    tagged.push(0x02);
                    tagged.extend_from_slice(&bytes);
                    extra_sections.push(tagged);
                }
                Err(e) => tracing::warn!("failed to serialize RDF ontology: {e}"),
            }
        }
    }

    // Serialize HNSW vector index as a tagged extra section (tag 0x03).
    // Only written when the index is non-empty; an absent section is treated
    // as empty on restore, so old snapshots remain compatible.
    state.graph.read(|g| {
        if let Some(hnsw) = g.hnsw_index() {
            let hnsw_graph = hnsw.load_graph();
            if !hnsw_graph.is_empty() {
                match hnsw_graph.to_bytes() {
                    Ok(bytes) => {
                        let mut tagged = Vec::with_capacity(1 + bytes.len());
                        tagged.push(0x03);
                        tagged.extend_from_slice(&bytes);
                        extra_sections.push(tagged);
                    }
                    Err(e) => tracing::warn!("failed to serialize HNSW index: {e}"),
                }
            }
        }
    });

    // Serialize materialized view definitions as tagged extra section (tag 0x04).
    state.graph.read(|g| {
        let view_defs = g.view_registry().to_vec();
        if !view_defs.is_empty() {
            match postcard::to_allocvec(&view_defs) {
                Ok(bytes) => {
                    let mut tagged = Vec::with_capacity(1 + bytes.len());
                    tagged.push(0x04);
                    tagged.extend_from_slice(&bytes);
                    extra_sections.push(tagged);
                }
                Err(e) => tracing::warn!("failed to serialize view definitions: {e}"),
            }
        }
    });

    let snapshot = GraphSnapshot {
        nodes: raw_nodes.iter().map(SnapshotNode::from_node).collect(),
        edges: raw_edges.iter().map(SnapshotEdge::from_edge).collect(),
        next_node_id: next_node,
        next_edge_id: next_edge,
        changelog_sequence: seq.saturating_sub(1),
        schemas,
        triggers,
        extra_sections,
    };
    let snap_dir = state.config.data_dir.join("snapshots");
    let snap_path = snap_dir.join(snapshot_filename(seq));

    let bytes = write_snapshot_opts(&snapshot, &snap_path, state.config.persist.fsync_parent_dir)?;

    // When sync is enabled, cap WAL truncation at the sync cursor so that
    // un-pushed entries are preserved for the next upstream push. The
    // atomic avoids loading the cursor file from disk on every snapshot.
    let truncate_floor = if state.config.sync.is_enabled() {
        state
            .sync
            .last_pushed_seq
            .load(std::sync::atomic::Ordering::Relaxed)
    } else {
        0
    };

    let truncate_seq = seq.saturating_sub(1);
    if truncate_floor > 0 && truncate_seq > truncate_floor {
        tracing::info!(
            truncate_seq,
            truncate_floor,
            "WAL truncation capped at sync cursor"
        );
        wal_guard.truncate(truncate_floor)?;
    } else {
        wal_guard.truncate(truncate_seq)?;
    }
    drop(wal_guard);

    tracing::info!(
        path = %snap_path.display(),
        bytes = bytes,
        sequence = seq,
        nodes = snapshot.nodes.len(),
        edges = snapshot.edges.len(),
        "snapshot written, WAL truncated"
    );

    cleanup_old_snapshots(&snap_dir, state.config.persist.max_snapshots);

    Ok(())
}

/// Periodically flush expired hot tier data to Parquet.
async fn ts_flush_loop(state: Arc<ServerState>, interval: Duration, cancel: CancellationToken) {
    let ts_dir = state.config.data_dir.join("ts");
    let flush_task = FlushTask::new(Arc::clone(&state.hot_tier), &ts_dir);

    let mut tick = tokio::time::interval(interval);
    tick.tick().await;

    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = cancel.cancelled() => {
                tracing::debug!("TS flush task shutting down");
                return;
            }
        }

        match flush_task.flush_once() {
            Ok(0) => {}
            Ok(n) => tracing::info!(samples = n, "TS flush complete"),
            Err(e) => tracing::error!("TS flush failed: {e}"),
        }

        // Evict idle buffers (sensors that stopped reporting)
        let eviction_hours = state.config.ts.idle_eviction_hours;
        if eviction_hours > 0 {
            let cutoff =
                selene_core::now_nanos() - i64::from(eviction_hours) * 3_600 * 1_000_000_000;
            let evicted = state.hot_tier.evict_idle(cutoff);
            if evicted > 0 {
                tracing::info!(evicted, "evicted idle TS buffers");
            }
        }
    }
}

/// Periodically clean up expired Parquet directories.
async fn ts_retention_loop(
    state: Arc<ServerState>,
    retention_days: u32,
    cancel: CancellationToken,
) {
    let ts_dir = state.config.data_dir.join("ts");

    let mut tick = tokio::time::interval(Duration::from_secs(3600));
    tick.tick().await;

    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = cancel.cancelled() => {
                tracing::debug!("TS retention task shutting down");
                return;
            }
        }

        let pipeline = if state.export_pipeline.is_empty() {
            None
        } else {
            Some(&*state.export_pipeline)
        };
        match retention::cleanup_expired_with_export(&ts_dir, retention_days, pipeline).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(deleted = n, "TS retention cleanup"),
            Err(e) => tracing::error!("TS retention failed: {e}"),
        }
    }
}

/// Periodically compact old TS date directories (merge small files).
async fn ts_compact_loop(state: Arc<ServerState>, interval: Duration, cancel: CancellationToken) {
    let ts_dir = state.config.data_dir.join("ts");
    let min_age_hours = state.config.ts.compact_after_hours;

    let mut tick = tokio::time::interval(interval);
    tick.tick().await; // skip immediate first tick

    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = cancel.cancelled() => {
                tracing::debug!("TS compaction task shutting down");
                return;
            }
        }

        match selene_ts::compact::compact_old_directories(&ts_dir, min_age_hours) {
            Ok(0) => {}
            Ok(n) => tracing::info!(compacted = n, "TS daily compaction"),
            Err(e) => tracing::error!("TS compaction failed: {e}"),
        }
    }
}

/// Delete old snapshot files, keeping the `keep` most recent.
fn cleanup_old_snapshots(snap_dir: &std::path::Path, keep: usize) {
    let Ok(entries) = std::fs::read_dir(snap_dir) else {
        return;
    };

    let mut snap_files: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().is_some_and(|ext| ext == "snap")
                && p.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("snap-"))
        })
        .collect();

    snap_files.sort();

    if snap_files.len() <= keep {
        return;
    }

    let to_delete = snap_files.len() - keep;
    for path in &snap_files[..to_delete] {
        if let Err(e) = std::fs::remove_file(path) {
            tracing::warn!(path = %path.display(), error = %e, "failed to delete old snapshot");
        } else {
            tracing::debug!(path = %path.display(), "deleted old snapshot");
        }
    }

    if to_delete > 0 {
        tracing::info!(
            deleted = to_delete,
            kept = keep,
            "snapshot cleanup complete"
        );
    }
}

/// Periodically update Prometheus metrics with graph and connection stats.
async fn metrics_update_loop(state: Arc<ServerState>, cancel: CancellationToken) {
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    tick.tick().await; // skip immediate first tick

    // Prune the auth rate limiter every ~60s (6 ticks at 10s each).
    let mut prune_counter: u32 = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {
                crate::metrics::update_graph_stats(&state);

                prune_counter += 1;
                if prune_counter >= 6 {
                    prune_counter = 0;
                    state.auth_rate_limiter.prune_expired();

                    // Prune expired OAuth refresh tokens and deny-list entries.
                    if let Some(oauth_svc) =
                        state.services.get::<crate::http::mcp::oauth::OAuthService>()
                    {
                        oauth_svc.token_service.prune_expired();
                    }

                    // Prune expired authorization codes and CSRF nonces.
                    if let Some(code_store) =
                        state.services.get::<crate::http::mcp::oauth::AuthCodeStore>()
                    {
                        code_store.prune_expired();
                    }
                }
            }
        }
    }
}

/// Background search index updater — watches changelog and incrementally updates tantivy indexes.
async fn search_index_loop(state: Arc<ServerState>, cancel: CancellationToken) {
    use selene_core::changeset::Change;

    let mut rx = state.persistence.changelog_notify.subscribe();
    let mut last_seq: u64 = 0;
    let commit_interval = Duration::from_secs(1);
    let mut commit_tick = tokio::time::interval(commit_interval);

    tracing::info!("search index updater started");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                if let Some(svc) = state.services.get::<crate::search::SearchIndexService>() {
                    svc.index.commit_all();
                }
                tracing::debug!("search index updater shutting down");
                return;
            }
            result = rx.recv() => {
                match result {
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "search indexer lagged, advancing sequence");
                        last_seq = state.persistence.changelog.lock().current_sequence();
                        continue;
                    }
                    Err(_) => return,
                }

                let search = match state.services.get::<crate::search::SearchIndexService>() {
                    Some(svc) => &svc.index,
                    None => continue,
                };

                let changes = state.persistence.changelog.lock().since(last_seq).unwrap_or_default();
                if let Some(last) = changes.last() {
                    last_seq = last.sequence;
                }

                let snapshot = state.graph.load_snapshot();
                for entry in &changes {
                    for change in &entry.changes {
                        match change {
                            Change::PropertySet { node_id, key, value, .. } => {
                                if let Some(text) = value.as_str()
                                    && let Some(node) = snapshot.get_node(*node_id)
                                {
                                    for label in node.labels.iter() {
                                        search.index_property(*node_id, label, *key, text);
                                    }
                                }
                            }
                            Change::NodeDeleted { node_id, .. } => {
                                search.remove_node(*node_id);
                            }
                            Change::PropertyRemoved { node_id, .. } => {
                                // Remove from all indexes for this node (conservative but safe)
                                search.remove_node(*node_id);
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ = commit_tick.tick() => {
                if let Some(svc) = state.services.get::<crate::search::SearchIndexService>() {
                    svc.index.commit_all();
                }
            }
        }
    }
}

/// Auto-embed background task — watches changelog for text property changes
/// and generates vector embeddings asynchronously.
async fn auto_embed_loop(
    state: Arc<ServerState>,
    rules: Vec<crate::config::AutoEmbedRule>,
    cancel: CancellationToken,
) {
    use selene_core::changeset::Change;
    use selene_core::{IStr, Value};
    use std::collections::HashMap;
    use std::hash::{Hash, Hasher};

    /// Compute a content hash for deduplication. If the text hasn't changed
    /// since the last embed, we skip the (expensive) embed() call.
    fn content_hash(text: &str) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        text.hash(&mut hasher);
        hasher.finish()
    }

    let mut rx = state.persistence.changelog_notify.subscribe();
    let mut last_seq: u64 = 0;

    // Content-hash cache: (node_id, text_property) -> hash of last embedded text.
    // Persists across loop iterations but lost on restart (fine -- embeddings
    // are regenerated from the graph on startup).
    let mut embed_cache: HashMap<(u64, String), u64> = HashMap::new();

    // Debounce: accumulate changes for 200ms before processing
    let debounce = Duration::from_millis(200);

    tracing::info!(rules = rules.len(), "auto-embed task started");

    loop {
        // Wait for a changelog notification or cancellation
        let _seq = tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("auto-embed task shutting down");
                return;
            }
            result = rx.recv() => {
                match result {
                    Ok(seq) => seq,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "auto-embed lagged, advancing sequence");
                        last_seq = state.persistence.changelog.lock().current_sequence();
                        continue;
                    }
                    Err(_) => return, // channel closed
                }
            }
        };

        // Debounce: wait briefly to batch multiple rapid changes
        tokio::time::sleep(debounce).await;

        // Read all new changes since last processed sequence
        let changes: Vec<selene_graph::ChangelogEntry> = {
            let log = state.persistence.changelog.lock();
            log.since(last_seq).unwrap_or_default()
        };

        if changes.is_empty() {
            continue;
        }

        // Update last_seq to the highest sequence we've seen
        if let Some(last) = changes.last() {
            last_seq = last.sequence;
        }

        // Collect (node_id, text_property_value, rule) for nodes that need embedding
        let mut to_embed: HashMap<u64, (String, &crate::config::AutoEmbedRule)> = HashMap::new();

        for entry in &changes {
            for change in &entry.changes {
                if let Change::PropertySet {
                    node_id,
                    key,
                    value,
                    ..
                } = change
                {
                    let key_str = key.as_ref();
                    let text = match value.as_str() {
                        Some(s) => s.to_string(),
                        None => continue,
                    };

                    // Check if this property change matches any auto-embed rule.
                    // Skip changes to embedding properties (prevents feedback loops).
                    for rule in &rules {
                        if key_str == rule.embedding_property {
                            continue; // This is our own output, not an input
                        }
                        if rule.text_property == key_str {
                            // Verify node has the matching label
                            let nid = *node_id;
                            let label = &rule.label;
                            let has_label = state.graph.read(|g| {
                                g.get_node(nid).is_some_and(|n| {
                                    n.labels.iter().any(|l| l.as_str() == label.as_str())
                                })
                            });
                            if has_label {
                                to_embed.insert(node_id.0, (text, rule));
                                break;
                            }
                        }
                    }
                }
            }
        }

        if to_embed.is_empty() {
            continue;
        }

        tracing::debug!(count = to_embed.len(), "auto-embed: generating embeddings");

        // Generate embeddings and write them back
        let mut embedded = 0u64;
        let mut skipped = 0u64;
        for (node_id, (text, rule)) in &to_embed {
            // Content-hash deduplication: skip if the text hasn't changed
            // since the last successful embed for this entity+property.
            let hash = content_hash(text);
            let cache_key = (*node_id, rule.text_property.clone());
            if embed_cache.get(&cache_key).copied() == Some(hash) {
                skipped += 1;
                continue;
            }

            match selene_gql::runtime::embed::embed_text(text) {
                Ok(vec) => {
                    let embed_key = IStr::new(&rule.embedding_property);
                    let nid = selene_core::NodeId(*node_id);
                    if let Err(e) = state
                        .graph
                        .write(|m| -> Result<(), selene_graph::GraphError> {
                            m.set_property(
                                nid,
                                embed_key,
                                Value::Vector(std::sync::Arc::from(vec)),
                            )?;
                            Ok(())
                        })
                    {
                        tracing::warn!(
                            node_id = node_id,
                            error = %e,
                            "auto-embed: failed to write embedding"
                        );
                    } else {
                        embed_cache.insert(cache_key, hash);
                        embedded += 1;
                    }
                    // Not WAL-persisted; regenerated on recovery. Snapshot captures them.
                }
                Err(e) => {
                    tracing::warn!(
                        node_id = node_id,
                        property = &rule.text_property,
                        error = %e,
                        "auto-embed failed"
                    );
                }
            }
        }

        if embedded > 0 || skipped > 0 {
            tracing::info!(embedded, skipped, "auto-embed: batch complete");
        }
    }
}

// ── Version store pruning ────────────────────────────────────────

async fn version_prune_loop(
    state: Arc<crate::bootstrap::ServerState>,
    prune_interval_hours: u32,
    cancel: tokio_util::sync::CancellationToken,
) {
    use std::time::Duration;

    let interval_secs = u64::from(prune_interval_hours.max(1)) * 3600;
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.tick().await; // skip first immediate tick

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Some(vs_svc) = state.services.get::<crate::version_store::VersionStoreService>() {
                    let retention = vs_svc.store.read().retention_nanos();
                    let cutoff = selene_core::entity::now_nanos() - retention;
                    let pruned = vs_svc.store.write().prune(cutoff);
                    if pruned > 0 {
                        tracing::info!(pruned, "version store: pruned expired versions");
                    }
                }
            }
            _ = cancel.cancelled() => return,
        }
    }
}

/// Stats collector changelog subscriber -- keeps per-label counts in sync.
async fn stats_collector_loop(state: Arc<ServerState>, cancel: CancellationToken) {
    use selene_core::changeset::Change;

    let Some(collector) = state
        .services
        .get::<crate::stats_subscriber::StatsCollector>()
    else {
        return;
    };

    let mut rx = state.persistence.changelog_notify.subscribe();
    // Start from current sequence to skip entries covered by bootstrap rebuild
    let mut last_seq: u64 = {
        let cl = state.persistence.changelog.lock();
        cl.since(0)
            .map_or(0, |entries| entries.last().map_or(0, |e| e.sequence))
    };

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            result = rx.recv() => {
                match result {
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "stats collector lagged, rebuilding from graph");
                        state.graph.read(|g| {
                            collector.rebuild_from_graph(g.node_label_counts(), g.edge_label_counts());
                        });
                        last_seq = state.persistence.changelog.lock().current_sequence();
                        continue;
                    }
                    Err(_) => return, // channel closed
                }

                let entries = state.persistence.changelog.lock().since(last_seq).unwrap_or_default();
                if let Some(last) = entries.last() {
                    last_seq = last.sequence;
                }

                for entry in &entries {
                    for change in &entry.changes {
                        match change {
                            Change::LabelAdded { label, .. } => {
                                collector.increment_node(*label);
                            }
                            Change::LabelRemoved { label, .. } => {
                                collector.decrement_node(*label);
                            }
                            Change::NodeDeleted { labels, .. } => {
                                for label in labels {
                                    collector.decrement_node(*label);
                                }
                            }
                            Change::EdgeCreated { label, .. } => {
                                collector.increment_edge(*label);
                            }
                            Change::EdgeDeleted { label, .. } => {
                                collector.decrement_edge(*label);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}

/// Materialized view changelog subscriber -- keeps aggregate state in sync.
async fn view_state_loop(state: Arc<ServerState>, cancel: CancellationToken) {
    use selene_core::changeset::Change;

    let store = match state.services.get::<crate::view_state::ViewStateService>() {
        Some(svc) => Arc::clone(&svc.store),
        None => return,
    };

    let mut rx = state.persistence.changelog_notify.subscribe();
    // Start from current sequence to skip entries covered by bootstrap rebuild
    let mut last_seq: u64 = {
        let cl = state.persistence.changelog.lock();
        cl.since(0)
            .map_or(0, |entries| entries.last().map_or(0, |e| e.sequence))
    };

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            result = rx.recv() => {
                match result {
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "view state subscriber lagged, rebuilding");
                        state.graph.read(|g| {
                            let defs = g.view_registry().to_vec();
                            store.rebuild_all(&defs, g);
                        });
                        last_seq = state.persistence.changelog.lock().current_sequence();
                        continue;
                    }
                    Err(_) => return, // channel closed
                }

                let entries = state.persistence.changelog.lock().since(last_seq).unwrap_or_default();
                if let Some(last) = entries.last() {
                    last_seq = last.sequence;
                }

                let all_changes: Vec<Change> = entries
                    .iter()
                    .flat_map(|e| e.changes.iter().cloned())
                    .collect();
                if !all_changes.is_empty() {
                    state.graph.read(|g| {
                        let defs = g.view_registry().to_vec();
                        store.apply_changes(&all_changes, g, &defs);
                    });
                }
            }
        }
    }
}

/// Vector store changelog subscriber -- keeps contiguous vector buffer in sync.
async fn vector_store_loop(state: Arc<ServerState>, cancel: CancellationToken) {
    use selene_core::Value;
    use selene_core::changeset::Change;

    let store = match state
        .services
        .get::<crate::vector_store::VectorStoreService>()
    {
        Some(svc) => Arc::clone(&svc.store),
        None => return,
    };

    let mut rx = state.persistence.changelog_notify.subscribe();
    // Start from current sequence to skip entries covered by bootstrap rebuild
    let mut last_seq: u64 = {
        let cl = state.persistence.changelog.lock();
        cl.since(0)
            .map_or(0, |entries| entries.last().map_or(0, |e| e.sequence))
    };

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = rx.recv() => {
                let entries = state.persistence.changelog.lock().since(last_seq).unwrap_or_default();
                if let Some(last) = entries.last() {
                    last_seq = last.sequence;
                }

                let mut store = store.write();
                for entry in &entries {
                    for change in &entry.changes {
                        match change {
                            Change::PropertySet { node_id, key, value: Value::Vector(v), .. } => {
                                store.upsert(*node_id, *key, v);
                            }
                            Change::PropertySet { .. } => {}
                            Change::PropertyRemoved { node_id, key, .. } => {
                                store.remove(*node_id, *key);
                            }
                            Change::NodeDeleted { node_id, .. } => {
                                store.remove_node(*node_id);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}

// ── HNSW rebuild ────────────────────────────────────────────────

/// Background task: watches for vector property changes and maintains the HNSW index.
///
/// On startup, if the graph has vector properties but no HNSW index, builds one.
/// Then watches the changelog for vector property changes, applies incremental
/// inserts and tombstones, and periodically snapshots the mutable graph into the
/// lock-free read path.
async fn hnsw_rebuild_loop(state: Arc<ServerState>, cancel: CancellationToken) {
    use selene_core::Value;
    use selene_core::changeset::Change;

    let params = state.config.vector.hnsw_params();
    let mut rx = state.persistence.changelog_notify.subscribe();
    let mut last_seq: u64;

    // Initial build: if graph has vector properties but no HNSW index, scan
    // and build from scratch so the index is available immediately on startup.
    {
        let needs_build = state.graph.read(|g| g.hnsw_index().is_none());
        if needs_build {
            let mut vectors: Vec<(selene_core::NodeId, std::sync::Arc<[f32]>)> = Vec::new();
            state.graph.read(|g| {
                for node_id in g.all_node_ids() {
                    if let Some(node) = g.get_node(node_id) {
                        for (_, value) in node.properties.iter() {
                            if let Value::Vector(v) = value {
                                vectors.push((node_id, Arc::clone(v)));
                                break; // one vector property per node
                            }
                        }
                    }
                }
            });
            if !vectors.is_empty() {
                let stored_dims = vectors[0].1.len() as u16;
                let provider_dims = selene_gql::runtime::embed::embedding_dimensions()
                    .map(|d| d as u16)
                    .unwrap_or(stored_dims);

                if stored_dims != provider_dims {
                    tracing::warn!(
                        stored = stored_dims,
                        provider = provider_dims,
                        "HNSW dimension mismatch: stored vectors have different dimensions \
                         than current embedding model. Run CALL graph.reindex() to re-embed."
                    );
                }

                let index = selene_graph::hnsw::HnswIndex::new(params.clone(), stored_dims);
                index.rebuild(vectors);
                let index_arc = std::sync::Arc::new(index);
                state.graph.inner().write().set_hnsw_index(index_arc);
                state.graph.publish_snapshot();
                tracing::info!(dimensions = stored_dims, "HNSW index built on startup");
            }
        }

        last_seq = state.persistence.changelog.lock().current_sequence();
    }

    // Watch changelog for vector property changes.
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("HNSW rebuild task shutting down");
                break;
            }
            result = rx.recv() => {
                match result {
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "HNSW rebuild task lagged, advancing sequence");
                        last_seq = state.persistence.changelog.lock().current_sequence();
                        continue;
                    }
                    Err(_) => break, // channel closed
                }

                let entries = state.persistence.changelog.lock().since(last_seq).unwrap_or_default();
                if let Some(last) = entries.last() {
                    last_seq = last.sequence;
                }

                // Get the current HNSW index; if none exists, skip until
                // the initial build path above has placed one.
                let Some(hnsw) = state.graph.load_snapshot().hnsw_index().cloned() else {
                    continue;
                };

                for entry in &entries {
                    for change in &entry.changes {
                        match change {
                            Change::PropertySet {
                                node_id,
                                value: Value::Vector(v),
                                old_value,
                                ..
                            } => {
                                // Skip if vector dimensions don't match index
                                let index_dims = hnsw.load_graph().dimensions() as usize;
                                if v.len() != index_dims {
                                    tracing::warn!(
                                        node_id = node_id.0,
                                        vec_dims = v.len(),
                                        index_dims,
                                        "skipping HNSW insert: vector dimension mismatch"
                                    );
                                    continue;
                                }
                                // Tombstone the old entry when updating an
                                // existing vector to prevent ghost nodes.
                                if matches!(old_value, Some(Value::Vector(_))) {
                                    hnsw.remove(*node_id);
                                }
                                hnsw.insert(*node_id, Arc::clone(v));
                            }
                            Change::NodeDeleted { node_id, .. } => {
                                hnsw.remove(*node_id);
                            }
                            Change::PropertyRemoved {
                                node_id,
                                old_value: Some(Value::Vector(_)),
                                ..
                            } => {
                                hnsw.remove(*node_id);
                            }
                            _ => {}
                        }
                    }
                }

                // Snapshot the mutable graph when enough mutations have
                // accumulated or tombstone ratio is too high.
                const SNAPSHOT_THRESHOLD: u64 = 100;
                const TOMBSTONE_THRESHOLD: f64 = 0.2;
                if hnsw.pending_count() >= SNAPSHOT_THRESHOLD
                    || hnsw.tombstone_ratio() > TOMBSTONE_THRESHOLD
                {
                    hnsw.snapshot();
                    tracing::info!(
                        pending = hnsw.pending_count(),
                        len = hnsw.len(),
                        "HNSW index snapshot completed"
                    );
                }
            }
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    /// Compute the effective truncation target the same way `take_snapshot`
    /// does: when a non-zero sync cursor floor exists and the snapshot
    /// sequence exceeds it, cap truncation at the floor.
    fn effective_truncate(truncate_seq: u64, truncate_floor: u64) -> u64 {
        if truncate_floor > 0 && truncate_seq > truncate_floor {
            truncate_floor
        } else {
            truncate_seq
        }
    }

    #[test]
    fn truncation_guard_caps_at_sync_cursor() {
        let truncate_seq = 100_u64;
        let truncate_floor = 50_u64;
        assert_eq!(effective_truncate(truncate_seq, truncate_floor), 50);
    }

    #[test]
    fn truncation_guard_inactive_without_sync() {
        let truncate_seq = 100_u64;
        let truncate_floor = 0_u64;
        assert_eq!(effective_truncate(truncate_seq, truncate_floor), 100);
    }

    #[test]
    fn truncation_guard_no_cap_when_seq_below_floor() {
        // When the snapshot sequence is at or below the floor, no capping
        // is needed because everything up to that point was already pushed.
        let truncate_seq = 30_u64;
        let truncate_floor = 50_u64;
        assert_eq!(effective_truncate(truncate_seq, truncate_floor), 30);
    }
}
