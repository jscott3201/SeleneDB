//! Server bootstrap — initialize all components from config.

use std::sync::Arc;

use arc_swap::ArcSwap;
use selene_graph::{ChangelogBuffer, CsrAdjacency, SchemaValidator, SeleneGraph, SharedGraph};
use selene_persist::recovery;
use selene_persist::{SyncPolicy, Wal};
use selene_ts::HotTier;
use tracing::Instrument;

use crate::auth::AuthEngine;
use crate::config::SeleneConfig;

/// Persistence-related components: WAL, changelog, and batching layer.
pub struct PersistenceState {
    pub(crate) wal: Arc<parking_lot::Mutex<Wal>>,
    pub(crate) wal_coalescer: Arc<crate::wal_coalescer::WalCoalescer>,
    pub(crate) changelog: Arc<parking_lot::Mutex<ChangelogBuffer>>,
    pub(crate) changelog_notify: tokio::sync::broadcast::Sender<u64>,
}

/// Sync protocol state: causal clocks, merge tracking, and peer subscriptions.
pub struct SyncState {
    pub(crate) hlc: Arc<uhlc::HLC>,
    pub(crate) merge_tracker: Arc<parking_lot::Mutex<crate::merge_tracker::MergeTracker>>,
    pub(crate) last_pushed_seq: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) peer_subscription_hashes:
        Arc<parking_lot::Mutex<rustc_hash::FxHashMap<String, u64>>>,
    pub(crate) peer_sync_filters: Arc<
        parking_lot::Mutex<rustc_hash::FxHashMap<String, crate::subscription::SubscriptionFilter>>,
    >,
}

/// Replica mode state: role flag, upstream address, and lag counter.
pub struct ReplicaState {
    pub(crate) is_replica: bool,
    pub(crate) primary_addr: Option<String>,
    pub(crate) lag: Option<Arc<std::sync::atomic::AtomicU64>>,
}

/// All initialized server components, ready for the QUIC listener.
pub struct ServerState {
    pub(crate) graph: SharedGraph,
    pub(crate) hot_tier: Arc<HotTier>,
    pub(crate) auth_engine: Arc<AuthEngine>,
    pub(crate) config: SeleneConfig,
    /// HTTP auth rate limiter for brute-force protection.
    pub(crate) auth_rate_limiter: crate::http::auth::AuthRateLimiter,
    /// Mutation batcher -- serializes all writes through a single background task.
    pub(crate) mutation_batcher: crate::mutation_batcher::MutationBatcher,
    /// Service registry -- typed container for runtime-activated optional services.
    pub(crate) services: crate::service_registry::ServiceRegistry,
    /// Export pipeline for TS retention (ships data before deletion).
    pub(crate) export_pipeline: Arc<selene_ts::export::ExportPipeline>,
    /// GQL plan cache -- parsed ASTs keyed by query text hash.
    pub(crate) plan_cache: Arc<selene_gql::PlanCache>,
    /// Persistence-related components: WAL, changelog, batching.
    pub(crate) persistence: PersistenceState,
    /// Sync protocol state: HLC, merge tracker, peer subscriptions.
    pub(crate) sync: SyncState,
    /// Replica mode state: role flag, upstream address, lag counter.
    pub(crate) replica: ReplicaState,
    /// Set to true once all startup services have initialized and the
    /// server is ready to serve queries. Used by the `/ready` endpoint.
    pub(crate) ready: std::sync::atomic::AtomicBool,
    /// RDF ontology store for TBox quads (None if not configured).
    pub(crate) rdf_ontology: Option<Arc<parking_lot::RwLock<selene_rdf::ontology::OntologyStore>>>,
    /// Cached RDF namespace (built once from config at startup).
    pub(crate) rdf_namespace: selene_rdf::namespace::RdfNamespace,
    /// Generation-gated CSR cache. Avoids rebuilding the CSR for every read
    /// query when the graph has not changed. The tuple stores
    /// `(generation, Arc<CsrAdjacency>)`.
    pub(crate) csr_cache: Arc<ArcSwap<(u64, Arc<CsrAdjacency>)>>,
    /// Projection catalog for graph algorithms (persists across requests).
    pub(crate) projection_catalog: selene_gql::runtime::procedures::algorithms::SharedCatalog,
    /// Set when a schema mutation has been applied in memory but its
    /// corresponding durable snapshot has failed. Subsequent schema
    /// operations will retry the snapshot before allowing any idempotent
    /// early-return path (`AlreadyExistsEqual`, `NotFound` on unregister)
    /// to report success. This closes the durability window flagged in
    /// the 2026-04-24 review for finding 11026: without the flag a
    /// retry of a schema call could hit the idempotent path and return
    /// success while the schema change is still not on disk.
    pub(crate) schema_persist_pending: std::sync::atomic::AtomicBool,
}

// ── Accessors (pub for embedder/test API, pub(crate) for internal) ─
impl ServerState {
    /// The shared graph store.
    pub fn graph(&self) -> &SharedGraph {
        &self.graph
    }

    /// The time-series hot tier.
    pub fn hot_tier(&self) -> &Arc<HotTier> {
        &self.hot_tier
    }

    /// Server configuration (immutable after bootstrap).
    pub fn config(&self) -> &SeleneConfig {
        &self.config
    }

    /// The Cedar authorization engine.
    pub fn auth_engine(&self) -> &Arc<AuthEngine> {
        &self.auth_engine
    }

    /// The changelog buffer for delta sync subscriptions.
    pub fn changelog(&self) -> &Arc<parking_lot::Mutex<ChangelogBuffer>> {
        &self.persistence.changelog
    }

    /// The write-ahead log.
    pub fn wal(&self) -> &Arc<parking_lot::Mutex<Wal>> {
        &self.persistence.wal
    }

    /// Whether the server is ready to serve queries (background tasks started).
    pub fn is_ready(&self) -> bool {
        self.ready.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Mark the server as ready to serve queries.
    pub fn set_ready(&self) {
        self.ready.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether this node is a read-only replica.
    pub fn is_replica(&self) -> bool {
        self.replica.is_replica
    }

    /// Set replica mode (for testing).
    pub fn set_replica(&mut self, is_replica: bool) {
        self.replica.is_replica = is_replica;
    }

    /// Current replication lag in changelog sequences (None if primary).
    pub fn replica_lag(&self) -> Option<&Arc<std::sync::atomic::AtomicU64>> {
        self.replica.lag.as_ref()
    }

    /// Set the primary address for replica mode.
    pub fn set_replica_primary_addr(&mut self, addr: Option<String>) {
        self.replica.primary_addr = addr;
    }

    /// Set the replication lag counter for replica mode.
    pub fn set_replica_lag(&mut self, lag: Option<Arc<std::sync::atomic::AtomicU64>>) {
        self.replica.lag = lag;
    }

    /// The service registry for runtime-activated optional services.
    pub fn services(&self) -> &crate::service_registry::ServiceRegistry {
        &self.services
    }

    /// The GQL plan cache.
    pub fn plan_cache(&self) -> &Arc<selene_gql::PlanCache> {
        &self.plan_cache
    }

    /// The export pipeline for TS retention.
    pub fn export_pipeline(&self) -> &Arc<selene_ts::export::ExportPipeline> {
        &self.export_pipeline
    }

    /// The WAL coalescer for batched writes.
    pub fn wal_coalescer(&self) -> &Arc<crate::wal_coalescer::WalCoalescer> {
        &self.persistence.wal_coalescer
    }

    /// The mutation batcher for serialized writes.
    pub fn mutation_batcher(&self) -> &crate::mutation_batcher::MutationBatcher {
        &self.mutation_batcher
    }

    /// Broadcast channel for notifying changelog subscribers.
    pub fn changelog_notify(&self) -> &tokio::sync::broadcast::Sender<u64> {
        &self.persistence.changelog_notify
    }

    /// The HTTP auth rate limiter.
    pub fn auth_rate_limiter(&self) -> &crate::http::auth::AuthRateLimiter {
        &self.auth_rate_limiter
    }

    /// The Hybrid Logical Clock.
    pub fn hlc(&self) -> &Arc<uhlc::HLC> {
        &self.sync.hlc
    }

    /// The per-property HLC tracker for LWW merge during sync push.
    pub fn merge_tracker(&self) -> &Arc<parking_lot::Mutex<crate::merge_tracker::MergeTracker>> {
        &self.sync.merge_tracker
    }

    /// Last pushed WAL sequence (atomic, for lock-free truncation guard reads).
    pub fn last_pushed_seq(&self) -> &Arc<std::sync::atomic::AtomicU64> {
        &self.sync.last_pushed_seq
    }

    /// The primary address for replica mode (None if primary).
    pub fn replica_primary_addr(&self) -> Option<&str> {
        self.replica.primary_addr.as_deref()
    }

    /// The RDF ontology store (None if not configured).
    pub fn rdf_ontology(
        &self,
    ) -> Option<&Arc<parking_lot::RwLock<selene_rdf::ontology::OntologyStore>>> {
        self.rdf_ontology.as_ref()
    }

    /// The cached RDF namespace (built once from config at startup).
    pub fn rdf_namespace(&self) -> &selene_rdf::namespace::RdfNamespace {
        &self.rdf_namespace
    }

    /// The generation-gated CSR cache.
    pub fn csr_cache(&self) -> &Arc<ArcSwap<(u64, Arc<CsrAdjacency>)>> {
        &self.csr_cache
    }
}

/// Return a cached CSR if the graph generation matches, otherwise build a
/// fresh one and store it in the cache for subsequent callers.
pub fn get_or_build_csr(
    csr_cache: &ArcSwap<(u64, Arc<CsrAdjacency>)>,
    snapshot: &SeleneGraph,
) -> Arc<CsrAdjacency> {
    let generation = snapshot.generation();
    let cached = csr_cache.load();
    if cached.0 == generation {
        return Arc::clone(&cached.1);
    }
    let new_csr = Arc::new(CsrAdjacency::build(snapshot));
    csr_cache.store(Arc::new((generation, Arc::clone(&new_csr))));
    new_csr
}

// ── Bootstrap phase functions ────────────────────────────────────────────────

/// Recover graph state from persistence (snapshot + WAL replay), restore
/// HNSW vector index, materialized view definitions, schemas, and triggers.
///
/// Returns the shared graph and the extra sections from recovery (used by
/// temporal, RDF, and other services for their own snapshot restoration).
fn recover_graph(config: &SeleneConfig) -> anyhow::Result<(SharedGraph, Vec<Vec<u8>>)> {
    let mut graph = SeleneGraph::with_config(
        SchemaValidator::new(selene_core::schema::ValidationMode::Warn),
        config.changelog_capacity,
    );

    let recovery_result = recovery::recover(&config.data_dir)?;
    if recovery_result.nodes.is_empty() {
        tracing::info!("starting with empty graph");
    } else {
        tracing::info!(
            nodes = recovery_result.nodes.len(),
            edges = recovery_result.edges.len(),
            sequence = recovery_result.sequence,
            "recovered graph from persistence"
        );

        let nodes: Vec<selene_core::Node> = recovery_result
            .nodes
            .into_iter()
            .map(|rn| selene_core::Node {
                id: selene_core::NodeId(rn.id),
                labels: rn.labels,
                properties: rn.properties,
                created_at: rn.created_at,
                updated_at: rn.updated_at,
                version: rn.version,
                cached_json: None,
            })
            .collect();

        let edges: Vec<selene_core::Edge> = recovery_result
            .edges
            .into_iter()
            .map(|re| selene_core::Edge {
                id: selene_core::EdgeId(re.id),
                source: selene_core::NodeId(re.source),
                target: selene_core::NodeId(re.target),
                label: re.label,
                properties: re.properties,
                created_at: re.created_at,
            })
            .collect();

        graph.load_nodes(nodes);
        graph.load_edges(edges);
        graph
            .set_next_ids(recovery_result.next_node_id, recovery_result.next_edge_id)
            .expect("recovered next_ids exceed u32::MAX -- database corrupt");

        if !recovery_result.schemas.node_schemas.is_empty()
            || !recovery_result.schemas.edge_schemas.is_empty()
        {
            graph.schema_mut().import(
                recovery_result.schemas.node_schemas,
                recovery_result.schemas.edge_schemas,
            );
            graph.build_property_indexes();
            graph.build_composite_indexes();
            tracing::info!(
                node_schemas = graph.schema().node_schema_count(),
                edge_schemas = graph.schema().edge_schema_count(),
                "restored schemas from snapshot"
            );
        }
    }

    if !recovery_result.triggers.is_empty() {
        graph.trigger_registry_mut().load(recovery_result.triggers);
        tracing::info!(
            count = graph.trigger_registry().list().len(),
            "restored triggers from snapshot"
        );
    }

    let shared_graph = SharedGraph::new(graph);

    // Restore HNSW vector indexes from snapshot.
    // Tag 0x03: default namespace. Tag 0x05: named namespace.
    {
        let params = config.vector.hnsw_params();
        let mut restored = 0usize;
        let mut graph_w = shared_graph.inner().write();
        for tagged in &recovery_result.extra_sections {
            match tagged.first() {
                Some(&0x03) => {
                    let bytes = &tagged[1..];
                    if bytes.is_empty() {
                        continue;
                    }
                    match selene_graph::hnsw::HnswGraph::from_bytes(bytes) {
                        Ok(hnsw_graph) => {
                            restored += hnsw_graph.len();
                            let index =
                                std::sync::Arc::new(selene_graph::hnsw::HnswIndex::from_graph(
                                    hnsw_graph,
                                    params.clone(),
                                ));
                            graph_w.set_hnsw_index(index);
                        }
                        Err(e) => tracing::warn!("failed to deserialize HNSW index: {e}"),
                    }
                }
                Some(&0x05) => {
                    let bytes = &tagged[1..];
                    if bytes.len() < 2 {
                        continue;
                    }
                    let name_len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
                    if bytes.len() < 2 + name_len {
                        continue;
                    }
                    let ns = String::from_utf8_lossy(&bytes[2..2 + name_len]).into_owned();
                    let hnsw_bytes = &bytes[2 + name_len..];
                    if hnsw_bytes.is_empty() {
                        continue;
                    }
                    match selene_graph::hnsw::HnswGraph::from_bytes(hnsw_bytes) {
                        Ok(hnsw_graph) => {
                            restored += hnsw_graph.len();
                            let index =
                                std::sync::Arc::new(selene_graph::hnsw::HnswIndex::from_graph(
                                    hnsw_graph,
                                    params.clone(),
                                ));
                            graph_w.set_hnsw_index_for(ns.clone(), index);
                            tracing::debug!(
                                namespace = ns.as_str(),
                                "restored namespaced HNSW index"
                            );
                        }
                        Err(e) => tracing::warn!(
                            namespace = ns.as_str(),
                            "failed to deserialize HNSW index: {e}"
                        ),
                    }
                }
                _ => {}
            }
        }
        drop(graph_w);
        if restored > 0 {
            shared_graph.publish_snapshot();
            tracing::info!(vectors = restored, "restored HNSW indexes from snapshot");
        }
    }

    // Restore materialized view definitions from snapshot (tag 0x04)
    if let Some(tagged) = recovery_result
        .extra_sections
        .iter()
        .find(|s| s.first() == Some(&0x04))
    {
        let bytes = &tagged[1..];
        if !bytes.is_empty() {
            match postcard::from_bytes::<Vec<selene_graph::ViewDefinition>>(bytes) {
                Ok(defs) => {
                    let count = defs.len();
                    shared_graph
                        .inner()
                        .write()
                        .view_registry_mut()
                        .restore(defs);
                    shared_graph.publish_snapshot();
                    tracing::info!(
                        views = count,
                        "restored materialized view definitions from snapshot"
                    );
                }
                Err(e) => tracing::warn!("failed to deserialize view definitions: {e}"),
            }
        }
    }

    Ok((shared_graph, recovery_result.extra_sections))
}

/// Initialize the secure vault (key resolution + handle creation).
/// Non-fatal: returns (None, None) on failure.
fn init_vault(
    config: &SeleneConfig,
    vault_passphrase: Option<String>,
) -> (
    Option<Arc<crate::vault::VaultHandle>>,
    Option<Arc<crate::vault::crypto::MasterKey>>,
) {
    if config.vault.enabled {
        let vault_path = config
            .vault
            .vault_path
            .clone()
            .unwrap_or_else(|| config.data_dir.join("secure.vault"));
        match crate::vault::resolve_master_key(
            config.vault.master_key_file.as_deref(),
            config.dev_mode,
            Some(&vault_path),
            vault_passphrase,
        ) {
            Ok((master_key, key_source, salt)) => {
                match crate::vault::VaultHandle::open_or_create(
                    vault_path,
                    &master_key,
                    key_source,
                    salt,
                ) {
                    Ok((handle, admin_password)) => {
                        if let Some(password) = admin_password {
                            tracing::warn!(
                                "\n\
                                ╔══════════════════════════════════════════════════════╗\n\
                                ║  FIRST-RUN ADMIN CREDENTIAL                         ║\n\
                                ║  Identity: admin                                    ║\n\
                                ║  Password: {password:<40} ║\n\
                                ║  Change this credential immediately.                ║\n\
                                ╚══════════════════════════════════════════════════════╝"
                            );
                        }
                        (Some(Arc::new(handle)), Some(Arc::new(master_key)))
                    }
                    Err(e) => {
                        tracing::warn!("failed to open vault: {e} -- secure graph unavailable");
                        (None, None)
                    }
                }
            }
            Err(e) => {
                tracing::warn!("vault key not available: {e} -- secure graph disabled");
                (None, None)
            }
        }
    } else {
        if config.data_dir.join("secure.vault").exists() {
            tracing::warn!(
                "secure vault file found but vault not enabled -- secure graph unavailable"
            );
        }
        (None, None)
    }
}

/// Initialize all server components.
///
/// 1. Recover graph from snapshot + WAL (or start fresh)
/// 2. Create hot tier for time-series
/// 3. Open WAL for appending
/// 4. Initialize auth, federation, vault, and services
///
/// `vault_passphrase` is the pre-read `SELENE_VAULT_PASSPHRASE` env var value,
/// cleared from the process environment in `main()` before tokio starts.
#[tracing::instrument(skip_all, fields(profile = ?config.profile, data_dir = %config.data_dir.display()))]
pub async fn bootstrap(
    config: SeleneConfig,
    vault_passphrase: Option<String>,
) -> anyhow::Result<ServerState> {
    // Ensure data directory exists
    std::fs::create_dir_all(&config.data_dir)?;

    // 1. Recover graph from persistence (snapshot + WAL replay)
    let (shared_graph, recovery_extra_sections) = recover_graph(&config)?;
    let graph_inner = Arc::clone(shared_graph.inner());

    // 2. Create hot tier
    let hot_tier = Arc::new(HotTier::new(config.ts.clone()));

    // 3. Open WAL
    let wal_path = config.data_dir.join("wal.bin");
    let wal = Wal::open(&wal_path, SyncPolicy::default())?;
    let wal = Arc::new(parking_lot::Mutex::new(wal));

    // 4. Changelog buffer + broadcast
    let changelog = Arc::new(parking_lot::Mutex::new(ChangelogBuffer::new(
        config.changelog_capacity,
    )));
    let (changelog_notify, _) = tokio::sync::broadcast::channel(256);

    // 6. Initialize auth engine
    let auth_engine = if config.dev_mode {
        Arc::new(AuthEngine::dev_mode())
    } else {
        let policy_dir = config.data_dir.join("policies");
        Arc::new(AuthEngine::load(&policy_dir)?)
    };

    // 7. Initialize federation
    let federation_service = if config.federation.enabled {
        // Gather schema labels for registration
        let schema_labels: Vec<String> = {
            let g = graph_inner.read();
            g.schema()
                .all_node_schemas()
                .map(|s| s.label.to_string())
                .collect()
        };

        let registry = Arc::new(crate::federation::registry::PeerRegistry::new(
            config.federation.node_name.clone(),
            config.listen_addr.to_string(),
            schema_labels,
            config.federation.role,
            config.federation.peer_ttl_secs,
        ));

        let manager = Arc::new(crate::federation::manager::FederationManager::new(
            config.federation.clone(),
            Arc::clone(&registry),
            config.node_tls.clone(),
            config.dev_mode,
        ));

        tracing::info!(
            node_name = %config.federation.node_name,
            role = %config.federation.role,
            bootstrap_peers = ?config.federation.bootstrap_peers,
            "federation initialized"
        );

        Some(crate::federation::FederationService::new(registry, manager))
    } else {
        None
    };

    // 8. Initialize secure vault
    let (vault, vault_master_key) = init_vault(&config, vault_passphrase);

    let vault_enabled = vault.is_some();
    tracing::info!(
        addr = %config.listen_addr,
        data_dir = %config.data_dir.display(),
        dev_mode = config.dev_mode,
        vault_enabled,
        "server bootstrapped"
    );

    // Hybrid Logical Clock for causal ordering
    let hlc = Arc::new(uhlc::HLC::default());

    let wal_coalescer = if config.performance.wal_commit_delay_ms > 0 {
        let (coalescer, handle) = crate::wal_coalescer::WalCoalescer::with_group_commit(
            Arc::clone(&wal),
            Arc::clone(&changelog),
            changelog_notify.clone(),
            config.performance.wal_commit_delay_ms,
            Some(Arc::clone(&hlc)),
        );
        tokio::spawn(
            handle
                .run()
                .instrument(tracing::info_span!("wal_group_commit")),
        );
        tracing::info!(
            delay_ms = config.performance.wal_commit_delay_ms,
            "WAL group commit enabled"
        );
        Arc::new(coalescer)
    } else {
        Arc::new(
            crate::wal_coalescer::WalCoalescer::new(
                Arc::clone(&wal),
                Arc::clone(&changelog),
                changelog_notify.clone(),
            )
            .with_hlc(Arc::clone(&hlc)),
        )
    };

    // Initialize temporal version store from snapshot (if available).
    // Extra sections are tagged: first byte 0x01 = version store, 0x02 = RDF ontology.
    let temporal_version_store = if config.services.temporal.enabled {
        let vs = if let Some(tagged) = recovery_extra_sections
            .iter()
            .find(|s| s.first() == Some(&0x01))
        {
            let bytes = &tagged[1..];
            match postcard::from_bytes::<Vec<crate::version_store::SerializableVersionEntry>>(bytes)
            {
                Ok(entries) => {
                    let store = crate::version_store::VersionStore::from_serializable(
                        entries,
                        config.temporal.retention_days,
                    );
                    tracing::info!(
                        versions = store.version_count(),
                        chains = store.chain_count(),
                        "restored version store from snapshot"
                    );
                    store
                }
                Err(e) => {
                    tracing::warn!("failed to restore version store: {e}, starting fresh");
                    crate::version_store::VersionStore::new(config.temporal.retention_days)
                }
            }
        } else {
            crate::version_store::VersionStore::new(config.temporal.retention_days)
        };
        Some(Arc::new(parking_lot::RwLock::new(vs)))
    } else {
        None
    };

    // Initialize contiguous vector store from graph
    let vector_store_arc = if config.services.vector.enabled {
        let snap = shared_graph.load_snapshot();
        let mut store = crate::vector_store::VectorStore::new();
        store.rebuild_from_graph(&snap);
        tracing::info!(vectors = store.len(), "vector store initialized from graph");
        let arc = Arc::new(parking_lot::RwLock::new(store));
        crate::vector_store::init_vector_provider(Arc::clone(&arc));
        Some(arc)
    } else {
        None
    };

    // Initialize full-text search index from searchable schema properties
    let search_index_opt = {
        let snap = shared_graph.load_snapshot();
        let index_dir = config.data_dir.join("search_index");
        match crate::search::SearchIndex::open_or_create(&index_dir, snap.schema()) {
            Ok(index) => {
                if index.is_empty() {
                    tracing::debug!("no searchable properties defined — search index not created");
                    None
                } else {
                    if let Err(e) = index.rebuild_from_graph(&snap) {
                        tracing::warn!("search index rebuild failed: {e}");
                    }
                    let arc = std::sync::Arc::new(index);
                    crate::search::init_search_provider(std::sync::Arc::clone(&arc));
                    Some(arc)
                }
            }
            Err(e) => {
                tracing::warn!("search index initialization failed: {e}");
                None
            }
        }
    };

    // Initialize Prometheus metrics
    crate::metrics::init(config.metrics);

    let memory_budget = crate::mutation_batcher::MemoryBudget::new(
        config.memory.budget_bytes(),
        config.memory.soft_limit_bytes(),
    );
    let mutation_batcher = crate::mutation_batcher::MutationBatcher::spawn_with_budget(
        shared_graph.clone(),
        memory_budget,
    );
    let mut services = crate::service_registry::ServiceRegistry::new();

    // Register temporal version store as a service
    if let Some(vs) = temporal_version_store {
        services.register(crate::version_store::VersionStoreService::new(vs));
    }

    // Register vector store as a service
    if let Some(vs) = vector_store_arc {
        services.register(crate::vector_store::VectorStoreService::new(vs));
    }

    // Register search index as a service
    if let Some(si) = search_index_opt {
        services.register(crate::search::SearchIndexService::new(si));
    }

    // Register federation as a service
    if let Some(fed) = federation_service {
        services.register(fed);
    }

    // Register vault as a service (bundles handle + master key)
    if let (Some(v), Some(mk)) = (vault, vault_master_key) {
        services.register(crate::vault::VaultService::new(v, mk));
    }

    // Migrate any legacy main-graph `:principal` nodes into the vault.
    // Pre-1.3.0 deployments wrote principals to the main graph (via OAuth
    // dynamic registration and, in some clusters, manual admin setup); since
    // 1.3.0 the vault is the sole source of truth. Migration is idempotent:
    // a principal already present in the vault with the same identity is
    // skipped and the main-graph node is deleted anyway. After migration we
    // verify that no `:principal` nodes remain in the main graph — if any do
    // (e.g. the vault is not configured or a write failed), startup is aborted
    // rather than silently shipping a deployment with a split identity store.
    if let Err(e) = migrate_main_graph_principals_to_vault(&shared_graph, &services) {
        return Err(anyhow::anyhow!(
            "fatal: principal migration to vault failed: {e}"
        ));
    }

    // Register stats collector (always-on: rebuild from graph, then incremental via changelog)
    {
        let collector = crate::stats_subscriber::StatsCollector::new();
        shared_graph.read(|g| {
            collector.rebuild_from_graph(g.node_label_counts(), g.edge_label_counts());
        });
        services.register(collector);
    }

    // Register materialized view state store (rebuild from graph, then incremental via changelog)
    {
        let view_store = Arc::new(crate::view_state::ViewStateStore::new());
        shared_graph.read(|g| {
            let defs = g.view_registry().to_vec();
            if !defs.is_empty() {
                view_store.rebuild_all(&defs, g);
                tracing::info!(views = defs.len(), "rebuilt materialized view state");
            }
        });
        let provider = crate::view_state::ServerViewProvider::new(Arc::clone(&view_store));
        selene_gql::runtime::procedures::view_provider::set_view_provider(Arc::new(provider));
        services.register(crate::view_state::ViewStateService::new(view_store));
    }

    // Register OAuth token service (always registered when MCP is enabled)
    if config.mcp.enabled {
        // Priority: 1) explicit config, 2) vault-persisted key, 3) ephemeral random
        let signing_key_bytes = config
            .mcp
            .signing_key
            .as_deref()
            .and_then(|k| {
                use base64::Engine;
                match base64::engine::general_purpose::STANDARD.decode(k) {
                    Ok(bytes) if bytes.len() >= 32 => Some(bytes),
                    Ok(bytes) => {
                        tracing::error!(
                            "signing_key must be at least 32 bytes ({} provided)",
                            bytes.len()
                        );
                        None
                    }
                    Err(e) => {
                        tracing::error!("invalid base64 in [mcp] signing_key: {e}");
                        None
                    }
                }
            })
            .or_else(|| {
                // Try loading from (or generating into) the vault
                services
                    .get::<crate::vault::VaultService>()
                    .and_then(|vs| match vs.handle.resolve_signing_key(&vs.master_key) {
                        Ok(key) => Some(key),
                        Err(e) => {
                            tracing::warn!("failed to resolve signing key from vault: {e}");
                            None
                        }
                    })
            })
            .unwrap_or_else(|| {
                if !config.dev_mode {
                    tracing::warn!(
                        "no signing key configured and vault unavailable; JWT tokens will not survive restarts"
                    );
                }
                use rand::RngExt;
                let mut key = [0u8; 32];
                rand::rng().fill(&mut key[..]);
                key.to_vec()
            });

        let oauth_svc = Arc::new(crate::auth::oauth::OAuthTokenService::new(
            &signing_key_bytes,
            std::time::Duration::from_secs(config.mcp.access_token_ttl_secs),
            std::time::Duration::from_secs(config.mcp.refresh_token_ttl_secs),
        ));

        // Restore deny list from vault (revoked tokens survive restarts)
        if let Some(vs) = services.get::<crate::vault::VaultService>() {
            let deny_entries = vs.handle.load_deny_list();
            if !deny_entries.is_empty() {
                oauth_svc.load_deny_list(deny_entries);
            }
        }

        services.register(crate::http::mcp::oauth::OAuthService::new(oauth_svc));
        services.register(crate::http::mcp::oauth::AuthCodeStore::new());
    }

    // Register graph catalog (always-on core service)
    services.register(crate::service_registry::GraphCatalogService::new(Arc::new(
        parking_lot::Mutex::new(selene_graph::GraphCatalog::new()),
    )));

    #[allow(unused_mut)] // mut only needed with cloud-storage feature
    let mut export_pipeline = selene_ts::export::ExportPipeline::new();

    if let Some(ref cloud_url) = config.ts.cloud.url {
        let node_id = config.ts.cloud.node_id.clone().unwrap_or_else(|| {
            hostname::get().map_or_else(
                |_| "unknown".to_string(),
                |h| h.to_string_lossy().to_string(),
            )
        });
        let exporter = selene_ts::ObjectStoreExporter::new(cloud_url, node_id)
            .map_err(|e| anyhow::anyhow!("cloud export init: {e}"))?;
        export_pipeline.add_adapter(std::sync::Arc::new(exporter));
        tracing::info!(url = %cloud_url, "cloud export enabled");
    }

    let plan_cache = Arc::new(selene_gql::PlanCache::new());

    // Build the generation-gated CSR cache. Initialized with an empty graph;
    // the first query will trigger a full build against the recovered graph.
    let csr_cache = Arc::new(ArcSwap::from_pointee((
        0,
        Arc::new(CsrAdjacency::build(&SeleneGraph::new())),
    )));

    // Initialize RDF ontology store from snapshot extra section (if available).
    // Extra sections are tagged: first byte 0x02 = RDF ontology.
    let rdf_ontology = {
        let store = recovery_extra_sections
            .iter()
            .find(|s| s.first() == Some(&0x02))
            .and_then(|tagged| {
                let bytes = &tagged[1..];
                if bytes.is_empty() {
                    return None;
                }
                match selene_rdf::ontology::OntologyStore::from_nquads(bytes) {
                    Ok(store) => {
                        tracing::info!(quads = store.len(), "restored RDF ontology from snapshot");
                        Some(store)
                    }
                    Err(e) => {
                        tracing::warn!("failed to restore RDF ontology: {e}, starting fresh");
                        None
                    }
                }
            })
            .unwrap_or_default();
        let arc = Arc::new(parking_lot::RwLock::new(store));

        // Set up the RdfProvider OnceLock for GQL procedures
        let provider = crate::rdf_service::ServerRdfProvider::new(
            shared_graph.clone(),
            selene_rdf::namespace::RdfNamespace::new(&config.rdf.namespace),
            Arc::clone(&arc),
            Arc::clone(&csr_cache),
        );
        selene_gql::runtime::procedures::rdf::set_rdf_provider(Arc::new(provider));

        // Register as a service for health reporting
        services.register(crate::rdf_service::RdfOntologyService::new(Arc::clone(
            &arc,
        )));

        Some(arc)
    };

    let rdf_namespace = selene_rdf::namespace::RdfNamespace::new(&config.rdf.namespace);

    let persistence = PersistenceState {
        wal,
        wal_coalescer,
        changelog,
        changelog_notify,
    };

    let sync = SyncState {
        hlc,
        merge_tracker: Arc::new(parking_lot::Mutex::new(
            crate::merge_tracker::MergeTracker::new(),
        )),
        last_pushed_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        peer_subscription_hashes: Arc::new(parking_lot::Mutex::new(
            rustc_hash::FxHashMap::default(),
        )),
        peer_sync_filters: Arc::new(parking_lot::Mutex::new(rustc_hash::FxHashMap::default())),
    };

    let replica = ReplicaState {
        is_replica: false,
        primary_addr: None,
        lag: None,
    };

    let state = ServerState {
        graph: shared_graph,
        hot_tier,
        auth_engine,
        config,
        auth_rate_limiter: crate::http::auth::AuthRateLimiter::new(),
        mutation_batcher,
        services,
        export_pipeline: Arc::new(export_pipeline),
        plan_cache,
        persistence,
        sync,
        replica,
        ready: std::sync::atomic::AtomicBool::new(false),
        rdf_ontology,
        rdf_namespace,
        csr_cache,
        projection_catalog: selene_gql::runtime::procedures::algorithms::new_shared_catalog(),
        schema_persist_pending: std::sync::atomic::AtomicBool::new(false),
    };

    // Sweep any orphaned snapshot temp files left from previous runs.
    // The snapshot_export handler creates `*.snap.tmp` files and schedules
    // a delayed cleanup; if the server crashed before that cleanup ran,
    // the file would linger forever. Doing this on every boot keeps the
    // data dir tidy without needing to track per-export task handles.
    sweep_orphan_snapshot_temps(&state.config.data_dir);

    Ok(state)
}

/// Remove `*.snap.tmp` files from the data directory.
///
/// Called once at startup. The snapshot_export handler creates these as
/// temporary download files and removes them after a delay; on a crash
/// or hard-kill the cleanup task is dropped and the file lingers. This
/// best-effort sweep covers the recovery case. Errors are logged but
/// never block startup — losing a few KB to a missed cleanup is far
/// preferable to refusing to start.
/// Move any legacy `:principal` nodes from the main graph into the vault.
///
/// Called exactly once at bootstrap, immediately after the vault service is
/// registered. Pre-1.3.0 `:principal` nodes in the main graph are copied
/// into the vault (preserving identity, role, credential_hash, enabled
/// state, OAuth metadata, and any `[:scoped_to]` edge targets — these last
/// become a `scope_root_ids` property) and deleted from the main graph.
/// A main-graph principal whose identity already exists in the vault is
/// treated as stale and deleted without overwriting the vault record.
///
/// After migration we re-scan the main graph and bail if any `:principal`
/// node still exists. That guarantees the startup postcondition the rest
/// of the auth stack relies on: the main graph contains zero principals.
fn migrate_main_graph_principals_to_vault(
    shared_graph: &selene_graph::SharedGraph,
    services: &crate::service_registry::ServiceRegistry,
) -> anyhow::Result<()> {
    use selene_core::{IStr, Value};

    // Collect legacy principal records from the main graph without holding
    // the read lock while we enter the vault write path.
    struct LegacyPrincipal {
        node_id: selene_core::NodeId,
        identity: Option<String>,
        role: Option<String>,
        credential_hash: Option<String>,
        enabled: Option<bool>,
        client_name: Option<String>,
        redirect_uri: Option<String>,
        scope_roots: Vec<u64>,
    }

    fn string_prop(node: &selene_graph::NodeRef<'_>, key: &str) -> Option<String> {
        node.property(key)
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    }

    let legacy: Vec<LegacyPrincipal> = shared_graph.read(|g| {
        g.nodes_by_label("principal")
            .filter_map(|nid| {
                let node = g.get_node(nid)?;
                let enabled = match node.property("enabled") {
                    Some(Value::Bool(b)) => Some(*b),
                    _ => None,
                };
                let scope_roots: Vec<u64> = g
                    .outgoing(nid)
                    .iter()
                    .filter_map(|&eid| {
                        let edge = g.get_edge(eid)?;
                        if edge.label.as_str() == "scoped_to" {
                            Some(edge.target.0)
                        } else {
                            None
                        }
                    })
                    .collect();
                Some(LegacyPrincipal {
                    node_id: nid,
                    identity: string_prop(&node, "identity"),
                    role: string_prop(&node, "role"),
                    credential_hash: string_prop(&node, "credential_hash"),
                    enabled,
                    client_name: string_prop(&node, "client_name"),
                    redirect_uri: string_prop(&node, "redirect_uri"),
                    scope_roots,
                })
            })
            .collect()
    });

    if legacy.is_empty() {
        return Ok(());
    }

    tracing::warn!(
        count = legacy.len(),
        "migrating legacy `:principal` nodes from main graph to vault"
    );

    let Some(vault_svc) = services.get::<crate::vault::VaultService>() else {
        anyhow::bail!(
            "{} legacy main-graph `:principal` node(s) found but vault service is not configured; \
             refusing to start — configure a vault or manually purge the stale principal nodes",
            legacy.len()
        );
    };
    let vault = &vault_svc.handle;

    for p in &legacy {
        let Some(identity) = p.identity.clone() else {
            tracing::warn!(
                node_id = p.node_id.0,
                "skipping principal without identity during migration"
            );
            continue;
        };

        // Skip migration if vault already knows this identity — treat the
        // main-graph record as stale and let the subsequent delete sweep it.
        let already_in_vault = vault.graph.read(|g| {
            g.nodes_by_label("principal").any(|nid| {
                g.get_node(nid).is_some_and(|n| {
                    n.property("identity")
                        .is_some_and(|v| v.as_str() == Some(identity.as_str()))
                })
            })
        });

        if already_in_vault {
            tracing::warn!(
                identity = %identity,
                "principal already present in vault — dropping stale main-graph record"
            );
            continue;
        }

        let mut props = vec![(IStr::new("identity"), Value::str(&identity))];
        if let Some(ref role) = p.role {
            props.push((IStr::new("role"), Value::str(role)));
        }
        if let Some(ref hash) = p.credential_hash {
            props.push((IStr::new("credential_hash"), Value::str(hash)));
        }
        props.push((IStr::new("enabled"), Value::Bool(p.enabled.unwrap_or(true))));
        if let Some(ref name) = p.client_name {
            props.push((IStr::new("client_name"), Value::str(name)));
        }
        if let Some(ref uri) = p.redirect_uri {
            props.push((IStr::new("redirect_uri"), Value::str(uri)));
        }
        if !p.scope_roots.is_empty() {
            // Stored as UInt because NodeId is u64; casting to i64 via `as`
            // would silently wrap for ids above i64::MAX and the reader
            // (`projection::scope_roots`) drops negative items, corrupting
            // migrated scope data.
            let values: Arc<[Value]> = p
                .scope_roots
                .iter()
                .map(|id| Value::UInt(*id))
                .collect::<Vec<_>>()
                .into();
            props.push((IStr::new("scope_root_ids"), Value::List(values)));
        }

        vault
            .graph
            .write(|m| {
                m.create_node(
                    selene_core::LabelSet::from_strs(&["principal"]),
                    selene_core::PropertyMap::from_pairs(props),
                )
            })
            .map_err(|e| anyhow::anyhow!("vault write during migration failed: {e}"))?;

        tracing::info!(identity = %identity, "migrated principal into vault");
    }

    vault
        .flush(&vault_svc.master_key)
        .map_err(|e| anyhow::anyhow!("vault flush during migration failed: {e}"))?;

    // Collect every edge incident to a legacy principal up-front (the
    // mutation view does not expose adjacency). `delete_node` refuses to
    // drop a node with incident edges, so edges go first. We merge into a
    // HashSet because the same edge may appear in both endpoints' lists.
    let ids: Vec<selene_core::NodeId> = legacy.iter().map(|p| p.node_id).collect();
    let edges_to_drop: Vec<selene_core::EdgeId> = shared_graph.read(|g| {
        let mut set = std::collections::HashSet::new();
        for nid in &ids {
            for &eid in g.outgoing(*nid) {
                set.insert(eid);
            }
            for &eid in g.incoming(*nid) {
                set.insert(eid);
            }
        }
        set.into_iter().collect()
    });

    shared_graph
        .write(|m| {
            for eid in &edges_to_drop {
                // Ignore errors — an edge may already be gone if it was
                // shared between two principals we just processed, which is
                // fine.
                let _ = m.delete_edge(*eid);
            }
            for nid in &ids {
                m.delete_node(*nid)?;
            }
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("main-graph principal deletion failed: {e}"))?;

    // Postcondition: zero `:principal` nodes in the main graph. Anything
    // else is a silent failure we refuse to ship with.
    let remaining: usize = shared_graph.read(|g| g.nodes_by_label("principal").count());
    if remaining > 0 {
        anyhow::bail!(
            "principal migration incomplete: {remaining} `:principal` node(s) still present in main graph"
        );
    }

    Ok(())
}

fn sweep_orphan_snapshot_temps(data_dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(data_dir) else {
        return;
    };
    let mut removed = 0usize;
    for entry in entries.flatten() {
        let path = entry.path();
        let is_tmp_snapshot = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|name| name.starts_with("export-") && name.ends_with(".snap.tmp"));
        if !is_tmp_snapshot {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => removed += 1,
            Err(e) => tracing::warn!(
                path = %path.display(),
                error = %e,
                "failed to remove orphan snapshot temp file at startup"
            ),
        }
    }
    if removed > 0 {
        tracing::info!(removed, "swept orphan snapshot temp files from data_dir");
    }
}

impl ServerState {
    /// Create a minimal `ServerState` for unit testing ops functions.
    ///
    /// Uses a temp directory for WAL/snapshots, dev-mode auth (admin),
    /// and an empty graph. No QUIC, no TLS, no Cedar policy files.
    pub async fn for_testing(data_dir: &std::path::Path) -> Self {
        std::fs::create_dir_all(data_dir).unwrap();

        let graph = SeleneGraph::new();
        let shared_graph = SharedGraph::new(graph);
        let hot_tier = Arc::new(HotTier::new(selene_ts::TsConfig::default()));
        let wal_path = data_dir.join("wal.bin");
        let wal = Wal::open(&wal_path, selene_persist::SyncPolicy::OnSnapshot).unwrap();
        let wal = Arc::new(parking_lot::Mutex::new(wal));
        let changelog = Arc::new(parking_lot::Mutex::new(ChangelogBuffer::new(1_000)));
        let (changelog_notify, _) = tokio::sync::broadcast::channel(16);
        let auth_engine = Arc::new(AuthEngine::dev_mode());
        let config = crate::config::SeleneConfig::dev(data_dir);

        let hlc = Arc::new(uhlc::HLC::default());

        let wal_coalescer = Arc::new(
            crate::wal_coalescer::WalCoalescer::new(
                Arc::clone(&wal),
                Arc::clone(&changelog),
                changelog_notify.clone(),
            )
            .with_hlc(Arc::clone(&hlc)),
        );

        let mutation_batcher =
            crate::mutation_batcher::MutationBatcher::spawn(shared_graph.clone());

        let rdf_namespace = selene_rdf::namespace::RdfNamespace::new(&config.rdf.namespace);

        let persistence = PersistenceState {
            wal,
            wal_coalescer,
            changelog,
            changelog_notify,
        };

        let sync = SyncState {
            hlc,
            merge_tracker: Arc::new(parking_lot::Mutex::new(
                crate::merge_tracker::MergeTracker::new(),
            )),
            last_pushed_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_subscription_hashes: Arc::new(parking_lot::Mutex::new(
                rustc_hash::FxHashMap::default(),
            )),
            peer_sync_filters: Arc::new(parking_lot::Mutex::new(rustc_hash::FxHashMap::default())),
        };

        let replica = ReplicaState {
            is_replica: false,
            primary_addr: None,
            lag: None,
        };

        Self {
            graph: shared_graph,
            hot_tier,
            auth_engine,
            config,
            auth_rate_limiter: crate::http::auth::AuthRateLimiter::new(),
            mutation_batcher,
            ready: std::sync::atomic::AtomicBool::new(true),
            services: {
                let mut svc = crate::service_registry::ServiceRegistry::new();
                svc.register(crate::stats_subscriber::StatsCollector::new());
                let test_view_store = Arc::new(crate::view_state::ViewStateStore::new());
                selene_gql::runtime::procedures::view_provider::set_view_provider(Arc::new(
                    crate::view_state::ServerViewProvider::new(Arc::clone(&test_view_store)),
                ));
                svc.register(crate::view_state::ViewStateService::new(test_view_store));
                // Register OAuth service with ephemeral signing key for tests.
                let oauth_svc = Arc::new(crate::auth::oauth::OAuthTokenService::new(
                    b"test-signing-key-at-least-32-bytes!",
                    std::time::Duration::from_secs(900),
                    std::time::Duration::from_secs(604_800),
                ));
                svc.register(crate::http::mcp::oauth::OAuthService::new(oauth_svc));
                svc.register(crate::http::mcp::oauth::AuthCodeStore::new());
                svc.register(crate::service_registry::GraphCatalogService::new(Arc::new(
                    parking_lot::Mutex::new(selene_graph::GraphCatalog::new()),
                )));
                svc
            },
            export_pipeline: Arc::new(selene_ts::export::ExportPipeline::new()),
            plan_cache: Arc::new(selene_gql::PlanCache::new()),
            persistence,
            sync,
            replica,
            rdf_ontology: None,
            rdf_namespace,
            csr_cache: Arc::new(ArcSwap::from_pointee((
                0,
                Arc::new(CsrAdjacency::build(&SeleneGraph::new())),
            ))),
            projection_catalog: selene_gql::runtime::procedures::algorithms::new_shared_catalog(),
            schema_persist_pending: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::sweep_orphan_snapshot_temps;
    use std::fs::File;

    #[test]
    fn sweep_removes_export_temp_files_only() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();

        // Files we want gone after the sweep:
        File::create(p.join("export-1234-abcd.snap.tmp")).unwrap();
        File::create(p.join("export-5678-ef01.snap.tmp")).unwrap();

        // Files we want to survive:
        File::create(p.join("snapshot.snap")).unwrap(); // real snapshot
        File::create(p.join("wal.bin")).unwrap();
        File::create(p.join("export-1234.snap")).unwrap(); // not .tmp
        File::create(p.join("random.tmp")).unwrap(); // not export- prefix

        sweep_orphan_snapshot_temps(p);

        assert!(!p.join("export-1234-abcd.snap.tmp").exists());
        assert!(!p.join("export-5678-ef01.snap.tmp").exists());
        assert!(p.join("snapshot.snap").exists());
        assert!(p.join("wal.bin").exists());
        assert!(p.join("export-1234.snap").exists());
        assert!(p.join("random.tmp").exists());
    }

    #[test]
    fn sweep_is_a_noop_on_missing_dir() {
        // Bootstrap should never fail just because the data dir does not
        // exist yet (it is created earlier in `bootstrap`); the sweep itself
        // must tolerate that.
        let dir = tempfile::tempdir().unwrap();
        let nonexistent = dir.path().join("does-not-exist");
        sweep_orphan_snapshot_temps(&nonexistent);
        // No assertion — the test is that this returns without panicking.
    }

    #[test]
    fn sweep_handles_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        sweep_orphan_snapshot_temps(dir.path());
        // Empty dir before, empty dir after.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
