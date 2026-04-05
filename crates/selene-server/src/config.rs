//! Server configuration — TOML file + env var overlay + CLI overrides.

pub use crate::config_sync::*;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// TLS configuration for the QUIC listener.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    /// Path to PEM-encoded server certificate chain.
    pub cert_path: PathBuf,
    /// Path to PEM-encoded server private key.
    pub key_path: PathBuf,
    /// Path to PEM-encoded CA certificate for client verification (mTLS).
    pub ca_cert_path: Option<PathBuf>,
}

/// HTTP listener configuration.
#[derive(Clone, Deserialize)]
pub struct HttpConfig {
    /// Enable the HTTP listener.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// HTTP listen address (default: 0.0.0.0:8080).
    #[serde(default = "default_http_addr")]
    pub listen_addr: SocketAddr,
    /// Allowed CORS origins for production mode (default: empty = deny all cross-origin).
    /// Example: `cors_origins = ["https://dashboard.example.com"]`
    #[serde(default)]
    pub cors_origins: Vec<String>,
    /// Optional bearer token for /metrics endpoint. None = unauthenticated (dev only).
    #[serde(default)]
    pub metrics_token: Option<String>,
    /// Explicitly acknowledge plaintext HTTP in production (default: false).
    /// Set to true only if a TLS-terminating reverse proxy is in front.
    #[serde(default)]
    pub allow_plaintext: bool,
}

impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConfig")
            .field("enabled", &self.enabled)
            .field("listen_addr", &self.listen_addr)
            .field("cors_origins", &self.cors_origins)
            .field(
                "metrics_token",
                &self.metrics_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("allow_plaintext", &self.allow_plaintext)
            .finish()
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: default_http_addr(),
            cors_origins: Vec::new(),
            metrics_token: None,
            allow_plaintext: false,
        }
    }
}

/// MCP server configuration.
#[derive(Clone, Deserialize)]
pub struct McpConfig {
    /// Enable the MCP endpoint (requires HTTP).
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// API key for MCP authentication in production mode.
    ///
    /// When set, MCP clients must send `Authorization: Bearer <api_key>` on
    /// every HTTP request to `/mcp`. In dev mode this field is ignored (all
    /// requests are allowed without a key).
    ///
    /// Generate a key: `openssl rand -base64 32`
    #[serde(default)]
    pub api_key: Option<String>,

    /// HMAC-SHA256 signing key for JWT access tokens (base64-encoded).
    /// If absent, a random key is generated at startup (tokens will not
    /// survive restarts). Generate: `openssl rand -base64 32`
    #[serde(default)]
    pub signing_key: Option<String>,

    /// Require interactive approval for authorization code grants.
    /// When false (default), registered clients are auto-approved (headless).
    /// When true, an HTML consent page is shown at /oauth/authorize.
    #[serde(default)]
    pub require_approval: bool,

    /// Access token lifetime in seconds (default: 900 = 15 minutes).
    #[serde(default = "default_access_ttl")]
    pub access_token_ttl_secs: u64,

    /// Refresh token lifetime in seconds (default: 604800 = 7 days).
    #[serde(default = "default_refresh_ttl")]
    pub refresh_token_ttl_secs: u64,

    /// Static bearer token required for dynamic client registration.
    ///
    /// When set, `POST /oauth/register` requires
    /// `Authorization: Bearer <registration_token>`. This gates registration
    /// behind a secret known to the operator, preventing registration spam.
    /// When absent, registration is open (suitable for dev mode only).
    #[serde(default)]
    pub registration_token: Option<String>,

    /// MCP session idle timeout in seconds (default: 300 = 5 minutes).
    /// Sessions with no activity beyond this threshold are cleaned up.
    #[serde(default = "default_mcp_session_timeout")]
    pub session_timeout_secs: u64,

    /// Maximum concurrent MCP sessions (default: 32).
    #[serde(default = "default_mcp_max_sessions")]
    pub max_sessions: usize,
}

fn default_access_ttl() -> u64 {
    900
}
fn default_refresh_ttl() -> u64 {
    604_800
}
fn default_mcp_session_timeout() -> u64 {
    300
}
fn default_mcp_max_sessions() -> usize {
    32
}

impl std::fmt::Debug for McpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpConfig")
            .field("enabled", &self.enabled)
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field(
                "signing_key",
                &self.signing_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("require_approval", &self.require_approval)
            .field("access_token_ttl_secs", &self.access_token_ttl_secs)
            .field("refresh_token_ttl_secs", &self.refresh_token_ttl_secs)
            .field(
                "registration_token",
                &self.registration_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("session_timeout_secs", &self.session_timeout_secs)
            .field("max_sessions", &self.max_sessions)
            .finish()
    }
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            api_key: None,
            signing_key: None,
            require_approval: false,
            access_token_ttl_secs: 900,
            refresh_token_ttl_secs: 604_800,
            registration_token: None,
            session_timeout_secs: default_mcp_session_timeout(),
            max_sessions: default_mcp_max_sessions(),
        }
    }
}

/// Performance tuning configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct PerformanceConfig {
    /// Number of Rayon threads (0 = auto-detect from available cores).
    #[serde(default)]
    pub rayon_threads: usize,
    /// Query timeout in milliseconds (default: 30000).
    #[serde(default = "default_query_timeout")]
    pub query_timeout_ms: u64,
    /// Maximum concurrent queries (default: 64).
    #[serde(default = "default_max_queries")]
    pub max_concurrent_queries: usize,
    /// WAL group commit delay in milliseconds (default: 0 = immediate flush).
    /// When > 0, mutations are batched for up to this many ms before flushing,
    /// reducing fsync calls under bursty workloads. Trade latency for throughput.
    /// Recommended: edge=0 (reliability), gateway=2, cloud=5.
    #[serde(default)]
    pub wal_commit_delay_ms: u64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            rayon_threads: 0,
            query_timeout_ms: 30_000,
            max_concurrent_queries: 64,
            wal_commit_delay_ms: 0,
        }
    }
}

/// Secure vault configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct VaultConfig {
    /// Enable the secure vault.
    #[serde(default)]
    pub enabled: bool,
    /// Path to master key file (base64 or hex encoded, 32 bytes).
    pub master_key_file: Option<PathBuf>,
    /// Vault file path (default: data_dir/secure.vault).
    pub vault_path: Option<PathBuf>,
}

/// Vector embedding configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct VectorConfig {
    /// Path to model directory (safetensors + tokenizer + config).
    /// Default: {data_dir}/models/all-MiniLM-L6-v2/
    pub model_path: Option<PathBuf>,
    /// Remote embedding endpoint (alternative to local model).
    /// When set, embed() calls this HTTP endpoint instead of local candle.
    /// Example: "http://hub:8090/v1/embeddings"
    pub endpoint: Option<String>,
    /// Auto-embedding rules. When a text property changes on a node with
    /// a matching label, the background task generates an embedding and
    /// stores it on the specified embedding property.
    #[serde(default)]
    pub auto_embed: Vec<AutoEmbedRule>,
    /// Max HNSW connections per node per layer (default: 16).
    #[serde(default)]
    pub hnsw_m: Option<usize>,
    /// Max HNSW connections at layer 0 (default: 2*M).
    #[serde(default)]
    pub hnsw_m0: Option<usize>,
    /// HNSW build search width (default: 200).
    #[serde(default)]
    pub hnsw_ef_construction: Option<usize>,
    /// Default HNSW query search width (default: 50).
    #[serde(default)]
    pub hnsw_ef_search: Option<usize>,
}

impl VectorConfig {
    /// Build HnswParams from config, using defaults for unset values.
    pub fn hnsw_params(&self) -> selene_graph::hnsw::HnswParams {
        let mut params = selene_graph::hnsw::HnswParams::new(self.hnsw_m.unwrap_or(16));
        if let Some(m0) = self.hnsw_m0 {
            params.m0 = m0;
        }
        if let Some(ef) = self.hnsw_ef_construction {
            params.ef_construction = ef;
        }
        if let Some(ef) = self.hnsw_ef_search {
            params.ef_search = ef;
        }
        params
    }
}

/// A rule for automatic embedding generation.
#[derive(Debug, Clone, Deserialize)]
pub struct AutoEmbedRule {
    /// Node label to match (e.g., "sensor").
    pub label: String,
    /// Text property to embed (e.g., "name").
    pub text_property: String,
    /// Property to store the embedding (e.g., "embedding").
    #[serde(default = "default_embedding_property")]
    pub embedding_property: String,
}

fn default_embedding_property() -> String {
    "embedding".into()
}

// ── Runtime profiles & services ──────────────────────────────────────────

/// Runtime profile controlling default service activation and memory budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum ProfileType {
    #[default]
    Edge,
    Cloud,
    Standalone,
}

impl std::fmt::Display for ProfileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProfileType::Edge => write!(f, "edge"),
            ProfileType::Cloud => write!(f, "cloud"),
            ProfileType::Standalone => write!(f, "standalone"),
        }
    }
}

impl std::str::FromStr for ProfileType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "edge" => Ok(ProfileType::Edge),
            "cloud" => Ok(ProfileType::Cloud),
            "standalone" => Ok(ProfileType::Standalone),
            other => Err(format!(
                "unknown profile '{other}' — use edge, cloud, or standalone"
            )),
        }
    }
}

/// Per-service toggle.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct ServiceToggle {
    pub enabled: bool,
}

/// Services configuration — which optional subsystems are active.
/// Profile sets defaults; explicit TOML/env overrides any default.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServicesConfig {
    pub vector: ServiceToggle,
    pub search: ServiceToggle,
    pub temporal: ServiceToggle,
    pub federation: ServiceToggle,
    pub algorithms: ServiceToggle,
    pub mcp: ServiceToggle,
    pub replicas: ServiceToggle,
}

impl ServicesConfig {
    pub fn for_profile(profile: ProfileType) -> Self {
        match profile {
            ProfileType::Edge => ServicesConfig {
                vector: ServiceToggle { enabled: false },
                search: ServiceToggle { enabled: true },
                temporal: ServiceToggle { enabled: true },
                federation: ServiceToggle { enabled: false },
                algorithms: ServiceToggle { enabled: true },
                mcp: ServiceToggle { enabled: false },
                replicas: ServiceToggle { enabled: false },
            },
            ProfileType::Cloud => ServicesConfig {
                vector: ServiceToggle { enabled: true },
                search: ServiceToggle { enabled: true },
                temporal: ServiceToggle { enabled: true },
                federation: ServiceToggle { enabled: true },
                algorithms: ServiceToggle { enabled: true },
                mcp: ServiceToggle { enabled: false },
                replicas: ServiceToggle { enabled: true },
            },
            ProfileType::Standalone => ServicesConfig {
                vector: ServiceToggle { enabled: true },
                search: ServiceToggle { enabled: true },
                temporal: ServiceToggle { enabled: true },
                federation: ServiceToggle { enabled: false },
                algorithms: ServiceToggle { enabled: true },
                mcp: ServiceToggle { enabled: true },
                replicas: ServiceToggle { enabled: false },
            },
        }
    }
}

impl Default for ServicesConfig {
    fn default() -> Self {
        Self::for_profile(ProfileType::Edge)
    }
}

/// Memory budget configuration with two-threshold OOM protection.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    /// Memory budget in megabytes.
    pub budget_mb: u64,
    /// Soft limit as percentage of budget (triggers warnings + background throttle).
    pub soft_limit_percent: u8,
}

impl MemoryConfig {
    pub fn for_profile(profile: ProfileType) -> Self {
        match profile {
            ProfileType::Edge => MemoryConfig {
                budget_mb: 2048,
                soft_limit_percent: 80,
            },
            ProfileType::Cloud => MemoryConfig {
                budget_mb: 16384,
                soft_limit_percent: 80,
            },
            ProfileType::Standalone => MemoryConfig {
                budget_mb: 4096,
                soft_limit_percent: 80,
            },
        }
    }

    /// Budget in bytes.
    pub fn budget_bytes(&self) -> u64 {
        self.budget_mb * 1024 * 1024
    }

    /// Soft limit in bytes.
    pub fn soft_limit_bytes(&self) -> u64 {
        self.budget_bytes() * u64::from(self.soft_limit_percent) / 100
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self::for_profile(ProfileType::Edge)
    }
}

/// Metrics tier — basic (edge) or full (cloud/standalone).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum MetricsTier {
    #[default]
    Basic,
    Full,
}

impl MetricsTier {
    pub fn for_profile(profile: ProfileType) -> Self {
        match profile {
            ProfileType::Edge => MetricsTier::Basic,
            ProfileType::Cloud | ProfileType::Standalone => MetricsTier::Full,
        }
    }
}

/// TLS configuration for inter-node communication (replicas, federation).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct NodeTlsConfig {
    /// CA certificate for verifying peer identity.
    pub ca_cert: Option<PathBuf>,
    /// Local node certificate for mTLS.
    pub cert: Option<PathBuf>,
    /// Local node private key.
    pub key: Option<PathBuf>,
}

/// Replica-specific configuration.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ReplicaConfig {
    /// Identity string for authenticating to the primary.
    pub auth_identity: Option<String>,
    /// Credential for authenticating to the primary.
    pub auth_credentials: Option<String>,
    /// TLS server name override for the primary.
    pub server_name: Option<String>,
}

/// RDF import/export configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RdfConfig {
    /// Base namespace URI for minting RDF URIs from graph entities.
    #[serde(default = "default_rdf_namespace")]
    pub namespace: String,
    /// Materialize SOSA Observation instances for time-series data.
    #[serde(default)]
    pub materialize_observations: bool,
    /// Debounce interval (ms) for observation materialization.
    #[serde(default = "default_observation_debounce")]
    pub observation_debounce_ms: u64,
}

fn default_rdf_namespace() -> String {
    "selene:".into()
}

fn default_observation_debounce() -> u64 {
    1000
}

impl Default for RdfConfig {
    fn default() -> Self {
        Self {
            namespace: default_rdf_namespace(),
            materialize_observations: false,
            observation_debounce_ms: default_observation_debounce(),
        }
    }
}

/// Temporal version store configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TemporalConfig {
    /// Enable property version tracking.
    pub enabled: bool,
    /// Maximum age of archived versions in days.
    pub retention_days: u32,
    /// How often to prune expired versions (hours).
    pub prune_interval_hours: u32,
}

impl Default for TemporalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            retention_days: 90,
            prune_interval_hours: 1,
        }
    }
}

/// TOML-deserializable config file format.
#[derive(Debug, Clone, Deserialize, Default)]
struct ConfigFile {
    #[serde(default)]
    profile: Option<ProfileType>,
    #[serde(default)]
    listen_addr: Option<String>,
    data_dir: Option<String>,
    #[serde(default)]
    dev_mode: Option<bool>,
    tls: Option<TlsConfig>,
    #[serde(default)]
    http: Option<HttpConfig>,
    #[serde(default)]
    mcp: Option<McpConfig>,
    #[serde(default)]
    performance: Option<PerformanceConfig>,
    #[serde(default)]
    changelog_capacity: Option<usize>,
    #[serde(default)]
    quic_max_connections: Option<usize>,
    #[serde(default)]
    vault: Option<VaultConfig>,
    #[serde(default)]
    persist: Option<selene_persist::PersistConfig>,
    #[serde(default)]
    ts: Option<selene_ts::TsConfig>,
    #[serde(default)]
    vector: Option<VectorConfig>,
    #[serde(default)]
    temporal: Option<TemporalConfig>,
    #[serde(default)]
    rdf: Option<RdfConfig>,
    #[serde(default)]
    services: Option<ServicesConfig>,
    #[serde(default)]
    memory: Option<MemoryConfig>,
    #[serde(default)]
    metrics: Option<MetricsTier>,
    #[serde(default)]
    node_tls: NodeTlsConfig,
    #[serde(default)]
    replica: ReplicaConfig,
    #[serde(default)]
    sync: Option<SyncConfig>,
}

/// Top-level server configuration.
#[derive(Debug, Clone)]
pub struct SeleneConfig {
    /// Active runtime profile.
    pub profile: ProfileType,
    /// UDP address for the QUIC listener (default: 0.0.0.0:4510).
    pub listen_addr: SocketAddr,
    /// Data directory for WAL, snapshots, Parquet files.
    pub data_dir: PathBuf,
    /// Enable dev mode: self-signed TLS, no Cedar auth.
    pub dev_mode: bool,
    /// TLS configuration (required in production mode).
    pub tls: Option<TlsConfig>,
    /// Persistence settings.
    pub persist: selene_persist::PersistConfig,
    /// Time-series settings.
    pub ts: selene_ts::TsConfig,
    /// Changelog buffer capacity.
    pub changelog_capacity: usize,
    /// Maximum concurrent QUIC connections (default: 64).
    pub quic_max_connections: usize,
    /// HTTP listener settings.
    pub http: HttpConfig,
    /// MCP endpoint settings.
    pub mcp: McpConfig,
    /// Performance tuning.
    pub performance: PerformanceConfig,
    /// Secure vault settings.
    pub vault: VaultConfig,
    /// Vector embedding settings.
    pub vector: VectorConfig,
    /// Temporal version store settings.
    pub temporal: TemporalConfig,
    /// RDF import/export settings.
    pub rdf: RdfConfig,
    /// Service activation — which optional subsystems are enabled.
    pub services: ServicesConfig,
    /// Memory budget and OOM thresholds.
    pub memory: MemoryConfig,
    /// Metrics tier (basic or full).
    pub metrics: MetricsTier,
    /// TLS for inter-node communication (replicas, federation).
    pub node_tls: NodeTlsConfig,
    /// Replica-specific settings (auth, server name).
    pub replica: ReplicaConfig,
    /// Bidirectional sync settings for hub-spoke topologies.
    pub sync: SyncConfig,
    /// Federation settings.
    #[cfg(feature = "federation")]
    pub federation: crate::federation::config::FederationConfig,
}

impl SeleneConfig {
    /// Load configuration from TOML file, overlaying env vars.
    ///
    /// Priority: env vars > TOML file > defaults.
    pub fn load(path: Option<&Path>, data_dir_override: Option<&str>) -> anyhow::Result<Self> {
        let file_config = if let Some(path) = path {
            let content = std::fs::read_to_string(path)
                .map_err(|e| anyhow::anyhow!("failed to read config {}: {e}", path.display()))?;
            toml::from_str::<ConfigFile>(&content)
                .map_err(|e| anyhow::anyhow!("failed to parse config: {e}"))?
        } else {
            ConfigFile::default()
        };

        // Data dir: CLI override > env > TOML > default
        let data_dir = data_dir_override
            .map(PathBuf::from)
            .or_else(|| std::env::var("SELENE_DATA_DIR").ok().map(PathBuf::from))
            .or_else(|| file_config.data_dir.map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("/tmp/selene-data"));

        // Listen address: env > TOML > default
        let listen_str = std::env::var("SELENE_QUIC_LISTEN")
            .ok()
            .or(file_config.listen_addr)
            .unwrap_or_else(|| "0.0.0.0:4510".to_string());
        let listen_addr: SocketAddr = listen_str
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid listen address '{listen_str}': {e}"))?;

        // Dev mode: env > TOML > default (false — secure by default)
        let dev_mode = env_bool("SELENE_DEV_MODE")
            .or(file_config.dev_mode)
            .unwrap_or(false);

        // HTTP: env override for enabled
        let mut http = file_config.http.unwrap_or_default();
        if let Some(enabled) = env_bool("SELENE_HTTP_ENABLED") {
            http.enabled = enabled;
        }
        if let Ok(addr) = std::env::var("SELENE_HTTP_LISTEN")
            && let Ok(parsed) = addr.parse()
        {
            http.listen_addr = parsed;
        }
        if let Ok(token) = std::env::var("SELENE_METRICS_TOKEN") {
            http.metrics_token = Some(token);
        }

        // MCP: env override
        let mut mcp = file_config.mcp.unwrap_or_default();
        if let Some(enabled) = env_bool("SELENE_MCP_ENABLED") {
            mcp.enabled = enabled;
        }

        let performance = file_config.performance.unwrap_or_default();
        let changelog_capacity = file_config.changelog_capacity.unwrap_or(100_000);
        let quic_max_connections = file_config.quic_max_connections.unwrap_or(64);

        // Vector: TOML config
        let vector = file_config.vector.unwrap_or_default();

        // Vault: TOML config with env var override for enabled
        let mut vault = file_config.vault.unwrap_or_default();
        if let Some(enabled) = env_bool("SELENE_VAULT_ENABLED") {
            vault.enabled = enabled;
        }

        // Persistence: use TOML section if present, otherwise defaults. Fixup data_dir.
        let mut persist = file_config.persist.unwrap_or_default();
        persist.fixup_data_dir(&data_dir);

        // Time-series: use TOML section if present, otherwise defaults.
        let ts = file_config.ts.unwrap_or_default();

        // Temporal: TOML config
        let temporal = file_config.temporal.unwrap_or_default();

        // RDF: TOML config
        let rdf = file_config.rdf.unwrap_or_default();

        // ── Profile resolution ─────────────────────────────────────────
        // Priority: env > TOML > default (edge)
        let profile = std::env::var("SELENE_PROFILE")
            .ok()
            .and_then(|s| s.parse::<ProfileType>().ok())
            .or(file_config.profile)
            .unwrap_or_default();

        // Services: profile defaults, then TOML overlay, then env overrides
        let mut services = file_config
            .services
            .unwrap_or_else(|| ServicesConfig::for_profile(profile));
        // Env overrides for individual services
        if let Some(v) = env_bool("SELENE_SERVICES_VECTOR_ENABLED") {
            services.vector.enabled = v;
        }
        if let Some(v) = env_bool("SELENE_SERVICES_SEARCH_ENABLED") {
            services.search.enabled = v;
        }
        if let Some(v) = env_bool("SELENE_SERVICES_TEMPORAL_ENABLED") {
            services.temporal.enabled = v;
        }
        if let Some(v) = env_bool("SELENE_SERVICES_FEDERATION_ENABLED") {
            services.federation.enabled = v;
        }
        if let Some(v) = env_bool("SELENE_SERVICES_ALGORITHMS_ENABLED") {
            services.algorithms.enabled = v;
        }
        if let Some(v) = env_bool("SELENE_SERVICES_MCP_ENABLED") {
            services.mcp.enabled = v;
        }
        if let Some(v) = env_bool("SELENE_SERVICES_REPLICAS_ENABLED") {
            services.replicas.enabled = v;
        }

        // Memory: profile defaults, then TOML overlay, then env overrides
        let mut memory = file_config
            .memory
            .unwrap_or_else(|| MemoryConfig::for_profile(profile));
        if let Ok(v) = std::env::var("SELENE_MEMORY_BUDGET_MB")
            && let Ok(mb) = v.parse::<u64>()
        {
            memory.budget_mb = mb;
        }

        // Metrics tier: TOML or profile default
        let metrics = file_config
            .metrics
            .unwrap_or_else(|| MetricsTier::for_profile(profile));

        // Warn if data directory is under /tmp in production
        if !dev_mode && data_dir.starts_with("/tmp") {
            tracing::warn!(
                "data directory is under /tmp which is world-writable; \
                 set [persist] data_dir or SELENE_DATA_DIR for production use"
            );
        }

        Ok(Self {
            profile,
            listen_addr,
            data_dir: data_dir.clone(),
            dev_mode,
            tls: file_config.tls,
            persist,
            ts,
            changelog_capacity,
            quic_max_connections,
            http,
            mcp,
            performance,
            vault,
            vector,
            temporal,
            rdf,
            services,
            memory,
            metrics,
            node_tls: file_config.node_tls,
            replica: file_config.replica,
            sync: file_config.sync.unwrap_or_default(),
            #[cfg(feature = "federation")]
            federation: crate::federation::config::FederationConfig::default(),
        })
    }

    /// Re-apply profile defaults for services, memory, and metrics.
    /// Used when a CLI `--profile` override is specified after config loading.
    pub fn apply_profile(&mut self, profile: ProfileType) {
        self.profile = profile;
        self.services = ServicesConfig::for_profile(profile);
        self.memory = MemoryConfig::for_profile(profile);
        self.metrics = MetricsTier::for_profile(profile);
    }

    /// Create a dev-mode config with the given data directory.
    /// Uses Standalone profile defaults (all services enabled for development).
    pub fn dev(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let profile = ProfileType::Standalone;
        Self {
            profile,
            listen_addr: "0.0.0.0:4510".parse().unwrap(),
            data_dir: data_dir.clone(),
            dev_mode: true,
            tls: None,
            persist: selene_persist::PersistConfig::new(&data_dir),
            ts: selene_ts::TsConfig::default(),
            changelog_capacity: 100_000,
            quic_max_connections: 64,
            http: HttpConfig::default(),
            mcp: McpConfig::default(),
            vault: VaultConfig::default(),
            vector: VectorConfig::default(),
            temporal: TemporalConfig::default(),
            rdf: RdfConfig::default(),
            performance: PerformanceConfig::default(),
            services: ServicesConfig::for_profile(profile),
            memory: MemoryConfig::for_profile(profile),
            metrics: MetricsTier::for_profile(profile),
            node_tls: NodeTlsConfig::default(),
            replica: ReplicaConfig::default(),
            sync: SyncConfig::default(),
            #[cfg(feature = "federation")]
            federation: crate::federation::config::FederationConfig::default(),
        }
    }

    /// Create a production config with the given profile.
    pub fn production(data_dir: impl Into<PathBuf>, tls: TlsConfig) -> Self {
        let data_dir = data_dir.into();
        let profile = ProfileType::Edge;
        Self {
            profile,
            listen_addr: "0.0.0.0:4510".parse().unwrap(),
            data_dir: data_dir.clone(),
            dev_mode: false,
            tls: Some(tls),
            persist: selene_persist::PersistConfig::new(&data_dir),
            ts: selene_ts::TsConfig::default(),
            changelog_capacity: 100_000,
            quic_max_connections: 64,
            http: HttpConfig::default(),
            mcp: McpConfig::default(),
            performance: PerformanceConfig::default(),
            vault: VaultConfig::default(),
            vector: VectorConfig::default(),
            temporal: TemporalConfig::default(),
            rdf: RdfConfig::default(),
            services: ServicesConfig::for_profile(profile),
            memory: MemoryConfig::for_profile(profile),
            metrics: MetricsTier::for_profile(profile),
            node_tls: NodeTlsConfig::default(),
            replica: ReplicaConfig::default(),
            sync: SyncConfig::default(),
            #[cfg(feature = "federation")]
            federation: crate::federation::config::FederationConfig::default(),
        }
    }
}

fn env_bool(key: &str) -> Option<bool> {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
}

fn default_true() -> bool {
    true
}
fn default_http_addr() -> SocketAddr {
    "0.0.0.0:8080".parse().unwrap()
}
fn default_query_timeout() -> u64 {
    30_000
}
fn default_max_queries() -> usize {
    64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_config_defaults() {
        let config = SeleneConfig::dev("/tmp/test-data");
        assert!(config.dev_mode);
        assert_eq!(
            config.listen_addr,
            "0.0.0.0:4510".parse::<SocketAddr>().unwrap()
        );
        assert!(config.http.enabled);
        assert!(config.mcp.enabled);
        assert_eq!(config.performance.query_timeout_ms, 30_000);
    }

    #[test]
    fn load_from_toml_string() {
        let toml = r#"
listen_addr = "127.0.0.1:5000"
dev_mode = false
changelog_capacity = 50000

[http]
enabled = true
listen_addr = "0.0.0.0:9090"

[performance]
query_timeout_ms = 10000
"#;
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("selene.toml");
        std::fs::write(&config_path, toml).unwrap();

        let config = SeleneConfig::load(Some(&config_path), None).unwrap();
        assert_eq!(
            config.listen_addr,
            "127.0.0.1:5000".parse::<SocketAddr>().unwrap()
        );
        assert!(!config.dev_mode);
        assert_eq!(config.changelog_capacity, 50_000);
        assert_eq!(
            config.http.listen_addr,
            "0.0.0.0:9090".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(config.performance.query_timeout_ms, 10_000);
    }

    #[test]
    fn load_without_file_uses_defaults() {
        let config = SeleneConfig::load(None, Some("/tmp/test")).unwrap();
        // Secure by default: dev_mode is false unless explicitly set
        assert!(!config.dev_mode);
        assert_eq!(config.data_dir, PathBuf::from("/tmp/test"));
    }

    #[test]
    fn load_persist_and_ts_from_toml() {
        let toml = r#"
data_dir = "/tmp/selene-test"

[persist]
snapshot_interval_secs = 60
snapshot_max_wal_entries = 5000
max_snapshots = 5
fsync_parent_dir = false

[ts]
hot_retention_hours = 12
medium_retention_days = 14
flush_interval_minutes = 5
"#;
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("selene.toml");
        std::fs::write(&config_path, toml).unwrap();

        let config = SeleneConfig::load(Some(&config_path), None).unwrap();
        assert_eq!(config.persist.snapshot_interval_secs, 60);
        assert_eq!(config.persist.snapshot_max_wal_entries, 5000);
        assert_eq!(config.persist.max_snapshots, 5);
        assert!(!config.persist.fsync_parent_dir);
        // data_dir is set via fixup
        assert_eq!(config.persist.data_dir, config.data_dir);

        assert_eq!(config.ts.hot_retention_hours, 12);
        assert_eq!(config.ts.medium_retention_days, 14);
        assert_eq!(config.ts.flush_interval_minutes, 5);
    }

    #[test]
    fn load_defaults_when_sections_absent() {
        let config = SeleneConfig::load(None, Some("/tmp/test")).unwrap();
        // Persist defaults
        assert_eq!(config.persist.snapshot_interval_secs, 300);
        assert_eq!(config.persist.max_snapshots, 3);
        assert!(config.persist.fsync_parent_dir);
        // TS defaults
        assert_eq!(config.ts.hot_retention_hours, 24);
        assert_eq!(config.ts.medium_retention_days, 7);
    }

    // ── Profile & service tests ─────────────────────────────────────

    #[test]
    fn edge_profile_defaults() {
        let svc = ServicesConfig::for_profile(ProfileType::Edge);
        assert!(!svc.vector.enabled);
        assert!(svc.search.enabled);
        assert!(svc.temporal.enabled);
        assert!(!svc.federation.enabled);
        assert!(!svc.mcp.enabled);
        assert!(!svc.replicas.enabled);
        assert!(svc.algorithms.enabled);
    }

    #[test]
    fn cloud_profile_defaults() {
        let svc = ServicesConfig::for_profile(ProfileType::Cloud);
        assert!(svc.vector.enabled);
        assert!(svc.search.enabled);
        assert!(svc.temporal.enabled);
        assert!(svc.federation.enabled);
        assert!(!svc.mcp.enabled);
        assert!(svc.replicas.enabled);
    }

    #[test]
    fn standalone_profile_defaults() {
        let svc = ServicesConfig::for_profile(ProfileType::Standalone);
        assert!(svc.vector.enabled);
        assert!(svc.mcp.enabled);
        assert!(!svc.federation.enabled);
        assert!(!svc.replicas.enabled);
    }

    #[test]
    fn memory_defaults_by_profile() {
        assert_eq!(MemoryConfig::for_profile(ProfileType::Edge).budget_mb, 2048);
        assert_eq!(
            MemoryConfig::for_profile(ProfileType::Cloud).budget_mb,
            16384
        );
        assert_eq!(
            MemoryConfig::for_profile(ProfileType::Standalone).budget_mb,
            4096
        );
    }

    #[test]
    fn memory_budget_bytes_calculation() {
        let mem = MemoryConfig {
            budget_mb: 1024,
            soft_limit_percent: 80,
        };
        assert_eq!(mem.budget_bytes(), 1024 * 1024 * 1024);
        assert_eq!(mem.soft_limit_bytes(), 1024 * 1024 * 1024 * 80 / 100);
    }

    #[test]
    fn metrics_tier_by_profile() {
        assert_eq!(
            MetricsTier::for_profile(ProfileType::Edge),
            MetricsTier::Basic
        );
        assert_eq!(
            MetricsTier::for_profile(ProfileType::Cloud),
            MetricsTier::Full
        );
        assert_eq!(
            MetricsTier::for_profile(ProfileType::Standalone),
            MetricsTier::Full
        );
    }

    #[test]
    fn profile_from_str() {
        assert_eq!("edge".parse::<ProfileType>().unwrap(), ProfileType::Edge);
        assert_eq!("Cloud".parse::<ProfileType>().unwrap(), ProfileType::Cloud);
        assert_eq!(
            "STANDALONE".parse::<ProfileType>().unwrap(),
            ProfileType::Standalone
        );
        assert!("unknown".parse::<ProfileType>().is_err());
    }

    #[test]
    fn load_with_profile_from_toml() {
        let toml = r#"
profile = "cloud"

[memory]
budget_mb = 32768
"#;
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("selene.toml");
        std::fs::write(&config_path, toml).unwrap();

        let config = SeleneConfig::load(Some(&config_path), None).unwrap();
        assert_eq!(config.profile, ProfileType::Cloud);
        // Cloud profile defaults for services
        assert!(config.services.vector.enabled);
        assert!(config.services.federation.enabled);
        // Memory overridden from TOML
        assert_eq!(config.memory.budget_mb, 32768);
        // Metrics follows profile default
        assert_eq!(config.metrics, MetricsTier::Full);
    }

    #[test]
    fn load_with_services_override() {
        let toml = r#"
profile = "edge"

[services]
vector = { enabled = true }
"#;
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("selene.toml");
        std::fs::write(&config_path, toml).unwrap();

        let config = SeleneConfig::load(Some(&config_path), None).unwrap();
        assert_eq!(config.profile, ProfileType::Edge);
        // Vector explicitly enabled despite edge default being off
        assert!(config.services.vector.enabled);
    }

    // ── Sync config tests ──────────────────────────────────────────

    #[test]
    fn sync_config_defaults() {
        let sync = SyncConfig::default();
        assert!(!sync.is_enabled());
        assert_eq!(sync.batch_size, 100);
        assert_eq!(sync.push_interval_ms, 100);
        assert_eq!(sync.reconnect_delay_secs, 5);
        assert!(sync.upstream.is_empty());
        assert!(sync.peer_name.is_empty());
        assert!(sync.auth_identity.is_none());
        assert!(sync.auth_credentials.is_none());
        assert!(sync.server_name.is_none());
        assert_eq!(sync.max_sync_entries, 1_000);
        assert_eq!(sync.max_changes_per_entry, 10_000);
        assert_eq!(sync.max_subscription_rules, 50);
        assert_eq!(sync.max_predicates_per_rule, 20);
        assert_eq!(sync.max_in_list_size, 1000);
    }

    #[test]
    fn sync_config_enabled_when_both_set() {
        let sync = SyncConfig {
            upstream: "hub.example.com:4510".into(),
            peer_name: "edge-01".into(),
            ..Default::default()
        };
        assert!(sync.is_enabled());
    }

    #[test]
    fn sync_config_disabled_without_peer_name() {
        let sync = SyncConfig {
            upstream: "hub.example.com:4510".into(),
            ..Default::default()
        };
        assert!(!sync.is_enabled());
    }

    #[test]
    fn default_config_uses_edge_profile() {
        let config = SeleneConfig::load(None, Some("/tmp/test")).unwrap();
        assert_eq!(config.profile, ProfileType::Edge);
        assert!(!config.services.vector.enabled);
        assert!(config.services.search.enabled);
        assert_eq!(config.memory.budget_mb, 2048);
        assert_eq!(config.metrics, MetricsTier::Basic);
    }
}
