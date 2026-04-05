//! Federation configuration.

/// Federation configuration — peer discovery and mesh connectivity.
#[derive(Debug, Clone)]
pub struct FederationConfig {
    /// Enable federation.
    pub enabled: bool,
    /// This node's name in the federation mesh (e.g., "building_a").
    pub node_name: String,
    /// This node's role hint for topology awareness.
    pub role: PeerRole,
    /// Bootstrap peers to connect to on startup.
    pub bootstrap_peers: Vec<String>,
    /// Peer registry entry TTL in seconds (default: 300).
    pub peer_ttl_secs: u64,
    /// How often to re-pull peer directories in seconds (default: 60).
    pub refresh_interval_secs: u64,
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_name: "selene".into(),
            role: PeerRole::Building,
            bootstrap_peers: vec![],
            peer_ttl_secs: 300,
            refresh_interval_secs: 60,
        }
    }
}

/// Role hint for topology awareness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRole {
    Aggregator,
    Building,
    Device,
}

impl PeerRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            PeerRole::Aggregator => "aggregator",
            PeerRole::Building => "building",
            PeerRole::Device => "device",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s {
            "aggregator" => PeerRole::Aggregator,
            "device" => PeerRole::Device,
            _ => PeerRole::Building,
        }
    }
}

impl std::fmt::Display for PeerRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
