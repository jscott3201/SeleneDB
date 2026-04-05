//! Peer registry — cached directory of known federation peers.

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::RwLock;

use super::bloom::BloomFilter;
use super::config::PeerRole;

/// Information about a known federation peer.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Human-readable node name.
    pub name: String,
    /// QUIC address for connecting to this peer.
    pub address: String,
    /// Schema labels this peer can serve.
    pub schema_labels: Vec<String>,
    /// Role hint.
    pub role: PeerRole,
    /// When this peer was last seen (registration or heartbeat).
    pub last_seen: Instant,
    /// Whether we have an active connection to this peer.
    pub connected: bool,
    /// Bloom filter for label+property membership checks.
    pub bloom_filter: Option<BloomFilter>,
}

/// Cached registry of known federation peers.
///
/// Thread-safe — uses `RwLock` for concurrent read access.
pub struct PeerRegistry {
    peers: RwLock<HashMap<String, PeerInfo>>,
    /// This node's own info (for responding to peer list requests).
    local_info: PeerInfo,
    /// TTL for peer entries in seconds.
    ttl_secs: u64,
}

impl PeerRegistry {
    /// Create a new registry with the local node's info.
    pub fn new(
        name: String,
        address: String,
        schema_labels: Vec<String>,
        role: PeerRole,
        ttl_secs: u64,
    ) -> Self {
        Self {
            peers: RwLock::new(HashMap::new()),
            local_info: PeerInfo {
                name,
                address,
                schema_labels,
                role,
                last_seen: Instant::now(),
                connected: true,
                bloom_filter: None,
            },
            ttl_secs,
        }
    }

    /// Register or update a peer.
    pub fn register(&self, info: PeerInfo) {
        let name = info.name.clone();
        tracing::info!(peer = %name, address = %info.address, labels = ?info.schema_labels, "peer registered");
        self.peers.write().insert(name, info);
    }

    /// Remove a peer by name.
    pub fn remove(&self, name: &str) {
        self.peers.write().remove(name);
    }

    /// Get a peer's info by name.
    pub fn get(&self, name: &str) -> Option<PeerInfo> {
        self.peers.read().get(name).cloned()
    }

    /// Get all known peers (excluding self).
    pub fn all_peers(&self) -> Vec<PeerInfo> {
        self.peers.read().values().cloned().collect()
    }

    /// Get all known peers plus self (for peer list responses).
    pub fn all_peers_including_self(&self) -> Vec<PeerInfo> {
        let mut peers = self.all_peers();
        peers.push(self.local_info.clone());
        peers
    }

    /// Get the local node's info.
    pub fn local_info(&self) -> &PeerInfo {
        &self.local_info
    }

    /// Get all peer names that serve a given schema label.
    pub fn peers_for_label(&self, label: &str) -> Vec<String> {
        self.peers
            .read()
            .iter()
            .filter(|(_, info)| info.schema_labels.iter().any(|l| l == label))
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Get peer names whose Bloom filter might contain the given label.
    /// Peers without a Bloom filter are always included (conservative).
    pub fn peers_matching_label(&self, label: &str) -> Vec<String> {
        let key = format!("label:{label}");
        self.peers
            .read()
            .iter()
            .filter(|(_, info)| {
                info.bloom_filter
                    .as_ref()
                    .is_none_or(|bf| bf.might_contain(&key))
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Mark a peer as connected/disconnected.
    pub fn set_connected(&self, name: &str, connected: bool) {
        if let Some(peer) = self.peers.write().get_mut(name) {
            peer.connected = connected;
            if connected {
                peer.last_seen = Instant::now();
            }
        }
    }

    /// Prune stale peers that haven't been seen within the TTL.
    pub fn prune_stale(&self) {
        let cutoff = Instant::now()
            .checked_sub(std::time::Duration::from_secs(self.ttl_secs))
            .unwrap_or(Instant::now());
        self.peers.write().retain(|name, info| {
            if info.last_seen < cutoff {
                tracing::info!(peer = %name, "pruning stale peer");
                false
            } else {
                true
            }
        });
    }

    /// Number of known peers (excluding self).
    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }
}

impl std::fmt::Debug for PeerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerRegistry")
            .field("local", &self.local_info.name)
            .field("peer_count", &self.peer_count())
            .finish()
    }
}
