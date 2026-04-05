//! Federation manager — connection lifecycle for peer mesh.

use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use selene_client::{ClientConfig, SeleneClient};
use selene_wire::dto::federation::FederationRegisterPayload;

use super::config::{FederationConfig, PeerRole};
use super::error::FederationError;
use super::registry::{PeerInfo, PeerRegistry};
use crate::config::NodeTlsConfig;

/// Manages federation connections to peers.
///
/// Responsible for:
/// - Connecting to bootstrap peers on startup
/// - Exchanging registration with peers
/// - Pulling peer directories
/// - Reconnecting on failure
/// - Periodic stale peer pruning
pub struct FederationManager {
    pub registry: Arc<PeerRegistry>,
    connections: RwLock<std::collections::HashMap<String, Arc<SeleneClient>>>,
    config: FederationConfig,
    node_tls: NodeTlsConfig,
    dev_mode: bool,
}

impl FederationManager {
    pub fn new(
        config: FederationConfig,
        registry: Arc<PeerRegistry>,
        node_tls: NodeTlsConfig,
        dev_mode: bool,
    ) -> Self {
        Self {
            registry,
            connections: RwLock::new(std::collections::HashMap::new()),
            config,
            node_tls,
            dev_mode,
        }
    }

    /// Get an existing connection to a peer, or return None.
    pub fn get_connection(&self, peer_name: &str) -> Option<Arc<SeleneClient>> {
        self.connections.read().get(peer_name).cloned()
    }

    /// Connect to a peer by address and register ourselves.
    pub async fn connect_and_register(
        &self,
        address: &str,
    ) -> Result<(String, Arc<SeleneClient>), FederationError> {
        let addr: std::net::SocketAddr =
            address
                .parse()
                .map_err(|e| FederationError::ConnectionFailed {
                    peer: address.into(),
                    reason: format!("invalid address: {e}"),
                })?;
        let tls = if let (Some(ca), Some(cert), Some(key)) = (
            &self.node_tls.ca_cert,
            &self.node_tls.cert,
            &self.node_tls.key,
        ) {
            Some(selene_client::config::ClientTlsConfig {
                ca_cert_path: ca.clone(),
                cert_path: Some(cert.clone()),
                key_path: Some(key.clone()),
            })
        } else {
            None
        };

        let server_name = address.split(':').next().unwrap_or("localhost").to_string();

        let client_config = ClientConfig {
            server_addr: addr,
            server_name,
            insecure: self.dev_mode && tls.is_none(),
            tls,
            auth: None,
        };

        let client = SeleneClient::connect(&client_config).await.map_err(|e| {
            FederationError::ConnectionFailed {
                peer: address.into(),
                reason: format!("{e}"),
            }
        })?;
        let client = Arc::new(client);

        let local = self.registry.local_info();
        // Build bloom filter from local schema labels
        let bloom_bytes = if local.schema_labels.is_empty() {
            None
        } else {
            let bf = super::bloom::build_filter(&local.schema_labels, &[]);
            Some(bf.to_bytes())
        };
        let reg = FederationRegisterPayload {
            node_name: local.name.clone(),
            address: local.address.clone(),
            schema_labels: local.schema_labels.clone(),
            role: local.role.as_str().into(),
            bloom_filter: bloom_bytes,
        };
        client
            .federation_register(reg)
            .await
            .map_err(|e| FederationError::RegistrationRejected(format!("{e}")))?;

        let peer_list = client
            .federation_peer_list()
            .await
            .map_err(|e| FederationError::Client(format!("peer list: {e}")))?;

        // Register the peer we connected to (if they replied with their info)
        // Also register any peers they told us about
        let mut connected_peer_name = String::new();
        for peer in &peer_list.peers {
            if peer.address == address {
                connected_peer_name = peer.node_name.clone();
            }
            // Don't register ourselves
            if peer.node_name != self.registry.local_info().name {
                self.registry.register(PeerInfo {
                    name: peer.node_name.clone(),
                    address: peer.address.clone(),
                    schema_labels: peer.schema_labels.clone(),
                    role: PeerRole::from_str(&peer.role),
                    last_seen: Instant::now(),
                    connected: peer.address == address,
                    bloom_filter: peer
                        .bloom_filter
                        .as_deref()
                        .and_then(super::bloom::BloomFilter::from_bytes),
                });
            }
        }

        // Store the connection
        if !connected_peer_name.is_empty() {
            self.connections
                .write()
                .insert(connected_peer_name.clone(), Arc::clone(&client));
        }

        Ok((connected_peer_name, client))
    }

    /// Get or establish a connection to a named peer.
    pub async fn get_or_connect(
        &self,
        peer_name: &str,
    ) -> Result<Arc<SeleneClient>, FederationError> {
        if let Some(client) = self.get_connection(peer_name) {
            return Ok(client);
        }

        let info = self
            .registry
            .get(peer_name)
            .ok_or_else(|| FederationError::PeerNotFound(peer_name.into()))?;

        let (_, client) = self.connect_and_register(&info.address).await?;
        Ok(client)
    }

    /// Connect to all bootstrap peers.
    pub async fn bootstrap(&self) {
        for addr in &self.config.bootstrap_peers {
            tracing::info!(address = %addr, "connecting to bootstrap peer");
            match self.connect_and_register(addr).await {
                Ok((name, _)) => {
                    tracing::info!(peer = %name, "bootstrap peer connected");
                }
                Err(e) => {
                    tracing::warn!(address = %addr, error = %e, "failed to connect to bootstrap peer");
                }
            }
        }
    }

    /// Prune stale peers and drop their connections.
    pub fn prune(&self) {
        self.registry.prune_stale();

        // Remove connections for peers no longer in registry
        let known_peers: std::collections::HashSet<String> = self
            .registry
            .all_peers()
            .iter()
            .map(|p| p.name.clone())
            .collect();

        self.connections
            .write()
            .retain(|name, _| known_peers.contains(name));
    }

    /// Number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections.read().len()
    }
}

impl std::fmt::Debug for FederationManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederationManager")
            .field("connections", &self.connection_count())
            .field("peers", &self.registry.peer_count())
            .finish()
    }
}
