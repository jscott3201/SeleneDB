//! Federation — mesh connectivity between Selene nodes.
//!
//! Any Selene node can be a federator. The deployment topology
//! (hub → building → sol) is configuration, not architecture.

pub(crate) mod bloom;
pub mod config;
pub(crate) mod error;
pub(crate) mod handler;
pub mod manager;
pub mod registry;

// ── Service wrapper ──────────────────────────────────────────────────

use std::sync::Arc;

/// Federation as a registered service in the ServiceRegistry.
/// Holds both the PeerRegistry and FederationManager.
pub struct FederationService {
    pub registry: Arc<registry::PeerRegistry>,
    pub manager: Arc<manager::FederationManager>,
}

impl FederationService {
    pub fn new(
        registry: Arc<registry::PeerRegistry>,
        manager: Arc<manager::FederationManager>,
    ) -> Self {
        Self { registry, manager }
    }
}

impl crate::service_registry::Service for FederationService {
    fn name(&self) -> &'static str {
        "federation"
    }

    fn health(&self) -> crate::service_registry::ServiceHealth {
        crate::service_registry::ServiceHealth::Healthy
    }
}
