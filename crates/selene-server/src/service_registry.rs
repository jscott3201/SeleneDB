//! Service registry — typed service container for runtime-activated features.
//!
//! Instead of feature-gated `Option` fields on `ServerState`, optional subsystems
//! (vector store, search index, temporal versioning, federation, vault) implement
//! the [`Service`] trait and register at bootstrap via [`ServiceRegistry`].
//!
//! Ops code retrieves services with `state.services.get::<T>()`, which returns
//! `None` when a service is not enabled — a clean runtime check replacing
//! `#[cfg(feature = ...)]` guards.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

/// Health status reported by a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceHealth {
    Healthy,
    Degraded(String),
}

/// Trait implemented by all optional services.
pub trait Service: Send + Sync + 'static {
    /// Human-readable service name (e.g., "vector", "search", "temporal").
    fn name(&self) -> &'static str;

    /// Current health status.
    fn health(&self) -> ServiceHealth;
}

/// Type-map container for registered services.
///
/// Services are stored as `Arc<dyn Any + Send + Sync>` and retrieved by
/// concrete type via `TypeId`. Registration is at bootstrap; retrieval is
/// lock-free (plain `HashMap::get`).
pub struct ServiceRegistry {
    services: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    /// Ordered list of (name, type_id) for enumeration and metrics.
    entries: Vec<(String, TypeId)>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
            entries: Vec::new(),
        }
    }

    /// Register a service. Panics if the same type is registered twice —
    /// this is a bootstrap-time programming error, not a runtime condition.
    pub fn register<T: Service>(&mut self, service: T) {
        let tid = TypeId::of::<T>();
        let name = service.name().to_string();
        assert!(
            !self.services.contains_key(&tid),
            "service '{name}' registered twice"
        );
        self.entries.push((name, tid));
        self.services.insert(tid, Arc::new(service));
    }

    /// Retrieve a service by type. Returns `None` if not registered.
    pub fn get<T: Service>(&self) -> Option<&T> {
        self.services
            .get(&TypeId::of::<T>())
            .and_then(|arc| arc.downcast_ref::<T>())
    }

    /// Retrieve a service as `Arc<T>` for sharing across tasks.
    /// Returns `None` if not registered.
    pub fn get_arc<T: Service>(&self) -> Option<Arc<T>> {
        self.services
            .get(&TypeId::of::<T>())
            .and_then(|arc| Arc::clone(arc).downcast::<T>().ok())
    }

    /// List all registered service names in registration order.
    pub fn service_names(&self) -> Vec<&str> {
        self.entries.iter().map(|(name, _)| name.as_str()).collect()
    }

    /// Number of registered services.
    pub fn len(&self) -> usize {
        self.services.len()
    }

    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }
}

// ── Core service wrappers ────────────────────────────────────────────

/// GraphCatalog as a registered service — always present (core service).
/// Manages named graphs (CREATE GRAPH / DROP GRAPH).
pub struct GraphCatalogService {
    pub catalog: Arc<parking_lot::Mutex<selene_graph::GraphCatalog>>,
}

impl GraphCatalogService {
    pub fn new(catalog: Arc<parking_lot::Mutex<selene_graph::GraphCatalog>>) -> Self {
        Self { catalog }
    }
}

impl Service for GraphCatalogService {
    fn name(&self) -> &'static str {
        "graph_catalog"
    }

    fn health(&self) -> ServiceHealth {
        ServiceHealth::Healthy
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyService {
        svc_name: &'static str,
    }

    impl Service for DummyService {
        fn name(&self) -> &'static str {
            self.svc_name
        }
        fn health(&self) -> ServiceHealth {
            ServiceHealth::Healthy
        }
    }

    struct AnotherService;

    impl Service for AnotherService {
        fn name(&self) -> &'static str {
            "another"
        }
        fn health(&self) -> ServiceHealth {
            ServiceHealth::Degraded("test degradation".into())
        }
    }

    #[test]
    fn register_and_retrieve() {
        let mut registry = ServiceRegistry::new();
        registry.register(DummyService { svc_name: "test" });
        let svc = registry.get::<DummyService>();
        assert!(svc.is_some());
        assert_eq!(svc.unwrap().name(), "test");
    }

    #[test]
    fn get_missing_returns_none() {
        let registry = ServiceRegistry::new();
        assert!(registry.get::<DummyService>().is_none());
    }

    #[test]
    fn get_arc_returns_shared_reference() {
        let mut registry = ServiceRegistry::new();
        registry.register(DummyService { svc_name: "shared" });
        let arc = registry.get_arc::<DummyService>();
        assert!(arc.is_some());
        assert_eq!(arc.unwrap().name(), "shared");
    }

    #[test]
    fn list_services_returns_registered_in_order() {
        let mut registry = ServiceRegistry::new();
        registry.register(DummyService { svc_name: "alpha" });
        registry.register(AnotherService);
        let names = registry.service_names();
        assert_eq!(names, vec!["alpha", "another"]);
    }

    #[test]
    fn len_and_is_empty() {
        let mut registry = ServiceRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        registry.register(DummyService { svc_name: "test" });
        assert!(!registry.is_empty());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    #[should_panic(expected = "registered twice")]
    fn double_register_panics() {
        let mut registry = ServiceRegistry::new();
        registry.register(DummyService { svc_name: "first" });
        registry.register(DummyService { svc_name: "second" });
    }

    #[test]
    fn different_types_coexist() {
        let mut registry = ServiceRegistry::new();
        registry.register(DummyService { svc_name: "dummy" });
        registry.register(AnotherService);
        assert!(registry.get::<DummyService>().is_some());
        assert!(registry.get::<AnotherService>().is_some());
        assert_eq!(registry.len(), 2);
    }
}
