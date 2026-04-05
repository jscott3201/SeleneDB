//! Graph name resolution — maps `USE <graph>` names to execution targets.

use selene_graph::SharedGraph;

/// Where a graph name resolved to.
pub enum ResolvedGraph {
    /// The default graph (state.graph) — no USE prefix.
    Default,
    /// A local named graph from GraphCatalog.
    Local(SharedGraph),
    /// The secure vault graph.
    Vault,
    /// A remote peer — forward the query via federation (feature-gated).
    #[cfg(feature = "federation")]
    Remote { peer_name: String },
}

/// Resolves graph names to execution targets.
///
/// Resolution order:
/// 1. `"secure"` → Vault (if available)
/// 2. GraphCatalog for local named graphs
/// 3. PeerRegistry for remote peers (federation feature)
/// 4. Error: unknown graph
pub struct GraphResolver<'a> {
    catalog: &'a parking_lot::Mutex<selene_graph::GraphCatalog>,
    vault_available: bool,
    #[cfg(feature = "federation")]
    registry: Option<&'a crate::federation::registry::PeerRegistry>,
}

impl<'a> GraphResolver<'a> {
    pub fn new(
        catalog: &'a parking_lot::Mutex<selene_graph::GraphCatalog>,
        vault_available: bool,
        #[cfg(feature = "federation")] registry: Option<
            &'a crate::federation::registry::PeerRegistry,
        >,
    ) -> Self {
        Self {
            catalog,
            vault_available,
            #[cfg(feature = "federation")]
            registry,
        }
    }

    /// Resolve a graph name to an execution target.
    pub fn resolve(&self, name: &str) -> Result<ResolvedGraph, String> {
        // 1. Vault
        if name.eq_ignore_ascii_case("secure") {
            return if self.vault_available {
                Ok(ResolvedGraph::Vault)
            } else {
                Err("secure vault not available (not configured or key missing)".into())
            };
        }

        // 2. Local named graphs
        if let Some(graph) = self.catalog.lock().get_graph(name) {
            return Ok(ResolvedGraph::Local(graph.clone()));
        }

        // 3. Remote peers (federation)
        #[cfg(feature = "federation")]
        if let Some(registry) = self.registry
            && registry.get(name).is_some()
        {
            return Ok(ResolvedGraph::Remote {
                peer_name: name.to_string(),
            });
        }

        Err(format!("unknown graph: {name}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_vault() {
        let catalog = parking_lot::Mutex::new(selene_graph::GraphCatalog::new());
        let resolver = GraphResolver::new(
            &catalog,
            true,
            #[cfg(feature = "federation")]
            None,
        );
        assert!(matches!(
            resolver.resolve("secure"),
            Ok(ResolvedGraph::Vault)
        ));
        assert!(matches!(
            resolver.resolve("SECURE"),
            Ok(ResolvedGraph::Vault)
        ));
    }

    #[test]
    fn resolve_vault_unavailable() {
        let catalog = parking_lot::Mutex::new(selene_graph::GraphCatalog::new());
        let resolver = GraphResolver::new(
            &catalog,
            false,
            #[cfg(feature = "federation")]
            None,
        );
        assert!(resolver.resolve("secure").is_err());
    }

    #[test]
    fn resolve_local_graph() {
        let catalog = parking_lot::Mutex::new(selene_graph::GraphCatalog::new());
        let _ = catalog.lock().create_graph("audit_log");
        let resolver = GraphResolver::new(
            &catalog,
            false,
            #[cfg(feature = "federation")]
            None,
        );
        assert!(matches!(
            resolver.resolve("audit_log"),
            Ok(ResolvedGraph::Local(_))
        ));
    }

    #[test]
    fn resolve_unknown() {
        let catalog = parking_lot::Mutex::new(selene_graph::GraphCatalog::new());
        let resolver = GraphResolver::new(
            &catalog,
            false,
            #[cfg(feature = "federation")]
            None,
        );
        assert!(resolver.resolve("nonexistent").is_err());
    }
}
