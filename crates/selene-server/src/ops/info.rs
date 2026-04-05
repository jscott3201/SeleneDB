//! Server metadata -- version, profile, feature flags.

use crate::bootstrap::ServerState;

/// Build server info JSON, shared by HTTP and MCP transports.
pub fn server_info(state: &ServerState) -> serde_json::Value {
    serde_json::json!({
        "name": "selene",
        "version": env!("CARGO_PKG_VERSION"),
        "profile": state.config.profile.to_string(),
        "dev_mode": state.config.dev_mode,
        "features": {
            "federation": cfg!(feature = "federation"),
            "vector": cfg!(feature = "vector"),
            "search": cfg!(feature = "search"),
            "temporal": true,
            "rdf": cfg!(feature = "rdf"),
            "rdf_sparql": cfg!(feature = "rdf-sparql"),
            "cloud_storage": cfg!(feature = "cloud-storage"),
        }
    })
}
