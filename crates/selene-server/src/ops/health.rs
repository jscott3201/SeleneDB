//! Health operation.

use selene_wire::dto::service::HealthResponse;

use super::START_TIME;
use crate::bootstrap::ServerState;

/// Return server health status.
pub fn health(state: &ServerState) -> HealthResponse {
    let uptime = START_TIME.get().map_or(0, |t| t.elapsed().as_secs());
    let (node_count, edge_count) = state
        .graph
        .read(|g| (g.node_count() as u64, g.edge_count() as u64));

    HealthResponse {
        status: "ok".into(),
        node_count,
        edge_count,
        uptime_secs: uptime,
        dev_mode: state.config.dev_mode,
        role: if state.replica.is_replica {
            "replica".into()
        } else {
            "primary".into()
        },
        primary: state.replica.primary_addr.clone(),
        lag_sequences: state
            .replica
            .lag
            .as_ref()
            .map(|lag| lag.load(std::sync::atomic::Ordering::Relaxed)),
    }
}
