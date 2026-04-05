//! Time-series operations.

use selene_core::NodeId;
use selene_wire::dto::ts::TsSampleDto;

use super::{OpError, require_in_scope};
use crate::auth::handshake::AuthContext;
use crate::bootstrap::ServerState;

pub fn ts_write(
    state: &ServerState,
    auth: &AuthContext,
    samples: Vec<TsSampleDto>,
) -> Result<usize, OpError> {
    if state.replica.is_replica {
        return Err(OpError::ReadOnly);
    }
    let auth = super::refresh_scope_if_stale(state, auth);

    // Deduplicate entity IDs and validate with a single read lock
    let unique_entities: std::collections::HashSet<u64> =
        samples.iter().map(|s| s.entity_id).collect();

    state.graph.read(|g| {
        for &eid in &unique_entities {
            let nid = NodeId(eid);
            require_in_scope(&auth, nid)?;
            let node = g.get_node(nid).ok_or(OpError::NotFound {
                entity: "node",
                id: eid,
            })?;
            // Propagate schema encoding hints to the hot tier for this node.
            // set_encoding_hint is idempotent, so repeated calls are a no-op.
            for label in node.labels.iter() {
                if let Some(schema) = g.schema().node_schema(label.as_str()) {
                    for prop in &schema.properties {
                        if prop.encoding != selene_core::ValueEncoding::Gorilla {
                            state
                                .hot_tier
                                .set_encoding_hint(nid, &prop.name, prop.encoding);
                        }
                    }
                }
            }
        }
        Ok(())
    })?;

    let count = samples.len();

    // Batch append for single lock acquisition
    let batch: Vec<(NodeId, &str, selene_ts::TimeSample)> = samples
        .iter()
        .map(|s| {
            (
                NodeId(s.entity_id),
                s.property.as_str(),
                selene_ts::TimeSample {
                    timestamp_nanos: s.timestamp_nanos,
                    value: s.value,
                },
            )
        })
        .collect();
    state.hot_tier.append_batch(&batch);

    Ok(count)
}

pub fn ts_range(
    state: &ServerState,
    auth: &AuthContext,
    entity_id: u64,
    property: &str,
    start_nanos: i64,
    end_nanos: i64,
    limit: Option<usize>,
) -> Result<Vec<TsSampleDto>, OpError> {
    let auth = super::refresh_scope_if_stale(state, auth);
    require_in_scope(&auth, NodeId(entity_id))?;

    let samples = state
        .hot_tier
        .range(NodeId(entity_id), property, start_nanos, end_nanos);

    let iter = samples.into_iter().map(|s| TsSampleDto {
        entity_id,
        property: property.to_string(),
        timestamp_nanos: s.timestamp_nanos,
        value: s.value,
    });

    match limit {
        Some(n) => Ok(iter.take(n).collect()),
        None => Ok(iter.collect()),
    }
}
