//! Sync push DTOs for offline-first edge-to-hub synchronization.

use serde::{Deserialize, Serialize};

use selene_core::Value;
use selene_core::changeset::Change;

/// A single sync entry containing a sequence number, HLC timestamp,
/// and the associated graph changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEntry {
    pub sequence: u64,
    pub hlc_timestamp: u64,
    pub changes: Vec<Change>,
}

/// Request sent by an edge node to push buffered changes to a hub.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPushRequest {
    pub peer_name: String,
    pub entries: Vec<SyncEntry>,
}

/// Describes a single property-level conflict detected by the hub
/// during sync merge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConflict {
    pub entity_id: u64,
    pub property: String,
    pub edge_hlc: u64,
    pub hub_hlc: u64,
}

/// Response from the hub acknowledging receipt of sync entries,
/// reporting the highest acknowledged sequence and any conflicts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPushAckResponse {
    pub acked_sequence: u64,
    pub conflicts: Vec<SyncConflict>,
}

/// Direction for subscription filtering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncDirectionConfig {
    PushOnly,
    PullOnly,
    Bidirectional,
}

/// A property-level filter predicate (wire format).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyPredicateConfig {
    Eq { key: String, value: Value },
    In { key: String, values: Vec<Value> },
    Gt { key: String, value: Value },
    Lt { key: String, value: Value },
    Gte { key: String, value: Value },
    Lte { key: String, value: Value },
    IsNotNull { key: String },
}

/// A single subscription rule (wire format).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionRuleConfig {
    pub labels: Vec<String>,
    pub predicates: Vec<PropertyPredicateConfig>,
}

/// Complete subscription definition (wire format, String-based).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    pub name: String,
    pub rules: Vec<SubscriptionRuleConfig>,
    pub direction: SyncDirectionConfig,
}

/// Edge -> Hub: subscribe with a filter definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSubscribeRequest {
    pub peer_name: String,
    pub subscription: SubscriptionConfig,
    pub last_pulled_seq: u64,
}

/// Hub -> Edge: filtered starting state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSubscribeResponse {
    pub snapshot: Option<Vec<Change>>,
    pub scope_bitmap: Vec<u8>,
    pub changelog_seq: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};
    use selene_core::{IStr, NodeId, Value};

    #[test]
    fn sync_push_request_serde_round_trip() {
        let request = SyncPushRequest {
            peer_name: "edge-01".to_string(),
            entries: vec![
                SyncEntry {
                    sequence: 1,
                    hlc_timestamp: 5000,
                    changes: vec![Change::NodeCreated {
                        node_id: NodeId(10),
                    }],
                },
                SyncEntry {
                    sequence: 2,
                    hlc_timestamp: 6000,
                    changes: vec![Change::PropertySet {
                        node_id: NodeId(10),
                        key: IStr::new("temp"),
                        value: Value::Float(21.5),
                        old_value: None,
                    }],
                },
            ],
        };

        let bytes = serialize_payload(&request, WireFlags::empty()).unwrap();
        let decoded: SyncPushRequest = deserialize_payload(&bytes, WireFlags::empty()).unwrap();

        assert_eq!(decoded.peer_name, "edge-01");
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].sequence, 1);
        assert_eq!(decoded.entries[0].hlc_timestamp, 5000);
        assert_eq!(decoded.entries[0].changes.len(), 1);
        assert_eq!(decoded.entries[1].sequence, 2);
        assert_eq!(decoded.entries[1].hlc_timestamp, 6000);
        assert_eq!(decoded.entries[1].changes.len(), 1);
    }

    #[test]
    fn sync_push_ack_serde_round_trip() {
        let response = SyncPushAckResponse {
            acked_sequence: 42,
            conflicts: vec![SyncConflict {
                entity_id: 10,
                property: "temp".to_string(),
                edge_hlc: 5000,
                hub_hlc: 5500,
            }],
        };

        let bytes = serialize_payload(&response, WireFlags::empty()).unwrap();
        let decoded: SyncPushAckResponse = deserialize_payload(&bytes, WireFlags::empty()).unwrap();

        assert_eq!(decoded.acked_sequence, 42);
        assert_eq!(decoded.conflicts.len(), 1);
        assert_eq!(decoded.conflicts[0].entity_id, 10);
        assert_eq!(decoded.conflicts[0].property, "temp");
        assert_eq!(decoded.conflicts[0].edge_hlc, 5000);
        assert_eq!(decoded.conflicts[0].hub_hlc, 5500);
    }

    #[test]
    fn sync_subscribe_request_serde_round_trip() {
        let request = SyncSubscribeRequest {
            peer_name: "edge-hq".to_string(),
            subscription: SubscriptionConfig {
                name: "building-hq".to_string(),
                rules: vec![
                    SubscriptionRuleConfig {
                        labels: vec!["Sensor".to_string()],
                        predicates: vec![PropertyPredicateConfig::Eq {
                            key: "building".to_string(),
                            value: Value::String("HQ".into()),
                        }],
                    },
                    SubscriptionRuleConfig {
                        labels: vec!["Equipment".to_string()],
                        predicates: vec![],
                    },
                ],
                direction: SyncDirectionConfig::Bidirectional,
            },
            last_pulled_seq: 0,
        };

        let bytes = serialize_payload(&request, WireFlags::empty()).unwrap();
        let decoded: SyncSubscribeRequest =
            deserialize_payload(&bytes, WireFlags::empty()).unwrap();

        assert_eq!(decoded.peer_name, "edge-hq");
        assert_eq!(decoded.subscription.name, "building-hq");
        assert_eq!(decoded.subscription.rules.len(), 2);
        assert_eq!(decoded.subscription.rules[0].labels, vec!["Sensor"]);
        assert_eq!(decoded.last_pulled_seq, 0);
    }

    #[test]
    fn sync_subscribe_response_serde_round_trip() {
        let response = SyncSubscribeResponse {
            snapshot: Some(vec![Change::NodeCreated {
                node_id: NodeId(42),
            }]),
            scope_bitmap: vec![1, 2, 3, 4],
            changelog_seq: 100,
        };

        let bytes = serialize_payload(&response, WireFlags::empty()).unwrap();
        let decoded: SyncSubscribeResponse =
            deserialize_payload(&bytes, WireFlags::empty()).unwrap();

        assert!(decoded.snapshot.is_some());
        assert_eq!(decoded.snapshot.unwrap().len(), 1);
        assert_eq!(decoded.scope_bitmap, vec![1, 2, 3, 4]);
        assert_eq!(decoded.changelog_seq, 100);
    }
}
