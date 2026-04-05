//! Changelog DTOs for wire transfer (delta sync to hub).

use serde::{Deserialize, Serialize};

use selene_core::changeset::Change;

/// Request: subscribe to changelog events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangelogSubscribeRequest {
    /// Start receiving events after this sequence number.
    /// 0 = start from current position.
    pub since_sequence: u64,
    /// Sync peer name for subscription filter lookup.
    ///
    /// Set by sync edge nodes so the hub can locate the correct per-peer
    /// `SubscriptionFilter` that was registered during the sync subscribe
    /// handshake. `None` for CDC replica subscribers and other non-sync
    /// clients.
    #[serde(default)]
    pub peer_name: Option<String>,
}

/// Event: a changelog entry pushed to subscribers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangelogEventDto {
    pub sequence: u64,
    pub changes: Vec<Change>,
    /// If true, subscriber has fallen behind and must re-sync
    /// with a full graph slice.
    #[serde(default)]
    pub sync_lost: bool,
    /// HLC timestamp for causal ordering. 0 for pre-HLC entries.
    #[serde(default)]
    pub hlc_timestamp: u64,
}

/// Request: acknowledge receipt of events up to a sequence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangelogAckRequest {
    pub acked_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};
    use selene_core::NodeId;

    #[test]
    fn changelog_event_round_trip() {
        let event = ChangelogEventDto {
            sequence: 42,
            changes: vec![
                Change::NodeCreated { node_id: NodeId(1) },
                Change::PropertySet {
                    node_id: NodeId(1),
                    key: selene_core::IStr::new("temp"),
                    value: selene_core::Value::Float(72.5),
                    old_value: None,
                },
            ],
            sync_lost: false,
            hlc_timestamp: 1000,
        };

        let bytes = serialize_payload(&event, WireFlags::empty()).unwrap();
        let decoded: ChangelogEventDto = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.sequence, 42);
        assert_eq!(decoded.changes.len(), 2);
        assert!(!decoded.sync_lost);
        assert_eq!(decoded.hlc_timestamp, 1000);
    }

    #[test]
    fn sync_lost_event_round_trip() {
        let event = ChangelogEventDto {
            sequence: 0,
            changes: vec![],
            sync_lost: true,
            hlc_timestamp: 0,
        };

        let bytes = serialize_payload(&event, WireFlags::empty()).unwrap();
        let decoded: ChangelogEventDto = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert!(decoded.sync_lost);
    }

    #[test]
    fn subscribe_request_round_trip() {
        let req = ChangelogSubscribeRequest {
            since_sequence: 100,
            peer_name: Some("edge-1".to_string()),
        };
        let bytes = serialize_payload(&req, WireFlags::empty()).unwrap();
        let decoded: ChangelogSubscribeRequest =
            deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.since_sequence, 100);
        assert_eq!(decoded.peer_name.as_deref(), Some("edge-1"));
    }

    #[test]
    fn subscribe_request_no_peer_name() {
        let req = ChangelogSubscribeRequest {
            since_sequence: 0,
            peer_name: None,
        };
        let bytes = serialize_payload(&req, WireFlags::empty()).unwrap();
        let decoded: ChangelogSubscribeRequest =
            deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.since_sequence, 0);
        assert!(decoded.peer_name.is_none());
    }
}
