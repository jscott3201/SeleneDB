use crate::WireError;

/// Wire message type codes per SWP spec section 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MsgType {
    // Time Series (0x10-0x1F)
    TsWrite = 0x10,
    TsRangeQuery = 0x11,
    // Changelog (0x20-0x2F)
    ChangelogSubscribe = 0x20,
    ChangelogEvent = 0x21,
    ChangelogAck = 0x22,
    ChangelogUnsubscribe = 0x23,
    // Graph & Query (0x30-0x3F)
    GraphSliceRequest = 0x35,
    GqlQuery = 0x36,
    GqlExplain = 0x37,
    // Service (0x50-0x5F)
    Health = 0x50,
    Handshake = 0x53,
    // Replication (0x5A-0x5F)
    SnapshotRequest = 0x5A,
    SnapshotChunk = 0x5B,
    // Sync (0x5C-0x5F)
    SyncPush = 0x5C,
    SyncPushAck = 0x5D,
    SyncSubscribe = 0x5E,
    SyncSubscribeResponse = 0x5F,
    // Federation (0x54-0x59)
    FederationRegister = 0x54,
    FederationPeerList = 0x55,
    FederationPeerListResponse = 0x56,
    FederationGqlRequest = 0x57,
    FederationGqlResponse = 0x59,
    // Response (0x70-0x7F)
    Ok = 0x70,
    Error = 0x71,
    TsPayload = 0x74,
    TsArrowPayload = 0x75,
    GraphSlicePayload = 0x7D,
    GqlResultPayload = 0x7E,
}

impl TryFrom<u8> for MsgType {
    type Error = WireError;

    fn try_from(v: u8) -> Result<Self, WireError> {
        match v {
            0x10 => Ok(MsgType::TsWrite),
            0x11 => Ok(MsgType::TsRangeQuery),
            0x20 => Ok(MsgType::ChangelogSubscribe),
            0x21 => Ok(MsgType::ChangelogEvent),
            0x22 => Ok(MsgType::ChangelogAck),
            0x23 => Ok(MsgType::ChangelogUnsubscribe),
            0x35 => Ok(MsgType::GraphSliceRequest),
            0x36 => Ok(MsgType::GqlQuery),
            0x37 => Ok(MsgType::GqlExplain),
            0x50 => Ok(MsgType::Health),
            0x53 => Ok(MsgType::Handshake),
            0x54 => Ok(MsgType::FederationRegister),
            0x55 => Ok(MsgType::FederationPeerList),
            0x56 => Ok(MsgType::FederationPeerListResponse),
            0x57 => Ok(MsgType::FederationGqlRequest),
            0x59 => Ok(MsgType::FederationGqlResponse),
            0x5A => Ok(MsgType::SnapshotRequest),
            0x5B => Ok(MsgType::SnapshotChunk),
            0x5C => Ok(MsgType::SyncPush),
            0x5D => Ok(MsgType::SyncPushAck),
            0x5E => Ok(MsgType::SyncSubscribe),
            0x5F => Ok(MsgType::SyncSubscribeResponse),
            0x70 => Ok(MsgType::Ok),
            0x71 => Ok(MsgType::Error),
            0x74 => Ok(MsgType::TsPayload),
            0x75 => Ok(MsgType::TsArrowPayload),
            0x7D => Ok(MsgType::GraphSlicePayload),
            0x7E => Ok(MsgType::GqlResultPayload),
            _ => Err(WireError::InvalidMsgType(v)),
        }
    }
}

impl From<MsgType> for u8 {
    fn from(msg_type: MsgType) -> u8 {
        msg_type as u8
    }
}

impl MsgType {
    /// Returns `true` for response-type messages (0x70-0x7E, 0x56, 0x59, 0x5B).
    pub fn is_response(&self) -> bool {
        matches!(
            self,
            MsgType::Ok
                | MsgType::Error
                | MsgType::TsPayload
                | MsgType::TsArrowPayload
                | MsgType::GraphSlicePayload
                | MsgType::GqlResultPayload
                | MsgType::FederationPeerListResponse
                | MsgType::FederationGqlResponse
                | MsgType::SnapshotChunk
                | MsgType::SyncPushAck
                | MsgType::SyncSubscribeResponse
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All variants that exist in the enum, paired with their u8 codes.
    const ALL_VARIANTS: &[(MsgType, u8)] = &[
        (MsgType::TsWrite, 0x10),
        (MsgType::TsRangeQuery, 0x11),
        (MsgType::ChangelogSubscribe, 0x20),
        (MsgType::ChangelogEvent, 0x21),
        (MsgType::ChangelogAck, 0x22),
        (MsgType::ChangelogUnsubscribe, 0x23),
        (MsgType::GraphSliceRequest, 0x35),
        (MsgType::GqlQuery, 0x36),
        (MsgType::GqlExplain, 0x37),
        (MsgType::Health, 0x50),
        (MsgType::Handshake, 0x53),
        (MsgType::FederationRegister, 0x54),
        (MsgType::FederationPeerList, 0x55),
        (MsgType::FederationPeerListResponse, 0x56),
        (MsgType::FederationGqlRequest, 0x57),
        (MsgType::FederationGqlResponse, 0x59),
        (MsgType::SnapshotRequest, 0x5A),
        (MsgType::SnapshotChunk, 0x5B),
        (MsgType::SyncPush, 0x5C),
        (MsgType::SyncPushAck, 0x5D),
        (MsgType::SyncSubscribe, 0x5E),
        (MsgType::SyncSubscribeResponse, 0x5F),
        (MsgType::Ok, 0x70),
        (MsgType::Error, 0x71),
        (MsgType::TsPayload, 0x74),
        (MsgType::TsArrowPayload, 0x75),
        (MsgType::GraphSlicePayload, 0x7D),
        (MsgType::GqlResultPayload, 0x7E),
    ];

    #[test]
    fn u8_round_trip_all_variants() {
        for &(variant, code) in ALL_VARIANTS {
            // MsgType -> u8
            assert_eq!(
                u8::from(variant),
                code,
                "u8 for {variant:?} should be {code:#04x}"
            );
            // u8 -> MsgType
            assert_eq!(
                MsgType::try_from(code).unwrap(),
                variant,
                "try_from({code:#04x}) should be {variant:?}"
            );
        }
    }

    #[test]
    fn invalid_msg_type_rejected() {
        // Removed CRUD codes + gaps
        for bad in [
            0x00u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0F, 0x12, 0x13, 0x24, 0x30, 0x33, 0x34, 0x3F, 0x40, 0x41, 0x48, 0x4F, 0x51, 0x52,
            0x58, 0x60, 0x6F, 0x72, 0x73, 0x76, 0x79, 0x7A, 0x7B, 0x7C, 0x7F, 0x80, 0x81, 0x82,
            0xFF,
        ] {
            match MsgType::try_from(bad) {
                Err(WireError::InvalidMsgType(v)) => assert_eq!(v, bad),
                other => panic!("{bad:#04x} should be InvalidMsgType, got {other:?}"),
            }
        }
    }

    #[test]
    fn is_response_correct() {
        let response_variants = [
            MsgType::Ok,
            MsgType::Error,
            MsgType::TsPayload,
            MsgType::TsArrowPayload,
            MsgType::GraphSlicePayload,
            MsgType::GqlResultPayload,
            MsgType::FederationPeerListResponse,
            MsgType::FederationGqlResponse,
            MsgType::SnapshotChunk,
            MsgType::SyncPushAck,
            MsgType::SyncSubscribeResponse,
        ];
        let request_variants = [
            MsgType::TsWrite,
            MsgType::TsRangeQuery,
            MsgType::ChangelogSubscribe,
            MsgType::GraphSliceRequest,
            MsgType::GqlQuery,
            MsgType::GqlExplain,
            MsgType::Health,
            MsgType::Handshake,
            MsgType::FederationGqlRequest,
            MsgType::SnapshotRequest,
            MsgType::SyncPush,
            MsgType::SyncSubscribe,
        ];
        for v in response_variants {
            assert!(v.is_response(), "{v:?} should be a response type");
        }
        for v in request_variants {
            assert!(!v.is_response(), "{v:?} should not be a response type");
        }
    }
}
