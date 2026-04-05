//! Time-series DTOs for wire transfer.

use serde::{Deserialize, Serialize};

/// Request: write time-series samples.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsWriteRequest {
    pub samples: Vec<TsSampleDto>,
}

/// A single time-series sample for wire transfer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TsSampleDto {
    pub entity_id: u64,
    pub property: String,
    pub timestamp_nanos: i64,
    pub value: f64,
}

/// Request: query time-series range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsRangeRequest {
    pub entity_id: u64,
    pub property: String,
    pub start_nanos: i64,
    pub end_nanos: i64,
    pub limit: Option<u64>,
}

/// Response: time-series query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsPayload {
    pub samples: Vec<TsSampleDto>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};

    #[test]
    fn ts_write_request_round_trip() {
        let req = TsWriteRequest {
            samples: vec![
                TsSampleDto {
                    entity_id: 1,
                    property: "temp".into(),
                    timestamp_nanos: 1_000_000_000,
                    value: 72.5,
                },
                TsSampleDto {
                    entity_id: 2,
                    property: "humidity".into(),
                    timestamp_nanos: 1_000_000_000,
                    value: 45.0,
                },
            ],
        };

        let bytes = serialize_payload(&req, WireFlags::empty()).unwrap();
        let decoded: TsWriteRequest = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.samples.len(), 2);
        assert_eq!(decoded.samples[0].value, 72.5);
    }
}
