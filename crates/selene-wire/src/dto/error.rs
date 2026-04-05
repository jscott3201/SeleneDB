//! Error DTOs for wire transfer.

use serde::{Deserialize, Serialize};

/// Error response sent over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Error code (maps to stream-level error codes in SWP spec).
    pub code: u16,
    /// Human-readable error message.
    pub message: String,
    /// Optional suggestion for AI agent self-correction.
    pub suggestion: Option<String>,
}

/// Stream-level error codes (non-fatal, per SWP spec).
pub mod codes {
    pub const AUTHENTICATION_FAILED: u16 = 0x01;
    pub const AUTHORIZATION_DENIED: u16 = 0x02;
    pub const UNAUTHORIZED: u16 = 0x10;
    pub const NOT_FOUND: u16 = 0x11;
    pub const INVALID_REQUEST: u16 = 0x12;
    pub const CONFLICT: u16 = 0x13;
    pub const TIMEOUT: u16 = 0x14;
    pub const INTERNAL_ERROR: u16 = 0x15;
    pub const SCHEMA_VIOLATION: u16 = 0x16;
    pub const READ_ONLY: u16 = 0x17;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireFlags;
    use crate::serialize::{deserialize_payload, serialize_payload};

    #[test]
    fn error_response_round_trip() {
        let resp = ErrorResponse {
            code: codes::NOT_FOUND,
            message: "node 42 not found".into(),
            suggestion: Some("Check if the node ID exists with EntityList".into()),
        };

        let bytes = serialize_payload(&resp, WireFlags::empty()).unwrap();
        let decoded: ErrorResponse = deserialize_payload(&bytes, WireFlags::empty()).unwrap();
        assert_eq!(decoded.code, codes::NOT_FOUND);
        assert!(decoded.suggestion.is_some());
    }
}
