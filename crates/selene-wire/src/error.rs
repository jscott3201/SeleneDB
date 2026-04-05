use thiserror::Error;

/// Errors from the Selene Wire Protocol layer.
#[derive(Debug, Error)]
pub enum WireError {
    #[error("invalid message type: {0:#04x}")]
    InvalidMsgType(u8),

    #[error("payload too large: {0} bytes (max 16MB)")]
    PayloadTooLarge(usize),

    #[error("incomplete frame header")]
    IncompleteHeader,

    #[error("deserialization error: {0}")]
    DeserializationError(String),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("compression error: {0}")]
    CompressionError(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        assert_eq!(
            WireError::InvalidMsgType(0xFF).to_string(),
            "invalid message type: 0xff"
        );
        assert_eq!(
            WireError::PayloadTooLarge(20_000_000).to_string(),
            "payload too large: 20000000 bytes (max 16MB)"
        );
        assert_eq!(
            WireError::IncompleteHeader.to_string(),
            "incomplete frame header"
        );
        assert_eq!(
            WireError::DeserializationError("bad data".into()).to_string(),
            "deserialization error: bad data"
        );
        assert_eq!(
            WireError::SerializationError("encode fail".into()).to_string(),
            "serialization error: encode fail"
        );
        assert_eq!(
            WireError::CompressionError("zstd fail".into()).to_string(),
            "compression error: zstd fail"
        );
    }
}
