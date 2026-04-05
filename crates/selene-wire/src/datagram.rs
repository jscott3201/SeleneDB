//! QUIC datagram codec for fire-and-forget telemetry samples.
//!
//! Format: `entity_id(8) | property_len(1) | property(N) | timestamp_nanos(8) | value_f64(8)`
//! Minimum size: 25 bytes (empty property name). Typical: ~30 bytes.

use crate::error::WireError;

/// A single telemetry sample encoded for datagram transport.
#[derive(Debug, Clone, PartialEq)]
pub struct TelemetryDatagram {
    pub entity_id: u64,
    pub property: String,
    pub timestamp_nanos: i64,
    pub value: f64,
}

impl TelemetryDatagram {
    /// Encode to bytes for QUIC datagram.
    pub fn encode(&self) -> Vec<u8> {
        let prop_bytes = self.property.as_bytes();
        let len = 8 + 1 + prop_bytes.len() + 8 + 8;
        let mut buf = Vec::with_capacity(len);
        buf.extend_from_slice(&self.entity_id.to_le_bytes());
        buf.push(prop_bytes.len() as u8);
        buf.extend_from_slice(prop_bytes);
        buf.extend_from_slice(&self.timestamp_nanos.to_le_bytes());
        buf.extend_from_slice(&self.value.to_le_bytes());
        buf
    }

    /// Decode from bytes received as a QUIC datagram.
    pub fn decode(data: &[u8]) -> Result<Self, WireError> {
        if data.len() < 25 {
            return Err(WireError::IncompleteHeader);
        }

        let entity_id = u64::from_le_bytes(
            data[0..8]
                .try_into()
                .map_err(|_| WireError::IncompleteHeader)?,
        );
        let prop_len = data[8] as usize;

        let expected = 8 + 1 + prop_len + 8 + 8;
        if data.len() < expected {
            return Err(WireError::IncompleteHeader);
        }

        let property = std::str::from_utf8(&data[9..9 + prop_len])
            .map_err(|e| WireError::DeserializationError(format!("invalid property name: {e}")))?
            .to_string();

        let ts_offset = 9 + prop_len;
        let timestamp_nanos = i64::from_le_bytes(
            data[ts_offset..ts_offset + 8]
                .try_into()
                .map_err(|_| WireError::IncompleteHeader)?,
        );
        let value = f64::from_le_bytes(
            data[ts_offset + 8..ts_offset + 16]
                .try_into()
                .map_err(|_| WireError::IncompleteHeader)?,
        );

        Ok(Self {
            entity_id,
            property,
            timestamp_nanos,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let dg = TelemetryDatagram {
            entity_id: 42,
            property: "temp".into(),
            timestamp_nanos: 1_000_000_000,
            value: 72.5,
        };
        let bytes = dg.encode();
        assert_eq!(bytes.len(), 8 + 1 + 4 + 8 + 8); // 29 bytes
        let decoded = TelemetryDatagram::decode(&bytes).unwrap();
        assert_eq!(decoded, dg);
    }

    #[test]
    fn encode_decode_empty_property() {
        let dg = TelemetryDatagram {
            entity_id: 1,
            property: String::new(),
            timestamp_nanos: 0,
            value: 0.0,
        };
        let bytes = dg.encode();
        assert_eq!(bytes.len(), 25); // minimum
        let decoded = TelemetryDatagram::decode(&bytes).unwrap();
        assert_eq!(decoded, dg);
    }

    #[test]
    fn decode_too_short() {
        let result = TelemetryDatagram::decode(&[0; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_truncated_property() {
        let mut bytes = vec![0u8; 9];
        bytes[8] = 20; // claims 20-byte property
        // but only has 9 bytes total
        let result = TelemetryDatagram::decode(&bytes);
        assert!(result.is_err());
    }
}
