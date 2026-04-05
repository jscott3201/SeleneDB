//! Serialization helpers for SWP payloads.
//!
//! Default format: **postcard** (compact, no_std-friendly binary via serde).
//! JSON mode available via `WireFlags::JSON_FORMAT` for debugging and browser
//! clients. Zstd compression applied automatically for payloads >= 512 bytes.
//!
//! Datagrams (telemetry fast-path) use hand-packed binary; see [`crate::datagram`].

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::WireError;
use crate::flags::WireFlags;

/// Serialize `value` to bytes using postcard or JSON depending on `flags`.
///
/// - Default (no flags): postcard (compact binary via serde)
/// - `WireFlags::JSON_FORMAT` set: JSON via `serde_json`
pub fn serialize_payload<T: Serialize>(value: &T, flags: WireFlags) -> Result<Vec<u8>, WireError> {
    if flags.contains(WireFlags::JSON_FORMAT) {
        serde_json::to_vec(value)
            .map_err(|e| WireError::SerializationError(format!("JSON serialize: {e}")))
    } else {
        postcard::to_allocvec(value)
            .map_err(|e| WireError::SerializationError(format!("postcard serialize: {e}")))
    }
}

/// Deserialize bytes into `T` using postcard or JSON depending on `flags`.
///
/// - Default (no flags): postcard (compact binary via serde)
/// - `WireFlags::JSON_FORMAT` set: JSON via `serde_json`
pub fn deserialize_payload<T: DeserializeOwned>(
    bytes: &[u8],
    flags: WireFlags,
) -> Result<T, WireError> {
    if flags.contains(WireFlags::JSON_FORMAT) {
        serde_json::from_slice(bytes)
            .map_err(|e| WireError::DeserializationError(format!("JSON deserialize: {e}")))
    } else {
        postcard::from_bytes(bytes)
            .map_err(|e| WireError::DeserializationError(format!("postcard deserialize: {e}")))
    }
}

/// Compress `payload` with zstd level 3 if >= 512 bytes and compression reduces
/// size. Sets `WireFlags::COMPRESSED` in `flags` on success. Returns payload
/// unchanged if compression is skipped or would increase size.
pub fn maybe_compress(payload: Vec<u8>, flags: &mut WireFlags) -> Result<Vec<u8>, WireError> {
    if payload.len() >= 512 {
        let compressed = zstd::encode_all(payload.as_slice(), 3)
            .map_err(|e| WireError::CompressionError(format!("zstd compress: {e}")))?;
        if compressed.len() < payload.len() {
            *flags |= WireFlags::COMPRESSED;
            return Ok(compressed);
        }
    }
    Ok(payload)
}

/// Maximum decompressed output size (64 MiB). Prevents zip-bomb attacks where
/// a small compressed payload within the 16 MiB wire limit expands to gigabytes.
pub const MAX_DECOMPRESSED: usize = 64 * 1024 * 1024;

/// Decompress `payload` if `WireFlags::COMPRESSED` is set in `flags`.
/// Returns the payload unchanged when the flag is absent.
///
/// Enforces a [`MAX_DECOMPRESSED`] output size limit to prevent zip-bomb
/// denial-of-service attacks. The bounded read loop is shared with
/// selene-persist via `selene_core::io::read_bounded`.
pub fn maybe_decompress(payload: Vec<u8>, flags: WireFlags) -> Result<Vec<u8>, WireError> {
    if flags.contains(WireFlags::COMPRESSED) {
        let decoder = zstd::Decoder::new(payload.as_slice())
            .map_err(|e| WireError::CompressionError(format!("zstd init: {e}")))?;
        selene_core::io::read_bounded(decoder, MAX_DECOMPRESSED)
            .map_err(|e| WireError::CompressionError(format!("zstd decompress: {e}")))
    } else {
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestMsg {
        id: u64,
        name: String,
        values: Vec<f64>,
    }

    fn sample_msg() -> TestMsg {
        TestMsg {
            id: 42,
            name: "sensor-1".into(),
            values: vec![1.1, 2.2, 3.3],
        }
    }

    // -- Postcard round-trip --

    #[test]
    fn postcard_serialize_deserialize_round_trip() {
        let original = sample_msg();
        let flags = WireFlags::empty();
        let bytes = serialize_payload(&original, flags).unwrap();
        let decoded: TestMsg = deserialize_payload(&bytes, flags).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn postcard_bytes_are_not_json() {
        let bytes = serialize_payload(&sample_msg(), WireFlags::empty()).unwrap();
        assert_ne!(bytes.first(), Some(&b'{'));
    }

    #[test]
    fn postcard_is_compact() {
        let msg = sample_msg();
        let postcard_bytes = serialize_payload(&msg, WireFlags::empty()).unwrap();
        let json_bytes = serialize_payload(&msg, WireFlags::JSON_FORMAT).unwrap();
        // Postcard should be significantly smaller than JSON
        assert!(
            postcard_bytes.len() < json_bytes.len(),
            "postcard ({}) should be smaller than JSON ({})",
            postcard_bytes.len(),
            json_bytes.len()
        );
    }

    // -- JSON round-trip --

    #[test]
    fn json_serialize_deserialize_round_trip() {
        let original = sample_msg();
        let flags = WireFlags::JSON_FORMAT;
        let bytes = serialize_payload(&original, flags).unwrap();
        let decoded: TestMsg = deserialize_payload(&bytes, flags).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn json_bytes_are_valid_utf8() {
        let flags = WireFlags::JSON_FORMAT;
        let bytes = serialize_payload(&sample_msg(), flags).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("sensor-1"));
    }

    // -- Format flag determines codec --

    #[test]
    fn postcard_and_json_produce_different_bytes() {
        let msg = sample_msg();
        let pc = serialize_payload(&msg, WireFlags::empty()).unwrap();
        let js = serialize_payload(&msg, WireFlags::JSON_FORMAT).unwrap();
        assert_ne!(pc, js);
    }

    #[test]
    fn cross_format_deserialization_fails() {
        let msg = sample_msg();
        let pc_bytes = serialize_payload(&msg, WireFlags::empty()).unwrap();
        let result: Result<TestMsg, _> = deserialize_payload(&pc_bytes, WireFlags::JSON_FORMAT);
        assert!(result.is_err());
    }

    // -- Compression --

    #[test]
    fn small_payload_not_compressed() {
        let payload = b"hello world".to_vec();
        let mut flags = WireFlags::empty();
        let result = maybe_compress(payload.clone(), &mut flags).unwrap();
        assert_eq!(result, payload);
        assert!(!flags.contains(WireFlags::COMPRESSED));
    }

    #[test]
    fn large_payload_compressed() {
        let payload = b"ABCDEFGH".repeat(100);
        let mut flags = WireFlags::empty();
        let compressed = maybe_compress(payload.clone(), &mut flags).unwrap();
        assert!(flags.contains(WireFlags::COMPRESSED));
        assert!(compressed.len() < payload.len());
    }

    #[test]
    fn compression_decompression_round_trip() {
        let payload = b"Lorem ipsum dolor sit amet, ".repeat(30);
        let mut flags = WireFlags::empty();
        let compressed = maybe_compress(payload.clone(), &mut flags).unwrap();
        assert!(flags.contains(WireFlags::COMPRESSED));

        let decompressed = maybe_decompress(compressed, flags).unwrap();
        assert_eq!(decompressed, payload);
    }

    #[test]
    fn no_decompression_when_flag_absent() {
        let payload = b"small payload".to_vec();
        let flags = WireFlags::empty();
        let result = maybe_decompress(payload.clone(), flags).unwrap();
        assert_eq!(result, payload);
    }

    #[test]
    fn compress_exactly_512_bytes() {
        let payload = vec![0xABu8; 512];
        let mut flags = WireFlags::empty();
        let result = maybe_compress(payload.clone(), &mut flags).unwrap();
        if flags.contains(WireFlags::COMPRESSED) {
            let decompressed = maybe_decompress(result, flags).unwrap();
            assert_eq!(decompressed, payload);
        } else {
            assert_eq!(result, payload);
        }
    }

    #[test]
    fn compress_511_bytes_not_compressed() {
        let payload = vec![0u8; 511];
        let mut flags = WireFlags::empty();
        maybe_compress(payload, &mut flags).unwrap();
        assert!(!flags.contains(WireFlags::COMPRESSED));
    }

    #[test]
    fn full_pipeline_postcard_compress_decompress_deserialize() {
        let msgs: Vec<TestMsg> = (0..50)
            .map(|i| TestMsg {
                id: i,
                name: format!("entity-{i}"),
                values: (0..10).map(|j| (i * 10 + j) as f64).collect(),
            })
            .collect();

        let mut flags = WireFlags::empty();
        let bytes = serialize_payload(&msgs, flags).unwrap();
        let bytes = maybe_compress(bytes, &mut flags).unwrap();

        let bytes = maybe_decompress(bytes, flags).unwrap();
        let decoded: Vec<TestMsg> = deserialize_payload(&bytes, flags).unwrap();
        assert_eq!(decoded, msgs);
    }

    // -- Serde with core types --

    #[test]
    fn postcard_with_arc_str_via_serde() {
        use std::collections::HashMap;

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct NodeDto {
            id: u64,
            labels: Vec<String>,
            properties: HashMap<String, String>,
        }

        let dto = NodeDto {
            id: 42,
            labels: vec!["sensor".into(), "temperature".into()],
            properties: {
                let mut m = HashMap::new();
                m.insert("unit".into(), "°F".into());
                m
            },
        };

        let flags = WireFlags::empty();
        let bytes = serialize_payload(&dto, flags).unwrap();
        let decoded: NodeDto = deserialize_payload(&bytes, flags).unwrap();
        assert_eq!(decoded, dto);
    }
}
