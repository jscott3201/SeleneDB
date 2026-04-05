//! Shared zstd decompression with bomb protection.
//!
//! Both WAL and snapshot readers need to detect zstd-compressed payloads
//! (via magic bytes) and decompress with a size limit. This module provides
//! a single implementation used by both.
//!
//! The bounded read loop is shared with selene-wire via
//! `selene_core::io::read_bounded`.

use std::borrow::Cow;

use crate::error::PersistError;

/// Zstd frame magic bytes: `0x28 0xB5 0x2F 0xFD`.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Decompress `data` if it starts with the zstd magic bytes; otherwise return
/// a borrowed reference to the original data.
///
/// Decompression is capped at `max_decompressed` bytes to protect against
/// decompression bombs. Returns `PersistError::Serialization` on failure.
pub(crate) fn decompress_if_zstd(
    data: &[u8],
    max_decompressed: usize,
) -> Result<Cow<'_, [u8]>, PersistError> {
    if data.len() >= 4 && data[..4] == ZSTD_MAGIC {
        let decoder = zstd::Decoder::new(data)
            .map_err(|e| PersistError::Serialization(format!("zstd init: {e}")))?;
        let output = selene_core::io::read_bounded(decoder, max_decompressed)
            .map_err(|e| PersistError::Serialization(format!("zstd decompress: {e}")))?;
        Ok(Cow::Owned(output))
    } else {
        Ok(Cow::Borrowed(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncompressed_data_returned_as_is() {
        let data = b"hello world";
        let result = decompress_if_zstd(data, 1024).unwrap();
        assert_eq!(&*result, data);
        assert!(
            matches!(result, Cow::Borrowed(_)),
            "uncompressed should be Cow::Borrowed"
        );
    }

    #[test]
    fn compressed_data_decompressed() {
        let original = b"hello world hello world hello world";
        let compressed = zstd::encode_all(&original[..], 1).unwrap();
        let result = decompress_if_zstd(&compressed, 1024).unwrap();
        assert_eq!(&*result, original);
        assert!(
            matches!(result, Cow::Owned(_)),
            "decompressed should be Cow::Owned"
        );
    }

    #[test]
    fn bomb_protection_triggers() {
        // Compress some data, then try to decompress with a tiny limit
        let original = vec![0u8; 1000];
        let compressed = zstd::encode_all(&original[..], 1).unwrap();
        let result = decompress_if_zstd(&compressed, 100);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("limit"), "unexpected error: {err}");
    }

    #[test]
    fn empty_data_returned_as_is() {
        let result = decompress_if_zstd(&[], 1024).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn short_data_not_misidentified_as_zstd() {
        let result = decompress_if_zstd(&[0x28, 0xB5], 1024).unwrap();
        assert_eq!(&*result, &[0x28, 0xB5]);
    }
}
