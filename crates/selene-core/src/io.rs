//! Shared I/O utilities for bounded reads.

use std::io::Read;

/// Read all bytes from `reader` into a `Vec<u8>`, aborting if the output
/// exceeds `max_bytes`.
///
/// Used by selene-persist and selene-wire to enforce decompression bomb
/// protection on zstd-decoded streams. Each caller creates its own zstd
/// decoder and passes it here; this function handles only the bounded
/// read loop.
pub fn read_bounded(mut reader: impl Read, max_bytes: usize) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| format!("read error: {e}"))?;
        if n == 0 {
            break;
        }
        if output.len() + n > max_bytes {
            return Err(format!(
                "output exceeds {} MiB limit",
                max_bytes / (1024 * 1024)
            ));
        }
        output.extend_from_slice(&buf[..n]);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reads_all_bytes() {
        let data = b"hello world";
        let result = read_bounded(Cursor::new(data), 1024).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn rejects_oversized_input() {
        let data = vec![0u8; 2048];
        let err = read_bounded(Cursor::new(&data), 1024).unwrap_err();
        assert!(err.contains("limit"), "expected limit message, got: {err}");
    }

    #[test]
    fn empty_reader_returns_empty_vec() {
        let result = read_bounded(Cursor::new(b""), 1024).unwrap();
        assert!(result.is_empty());
    }
}
