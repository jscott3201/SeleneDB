//! Async frame I/O for SWP over any `AsyncWrite`/`AsyncRead` transport
//! (QUIC streams, TCP, duplex channels).
//!
//! Encodes/decodes the 6-byte SWP header and payload in a single call.
//! Used by both the server and client crates.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{Frame, HEADER_SIZE, WireError};

/// Write a [`Frame`] (6-byte header + payload) to an async writer.
pub async fn write_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    frame: &Frame,
) -> Result<(), WireError> {
    let mut header = [0u8; HEADER_SIZE];
    header[0] = u8::from(frame.msg_type);
    header[1] = frame.flags.bits();
    let len_bytes = (frame.payload.len() as u32).to_le_bytes();
    header[2..6].copy_from_slice(&len_bytes);

    writer.write_all(&header).await?;
    writer.write_all(&frame.payload).await?;

    Ok(())
}

/// Read a [`Frame`] (6-byte header + payload) from an async reader.
///
/// Returns `WireError::PayloadTooLarge` if the declared payload length exceeds
/// `MAX_PAYLOAD`.
pub async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Frame, WireError> {
    let mut header = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header).await?;

    let (msg_type, flags, payload_len) = Frame::decode_header(&header)?;
    let len = payload_len as usize;

    let mut payload_buf = vec![0u8; len];
    if len > 0 {
        reader.read_exact(&mut payload_buf).await?;
    }

    Ok(Frame {
        msg_type,
        flags,
        payload: bytes::Bytes::from(payload_buf),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MsgType, WireFlags};
    use bytes::Bytes;

    #[tokio::test]
    async fn write_read_round_trip_empty_payload() {
        let frame = Frame {
            msg_type: MsgType::Health,
            flags: WireFlags::empty(),
            payload: Bytes::new(),
        };

        let (mut client, mut server) = tokio::io::duplex(1024);
        write_frame(&mut client, &frame).await.unwrap();
        drop(client);

        let decoded = read_frame(&mut server).await.unwrap();
        assert_eq!(decoded.msg_type, MsgType::Health);
        assert!(decoded.flags.is_empty());
        assert!(decoded.payload.is_empty());
    }

    #[tokio::test]
    async fn write_read_round_trip_with_payload() {
        let frame = Frame {
            msg_type: MsgType::GqlQuery,
            flags: WireFlags::COMPRESSED,
            payload: Bytes::from(vec![1u8, 2, 3, 4, 5]),
        };

        let (mut client, mut server) = tokio::io::duplex(1024);
        write_frame(&mut client, &frame).await.unwrap();
        drop(client);

        let decoded = read_frame(&mut server).await.unwrap();
        assert_eq!(decoded.msg_type, MsgType::GqlQuery);
        assert_eq!(decoded.flags, WireFlags::COMPRESSED);
        assert_eq!(decoded.payload.as_ref(), &[1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn write_read_multiple_frames() {
        let frames = vec![
            Frame {
                msg_type: MsgType::Ok,
                flags: WireFlags::empty(),
                payload: Bytes::from("hello"),
            },
            Frame {
                msg_type: MsgType::GqlExplain,
                flags: WireFlags::JSON_FORMAT,
                payload: Bytes::from("world"),
            },
        ];

        let (mut client, mut server) = tokio::io::duplex(4096);
        for f in &frames {
            write_frame(&mut client, f).await.unwrap();
        }
        drop(client);

        let d1 = read_frame(&mut server).await.unwrap();
        assert_eq!(d1.msg_type, MsgType::Ok);
        assert_eq!(d1.payload, Bytes::from("hello"));

        let d2 = read_frame(&mut server).await.unwrap();
        assert_eq!(d2.msg_type, MsgType::GqlExplain);
        assert_eq!(d2.flags, WireFlags::JSON_FORMAT);
        assert_eq!(d2.payload, Bytes::from("world"));
    }
}
