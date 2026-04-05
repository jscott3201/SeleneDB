use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::WireError;
use crate::flags::WireFlags;
use crate::frame::{Frame, HEADER_SIZE, MAX_PAYLOAD};
use crate::msg_type::MsgType;

/// SWP codec for framing over QUIC streams or any AsyncRead/AsyncWrite transport.
///
/// Decodes in two phases: reads the 6-byte header, then reads `payload_len`
/// bytes for the payload. Maximum payload size is configurable via
/// [`SWPCodec::new`] to limit per-stream memory consumption.
pub struct SWPCodec {
    state: DecodeState,
    max_payload: usize,
}

enum DecodeState {
    Header,
    Payload {
        msg_type: MsgType,
        flags: WireFlags,
        len: usize,
    },
}

impl SWPCodec {
    /// Create a codec with a custom maximum payload size in bytes.
    pub fn new(max_payload: usize) -> Self {
        Self {
            state: DecodeState::Header,
            max_payload,
        }
    }
}

impl Default for SWPCodec {
    fn default() -> Self {
        Self::new(MAX_PAYLOAD)
    }
}

impl Decoder for SWPCodec {
    type Item = Frame;
    type Error = WireError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match &self.state {
                DecodeState::Header => {
                    if src.len() < HEADER_SIZE {
                        return Ok(None);
                    }
                    let header_bytes: [u8; HEADER_SIZE] =
                        src[..HEADER_SIZE].try_into().expect("checked length above");
                    let (msg_type, flags, payload_len) = Frame::decode_header(&header_bytes)?;
                    let len = payload_len as usize;
                    if len > self.max_payload {
                        return Err(WireError::PayloadTooLarge(len));
                    }
                    src.advance(HEADER_SIZE);
                    self.state = DecodeState::Payload {
                        msg_type,
                        flags,
                        len,
                    };
                }
                DecodeState::Payload {
                    msg_type,
                    flags,
                    len,
                } => {
                    let len = *len;
                    if src.len() < len {
                        return Ok(None);
                    }
                    let msg_type = *msg_type;
                    let flags = *flags;
                    let payload = src.split_to(len).freeze();
                    self.state = DecodeState::Header;
                    return Ok(Some(Frame {
                        msg_type,
                        flags,
                        payload,
                    }));
                }
            }
        }
    }
}

impl Encoder<Frame> for SWPCodec {
    type Error = WireError;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if frame.payload.len() > self.max_payload {
            return Err(WireError::PayloadTooLarge(frame.payload.len()));
        }
        dst.reserve(HEADER_SIZE + frame.payload.len());
        dst.put_u8(u8::from(frame.msg_type));
        dst.put_u8(frame.flags.bits());
        dst.put_u32_le(frame.payload.len() as u32);
        dst.put(frame.payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio_util::codec::{FramedRead, FramedWrite};

    use futures::SinkExt;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn codec_round_trip_empty_payload() {
        let frame = Frame {
            msg_type: MsgType::Health,
            flags: WireFlags::empty(),
            payload: Bytes::new(),
        };

        let (client, server) = tokio::io::duplex(1024);
        let mut writer = FramedWrite::new(client, SWPCodec::default());
        let mut reader = FramedRead::new(server, SWPCodec::default());

        writer.send(frame.clone()).await.unwrap();
        drop(writer);

        let decoded = reader.next().await.unwrap().unwrap();
        assert_eq!(decoded.msg_type, MsgType::Health);
        assert!(decoded.flags.is_empty());
        assert!(decoded.payload.is_empty());
    }

    #[tokio::test]
    async fn codec_round_trip_with_payload() {
        let payload = Bytes::from(vec![1u8, 2, 3, 4, 5]);
        let frame = Frame {
            msg_type: MsgType::GqlQuery,
            flags: WireFlags::COMPRESSED,
            payload: payload.clone(),
        };

        let (client, server) = tokio::io::duplex(1024);
        let mut writer = FramedWrite::new(client, SWPCodec::default());
        let mut reader = FramedRead::new(server, SWPCodec::default());

        writer.send(frame).await.unwrap();
        drop(writer);

        let decoded = reader.next().await.unwrap().unwrap();
        assert_eq!(decoded.msg_type, MsgType::GqlQuery);
        assert_eq!(decoded.flags, WireFlags::COMPRESSED);
        assert_eq!(decoded.payload, payload);
    }

    #[tokio::test]
    async fn codec_multiple_frames() {
        let frames = vec![
            Frame {
                msg_type: MsgType::GqlQuery,
                flags: WireFlags::empty(),
                payload: Bytes::from("hello"),
            },
            Frame {
                msg_type: MsgType::GqlExplain,
                flags: WireFlags::JSON_FORMAT,
                payload: Bytes::from("world"),
            },
            Frame {
                msg_type: MsgType::Ok,
                flags: WireFlags::empty(),
                payload: Bytes::new(),
            },
        ];

        let (client, server) = tokio::io::duplex(4096);
        let mut writer = FramedWrite::new(client, SWPCodec::default());
        let mut reader = FramedRead::new(server, SWPCodec::default());

        for f in &frames {
            writer.send(f.clone()).await.unwrap();
        }
        drop(writer);

        let d1 = reader.next().await.unwrap().unwrap();
        assert_eq!(d1.msg_type, MsgType::GqlQuery);
        assert_eq!(d1.payload, Bytes::from("hello"));

        let d2 = reader.next().await.unwrap().unwrap();
        assert_eq!(d2.msg_type, MsgType::GqlExplain);
        assert_eq!(d2.flags, WireFlags::JSON_FORMAT);

        let d3 = reader.next().await.unwrap().unwrap();
        assert_eq!(d3.msg_type, MsgType::Ok);
        assert!(d3.payload.is_empty());

        assert!(reader.next().await.is_none());
    }

    #[tokio::test]
    async fn codec_large_payload() {
        let payload = Bytes::from(vec![0xABu8; 1_000_000]); // 1MB
        let frame = Frame {
            msg_type: MsgType::TsPayload,
            flags: WireFlags::COMPRESSED,
            payload: payload.clone(),
        };

        let (client, server) = tokio::io::duplex(2_000_000);
        let mut writer = FramedWrite::new(client, SWPCodec::default());
        let mut reader = FramedRead::new(server, SWPCodec::default());

        writer.send(frame).await.unwrap();
        drop(writer);

        let decoded = reader.next().await.unwrap().unwrap();
        assert_eq!(decoded.payload.len(), 1_000_000);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn encoder_rejects_oversized_payload() {
        let frame = Frame {
            msg_type: MsgType::Ok,
            flags: WireFlags::empty(),
            payload: Bytes::from(vec![0u8; MAX_PAYLOAD + 1]),
        };
        let mut codec = SWPCodec::default();
        let mut dst = BytesMut::new();
        let result = codec.encode(frame, &mut dst);
        assert!(result.is_err());
    }

    #[test]
    fn decoder_partial_header_returns_none() {
        let mut codec = SWPCodec::default();
        let mut src = BytesMut::from(&[0x50, 0x00, 0x00][..]); // only 3 bytes
        let result = codec.decode(&mut src).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decoder_partial_payload_returns_none() {
        let mut codec = SWPCodec::default();
        // Header: MsgType::Health (0x50), no flags, payload_len=10 LE
        let mut src = BytesMut::new();
        src.put_u8(0x50);
        src.put_u8(0x00);
        src.put_u32_le(10);
        src.put_slice(&[1, 2, 3]); // only 3 of 10 bytes

        let result = codec.decode(&mut src).unwrap();
        assert!(result.is_none());
    }
}
