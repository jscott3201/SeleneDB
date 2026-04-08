use bytes::{BufMut, Bytes, BytesMut};

use crate::{MsgType, WireError, WireFlags};

/// Size of a SWP frame header in bytes.
pub const HEADER_SIZE: usize = 6;

/// Maximum allowed payload size (16 MiB).
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;

/// A single SWP frame: a 6-byte header followed by a variable-length payload.
///
/// Header layout (per spec section 3):
/// ```text
/// +----------+----------+--------------+
/// | msg_type | flags    | payload_len  |
/// | 1B       | 1B       | 4B LE        |
/// +----------+----------+--------------+
/// ```
#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: MsgType,
    pub flags: WireFlags,
    pub payload: Bytes,
}

impl Frame {
    /// Construct a new frame. Returns `WireError::PayloadTooLarge` if payload exceeds `MAX_PAYLOAD`.
    pub fn new(msg_type: MsgType, flags: WireFlags, payload: Bytes) -> Result<Self, WireError> {
        if payload.len() > MAX_PAYLOAD {
            return Err(WireError::PayloadTooLarge(payload.len()));
        }
        Ok(Self {
            msg_type,
            flags,
            payload,
        })
    }

    /// Encode to a contiguous byte buffer (header + payload).
    pub fn encode(&self) -> Bytes {
        let total = HEADER_SIZE + self.payload.len();
        let mut buf = BytesMut::with_capacity(total);
        buf.put_u8(u8::from(self.msg_type));
        buf.put_u8(self.flags.bits());
        buf.put_u32_le(self.payload.len() as u32);
        buf.put_slice(&self.payload);
        buf.freeze()
    }

    /// Parse the 6-byte header, returning `(msg_type, flags, payload_len)`.
    ///
    /// Returns `WireError::PayloadTooLarge` if `payload_len` exceeds `MAX_PAYLOAD`.
    /// Returns `WireError::InvalidMsgType` for unknown type bytes.
    pub fn decode_header(
        bytes: &[u8; HEADER_SIZE],
    ) -> Result<(MsgType, WireFlags, u32), WireError> {
        let msg_type = MsgType::try_from(bytes[0])?;
        let flags = WireFlags::from_bits_truncate(bytes[1]);
        let payload_len = u32::from_le_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
        if payload_len as usize > MAX_PAYLOAD {
            return Err(WireError::PayloadTooLarge(payload_len as usize));
        }
        Ok((msg_type, flags, payload_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(msg_type: MsgType, flags: WireFlags, payload: &[u8]) -> Frame {
        Frame::new(msg_type, flags, Bytes::copy_from_slice(payload)).unwrap()
    }

    // -- encode / decode round-trips --

    #[test]
    fn encode_decode_empty_payload() {
        let frame = make_frame(MsgType::Health, WireFlags::empty(), &[]);
        let encoded = frame.encode();
        assert_eq!(encoded.len(), HEADER_SIZE);

        let header: [u8; HEADER_SIZE] = encoded[..HEADER_SIZE].try_into().unwrap();
        let (msg_type, flags, len) = Frame::decode_header(&header).unwrap();
        assert_eq!(msg_type, MsgType::Health);
        assert_eq!(flags, WireFlags::empty());
        assert_eq!(len, 0);
    }

    #[test]
    fn encode_decode_with_payload() {
        let payload = b"hello, selene!";
        let frame = make_frame(MsgType::GqlQuery, WireFlags::JSON_FORMAT, payload);
        let encoded = frame.encode();
        assert_eq!(encoded.len(), HEADER_SIZE + payload.len());

        let header: [u8; HEADER_SIZE] = encoded[..HEADER_SIZE].try_into().unwrap();
        let (msg_type, flags, len) = Frame::decode_header(&header).unwrap();
        assert_eq!(msg_type, MsgType::GqlQuery);
        assert!(flags.contains(WireFlags::JSON_FORMAT));
        assert_eq!(len as usize, payload.len());
        assert_eq!(&encoded[HEADER_SIZE..], payload);
    }

    #[test]
    fn encode_decode_all_flag_combinations() {
        let all_flags = [
            WireFlags::empty(),
            WireFlags::COMPRESSED,
            WireFlags::JSON_FORMAT,
            WireFlags::ARROW_FORMAT,
            WireFlags::CONTINUED,
            WireFlags::COMPRESSED | WireFlags::JSON_FORMAT,
            WireFlags::COMPRESSED | WireFlags::CONTINUED,
            WireFlags::COMPRESSED
                | WireFlags::JSON_FORMAT
                | WireFlags::ARROW_FORMAT
                | WireFlags::CONTINUED,
        ];
        for flags in all_flags {
            let frame = make_frame(MsgType::Ok, flags, b"data");
            let encoded = frame.encode();
            let header: [u8; HEADER_SIZE] = encoded[..HEADER_SIZE].try_into().unwrap();
            let (_, decoded_flags, _) = Frame::decode_header(&header).unwrap();
            assert_eq!(decoded_flags, flags, "flags mismatch for {flags:?}");
        }
    }

    #[test]
    fn encode_payload_len_is_little_endian() {
        let payload = vec![0u8; 0x01_02_03];
        let frame = Frame::new(
            MsgType::TsPayload,
            WireFlags::empty(),
            Bytes::copy_from_slice(&payload),
        )
        .unwrap();
        let encoded = frame.encode();
        // bytes[2..6] are the LE payload_len
        assert_eq!(encoded[2], 0x03);
        assert_eq!(encoded[3], 0x02);
        assert_eq!(encoded[4], 0x01);
        assert_eq!(encoded[5], 0x00);
    }

    // -- payload size limits --

    #[test]
    fn new_rejects_payload_too_large() {
        let oversized = Bytes::from(vec![0u8; MAX_PAYLOAD + 1]);
        match Frame::new(MsgType::Ok, WireFlags::empty(), oversized) {
            Err(WireError::PayloadTooLarge(n)) => assert_eq!(n, MAX_PAYLOAD + 1),
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn decode_header_rejects_payload_too_large() {
        // Build a header with payload_len = MAX_PAYLOAD + 1
        let too_big = (MAX_PAYLOAD + 1) as u32;
        let bytes: [u8; HEADER_SIZE] = [
            u8::from(MsgType::Ok),
            0x00,
            (too_big & 0xFF) as u8,
            ((too_big >> 8) & 0xFF) as u8,
            ((too_big >> 16) & 0xFF) as u8,
            ((too_big >> 24) & 0xFF) as u8,
        ];
        match Frame::decode_header(&bytes) {
            Err(WireError::PayloadTooLarge(n)) => assert_eq!(n, MAX_PAYLOAD + 1),
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn new_accepts_max_payload_exactly() {
        let exact = Bytes::from(vec![0u8; MAX_PAYLOAD]);
        assert!(Frame::new(MsgType::Ok, WireFlags::empty(), exact).is_ok());
    }

    // -- invalid message type --

    #[test]
    fn decode_header_rejects_invalid_msg_type() {
        let bytes: [u8; HEADER_SIZE] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        match Frame::decode_header(&bytes) {
            Err(WireError::InvalidMsgType(0x00)) => {}
            other => panic!("expected InvalidMsgType(0x00), got {other:?}"),
        }
    }
}
