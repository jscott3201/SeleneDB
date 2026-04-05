use bitflags::bitflags;

bitflags! {
    /// Per-frame wire flags as defined in SWP spec section 3.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct WireFlags: u8 {
        /// Payload is zstd-compressed.
        const COMPRESSED   = 0b0000_0001;
        /// Payload is JSON instead of postcard (default binary format).
        const JSON_FORMAT  = 0b0000_0010;
        /// Payload is Arrow IPC format.
        const ARROW_FORMAT = 0b0000_0100;
        /// More frames follow for this logical message (streaming response).
        const CONTINUED    = 0b0000_1000;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_flags() {
        let f = WireFlags::empty();
        assert!(!f.contains(WireFlags::COMPRESSED));
        assert!(!f.contains(WireFlags::JSON_FORMAT));
        assert!(!f.contains(WireFlags::ARROW_FORMAT));
        assert!(!f.contains(WireFlags::CONTINUED));
        assert_eq!(f.bits(), 0x00);
    }

    #[test]
    fn individual_flags() {
        assert_eq!(WireFlags::COMPRESSED.bits(), 0x01);
        assert_eq!(WireFlags::JSON_FORMAT.bits(), 0x02);
        assert_eq!(WireFlags::ARROW_FORMAT.bits(), 0x04);
        assert_eq!(WireFlags::CONTINUED.bits(), 0x08);
    }

    #[test]
    fn combined_flags() {
        let f = WireFlags::COMPRESSED | WireFlags::JSON_FORMAT;
        assert_eq!(f.bits(), 0x03);
        assert!(f.contains(WireFlags::COMPRESSED));
        assert!(f.contains(WireFlags::JSON_FORMAT));
        assert!(!f.contains(WireFlags::ARROW_FORMAT));

        let all = WireFlags::COMPRESSED
            | WireFlags::JSON_FORMAT
            | WireFlags::ARROW_FORMAT
            | WireFlags::CONTINUED;
        assert_eq!(all.bits(), 0x0F);
    }

    #[test]
    fn from_bits_truncate_ignores_unknown() {
        // Upper nibble is reserved; truncate should drop it silently.
        let f = WireFlags::from_bits_truncate(0xFF);
        assert_eq!(f.bits(), 0x0F);
    }

    #[test]
    fn roundtrip_bits() {
        for raw in 0u8..=0x0Fu8 {
            let f = WireFlags::from_bits_truncate(raw);
            assert_eq!(f.bits(), raw);
        }
    }
}
