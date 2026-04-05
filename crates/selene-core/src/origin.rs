//! Change origin tracking for bidirectional sync.
//!
//! Every mutation carries an [`Origin`] that records whether it was produced
//! locally or arrived via replication. The sync layer uses this to prevent
//! echo loops: a replicated change is never re-sent to the peer that
//! originated it.

use serde::{Deserialize, Serialize};

/// Distinguishes locally-generated changes from replicated ones.
///
/// Encoded as a single `u8` for compact wire and WAL representation.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Origin {
    /// The change was produced by this node.
    #[default]
    Local = 0x00,
    /// The change arrived from a remote peer via replication.
    Replicated = 0x01,
}

impl Origin {
    /// Decode from a single byte. Unknown values default to [`Origin::Local`].
    #[inline]
    pub fn from_byte(b: u8) -> Self {
        match b {
            0x01 => Self::Replicated,
            _ => Self::Local,
        }
    }

    /// Encode as a single byte.
    #[inline]
    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for origin in [Origin::Local, Origin::Replicated] {
            assert_eq!(Origin::from_byte(origin.to_byte()), origin);
        }
    }

    #[test]
    fn unknown_byte_defaults_to_local() {
        assert_eq!(Origin::from_byte(0xFF), Origin::Local);
    }

    #[test]
    fn default_is_local() {
        assert_eq!(Origin::default(), Origin::Local);
    }

    #[test]
    fn serde_round_trip() {
        for origin in [Origin::Local, Origin::Replicated] {
            let bytes = postcard::to_allocvec(&origin).expect("serialize");
            let decoded: Origin = postcard::from_bytes(&bytes).expect("deserialize");
            assert_eq!(decoded, origin);
        }
    }
}
