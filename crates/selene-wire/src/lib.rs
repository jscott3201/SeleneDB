#![forbid(unsafe_code)]

pub mod codec;
pub mod datagram;
pub mod dto;
pub mod error;
pub mod flags;
pub mod frame;
pub mod io;
pub mod msg_type;
pub mod serialize;

pub use codec::SWPCodec;
pub use error::WireError;
pub use flags::WireFlags;
pub use frame::{Frame, HEADER_SIZE, MAX_PAYLOAD};
pub use msg_type::MsgType;
