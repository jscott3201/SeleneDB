//! Wire protocol DTOs for SWP.
//!
//! All DTOs derive `serde::Serialize + Deserialize`, supporting postcard
//! (wire default) and JSON (debugging/API). Core `Arc<str>` types are
//! converted to `String` for wire transfer.

pub mod changelog;
pub mod entity;
pub mod error;
pub mod federation;
pub mod gql;
pub mod graph_slice;
pub mod service;
pub mod snapshot;
pub mod sync;
pub mod ts;
