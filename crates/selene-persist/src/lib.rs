#![forbid(unsafe_code)]
//! WAL + snapshot persistence for Selene's in-memory graph.
//!
//! Provides standalone tools: [`Wal`] for appending change entries,
//! [`snapshot`] for full graph serialization, and [`recovery`] for
//! reconstructing a graph from a snapshot + WAL replay.
//!
//! The persistence layer does not wrap or own the graph. The caller
//! decides when and whether to persist.

pub(crate) mod compress;
pub mod config;
pub mod error;
pub mod recovery;
pub mod snapshot;
pub mod sync_cursor;
pub mod wal;
pub mod wal_reader;

pub use config::{PersistConfig, SyncPolicy};
pub use error::PersistError;
pub use wal::Wal;
