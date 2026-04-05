//! Hub-side handlers for sync push and subscribe requests from edge nodes.
//!
//! Split into submodules:
//! - [`push`]: SyncPush processing with LWW merge resolution
//! - [`subscribe`]: SyncSubscribe with filtered snapshots

use std::fmt;

/// Error returned when a `SyncPushRequest` violates server-side batch
/// limits configured in [`SyncConfig`](crate::config::SyncConfig).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncPushError {
    /// The request contains more entries than `max_sync_entries`.
    TooManyEntries { count: usize, limit: usize },
    /// An entry contains more changes than `max_changes_per_entry`.
    TooManyChanges {
        entry_index: usize,
        count: usize,
        limit: usize,
    },
}

impl fmt::Display for SyncPushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyEntries { count, limit } => {
                write!(
                    f,
                    "SyncPush request contains {count} entries, exceeding the limit of {limit}"
                )
            }
            Self::TooManyChanges {
                entry_index,
                count,
                limit,
            } => {
                write!(
                    f,
                    "SyncPush entry {entry_index} contains {count} changes, \
                     exceeding the limit of {limit}"
                )
            }
        }
    }
}

mod push;
mod subscribe;

pub use push::{handle_sync_push, validate_sync_push};
pub use subscribe::{handle_sync_subscribe, validate_sync_subscribe};
