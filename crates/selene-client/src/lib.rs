#![forbid(unsafe_code)]
//! Async QUIC client for Selene.
//!
//! High-level API for entity CRUD, time-series, GQL queries, and health
//! checks over the SWP wire protocol. Used by downstream consumers,
//! aggregation hubs, and selene-cli.

pub mod client;
pub mod config;
pub mod error;

pub use client::{ChangelogSubscription, SeleneClient};
pub use config::{AuthCredentials, ClientConfig, ClientTlsConfig};
pub use error::ClientError;
