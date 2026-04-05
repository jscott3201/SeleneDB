//! Cold-tier TS history provider: reads Parquet files for historical data.
//!
//! Delegates to a provider set via static OnceLock at server startup,
//! same pattern as HistoryProvider, VectorProvider, SearchProvider.

use std::sync::{Arc, OnceLock};

use crate::types::error::GqlError;

/// Trait for cold-tier time-series queries (decouples selene-gql from selene-server).
pub trait TsHistoryProvider: Send + Sync {
    /// Query historical TS data from Parquet files.
    /// Returns (timestamp_nanos, value) pairs sorted by timestamp.
    fn query(
        &self,
        entity_id: u64,
        property: &str,
        start_nanos: i64,
        end_nanos: i64,
    ) -> Vec<(i64, f64)>;
}

static TS_HISTORY_PROVIDER: OnceLock<Arc<dyn TsHistoryProvider>> = OnceLock::new();

/// Set the TS history provider. Called once at server startup.
pub fn set_ts_history_provider(provider: Arc<dyn TsHistoryProvider>) {
    let _ = TS_HISTORY_PROVIDER.set(provider);
}

/// Get the TS history provider, or error if not registered.
pub fn get_ts_history_provider() -> Result<&'static Arc<dyn TsHistoryProvider>, GqlError> {
    TS_HISTORY_PROVIDER
        .get()
        .ok_or_else(|| GqlError::InvalidArgument {
            message: "ts.history not available (no cold tier configured)".into(),
        })
}
