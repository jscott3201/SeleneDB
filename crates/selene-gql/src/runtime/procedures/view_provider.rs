//! Materialized view provider: reads pre-computed aggregate state.
//!
//! Delegates to a provider set via static OnceLock at server startup,
//! same pattern as VectorProvider, SearchProvider, TsHistoryProvider.

use std::sync::{Arc, OnceLock};

use selene_core::IStr;

use crate::types::error::GqlError;
use crate::types::value::GqlValue;

/// Trait for reading materialized view state (decouples selene-gql from selene-server).
pub trait ViewProvider: Send + Sync {
    /// Read the current aggregate row for a named view.
    /// Returns (column_alias, value) pairs, or error if view does not exist.
    fn read_view(&self, name: &str) -> Result<Vec<(IStr, GqlValue)>, GqlError>;

    /// Check if a view exists.
    fn view_exists(&self, name: &str) -> bool;
}

static VIEW_PROVIDER: OnceLock<Arc<dyn ViewProvider>> = OnceLock::new();

/// Set the view provider. Called once at server startup.
pub fn set_view_provider(provider: Arc<dyn ViewProvider>) {
    let _ = VIEW_PROVIDER.set(provider);
}

/// Get the view provider, or error if not registered.
pub fn get_view_provider() -> Result<&'static Arc<dyn ViewProvider>, GqlError> {
    VIEW_PROVIDER
        .get()
        .ok_or_else(|| GqlError::InvalidArgument {
            message: "materialized views not available".into(),
        })
}
