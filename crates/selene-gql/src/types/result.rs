//! GQL query result: output of query execution.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use selene_core::changeset::Change;

use super::error::{GqlStatus, MutationStats};
use crate::runtime::explain::ProfileStats;

/// The result of executing a GQL query or mutation.
///
/// Contains the result data as Arrow RecordBatch (for wire compatibility
/// with QUIC Arrow IPC), ISO GQLSTATUS information, and mutation statistics.
#[derive(Debug, Clone)]
pub struct GqlResult {
    /// Column schema derived from the RETURN clause at plan time.
    pub schema: Arc<arrow::datatypes::Schema>,
    /// Result rows as Arrow RecordBatch.
    pub batches: Vec<RecordBatch>,
    /// ISO GQLSTATUS code and message (success/no-data/error).
    pub status: GqlStatus,
    /// Counts of graph changes from INSERT/SET/DELETE (zero for read-only queries).
    pub mutations: MutationStats,
    /// Per-operator profiling stats (populated when profile=true).
    pub profile: Option<Vec<ProfileStats>>,
    /// Forward changes from mutations (empty for read-only queries).
    /// Used by the ops layer for WAL persistence, changelog, version archival.
    pub changes: Vec<Change>,
}

impl GqlResult {
    /// Create a successful empty result (no rows, no mutations).
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            batches: vec![],
            status: GqlStatus::success(0),
            mutations: MutationStats::default(),
            profile: None,
            changes: Vec::new(),
        }
    }

    /// Create a successful DDL result with a confirmation message.
    pub fn ddl_success(message: &str) -> Self {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field};

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "STATUS",
            DataType::Utf8,
            false,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![message]))],
        )
        .expect("ddl_success batch");
        Self {
            schema,
            batches: vec![batch],
            status: GqlStatus::success(1),
            mutations: MutationStats::default(),
            profile: None,
            changes: Vec::new(),
        }
    }

    /// Number of rows in the result.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Number of columns in the result.
    pub fn column_count(&self) -> usize {
        self.schema.fields().len()
    }

    /// True if the result has no rows.
    pub fn is_empty(&self) -> bool {
        self.row_count() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::error::GqlStatusCode;

    #[test]
    fn empty_result() {
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let result = GqlResult {
            schema,
            batches: vec![],
            status: GqlStatus::success(0),
            mutations: MutationStats::default(),
            profile: None,
            changes: vec![],
        };
        assert!(result.is_empty());
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.column_count(), 0);
        assert_eq!(result.status.code, GqlStatusCode::NoData);
    }
}
