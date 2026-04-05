//! GQL error types with ISO GQLSTATUS codes.
//!
//! Status codes map to ISO/IEC 39075:2024 Table 8:
//!   - Parse/Auth/UnknownProcedure -> 42000 (syntax error class)
//!   - Type/InvalidArgument/Graph/SchemaViolation -> 22000 (data exception class)
//!   - NotFound -> 02000 (no data)
//!   - Transaction/ResourcesExhausted/Internal -> 50000 (internal error)

/// ISO GQL status codes (ISO/IEC 39075 §23, Table 8).
///
/// Primary status codes reported in query execution outcomes.
/// Class (2 chars) + Subclass (3 chars) = 5-char GQLSTATUS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GqlStatusCode {
    /// 00000: successful completion with at least one row.
    Success,
    /// 02000: successful completion with zero rows.
    NoData,
    /// 22000: data exception (type errors, overflow, invalid values).
    DataException,
    /// 22012: division by zero.
    DivisionByZero,
    /// 22G03: invalid value type.
    InvalidValueType,
    /// 22G04: values not comparable.
    ValuesNotComparable,
    /// 25G01: active transaction exists.
    ActiveTransaction,
    /// 42000: syntax error or access rule violation.
    SyntaxError,
    /// 42001: invalid syntax.
    InvalidSyntax,
    /// 42002: invalid reference.
    InvalidReference,
    /// G1001: dependent object error (edges still exist).
    EdgesStillExist,
    /// G2000: graph type violation.
    GraphTypeViolation,
    /// 50000: internal/system error.
    InternalError,
}

impl GqlStatusCode {
    /// Five-digit GQLSTATUS string per ISO spec §23.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Success => "00000",
            Self::NoData => "02000",
            Self::DataException => "22000",
            Self::DivisionByZero => "22012",
            Self::InvalidValueType => "22G03",
            Self::ValuesNotComparable => "22G04",
            Self::ActiveTransaction => "25G01",
            Self::SyntaxError => "42000",
            Self::InvalidSyntax => "42001",
            Self::InvalidReference => "42002",
            Self::EdgesStillExist => "G1001",
            Self::GraphTypeViolation => "G2000",
            Self::InternalError => "50000",
        }
    }
}

impl std::fmt::Display for GqlStatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// GQL execution status included in every query result.
#[derive(Debug, Clone)]
pub struct GqlStatus {
    pub code: GqlStatusCode,
    pub message: String,
}

impl GqlStatus {
    pub fn success(row_count: usize) -> Self {
        if row_count > 0 {
            Self {
                code: GqlStatusCode::Success,
                message: "successful completion".into(),
            }
        } else {
            Self {
                code: GqlStatusCode::NoData,
                message: "no data".into(),
            }
        }
    }
}

/// Counts of graph changes from INSERT/SET/DELETE.
#[derive(Debug, Clone, Default)]
pub struct MutationStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub edges_created: usize,
    pub edges_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
}

/// GQL engine error type.
///
/// Maps to GQLSTATUS codes for ISO-compliant error reporting.
/// Each variant carries enough context for actionable error messages.
#[derive(Debug, thiserror::Error)]
pub enum GqlError {
    /// Parse error (GQLSTATUS 42000).
    #[error("syntax error: {message}")]
    Parse {
        message: String,
        /// (line, column) if available from the parser.
        position: Option<(usize, usize)>,
    },

    /// Type error (GQLSTATUS 22000): mismatch, overflow, invalid cast, storing graph references.
    #[error("type error: {message}")]
    Type { message: String },

    /// Entity not found (GQLSTATUS 02000).
    #[error("{entity} {id} not found")]
    NotFound { entity: &'static str, id: u64 },

    /// Authorization denied (GQLSTATUS 42000).
    #[error("access denied")]
    AuthDenied,

    /// Invalid argument to a function or procedure (GQLSTATUS 22000).
    #[error("invalid argument: {message}")]
    InvalidArgument { message: String },

    /// Unknown procedure name in CALL (GQLSTATUS 42000).
    #[error("unknown procedure: {name}")]
    UnknownProcedure { name: String },

    /// Transaction error, commit/rollback issues (GQLSTATUS 50000).
    #[error("transaction error: {message}")]
    Transaction { message: String },

    /// Graph mutation error from TrackedMutation (GQLSTATUS 22000).
    #[error("graph error: {message}")]
    Graph { message: String },

    /// Schema violation on mutation (GQLSTATUS 22000).
    #[error("schema violation: {message}")]
    SchemaViolation { message: String },

    /// Resources exhausted (GQLSTATUS 50000).
    #[error("resources exhausted: {message}")]
    ResourcesExhausted { message: String },

    /// Internal error (GQLSTATUS 50000).
    #[error("internal error: {message}")]
    Internal { message: String },

    /// Unsupported operation (GQLSTATUS 50000).
    ///
    /// Used by `eval_vec` to signal that an expression node is not yet handled
    /// by the batch evaluator, triggering fallback to per-row evaluation.
    #[error("unsupported: {feature}")]
    Unsupported { feature: String },
}

impl GqlError {
    pub fn type_error(msg: impl Into<String>) -> Self {
        Self::Type {
            message: msg.into(),
        }
    }

    pub fn parse_error(msg: impl Into<String>) -> Self {
        Self::Parse {
            message: msg.into(),
            position: None,
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal {
            message: msg.into(),
        }
    }

    /// Map this error to its ISO GQLSTATUS code.
    pub fn status_code(&self) -> GqlStatusCode {
        match self {
            Self::Parse { .. } | Self::AuthDenied | Self::UnknownProcedure { .. } => {
                GqlStatusCode::SyntaxError
            }
            Self::Type { .. }
            | Self::InvalidArgument { .. }
            | Self::Graph { .. }
            | Self::SchemaViolation { .. } => GqlStatusCode::DataException,
            Self::NotFound { .. } => GqlStatusCode::NoData,
            Self::Transaction { .. }
            | Self::ResourcesExhausted { .. }
            | Self::Internal { .. }
            | Self::Unsupported { .. } => GqlStatusCode::InternalError,
        }
    }
}

impl From<selene_graph::GraphError> for GqlError {
    fn from(e: selene_graph::GraphError) -> Self {
        use selene_graph::GraphError;
        match e {
            GraphError::NodeNotFound(id) => Self::NotFound {
                entity: "node",
                id: id.0,
            },
            GraphError::EdgeNotFound(id) => Self::NotFound {
                entity: "edge",
                id: id.0,
            },
            GraphError::SchemaViolation(msg) => Self::SchemaViolation { message: msg },
            other => Self::Graph {
                message: other.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_codes_match_iso() {
        assert_eq!(GqlStatusCode::Success.code(), "00000");
        assert_eq!(GqlStatusCode::NoData.code(), "02000");
        assert_eq!(GqlStatusCode::DataException.code(), "22000");
        assert_eq!(GqlStatusCode::SyntaxError.code(), "42000");
        assert_eq!(GqlStatusCode::InternalError.code(), "50000");
    }

    #[test]
    fn status_display() {
        assert_eq!(format!("{}", GqlStatusCode::Success), "00000");
    }

    #[test]
    fn success_status_with_rows() {
        let status = GqlStatus::success(10);
        assert_eq!(status.code, GqlStatusCode::Success);
    }

    #[test]
    fn success_status_no_rows() {
        let status = GqlStatus::success(0);
        assert_eq!(status.code, GqlStatusCode::NoData);
    }

    #[test]
    fn error_status_code_mapping() {
        assert_eq!(
            GqlError::parse_error("bad").status_code(),
            GqlStatusCode::SyntaxError
        );
        assert_eq!(
            GqlError::type_error("mismatch").status_code(),
            GqlStatusCode::DataException
        );
        assert_eq!(
            GqlError::internal("bug").status_code(),
            GqlStatusCode::InternalError
        );
        assert_eq!(
            GqlError::NotFound {
                entity: "node",
                id: 1
            }
            .status_code(),
            GqlStatusCode::NoData
        );
    }

    #[test]
    fn error_display() {
        let e = GqlError::type_error("UINT64 value exceeds INT64 range");
        assert_eq!(
            format!("{e}"),
            "type error: UINT64 value exceeds INT64 range"
        );
    }

    #[test]
    fn mutation_stats_default() {
        let stats = MutationStats::default();
        assert_eq!(stats.nodes_created, 0);
        assert_eq!(stats.edges_created, 0);
        assert_eq!(stats.properties_set, 0);
    }

    #[test]
    fn graph_error_conversion() {
        let ge = selene_graph::GraphError::NodeNotFound(selene_core::NodeId(42));
        let gql_err: GqlError = ge.into();
        match gql_err {
            GqlError::NotFound { entity, id } => {
                assert_eq!(entity, "node");
                assert_eq!(id, 42);
            }
            _ => panic!("expected NotFound"),
        }
    }
}
