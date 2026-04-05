//! Time-series error types.

#[derive(Debug, thiserror::Error)]
pub enum TsError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}
