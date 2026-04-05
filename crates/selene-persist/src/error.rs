//! Persistence error types.

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CRC mismatch: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("invalid WAL magic: expected SWAL")]
    InvalidWalMagic,

    #[error("unsupported WAL version: {0}")]
    UnsupportedWalVersion(u16),

    #[error("WAL corrupted: {0}")]
    WalCorrupted(String),

    #[error("corruption: {0}")]
    Corruption(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("snapshot deserialization failed: {0}")]
    SnapshotRead(String),
}
