use selene_core::{EdgeId, NodeId};

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("node not found: {0}")]
    NodeNotFound(NodeId),
    #[error("edge not found: {0}")]
    EdgeNotFound(EdgeId),
    #[error("schema violation: {0}")]
    SchemaViolation(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("capacity exceeded: {0}")]
    CapacityExceeded(String),
    #[error("{0}")]
    Other(String),
}
