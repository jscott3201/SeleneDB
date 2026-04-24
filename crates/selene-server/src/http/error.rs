//! Map OpError to HTTP responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};

use crate::ops::OpError;

/// Axum-compatible error wrapper around OpError.
pub struct HttpError(pub OpError);

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            OpError::NotFound { entity, id } => {
                (StatusCode::NOT_FOUND, format!("{entity} {id} not found"))
            }
            OpError::AuthDenied => (StatusCode::FORBIDDEN, "access denied".into()),
            OpError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            OpError::SchemaViolation(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.clone()),
            OpError::InvalidRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            OpError::QueryError(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            OpError::Internal(msg) => {
                tracing::error!(detail = %msg, "internal server error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
            OpError::ReadOnly => (StatusCode::METHOD_NOT_ALLOWED, "read-only replica".into()),
            OpError::ResourcesExhausted(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
            OpError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

impl From<OpError> for HttpError {
    fn from(e: OpError) -> Self {
        Self(e)
    }
}

impl HttpError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self(OpError::InvalidRequest(msg.into()))
    }

    pub fn from_graph_error(e: selene_graph::error::GraphError) -> Self {
        Self(crate::ops::graph_err(e))
    }
}
