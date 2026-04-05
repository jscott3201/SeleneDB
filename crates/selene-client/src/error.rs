//! Client error types.

use selene_wire::dto::error::ErrorResponse;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("connect error: {0}")]
    Connect(#[from] quinn::ConnectError),

    #[error("write error: {0}")]
    Write(#[from] quinn::WriteError),

    #[error("read error: {0}")]
    Read(#[from] quinn::ReadExactError),

    #[error("wire error: {0}")]
    Wire(#[from] selene_wire::WireError),

    #[error("server error ({code}): {message}")]
    Server {
        code: u16,
        message: String,
        suggestion: Option<String>,
    },

    #[error("unexpected response type: {0:?}")]
    UnexpectedResponse(selene_wire::MsgType),

    #[error("{0}")]
    Other(String),
}

impl From<ErrorResponse> for ClientError {
    fn from(e: ErrorResponse) -> Self {
        Self::Server {
            code: e.code,
            message: e.message,
            suggestion: e.suggestion,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_error_response_preserves_all_fields() {
        let resp = ErrorResponse {
            code: 404,
            message: "node not found".into(),
            suggestion: Some("check the node ID".into()),
        };
        let err = ClientError::from(resp);
        match &err {
            ClientError::Server {
                code,
                message,
                suggestion,
            } => {
                assert_eq!(*code, 404);
                assert_eq!(message, "node not found");
                assert_eq!(suggestion.as_deref(), Some("check the node ID"));
            }
            other => panic!("expected Server variant, got: {other:?}"),
        }
    }

    #[test]
    fn from_error_response_with_none_suggestion() {
        let resp = ErrorResponse {
            code: 500,
            message: "internal error".into(),
            suggestion: None,
        };
        let err = ClientError::from(resp);
        match &err {
            ClientError::Server {
                code,
                message,
                suggestion,
            } => {
                assert_eq!(*code, 500);
                assert_eq!(message, "internal error");
                assert!(suggestion.is_none());
            }
            other => panic!("expected Server variant, got: {other:?}"),
        }
    }

    #[test]
    fn server_error_display_includes_code_and_message() {
        let err = ClientError::Server {
            code: 400,
            message: "bad query syntax".into(),
            suggestion: Some("add a RETURN clause".into()),
        };
        let display = format!("{err}");
        assert!(
            display.contains("400"),
            "Display should include error code, got: {display}"
        );
        assert!(
            display.contains("bad query syntax"),
            "Display should include message, got: {display}"
        );
    }

    #[test]
    fn server_error_display_does_not_include_suggestion() {
        // The Display impl for Server uses "server error ({code}): {message}"
        // and does not print the suggestion field.
        let err = ClientError::Server {
            code: 422,
            message: "validation failed".into(),
            suggestion: Some("this should not appear in Display".into()),
        };
        let display = format!("{err}");
        assert!(
            !display.contains("this should not appear"),
            "Display should not include suggestion, got: {display}"
        );
    }

    #[test]
    fn other_error_display() {
        let err = ClientError::Other("something went wrong".into());
        let display = format!("{err}");
        assert_eq!(display, "something went wrong");
    }

    #[test]
    fn unexpected_response_display() {
        let err = ClientError::UnexpectedResponse(selene_wire::MsgType::Health);
        let display = format!("{err}");
        assert!(
            display.contains("Health"),
            "Display should include the MsgType debug name, got: {display}"
        );
        assert!(
            display.contains("unexpected response type"),
            "Display should include the variant prefix, got: {display}"
        );
    }

    #[test]
    fn wire_error_from_conversion() {
        let wire_err = selene_wire::WireError::PayloadTooLarge(999_999);
        let err = ClientError::from(wire_err);
        let display = format!("{err}");
        assert!(
            display.contains("wire error"),
            "Display should include 'wire error' prefix, got: {display}"
        );
        assert!(
            display.contains("999999") || display.contains("999_999"),
            "Display should include the payload size, got: {display}"
        );
    }
}
