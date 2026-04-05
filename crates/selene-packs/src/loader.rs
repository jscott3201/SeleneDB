//! Schema pack error types and shared parsing utilities.

use selene_core::schema::ValueType;

#[derive(Debug, thiserror::Error)]
pub enum PackError {
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("JSON parse error: {0}")]
    Json(String),
    #[error(
        "invalid value type '{0}' -- use bool, int, float, string, timestamp, bytes, list, any"
    )]
    InvalidValueType(String),
    #[error("invalid validation mode '{0}' -- use warn or strict")]
    InvalidValidationMode(String),
    #[error("invalid default value for property '{name}': {reason}")]
    InvalidDefault { name: String, reason: String },
}

pub fn parse_value_type(s: &str) -> Result<ValueType, PackError> {
    match s.to_lowercase().as_str() {
        "bool" | "boolean" => Ok(ValueType::Bool),
        "int" | "integer" | "i64" => Ok(ValueType::Int),
        "uint" | "u64" => Ok(ValueType::UInt),
        "float" | "double" | "f64" => Ok(ValueType::Float),
        "string" | "str" | "text" => Ok(ValueType::String),
        "timestamp" | "datetime" | "zoned_datetime" => Ok(ValueType::ZonedDateTime),
        "date" => Ok(ValueType::Date),
        "local_datetime" => Ok(ValueType::LocalDateTime),
        "duration" => Ok(ValueType::Duration),
        "bytes" | "binary" => Ok(ValueType::Bytes),
        "list" | "array" => Ok(ValueType::List),
        "vector" | "embedding" => Ok(ValueType::Vector),
        "any" => Ok(ValueType::Any),
        other => Err(PackError::InvalidValueType(other.into())),
    }
}

pub fn parse_validation_mode(s: &str) -> Result<selene_core::schema::ValidationMode, PackError> {
    match s.to_lowercase().as_str() {
        "warn" => Ok(selene_core::schema::ValidationMode::Warn),
        "strict" => Ok(selene_core::schema::ValidationMode::Strict),
        other => Err(PackError::InvalidValidationMode(other.into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_value_types() {
        assert!(matches!(parse_value_type("bool"), Ok(ValueType::Bool)));
        assert!(matches!(parse_value_type("boolean"), Ok(ValueType::Bool)));
        assert!(matches!(parse_value_type("int"), Ok(ValueType::Int)));
        assert!(matches!(parse_value_type("integer"), Ok(ValueType::Int)));
        assert!(matches!(parse_value_type("float"), Ok(ValueType::Float)));
        assert!(matches!(parse_value_type("double"), Ok(ValueType::Float)));
        assert!(matches!(parse_value_type("string"), Ok(ValueType::String)));
        assert!(matches!(parse_value_type("str"), Ok(ValueType::String)));
        assert!(matches!(
            parse_value_type("timestamp"),
            Ok(ValueType::ZonedDateTime)
        ));
        assert!(matches!(parse_value_type("any"), Ok(ValueType::Any)));
        assert!(matches!(parse_value_type("vector"), Ok(ValueType::Vector)));
        assert!(matches!(
            parse_value_type("embedding"),
            Ok(ValueType::Vector)
        ));
        assert!(parse_value_type("invalid").is_err());
    }
}
