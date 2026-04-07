//! MCP tool response formatting.
//!
//! All MCP tool responses use minified JSON to minimize token usage for LLM
//! consumers. Indentation whitespace is pure overhead in the MCP context.

/// Format a serializable value as minified JSON for MCP tool output.
pub fn format_json(val: &impl serde::Serialize) -> String {
    serde_json::to_string(val).unwrap_or_default()
}

/// Format an inline `serde_json::Value` as minified JSON.
pub fn format_value(val: serde_json::Value) -> String {
    serde_json::to_string(&val).unwrap_or_default()
}
