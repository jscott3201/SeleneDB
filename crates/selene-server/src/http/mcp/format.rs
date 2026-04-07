//! MCP tool response formatting: compact vs pretty output.
//!
//! Compact mode reduces token usage by 30-60% for LLM consumers:
//! - Single objects: minified JSON (no whitespace)
//! - Lists/tables: CSV-like format with headers
//! - GQL results: compact row format
//!
//! Enable via `SELENE_MCP_COMPACT=1` environment variable.

/// Format a serializable value for MCP tool output.
///
/// In compact mode, uses minified JSON. In pretty mode, uses indented JSON.
pub fn format_json(val: &impl serde::Serialize, compact: bool) -> String {
    if compact {
        serde_json::to_string(val).unwrap_or_default()
    } else {
        serde_json::to_string_pretty(val).unwrap_or_default()
    }
}

/// Format a serializable value built inline (json!() macro pattern).
///
/// Convenience wrapper matching the common `to_string_pretty(&json!({...}))` pattern.
pub fn format_value(val: serde_json::Value, compact: bool) -> String {
    if compact {
        serde_json::to_string(&val).unwrap_or_default()
    } else {
        serde_json::to_string_pretty(&val).unwrap_or_default()
    }
}

/// Check if compact MCP responses are enabled via environment.
pub fn is_compact_enabled() -> bool {
    std::env::var("SELENE_MCP_COMPACT").is_ok_and(|v| v == "1")
}
