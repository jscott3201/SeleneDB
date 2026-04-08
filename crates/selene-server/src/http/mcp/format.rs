//! MCP tool response formatting.
//!
//! All MCP tool responses use minified JSON to minimize token usage for LLM
//! consumers. Indentation whitespace is pure overhead in the MCP context.
//!
//! Tools that return structured data provide both `content` (text for LLMs)
//! and `structured_content` (JSON for programmatic access by MCP clients).

use rmcp::model::{CallToolResult, Content};

/// Format a serializable value as minified JSON for MCP tool output.
pub fn format_json(val: &impl serde::Serialize) -> String {
    serde_json::to_string(val).unwrap_or_default()
}

/// Format an inline `serde_json::Value` as minified JSON.
pub fn format_value(val: serde_json::Value) -> String {
    serde_json::to_string(&val).unwrap_or_default()
}

/// Build a tool result with both text content (for LLMs) and structured
/// content (for programmatic MCP clients). The structured content is the
/// raw JSON value; the text content is the minified JSON string.
pub fn structured_result(val: serde_json::Value) -> CallToolResult {
    let text = serde_json::to_string(&val).unwrap_or_default();
    let mut result = CallToolResult::success(vec![Content::text(text)]);
    result.structured_content = Some(val);
    result
}
