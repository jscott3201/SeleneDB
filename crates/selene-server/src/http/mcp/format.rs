//! MCP tool response formatting.
//!
//! All MCP tool responses use minified JSON to minimize token usage for LLM
//! consumers. Indentation whitespace is pure overhead in the MCP context.
//!
//! ## Picking the right helper
//!
//! - **[`structured_result`]** when the tool's output *is* the data: lists,
//!   fetched entities, search results, query rows. The JSON value is set as
//!   both `content` (so an LLM can read it as text) and `structured_content`
//!   (so a programmatic MCP client can deserialize it directly).
//!
//! - **[`structured_text_result`]** when the tool wants to surface a human
//!   summary alongside the structured data — e.g. `gql_query` showing
//!   "Status: 00000 — successful completion" above the rows. The summary
//!   goes in `content`, the data goes in `structured_content`. This avoids
//!   forcing programmatic clients to parse the human prose.
//!
//! - Plain `Content::text(...)` for tools that return only a status string
//!   (e.g. "deleted node 42"). These have no structured payload to surface
//!   and clients should not key on the wording.

use rmcp::model::{CallToolResult, Content};

/// Format a serializable value as minified JSON for MCP tool output.
pub fn format_json(val: &impl serde::Serialize) -> String {
    serde_json::to_string(val).unwrap_or_default()
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

/// Build a tool result with a human-readable summary in `content` and the
/// raw JSON payload in `structured_content`. Use when the tool wants to
/// describe the result ("Status: 00000 — N rows") in addition to handing
/// back machine-readable data.
pub fn structured_text_result(text: impl Into<String>, val: serde_json::Value) -> CallToolResult {
    let mut result = CallToolResult::success(vec![Content::text(text.into())]);
    result.structured_content = Some(val);
    result
}
