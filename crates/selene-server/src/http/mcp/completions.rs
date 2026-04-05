//! MCP completions -- autocompletion for prompts, resources, and tool arguments.

use rmcp::model::{CompleteRequestParams, CompleteResult, CompletionInfo, Reference};
use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, RoleServer};

use super::SeleneTools;

/// Known prompt names exposed by this server.
const PROMPT_NAMES: &[&str] = &["explore-graph", "query-helper", "import-guide"];

/// Known static resource URIs exposed by this server.
const RESOURCE_URIS: &[&str] = &[
    "selene://health",
    "selene://stats",
    "selene://schemas",
    "selene://info",
];

impl SeleneTools {
    /// Handle `completion/complete` requests.
    ///
    /// Provides autocompletion for:
    /// - Prompt names (partial match against the 3 known prompts)
    /// - Resource URIs (partial match against static resources)
    /// - Tool arguments named `label` or `labels` (returns graph label names)
    pub(crate) async fn handle_complete(
        &self,
        request: CompleteRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, McpError> {
        let partial = &request.argument.value;
        let arg_name = &request.argument.name;

        let values = match &request.r#ref {
            Reference::Prompt(_) => complete_prompt_names(partial),
            Reference::Resource(_) => complete_resource_uris(partial),
        };

        // If no ref-specific completions and the argument looks like a label
        // parameter, provide label completions from the graph.
        let values = if values.is_empty() && is_label_argument(arg_name) {
            self.complete_labels(partial)
        } else {
            values
        };

        let total = values.len() as u32;
        let info = CompletionInfo {
            values,
            total: Some(total),
            has_more: Some(false),
        };
        Ok(CompleteResult::new(info))
    }

    /// Return graph label names that start with the given prefix.
    fn complete_labels(&self, partial: &str) -> Vec<String> {
        let lower = partial.to_lowercase();
        self.state.graph.read(|g| {
            let mut labels: Vec<String> = g
                .node_label_counts()
                .keys()
                .map(|istr| istr.as_str().to_string())
                .filter(|name| name.to_lowercase().starts_with(&lower))
                .collect();
            // Also include edge labels.
            for istr in g.edge_label_counts().keys() {
                let name = istr.as_str().to_string();
                if name.to_lowercase().starts_with(&lower) && !labels.contains(&name) {
                    labels.push(name);
                }
            }
            labels.sort();
            labels.truncate(CompletionInfo::MAX_VALUES);
            labels
        })
    }
}

/// Match partial text against known prompt names.
fn complete_prompt_names(partial: &str) -> Vec<String> {
    let lower = partial.to_lowercase();
    PROMPT_NAMES
        .iter()
        .filter(|name| name.to_lowercase().starts_with(&lower))
        .map(|s| (*s).to_string())
        .collect()
}

/// Match partial text against known static resource URIs.
fn complete_resource_uris(partial: &str) -> Vec<String> {
    let lower = partial.to_lowercase();
    RESOURCE_URIS
        .iter()
        .filter(|uri| uri.to_lowercase().starts_with(&lower))
        .map(|s| (*s).to_string())
        .collect()
}

/// Check whether an argument name refers to a label parameter.
fn is_label_argument(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower == "label" || lower == "labels"
}
