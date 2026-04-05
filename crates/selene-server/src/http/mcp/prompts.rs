//! MCP prompts -- guided workflow templates for AI agents.

use std::fmt::Write;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{PromptMessage, PromptMessageRole};
use rmcp::{prompt, prompt_router};
use schemars::JsonSchema;
use serde::Deserialize;

use super::SeleneTools;

#[derive(Deserialize, JsonSchema)]
pub(crate) struct QueryHelperParams {
    /// Natural language description of what you want to query.
    /// Example: "find all sensors with temperature above 72"
    pub intent: String,
}

#[derive(Deserialize, JsonSchema)]
pub(crate) struct ImportGuideParams {
    /// Data format: "csv", "json", or "toml".
    pub format: String,
}

#[prompt_router]
impl SeleneTools {
    pub(crate) fn build_prompt_router() -> rmcp::handler::server::router::prompt::PromptRouter<Self>
    {
        Self::prompt_router()
    }

    #[prompt(
        name = "explore-graph",
        description = "Get a structured plan to explore the graph. Returns health status, statistics, schema overview, and sample queries to run."
    )]
    async fn explore_graph(&self) -> Vec<PromptMessage> {
        let health = serde_json::to_string_pretty(&crate::ops::health::health(&self.state))
            .unwrap_or_default();
        let info = serde_json::to_string_pretty(&crate::ops::info::server_info(&self.state))
            .unwrap_or_default();

        let schema_summary = self.state.graph.read(|g| {
            let schemas = g.schema();
            let node_labels: Vec<&str> = schemas.all_node_schemas().map(|s| &*s.label).collect();
            let edge_labels: Vec<&str> = schemas.all_edge_schemas().map(|s| &*s.label).collect();
            format!(
                "Node types: {}\nEdge types: {}",
                if node_labels.is_empty() {
                    "(none)".to_string()
                } else {
                    node_labels.join(", ")
                },
                if edge_labels.is_empty() {
                    "(none)".to_string()
                } else {
                    edge_labels.join(", ")
                },
            )
        });

        vec![
            PromptMessage::new_text(
                PromptMessageRole::User,
                "I want to explore this Selene graph database. Give me an overview and suggest what to look at.",
            ),
            PromptMessage::new_text(
                PromptMessageRole::Assistant,
                format!(
                    "Here is the current state of this Selene instance:\n\n\
                     **Server Info:**\n```json\n{info}\n```\n\n\
                     **Health:**\n```json\n{health}\n```\n\n\
                     **Schema:**\n{schema_summary}\n\n\
                     **Suggested next steps:**\n\
                     1. Run `gql_query` with `MATCH (n) RETURN DISTINCT labels(n) AS labels, count(*) AS count` to see node distribution\n\
                     2. Use `graph_stats` tool for per-label breakdowns\n\
                     3. Use `list_schemas` to see full schema definitions\n\
                     4. Pick a label and run `MATCH (n:label_name) RETURN n LIMIT 5` to inspect sample nodes\n\
                     5. Use `node_edges` on an interesting node to explore its relationships"
                ),
            ),
        ]
    }

    #[prompt(
        name = "query-helper",
        description = "Generate a GQL query from a natural language description. Provides schema context so the agent can write accurate queries."
    )]
    async fn query_helper(&self, params: Parameters<QueryHelperParams>) -> Vec<PromptMessage> {
        let intent = &params.0.intent;

        let schema_context = self.state.graph.read(|g| {
            let schemas = g.schema();
            let mut ctx = String::new();
            for s in schemas.all_node_schemas() {
                let _ = write!(ctx, "Node :{} -- properties: ", &*s.label);
                let props: Vec<String> = s
                    .properties
                    .iter()
                    .map(|p| {
                        format!(
                            "{}: {:?}{}",
                            p.name,
                            p.value_type,
                            if p.required { " (required)" } else { "" }
                        )
                    })
                    .collect();
                ctx.push_str(&props.join(", "));
                ctx.push('\n');
            }
            for s in schemas.all_edge_schemas() {
                let _ = write!(ctx, "Edge :{} -- ", &*s.label);
                if !s.source_labels.is_empty() || !s.target_labels.is_empty() {
                    let src: Vec<&str> = s.source_labels.iter().map(|l| l.as_ref()).collect();
                    let tgt: Vec<&str> = s.target_labels.iter().map(|l| l.as_ref()).collect();
                    let _ = write!(ctx, "from {src:?} to {tgt:?}");
                }
                ctx.push('\n');
            }
            if ctx.is_empty() {
                ctx.push_str("(no schemas registered -- graph is schemaless)");
            }
            ctx
        });

        vec![PromptMessage::new_text(
            PromptMessageRole::User,
            format!(
                "Write a GQL query for: {intent}\n\n\
                 Available schema:\n{schema_context}\n\n\
                 GQL syntax notes:\n\
                 - Pattern matching: MATCH (n:label)-[:edge_type]->(m:label)\n\
                 - Filtering: FILTER n.property > value\n\
                 - Return: RETURN n.prop AS alias\n\
                 - Aggregation: RETURN count(*), avg(n.value)\n\
                 - Variable-length paths: MATCH (a)-[:type]->{{1,3}}(b)\n\
                 - Insert: INSERT (:label {{prop: value}})"
            ),
        )]
    }

    #[prompt(
        name = "import-guide",
        description = "Step-by-step guide for importing data into Selene. Specify the format: csv, json (React Flow), or toml (schema pack)."
    )]
    async fn import_guide(&self, params: Parameters<ImportGuideParams>) -> Vec<PromptMessage> {
        let format = params.0.format.to_lowercase();
        let guide = match format.as_str() {
            "csv" => {
                "\
**CSV Import Guide:**\n\n\
1. **Node import:** Prepare CSV with header row. Each column becomes a property. Types are auto-detected (int, float, bool, string).\n\
   - Call `csv_import` with `csv_type: \"nodes\"`, `label: \"your_label\"`, and `content: \"col1,col2\\nval1,val2\"`\n\
   - Optional: set `delimiter` for TSV or other formats\n\n\
2. **Edge import:** CSV must have `source_id`, `target_id`, `label` columns. Additional columns become edge properties.\n\
   - Call `csv_import` with `csv_type: \"edges\"` and `content: \"source_id,target_id,label\\n1,2,contains\"`\n\n\
3. **Tip:** Create schemas first (`create_schema`) to get validation and default values on import."
            }

            "json" => {
                "\
**React Flow JSON Import Guide:**\n\n\
1. Prepare a JSON object with `nodes` and `edges` arrays in React Flow format.\n\
2. Each node needs `id`, `type` (becomes label), and `data` (becomes properties).\n\
3. Each edge needs `id`, `source`, `target`, and optionally `label`.\n\
4. Call `import_reactflow` with the JSON object.\n\
5. The response includes an ID mapping from React Flow IDs to Selene numeric IDs."
            }

            "toml" => {
                "\
**Schema Pack TOML Import Guide:**\n\n\
1. Write a TOML schema pack defining types and relationships.\n\
2. Call `import_schema_pack` with the TOML content.\n\
3. Then use `create_node` or `csv_import` to add data conforming to the schemas."
            }

            _ => {
                "Supported formats: csv, json (React Flow), toml (schema pack). Please specify one of these."
            }
        };

        vec![
            PromptMessage::new_text(
                PromptMessageRole::User,
                format!("How do I import {format} data into Selene?"),
            ),
            PromptMessage::new_text(PromptMessageRole::Assistant, guide.to_string()),
        ]
    }
}
