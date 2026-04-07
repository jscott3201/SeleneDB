//! Procedure catalog introspection: graph.procedures.

use selene_core::IStr;
use selene_graph::SeleneGraph;
use selene_ts::HotTier;
use smallvec::smallvec;
use smol_str::SmolStr;

use super::{Procedure, ProcedureRow, ProcedureSignature, YieldColumn};
use crate::types::error::GqlError;
use crate::types::value::{GqlType, GqlValue};

/// Snapshot of a single procedure's metadata for introspection.
struct ProcedureMetadata {
    name: String,
    params: String,
    yields: String,
}

/// Lists all registered procedures with their parameter and yield signatures.
pub struct GraphProcedures {
    entries: Vec<ProcedureMetadata>,
}

impl GraphProcedures {
    /// Build from the current registry contents. Call after all other procedures
    /// are registered so the snapshot is complete.
    pub fn from_registry(registry: &super::ProcedureRegistry) -> Self {
        let mut entries: Vec<ProcedureMetadata> = registry
            .iter()
            .map(|(_, proc)| {
                let sig = proc.signature();
                let params = sig
                    .params
                    .iter()
                    .map(|p| format!("{}: {}", p.name, p.typ))
                    .collect::<Vec<_>>()
                    .join(", ");
                let yields = sig
                    .yields
                    .iter()
                    .map(|y| format!("{}: {}", y.name, y.typ))
                    .collect::<Vec<_>>()
                    .join(", ");
                ProcedureMetadata {
                    name: proc.name().to_string(),
                    params,
                    yields,
                }
            })
            .collect();
        // Include ourselves in the catalog.
        entries.push(ProcedureMetadata {
            name: "graph.procedures".to_string(),
            params: String::new(),
            yields: "name: STRING, params: STRING, yields: STRING".to_string(),
        });
        // Sort by name for deterministic output.
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Self { entries }
    }
}

impl Procedure for GraphProcedures {
    fn name(&self) -> &'static str {
        "graph.procedures"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            params: vec![],
            yields: vec![
                YieldColumn {
                    name: "name",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "params",
                    typ: GqlType::String,
                },
                YieldColumn {
                    name: "yields",
                    typ: GqlType::String,
                },
            ],
        }
    }

    fn execute(
        &self,
        _args: &[GqlValue],
        _graph: &SeleneGraph,
        _ht: Option<&HotTier>,
        _scope: Option<&roaring::RoaringBitmap>,
    ) -> Result<Vec<ProcedureRow>, GqlError> {
        Ok(self
            .entries
            .iter()
            .map(|entry| {
                smallvec![
                    (
                        IStr::new("name"),
                        GqlValue::String(SmolStr::new(&entry.name))
                    ),
                    (
                        IStr::new("params"),
                        GqlValue::String(SmolStr::new(&entry.params))
                    ),
                    (
                        IStr::new("yields"),
                        GqlValue::String(SmolStr::new(&entry.yields))
                    ),
                ]
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::procedures::ProcedureRegistry;
    use selene_graph::SeleneGraph;

    #[test]
    fn procedures_lists_itself() {
        let reg = ProcedureRegistry::with_builtins();
        let names: Vec<&str> = reg.iter().map(|(_, p)| p.name()).collect();
        assert!(
            names.contains(&"graph.procedures"),
            "graph.procedures should be registered"
        );
    }

    #[test]
    fn procedures_returns_rows_with_correct_columns() {
        let reg = ProcedureRegistry::with_builtins();
        let proc = reg
            .get(&IStr::new("graph.procedures"))
            .expect("graph.procedures not registered");
        let graph = SeleneGraph::new();
        let rows = proc.execute(&[], &graph, None, None).unwrap();

        assert!(!rows.is_empty(), "should return at least one row");

        // Every row should have three columns: name, params, yields
        for row in &rows {
            assert_eq!(row.len(), 3);
            assert_eq!(row[0].0.as_str(), "name");
            assert_eq!(row[1].0.as_str(), "params");
            assert_eq!(row[2].0.as_str(), "yields");
        }
    }

    #[test]
    fn procedures_includes_known_builtins() {
        let reg = ProcedureRegistry::with_builtins();
        let proc = reg
            .get(&IStr::new("graph.procedures"))
            .expect("graph.procedures not registered");
        let graph = SeleneGraph::new();
        let rows = proc.execute(&[], &graph, None, None).unwrap();

        let names: Vec<&str> = rows.iter().map(|r| r[0].1.as_str().unwrap()).collect();

        assert!(names.contains(&"graph.labels"), "missing graph.labels");
        assert!(
            names.contains(&"graph.node_count"),
            "missing graph.node_count"
        );
        assert!(
            names.contains(&"graph.procedures"),
            "missing graph.procedures"
        );
    }

    #[test]
    fn procedures_sorted_by_name() {
        let reg = ProcedureRegistry::with_builtins();
        let proc = reg
            .get(&IStr::new("graph.procedures"))
            .expect("graph.procedures not registered");
        let graph = SeleneGraph::new();
        let rows = proc.execute(&[], &graph, None, None).unwrap();

        let names: Vec<&str> = rows.iter().map(|r| r[0].1.as_str().unwrap()).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted, "rows should be sorted by name");
    }

    #[test]
    fn procedures_params_and_yields_format() {
        let reg = ProcedureRegistry::with_builtins();
        let proc = reg
            .get(&IStr::new("graph.procedures"))
            .expect("graph.procedures not registered");
        let graph = SeleneGraph::new();
        let rows = proc.execute(&[], &graph, None, None).unwrap();

        // Find graph.schema which has params and multiple yield columns
        let schema_row = rows
            .iter()
            .find(|r| r[0].1.as_str().unwrap() == "graph.schema")
            .expect("graph.schema not found");

        let params = schema_row[1].1.as_str().unwrap();
        assert!(
            params.contains("label"),
            "graph.schema params should mention 'label'"
        );

        let yields = schema_row[2].1.as_str().unwrap();
        assert!(
            yields.contains("property"),
            "graph.schema yields should mention 'property'"
        );
    }
}
