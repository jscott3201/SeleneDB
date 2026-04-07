//! Execution plan representation -- the output of the planner.

use std::sync::Arc;

use selene_core::IStr;

use crate::ast::expr::{Expr, ProcedureCall};
use crate::ast::mutation::MutationOp;
use crate::ast::pattern::{EdgeDirection, LabelExpr};
use crate::ast::statement::OrderTerm;
use crate::types::value::GqlValue;

/// Index-ordered scan parameters -- produced by the optimizer when
/// ORDER BY + LIMIT can be served directly from a BTreeMap property index.
#[derive(Debug, Clone)]
pub struct IndexOrder {
    /// Property key to sort by (must match a BTreeMap property index).
    pub key: IStr,
    /// Sort in descending order.
    pub descending: bool,
    /// Maximum results to return from the index scan.
    pub limit: usize,
}

/// Range index hint -- produced by the optimizer when a range predicate
/// (>, <, >=, <=) can be served by a BTreeMap property index range scan.
#[derive(Debug, Clone)]
pub struct RangeIndexHint {
    /// Property key to scan.
    pub key: IStr,
    /// Lower bound: (value, inclusive). None means unbounded below.
    pub lower: Option<(GqlValue, bool)>,
    /// Upper bound: (value, inclusive). None means unbounded above.
    pub upper: Option<(GqlValue, bool)>,
}

/// IN-list hint -- produced by the optimizer when an IN-list with all literal
/// values can be served by multi-probe index lookups at scan time.
#[derive(Debug, Clone)]
pub struct InListHint {
    /// Property key to probe.
    pub key: IStr,
    /// Literal values to look up in the index.
    pub values: Vec<GqlValue>,
}

/// Complete execution plan for a GQL statement.
#[derive(Debug)]
pub struct ExecutionPlan {
    /// Phase 1: pattern matching (materialized).
    pub pattern_ops: Vec<PatternOp>,
    /// Phase 2: pipeline stages (streaming where possible).
    pub pipeline: Vec<PipelineOp>,
    /// Mutation operations (executed after pattern, before pipeline).
    pub mutations: Vec<MutationOp>,
    /// Output schema (derived from RETURN at plan time).
    pub output_schema: Arc<arrow::datatypes::Schema>,
    /// When true, the query is a pure `MATCH (...) RETURN count(*)` with no
    /// GROUP BY, HAVING, DISTINCT, or other projections. The executor can skip
    /// binding materialization and return bitmap cardinality directly.
    pub count_only: bool,
}

/// Pattern matching operation -- produces bindings from graph traversal.
#[derive(Debug)]
pub enum PatternOp {
    /// Scan nodes by label expression.
    LabelScan {
        var: IStr,
        labels: Option<LabelExpr>,
        inline_props: Vec<(IStr, Expr)>,
        /// Range/inequality property filters pushed down from pipeline FILTER.
        property_filters: Vec<crate::pattern::scan::PropertyFilter>,
        /// If set, use BTreeMap property index for sorted scan (index ORDER BY).
        /// Produced by `IndexOrderRule` optimizer. Falls back to regular scan
        /// at execution time if no index exists for (label, key).
        index_order: Option<IndexOrder>,
        /// If set, try composite index lookup at scan time.
        /// Contains the ordered property keys that form the composite index.
        /// Produced by `CompositeIndexLookupRule` optimizer.
        composite_index_keys: Option<Vec<IStr>>,
        /// If set, use BTreeMap range() for index-accelerated range filtering.
        /// Produced by `RangeIndexScanRule`. Falls back to sequential
        /// property_filters at execution time if no TypedIndex exists.
        range_index_hint: Option<RangeIndexHint>,
        /// If set, use multi-probe index lookups for IN-list filtering.
        /// Produced by `InListOptimizationRule`.
        in_list_hint: Option<InListHint>,
    },

    /// Single-hop edge expansion from bound source.
    Expand {
        source_var: IStr,
        edge_var: Option<IStr>,
        target_var: IStr,
        edge_labels: Option<LabelExpr>,
        target_labels: Option<LabelExpr>,
        direction: EdgeDirection,
        /// Property filters on the target node, applied before binding clone.
        /// Produced by `ExpandFilterPushdownRule`.
        target_property_filters: Vec<crate::pattern::scan::PropertyFilter>,
        /// Property filters on the edge, applied before binding clone.
        /// Produced by `ExpandFilterPushdownRule`.
        edge_property_filters: Vec<crate::pattern::scan::PropertyFilter>,
    },

    /// Variable-length expansion with depth bounds.
    VarExpand {
        source_var: IStr,
        edge_var: Option<IStr>,
        target_var: IStr,
        edge_labels: Option<LabelExpr>,
        target_labels: Option<LabelExpr>,
        direction: EdgeDirection,
        min_hops: u32,
        max_hops: Option<u32>,
        trail: bool,
        acyclic: bool,
        simple: bool,
        /// ANY SHORTEST: emit only one path at min depth. ALL SHORTEST: emit all at min depth.
        shortest: Option<crate::ast::pattern::PathSelector>,
        path_var: Option<IStr>,
    },

    /// OPTIONAL pattern -- left-outer-join semantics.
    /// If inner ops produce no results for an input binding,
    /// emit one result with NULL-filled variables.
    Optional {
        /// The inner pattern ops to execute optionally.
        inner_ops: Vec<PatternOp>,
        /// Variables introduced by the optional pattern (NULL-filled on no match).
        new_vars: Vec<IStr>,
        /// Variables shared with outer scope (used to correlate inner results).
        join_vars: Vec<IStr>,
    },

    /// Join two pattern results on shared variables.
    Join {
        /// Index of the right-side pattern's first op in the ops list.
        right_start: usize,
        right_end: usize,
        join_vars: Vec<IStr>,
    },

    /// Cycle closure -- the last edge in a cyclic pattern points back
    /// to an already-bound variable. Filter bindings where the expand
    /// target equals the bound variable.
    CycleJoin {
        /// Variable already bound (the cycle target).
        bound_var: IStr,
        /// Source variable for the closing edge.
        source_var: IStr,
        /// Edge labels to match on the closing edge.
        edge_labels: Option<LabelExpr>,
        /// Direction of the closing edge.
        direction: EdgeDirection,
    },

    /// DIFFERENT EDGES match mode -- filter bindings where any two
    /// edge variables reference the same edge.
    DifferentEdgesFilter {
        /// Edge variable names that must all be distinct.
        edge_vars: Vec<IStr>,
    },

    /// Filter applied between pattern ops (interleaved by `FilterInterleavingRule`).
    /// Same semantics as `PipelineOp::Filter` but positioned within the pattern phase
    /// to reduce intermediate result sizes before subsequent Expand operations.
    IntermediateFilter { predicate: Expr },

    /// Worst-case optimal multi-way join for cyclic patterns.
    ///
    /// Replaces a sequence of LabelScan + Expand + Expand + CycleJoin
    /// when the optimizer determines the cyclic pattern benefits from
    /// intersection-based execution. Uses sorted merge intersection on
    /// CSR neighbor lists to bound intermediate results by the AGM
    /// fractional edge cover (O(m^1.5) for triangles instead of O(m^2)).
    WcoJoin {
        /// The anchor scan variable (scan root for the first relation).
        scan_var: IStr,
        /// Label filter for the scan root.
        scan_labels: Option<LabelExpr>,
        /// Property filters on the scan root.
        scan_property_filters: Vec<crate::pattern::scan::PropertyFilter>,
        /// The relations forming the cycle (3 for triangles).
        relations: Vec<WcoRelation>,
    },
}

/// One edge relation in a WCO multi-way join.
#[derive(Debug)]
pub struct WcoRelation {
    pub source_var: IStr,
    pub edge_var: Option<IStr>,
    pub target_var: IStr,
    pub edge_label: Option<IStr>,
    pub target_labels: Option<LabelExpr>,
    pub direction: EdgeDirection,
    pub target_property_filters: Vec<crate::pattern::scan::PropertyFilter>,
}

impl PatternOp {
    /// Collect variables introduced by this operation into `set`.
    pub fn collect_vars(&self, set: &mut std::collections::HashSet<IStr>) {
        match self {
            PatternOp::LabelScan { var, .. } => {
                set.insert(*var);
            }
            PatternOp::Expand {
                edge_var,
                target_var,
                ..
            } => {
                if let Some(ev) = edge_var {
                    set.insert(*ev);
                }
                set.insert(*target_var);
            }
            PatternOp::VarExpand {
                edge_var,
                target_var,
                path_var,
                ..
            } => {
                if let Some(ev) = edge_var {
                    set.insert(*ev);
                }
                set.insert(*target_var);
                if let Some(pv) = path_var {
                    set.insert(*pv);
                }
            }
            PatternOp::Optional {
                new_vars,
                inner_ops,
                ..
            } => {
                for v in new_vars {
                    set.insert(*v);
                }
                for op in inner_ops {
                    op.collect_vars(set);
                }
            }
            PatternOp::WcoJoin {
                scan_var,
                relations,
                ..
            } => {
                set.insert(*scan_var);
                for rel in relations {
                    if let Some(ev) = rel.edge_var {
                        set.insert(ev);
                    }
                    set.insert(rel.target_var);
                }
            }
            PatternOp::Join { .. }
            | PatternOp::CycleJoin { .. }
            | PatternOp::DifferentEdgesFilter { .. }
            | PatternOp::IntermediateFilter { .. } => {}
        }
    }
}

/// Collect all variables bound by a sequence of pattern ops.
pub(crate) fn collect_bound_vars(ops: &[PatternOp]) -> std::collections::HashSet<IStr> {
    let mut vars = std::collections::HashSet::new();
    for op in ops {
        op.collect_vars(&mut vars);
    }
    vars
}

/// Pipeline operation -- processes bindings after pattern matching.
#[derive(Debug)]
pub enum PipelineOp {
    /// LET var = expr -- streaming map.
    Let { bindings: Vec<(IStr, Expr)> },

    /// FILTER predicate -- streaming filter (three-valued).
    Filter { predicate: Expr },

    /// ORDER BY -- pipeline breaker (must materialize + sort).
    Sort { terms: Vec<OrderTerm> },

    /// OFFSET n or $param -- streaming skip.
    Offset {
        value: crate::ast::statement::LimitValue,
    },

    /// LIMIT n or $param -- streaming truncate.
    Limit {
        value: crate::ast::statement::LimitValue,
    },

    /// RETURN -- terminal projection with optional GROUP BY + HAVING.
    Return {
        projections: Vec<PlannedProjection>,
        group_by: Vec<Expr>,
        distinct: bool,
        having: Option<Expr>,
        /// RETURN * -- project all bound variables (projections will be empty).
        all: bool,
    },

    /// WITH -- intermediate scope-resetting projection.
    /// Same semantics as Return (project + aggregate) but does not terminate the pipeline.
    /// Only projected variables survive into subsequent pipeline stages.
    With {
        projections: Vec<PlannedProjection>,
        group_by: Vec<Expr>,
        distinct: bool,
        having: Option<Expr>,
        where_filter: Option<Expr>,
    },

    /// CALL procedure -- inline procedure execution.
    Call { procedure: ProcedureCall },

    /// ORDER BY + LIMIT fused into bounded heap: O(N log K) vs O(N log N).
    TopK {
        terms: Vec<OrderTerm>,
        limit: crate::ast::statement::LimitValue,
    },

    /// CALL { subquery } -- inline subquery per input row.
    Subquery { plan: Box<ExecutionPlan> },

    /// FOR var IN expr -- unwind list to rows.
    For { var: IStr, list_expr: Expr },

    /// MATCH following a WITH -- correlated pattern expansion seeded by prior bindings.
    ///
    /// Generated when a MATCH appears after a WITH clause. At execution time, the
    /// pattern ops are run per-binding using the WITH output as seeds. Variables
    /// already present in the seed binding short-circuit their LabelScan (correlated
    /// path). An optional WHERE filter from the MATCH clause is applied after expansion.
    NestedMatch {
        pattern_ops: Vec<PatternOp>,
        where_filter: Option<Expr>,
    },

    /// Read materialized view state (produced by MATCH VIEW ... YIELD).
    ViewScan {
        view_name: IStr,
        yields: Vec<(IStr, Option<IStr>)>, // (column_name, alias)
        yield_star: bool,
    },
}

/// A planned RETURN projection with resolved alias.
#[derive(Debug, Clone)]
pub struct PlannedProjection {
    pub expr: Expr,
    pub alias: IStr,
}
