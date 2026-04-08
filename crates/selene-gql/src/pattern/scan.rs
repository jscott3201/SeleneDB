//! LabelScan -- starting point for pattern matching.
//!
//! Resolves label expressions to RoaringBitmaps, applies auth scope
//! as an implicit AND, iterates matching nodes, and applies property
//! pushdown filters before creating bindings.

use roaring::RoaringBitmap;
use selene_core::{IStr, NodeId};
use selene_graph::SeleneGraph;

use crate::ast::expr::{CompareOp, Expr};
use crate::ast::pattern::LabelExpr;
use crate::runtime::eval;
use crate::types::binding::{Binding, BoundValue};
use crate::types::chunk::{ChunkSchema, ColumnBuilder, ColumnKind, DataChunk};
use crate::types::error::GqlError;
use crate::types::value::GqlValue;

/// Shared context for scan operations, bundling the graph reference,
/// authorization scope, property pushdown filters, and evaluation context
/// (needed to resolve `$param` bindings in inline property expressions).
pub(crate) struct ScanContext<'a> {
    pub graph: &'a SeleneGraph,
    pub scope: Option<&'a RoaringBitmap>,
    pub property_filters: &'a [PropertyFilter],
    pub eval_ctx: &'a crate::runtime::eval::EvalContext<'a>,
}

/// Resolve a LabelExpr to a RoaringBitmap of matching node IDs.
pub(crate) fn resolve_label_expr(expr: &LabelExpr, graph: &SeleneGraph) -> RoaringBitmap {
    match expr {
        LabelExpr::Name(name) if name.as_str() == "%" => {
            // Wildcard: match all nodes
            graph.all_node_bitmap()
        }
        LabelExpr::Name(name) => graph
            .label_bitmap(name.as_str())
            .cloned()
            .unwrap_or_default(),
        LabelExpr::Or(items) => items.iter().fold(RoaringBitmap::new(), |acc, e| {
            acc | resolve_label_expr(e, graph)
        }),
        LabelExpr::And(items) => {
            let mut iter = items.iter();
            match iter.next() {
                Some(first) => {
                    let initial = resolve_label_expr(first, graph);
                    iter.fold(initial, |acc, e| acc & resolve_label_expr(e, graph))
                }
                None => RoaringBitmap::new(),
            }
        }
        LabelExpr::Not(inner) => {
            let all = graph.all_node_bitmap();
            let excluded = resolve_label_expr(inner, graph);
            all - excluded
        }
        LabelExpr::Wildcard => graph.all_node_bitmap(),
        // RPQ variants: for node-level bitmap resolution, treat as their base expression
        LabelExpr::Star(inner) | LabelExpr::Plus(inner) | LabelExpr::Optional(inner) => {
            resolve_label_expr(inner, graph)
        }
        LabelExpr::Concat(items) => {
            // For node-level resolution, use the first element
            items.first().map_or(RoaringBitmap::new(), |first| {
                resolve_label_expr(first, graph)
            })
        }
    }
}

/// Extract a single label IStr from a LabelExpr, if it's a simple `:label` pattern.
/// Returns None for complex expressions (OR, AND, NOT, wildcard, RPQ).
pub(crate) fn single_label(expr: &LabelExpr) -> Option<IStr> {
    match expr {
        LabelExpr::Name(name) if name.as_str() != "%" => IStr::try_get(name.as_str()),
        _ => None,
    }
}

/// Check if a single edge label matches a LabelExpr.
/// Used during Expand when filtering edges one at a time.
#[allow(clippy::only_used_in_recursion)]
pub(crate) fn label_matches(label: IStr, expr: &LabelExpr, graph: &SeleneGraph) -> bool {
    match expr {
        LabelExpr::Name(name) => label == *name,
        LabelExpr::Or(items) => items.iter().any(|e| label_matches(label, e, graph)),
        LabelExpr::And(items) => items.iter().all(|e| label_matches(label, e, graph)),
        LabelExpr::Not(inner) => !label_matches(label, inner, graph),
        LabelExpr::Wildcard => true,
        // RPQ variants: for single-edge matching, match the inner expression
        LabelExpr::Star(inner) | LabelExpr::Plus(inner) | LabelExpr::Optional(inner) => {
            label_matches(label, inner, graph)
        }
        LabelExpr::Concat(items) => {
            // Single-edge: match if it matches the first element in the concatenation
            items
                .first()
                .is_some_and(|first| label_matches(label, first, graph))
        }
    }
}

/// Property filter for scan-time pushdown.
/// Checked before creating a binding -- avoids materializing filtered-out nodes.
#[derive(Debug, Clone)]
pub struct PropertyFilter {
    pub key: IStr,
    pub op: CompareOp,
    pub value: GqlValue,
}

impl PropertyFilter {
    /// Check if a node's properties satisfy this filter.
    pub fn matches(&self, graph: &SeleneGraph, node_id: NodeId) -> bool {
        let Some(node) = graph.get_node(node_id) else {
            return false;
        };
        let Some(prop_val) = node.properties.get(self.key).map(GqlValue::from) else {
            return false; // missing property fails filter
        };
        self.compare(&prop_val)
    }

    /// Check if an edge's properties satisfy this filter.
    pub fn matches_edge(&self, graph: &SeleneGraph, edge_id: selene_core::EdgeId) -> bool {
        let Some(edge) = graph.get_edge(edge_id) else {
            return false;
        };
        let Some(prop_val) = edge.properties.get(self.key).map(GqlValue::from) else {
            return false;
        };
        self.compare(&prop_val)
    }

    /// Core comparison logic shared by node and edge matching.
    fn compare(&self, prop_val: &GqlValue) -> bool {
        match self.op {
            CompareOp::Eq => prop_val.gql_eq(&self.value).is_true(),
            CompareOp::Neq => {
                let result = prop_val.gql_eq(&self.value);
                !result.is_true() && !result.is_unknown()
            }
            CompareOp::Lt | CompareOp::Gt | CompareOp::Lte | CompareOp::Gte => {
                match prop_val.gql_order(&self.value) {
                    Ok(ord) => match self.op {
                        CompareOp::Lt => ord == std::cmp::Ordering::Less,
                        CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                        CompareOp::Lte => ord != std::cmp::Ordering::Greater,
                        CompareOp::Gte => ord != std::cmp::Ordering::Less,
                        _ => unreachable!(),
                    },
                    Err(_) => false,
                }
            }
        }
    }
}

/// Columnar label scan producing a `DataChunk` with a single NodeIds column.
///
/// Same logic as `execute_label_scan_with_limit` (bitmap resolution, scope
/// intersection, index acceleration, property filters) but outputs directly
/// to a `ColumnBuilder` instead of allocating per-node `Binding` objects.
pub(crate) fn execute_label_scan_chunk(
    var: IStr,
    labels: Option<&LabelExpr>,
    inline_props: &[(IStr, Expr)],
    max_results: Option<usize>,
    ctx: &ScanContext<'_>,
) -> Result<DataChunk, GqlError> {
    let ScanContext {
        graph,
        scope,
        property_filters,
        eval_ctx,
    } = *ctx;
    // 1. Get candidate bitmap
    let mut bitmap = match labels {
        Some(expr) => resolve_label_expr(expr, graph),
        None => graph.all_node_bitmap(),
    };

    // 2. Apply auth scope
    if let Some(scope_bitmap) = scope {
        bitmap &= scope_bitmap;
    }

    // 2.5 Index-accelerated scan: narrow bitmap using TypedIndex lookups.
    let mut index_used = smallvec::SmallVec::<[bool; 8]>::from_elem(false, inline_props.len());
    if let Some(label_expr) = labels
        && let Some(label_istr) = single_label(label_expr)
    {
        for (i, (key, expr)) in inline_props.iter().enumerate() {
            if let Expr::Literal(val) = expr
                && let Ok(core_val) = selene_core::Value::try_from(val)
                && let Some(node_ids) = graph.property_index_lookup(label_istr, *key, &core_val)
            {
                let index_bitmap: RoaringBitmap = node_ids.iter().map(|nid| nid.0 as u32).collect();
                bitmap &= index_bitmap;
                index_used[i] = true;
            }
        }
    }
    let effective_inline: smallvec::SmallVec<[&(IStr, Expr); 8]> = inline_props
        .iter()
        .enumerate()
        .filter(|(i, _)| !index_used[*i])
        .map(|(_, p)| p)
        .collect();

    // 3. Build columnar output
    let capacity = match max_results {
        Some(max) => max.min(bitmap.len() as usize),
        None => bitmap.len() as usize,
    };
    let mut builder = ColumnBuilder::new_node_ids(capacity);
    let mut count = 0usize;

    for node_id_u32 in &bitmap {
        let node_id = NodeId(u64::from(node_id_u32));

        if !property_filters.is_empty()
            && !property_filters.iter().all(|f| f.matches(graph, node_id))
        {
            continue;
        }

        if !effective_inline.is_empty() {
            let mut matches = true;
            for (key, expected_expr) in effective_inline.iter().map(|p| (&p.0, &p.1)) {
                let owned_val;
                let expected_ref = if let Expr::Literal(val) = expected_expr {
                    val
                } else {
                    let binding = Binding::single(var, BoundValue::Node(node_id));
                    owned_val = eval::eval_expr_ctx(expected_expr, &binding, eval_ctx)?;
                    &owned_val
                };
                let actual = match graph.get_node(node_id) {
                    Some(node) => match node.properties.get(*key) {
                        Some(v) => GqlValue::from(v),
                        None => GqlValue::Null,
                    },
                    None => GqlValue::Null,
                };
                if !actual.gql_eq(expected_ref).is_true() {
                    matches = false;
                    break;
                }
            }
            if !matches {
                continue;
            }
        }

        builder.append_node_id(node_id);
        count += 1;

        if let Some(max) = max_results
            && count >= max
        {
            break;
        }
    }

    let mut schema = ChunkSchema::new();
    schema.extend(var, ColumnKind::NodeId);
    Ok(DataChunk::from_builders(vec![builder], schema, count))
}

/// Label scan with optional result limit (legacy Binding adapter).
/// Delegates to `execute_label_scan_chunk` and converts via `.to_bindings()`.
#[allow(dead_code)]
pub(crate) fn execute_label_scan_with_limit(
    var: IStr,
    labels: Option<&LabelExpr>,
    inline_props: &[(IStr, Expr)],
    max_results: Option<usize>,
    ctx: &ScanContext<'_>,
) -> Result<Vec<Binding>, GqlError> {
    let ScanContext {
        graph,
        scope,
        property_filters,
        eval_ctx,
    } = *ctx;
    // Parallel path: bypass chunk for large scans without limit (rayon needs
    // owned Bindings for work-stealing). Once pattern orchestration (T7)
    // threads DataChunks end-to-end, this parallel path can be removed.
    if max_results.is_none() {
        let mut bitmap = match labels {
            Some(expr) => resolve_label_expr(expr, graph),
            None => graph.all_node_bitmap(),
        };
        if let Some(scope_bitmap) = scope {
            bitmap &= scope_bitmap;
        }
        if bitmap.len() as usize >= crate::parallel::parallel_threshold() {
            let mut index_used =
                smallvec::SmallVec::<[bool; 8]>::from_elem(false, inline_props.len());
            if let Some(label_expr) = labels
                && let Some(label_istr) = single_label(label_expr)
            {
                for (i, (key, expr)) in inline_props.iter().enumerate() {
                    if let Expr::Literal(val) = expr
                        && let Ok(core_val) = selene_core::Value::try_from(val)
                        && let Some(node_ids) =
                            graph.property_index_lookup(label_istr, *key, &core_val)
                    {
                        let index_bitmap: RoaringBitmap =
                            node_ids.iter().map(|nid| nid.0 as u32).collect();
                        bitmap &= index_bitmap;
                        index_used[i] = true;
                    }
                }
            }
            let effective_inline: Vec<(IStr, Expr)> = inline_props
                .iter()
                .enumerate()
                .filter(|(i, _)| !index_used[*i])
                .map(|(_, p)| p.clone())
                .collect();
            return Ok(execute_label_scan_parallel(
                var,
                &bitmap,
                property_filters,
                &effective_inline,
                graph,
                eval_ctx,
            ));
        }
    }

    let chunk = execute_label_scan_chunk(var, labels, inline_props, max_results, ctx)?;
    Ok(chunk.to_bindings())
}

/// Count-only label scan -- returns the number of matching nodes without
/// materializing Binding objects. Uses bitmap cardinality when no filters
/// are present (O(1)). Otherwise iterates and counts without allocation.
pub(crate) fn count_label_scan(
    labels: Option<&LabelExpr>,
    inline_props: &[(IStr, Expr)],
    ctx: &ScanContext<'_>,
) -> Result<u64, GqlError> {
    let ScanContext {
        graph,
        scope,
        property_filters,
        eval_ctx,
    } = *ctx;
    // 1. Get candidate bitmap
    let mut bitmap = match labels {
        Some(expr) => resolve_label_expr(expr, graph),
        None => graph.all_node_bitmap(),
    };

    // 2. Apply auth scope
    if let Some(scope_bitmap) = scope {
        bitmap &= scope_bitmap;
    }

    // 3. Fast path: no filters → bitmap cardinality is O(1)
    if property_filters.is_empty() && inline_props.is_empty() {
        return Ok(bitmap.len());
    }

    // 4. Slow path: iterate and count without materializing Bindings
    let count_var = IStr::new("_count");
    let mut count = 0u64;
    for node_id_u32 in &bitmap {
        let node_id = NodeId(u64::from(node_id_u32));

        // Apply property pushdown filters
        if !property_filters.is_empty()
            && !property_filters.iter().all(|f| f.matches(graph, node_id))
        {
            continue;
        }

        // Apply inline property equality filters
        if !inline_props.is_empty() {
            let mut matches = true;
            for (key, expected_expr) in inline_props {
                let owned_val;
                let expected_ref = if let Expr::Literal(val) = expected_expr {
                    val
                } else {
                    let binding = Binding::single(count_var, BoundValue::Node(node_id));
                    owned_val = eval::eval_expr_ctx(expected_expr, &binding, eval_ctx)?;
                    &owned_val
                };
                let actual = match graph.get_node(node_id) {
                    Some(node) => match node.properties.get(*key) {
                        Some(v) => GqlValue::from(v),
                        None => GqlValue::Null,
                    },
                    None => GqlValue::Null,
                };
                if !actual.gql_eq(expected_ref).is_true() {
                    matches = false;
                    break;
                }
            }
            if !matches {
                continue;
            }
        }
        count += 1;
    }

    Ok(count)
}

/// Parallel label scan using Rayon work-stealing.
/// Only used for large bitmaps without limit pushdown.
#[allow(dead_code)]
fn execute_label_scan_parallel(
    var: IStr,
    bitmap: &RoaringBitmap,
    property_filters: &[PropertyFilter],
    inline_props: &[(IStr, Expr)],
    graph: &SeleneGraph,
    eval_ctx: &crate::runtime::eval::EvalContext<'_>,
) -> Vec<Binding> {
    use rayon::prelude::*;

    let node_ids: Vec<u32> = bitmap.iter().collect();

    node_ids
        .par_iter()
        .with_min_len(crate::parallel::parallel_threshold())
        .filter_map(|&nid_u32| {
            let node_id = NodeId(u64::from(nid_u32));

            // Property pushdown filters
            if !property_filters.is_empty()
                && !property_filters.iter().all(|f| f.matches(graph, node_id))
            {
                return None;
            }

            // Inline property matching
            if !inline_props.is_empty() {
                for (key, expected_expr) in inline_props {
                    let owned_val;
                    let expected_ref = if let Expr::Literal(val) = expected_expr {
                        val
                    } else {
                        let binding = Binding::single(var, BoundValue::Node(node_id));
                        owned_val = eval::eval_expr_ctx(expected_expr, &binding, eval_ctx).ok()?;
                        &owned_val
                    };
                    let actual = match graph.get_node(node_id) {
                        Some(node) => match node.properties.get(*key) {
                            Some(v) => GqlValue::from(v),
                            None => GqlValue::Null,
                        },
                        None => GqlValue::Null,
                    };
                    if !actual.gql_eq(expected_ref).is_true() {
                        return None;
                    }
                }
            }

            Some(Binding::single(var, BoundValue::Node(node_id)))
        })
        .collect()
}

/// Columnar index-ordered scan: iterate TypedIndex in native sort order.
///
/// Produces a DataChunk with an ordered NodeIds column. Early termination
/// after `limit` matching nodes. Returns `None` if no index exists.
pub(crate) fn execute_index_ordered_scan_chunk(
    var: IStr,
    label: IStr,
    key: IStr,
    descending: bool,
    limit: usize,
    ctx: &ScanContext<'_>,
) -> Option<DataChunk> {
    let ScanContext {
        graph,
        scope,
        property_filters,
        eval_ctx: _,
    } = *ctx;
    let index = graph.property_index_entries(label, key)?;
    let mut builder = ColumnBuilder::new_node_ids(limit);
    let mut count = 0usize;

    let collector = |nid: selene_core::NodeId| -> bool {
        if let Some(scope) = scope
            && !scope.contains(nid.0 as u32)
        {
            return true;
        }
        if !graph.contains_node(nid) {
            return true;
        }
        if property_filters.iter().any(|f| !f.matches(graph, nid)) {
            return true;
        }
        builder.append_node_id(nid);
        count += 1;
        count < limit
    };

    if descending {
        index.iter_desc(collector);
    } else {
        index.iter_asc(collector);
    }

    let mut schema = ChunkSchema::new();
    schema.extend(var, ColumnKind::NodeId);
    Some(DataChunk::from_builders(vec![builder], schema, count))
}

/// Index-ordered scan returning `Vec<Binding>` (legacy adapter).
#[allow(dead_code)]
pub(crate) fn execute_index_ordered_scan(
    var: IStr,
    label: IStr,
    key: IStr,
    descending: bool,
    limit: usize,
    ctx: &ScanContext<'_>,
) -> Option<Vec<Binding>> {
    execute_index_ordered_scan_chunk(var, label, key, descending, limit, ctx)
        .map(|chunk| chunk.to_bindings())
}

/// Columnar composite index scan: look up nodes by multi-property composite key.
///
/// Returns `None` if no composite index exists or values cannot be extracted.
pub(crate) fn execute_composite_index_scan_chunk(
    var: IStr,
    label: IStr,
    hint_keys: &[IStr],
    inline_props: &[(IStr, Expr)],
    max_results: Option<usize>,
    ctx: &ScanContext<'_>,
) -> Option<DataChunk> {
    let ScanContext {
        graph,
        scope,
        property_filters,
        eval_ctx: _,
    } = *ctx;
    let mut prop_map = std::collections::HashMap::new();
    for (key, expr) in inline_props {
        if let Expr::Literal(val) = expr
            && let Ok(core_val) = selene_core::Value::try_from(val)
        {
            prop_map.insert(*key, core_val);
        }
    }

    let values: Vec<&selene_core::Value> =
        hint_keys.iter().filter_map(|k| prop_map.get(k)).collect();

    if values.len() != hint_keys.len() {
        return None;
    }

    // Try direct lookup, then fallback to scanning all composite indexes.
    let candidates = std::iter::once(hint_keys.to_vec()).chain(
        graph
            .composite_indexes_for_label(label)
            .filter_map(|((idx_label, idx_props), _)| {
                if *idx_label == label {
                    Some(idx_props.clone())
                } else {
                    None
                }
            }),
    );

    for keys in candidates {
        let idx_values: Vec<&selene_core::Value> =
            keys.iter().filter_map(|k| prop_map.get(k)).collect();
        if idx_values.len() != keys.len() {
            continue;
        }

        if let Some(node_ids) = graph.composite_index_lookup(label, &keys, &idx_values) {
            let mut builder = ColumnBuilder::new_node_ids(node_ids.len());
            let mut count = 0usize;

            for &nid in node_ids {
                if let Some(scope) = scope
                    && !scope.contains(nid.0 as u32)
                {
                    continue;
                }
                if !graph.contains_node(nid) {
                    continue;
                }
                if !property_filters.is_empty()
                    && !property_filters.iter().all(|f| f.matches(graph, nid))
                {
                    continue;
                }
                builder.append_node_id(nid);
                count += 1;
                if let Some(max) = max_results
                    && count >= max
                {
                    break;
                }
            }

            let mut schema = ChunkSchema::new();
            schema.extend(var, ColumnKind::NodeId);
            return Some(DataChunk::from_builders(vec![builder], schema, count));
        }
    }

    None
}

/// Composite index scan returning `Vec<Binding>` (legacy adapter).
#[allow(dead_code)]
pub(crate) fn execute_composite_index_scan(
    var: IStr,
    label: IStr,
    hint_keys: &[IStr],
    inline_props: &[(IStr, Expr)],
    max_results: Option<usize>,
    ctx: &ScanContext<'_>,
) -> Option<Vec<Binding>> {
    execute_composite_index_scan_chunk(var, label, hint_keys, inline_props, max_results, ctx)
        .map(|chunk| chunk.to_bindings())
}

#[cfg(test)]
mod tests {
    use super::*;
    use selene_core::{LabelSet, PropertyMap, Value};
    use smol_str::SmolStr;

    /// Build a default EvalContext for scan tests (no parameters needed).
    fn default_eval_ctx(graph: &SeleneGraph) -> crate::runtime::eval::EvalContext<'_> {
        let registry = crate::runtime::functions::FunctionRegistry::builtins();
        crate::runtime::eval::EvalContext::new(graph, registry)
    }

    fn setup_graph() -> SeleneGraph {
        let mut g = SeleneGraph::new();
        let mut m = g.mutate();
        // Node 1: sensor, temp=72.5
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S1"))),
                (IStr::new("temp"), Value::Float(72.5)),
            ]),
        )
        .unwrap();
        // Node 2: sensor, temp=80.0
        m.create_node(
            LabelSet::from_strs(&["sensor"]),
            PropertyMap::from_pairs(vec![
                (IStr::new("name"), Value::String(SmolStr::new("S2"))),
                (IStr::new("temp"), Value::Float(80.0)),
            ]),
        )
        .unwrap();
        // Node 3: building
        m.create_node(
            LabelSet::from_strs(&["building"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("HQ")))]),
        )
        .unwrap();
        // Node 4: sensor, offline
        m.create_node(
            LabelSet::from_strs(&["sensor", "offline"]),
            PropertyMap::from_pairs(vec![(IStr::new("name"), Value::String(SmolStr::new("S3")))]),
        )
        .unwrap();
        m.commit(0).unwrap();
        g
    }

    // ── LabelExpr resolution ──

    #[test]
    fn resolve_single_label() {
        let g = setup_graph();
        let expr = LabelExpr::Name(IStr::new("sensor"));
        let bitmap = resolve_label_expr(&expr, &g);
        assert_eq!(bitmap.len(), 3); // S1, S2, S3
    }

    #[test]
    fn resolve_label_or() {
        let g = setup_graph();
        let expr = LabelExpr::Or(vec![
            LabelExpr::Name(IStr::new("sensor")),
            LabelExpr::Name(IStr::new("building")),
        ]);
        let bitmap = resolve_label_expr(&expr, &g);
        assert_eq!(bitmap.len(), 4); // all nodes
    }

    #[test]
    fn resolve_label_and() {
        let g = setup_graph();
        let expr = LabelExpr::And(vec![
            LabelExpr::Name(IStr::new("sensor")),
            LabelExpr::Name(IStr::new("offline")),
        ]);
        let bitmap = resolve_label_expr(&expr, &g);
        assert_eq!(bitmap.len(), 1); // only S3
        assert!(bitmap.contains(4));
    }

    #[test]
    fn resolve_label_not() {
        let g = setup_graph();
        // All nodes that are NOT sensors
        let expr = LabelExpr::Not(Box::new(LabelExpr::Name(IStr::new("sensor"))));
        let bitmap = resolve_label_expr(&expr, &g);
        assert_eq!(bitmap.len(), 1); // only building
        assert!(bitmap.contains(3));
    }

    #[test]
    fn resolve_label_and_not() {
        let g = setup_graph();
        // sensor AND NOT offline
        let expr = LabelExpr::And(vec![
            LabelExpr::Name(IStr::new("sensor")),
            LabelExpr::Not(Box::new(LabelExpr::Name(IStr::new("offline")))),
        ]);
        let bitmap = resolve_label_expr(&expr, &g);
        assert_eq!(bitmap.len(), 2); // S1, S2 (not S3)
    }

    #[test]
    fn resolve_unknown_label() {
        let g = setup_graph();
        let expr = LabelExpr::Name(IStr::new("nonexistent"));
        let bitmap = resolve_label_expr(&expr, &g);
        assert!(bitmap.is_empty());
    }

    // ── LabelScan ──

    #[test]
    fn scan_all_sensors() {
        let g = setup_graph();
        let eval_ctx = default_eval_ctx(&g);
        let var = IStr::new("s");
        let labels = LabelExpr::Name(IStr::new("sensor"));
        let ctx = ScanContext {
            graph: &g,
            scope: None,
            property_filters: &[],
            eval_ctx: &eval_ctx,
        };
        let bindings = execute_label_scan_with_limit(var, Some(&labels), &[], None, &ctx).unwrap();
        assert_eq!(bindings.len(), 3);
    }

    #[test]
    fn scan_with_scope() {
        let g = setup_graph();
        let eval_ctx = default_eval_ctx(&g);
        let var = IStr::new("s");
        let labels = LabelExpr::Name(IStr::new("sensor"));
        // Scope: only nodes 1 and 2
        let mut scope = RoaringBitmap::new();
        scope.insert(1);
        scope.insert(2);
        let ctx = ScanContext {
            graph: &g,
            scope: Some(&scope),
            property_filters: &[],
            eval_ctx: &eval_ctx,
        };
        let bindings = execute_label_scan_with_limit(var, Some(&labels), &[], None, &ctx).unwrap();
        assert_eq!(bindings.len(), 2);
    }

    #[test]
    fn scan_no_label_gets_all() {
        let g = setup_graph();
        let eval_ctx = default_eval_ctx(&g);
        let var = IStr::new("n");
        let ctx = ScanContext {
            graph: &g,
            scope: None,
            property_filters: &[],
            eval_ctx: &eval_ctx,
        };
        let bindings = execute_label_scan_with_limit(var, None, &[], None, &ctx).unwrap();
        assert_eq!(bindings.len(), 4); // all nodes
    }

    #[test]
    fn scan_with_property_filter() {
        let g = setup_graph();
        let eval_ctx = default_eval_ctx(&g);
        let var = IStr::new("s");
        let labels = LabelExpr::Name(IStr::new("sensor"));
        let filters = vec![PropertyFilter {
            key: IStr::new("temp"),
            op: CompareOp::Gt,
            value: GqlValue::Float(75.0),
        }];
        let ctx = ScanContext {
            graph: &g,
            scope: None,
            property_filters: &filters,
            eval_ctx: &eval_ctx,
        };
        let bindings = execute_label_scan_with_limit(var, Some(&labels), &[], None, &ctx).unwrap();
        assert_eq!(bindings.len(), 1); // only S2 (temp=80.0)
        assert_eq!(bindings[0].get_node_id(&var).unwrap(), NodeId(2));
    }

    #[test]
    fn scan_with_inline_properties() {
        let g = setup_graph();
        let eval_ctx = default_eval_ctx(&g);
        let var = IStr::new("s");
        let labels = LabelExpr::Name(IStr::new("sensor"));
        let props = vec![(
            IStr::new("name"),
            Expr::Literal(GqlValue::String(SmolStr::new("S1"))),
        )];
        let ctx = ScanContext {
            graph: &g,
            scope: None,
            property_filters: &[],
            eval_ctx: &eval_ctx,
        };
        let bindings =
            execute_label_scan_with_limit(var, Some(&labels), &props, None, &ctx).unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].get_node_id(&var).unwrap(), NodeId(1));
    }

    #[test]
    fn scan_binding_has_correct_variable() {
        let g = setup_graph();
        let eval_ctx = default_eval_ctx(&g);
        let var = IStr::new("my_node");
        let labels = LabelExpr::Name(IStr::new("building"));
        let ctx = ScanContext {
            graph: &g,
            scope: None,
            property_filters: &[],
            eval_ctx: &eval_ctx,
        };
        let bindings = execute_label_scan_with_limit(var, Some(&labels), &[], None, &ctx).unwrap();
        assert_eq!(bindings.len(), 1);
        assert!(bindings[0].contains(&var));
        assert_eq!(bindings[0].get_node_id(&var).unwrap(), NodeId(3));
    }

    // ── label_matches ──

    #[test]
    fn label_matches_simple() {
        let g = setup_graph();
        assert!(label_matches(
            IStr::new("sensor"),
            &LabelExpr::Name(IStr::new("sensor")),
            &g
        ));
        assert!(!label_matches(
            IStr::new("building"),
            &LabelExpr::Name(IStr::new("sensor")),
            &g
        ));
    }

    #[test]
    fn label_matches_or() {
        let g = setup_graph();
        let expr = LabelExpr::Or(vec![
            LabelExpr::Name(IStr::new("sensor")),
            LabelExpr::Name(IStr::new("building")),
        ]);
        assert!(label_matches(IStr::new("sensor"), &expr, &g));
        assert!(label_matches(IStr::new("building"), &expr, &g));
        assert!(!label_matches(IStr::new("other"), &expr, &g));
    }
}
