//! Top-level GQL execution: wires parser, planner, pattern executor,
//! pipeline executor, and mutation executor together.

use std::collections::HashMap;
use std::sync::Arc;

use roaring::RoaringBitmap;
use selene_core::IStr;
use selene_graph::{SeleneGraph, SharedGraph};

/// Query parameter map binding $param names to GqlValues.
pub type ParameterMap = HashMap<IStr, GqlValue>;

use crate::ast::expr::Expr;
use crate::ast::statement::GqlStatement;
use crate::parser::parse_statement;
use crate::pattern::join;
use crate::pipeline::stages;
use crate::planner;
use crate::planner::plan::*;
use crate::runtime::eval::{self, EvalContext};
use crate::runtime::functions::FunctionRegistry;
use crate::runtime::procedures::ProcedureRegistry;
use crate::types::binding::Binding;
use crate::types::chunk::DataChunk;
use crate::types::error::{GqlError, GqlStatus, MutationStats};
use crate::types::result::GqlResult;
use crate::types::value::GqlValue;

/// Rename Arrow batch schemas from internal (uppercased) names to display
/// (original-case) names. This is a zero-copy operation at the data level —
/// only the schema metadata changes.
fn apply_display_schema(
    schema: Arc<arrow::datatypes::Schema>,
    batches: Vec<arrow::record_batch::RecordBatch>,
    display_schema: &Arc<arrow::datatypes::Schema>,
) -> (
    Arc<arrow::datatypes::Schema>,
    Vec<arrow::record_batch::RecordBatch>,
) {
    // Skip rename if schemas already match or have different field counts
    // (different counts can happen with RETURN */YIELD * expansion at runtime).
    if schema.fields().len() != display_schema.fields().len()
        || schema.fields() == display_schema.fields()
    {
        return (schema, batches);
    }
    // Merge: take display names from the display schema but preserve data types
    // from the original schema (expression-level inference may default to Utf8
    // for property access, while the actual columns carry the real types).
    let merged_fields: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .zip(display_schema.fields())
        .map(|(orig, disp)| {
            arrow::datatypes::Field::new(disp.name(), orig.data_type().clone(), orig.is_nullable())
        })
        .collect();
    let merged_schema = Arc::new(arrow::datatypes::Schema::new(merged_fields));
    let new_batches = batches
        .into_iter()
        .map(|b| {
            arrow::record_batch::RecordBatch::try_new(merged_schema.clone(), b.columns().to_vec())
                .unwrap_or(b)
        })
        .collect();
    (merged_schema, new_batches)
}

/// Execute a GQL mutation with auto-commit.
///
/// All mutations from the statement are wrapped in a single `SharedGraph::write()`
/// call for atomicity. If any mutation fails, the entire batch rolls back.
fn execute_mut(
    gql: &str,
    shared: &SharedGraph,
    hot_tier: Option<&selene_ts::HotTier>,
    scope: Option<&RoaringBitmap>,
    parameters: Option<&ParameterMap>,
) -> Result<GqlResult, GqlError> {
    let stmt = parse_statement(gql)?;
    let snapshot = shared.load_snapshot();
    let registry = ProcedureRegistry::builtins();
    let func_registry = FunctionRegistry::builtins();
    let mut ctx = EvalContext::new(&snapshot, func_registry);
    if let Some(params) = parameters {
        ctx = ctx.with_parameters(params);
    }

    match &stmt {
        GqlStatement::Query(pipeline) => {
            let plan = planner::plan_query(pipeline, &snapshot)?;
            execute_plan(
                &plan,
                &snapshot,
                scope,
                hot_tier,
                Some(registry),
                parameters,
            )
        }
        GqlStatement::Chained { blocks } => {
            let mut result = None;
            for block in blocks {
                let plan = planner::plan_query(block, &snapshot)?;
                result = Some(execute_plan(
                    &plan,
                    &snapshot,
                    scope,
                    hot_tier,
                    Some(registry),
                    parameters,
                )?);
            }
            result.ok_or_else(|| GqlError::internal("empty chained query"))
        }
        GqlStatement::Mutate(mp) => {
            let plan = planner::plan_mutation(mp, &snapshot)?;

            // Execute pattern + pre-mutation pipeline against snapshot
            let mut bindings =
                execute_pattern_ops_with_eval_ctx(&plan.pattern_ops, &snapshot, scope, &ctx)?;
            for op in &plan.pipeline {
                match op {
                    PipelineOp::Return { .. } => break,
                    PipelineOp::Call { procedure: call } => {
                        bindings = execute_call(
                            bindings,
                            call,
                            &snapshot,
                            hot_tier,
                            Some(registry),
                            scope,
                            Some(&ctx),
                        )?;
                    }
                    _ => {
                        bindings = stages::execute_pipeline_op(op, bindings, &ctx)?;
                    }
                }
            }

            // Execute all mutations atomically in a single write()
            let mut mutation_stats = MutationStats::default();
            let mut mutation_changes = Vec::new();
            if !plan.mutations.is_empty() {
                let (stats, changes) = execute_mutations_write(
                    shared,
                    &plan.mutations,
                    &mut bindings,
                    &snapshot,
                    scope,
                    parameters,
                )?;
                mutation_stats = stats;
                mutation_changes = changes;
            }

            // Release the pre-mutation snapshot before loading the post-mutation one.
            // EvalContext borrows snapshot, so drop it first to end the borrow.
            #[allow(clippy::drop_non_drop)]
            drop(ctx);
            drop(snapshot);

            // Execute post-mutation pipeline (RETURN)
            let post_snapshot = shared.load_snapshot();
            let post_registry = FunctionRegistry::builtins();
            let post_ctx = EvalContext::new(&post_snapshot, post_registry);
            for op in &plan.pipeline {
                if matches!(op, PipelineOp::Return { .. })
                    || matches!(op, PipelineOp::Sort { .. })
                    || matches!(op, PipelineOp::Offset { .. })
                    || matches!(op, PipelineOp::Limit { .. })
                {
                    bindings = stages::execute_pipeline_op(op, bindings, &post_ctx)?;
                }
            }

            let row_count = bindings.len();
            let aliases: Vec<IStr> = plan
                .output_schema
                .fields()
                .iter()
                .map(|f| IStr::new(f.name()))
                .collect();
            let schema = infer_schema_from_bindings(&bindings, &aliases);
            let batches = materialize_to_arrow(&bindings, &schema)?;
            let (schema, batches) = apply_display_schema(schema, batches, &plan.display_schema);

            Ok(GqlResult {
                schema,
                batches,
                status: GqlStatus::success(row_count),
                mutations: mutation_stats,
                profile: None,
                changes: mutation_changes,
            })
        }
        GqlStatement::Composite { first, rest } => {
            let first_plan = planner::plan_query(first, &snapshot)?;
            let mut result = execute_plan(
                &first_plan,
                &snapshot,
                scope,
                hot_tier,
                Some(registry),
                parameters,
            )?;
            for (op, pipeline) in rest {
                let plan = planner::plan_query(pipeline, &snapshot)?;
                let other = execute_plan(
                    &plan,
                    &snapshot,
                    scope,
                    hot_tier,
                    Some(registry),
                    parameters,
                )?;
                result = apply_set_op(*op, result, other);
            }
            let rows = result.row_count();
            result.status = GqlStatus::success(rows);
            Ok(result)
        }
        GqlStatement::StartTransaction | GqlStatement::Commit | GqlStatement::Rollback => {
            Err(GqlError::Internal {
                message: "multi-statement transactions are not supported over the wire protocol; \
                          use single-statement auto-commit mutations or the Rust API \
                          (SharedGraph::begin_transaction) for multi-statement atomicity"
                    .into(),
            })
        }
        GqlStatement::CreateTrigger(stmt) => ddl::create_trigger(shared, stmt),
        GqlStatement::DropTrigger(name) => ddl::drop_trigger(shared, name),
        GqlStatement::ShowTriggers => ddl::show_triggers(shared),
        // ── Type DDL ────────────────────────────────────────────────
        GqlStatement::CreateNodeType {
            label,
            parent,
            properties,
            or_replace,
            if_not_exists,
        } => ddl::create_node_type(
            shared,
            label,
            parent.as_deref(),
            properties,
            *or_replace,
            *if_not_exists,
        ),
        GqlStatement::DropNodeType { label, if_exists } => {
            ddl::drop_node_type(shared, label, *if_exists)
        }
        GqlStatement::ShowNodeTypes => ddl::show_node_types(shared),
        GqlStatement::CreateEdgeType {
            label,
            source_labels,
            target_labels,
            properties,
            or_replace,
            if_not_exists,
        } => ddl::create_edge_type(
            shared,
            label,
            source_labels,
            target_labels,
            properties,
            *or_replace,
            *if_not_exists,
        ),
        GqlStatement::DropEdgeType { label, if_exists } => {
            ddl::drop_edge_type(shared, label, *if_exists)
        }
        GqlStatement::ShowEdgeTypes => ddl::show_edge_types(shared),

        // ── Materialized View DDL ────────────────────────────────────
        GqlStatement::CreateMaterializedView {
            name,
            or_replace,
            if_not_exists,
            definition_text,
            match_clause,
            return_clause,
        } => ddl::create_materialized_view(
            shared,
            name.as_str(),
            *or_replace,
            *if_not_exists,
            definition_text,
            match_clause,
            return_clause,
        ),
        GqlStatement::DropMaterializedView { name, if_exists } => {
            ddl::drop_materialized_view(shared, name.as_str(), *if_exists)
        }
        GqlStatement::ShowMaterializedViews => ddl::show_materialized_views(shared),

        // DDL statements are handled at the server ops layer, not in the GQL engine
        _ => Err(GqlError::internal(
            "DDL statements must be executed via the server ops layer",
        )),
    }
}

/// Execute a GQL statement within an existing transaction.
///
/// Mutations go through `txn.mutate()`. The transaction must be committed
/// by the caller after all statements are executed.
fn execute_in_transaction(
    gql: &str,
    txn: &mut selene_graph::TransactionHandle<'_>,
    hot_tier: Option<&selene_ts::HotTier>,
    scope: Option<&RoaringBitmap>,
    parameters: Option<&ParameterMap>,
) -> Result<GqlResult, GqlError> {
    let stmt = parse_statement(gql)?;
    let graph = txn.graph();
    let registry = ProcedureRegistry::builtins();

    match &stmt {
        GqlStatement::Query(pipeline) => {
            let plan = planner::plan_query(pipeline, graph)?;
            execute_plan(&plan, graph, scope, hot_tier, Some(registry), parameters)
        }
        GqlStatement::Mutate(mp) => {
            // Plan + pattern match + pre-filter (immutable borrow of txn.graph())
            let (plan, mut bindings) = {
                let graph = txn.graph();
                let func_reg = FunctionRegistry::builtins();
                let mut ctx = EvalContext::new(graph, func_reg).with_scope(scope);
                if let Some(params) = parameters {
                    ctx = ctx.with_parameters(params);
                }
                let plan = planner::plan_mutation(mp, graph)?;
                let mut bindings =
                    execute_pattern_ops_with_eval_ctx(&plan.pattern_ops, graph, scope, &ctx)?;
                for op in &plan.pipeline {
                    match op {
                        PipelineOp::Return { .. } => break,
                        PipelineOp::Call { procedure: call } => {
                            bindings = execute_call(
                                bindings,
                                call,
                                graph,
                                hot_tier,
                                Some(registry),
                                scope,
                                Some(&ctx),
                            )?;
                        }
                        _ => {
                            bindings = stages::execute_pipeline_op(op, bindings, &ctx)?;
                        }
                    }
                }
                (plan, bindings)
            }; // graph borrow dropped here

            // Execute mutations (mutable borrow of txn)
            let mut mutation_stats = MutationStats::default();
            let change_count_before = txn.change_count();
            for mutation in &plan.mutations {
                execute_single_mutation_in_txn(
                    txn,
                    mutation,
                    &mut bindings,
                    scope,
                    &mut mutation_stats,
                    parameters,
                )?;
            }

            // Evaluate triggers against the mutations just committed.
            // Uses txn.graph() (immutable) for condition evaluation and
            // txn.mutate() for trigger actions, within the same transaction.
            if txn.change_count() > change_count_before
                && !txn.graph().trigger_registry().is_empty()
            {
                // Collect the changes from this batch of mutations
                let recent_changes = txn.accumulated_changes()[change_count_before..].to_vec();
                // Evaluate triggers directly on the transaction's graph
                let graph = txn.graph_mut();
                let trigger_changes =
                    super::triggers::evaluate_triggers(graph, &recent_changes, 0)?;
                // Push trigger-generated changes into the transaction so they
                // appear in the Vec<Change> returned by commit() for WAL persistence.
                txn.extend_changes(trigger_changes);
            }

            // Post-mutation pipeline (new immutable borrow)
            {
                let post_graph = txn.graph();
                let post_registry = FunctionRegistry::builtins();
                let post_ctx = EvalContext::new(post_graph, post_registry).with_scope(scope);
                for op in &plan.pipeline {
                    if matches!(op, PipelineOp::Return { .. })
                        || matches!(op, PipelineOp::Sort { .. })
                        || matches!(op, PipelineOp::Offset { .. })
                        || matches!(op, PipelineOp::Limit { .. })
                    {
                        bindings = stages::execute_pipeline_op(op, bindings, &post_ctx)?;
                    }
                }

                let row_count = bindings.len();
                let aliases: Vec<IStr> = plan
                    .output_schema
                    .fields()
                    .iter()
                    .map(|f| IStr::new(f.name()))
                    .collect();
                let schema = infer_schema_from_bindings(&bindings, &aliases);
                let batches = materialize_to_arrow(&bindings, &schema)?;
                let (schema, batches) = apply_display_schema(schema, batches, &plan.display_schema);

                Ok(GqlResult {
                    schema,
                    batches,
                    status: GqlStatus::success(row_count),
                    mutations: mutation_stats,
                    profile: None,
                    changes: vec![],
                })
            }
        }
        _ => Ok(GqlResult::empty()),
    }
}

/// Execute a parsed GQL statement with a pre-built CSR adjacency.
///
/// When `csr` is `Some`, expand operations use flat-array O(1) neighbor access
/// with typed edge lookup instead of ImblMap HAMT traversal. The CSR should be
/// built once per snapshot at the server layer and reused across queries.
fn execute_statement_with_csr(
    stmt: &GqlStatement,
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    hot_tier: Option<&selene_ts::HotTier>,
    procedures: Option<&ProcedureRegistry>,
    parameters: Option<&ParameterMap>,
    csr: Option<&selene_graph::CsrAdjacency>,
) -> Result<GqlResult, GqlError> {
    match stmt {
        GqlStatement::Query(pipeline) => {
            let plan = planner::plan_query(pipeline, graph)?;
            execute_plan_with_csr(&plan, graph, scope, hot_tier, procedures, parameters, csr)
        }
        GqlStatement::Chained { blocks } => {
            // NEXT chaining: output of block N becomes input bindings for block N+1
            let mut prev_bindings: Option<Vec<Binding>> = None;
            let mut last_result = None;
            let registry = FunctionRegistry::builtins();

            for block in blocks {
                let plan = planner::plan_query(block, graph)?;
                let mut ctx = EvalContext::new(graph, registry).with_scope(scope);
                if let Some(params) = parameters {
                    ctx = ctx.with_parameters(params);
                }

                let scan_limit = detect_scan_limit(&plan.pattern_ops, &plan.pipeline);
                let mut bindings = execute_pattern_ops_with_csr_and_ctx(
                    &plan.pattern_ops,
                    graph,
                    scope,
                    scan_limit,
                    None,
                    csr,
                    &ctx,
                )?;

                // If we have results from the previous block, merge them
                if let Some(prev) = prev_bindings.take() {
                    if plan.pattern_ops.is_empty() {
                        bindings = prev;
                    } else {
                        bindings = join::execute_cartesian_product(&prev, &bindings)?;
                    }
                }
                check_binding_limit(&bindings)?;

                for op in &plan.pipeline {
                    match op {
                        PipelineOp::Call { procedure: call } => {
                            bindings = execute_call(
                                bindings,
                                call,
                                graph,
                                hot_tier,
                                procedures,
                                scope,
                                Some(&ctx),
                            )?;
                        }
                        PipelineOp::Subquery { plan: sub_plan } => {
                            bindings = execute_subquery(
                                bindings,
                                sub_plan,
                                graph,
                                scope,
                                hot_tier,
                                procedures,
                                Some(&ctx),
                            )?;
                        }
                        _ => {
                            bindings = stages::execute_pipeline_op(op, bindings, &ctx)?;
                        }
                    }
                }

                let row_count = bindings.len();
                let aliases: Vec<IStr> = plan
                    .output_schema
                    .fields()
                    .iter()
                    .map(|f| IStr::new(f.name()))
                    .collect();
                let schema = infer_schema_from_bindings(&bindings, &aliases);
                let batches = materialize_to_arrow(&bindings, &schema)?;
                let (schema, batches) = apply_display_schema(schema, batches, &plan.display_schema);
                prev_bindings = Some(bindings);
                last_result = Some(GqlResult {
                    schema,
                    batches,
                    status: GqlStatus::success(row_count),
                    mutations: MutationStats::default(),
                    profile: None,
                    changes: vec![],
                });
            }
            last_result.ok_or_else(|| GqlError::internal("empty chained query"))
        }
        GqlStatement::Composite { first, rest } => {
            let first_plan = planner::plan_query(first, graph)?;
            let mut result =
                execute_plan(&first_plan, graph, scope, hot_tier, procedures, parameters)?;

            for (op, pipeline) in rest {
                let plan = planner::plan_query(pipeline, graph)?;
                let other = execute_plan(&plan, graph, scope, hot_tier, procedures, parameters)?;
                result = apply_set_op(*op, result, other);
            }
            let rows = result.row_count();
            result.status = GqlStatus::success(rows);
            Ok(result)
        }
        GqlStatement::Mutate(mp) => {
            let plan = planner::plan_mutation(mp, graph)?;
            execute_plan(&plan, graph, scope, hot_tier, procedures, parameters)
        }
        GqlStatement::StartTransaction | GqlStatement::Commit | GqlStatement::Rollback => {
            Err(GqlError::Internal {
                message: "multi-statement transactions are not supported over the wire protocol"
                    .into(),
            })
        }
        // DDL statements are handled at the server ops layer
        _ => Err(GqlError::internal(
            "DDL statements must be executed via the server ops layer",
        )),
    }
}

/// Execute a count-only query without materializing bindings.
/// Returns `Some(count)` if the short-circuit applies, `None` to fall back.
fn execute_count_only(
    plan: &ExecutionPlan,
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    ctx: &EvalContext<'_>,
) -> Result<Option<u64>, GqlError> {
    use crate::pattern::scan;

    // Extract the LabelScan parameters
    let PatternOp::LabelScan {
        labels,
        property_filters,
        inline_props,
        ..
    } = &plan.pattern_ops[0]
    else {
        return Ok(None);
    };

    // If there are pipeline-level filters, fall back to normal execution.
    // Count-only only handles scan-level filters (property_filters + inline_props).
    for op in &plan.pipeline {
        if matches!(op, PipelineOp::Filter { .. }) {
            return Ok(None);
        }
    }

    let count = scan::count_label_scan(
        labels.as_ref(),
        inline_props,
        &scan::ScanContext {
            graph,
            scope,
            property_filters,
            eval_ctx: ctx,
        },
    )?;
    Ok(Some(count))
}

/// Execute a planned query/mutation with default options and no CSR.
fn execute_plan(
    plan: &ExecutionPlan,
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    hot_tier: Option<&selene_ts::HotTier>,
    procedures: Option<&ProcedureRegistry>,
    parameters: Option<&ParameterMap>,
) -> Result<GqlResult, GqlError> {
    execute_plan_inner(
        plan,
        graph,
        scope,
        hot_tier,
        procedures,
        parameters,
        &crate::GqlOptions::default(),
        None,
    )
}

/// Execute a planned query with a pre-built CSR adjacency and default options.
fn execute_plan_with_csr(
    plan: &ExecutionPlan,
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    hot_tier: Option<&selene_ts::HotTier>,
    procedures: Option<&ProcedureRegistry>,
    parameters: Option<&ParameterMap>,
    csr: Option<&selene_graph::CsrAdjacency>,
) -> Result<GqlResult, GqlError> {
    execute_plan_inner(
        plan,
        graph,
        scope,
        hot_tier,
        procedures,
        parameters,
        &crate::GqlOptions::default(),
        csr,
    )
}

/// Core plan execution. All variants delegate here.
#[allow(clippy::too_many_arguments)]
fn execute_plan_inner(
    plan: &ExecutionPlan,
    graph: &SeleneGraph,
    scope: Option<&RoaringBitmap>,
    hot_tier: Option<&selene_ts::HotTier>,
    procedures: Option<&ProcedureRegistry>,
    parameters: Option<&ParameterMap>,
    options: &crate::GqlOptions,
    csr: Option<&selene_graph::CsrAdjacency>,
) -> Result<GqlResult, GqlError> {
    // Create evaluation context with optional parameters
    let registry = FunctionRegistry::builtins();
    let mut ctx = EvalContext::new(graph, registry)
        .with_options(options.clone())
        .with_scope(scope);
    if let Some(params) = parameters {
        ctx = ctx.with_parameters(params);
    }

    // ── Count-only short-circuit ──
    // Skip all binding materialization. Return a single row with the count.
    if plan.count_only
        && let Some(count) = execute_count_only(plan, graph, scope, &ctx)?
    {
        let count_alias = plan
            .display_schema
            .fields()
            .first()
            .map_or("count(*)", |f| f.name().as_str());
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(count_alias, arrow::datatypes::DataType::Int64, false),
        ]));
        let array = arrow::array::Int64Array::from(vec![count as i64]);
        let batch =
            arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])
                .map_err(|e| GqlError::internal(format!("Arrow batch: {e}")))?;
        return Ok(GqlResult {
            schema,
            batches: vec![batch],
            status: GqlStatus::success(1),
            mutations: MutationStats::default(),
            profile: None,
            changes: vec![],
        });
    }
    // Fall through to normal execution if short-circuit not applicable

    // Detect simple LIMIT pushdown: single LabelScan + pipeline ends with LIMIT
    let scan_limit = detect_scan_limit(&plan.pattern_ops, &plan.pipeline);

    // Pattern matching -> DataChunk or FactorizedChunk
    // When factorized execution is enabled, try the factorized path first.
    // It avoids materializing the full Cartesian product for multi-hop
    // patterns by storing parent linkage pointers instead of replicating
    // parent columns. Falls back to flat if the pattern is not factorizable.
    //
    // A FactorizedChunk is kept through streaming pipeline ops (Filter,
    // Offset, Limit) and only flattened when a non-streaming op is reached
    // (Sort, GroupBy, RETURN, CALL, Subquery, TopK).
    let (mut chunk, mut factorized) = if options.factorized && plan.mutations.is_empty() {
        match pattern::execute_pattern_ops_as_factorized_chunk(
            &plan.pattern_ops,
            graph,
            scope,
            csr,
            Some(&ctx),
        ) {
            Some(Ok(fc)) => (None, Some(fc)),
            Some(Err(e)) => return Err(e),
            None => {
                let c = execute_pattern_ops_as_chunk_with_ctx(
                    &plan.pattern_ops,
                    graph,
                    scope,
                    scan_limit,
                    None,
                    csr,
                    &ctx,
                )?;
                (Some(c), None)
            }
        }
    } else {
        let c = execute_pattern_ops_as_chunk_with_ctx(
            &plan.pattern_ops,
            graph,
            scope,
            scan_limit,
            None,
            csr,
            &ctx,
        )?;
        (Some(c), None)
    };

    // Helper: ensure we have a flat DataChunk (flattening factorized if needed)
    let ensure_flat = |chunk: &mut Option<DataChunk>,
                       factorized: &mut Option<crate::types::factor::FactorizedChunk>|
     -> DataChunk {
        if let Some(c) = chunk.take() {
            c
        } else if let Some(fc) = factorized.take() {
            fc.flatten()
        } else {
            DataChunk::unit() // should not happen
        }
    };

    // Check memory limit
    if let Some(ref fc) = factorized {
        // Check deepest level count as a proxy (avoid full flatten just for the check)
        if fc.active_len() > max_bindings() {
            return Err(GqlError::internal(format!(
                "query exceeded maximum result size ({} rows)",
                fc.active_len()
            )));
        }
    }
    if let Some(ref c) = chunk {
        check_chunk_limit(c)?;
    }

    // Pipeline stages that come before mutations (FILTER).
    // Mutations need filtered bindings, not all pattern matches.
    let mut pre_mutation_pipeline = Vec::new();
    let mut post_mutation_pipeline = Vec::new();
    let mut seen_return = false;
    for op in &plan.pipeline {
        if matches!(op, PipelineOp::Return { .. }) {
            seen_return = true;
        }
        if !plan.mutations.is_empty() && !seen_return {
            // Pipeline ops before RETURN apply before mutations
            match op {
                PipelineOp::Filter { .. } => pre_mutation_pipeline.push(op),
                _ => post_mutation_pipeline.push(op),
            }
        } else {
            post_mutation_pipeline.push(op);
        }
    }

    // Apply pre-mutation filters
    for op in &pre_mutation_pipeline {
        // Pre-mutation filters always flatten (mutations need flat bindings)
        let mut c = ensure_flat(&mut chunk, &mut factorized);
        c = stages::execute_pipeline_op_chunk(op, c, &ctx)?;
        chunk = Some(c);
    }

    // Execute mutations (if any): count using chunk row count directly
    let mut mutation_stats = MutationStats::default();
    if !plan.mutations.is_empty() {
        let c = ensure_flat(&mut chunk, &mut factorized);
        let row_count = c.active_len();
        for mutation in &plan.mutations {
            count_mutation(mutation, row_count, &mut mutation_stats);
        }
        chunk = Some(c);
    }

    // Pipeline processing.
    // When factorized state is active, Offset/Limit operate directly on
    // the deepest level's SelectionVector. Filter flattens for eval_vec,
    // applies the result back. All other ops trigger a flatten.
    let mut op_idx = 0;
    while op_idx < post_mutation_pipeline.len() {
        let op = post_mutation_pipeline[op_idx];

        // Factorized-native path for simple streaming ops
        if factorized.is_some() {
            match op {
                PipelineOp::Offset { value } => {
                    let n = value.resolve(ctx.parameters)? as usize;
                    let fc = factorized.as_mut().unwrap();
                    let deep = fc.deepest_mut();
                    let phys_len = deep.len;
                    deep.selection.skip(n, phys_len);
                    op_idx += 1;
                    continue;
                }
                PipelineOp::Limit { value } => {
                    let n = value.resolve(ctx.parameters)? as usize;
                    let fc = factorized.as_mut().unwrap();
                    let deep = fc.deepest_mut();
                    let phys_len = deep.len;
                    deep.selection.truncate(n, phys_len);
                    op_idx += 1;
                    continue;
                }
                PipelineOp::Filter { predicate } if !contains_subquery_expr(predicate) => {
                    // Flatten temporarily for eval_vec, apply result back.
                    // Use the main ctx (which has parameters and scope).
                    let fc = factorized.as_mut().unwrap();
                    let flat = fc.flatten();
                    let gatherer =
                        crate::runtime::vector::gather::GraphPropertyGatherer::new(graph);

                    let active_before: Vec<usize> = {
                        let deep = fc.deepest();
                        deep.selection.active_indices(deep.len).collect()
                    };

                    if let Ok(crate::types::chunk::Column::Bool(arr)) =
                        crate::runtime::vector::eval_vec(predicate, &flat, &gatherer, &ctx)
                    {
                        use arrow::array::Array;
                        let mut new_active = Vec::with_capacity(active_before.len());
                        for (pos, &phys_idx) in active_before.iter().enumerate() {
                            if !arr.is_null(pos) && arr.value(pos) {
                                new_active.push(phys_idx as u32);
                            }
                        }
                        let deep = fc.deepest_mut();
                        deep.selection =
                            crate::types::chunk::SelectionVector::from_indices(new_active);
                    } else {
                        // Per-row fallback
                        let mut new_active = Vec::with_capacity(active_before.len());
                        for (pos, &phys_idx) in active_before.iter().enumerate() {
                            let row = flat.row_view(pos);
                            let pass =
                                crate::runtime::eval::eval_predicate_row(predicate, &row, &ctx)
                                    .is_ok_and(|t| t.is_true());
                            if pass {
                                new_active.push(phys_idx as u32);
                            }
                        }
                        let deep = fc.deepest_mut();
                        deep.selection =
                            crate::types::chunk::SelectionVector::from_indices(new_active);
                    }
                    op_idx += 1;
                    continue;
                }
                // All other ops: flatten and fall through to flat path
                _ => {
                    let c = ensure_flat(&mut chunk, &mut factorized);
                    chunk = Some(c);
                }
            }
        }

        // Flat path (original logic)
        let c = chunk.take().unwrap_or_else(|| {
            // Should not happen: factorized was flattened above
            DataChunk::unit()
        });

        match op {
            // Fused Return+TopK with lazy projection
            PipelineOp::Return {
                projections,
                group_by,
                distinct,
                having,
                all,
            } if !*distinct
                && !*all
                && group_by.is_empty()
                && having.is_none()
                && op_idx + 1 < post_mutation_pipeline.len()
                && matches!(post_mutation_pipeline[op_idx + 1], PipelineOp::TopK { .. }) =>
            {
                if let PipelineOp::TopK { terms, limit } = &post_mutation_pipeline[op_idx + 1] {
                    let k = limit.resolve(ctx.parameters)?;
                    let bindings = c.to_bindings();
                    let result =
                        stages::execute_return_topk(bindings, projections, terms, k, &ctx)?;
                    chunk = Some(join::bindings_to_chunk_generic(&result));
                    op_idx += 2;
                    continue;
                }
                chunk = Some(stages::execute_pipeline_op_chunk(op, c, &ctx)?);
            }
            // CALL needs procedure registry and graph context
            PipelineOp::Call { procedure: call } => {
                let bindings = c.to_bindings();
                let result = execute_call(
                    bindings,
                    call,
                    graph,
                    hot_tier,
                    procedures,
                    scope,
                    Some(&ctx),
                )?;
                chunk = Some(join::bindings_to_chunk_generic(&result));
            }
            // Subquery needs correlated execution with graph context
            PipelineOp::Subquery { plan: sub_plan } => {
                let bindings = c.to_bindings();
                let result = execute_subquery(
                    bindings,
                    sub_plan,
                    graph,
                    scope,
                    hot_tier,
                    procedures,
                    Some(&ctx),
                )?;
                chunk = Some(join::bindings_to_chunk_generic(&result));
            }
            // NestedMatch: correlated MATCH after WITH -- run pattern ops seeded
            // by each input binding and merge results.
            PipelineOp::NestedMatch {
                pattern_ops: nested_ops,
                where_filter,
            } => {
                let bindings = c.to_bindings();
                let result = execute_nested_match(
                    bindings,
                    nested_ops,
                    where_filter.as_ref(),
                    graph,
                    scope,
                    Some(&ctx),
                )?;
                chunk = Some(join::bindings_to_chunk_generic(&result));
            }
            // ViewScan: read materialized view state via provider
            PipelineOp::ViewScan { .. } => {
                let bindings = c.to_bindings();
                let result = stages::execute_pipeline_op(op, bindings, &ctx)?;
                chunk = Some(join::bindings_to_chunk_generic(&result));
            }
            // Streaming ops: collect consecutive and apply as chunk ops
            PipelineOp::Let { .. }
            | PipelineOp::Filter { .. }
            | PipelineOp::Offset { .. }
            | PipelineOp::Limit { .. } => {
                let stream_start = op_idx;
                while op_idx < post_mutation_pipeline.len()
                    && matches!(
                        post_mutation_pipeline[op_idx],
                        PipelineOp::Let { .. }
                            | PipelineOp::Filter { .. }
                            | PipelineOp::Offset { .. }
                            | PipelineOp::Limit { .. }
                    )
                {
                    op_idx += 1;
                }
                let stream_ops = &post_mutation_pipeline[stream_start..op_idx];
                chunk = Some(execute_streaming_fused_chunk(c, stream_ops, &ctx)?);
                continue;
            }
            // Sort, RETURN, With, TopK, For
            _ => {
                chunk = Some(stages::execute_pipeline_op_chunk(op, c, &ctx)?);
            }
        }
        op_idx += 1;
    }

    // Final flatten if still factorized (e.g., no pipeline ops)
    let final_chunk = ensure_flat(&mut chunk, &mut factorized);

    // Build result: direct Arrow materialization from DataChunk.
    let row_count = final_chunk.active_len();
    let mut aliases: Vec<IStr> = plan
        .output_schema
        .fields()
        .iter()
        .map(|f| IStr::new(f.name()))
        .collect();
    // RETURN * / YIELD *: output_schema is empty at plan time because the
    // projected columns are not known until runtime. Fall back to the
    // chunk's own schema so that all columns are materialized.
    if aliases.is_empty() && final_chunk.column_count() > 0 {
        aliases = final_chunk.schema().iter().map(|(name, _)| *name).collect();
    }
    let (schema, batches) = materialize_chunk_to_arrow(&final_chunk, &aliases)?;
    let (schema, batches) = apply_display_schema(schema, batches, &plan.display_schema);

    Ok(GqlResult {
        schema,
        batches,
        status: GqlStatus::success(row_count),
        mutations: mutation_stats,
        profile: None,
        changes: vec![],
    })
}

/// Maximum memory budget for a single query's bindings (in bytes).
pub(super) const MAX_QUERY_BYTES: usize = 64 * 1024 * 1024; // 64 MB
/// Default maximum number of bindings.
const DEFAULT_MAX_BINDINGS: usize = 100_000;

/// Read max bindings from `SELENE_MAX_BINDINGS` env var (cached via OnceLock).
pub(super) fn max_bindings() -> usize {
    static VALUE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *VALUE.get_or_init(|| {
        std::env::var("SELENE_MAX_BINDINGS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_BINDINGS)
    })
}

/// Check binding memory usage against the budget.
pub(super) fn check_binding_limit(bindings: &[Binding]) -> Result<(), GqlError> {
    let limit = max_bindings();
    if bindings.len() > limit {
        return Err(GqlError::ResourcesExhausted {
            message: format!("query produced {} bindings (max {})", bindings.len(), limit),
        });
    }
    let estimated = estimate_binding_bytes(bindings);
    if estimated > MAX_QUERY_BYTES {
        return Err(GqlError::ResourcesExhausted {
            message: format!(
                "query memory estimate {:.1} MB exceeds budget {:.1} MB ({} bindings)",
                estimated as f64 / (1024.0 * 1024.0),
                MAX_QUERY_BYTES as f64 / (1024.0 * 1024.0),
                bindings.len(),
            ),
        });
    }
    Ok(())
}

/// Estimate total memory usage of bindings. 64 bytes base + 24 per variable.
fn estimate_binding_bytes(bindings: &[Binding]) -> usize {
    if bindings.is_empty() {
        return 0;
    }
    let vars = bindings[0].len();
    bindings.len() * (64 + vars * 24)
}

/// Check DataChunk row count against the budget.
pub(crate) fn check_chunk_limit(chunk: &crate::types::chunk::DataChunk) -> Result<(), GqlError> {
    let limit = max_bindings();
    let active = chunk.active_len();
    if active > limit {
        return Err(GqlError::ResourcesExhausted {
            message: format!("query produced {active} rows (max {limit})"),
        });
    }
    // Conservative estimate: 8 bytes per column per row (u64 worst case)
    let cols = chunk.column_count();
    let estimated = active * cols * 8;
    if estimated > MAX_QUERY_BYTES {
        return Err(GqlError::ResourcesExhausted {
            message: format!(
                "query memory estimate {:.1} MB exceeds budget {:.1} MB ({active} rows, {cols} cols)",
                estimated as f64 / (1024.0 * 1024.0),
                MAX_QUERY_BYTES as f64 / (1024.0 * 1024.0),
            ),
        });
    }
    Ok(())
}

/// Detect if a simple LIMIT can be pushed into the pattern scan.
/// Only valid when: single LabelScan (no Expand/VarExpand/Join), no FILTER
/// before RETURN, and pipeline has a LIMIT.
fn detect_scan_limit(pattern_ops: &[PatternOp], pipeline: &[PipelineOp]) -> Option<usize> {
    // Must be a single scan
    if pattern_ops.len() != 1 || !matches!(pattern_ops[0], PatternOp::LabelScan { .. }) {
        return None;
    }
    // Cannot push LIMIT past ops that change row order or cardinality
    let has_blocking_op = pipeline.iter().any(|op| match op {
        PipelineOp::Filter { .. } => true,
        PipelineOp::Sort { .. } => true,
        PipelineOp::TopK { .. } => true,
        PipelineOp::Return {
            group_by, distinct, ..
        } => !group_by.is_empty() || *distinct,
        PipelineOp::With { .. } => true, // WITH resets scope, always blocking
        _ => false,
    });
    if has_blocking_op {
        return None;
    }
    // Find LIMIT in pipeline (only literal values usable at plan time)
    for op in pipeline {
        if let PipelineOp::Limit {
            value: crate::ast::statement::LimitValue::Literal(n),
        } = op
        {
            return Some(*n as usize);
        }
    }
    None
}

/// Execute pattern operations sequentially, building up bindings.
mod arrow_io;
mod call;
mod ddl;
mod mutation;
mod mutation_txn;
mod pattern;

use arrow_io::{
    apply_set_op, infer_schema_from_bindings, materialize_chunk_to_arrow, materialize_to_arrow,
};
use call::{execute_call, execute_nested_match, execute_subquery};
use mutation::{count_mutation, execute_mutations_write};
use mutation_txn::execute_single_mutation_in_txn;
use pattern::{execute_pattern_ops_as_chunk_with_ctx, execute_pattern_ops_with_csr_and_ctx};
pub(crate) use pattern::{
    execute_pattern_ops_correlated_with_ctx, execute_pattern_ops_with_eval_ctx,
    execute_pattern_ops_with_max_and_ctx,
};

/// Fused streaming on DataChunk: applies consecutive LET/FILTER/OFFSET/LIMIT
/// as columnar operations, avoiding per-row dispatch. LET and FILTER use
/// eval_vec (batch expression evaluator), OFFSET and LIMIT use the selection
/// vector for zero-copy row skipping/truncation.
fn execute_streaming_fused_chunk(
    mut chunk: DataChunk,
    ops: &[&PipelineOp],
    ctx: &EvalContext<'_>,
) -> Result<DataChunk, GqlError> {
    for op in ops {
        chunk = stages::execute_pipeline_op_chunk(op, chunk, ctx)?;
    }
    Ok(chunk)
}

// ── Builder API ─────────────────────────────────────────────────────

/// Internal enum distinguishing string input from pre-parsed statements.
enum QueryInput<'a> {
    String(&'a str),
    Statement(&'a GqlStatement),
}

/// Fluent builder for read-only GQL queries.
///
/// Supports both string and pre-parsed statement input. All optional
/// parameters default to `None`. Call [`execute`](QueryBuilder::execute)
/// as the terminal method.
///
/// # Examples
///
/// ```ignore
/// use selene_gql::QueryBuilder;
///
/// // From string
/// let result = QueryBuilder::new("MATCH (s:sensor) RETURN s.name", &graph)
///     .with_scope(&scope)
///     .execute()?;
///
/// // From pre-parsed statement
/// let result = QueryBuilder::from_statement(&stmt, &graph)
///     .with_hot_tier(&hot_tier)
///     .with_csr(&csr)
///     .execute()?;
/// ```
pub struct QueryBuilder<'a> {
    input: QueryInput<'a>,
    graph: &'a SeleneGraph,
    scope: Option<&'a RoaringBitmap>,
    hot_tier: Option<&'a selene_ts::HotTier>,
    procedures: Option<&'a ProcedureRegistry>,
    parameters: Option<&'a ParameterMap>,
    csr: Option<&'a selene_graph::CsrAdjacency>,
    options: Option<&'a crate::GqlOptions>,
}

impl<'a> QueryBuilder<'a> {
    /// Create a builder from a GQL query string.
    pub fn new(query: &'a str, graph: &'a SeleneGraph) -> Self {
        Self {
            input: QueryInput::String(query),
            graph,
            scope: None,
            hot_tier: None,
            procedures: None,
            parameters: None,
            csr: None,
            options: None,
        }
    }

    /// Create a builder from a pre-parsed GQL statement.
    pub fn from_statement(stmt: &'a GqlStatement, graph: &'a SeleneGraph) -> Self {
        Self {
            input: QueryInput::Statement(stmt),
            graph,
            scope: None,
            hot_tier: None,
            procedures: None,
            parameters: None,
            csr: None,
            options: None,
        }
    }

    /// Restrict results to the given authorization scope bitmap.
    pub fn with_scope(mut self, scope: &'a RoaringBitmap) -> Self {
        self.scope = Some(scope);
        self
    }

    /// Provide time-series hot tier for CALL ts.* procedures.
    pub fn with_hot_tier(mut self, hot_tier: &'a selene_ts::HotTier) -> Self {
        self.hot_tier = Some(hot_tier);
        self
    }

    /// Provide a custom procedure registry.
    pub fn with_procedures(mut self, procedures: &'a ProcedureRegistry) -> Self {
        self.procedures = Some(procedures);
        self
    }

    /// Provide query parameters ($param bindings).
    pub fn with_parameters(mut self, parameters: &'a ParameterMap) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Provide a pre-built CSR adjacency for O(1) neighbor lookups.
    pub fn with_csr(mut self, csr: &'a selene_graph::CsrAdjacency) -> Self {
        self.csr = Some(csr);
        self
    }

    /// Provide GQL options (e.g. strict coercion mode).
    pub fn with_options(mut self, options: &'a crate::GqlOptions) -> Self {
        self.options = Some(options);
        self
    }

    /// Parse (if needed), plan, and execute the query. Terminal method.
    ///
    /// When no explicit procedure registry is provided via
    /// [`with_procedures`](Self::with_procedures), the built-in procedure
    /// registry is created automatically so that CALL statements (e.g.
    /// `ts.latest`, `graph.similarNodes`) work out of the box.
    pub fn execute(self) -> Result<GqlResult, GqlError> {
        let owned_stmt;
        let stmt = match self.input {
            QueryInput::String(gql) => {
                owned_stmt = parse_statement(gql)?;
                &owned_stmt
            }
            QueryInput::Statement(s) => s,
        };

        // Auto-create builtin procedures when none are explicitly provided,
        // so CALL statements work without requiring callers to wire up a
        // ProcedureRegistry manually.
        let owned_builtins = ProcedureRegistry::builtins();
        let procedures: Option<&ProcedureRegistry> = self.procedures.or(Some(owned_builtins));

        // If explicit options are provided and the statement is a query,
        // thread them through the plan execution path.
        if let Some(options) = self.options
            && let GqlStatement::Query(pipeline) = stmt
        {
            let plan = planner::plan_query(pipeline, self.graph)?;
            return execute_plan_inner(
                &plan,
                self.graph,
                self.scope,
                self.hot_tier,
                procedures,
                self.parameters,
                options,
                self.csr,
            );
        }

        execute_statement_with_csr(
            stmt,
            self.graph,
            self.scope,
            self.hot_tier,
            procedures,
            self.parameters,
            self.csr,
        )
    }
}

/// Fluent builder for GQL mutations.
///
/// Collects optional scope and hot tier, then executes via either
/// [`execute`](MutationBuilder::execute) (auto-commit) or
/// [`execute_in_transaction`](MutationBuilder::execute_in_transaction).
///
/// # Examples
///
/// ```ignore
/// use selene_gql::MutationBuilder;
///
/// // Auto-commit
/// let result = MutationBuilder::new("INSERT (:sensor {name: 'T1'})")
///     .with_scope(&scope)
///     .execute(&shared)?;
///
/// // Within an existing transaction
/// let result = MutationBuilder::new("INSERT (:sensor {name: 'T2'})")
///     .execute_in_transaction(&mut txn)?;
/// ```
pub struct MutationBuilder<'a> {
    query: &'a str,
    scope: Option<&'a RoaringBitmap>,
    hot_tier: Option<&'a selene_ts::HotTier>,
    parameters: Option<&'a ParameterMap>,
}

impl<'a> MutationBuilder<'a> {
    /// Create a builder from a GQL mutation string.
    pub fn new(query: &'a str) -> Self {
        Self {
            query,
            scope: None,
            hot_tier: None,
            parameters: None,
        }
    }

    /// Restrict mutations to the given authorization scope bitmap.
    pub fn with_scope(mut self, scope: &'a RoaringBitmap) -> Self {
        self.scope = Some(scope);
        self
    }

    /// Provide time-series hot tier for CALL ts.* procedures within mutations.
    pub fn with_hot_tier(mut self, hot_tier: &'a selene_ts::HotTier) -> Self {
        self.hot_tier = Some(hot_tier);
        self
    }

    /// Provide query parameters ($param bindings).
    pub fn with_parameters(mut self, parameters: &'a ParameterMap) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Execute as an auto-commit mutation against a shared graph.
    pub fn execute(self, shared: &SharedGraph) -> Result<GqlResult, GqlError> {
        execute_mut(
            self.query,
            shared,
            self.hot_tier,
            self.scope,
            self.parameters,
        )
    }

    /// Execute within an existing transaction.
    ///
    /// The transaction must be committed by the caller after all
    /// statements have been executed.
    pub fn execute_in_transaction(
        self,
        txn: &mut selene_graph::TransactionHandle<'_>,
    ) -> Result<GqlResult, GqlError> {
        execute_in_transaction(self.query, txn, self.hot_tier, self.scope, self.parameters)
    }
}

/// Check whether an expression contains a subquery (EXISTS, COUNT, VALUE, COLLECT).
/// Subquery predicates cannot be evaluated by the vectorized path and must
/// fall through to the flat per-row evaluator.
fn contains_subquery_expr(expr: &Expr) -> bool {
    let mut found = false;
    expr.walk(&mut |e| {
        if !found {
            match e {
                Expr::Exists { .. }
                | Expr::CountSubquery(_)
                | Expr::ValueSubquery(_)
                | Expr::CollectSubquery(_) => found = true,
                _ => {}
            }
        }
    });
    found
}

#[cfg(test)]
mod tests;
