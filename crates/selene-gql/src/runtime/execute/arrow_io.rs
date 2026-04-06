//! Arrow materialization and set operations (UNION/INTERSECT/EXCEPT).

use std::sync::Arc;

use arrow::row::{RowConverter, SortField};
use selene_core::IStr;

use crate::types::binding::{Binding, BoundValue};
use crate::types::error::GqlError;
use crate::types::result::GqlResult;
use crate::types::value::GqlValue;

/// Apply a set operation to two GqlResult values.
pub(super) fn apply_set_op(
    op: crate::ast::statement::SetOp,
    mut left: GqlResult,
    right: GqlResult,
) -> GqlResult {
    use crate::ast::statement::SetOp;

    let schema = left
        .batches
        .first()
        .or(right.batches.first())
        .map(|b| b.schema());

    let converter = schema.map(|s| {
        let fields = sort_fields_from_schema(&s);
        RowConverter::new(fields).unwrap()
    });

    match op {
        SetOp::UnionAll => {
            left.batches.extend(right.batches);
            left
        }
        SetOp::UnionDistinct => {
            left.batches.extend(right.batches);
            if let Some(ref conv) = converter {
                dedup_batches(&mut left, conv);
            }
            left
        }
        SetOp::IntersectDistinct => {
            if let Some(ref conv) = converter {
                let right_keys = collect_row_keys(&right.batches, conv);
                retain_matching(&mut left, &right_keys, true, conv);
                dedup_batches(&mut left, conv);
            }
            left
        }
        SetOp::IntersectAll => {
            if let Some(ref conv) = converter {
                let right_keys = collect_row_keys(&right.batches, conv);
                retain_matching(&mut left, &right_keys, true, conv);
            }
            left
        }
        SetOp::ExceptDistinct => {
            if let Some(ref conv) = converter {
                let right_keys = collect_row_keys(&right.batches, conv);
                retain_matching(&mut left, &right_keys, false, conv);
                dedup_batches(&mut left, conv);
            }
            left
        }
        SetOp::ExceptAll => {
            if let Some(ref conv) = converter {
                let right_keys = collect_row_keys(&right.batches, conv);
                retain_matching(&mut left, &right_keys, false, conv);
            }
            left
        }
        SetOp::Otherwise => {
            if left.row_count() == 0 {
                right
            } else {
                left
            }
        }
    }
}

fn sort_fields_from_schema(schema: &arrow::datatypes::Schema) -> Vec<SortField> {
    schema
        .fields()
        .iter()
        .map(|f| SortField::new(f.data_type().clone()))
        .collect()
}

fn row_hashes(converter: &RowConverter, batch: &arrow::record_batch::RecordBatch) -> Vec<u64> {
    use std::hash::{Hash, Hasher};
    let rows = converter.convert_columns(batch.columns()).unwrap();
    (0..batch.num_rows())
        .map(|i| {
            let mut hasher = std::hash::DefaultHasher::new();
            rows.row(i).as_ref().hash(&mut hasher);
            hasher.finish()
        })
        .collect()
}

fn collect_row_keys(
    batches: &[arrow::record_batch::RecordBatch],
    converter: &RowConverter,
) -> std::collections::HashSet<u64> {
    let mut keys = std::collections::HashSet::new();
    for batch in batches {
        keys.extend(row_hashes(converter, batch));
    }
    keys
}

fn retain_matching(
    result: &mut GqlResult,
    keys: &std::collections::HashSet<u64>,
    keep_matching: bool,
    converter: &RowConverter,
) {
    use arrow::array::BooleanArray;
    let mut new_batches = Vec::new();
    for batch in &result.batches {
        let hashes = row_hashes(converter, batch);
        let keep: Vec<bool> = hashes
            .iter()
            .map(|h| {
                let in_set = keys.contains(h);
                if keep_matching { in_set } else { !in_set }
            })
            .collect();
        let filter_arr = BooleanArray::from(keep);
        if let Ok(filtered) = arrow::compute::filter_record_batch(batch, &filter_arr)
            && filtered.num_rows() > 0
        {
            new_batches.push(filtered);
        }
    }
    result.batches = new_batches;
}

fn dedup_batches(result: &mut GqlResult, converter: &RowConverter) {
    use arrow::array::BooleanArray;
    let mut seen = std::collections::HashSet::new();
    let mut new_batches = Vec::new();
    for batch in &result.batches {
        let hashes = row_hashes(converter, batch);
        let keep: Vec<bool> = hashes.iter().map(|h| seen.insert(*h)).collect();
        let filter_arr = BooleanArray::from(keep);
        if let Ok(filtered) = arrow::compute::filter_record_batch(batch, &filter_arr)
            && filtered.num_rows() > 0
        {
            new_batches.push(filtered);
        }
    }
    result.batches = new_batches;
}

/// Infer Arrow schema from the first result row's actual types.
pub(super) fn infer_schema_from_bindings(
    bindings: &[Binding],
    aliases: &[IStr],
) -> Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field};

    if bindings.is_empty() {
        let fields: Vec<Field> = aliases
            .iter()
            .map(|a| Field::new(a.as_str(), DataType::Utf8, true))
            .collect();
        return Arc::new(arrow::datatypes::Schema::new(fields));
    }

    let first = &bindings[0];
    let fields: Vec<Field> = aliases
        .iter()
        .map(|alias| {
            let dt = match first.get(alias) {
                Some(BoundValue::Scalar(GqlValue::Int(_))) => DataType::Int64,
                Some(BoundValue::Scalar(GqlValue::UInt(_))) => DataType::UInt64,
                Some(BoundValue::Scalar(GqlValue::Float(_))) => DataType::Float64,
                Some(BoundValue::Scalar(GqlValue::Bool(_))) => DataType::Boolean,
                Some(BoundValue::Scalar(GqlValue::ZonedDateTime(_))) => DataType::Int64,
                Some(BoundValue::Node(_)) => DataType::UInt64,
                Some(BoundValue::Edge(_)) => DataType::UInt64,
                _ => DataType::Utf8,
            };
            Field::new(alias.as_str(), dt, true)
        })
        .collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

/// Materialize bindings to Arrow RecordBatch with type-specific builders.
/// Single-pass: iterates bindings once, appending to all column builders simultaneously.
/// Target batch size for Arrow output. Limits peak memory.
pub(super) const ARROW_BATCH_SIZE: usize = 8192;

pub(super) fn materialize_to_arrow(
    bindings: &[Binding],
    schema: &Arc<arrow::datatypes::Schema>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, GqlError> {
    if bindings.is_empty() || schema.fields().is_empty() {
        return Ok(vec![arrow::record_batch::RecordBatch::new_empty(
            Arc::clone(schema),
        )]);
    }

    let col_names: Vec<IStr> = schema
        .fields()
        .iter()
        .map(|f| IStr::new(f.name()))
        .collect();

    // Parallel path: build batches concurrently when above threshold
    if bindings.len() >= crate::parallel::parallel_threshold() {
        use rayon::prelude::*;
        let batches: Result<Vec<_>, GqlError> = bindings
            .par_chunks(ARROW_BATCH_SIZE)
            .map(|chunk| build_arrow_batch(chunk, schema, &col_names))
            .collect();
        return batches;
    }

    // Serial path
    let mut batches = Vec::new();
    for chunk in bindings.chunks(ARROW_BATCH_SIZE) {
        batches.push(build_arrow_batch(chunk, schema, &col_names)?);
    }
    Ok(batches)
}

/// Build a single Arrow RecordBatch from a chunk of bindings.
fn build_arrow_batch(
    chunk: &[Binding],
    schema: &Arc<arrow::datatypes::Schema>,
    col_names: &[IStr],
) -> Result<arrow::record_batch::RecordBatch, GqlError> {
    use arrow::array::*;

    let mut builders: Vec<TypedColumnBuilder> = schema
        .fields()
        .iter()
        .map(|f| TypedColumnBuilder::new(f.data_type().clone(), chunk.len()))
        .collect();

    for binding in chunk {
        for (i, col_name) in col_names.iter().enumerate() {
            match binding.get(col_name) {
                Some(BoundValue::Scalar(val)) => builders[i].append(Some(val)),
                Some(BoundValue::Node(id)) => builders[i].append_u64(id.0),
                Some(BoundValue::Edge(id)) => builders[i].append_u64(id.0),
                Some(BoundValue::Path(p)) => {
                    let s = format!("{p:?}");
                    builders[i].append_str(&s);
                }
                Some(BoundValue::Group(edges)) => {
                    let s = format!(
                        "[{}]",
                        edges
                            .iter()
                            .map(|e| e.0.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    builders[i].append_str(&s);
                }
                None => builders[i].append(None),
            }
        }
    }

    let columns: Vec<Arc<dyn Array>> = builders.into_iter().map(|b| b.finish()).collect();
    arrow::record_batch::RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| GqlError::internal(format!("Arrow error: {e}")))
}

/// Materialize a DataChunk directly to Arrow RecordBatches.
///
/// Maps chunk columns to output schema aliases by name. Since DataChunk
/// columns are already Arrow-backed, this avoids rebuilding arrays from
/// scratch (the main cost of the binding-based `materialize_to_arrow`).
/// The `Values` column variant is the only one that requires conversion
/// (GqlValue -> StringArray).
pub(super) fn materialize_chunk_to_arrow(
    chunk: &crate::types::chunk::DataChunk,
    aliases: &[IStr],
) -> Result<
    (
        Arc<arrow::datatypes::Schema>,
        Vec<arrow::record_batch::RecordBatch>,
    ),
    GqlError,
> {
    use arrow::datatypes::{DataType, Field};

    if chunk.active_len() == 0 || aliases.is_empty() {
        let fields: Vec<Field> = aliases
            .iter()
            .map(|a| Field::new(a.as_str(), DataType::Utf8, true))
            .collect();
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        return Ok((
            Arc::clone(&schema),
            vec![arrow::record_batch::RecordBatch::new_empty(schema)],
        ));
    }

    // Compact once if selection is sparse (applies filter, produces dense chunk)
    let dense = if chunk.selection().is_dense() {
        std::borrow::Cow::Borrowed(chunk)
    } else {
        std::borrow::Cow::Owned(chunk.compact())
    };

    // Build schema from actual column types, ordered by aliases
    let schema = dense.schema();
    let fields: Vec<Field> = aliases
        .iter()
        .map(|alias| {
            if let Some(slot) = schema.slot_of(alias) {
                let kind = schema.kind_at(slot).unwrap();
                Field::new(alias.as_str(), kind.arrow_data_type(), true)
            } else {
                // Column not found in chunk (should not happen after RETURN)
                Field::new(alias.as_str(), DataType::Utf8, true)
            }
        })
        .collect();
    let arrow_schema = Arc::new(arrow::datatypes::Schema::new(fields));

    // Collect arrays in alias order (handles column reordering)
    let arrays: Vec<arrow::array::ArrayRef> = aliases
        .iter()
        .map(|alias| {
            if let Some(slot) = schema.slot_of(alias) {
                dense.column(slot).as_array_ref()
            } else {
                // Missing column: fill with null-valued StringArray (matches Utf8 schema)
                let mut builder = arrow::array::StringBuilder::new();
                for _ in 0..dense.len() {
                    builder.append_null();
                }
                Arc::new(builder.finish()) as arrow::array::ArrayRef
            }
        })
        .collect();

    let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&arrow_schema), arrays)
        .map_err(|e| GqlError::internal(format!("Arrow RecordBatch from chunk: {e}")))?;

    Ok((arrow_schema, vec![batch]))
}

/// Typed column builder wrapping Arrow builders for single-pass materialization.
enum TypedColumnBuilder {
    Int64(arrow::array::Int64Builder),
    UInt64(arrow::array::UInt64Builder),
    Float64(arrow::array::Float64Builder),
    Boolean(arrow::array::BooleanBuilder),
    Utf8(arrow::array::StringBuilder),
}

impl TypedColumnBuilder {
    fn new(dt: arrow::datatypes::DataType, capacity: usize) -> Self {
        use arrow::datatypes::DataType;
        match dt {
            DataType::Int64 => Self::Int64(arrow::array::Int64Builder::with_capacity(capacity)),
            DataType::UInt64 => Self::UInt64(arrow::array::UInt64Builder::with_capacity(capacity)),
            DataType::Float64 => {
                Self::Float64(arrow::array::Float64Builder::with_capacity(capacity))
            }
            DataType::Boolean => {
                Self::Boolean(arrow::array::BooleanBuilder::with_capacity(capacity))
            }
            _ => Self::Utf8(arrow::array::StringBuilder::new()),
        }
    }

    fn append(&mut self, val: Option<&GqlValue>) {
        match self {
            Self::Int64(b) => match val {
                Some(GqlValue::Int(i)) => b.append_value(*i),
                Some(GqlValue::UInt(u)) => b.append_value(*u as i64),
                Some(GqlValue::ZonedDateTime(zdt)) => b.append_value(zdt.nanos),
                _ => b.append_null(),
            },
            Self::UInt64(b) => match val {
                Some(GqlValue::UInt(u)) => b.append_value(*u),
                Some(GqlValue::Int(i)) => b.append_value(*i as u64),
                _ => b.append_null(),
            },
            Self::Float64(b) => match val {
                Some(GqlValue::Float(f)) => b.append_value(*f),
                Some(GqlValue::Int(i)) => b.append_value(*i as f64),
                Some(GqlValue::UInt(u)) => b.append_value(*u as f64),
                _ => b.append_null(),
            },
            Self::Boolean(b) => match val {
                Some(GqlValue::Bool(v)) => b.append_value(*v),
                _ => b.append_null(),
            },
            Self::Utf8(b) => match val {
                Some(GqlValue::Null) | None => b.append_null(),
                Some(GqlValue::String(s)) => b.append_value(s.as_str()),
                Some(v) => b.append_value(format!("{v}")),
            },
        }
    }

    /// Append a u64 value (Node/Edge ID) directly.
    fn append_u64(&mut self, val: u64) {
        match self {
            Self::UInt64(b) => b.append_value(val),
            Self::Int64(b) => b.append_value(val as i64),
            Self::Utf8(b) => b.append_value(val.to_string()),
            _ => match self {
                Self::Float64(b) => b.append_value(val as f64),
                Self::Boolean(b) => b.append_null(),
                _ => unreachable!(),
            },
        }
    }

    /// Append a string value directly (no clone).
    fn append_str(&mut self, s: &str) {
        match self {
            Self::Utf8(b) => b.append_value(s),
            _ => self.append(None), // type mismatch → null
        }
    }

    fn finish(self) -> Arc<dyn arrow::array::Array> {
        match self {
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::UInt64(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
        }
    }
}
