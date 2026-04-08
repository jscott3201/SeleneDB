//! Arrow-to-JSON and Arrow-to-IPC serialization helpers.

/// Convert Arrow RecordBatches to JSON array string.
pub(super) fn batches_to_json(
    batches: &[arrow::record_batch::RecordBatch],
    schema: &arrow::datatypes::Schema,
) -> String {
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = arrow_value_to_json(col.as_ref(), row_idx);
                obj.insert(field.name().clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }
    serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string())
}

/// Extract a single cell value from an Arrow array as JSON.
fn arrow_value_to_json(array: &dyn arrow::array::Array, idx: usize) -> serde_json::Value {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::DataType;

    if array.is_null(idx) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::json!(a.value(idx))
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            serde_json::json!(a.value(idx))
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::json!(a.value(idx))
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::json!(a.value(idx))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::json!(a.value(idx))
        }
        _ => serde_json::Value::Null,
    }
}

/// Convert Arrow RecordBatches to IPC bytes.
pub(super) fn batches_to_ipc(
    batches: &[arrow::record_batch::RecordBatch],
    schema: &arrow::datatypes::Schema,
) -> Vec<u8> {
    use arrow::ipc::writer::StreamWriter;
    use std::sync::Arc;

    let mut buf = Vec::new();
    let schema = Arc::new(schema.clone());
    if let Ok(mut writer) = StreamWriter::try_new(&mut buf, &schema) {
        for batch in batches {
            let _ = writer.write(batch);
        }
        let _ = writer.finish();
    }
    buf
}
