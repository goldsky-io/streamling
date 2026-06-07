use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result;

pub mod json;

pub mod avro;

pub mod ipc;

/// Converter between Arrow's RecordBatch and another format (e.g. JSON)
pub trait FromArrowConverter<T> {
    fn convert_from_batch(&self, batch: &RecordBatch) -> Result<Vec<T>>;
}

/// Converter between a given value (e.g. Avro's GenericRecord) and Arrow's RecordBatch
/// Buffer a batch of values first, then call `to_batch` to convert them to a RecordBatch
/// and empty the buffer
pub trait ToArrowConverter<T> {
    fn buffer(&mut self, value: T);
    fn convert_to_batch(&mut self) -> Result<RecordBatch>;
}
