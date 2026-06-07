//! Reads an event-time column from an Arrow `RecordBatch` and converts each
//! row to a `u64` milliseconds-since-epoch value.
//!
//! Supports:
//!   * Arrow `Timestamp(Second|Millisecond|Microsecond|Nanosecond)` columns —
//!     self-describing, the configured `EventTimeUnit` (if any) is ignored.
//!   * `Int64` / `UInt64` columns with a required `EventTimeUnit` annotation
//!     (`seconds`, `milliseconds`, or `microseconds`).
//!
//! Per-row failure modes (overflow, negative pre-epoch values) silently
//! produce `None` for that row rather than failing the whole batch — the
//! pipeline must keep running on best-effort lag instrumentation per R5a.

use arrow::array::{
    Array, Int64Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use std::fmt;

use crate::topology::{EventTimeConfig, EventTimeUnit};

/// Reader for a single event-time column. Construct once at pipeline scan
/// time; reuse across batches.
#[derive(Debug, Clone)]
pub struct EventTimeReader {
    column_name: String,
    /// Unit annotation for integer columns. Ignored for Arrow `Timestamp(_)`
    /// columns, which self-describe their unit.
    unit: Option<EventTimeUnit>,
}

/// Reasons `read_batch` may return `Err`. None of these are fatal at the
/// pipeline level — the caller (`WrappingExec`) logs once and skips
/// instrumentation for the remaining lifetime of the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventTimeReadError {
    /// The configured column name is not present in the batch schema.
    /// Likely cause: misconfiguration, or the column was projected away by
    /// DataFusion before reaching the wrapping exec.
    ColumnMissing(String),
    /// The column has an Arrow data type the reader does not know how to
    /// interpret as event time (e.g. `Utf8`, `Float64`).
    UnsupportedType { column: String, data_type: DataType },
    /// An `Int64` or `UInt64` column was provided without the required
    /// `unit` annotation in `telemetry.event_time.unit`.
    UnitRequired(String),
}

impl fmt::Display for EventTimeReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EventTimeReadError::ColumnMissing(col) => {
                write!(f, "event-time column '{}' not found in batch schema", col)
            }
            EventTimeReadError::UnsupportedType { column, data_type } => write!(
                f,
                "event-time column '{}' has unsupported data type {:?}; supported: \
                 Timestamp(_) and Int64/UInt64 (with `unit`)",
                column, data_type
            ),
            EventTimeReadError::UnitRequired(col) => write!(
                f,
                "event-time column '{}' is an integer type but `telemetry.event_time.unit` \
                 was not configured (one of: seconds, milliseconds, microseconds)",
                col
            ),
        }
    }
}

impl std::error::Error for EventTimeReadError {}

impl EventTimeReader {
    pub fn new(column_name: impl Into<String>, unit: Option<EventTimeUnit>) -> Self {
        Self {
            column_name: column_name.into(),
            unit,
        }
    }

    pub fn from_config(config: &EventTimeConfig) -> Self {
        Self::new(config.column.clone(), config.unit.clone())
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Iterate per-row event-time values in milliseconds since the unix
    /// epoch, invoking `f(Some(ms))` for resolved rows and `f(None)` for
    /// null / overflow / pre-epoch rows. Iterates `batch.num_rows()` times
    /// in total.
    ///
    /// Callback-based so the hot path (histogram `record` + running max)
    /// can run in one pass without allocating an intermediate
    /// `Vec<Option<u64>>` per batch. A pipeline with telemetry configured
    /// on source + transform + sink avoids three per-batch allocations.
    pub fn for_each_value(
        &self,
        batch: &RecordBatch,
        mut f: impl FnMut(Option<u64>),
    ) -> Result<(), EventTimeReadError> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            // Skip the column lookup on empty batches — defensive against
            // sources that emit empty batches before any schema-bearing
            // ones, and saves work in steady state.
            return Ok(());
        }
        let array = batch
            .column_by_name(&self.column_name)
            .ok_or_else(|| EventTimeReadError::ColumnMissing(self.column_name.clone()))?;

        match array.data_type() {
            DataType::Timestamp(time_unit, _) => {
                timestamp_for_each_value(array.as_ref(), *time_unit, &self.column_name, &mut f)
            }
            DataType::Int64 => {
                let unit = self
                    .unit
                    .as_ref()
                    .ok_or_else(|| EventTimeReadError::UnitRequired(self.column_name.clone()))?;
                let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    EventTimeReadError::UnsupportedType {
                        column: self.column_name.clone(),
                        data_type: array.data_type().clone(),
                    }
                })?;
                int64_for_each_value(arr, unit, &mut f);
                Ok(())
            }
            DataType::UInt64 => {
                let unit = self
                    .unit
                    .as_ref()
                    .ok_or_else(|| EventTimeReadError::UnitRequired(self.column_name.clone()))?;
                let arr = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| EventTimeReadError::UnsupportedType {
                        column: self.column_name.clone(),
                        data_type: array.data_type().clone(),
                    })?;
                uint64_for_each_value(arr, unit, &mut f);
                Ok(())
            }
            _ => Err(EventTimeReadError::UnsupportedType {
                column: self.column_name.clone(),
                data_type: array.data_type().clone(),
            }),
        }
    }

    /// Convenience adapter that collects `for_each_value` into a vec. Kept
    /// for tests and ad-hoc callers that want the whole batch at once; the
    /// hot pipeline path should prefer `for_each_value` to skip the
    /// allocation.
    pub fn read_batch(&self, batch: &RecordBatch) -> Result<Vec<Option<u64>>, EventTimeReadError> {
        let mut out = Vec::with_capacity(batch.num_rows());
        self.for_each_value(batch, |v| out.push(v))?;
        Ok(out)
    }
}

/// Convert a signed seconds-since-epoch value to milliseconds, returning
/// `None` for negative inputs (pre-epoch) or overflow.
#[inline]
fn i64_seconds_to_ms(value: i64) -> Option<u64> {
    if value < 0 {
        return None;
    }
    (value as u64).checked_mul(1_000)
}

#[inline]
fn i64_to_u64_nonneg(value: i64) -> Option<u64> {
    if value < 0 { None } else { Some(value as u64) }
}

fn timestamp_for_each_value(
    array: &dyn Array,
    time_unit: TimeUnit,
    column_name: &str,
    f: &mut impl FnMut(Option<u64>),
) -> Result<(), EventTimeReadError> {
    let num_rows = array.len();
    // Preserve the full data_type (including timezone) in the error so the
    // user-facing message accurately reflects the observed column type.
    let unsupported = |column: &str, arr: &dyn Array| EventTimeReadError::UnsupportedType {
        column: column.to_string(),
        data_type: arr.data_type().clone(),
    };

    match time_unit {
        TimeUnit::Second => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| unsupported(column_name, array))?;
            for i in 0..num_rows {
                if arr.is_null(i) {
                    f(None);
                } else {
                    f(i64_seconds_to_ms(arr.value(i)));
                }
            }
        }
        TimeUnit::Millisecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| unsupported(column_name, array))?;
            for i in 0..num_rows {
                if arr.is_null(i) {
                    f(None);
                } else {
                    f(i64_to_u64_nonneg(arr.value(i)));
                }
            }
        }
        TimeUnit::Microsecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| unsupported(column_name, array))?;
            for i in 0..num_rows {
                if arr.is_null(i) {
                    f(None);
                } else {
                    // Floor-divide; sub-millisecond precision is dropped on purpose.
                    f(i64_to_u64_nonneg(arr.value(i)).map(|v| v / 1_000));
                }
            }
        }
        TimeUnit::Nanosecond => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| unsupported(column_name, array))?;
            for i in 0..num_rows {
                if arr.is_null(i) {
                    f(None);
                } else {
                    f(i64_to_u64_nonneg(arr.value(i)).map(|v| v / 1_000_000));
                }
            }
        }
    }
    Ok(())
}

fn int64_for_each_value(arr: &Int64Array, unit: &EventTimeUnit, f: &mut impl FnMut(Option<u64>)) {
    for i in 0..arr.len() {
        if arr.is_null(i) {
            f(None);
            continue;
        }
        let raw = arr.value(i);
        let ms = match unit {
            EventTimeUnit::Seconds => i64_seconds_to_ms(raw),
            EventTimeUnit::Milliseconds => i64_to_u64_nonneg(raw),
            EventTimeUnit::Microseconds => i64_to_u64_nonneg(raw).map(|v| v / 1_000),
        };
        f(ms);
    }
}

fn uint64_for_each_value(arr: &UInt64Array, unit: &EventTimeUnit, f: &mut impl FnMut(Option<u64>)) {
    for i in 0..arr.len() {
        if arr.is_null(i) {
            f(None);
            continue;
        }
        let raw = arr.value(i);
        let ms = match unit {
            // Stay in u64 throughout — `UInt64` callers may carry values
            // beyond `i64::MAX`, and round-tripping through i64 would lose
            // them. `checked_mul` guards seconds→ms overflow.
            EventTimeUnit::Seconds => raw.checked_mul(1_000),
            EventTimeUnit::Milliseconds => Some(raw),
            EventTimeUnit::Microseconds => Some(raw / 1_000),
        };
        f(ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn schema_one(name: &str, dt: DataType) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(name, dt, true)]))
    }

    fn batch_with(array: Arc<dyn Array>, schema: Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    // ------------------------------------------------------------------
    // Happy paths — Timestamp(_)
    // ------------------------------------------------------------------

    #[test]
    fn read_timestamp_millisecond_no_nulls() {
        let arr: Arc<dyn Array> = Arc::new(TimestampMillisecondArray::from(vec![
            1_000_i64, 2_500, 3_141, 0, 999_999,
        ]));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Millisecond, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(
            values,
            vec![
                Some(1_000),
                Some(2_500),
                Some(3_141),
                Some(0),
                Some(999_999)
            ]
        );
    }

    #[test]
    fn read_timestamp_second_multiplies_by_1000() {
        let arr: Arc<dyn Array> =
            Arc::new(TimestampSecondArray::from(vec![1_i64, 2, 1_700_000_000]));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Second, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(
            values,
            vec![Some(1_000), Some(2_000), Some(1_700_000_000_000)]
        );
    }

    #[test]
    fn read_timestamp_microsecond_floors_to_ms() {
        let arr: Arc<dyn Array> = Arc::new(TimestampMicrosecondArray::from(vec![
            1_999_i64, 2_000, 999, 0,
        ]));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Microsecond, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        // 1_999us → 1ms (floor), 2_000us → 2ms, 999us → 0ms (floor), 0us → 0ms
        assert_eq!(values, vec![Some(1), Some(2), Some(0), Some(0)]);
    }

    #[test]
    fn read_timestamp_nanosecond_floors_to_ms() {
        let arr: Arc<dyn Array> = Arc::new(TimestampNanosecondArray::from(vec![
            1_500_000_i64,
            2_000_000,
            999_999,
        ]));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Nanosecond, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![Some(1), Some(2), Some(0)]);
    }

    #[test]
    fn read_timestamp_unit_annotation_ignored_for_arrow_self_describing() {
        // Configuring `unit: seconds` on a Timestamp(Second) column must
        // produce identical output to leaving `unit` unset — Arrow self-describes.
        let arr: Arc<dyn Array> = Arc::new(TimestampSecondArray::from(vec![10_i64, 20]));
        let schema = schema_one("t", DataType::Timestamp(TimeUnit::Second, None));
        let batch = batch_with(arr, schema);
        let with_unit = EventTimeReader::new("t", Some(EventTimeUnit::Seconds));
        let without_unit = EventTimeReader::new("t", None);
        assert_eq!(
            with_unit.read_batch(&batch).unwrap(),
            without_unit.read_batch(&batch).unwrap()
        );
    }

    // ------------------------------------------------------------------
    // Happy paths — Int64 / UInt64 with unit
    // ------------------------------------------------------------------

    #[test]
    fn read_int64_seconds_multiplies_by_1000() {
        let arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1_i64, 2, 1_700_000_000]));
        let batch = batch_with(arr, schema_one("t", DataType::Int64));
        let reader = EventTimeReader::new("t", Some(EventTimeUnit::Seconds));
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(
            values,
            vec![Some(1_000), Some(2_000), Some(1_700_000_000_000)]
        );
    }

    #[test]
    fn read_int64_milliseconds_unchanged() {
        let arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1_000_i64, 2_500]));
        let batch = batch_with(arr, schema_one("t", DataType::Int64));
        let reader = EventTimeReader::new("t", Some(EventTimeUnit::Milliseconds));
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![Some(1_000), Some(2_500)]);
    }

    #[test]
    fn read_uint64_microseconds_floors() {
        let arr: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![1_999_u64, 2_000, 999]));
        let batch = batch_with(arr, schema_one("t", DataType::UInt64));
        let reader = EventTimeReader::new("t", Some(EventTimeUnit::Microseconds));
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![Some(1), Some(2), Some(0)]);
    }

    #[test]
    fn read_uint64_milliseconds_preserves_max_value() {
        // u64::MAX must round-trip without truncation through i64.
        let arr: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![u64::MAX]));
        let batch = batch_with(arr, schema_one("t", DataType::UInt64));
        let reader = EventTimeReader::new("t", Some(EventTimeUnit::Milliseconds));
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![Some(u64::MAX)]);
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn empty_batch_returns_empty_vec() {
        let arr: Arc<dyn Array> = Arc::new(TimestampMillisecondArray::from(Vec::<i64>::new()));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Millisecond, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        assert!(values.is_empty());
    }

    #[test]
    fn nulls_become_none() {
        let arr: Arc<dyn Array> = Arc::new(TimestampMillisecondArray::from(vec![
            Some(1_000_i64),
            None,
            Some(3_000),
            None,
        ]));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Millisecond, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![Some(1_000), None, Some(3_000), None]);
    }

    #[test]
    fn timestamp_second_near_i64_max_overflows_to_none() {
        // i64::MAX seconds * 1000 overflows u64; expect None.
        let arr: Arc<dyn Array> = Arc::new(TimestampSecondArray::from(vec![i64::MAX]));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Second, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![None]);
    }

    #[test]
    fn timestamp_second_max_safe_value_converts() {
        // Largest value that does not overflow when multiplied by 1000.
        let safe = (u64::MAX / 1_000) as i64;
        let arr: Arc<dyn Array> = Arc::new(TimestampSecondArray::from(vec![safe]));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Second, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![Some(safe as u64 * 1_000)]);
    }

    #[test]
    fn negative_timestamp_value_is_none() {
        let arr: Arc<dyn Array> =
            Arc::new(TimestampMillisecondArray::from(vec![-1_i64, -1_000, 5_000]));
        let batch = batch_with(
            arr,
            schema_one("t", DataType::Timestamp(TimeUnit::Millisecond, None)),
        );
        let reader = EventTimeReader::new("t", None);
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![None, None, Some(5_000)]);
    }

    #[test]
    fn negative_int64_value_is_none() {
        let arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![-5_i64, 100, -1]));
        let batch = batch_with(arr, schema_one("t", DataType::Int64));
        let reader = EventTimeReader::new("t", Some(EventTimeUnit::Seconds));
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![None, Some(100_000), None]);
    }

    #[test]
    fn uint64_seconds_overflow_is_none() {
        // u64::MAX seconds * 1000 overflows checked_mul.
        let arr: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![u64::MAX, 5]));
        let batch = batch_with(arr, schema_one("t", DataType::UInt64));
        let reader = EventTimeReader::new("t", Some(EventTimeUnit::Seconds));
        let values = reader.read_batch(&batch).unwrap();
        assert_eq!(values, vec![None, Some(5_000)]);
    }

    // ------------------------------------------------------------------
    // Error paths
    // ------------------------------------------------------------------

    #[test]
    fn missing_column_returns_column_missing() {
        let arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1_i64]));
        let batch = batch_with(arr, schema_one("present", DataType::Int64));
        let reader = EventTimeReader::new("absent", Some(EventTimeUnit::Milliseconds));
        let err = reader.read_batch(&batch).unwrap_err();
        assert_eq!(err, EventTimeReadError::ColumnMissing("absent".to_string()));
    }

    #[test]
    fn unsupported_type_utf8_returns_error() {
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["2026-04-13"]));
        let batch = batch_with(arr, schema_one("t", DataType::Utf8));
        let reader = EventTimeReader::new("t", None);
        let err = reader.read_batch(&batch).unwrap_err();
        assert!(matches!(err, EventTimeReadError::UnsupportedType { .. }));
    }

    #[test]
    fn unsupported_type_float64_returns_error() {
        let arr: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.0_f64]));
        let batch = batch_with(arr, schema_one("t", DataType::Float64));
        let reader = EventTimeReader::new("t", Some(EventTimeUnit::Milliseconds));
        let err = reader.read_batch(&batch).unwrap_err();
        assert!(matches!(err, EventTimeReadError::UnsupportedType { .. }));
    }

    #[test]
    fn int64_without_unit_returns_unit_required() {
        let arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1_i64]));
        let batch = batch_with(arr, schema_one("t", DataType::Int64));
        let reader = EventTimeReader::new("t", None);
        let err = reader.read_batch(&batch).unwrap_err();
        assert_eq!(err, EventTimeReadError::UnitRequired("t".to_string()));
    }

    #[test]
    fn uint64_without_unit_returns_unit_required() {
        let arr: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![1_u64]));
        let batch = batch_with(arr, schema_one("t", DataType::UInt64));
        let reader = EventTimeReader::new("t", None);
        let err = reader.read_batch(&batch).unwrap_err();
        assert_eq!(err, EventTimeReadError::UnitRequired("t".to_string()));
    }
}
