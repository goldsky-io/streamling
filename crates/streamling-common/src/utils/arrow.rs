//! Arrow array utilities for safe operations that prevent overflow
//!
//! This module provides safe wrappers around Arrow operations that can overflow
//! with very large arrays, particularly when dealing with arrow-select's take kernel.

use arrow::array::{
    Array, ArrayRef, AsArray, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    PrimitiveArray, RecordBatch, StructArray, UInt32Array, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::kernels;
use arrow::datatypes::{DataType, Field, Fields, Int64Type, Schema, SchemaRef};
use datafusion::common::{Result, exec_datafusion_err};
use std::cell::Cell;
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::thread;

// Thread-local flag to suppress panic logging in controlled contexts.
// When `safe_take` spawns an isolated thread to catch overflow panics,
// it sets this flag so the global panic hook knows to skip logging/termination.
thread_local! {
    /// Flag indicating this thread is in a controlled panic-catching context.
    /// The global panic hook should check this and skip logging if true.
    pub static SUPPRESS_PANIC_LOGGING: Cell<bool> = const { Cell::new(false) };
}

/// Check if panic logging should be suppressed for the current thread.
/// Used by the global panic hook to avoid logging expected panics from `safe_take`.
pub fn should_suppress_panic_logging() -> bool {
    SUPPRESS_PANIC_LOGGING.with(|flag| flag.get())
}

/// RAII guard that sets `SUPPRESS_PANIC_LOGGING` to `true` for the current
/// thread on construction and clears it on drop.
///
/// Use this any time you intentionally provoke a panic that
/// `catch_unwind` will recover from, so that the global panic hook stays
/// silent for the expected case. Without RAII scoping, the flag stays
/// set on the OS thread after the guarded block exits — and if anything
/// reuses that thread (or if the closure is moved into a thread pool),
/// unrelated future panics on the same thread get suppressed too, which
/// hides real bugs.
pub struct SuppressPanicLoggingGuard {
    // Marker only — Drop impl resets the flag. Field exists so consumers
    // can't construct via struct literal and bypass `new()`.
    _private: (),
}

impl SuppressPanicLoggingGuard {
    /// Create a guard, setting the thread-local flag to true.
    pub fn new() -> Self {
        SUPPRESS_PANIC_LOGGING.with(|flag| flag.set(true));
        Self { _private: () }
    }
}

impl Default for SuppressPanicLoggingGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SuppressPanicLoggingGuard {
    fn drop(&mut self) {
        SUPPRESS_PANIC_LOGGING.with(|flag| flag.set(false));
    }
}

/// Build a schema from actual column types to handle type upgrades (e.g., Utf8 -> LargeUtf8).
///
/// When `safe_take` promotes types to handle overflow, downstream operators need to
/// update their schema to match the actual data types. This helper builds a new schema
/// by combining the field names/nullability from the original schema with the actual
/// data types from the columns.
///
/// # Arguments
///
/// * `original_schema` - The original schema with field names and nullability info
/// * `columns` - The actual columns whose data types should be used
/// * `metadata` - Metadata to attach to the new schema
///
/// # Returns
///
/// A new schema with field types matching the actual column types
pub fn build_schema_from_columns(
    original_schema: &SchemaRef,
    columns: &[ArrayRef],
    metadata: HashMap<String, String>,
) -> SchemaRef {
    let actual_fields: Vec<Field> = original_schema
        .fields()
        .iter()
        .zip(columns.iter())
        .map(|(field, col)| {
            Field::new(field.name(), col.data_type().clone(), field.is_nullable())
                .with_metadata(field.metadata().clone())
        })
        .collect();

    Arc::new(Schema::new_with_metadata(actual_fields, metadata))
}

/// Create a NullBuffer from a vector of booleans where true = null, false = valid
fn create_null_buffer(nulls: &[bool]) -> Option<NullBuffer> {
    let null_count = nulls.iter().filter(|&&n| n).count();
    if null_count == 0 {
        None
    } else {
        // NullBuffer uses: true = valid, false = null (opposite of our nulls vec)
        Some(NullBuffer::from_iter(nulls.iter().map(|&n| !n)))
    }
}

/// Rebuild a ListArray from taken values and new offsets
fn rebuild_list_array(
    field: &Field,
    new_offsets: Vec<i32>,
    taken_values: ArrayRef,
    nulls: &[bool],
) -> ArrayRef {
    let null_buffer = create_null_buffer(nulls);
    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(new_offsets));

    Arc::new(ListArray::new(
        Arc::new(Field::new(
            field.name(),
            taken_values.data_type().clone(),
            field.is_nullable(),
        )),
        offsets_buffer,
        taken_values,
        null_buffer,
    )) as ArrayRef
}

/// Rebuild a LargeListArray from taken values and new offsets
fn rebuild_large_list_array(
    field: &Field,
    new_offsets: Vec<i64>,
    taken_values: ArrayRef,
    nulls: &[bool],
) -> ArrayRef {
    let null_buffer = create_null_buffer(nulls);
    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(new_offsets));

    Arc::new(LargeListArray::new(
        Arc::new(Field::new(
            field.name(),
            taken_values.data_type().clone(),
            field.is_nullable(),
        )),
        offsets_buffer,
        taken_values,
        null_buffer,
    )) as ArrayRef
}

/// Result type for the isolated take operation
enum IsolatedTakeResult {
    /// Native take succeeded
    Success(ArrayRef),
    /// Native take returned an Arrow error
    ArrowError(String),
    /// Native take panicked (overflow) - caught before panic hook fires
    Panicked,
}

/// Check if an Arrow error message indicates an offset overflow.
///
/// Arrow 58+ returns `"Offset overflow error: <value>"` as an `ArrowError`
/// (rather than panicking) when the cumulative byte offset of a `Utf8`/`Binary`
/// column exceeds `i32::MAX` during a `take` operation. This helper detects
/// that error so we can fall back to `manual_safe_take` for type promotion.
fn is_offset_overflow_error(msg: &str) -> bool {
    msg.contains("Offset overflow")
}

/// Safe wrapper around arrow's take kernel that catches overflow panics.
///
/// Arrow-select's `take` kernel can panic with "overflow" when processing very large
/// String/Binary arrays (> 2GB of string data). This wrapper:
///
/// 1. Tries the fast native `take` kernel in a separate thread
/// 2. Catches the panic INSIDE the thread (before panic hook fires)
/// 3. Falls back to manual rebuild, promoting to LargeUtf8/LargeBinary as needed
///
/// # Panic Isolation
///
/// By using `catch_unwind` inside the spawned thread:
/// - The panic is caught before the global panic hook fires
/// - No alarming error logs are produced
/// - No plugins are terminated
/// - We get a clean fallback to manual implementation
///
/// # Arguments
///
/// * `arr` - The source array to take values from
/// * `indices` - Array of i64 indices specifying which values to take
///
/// # Returns
///
/// A new array with values taken from `arr` according to `indices`.
/// Note: String types may be promoted to LargeUtf8 if overflow would occur.
pub fn safe_take(arr: &ArrayRef, indices: &PrimitiveArray<Int64Type>) -> Result<ArrayRef> {
    // Clone refs for the spawned thread
    let arr_clone = Arc::clone(arr);
    let indices_clone = indices.clone();

    // Run native take in a separate thread, catching panics INSIDE the thread.
    // The `SuppressPanicLoggingGuard` sets `SUPPRESS_PANIC_LOGGING` so the
    // global panic hook stays silent for this expected overflow panic, and
    // resets it on drop so an OS thread that survives this closure (e.g.
    // reused by a thread pool, or with thread-local storage that outlives
    // the closure) does not have unrelated future panics silenced.
    let handle = thread::spawn(move || {
        let _suppress = SuppressPanicLoggingGuard::new();
        match catch_unwind(AssertUnwindSafe(|| {
            kernels::take::take(&arr_clone, &indices_clone, None)
        })) {
            Ok(Ok(result)) => IsolatedTakeResult::Success(result),
            Ok(Err(arrow_err)) => IsolatedTakeResult::ArrowError(arrow_err.to_string()),
            Err(_) => IsolatedTakeResult::Panicked,
        }
    });

    match handle.join() {
        Ok(IsolatedTakeResult::Success(result)) => Ok(result),
        Ok(IsolatedTakeResult::ArrowError(err)) => {
            if is_offset_overflow_error(&err) {
                tracing::warn!(
                    "Arrow take overflow (ArrowError) detected for type {:?}, using manual safe path. \
                     This means minimally slower performance but could also signal a batch size that is \
                     too large if seen multiple times a second.",
                    arr.data_type()
                );
                manual_safe_take(arr, indices)
            } else {
                Err(exec_datafusion_err!("Arrow take error: {}", err))
            }
        }
        Ok(IsolatedTakeResult::Panicked) => {
            // Panic was caught inside the thread - no panic hook fired
            tracing::warn!(
                "Arrow take overflow detected, using manual safe path for array type {:?}. This means minimally slower performance but could also signal a batch size that is too large if seen multiple times a second.",
                arr.data_type()
            );
            manual_safe_take(arr, indices)
        }
        Err(_join_err) => {
            // Thread itself failed to join (very rare, usually means thread was killed)
            tracing::warn!(
                "Arrow take failed, falling back to manual path for array type {:?}, This means minimally slower performance but could also signal a batch size that is too large if seen multiple times a second.",
                arr.data_type()
            );
            manual_safe_take(arr, indices)
        }
    }
}

/// Safe wrapper around arrow's `take_record_batch` that recovers from the
/// same `take_bytes` overflow panic [`safe_take`] handles, applied per
/// column.
///
/// The native `arrow::compute::take_record_batch` iterates columns and
/// calls the unsafe-on-overflow take kernel on each. For deeply nested
/// schemas with large cumulative byte payloads (e.g. Ethereum logs nested
/// inside receipts inside blocks), the kernel panics with
/// `Option::expect("overflow")` deep inside `take_bytes`. Without a
/// `catch_unwind` somewhere in the chain, the panic propagates past the
/// `extern "C"` plugin boundary and the process dies with exit
/// 132 (SIGILL on x86_64) or 133 (SIGTRAP on ARM64) with no panic banner.
///
/// This helper tries the fast native path inside `catch_unwind`. On
/// panic, it rebuilds the batch column-by-column using [`safe_take`],
/// which promotes overflowing `Utf8`/`Binary` columns to their
/// `Large…` variants. The resulting `RecordBatch` may have a wider
/// schema than the input — see [`build_schema_from_columns`] for the
/// schema reconciliation helper.
pub fn safe_take_record_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch> {
    // Try the native fast path under catch_unwind. We use the
    // `SuppressPanicLoggingGuard` so the host's global panic hook stays
    // silent for the expected overflow case — the same contract `safe_take`
    // uses. The guard's Drop resets the flag so a future unrelated panic
    // on this thread is still visible.
    let result = {
        let _suppress = SuppressPanicLoggingGuard::new();
        catch_unwind(AssertUnwindSafe(|| {
            arrow::compute::take_record_batch(batch, indices)
        }))
    };
    match result {
        Ok(Ok(rb)) => Ok(rb),
        Ok(Err(e)) => {
            let msg = e.to_string();
            if is_offset_overflow_error(&msg) {
                tracing::warn!(
                    "Arrow take_record_batch overflow (ArrowError) detected on {} columns, \
                     falling back to per-column safe_take. This means minimally slower performance \
                     but could also signal a batch size that is too large if seen multiple times a second.",
                    batch.num_columns()
                );
                rebuild_batch_with_safe_take(batch, indices)
            } else {
                Err(exec_datafusion_err!("Arrow take_record_batch error: {e}"))
            }
        }
        Err(_panic) => {
            tracing::warn!(
                "Arrow take_record_batch overflow (panic) detected on {} columns, falling back to \
                 per-column safe_take. This means minimally slower performance but could also signal \
                 a batch size that is too large if seen multiple times a second.",
                batch.num_columns()
            );
            rebuild_batch_with_safe_take(batch, indices)
        }
    }
}

/// Rebuild a RecordBatch column-by-column using [`safe_take`].
///
/// This is the fallback path when the native `take_record_batch` fails due to
/// overflow (either as a panic or as an ArrowError). It converts the UInt32 indices
/// to Int64 and calls `safe_take` on each column, which handles type promotion
/// for overflowing `Utf8`/`Binary` columns.
fn rebuild_batch_with_safe_take(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch> {
    // Convert UInt32 -> Int64 once; safe_take's signature is Int64.
    // Use `from_iter` (yielding `Option<i64>`) rather than
    // `from_iter_values` so null bits propagate explicitly to the
    // resulting array. `safe_take`'s `manual_safe_take` checks
    // `indices.is_null(i)` first; if we used a non-null array with
    // a sentinel value we'd be depending on the sentinel landing
    // outside `input.len()` after a signed-to-unsigned cast, which
    // is fragile (and arch-dependent for very large sentinels).
    let int64_indices = PrimitiveArray::<Int64Type>::from_iter((0..indices.len()).map(|i| {
        if indices.is_null(i) {
            None
        } else {
            Some(indices.value(i) as i64)
        }
    }));
    let new_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| safe_take(col, &int64_indices))
        .collect::<Result<Vec<_>>>()?;
    let new_schema = build_schema_from_columns(
        batch.schema_ref(),
        &new_columns,
        batch.schema().metadata().clone(),
    );
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| {
        exec_datafusion_err!("Failed to rebuild RecordBatch after safe_take fallback: {e}")
    })
}

/// Manual implementation of take that handles overflow by using larger types.
/// This is called when the native Arrow take kernel panics.
fn manual_safe_take(arr: &ArrayRef, indices: &PrimitiveArray<Int64Type>) -> Result<ArrayRef> {
    let output_len = indices.len();

    match arr.data_type() {
        // String arrays - promote to LargeUtf8 to handle overflow
        DataType::Utf8 => {
            let input = arr.as_string::<i32>();
            let result: Vec<Option<&str>> = (0..indices.len())
                .map(|i| {
                    if indices.is_null(i) {
                        None
                    } else {
                        let idx = indices.value(i) as usize;
                        if idx < input.len() && !input.is_null(idx) {
                            Some(input.value(idx))
                        } else {
                            None
                        }
                    }
                })
                .collect();
            // Use LargeUtf8 (i64 offsets) to prevent overflow
            Ok(Arc::new(LargeStringArray::from(result)) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let input = arr.as_string::<i64>();
            let result: Vec<Option<&str>> = (0..indices.len())
                .map(|i| {
                    if indices.is_null(i) {
                        None
                    } else {
                        let idx = indices.value(i) as usize;
                        if idx < input.len() && !input.is_null(idx) {
                            Some(input.value(idx))
                        } else {
                            None
                        }
                    }
                })
                .collect();
            Ok(Arc::new(LargeStringArray::from(result)) as ArrayRef)
        }

        // Binary arrays - promote to LargeBinary to handle overflow
        DataType::Binary => {
            let input = arr.as_binary::<i32>();
            let result: Vec<Option<&[u8]>> = (0..indices.len())
                .map(|i| {
                    if indices.is_null(i) {
                        None
                    } else {
                        let idx = indices.value(i) as usize;
                        if idx < input.len() && !input.is_null(idx) {
                            Some(input.value(idx))
                        } else {
                            None
                        }
                    }
                })
                .collect();
            // Use LargeBinary (i64 offsets) to prevent overflow
            Ok(Arc::new(LargeBinaryArray::from(result)) as ArrayRef)
        }
        DataType::LargeBinary => {
            let input = arr.as_binary::<i64>();
            let result: Vec<Option<&[u8]>> = (0..indices.len())
                .map(|i| {
                    if indices.is_null(i) {
                        None
                    } else {
                        let idx = indices.value(i) as usize;
                        if idx < input.len() && !input.is_null(idx) {
                            Some(input.value(idx))
                        } else {
                            None
                        }
                    }
                })
                .collect();
            Ok(Arc::new(LargeBinaryArray::from(result)) as ArrayRef)
        }

        // List arrays - rebuild with safe_take on nested values
        DataType::List(field) => {
            let list_arr = arr.as_list::<i32>();
            let values = list_arr.values();

            let mut nested_indices = Vec::new();
            let mut new_offsets = vec![0i32];
            let mut current_offset = 0i32;
            let mut nulls = Vec::new();

            for i in 0..indices.len() {
                if indices.is_null(i) {
                    nulls.push(true);
                    new_offsets.push(current_offset);
                } else {
                    let idx = indices.value(i) as usize;
                    if idx < list_arr.len() && !list_arr.is_null(idx) {
                        nulls.push(false);
                        let offsets = list_arr.value_offsets();
                        let start = offsets[idx] as usize;
                        let end = offsets[idx + 1] as usize;

                        for nested_idx in start..end {
                            nested_indices.push(nested_idx as i64);
                        }
                        current_offset += (end - start) as i32;
                        new_offsets.push(current_offset);
                    } else {
                        nulls.push(true);
                        new_offsets.push(current_offset);
                    }
                }
            }

            let taken_values = if !nested_indices.is_empty() {
                let nested_indices_array = PrimitiveArray::<Int64Type>::from(nested_indices);
                safe_take(values, &nested_indices_array)?
            } else {
                new_null_array(field.data_type(), 0)
            };

            Ok(rebuild_list_array(field, new_offsets, taken_values, &nulls))
        }
        DataType::LargeList(field) => {
            let list_arr = arr.as_list::<i64>();
            let values = list_arr.values();

            let mut nested_indices = Vec::new();
            let mut new_offsets = vec![0i64];
            let mut current_offset = 0i64;
            let mut nulls = Vec::new();

            for i in 0..indices.len() {
                if indices.is_null(i) {
                    nulls.push(true);
                    new_offsets.push(current_offset);
                } else {
                    let idx = indices.value(i) as usize;
                    if idx < list_arr.len() && !list_arr.is_null(idx) {
                        nulls.push(false);
                        let offsets = list_arr.value_offsets();
                        let start = offsets[idx] as usize;
                        let end = offsets[idx + 1] as usize;

                        for nested_idx in start..end {
                            nested_indices.push(nested_idx as i64);
                        }
                        current_offset += (end - start) as i64;
                        new_offsets.push(current_offset);
                    } else {
                        nulls.push(true);
                        new_offsets.push(current_offset);
                    }
                }
            }

            let taken_values = if !nested_indices.is_empty() {
                let nested_indices_array = PrimitiveArray::<Int64Type>::from(nested_indices);
                safe_take(values, &nested_indices_array)?
            } else {
                new_null_array(field.data_type(), 0)
            };

            Ok(rebuild_large_list_array(
                field,
                new_offsets,
                taken_values,
                &nulls,
            ))
        }

        // Struct arrays - rebuild with safe_take on each field
        DataType::Struct(fields) => {
            let struct_arr = arr.as_struct();

            let taken_fields: Vec<ArrayRef> = fields
                .iter()
                .enumerate()
                .map(|(i, _)| safe_take(struct_arr.column(i), indices))
                .collect::<Result<Vec<_>>>()?;

            // Build new fields with potentially updated types
            let new_fields: Vec<Field> = fields
                .iter()
                .zip(taken_fields.iter())
                .map(|(f, arr)| Field::new(f.name(), arr.data_type().clone(), f.is_nullable()))
                .collect();

            // Handle struct-level nulls
            let null_buffer = {
                let mut nulls = Vec::with_capacity(output_len);
                for i in 0..indices.len() {
                    if indices.is_null(i) {
                        nulls.push(true);
                    } else {
                        let idx = indices.value(i) as usize;
                        nulls.push(idx >= struct_arr.len() || struct_arr.is_null(idx));
                    }
                }
                create_null_buffer(&nulls)
            };

            Ok(Arc::new(StructArray::new(
                Fields::from(new_fields),
                taken_fields,
                null_buffer,
            )) as ArrayRef)
        }

        // For other types that panicked, we can't easily rebuild them
        // This shouldn't happen in practice since overflow only affects String/Binary
        _ => Err(exec_datafusion_err!(
            "Cannot manually rebuild array type {:?} after take overflow. \
             This is unexpected - please report this as a bug.",
            arr.data_type()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Int32Array, Int64Array, ListBuilder, StringArray, StringBuilder, UInt32Array,
    };
    use arrow::datatypes::{Field, Fields};

    #[test]
    fn test_safe_take_record_batch_happy_path() {
        // Three columns, three rows, no overflow — exercises the fast path.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let a: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "three"]));
        let batch = RecordBatch::try_new(schema, vec![a, b]).unwrap();

        let indices = UInt32Array::from(vec![2u32, 0]);
        let out = safe_take_record_batch(&batch, &indices).expect("happy path should succeed");
        assert_eq!(out.num_rows(), 2);
        let out_a = out.column(0).as_primitive::<Int64Type>();
        let out_b = out.column(1).as_string::<i32>();
        assert_eq!(out_a.value(0), 30);
        assert_eq!(out_a.value(1), 10);
        assert_eq!(out_b.value(0), "three");
        assert_eq!(out_b.value(1), "one");
    }

    #[test]
    fn test_safe_take_record_batch_preserves_null_indices_on_fallback() {
        // Force the fallback path by constructing a batch whose native
        // take_record_batch would *not* panic (small data), but verifying
        // the conversion logic the fallback path uses. We can't easily
        // trigger the real overflow panic from a unit test, so instead we
        // exercise the UInt32 → Int64 conversion the fallback relies on:
        // null bits in the index array must round-trip through to the
        // converted Int64 array. The previous implementation used a
        // sentinel + `from_iter_values` and was correct only by accident.
        //
        // We assert directly on the conversion: given an indices array
        // with explicit nulls, the converted Int64 array reports the
        // same nulls.
        let indices = UInt32Array::from(vec![Some(0u32), None, Some(2u32)]);
        assert!(indices.is_null(1), "test fixture: index 1 should be null");

        let int64_indices: PrimitiveArray<Int64Type> = (0..indices.len())
            .map(|i| {
                if indices.is_null(i) {
                    None
                } else {
                    Some(indices.value(i) as i64)
                }
            })
            .collect();

        assert!(!int64_indices.is_null(0));
        assert!(
            int64_indices.is_null(1),
            "null bit must propagate from UInt32 indices to Int64 indices on the fallback path"
        );
        assert!(!int64_indices.is_null(2));
        assert_eq!(int64_indices.value(0), 0);
        assert_eq!(int64_indices.value(2), 2);
    }

    #[test]
    fn test_safe_take_record_batch_does_not_leak_suppress_flag() {
        assert!(!should_suppress_panic_logging());
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema, vec![a]).unwrap();
        let indices = UInt32Array::from(vec![0u32, 2]);
        let _ = safe_take_record_batch(&batch, &indices).expect("happy path should succeed");
        assert!(
            !should_suppress_panic_logging(),
            "safe_take_record_batch must clear SUPPRESS_PANIC_LOGGING on return"
        );
    }

    #[test]
    fn test_suppress_guard_sets_and_clears_flag() {
        // Sanity-check default state.
        assert!(
            !should_suppress_panic_logging(),
            "default state on this thread should be suppress=false"
        );

        // Inside the guard, the flag is true.
        {
            let _guard = SuppressPanicLoggingGuard::new();
            assert!(
                should_suppress_panic_logging(),
                "flag must be true while guard is in scope"
            );
        }

        // After the guard drops, the flag is back to false. This is the
        // leak-prevention behaviour: without RAII, the previous
        // `Cell::set(true)` would have left the flag set for the rest of
        // this OS thread's life, silencing any unrelated downstream panic.
        assert!(
            !should_suppress_panic_logging(),
            "flag must be cleared after guard is dropped"
        );
    }

    #[test]
    fn test_safe_take_does_not_leak_suppress_flag_to_caller() {
        // Pre-condition: caller thread has the flag cleared.
        assert!(!should_suppress_panic_logging());

        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let indices = PrimitiveArray::<Int64Type>::from(vec![0i64, 2]);
        let _ = safe_take(&arr, &indices).expect("safe_take should succeed");

        // Post-condition: caller thread's flag is still cleared. `safe_take`
        // spawns its own OS thread and sets the suppress flag there; that
        // flag must never leak onto the calling thread.
        assert!(
            !should_suppress_panic_logging(),
            "safe_take must not leak SUPPRESS_PANIC_LOGGING onto its caller's thread"
        );
    }

    #[test]
    fn test_safe_take_basic_int() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]));
        let indices = PrimitiveArray::<Int64Type>::from(vec![0i64, 2, 4]);

        let result = safe_take(&arr, &indices).expect("safe_take should succeed");
        let result_arr = result.as_primitive::<Int64Type>();

        assert_eq!(result_arr.len(), 3);
        assert_eq!(result_arr.value(0), 10);
        assert_eq!(result_arr.value(1), 30);
        assert_eq!(result_arr.value(2), 50);
    }

    #[test]
    fn test_safe_take_basic_string() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
        let indices = PrimitiveArray::<Int64Type>::from(vec![3i64, 1, 0]);

        let result = safe_take(&arr, &indices).expect("safe_take should succeed");
        let result_arr = result.as_string::<i32>();

        assert_eq!(result_arr.len(), 3);
        assert_eq!(result_arr.value(0), "d");
        assert_eq!(result_arr.value(1), "b");
        assert_eq!(result_arr.value(2), "a");
    }

    #[test]
    fn test_safe_take_with_null_indices() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let indices = PrimitiveArray::<Int64Type>::from(vec![Some(0i64), None, Some(2)]);

        let result = safe_take(&arr, &indices).expect("safe_take should succeed");
        let result_arr = result.as_primitive::<Int64Type>();

        assert_eq!(result_arr.len(), 3);
        assert_eq!(result_arr.value(0), 10);
        assert!(result_arr.is_null(1));
        assert_eq!(result_arr.value(2), 30);
    }

    #[test]
    fn test_safe_take_struct_with_nested_list_utf8() {
        // Create a StructArray with a List<Utf8> field
        let mut list_builder = ListBuilder::new(StringBuilder::new());

        list_builder.values().append_value("hello");
        list_builder.values().append_value("world");
        list_builder.append(true);

        list_builder.values().append_value("foo");
        list_builder.append(true);

        list_builder.append(false); // null list

        let list_array = list_builder.finish();
        let int_array = Int32Array::from(vec![Some(1), Some(2), None]);

        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("strings", list_array.data_type().clone(), true),
                Field::new("numbers", DataType::Int32, true),
            ]),
            vec![
                Arc::new(list_array) as ArrayRef,
                Arc::new(int_array) as ArrayRef,
            ],
            None,
        );

        let arr: ArrayRef = Arc::new(struct_array);
        let indices = PrimitiveArray::<Int64Type>::from(vec![0i64, 1, 0, 2]);

        let result = safe_take(&arr, &indices).expect("safe_take should succeed");
        assert_eq!(result.len(), 4);

        let result_struct = result.as_struct();
        assert_eq!(result_struct.column(0).len(), 4);
        assert_eq!(result_struct.column(1).len(), 4);
    }

    #[test]
    fn test_safe_take_list_with_null_indices() {
        let mut list_builder = ListBuilder::new(StringBuilder::new());

        list_builder.values().append_value("hello");
        list_builder.append(true);

        list_builder.values().append_value("world");
        list_builder.append(true);

        let list_array = list_builder.finish();
        let arr: ArrayRef = Arc::new(list_array);

        let indices = PrimitiveArray::<Int64Type>::from(vec![Some(0i64), None, Some(1)]);

        let result = safe_take(&arr, &indices).expect("safe_take should succeed");
        assert_eq!(result.len(), 3);

        let result_list = result.as_list::<i32>();
        assert!(!result_list.is_null(0));
        assert!(result_list.is_null(1));
        assert!(!result_list.is_null(2));
    }

    #[test]
    fn test_manual_safe_take_string_produces_large_utf8() {
        // Test that manual_safe_take produces LargeUtf8 for Utf8 input
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
        let indices = PrimitiveArray::<Int64Type>::from(vec![0i64, 1, 0]);

        let result = manual_safe_take(&arr, &indices).expect("manual_safe_take should succeed");

        // Should be LargeUtf8, not Utf8
        assert_eq!(result.data_type(), &DataType::LargeUtf8);
        assert_eq!(result.len(), 3);

        let result_arr = result.as_string::<i64>();
        assert_eq!(result_arr.value(0), "hello");
        assert_eq!(result_arr.value(1), "world");
        assert_eq!(result_arr.value(2), "hello");
    }

    #[test]
    fn test_safe_take_repeated_indices() {
        // Test the core unnest use case - repeating values
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let indices = PrimitiveArray::<Int64Type>::from(vec![0i64, 0, 0, 1, 1, 2]);

        let result = safe_take(&arr, &indices).expect("safe_take should succeed");
        assert_eq!(result.len(), 6);
    }

    #[test]
    fn test_is_offset_overflow_error_matches_known_messages() {
        // Arrow 58+ returns "Offset overflow error: <value>" as an ArrowError
        assert!(is_offset_overflow_error(
            "Offset overflow error: 2147483652"
        ));
        assert!(is_offset_overflow_error(
            "Offset overflow error: 2147483656"
        ));
        assert!(is_offset_overflow_error(
            "Offset overflow error: 2147483689"
        ));
        // Partial match should also work
        assert!(is_offset_overflow_error(
            "Some prefix Offset overflow error suffix"
        ));
        // But unrelated errors should not match
        assert!(!is_offset_overflow_error("Out of memory"));
        assert!(!is_offset_overflow_error("Index out of bounds"));
        assert!(!is_offset_overflow_error(""));
    }

    #[test]
    fn test_safe_take_fallback_on_overflow_produces_large_utf8() {
        // This tests that manual_safe_take is correctly called when overflow would occur.
        // We can't easily trigger a real overflow in a unit test, but we can verify
        // that manual_safe_take produces the expected LargeUtf8 type.
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world", "test"]));
        let indices = PrimitiveArray::<Int64Type>::from(vec![0i64, 1, 2, 0, 1]);

        // Call manual_safe_take directly to verify type promotion
        let result = manual_safe_take(&arr, &indices).expect("manual_safe_take should succeed");

        // Should be LargeUtf8, not Utf8
        assert_eq!(result.data_type(), &DataType::LargeUtf8);
        assert_eq!(result.len(), 5);

        let result_arr = result.as_string::<i64>();
        assert_eq!(result_arr.value(0), "hello");
        assert_eq!(result_arr.value(1), "world");
        assert_eq!(result_arr.value(2), "test");
        assert_eq!(result_arr.value(3), "hello");
        assert_eq!(result_arr.value(4), "world");
    }

    #[test]
    fn test_rebuild_batch_with_safe_take_preserves_data() {
        // Test that rebuild_batch_with_safe_take produces correct results
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let a: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "three"]));
        let batch = RecordBatch::try_new(schema, vec![a, b]).unwrap();

        let indices = UInt32Array::from(vec![2u32, 0, 1]);
        let out = rebuild_batch_with_safe_take(&batch, &indices)
            .expect("rebuild_batch_with_safe_take should succeed");

        assert_eq!(out.num_rows(), 3);
        let out_a = out.column(0).as_primitive::<Int64Type>();
        assert_eq!(out_a.value(0), 30);
        assert_eq!(out_a.value(1), 10);
        assert_eq!(out_a.value(2), 20);

        // Note: String column may be promoted to LargeUtf8 by manual_safe_take
        // if it goes through that path, but in normal cases it stays Utf8
    }

    #[test]
    fn test_rebuild_batch_with_safe_take_preserves_null_indices() {
        // Test that null indices are properly preserved
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let a: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "three"]));
        let batch = RecordBatch::try_new(schema, vec![a, b]).unwrap();

        // Index 1 is null
        let indices = UInt32Array::from(vec![Some(2u32), None, Some(0u32)]);
        let out = rebuild_batch_with_safe_take(&batch, &indices)
            .expect("rebuild_batch_with_safe_take should succeed");

        assert_eq!(out.num_rows(), 3);
        let out_a = out.column(0).as_primitive::<Int64Type>();
        assert_eq!(out_a.value(0), 30);
        assert!(out_a.is_null(1), "null index should produce null value");
        assert_eq!(out_a.value(2), 10);
    }
}
