use crate::error::{Result, ResultExt};
use crate::{streamling_err, streamling_user_err};
use arrow::array::{ArrayRef, Int64Array, LargeStringArray, StringArray, make_comparator};
use arrow::compute::SortOptions;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::Arc;

pub fn deduplicate_record_batch(batch: &RecordBatch, primary_key: &str) -> Result<RecordBatch> {
    if primary_key.is_empty() {
        return Ok(batch.clone());
    }

    let primary_keys = crate::utils::parse_primary_key_columns(primary_key);

    let indices: Result<Vec<usize>> = primary_keys
        .iter()
        .map(|name| {
            batch.schema().index_of(name).map_err(|_| {
                streamling_user_err!("Primary key column '{}' not found in schema", name)
            })
        })
        .collect();
    let indices = indices?;

    if indices.is_empty() {
        return Ok(batch.clone());
    }

    let key_columns: Vec<ArrayRef> = indices.iter().map(|i| batch.column(*i).clone()).collect();

    // Per-column comparators verify primary-key equality on hash collisions.
    // Comparing a column against itself yields `Ordering::Equal` iff the two
    // row indices have semantically equal values (including null vs null).
    let pk_comparators: Vec<_> = key_columns
        .iter()
        .map(|col| make_comparator(col.as_ref(), col.as_ref(), SortOptions::default()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .streamling_context("failed to build primary key comparator for dedup")?;
    let pk_eq = |i: usize, j: usize| -> bool { pk_comparators.iter().all(|cmp| cmp(i, j).is_eq()) };

    // Bucket row indices by their primary-key hash. STRM-5990 was caused by
    // the previous code keying the map on the hash itself, so two distinct
    // PK tuples that hashed to the same `u64` would silently overwrite each
    // other. We still hash for fast bucketing, but resolve hash collisions
    // by comparing the actual primary-key values via `pk_eq`.
    let mut buckets: HashMap<u64, Vec<usize>> = HashMap::with_capacity(batch.num_rows());

    for row_index in 0..batch.num_rows() {
        let mut hasher = DefaultHasher::new();
        for col in &key_columns {
            hash_array_value(&mut hasher, col, row_index);
        }
        let hash = hasher.finish();

        let bucket = buckets.entry(hash).or_default();
        let mut updated = false;
        for existing in bucket.iter_mut() {
            if pk_eq(*existing, row_index) {
                *existing = row_index; // keep the latest occurrence of this PK
                updated = true;
                break;
            }
        }
        if !updated {
            bucket.push(row_index);
        }
    }

    // Surviving indices in their original ascending order, so the relative
    // order of distinct PK tuples is preserved.
    let mut unique_indices: Vec<i64> = buckets.into_values().flatten().map(|i| i as i64).collect();
    unique_indices.sort_unstable();
    let indices_array = Int64Array::from(unique_indices);

    let unique_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| {
            crate::utils::arrow::safe_take(col, &indices_array)
                .streamling_context("failed to take unique rows from column")
        })
        .collect::<Result<Vec<_>>>()?;

    let new_schema = crate::utils::arrow::build_schema_from_columns(
        &batch.schema(),
        &unique_columns,
        batch.schema().metadata().clone(),
    );

    let unique_batch = RecordBatch::try_new(new_schema, unique_columns)
        .streamling_context("failed to create deduplicated batch")?;

    Ok(unique_batch)
}

/// Deduplicate across multiple RecordBatches, preserving order and keeping the latest occurrence
/// of each primary key across all batches.
///
/// This function concatenates all batches together, then applies deduplication logic that
/// keeps the latest occurrence of each primary key. The order of batches is preserved:
/// rows from later batches take precedence over rows from earlier batches for the same key.
///
/// Hash collisions in the dedup map are resolved by comparing actual primary-key
/// values, so concatenating across batches is safe even at large flush volumes —
/// distinct PK tuples cannot be silently collapsed by a `u64` collision (STRM-5990
/// was a regression of this property).
///
/// # Arguments
/// * `batches` - A slice of RecordBatches to deduplicate
/// * `primary_key` - Comma-separated primary key column names (e.g., "id" or "id,name")
///
/// # Returns
/// A single deduplicated RecordBatch containing only the latest occurrence of each primary key
///
/// # Errors
/// Returns an error if:
/// - Any batch has a different schema
/// - Primary key columns are not found in the schema
/// - Concatenation fails
pub fn deduplicate_record_batches(
    batches: &[RecordBatch],
    primary_key: &str,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(streamling_err!("Cannot deduplicate empty batch list"));
    }

    if batches.len() == 1 {
        return deduplicate_record_batch(&batches[0], primary_key);
    }

    if primary_key.is_empty() {
        // No primary key means no deduplication needed, just concatenate
        let schema = batches[0].schema();
        return concat_batches(&schema, batches.iter())
            .streamling_context("failed to concatenate batches");
    }

    // Concatenate all batches first
    let schema = batches[0].schema();
    let concatenated = concat_batches(&schema, batches.iter())
        .streamling_context("failed to concatenate batches for dedup")?;

    // Apply deduplication to the concatenated batch
    deduplicate_record_batch(&concatenated, primary_key)
}

/// Tombstone detection for version-aware deduplication. A row is a tombstone
/// when the Utf8/LargeUtf8 `column` equals `value`; if such a row is the
/// max-version winner for its key, the key is dropped entirely, mirroring
/// ReplacingMergeTree `FINAL` semantics.
#[derive(Debug, Clone)]
pub struct TombstoneRule {
    pub column: String,
    pub value: String,
}

/// Deduplicate a RecordBatch by `primary_key`, keeping the row with the maximum
/// `version_column` value per key.
///
/// Unlike [`deduplicate_record_batch`] (which keeps the last row by position),
/// this picks the winner by version, so it is correct for streams whose rows do
/// not arrive in version order — e.g. a ClickHouse source scan that deliberately
/// omits `ORDER BY`. Ties on `version_column` break by position (later wins),
/// matching ReplacingMergeTree merge behaviour.
///
/// When `tombstone` is set, keys whose winning row is a tombstone are dropped
/// entirely (ReplacingMergeTree `FINAL` semantics).
pub fn deduplicate_record_batch_by_version(
    batch: &RecordBatch,
    primary_key: &str,
    version_column: &str,
    tombstone: Option<&TombstoneRule>,
) -> Result<RecordBatch> {
    if primary_key.is_empty() {
        return Ok(batch.clone());
    }

    let primary_keys = crate::utils::parse_primary_key_columns(primary_key);

    let pk_indices: Vec<usize> = primary_keys
        .iter()
        .map(|name| {
            batch.schema().index_of(name).map_err(|_| {
                streamling_user_err!("Primary key column '{}' not found in schema", name)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if pk_indices.is_empty() {
        return Ok(batch.clone());
    }

    let version_idx = batch.schema().index_of(version_column).map_err(|_| {
        streamling_user_err!("Version column '{}' not found in schema", version_column)
    })?;

    let key_columns: Vec<ArrayRef> = pk_indices
        .iter()
        .map(|i| batch.column(*i).clone())
        .collect();
    let version_array = batch.column(version_idx).clone();

    let pk_comparators: Vec<_> = key_columns
        .iter()
        .map(|col| make_comparator(col.as_ref(), col.as_ref(), SortOptions::default()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .streamling_context("failed to build primary key comparator for dedup")?;
    let pk_eq = |i: usize, j: usize| -> bool { pk_comparators.iter().all(|cmp| cmp(i, j).is_eq()) };

    let version_cmp = make_comparator(
        version_array.as_ref(),
        version_array.as_ref(),
        SortOptions::default(),
    )
    .streamling_context("failed to build version comparator for dedup")?;

    // Tombstone: row matches when `tombstone_column[row] == value`. Build a
    // single-element literal array of the column's own type so the comparison
    // is type-safe without a per-row ScalarValue allocation.
    let tombstone_cmp = match tombstone {
        Some(rule) => match batch.schema().index_of(&rule.column) {
            Ok(idx) => {
                let col = batch.column(idx);
                let value_arr: ArrayRef = match col.data_type() {
                    DataType::Utf8 => Arc::new(StringArray::from(vec![Some(rule.value.as_str())])),
                    DataType::LargeUtf8 => {
                        Arc::new(LargeStringArray::from(vec![Some(rule.value.as_str())]))
                    }
                    other => {
                        return Err(streamling_user_err!(
                            "Tombstone column '{}' must be Utf8 or LargeUtf8, got {:?}",
                            rule.column,
                            other
                        ));
                    }
                };
                let cmp = make_comparator(col.as_ref(), value_arr.as_ref(), SortOptions::default())
                    .streamling_context("failed to build tombstone comparator for dedup")?;
                Some(cmp)
            }
            Err(_) => None,
        },
        None => None,
    };
    let is_tombstone = |row: usize| tombstone_cmp.as_ref().is_some_and(|c| c(row, 0).is_eq());

    // Bucket winning row index (and whether it is a tombstone) per primary-key
    // hash. Collisions are resolved by real PK equality, same as
    // `deduplicate_record_batch` (STRM-5990).
    let mut buckets: HashMap<u64, Vec<(usize, bool)>> = HashMap::with_capacity(batch.num_rows());

    for row_index in 0..batch.num_rows() {
        let mut hasher = DefaultHasher::new();
        for col in &key_columns {
            hash_array_value(&mut hasher, col, row_index);
        }
        let hash = hasher.finish();
        let row_is_tomb = is_tombstone(row_index);

        let bucket = buckets.entry(hash).or_default();
        let mut updated = false;
        for entry in bucket.iter_mut() {
            let (existing, _) = *entry;
            if pk_eq(existing, row_index) {
                // Replace when the new row's version >= the current winner's.
                // `>=` (existing is NOT greater) makes ties resolve to the later
                // position, matching ReplacingMergeTree merges.
                if !version_cmp(existing, row_index).is_gt() {
                    *entry = (row_index, row_is_tomb);
                }
                updated = true;
                break;
            }
        }
        if !updated {
            bucket.push((row_index, row_is_tomb));
        }
    }

    // Survivors in original ascending order; drop tombstoned keys entirely.
    let mut unique_indices: Vec<i64> = buckets
        .into_values()
        .flatten()
        .filter(|(_, is_tomb)| !is_tomb)
        .map(|(row, _)| row as i64)
        .collect();
    unique_indices.sort_unstable();
    let indices_array = Int64Array::from(unique_indices);

    let unique_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| {
            crate::utils::arrow::safe_take(col, &indices_array)
                .streamling_context("failed to take unique rows from column")
        })
        .collect::<Result<Vec<_>>>()?;

    let new_schema = crate::utils::arrow::build_schema_from_columns(
        &batch.schema(),
        &unique_columns,
        batch.schema().metadata().clone(),
    );

    let unique_batch = RecordBatch::try_new(new_schema, unique_columns)
        .streamling_context("failed to create deduplicated batch")?;

    Ok(unique_batch)
}

/// Deduplicate across multiple RecordBatches by version, keeping the max-version
/// row per primary key. Concatenates the batches first, so duplicates that
/// straddle batch boundaries (common when a page is split into multiple IPC
/// blocks) are collapsed correctly.
pub fn deduplicate_record_batches_by_version(
    batches: &[RecordBatch],
    primary_key: &str,
    version_column: &str,
    tombstone: Option<&TombstoneRule>,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(streamling_err!("Cannot deduplicate empty batch list"));
    }

    if batches.len() == 1 {
        return deduplicate_record_batch_by_version(
            &batches[0],
            primary_key,
            version_column,
            tombstone,
        );
    }

    let schema = batches[0].schema();
    let concatenated = concat_batches(&schema, batches.iter())
        .streamling_context("failed to concatenate batches for dedup")?;

    deduplicate_record_batch_by_version(&concatenated, primary_key, version_column, tombstone)
}

/// Feed an array value at `index` into `hasher`. Used to bucket rows by their
/// primary-key hash; equality is verified separately via `make_comparator` so
/// `u64` hash collisions do not drop rows (STRM-5990).
#[inline]
fn hash_array_value(hasher: &mut impl Hasher, array: &ArrayRef, index: usize) {
    use arrow::array::*;
    use arrow::datatypes::*;

    if array.is_null(index) {
        hasher.write_u8(0);
        return;
    }
    hasher.write_u8(1);

    match array.data_type() {
        DataType::Utf8 => {
            let s = array.as_any().downcast_ref::<StringArray>().unwrap();
            hasher.write(s.value(index).as_bytes());
        }
        DataType::LargeUtf8 => {
            let s = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            hasher.write(s.value(index).as_bytes());
        }
        DataType::Int8 => hasher.write_i8(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(index),
        ),
        DataType::Int16 => hasher.write_i16(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(index),
        ),
        DataType::Int32 => hasher.write_i32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(index),
        ),
        DataType::Int64 => hasher.write_i64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(index),
        ),
        DataType::UInt8 => hasher.write_u8(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(index),
        ),
        DataType::UInt16 => hasher.write_u16(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(index),
        ),
        DataType::UInt32 => hasher.write_u32(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(index),
        ),
        DataType::UInt64 => hasher.write_u64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(index),
        ),
        DataType::Float32 => hasher.write_u32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(index)
                .to_bits(),
        ),
        DataType::Float64 => hasher.write_u64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(index)
                .to_bits(),
        ),
        DataType::Boolean => hasher.write_u8(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(index) as u8,
        ),
        DataType::Date32 => hasher.write_i32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(index),
        ),
        DataType::Date64 => hasher.write_i64(
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap()
                .value(index),
        ),
        DataType::Binary | DataType::LargeBinary => {
            let b = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            hasher.write(b.value(index));
        }
        // Anything else (timestamps, decimals, dictionaries, lists, etc.) goes
        // through the display fallback. This loses some hash-quality bits but
        // is still correct: equality on hash collisions is checked by
        // `make_comparator`, which handles all comparable Arrow types.
        _ => {
            let s = arrow::util::display::array_value_to_string(array, index).unwrap_or_default();
            hasher.write(s.as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, Int32Array, Int64Array, LargeStringArray, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use std::sync::Arc;

    fn create_test_batch(
        id_values: Vec<Option<&str>>,
        name_values: Vec<Option<&str>>,
        value_values: Vec<Option<i64>>,
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]);

        let id_array: ArrayRef = Arc::new(StringArray::from(id_values));
        let name_array: ArrayRef = Arc::new(StringArray::from(name_values));
        let value_array: ArrayRef = Arc::new(Int64Array::from(value_values));

        RecordBatch::try_new(Arc::new(schema), vec![id_array, name_array, value_array]).unwrap()
    }

    #[test]
    fn test_deduplicate_single_key_keeps_latest() {
        // Create batch with duplicates: id=1 appears 3 times with different values
        // Row 0: id=1, name=Alice, value=10
        // Row 1: id=2, name=Bob, value=20
        // Row 2: id=1, name=Charlie, value=30  <- latest for id=1, should keep this
        // Row 3: id=3, name=David, value=40
        // Row 4: id=1, name=Eve, value=50     <- even later for id=1, should keep this instead
        let batch = create_test_batch(
            vec![Some("1"), Some("2"), Some("1"), Some("3"), Some("1")],
            vec![
                Some("Alice"),
                Some("Bob"),
                Some("Charlie"),
                Some("David"),
                Some("Eve"),
            ],
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );

        let result = deduplicate_record_batch(&batch, "id").unwrap();

        assert_eq!(result.num_rows(), 3);

        // Check that we kept the latest value for id=1 (row 4: Eve, value=50)
        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name_col = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Verify id=1 appears only once with the latest values (Eve, 50)
        let mut found_id1 = false;
        for i in 0..result.num_rows() {
            if id_col.value(i) == "1" {
                assert!(!found_id1, "id=1 should appear only once");
                found_id1 = true;
                assert_eq!(name_col.value(i), "Eve");
                assert_eq!(value_col.value(i), 50);
            }
        }
        assert!(found_id1, "id=1 should be present");

        // Verify other rows are present
        assert!(id_col.value(0) == "2" || id_col.value(1) == "2" || id_col.value(2) == "2");
        assert!(id_col.value(0) == "3" || id_col.value(1) == "3" || id_col.value(2) == "3");
    }

    #[test]
    fn test_deduplicate_composite_key_keeps_latest() {
        // Test with composite primary key (id, name)
        // Row 0: id=1, name=A, value=10
        // Row 1: id=1, name=A, value=20  <- duplicate of (1,A), should keep this (latest)
        // Row 2: id=1, name=B, value=30  <- different key (1,B), should keep
        // Row 3: id=2, name=A, value=40  <- different key (2,A), should keep
        let batch = create_test_batch(
            vec![Some("1"), Some("1"), Some("1"), Some("2")],
            vec![Some("A"), Some("A"), Some("B"), Some("A")],
            vec![Some(10), Some(20), Some(30), Some(40)],
        );

        let result = deduplicate_record_batch(&batch, "id,name").unwrap();

        assert_eq!(result.num_rows(), 3);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name_col = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Verify (1,A) appears only once with latest value (20)
        let mut found_1a = false;
        for i in 0..result.num_rows() {
            if id_col.value(i) == "1" && name_col.value(i) == "A" {
                assert!(!found_1a, "(1,A) should appear only once");
                found_1a = true;
                assert_eq!(value_col.value(i), 20);
            }
        }
        assert!(found_1a, "(1,A) should be present");
    }

    #[test]
    fn test_deduplicate_no_duplicates() {
        // Batch with no duplicates - should return unchanged
        let batch = create_test_batch(
            vec![Some("1"), Some("2"), Some("3")],
            vec![Some("A"), Some("B"), Some("C")],
            vec![Some(10), Some(20), Some(30)],
        );

        let result = deduplicate_record_batch(&batch, "id").unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(result, batch);
    }

    #[test]
    fn test_deduplicate_empty_primary_key() {
        // Empty primary key should return original batch
        let batch = create_test_batch(
            vec![Some("1"), Some("1")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let result = deduplicate_record_batch(&batch, "").unwrap();

        assert_eq!(result, batch);
    }

    #[test]
    fn test_deduplicate_all_duplicates() {
        // All rows have the same key - should keep only the last one
        let batch = create_test_batch(
            vec![Some("1"), Some("1"), Some("1")],
            vec![Some("A"), Some("B"), Some("C")],
            vec![Some(10), Some(20), Some(30)],
        );

        let result = deduplicate_record_batch(&batch, "id").unwrap();

        assert_eq!(result.num_rows(), 1);
        let name_col = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Should keep the last row (C, 30)
        assert_eq!(name_col.value(0), "C");
        assert_eq!(value_col.value(0), 30);
    }

    #[test]
    fn test_deduplicate_preserves_order() {
        // Verify that non-duplicate rows maintain their relative order
        // Row 0: id=1, value=10
        // Row 1: id=2, value=20
        // Row 2: id=1, value=30  <- duplicate, should be removed
        // Row 3: id=3, value=40
        // Expected: id=1 (latest), id=2, id=3 in order
        let batch = create_test_batch(
            vec![Some("1"), Some("2"), Some("1"), Some("3")],
            vec![Some("A"), Some("B"), Some("C"), Some("D")],
            vec![Some(10), Some(20), Some(30), Some(40)],
        );

        let result = deduplicate_record_batch(&batch, "id").unwrap();

        assert_eq!(result.num_rows(), 3);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Check ordering: should maintain relative order of non-duplicates
        // id=1 should have latest value (30), but row order should be preserved
        // Since we reverse, collect, then reverse again, we keep original order
        // So we expect: id=1 (latest, value=30), id=2 (value=20), id=3 (value=40)
        let id1_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "1")
            .unwrap();
        assert_eq!(value_col.value(id1_idx), 30); // Latest value for id=1
    }

    #[test]
    fn test_deduplicate_missing_column() {
        let batch = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let result = deduplicate_record_batch(&batch, "nonexistent");

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Primary key column 'nonexistent' not found")
        );
    }

    #[test]
    fn test_deduplicate_whitespace_in_primary_key() {
        // Test that whitespace in comma-delimited primary key is handled
        // Row 0: id=1, name=A, value=10
        // Row 1: id=1, name=A, value=20  <- duplicate of (1,A), should keep this (latest)
        // Row 2: id=2, name=B, value=30
        let batch = create_test_batch(
            vec![Some("1"), Some("1"), Some("2")],
            vec![Some("A"), Some("A"), Some("B")],
            vec![Some(10), Some(20), Some(30)],
        );

        let result = deduplicate_record_batch(&batch, " id , name ").unwrap();

        assert_eq!(result.num_rows(), 2); // Should deduplicate based on composite key

        // Verify (1,A) has latest value (20)
        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name_col = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        for i in 0..result.num_rows() {
            if id_col.value(i) == "1" && name_col.value(i) == "A" {
                assert_eq!(value_col.value(i), 20);
            }
        }
    }

    #[test]
    fn test_deduplicate_record_batches_empty() {
        let result = deduplicate_record_batches(&[], "id");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot deduplicate empty batch list")
        );
    }

    #[test]
    fn test_deduplicate_record_batches_single_batch() {
        // Single batch should behave like deduplicate_record_batch
        let batch = create_test_batch(
            vec![Some("1"), Some("2"), Some("1")],
            vec![Some("A"), Some("B"), Some("C")],
            vec![Some(10), Some(20), Some(30)],
        );

        let result = deduplicate_record_batches(std::slice::from_ref(&batch), "id").unwrap();
        let single_result = deduplicate_record_batch(&batch, "id").unwrap();

        assert_eq!(result.num_rows(), single_result.num_rows());
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_deduplicate_record_batches_across_batches_keeps_latest() {
        // Batch 1: id=1 (value=10), id=2 (value=20)
        // Batch 2: id=1 (value=30), id=3 (value=40)
        // Expected: id=1 (value=30 from batch 2), id=2 (value=20 from batch 1), id=3 (value=40 from batch 2)
        let batch1 = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let batch2 = create_test_batch(
            vec![Some("1"), Some("3")],
            vec![Some("C"), Some("D")],
            vec![Some(30), Some(40)],
        );

        let result = deduplicate_record_batches(&[batch1, batch2], "id").unwrap();

        assert_eq!(result.num_rows(), 3);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Find id=1 - should have value 30 (from batch 2)
        let id1_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "1")
            .unwrap();
        assert_eq!(
            value_col.value(id1_idx),
            30,
            "id=1 should have latest value 30"
        );

        // Find id=2 - should have value 20 (from batch 1)
        let id2_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "2")
            .unwrap();
        assert_eq!(value_col.value(id2_idx), 20, "id=2 should have value 20");

        // Find id=3 - should have value 40 (from batch 2)
        let id3_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "3")
            .unwrap();
        assert_eq!(value_col.value(id3_idx), 40, "id=3 should have value 40");
    }

    #[test]
    fn test_deduplicate_record_batches_preserves_order() {
        // Test that order is preserved: non-duplicates maintain their relative order
        // Batch 1: id=1 (value=10), id=2 (value=20)
        // Batch 2: id=3 (value=30), id=1 (value=40) <- id=1 duplicate, should replace batch 1's id=1
        // Expected order: id=1 (value=40), id=2 (value=20), id=3 (value=30)
        let batch1 = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let batch2 = create_test_batch(
            vec![Some("3"), Some("1")],
            vec![Some("C"), Some("D")],
            vec![Some(30), Some(40)],
        );

        let result = deduplicate_record_batches(&[batch1, batch2], "id").unwrap();

        assert_eq!(result.num_rows(), 3);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Verify id=1 has the latest value (40 from batch 2)
        let id1_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "1")
            .unwrap();
        assert_eq!(value_col.value(id1_idx), 40);

        // Verify all expected IDs are present
        let ids: Vec<String> = (0..result.num_rows())
            .map(|i| id_col.value(i).to_string())
            .collect();
        assert!(ids.contains(&"1".to_string()));
        assert!(ids.contains(&"2".to_string()));
        assert!(ids.contains(&"3".to_string()));
    }

    #[test]
    fn test_deduplicate_record_batches_multiple_batches() {
        // Test with 3 batches where duplicates appear across all batches
        // Batch 1: id=1 (value=10), id=2 (value=20)
        // Batch 2: id=1 (value=30), id=3 (value=40)
        // Batch 3: id=1 (value=50), id=2 (value=60)
        // Expected: id=1 (value=50 from batch 3), id=2 (value=60 from batch 3), id=3 (value=40 from batch 2)
        let batch1 = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let batch2 = create_test_batch(
            vec![Some("1"), Some("3")],
            vec![Some("C"), Some("D")],
            vec![Some(30), Some(40)],
        );

        let batch3 = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("E"), Some("F")],
            vec![Some(50), Some(60)],
        );

        let result = deduplicate_record_batches(&[batch1, batch2, batch3], "id").unwrap();

        assert_eq!(result.num_rows(), 3);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Verify id=1 has the latest value (50 from batch 3)
        let id1_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "1")
            .unwrap();
        assert_eq!(
            value_col.value(id1_idx),
            50,
            "id=1 should have latest value 50"
        );

        // Verify id=2 has the latest value (60 from batch 3)
        let id2_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "2")
            .unwrap();
        assert_eq!(
            value_col.value(id2_idx),
            60,
            "id=2 should have latest value 60"
        );

        // Verify id=3 has value 40 (from batch 2)
        let id3_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "3")
            .unwrap();
        assert_eq!(value_col.value(id3_idx), 40, "id=3 should have value 40");
    }

    #[test]
    fn test_deduplicate_record_batches_composite_key() {
        // Test with composite primary key across batches
        // Batch 1: (id=1, name=A, value=10), (id=2, name=B, value=20)
        // Batch 2: (id=1, name=A, value=30), (id=1, name=C, value=40)
        // Expected: (id=1, name=A, value=30 from batch 2), (id=2, name=B, value=20 from batch 1), (id=1, name=C, value=40 from batch 2)
        let batch1 = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let batch2 = create_test_batch(
            vec![Some("1"), Some("1")],
            vec![Some("A"), Some("C")],
            vec![Some(30), Some(40)],
        );

        let result = deduplicate_record_batches(&[batch1, batch2], "id,name").unwrap();

        assert_eq!(result.num_rows(), 3);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name_col = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Verify (1,A) has latest value (30 from batch 2)
        let id1a_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "1" && name_col.value(i) == "A")
            .unwrap();
        assert_eq!(value_col.value(id1a_idx), 30);

        // Verify (2,B) has value 20 (from batch 1)
        let id2b_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "2" && name_col.value(i) == "B")
            .unwrap();
        assert_eq!(value_col.value(id2b_idx), 20);

        // Verify (1,C) has value 40 (from batch 2)
        let id1c_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "1" && name_col.value(i) == "C")
            .unwrap();
        assert_eq!(value_col.value(id1c_idx), 40);
    }

    #[test]
    fn test_deduplicate_record_batches_no_duplicates() {
        // Test with batches that have no duplicates
        let batch1 = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let batch2 = create_test_batch(
            vec![Some("3"), Some("4")],
            vec![Some("C"), Some("D")],
            vec![Some(30), Some(40)],
        );

        let result = deduplicate_record_batches(&[batch1, batch2], "id").unwrap();

        assert_eq!(result.num_rows(), 4);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let ids: Vec<String> = (0..result.num_rows())
            .map(|i| id_col.value(i).to_string())
            .collect();
        assert!(ids.contains(&"1".to_string()));
        assert!(ids.contains(&"2".to_string()));
        assert!(ids.contains(&"3".to_string()));
        assert!(ids.contains(&"4".to_string()));
    }

    #[test]
    fn test_deduplicate_record_batches_empty_primary_key() {
        // Empty primary key should just concatenate without deduplication
        let batch1 = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let batch2 = create_test_batch(
            vec![Some("3"), Some("4")],
            vec![Some("C"), Some("D")],
            vec![Some(30), Some(40)],
        );

        let result = deduplicate_record_batches(&[batch1, batch2], "").unwrap();

        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_deduplicate_record_batches_with_empty_batches() {
        // Test with empty batches mixed with non-empty batches
        let empty_batch = create_test_batch(vec![], vec![], vec![]);
        let batch1 = create_test_batch(
            vec![Some("1"), Some("2")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );
        let batch2 = create_test_batch(vec![Some("3")], vec![Some("C")], vec![Some(30)]);

        // Empty batch + data + empty batch + data
        let result = deduplicate_record_batches(
            &[empty_batch.clone(), batch1, empty_batch.clone(), batch2],
            "id",
        )
        .unwrap();

        assert_eq!(result.num_rows(), 3);
        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids: Vec<String> = (0..result.num_rows())
            .map(|i| id_col.value(i).to_string())
            .collect();
        assert!(ids.contains(&"1".to_string()));
        assert!(ids.contains(&"2".to_string()));
        assert!(ids.contains(&"3".to_string()));
    }

    #[test]
    fn test_deduplicate_record_batches_null_primary_keys() {
        // Test that null values in primary key columns are handled correctly
        // All rows with null primary keys should be considered duplicates
        let batch1 = create_test_batch(
            vec![Some("1"), None, None],
            vec![Some("A"), Some("B"), Some("C")],
            vec![Some(10), Some(20), Some(30)],
        );

        let batch2 = create_test_batch(
            vec![None, Some("2")],
            vec![Some("D"), Some("E")],
            vec![Some(40), Some(50)],
        );

        // All null primary keys should deduplicate to one row (latest: value=40 from batch2)
        // id=1 and id=2 should remain
        let result = deduplicate_record_batches(&[batch1, batch2], "id").unwrap();

        // Should have: id=1, id=2, and one null (latest from batch2: value=40)
        assert_eq!(result.num_rows(), 3);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Find null primary key row - should have value 40 (latest)
        let null_idx = (0..result.num_rows()).find(|&i| id_col.is_null(i)).unwrap();
        assert_eq!(
            value_col.value(null_idx),
            40,
            "Null primary key should keep latest value"
        );
    }

    #[test]
    fn test_deduplicate_record_batches_composite_key_with_nulls() {
        // Test composite key with nulls in different columns
        // (null, "A") vs ("1", null) vs (null, null) should all be different keys
        let batch1 = create_test_batch(
            vec![None, Some("1"), None],
            vec![Some("A"), None, None],
            vec![Some(10), Some(20), Some(30)],
        );

        let batch2 = create_test_batch(
            vec![None, Some("1"), None],
            vec![Some("A"), Some("B"), None],
            vec![Some(40), Some(50), Some(60)],
        );

        // (null, "A") should deduplicate to value 40 (latest from batch2)
        // ("1", null) from batch1 and ("1", "B") from batch2 are DIFFERENT keys (null != "B")
        // So we should have: (null, "A"), ("1", null), ("1", "B"), (null, null)
        // (null, null) should deduplicate to value 60 (latest from batch2)
        let result = deduplicate_record_batches(&[batch1, batch2], "id,name").unwrap();

        assert_eq!(result.num_rows(), 4);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let name_col = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Find (null, "A") - should have value 40
        let null_a_idx = (0..result.num_rows())
            .find(|&i| id_col.is_null(i) && name_col.value(i) == "A")
            .unwrap();
        assert_eq!(value_col.value(null_a_idx), 40);

        // Find ("1", null) - should have value 20 (from batch1, not replaced since ("1", "B") is different key)
        let one_null_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "1" && name_col.is_null(i))
            .unwrap();
        assert_eq!(value_col.value(one_null_idx), 20);

        // Find ("1", "B") - should have value 50 (new key from batch2)
        let one_b_idx = (0..result.num_rows())
            .find(|&i| id_col.value(i) == "1" && name_col.value(i) == "B")
            .unwrap();
        assert_eq!(value_col.value(one_b_idx), 50);

        // Find (null, null) - should have value 60
        let null_null_idx = (0..result.num_rows())
            .find(|&i| id_col.is_null(i) && name_col.is_null(i))
            .unwrap();
        assert_eq!(value_col.value(null_null_idx), 60);
    }

    #[test]
    fn test_deduplicate_record_batches_all_duplicates() {
        // Test case where all rows have the same primary key
        let batch1 = create_test_batch(
            vec![Some("1"), Some("1")],
            vec![Some("A"), Some("B")],
            vec![Some(10), Some(20)],
        );

        let batch2 = create_test_batch(vec![Some("1")], vec![Some("C")], vec![Some(30)]);

        // All rows have id=1, should result in single row with latest value (30)
        let result = deduplicate_record_batches(&[batch1, batch2], "id").unwrap();

        assert_eq!(result.num_rows(), 1);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(id_col.value(0), "1");
        assert_eq!(value_col.value(0), 30, "Should keep latest value");
    }

    #[test]
    fn test_deduplicate_record_batches_schema_mismatch() {
        // Test that schema mismatches are caught
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![
                Arc::new(StringArray::from(vec!["1", "2"])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();

        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false), // Different type!
        ]);
        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![
                Arc::new(StringArray::from(vec!["3"])),
                Arc::new(Int32Array::from(vec![30])),
            ],
        )
        .unwrap();

        // Should fail due to schema mismatch
        let result = deduplicate_record_batches(&[batch1, batch2], "id");
        assert!(result.is_err(), "Should fail with schema mismatch");
    }

    #[test]
    fn test_deduplicate_record_batches_missing_primary_key_column() {
        // Test that missing primary key column is caught
        let batch1 = create_test_batch(vec![Some("1")], vec![Some("A")], vec![Some(10)]);

        // Batch with different schema missing "id" column
        let schema2 = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]);
        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![
                Arc::new(StringArray::from(vec!["B"])),
                Arc::new(Int64Array::from(vec![20])),
            ],
        )
        .unwrap();

        // Should fail due to missing primary key column
        let result = deduplicate_record_batches(&[batch1, batch2], "id");
        assert!(
            result.is_err(),
            "Should fail with missing primary key column"
        );
    }

    /// STRM-5990 regression: distinct primary keys whose `DefaultHasher` u64
    /// outputs collide must not silently drop rows. The previous implementation
    /// keyed its dedup map on the raw hash, so the second row would overwrite
    /// the first and the first would vanish from the output.
    ///
    /// `pk_a` and `pk_b` were produced by exhaustive search against the exact
    /// hashing scheme the previous implementation used for a single non-null
    /// `Binary` column:
    ///
    /// ```text
    /// let mut h = DefaultHasher::new();
    /// h.write_u8(1);       // non-null marker emitted by hash_array_value
    /// h.write(pk_bytes);   // BinaryArray::value(i)
    /// h.finish()
    /// ```
    #[test]
    fn test_deduplicate_default_hasher_collision_keeps_both_rows() {
        let pk_a: &[u8] = &[40, 157, 55, 89, 125, 54, 41, 137, 118, 34];
        let pk_b: &[u8] = &[41, 146, 117, 72, 105, 143, 36, 83, 123, 33];

        let hash = |bytes: &[u8]| {
            let mut h = DefaultHasher::new();
            h.write_u8(1);
            h.write(bytes);
            h.finish()
        };
        assert_ne!(pk_a, pk_b, "primary keys must be distinct");
        assert_eq!(
            hash(pk_a),
            hash(pk_b),
            "stdlib DefaultHasher no longer produces this collision; \
             search for a new pair before relying on this regression"
        );

        let schema = Schema::new(vec![
            Field::new("id", DataType::Binary, true),
            Field::new("value", DataType::Int64, true),
        ]);
        let id_array: ArrayRef = Arc::new(BinaryArray::from(vec![Some(pk_a), Some(pk_b)]));
        let value_array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![id_array, value_array]).unwrap();

        let result = deduplicate_record_batch(&batch, "id").unwrap();

        assert_eq!(
            result.num_rows(),
            2,
            "both rows must survive; distinct primary keys that hash to the \
             same u64 were silently collapsed by the old hash-keyed dedup map"
        );

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let value_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let row_for = |pk: &[u8]| -> i64 {
            let idx = (0..result.num_rows())
                .find(|&i| id_col.value(i) == pk)
                .unwrap_or_else(|| panic!("pk {:?} dropped by dedup", pk));
            value_col.value(idx)
        };
        assert_eq!(row_for(pk_a), 1);
        assert_eq!(row_for(pk_b), 2);
    }

    /// Regression: `hash_array_value` previously matched `Utf8 | LargeUtf8` in
    /// one arm and unconditionally downcast to `StringArray`. For a
    /// `LargeUtf8` column the downcast returns `None` and `.unwrap()` panics
    /// at sink startup — the failure mode observed on the
    /// `v2-evm-polygon-1-0-0` pipeline once the EVM plugin started emitting
    /// LargeUtf8 receipt/log columns.
    #[test]
    fn test_deduplicate_large_utf8_primary_key() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::LargeUtf8, true),
            Field::new("value", DataType::Int64, true),
        ]);
        let id_array: ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
        ]));
        let value_array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)]));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![id_array, value_array]).unwrap();

        let result = deduplicate_record_batch(&batch, "id").unwrap();

        assert_eq!(result.num_rows(), 2);

        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let value_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let row_for = |pk: &str| -> i64 {
            let idx = (0..result.num_rows())
                .find(|&i| id_col.value(i) == pk)
                .unwrap_or_else(|| panic!("pk {pk:?} dropped by dedup"));
            value_col.value(idx)
        };
        // id=a appears twice; latest occurrence (value=3) wins.
        assert_eq!(row_for("a"), 3);
        assert_eq!(row_for("b"), 2);
    }
    fn create_versioned_batch(
        id_values: Vec<Option<&str>>,
        value_values: Vec<Option<i64>>,
        version_values: Vec<Option<i64>>,
        op_values: Vec<Option<&str>>,
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
            Field::new("insert_timestamp", DataType::Int64, true),
            Field::new("_gs_op", DataType::Utf8, true),
        ]);
        let id_array: ArrayRef = Arc::new(StringArray::from(id_values));
        let value_array: ArrayRef = Arc::new(Int64Array::from(value_values));
        let version_array: ArrayRef = Arc::new(Int64Array::from(version_values));
        let op_array: ArrayRef = Arc::new(StringArray::from(op_values));
        RecordBatch::try_new(
            Arc::new(schema),
            vec![id_array, value_array, version_array, op_array],
        )
        .unwrap()
    }

    #[test]
    fn test_dedup_by_version_keeps_max_version_not_position() {
        // id=1 appears twice; the EARLIER row has the HIGHER version.
        // Position-based dedup would wrongly keep row 1; version-aware keeps row 0.
        let batch = create_versioned_batch(
            vec![Some("1"), Some("1")],
            vec![Some(10), Some(20)],
            vec![Some(100), Some(50)],
            vec![Some("i"), Some("i")],
        );
        let result =
            deduplicate_record_batch_by_version(&batch, "id", "insert_timestamp", None).unwrap();
        assert_eq!(result.num_rows(), 1);
        let value_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            value_col.value(0),
            10,
            "max-version row (value=10) must win"
        );
    }

    #[test]
    fn test_dedup_by_version_tie_keeps_later_position() {
        // Equal versions: later position wins (ReplacingMergeTree merge order).
        let batch = create_versioned_batch(
            vec![Some("1"), Some("1")],
            vec![Some(10), Some(20)],
            vec![Some(100), Some(100)],
            vec![Some("i"), Some("i")],
        );
        let result =
            deduplicate_record_batch_by_version(&batch, "id", "insert_timestamp", None).unwrap();
        assert_eq!(result.num_rows(), 1);
        let value_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(value_col.value(0), 20);
    }

    #[test]
    fn test_dedup_by_version_tombstone_drops_key_when_winner() {
        // id=1: live insert (version 100) then a delete (version 200). The
        // delete is the max-version winner -> the whole key is dropped (FINAL).
        let batch = create_versioned_batch(
            vec![Some("1"), Some("1"), Some("2")],
            vec![Some(10), Some(20), Some(30)],
            vec![Some(100), Some(200), Some(50)],
            vec![Some("i"), Some("d"), Some("i")],
        );
        let tombstone = TombstoneRule {
            column: "_gs_op".to_string(),
            value: "d".to_string(),
        };
        let result =
            deduplicate_record_batch_by_version(&batch, "id", "insert_timestamp", Some(&tombstone))
                .unwrap();
        assert_eq!(result.num_rows(), 1);
        let id_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(id_col.value(0), "2", "tombstoned id=1 must be dropped");
    }

    #[test]
    fn test_dedup_by_version_tombstone_kept_when_not_winner() {
        // id=1: delete (version 50) then live insert (version 200). The delete
        // is NOT the winner -> key survives with the live row.
        let batch = create_versioned_batch(
            vec![Some("1"), Some("1")],
            vec![Some(10), Some(20)],
            vec![Some(50), Some(200)],
            vec![Some("d"), Some("i")],
        );
        let tombstone = TombstoneRule {
            column: "_gs_op".to_string(),
            value: "d".to_string(),
        };
        let result =
            deduplicate_record_batch_by_version(&batch, "id", "insert_timestamp", Some(&tombstone))
                .unwrap();
        assert_eq!(result.num_rows(), 1);
        let op_col = result
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(op_col.value(0), "i");
    }

    #[test]
    fn test_dedup_by_version_missing_version_column_errors() {
        let batch = create_versioned_batch(
            vec![Some("1")],
            vec![Some(10)],
            vec![Some(100)],
            vec![Some("i")],
        );
        let result = deduplicate_record_batch_by_version(&batch, "id", "nope", None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Version column 'nope' not found")
        );
    }

    #[test]
    fn test_dedup_by_version_across_batches() {
        // Same key split across two batches; the higher version lives in batch 1.
        let batch1 = create_versioned_batch(
            vec![Some("1")],
            vec![Some(10)],
            vec![Some(300)],
            vec![Some("i")],
        );
        let batch2 = create_versioned_batch(
            vec![Some("1")],
            vec![Some(20)],
            vec![Some(100)],
            vec![Some("i")],
        );
        let result = deduplicate_record_batches_by_version(
            &[batch1, batch2],
            "id",
            "insert_timestamp",
            None,
        )
        .unwrap();
        assert_eq!(result.num_rows(), 1);
        let value_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            value_col.value(0),
            10,
            "max-version (batch1, value=10) must win"
        );
    }
}
