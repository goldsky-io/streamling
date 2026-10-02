use std::time::Duration;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use streamling_core::error::{Result, ResultExt};
use streamling_core::streamling_err;

use super::range_controller::RangeController;

/// Paging inside ONE first-sort-key value that alone overflows a page.
///
/// Range pagination cannot split a single first-key value, so such a key is
/// read ordered by the full sorting key with `LIMIT limit + 1` and keyset-paged
/// on the remaining keys (`(k1, ..) > cursor`). Order, seek and cursor all use
/// the raw table values, never the projected output columns (see
/// `ClickHouseQueryBuilder::set_in_key_after`): the cursor is the `toString` of
/// each remaining key ([`cursor_column`]), which ClickHouse parses back into
/// the key's own type in the seek.
///
/// Pages are cut only at a full sort-key tuple boundary: the trailing tuple
/// group of a full page is dropped and re-read by the next page, so every
/// version of one tuple lands in the same page and version-aware dedup stays
/// correct.
#[derive(Debug)]
pub struct InKeyScan {
    /// Remaining sort-key tuple to read strictly after; empty on the first page.
    pub after: Vec<ScalarValue>,
    pub pager: InKeyPager,
}

/// Row budget of the next in-key page.
///
/// `limit` starts at `page_size`. It shrinks under the byte tripwire (never
/// below the first tuple group of the page it re-reads, so every re-read makes
/// progress) and on timeouts, and after every emitted page it is resized from
/// the observed byte density and elapsed time, as `RangeController` resizes a
/// range. A limit that timed out caps every later limit of the key, so a tuple
/// group too large to read within the query timeout fails instead of looping.
#[derive(Debug)]
pub struct InKeyPager {
    page_size: u64,
    max_page_bytes: u64,
    soft_time_budget: Duration,
    limit: u64,
    /// Smallest limit that timed out; `u64::MAX` until one does.
    ceiling: u64,
}

/// What to do with an in-key page that was fully buffered.
#[derive(Debug, PartialEq)]
pub enum InKeyStep {
    /// Emit the first `rows` rows; the key continues strictly after `cursor`
    /// (the remaining sort-key values of the last emitted tuple).
    Emit {
        rows: usize,
        cursor: Vec<ScalarValue>,
    },
    /// The key is exhausted: emit every row and move past the key.
    Finish,
    /// Nothing emitted: re-read the same cursor with the new `limit`.
    Retry,
    /// A single full sort-key tuple exceeds what one page can hold.
    Unsplittable(String),
}

/// Output column of an in-key page carrying `toString` of remaining sort key
/// `i` (0-based). The query appends these after the scan columns.
pub fn cursor_column(i: usize) -> String {
    format!("_gs_cursor_{}", i + 1)
}

/// Why a first-key value cannot be paged within, given the remaining sort keys
/// and their ClickHouse Arrow types, or `None` when it can. A float key cannot
/// be sought past: `f > 'nan'` matches nothing and `-0 = 0`.
pub fn unsupported_reason(rest_keys: &[String], rest_key_types: &Schema) -> Option<String> {
    if rest_keys.is_empty() {
        return Some("the table has a single sorting key".to_string());
    }
    rest_keys
        .iter()
        .zip(rest_key_types.fields())
        .find_map(|(key, field)| match field.data_type() {
            dt @ (DataType::Float16 | DataType::Float32 | DataType::Float64) => Some(format!(
                "sorting key '{key}' is a float ({dt:?}); NaN and signed zeros cannot be sought past"
            )),
            dt if dt.is_nested() || matches!(dt, DataType::Null) => Some(format!(
                "sorting key '{key}' has type {dt:?}, which has no seekable string form"
            )),
            _ => None,
        })
}

/// A checkpointed cursor must hold one value per remaining sort key, each NULL
/// or a string or integer the SQL literal formatter renders exactly.
pub fn validate_cursor(cursor: &[ScalarValue], rest_keys: &[String]) -> Result<()> {
    if cursor.len() != rest_keys.len() {
        return Err(streamling_err!(
            "checkpointed in-key cursor {:?} does not match the remaining sorting keys {:?}",
            cursor,
            rest_keys
        ));
    }
    for (value, key) in cursor.iter().zip(rest_keys) {
        let renderable = value.is_null()
            || matches!(
                value,
                ScalarValue::Utf8(_)
                    | ScalarValue::LargeUtf8(_)
                    | ScalarValue::Int8(_)
                    | ScalarValue::Int16(_)
                    | ScalarValue::Int32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::UInt8(_)
                    | ScalarValue::UInt16(_)
                    | ScalarValue::UInt32(_)
                    | ScalarValue::UInt64(_)
            );
        if !renderable {
            return Err(streamling_err!(
                "checkpointed in-key cursor value {:?} for sorting key '{}' is not a string or integer",
                value,
                key
            ));
        }
    }
    Ok(())
}

impl InKeyPager {
    pub fn new(page_size: u64, max_page_bytes: u64, soft_time_budget: Duration) -> Self {
        Self {
            page_size,
            max_page_bytes,
            soft_time_budget,
            limit: page_size,
            ceiling: u64::MAX,
        }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Size the first page from a read of the same key that overflowed.
    pub fn fit_to(&mut self, rows: usize, bytes: u64) {
        self.limit = self.byte_limit(rows, bytes).clamp(1, self.page_size);
    }

    /// A timed-out page is re-read with half the rows. `false` when the limit
    /// is already one row.
    pub fn on_timeout(&mut self) -> bool {
        if self.limit <= 1 {
            return false;
        }
        self.ceiling = self.limit;
        self.limit /= 2;
        true
    }

    /// Largest limit whose page stays under the byte target, at the observed
    /// bytes-per-row density.
    fn byte_limit(&self, rows: usize, bytes: u64) -> u64 {
        if bytes == 0 || rows == 0 {
            return self.page_size;
        }
        let per_row = bytes as f64 / rows as f64;
        ((self.max_page_bytes as f64 * RangeController::BYTE_TARGET_RATIO) / per_row) as u64
    }

    /// Largest limit whose page stays within the soft time budget, at the
    /// observed rows-per-second rate.
    fn time_limit(&self, rows: usize, elapsed: Duration) -> u64 {
        if elapsed.is_zero() {
            return u64::MAX;
        }
        (rows as f64 * self.soft_time_budget.as_secs_f64() / elapsed.as_secs_f64()) as u64
    }

    /// Limit for the page after one of `rows` rows and `bytes` bytes read in
    /// `elapsed`.
    fn next_limit(&self, rows: usize, bytes: u64, elapsed: Duration) -> u64 {
        self.byte_limit(rows, bytes)
            .min(self.time_limit(rows, elapsed))
            .min(self.ceiling - 1)
            .clamp(1, self.page_size)
    }

    /// Decide what to do with a buffered page of `bytes` bytes read at the
    /// current limit in `elapsed`. The batches are ordered by the full sorting
    /// key and end with one cursor column per remaining sort key `rest_keys`.
    pub fn plan(
        &mut self,
        batches: &[RecordBatch],
        rest_keys: &[String],
        bytes: u64,
        elapsed: Duration,
    ) -> Result<InKeyStep> {
        let rows = TupleRows::new(batches, rest_keys)?;
        let n = rows.len();
        let limit = self.limit;

        if bytes > self.max_page_bytes {
            let first_group = rows.first_group_len();
            if first_group < n {
                let shrunk = self
                    .byte_limit(n, bytes)
                    .min(limit - 1)
                    .max(first_group as u64);
                if shrunk < limit {
                    self.limit = shrunk;
                    return Ok(InKeyStep::Retry);
                }
                // The page is the first tuple group plus one lookahead row that
                // proves the group complete: emit the group if it fits alone.
                if prefix_bytes(batches, first_group)? <= self.max_page_bytes {
                    self.limit = self.next_limit(n, bytes, elapsed);
                    return Ok(InKeyStep::Emit {
                        rows: first_group,
                        cursor: rows.cursor(first_group - 1),
                    });
                }
            }
            return Ok(InKeyStep::Unsplittable(format!(
                "a single sort-key tuple {} holds more than max_page_bytes={} ({} rows read, {} bytes)",
                rows.describe(0),
                self.max_page_bytes,
                n,
                bytes
            )));
        }

        if n as u64 <= limit {
            return Ok(InKeyStep::Finish);
        }

        // Full page (`limit + 1` rows): more of this key remains. Cut before the
        // trailing tuple group, whose remaining versions may lie past the LIMIT.
        let next = self.next_limit(n, bytes, elapsed);
        let cut = rows.last_group_start();
        if cut == 0 {
            // One tuple group fills the page: read more of it, as far as the
            // byte and time budgets allow.
            if next > limit {
                self.limit = next;
                return Ok(InKeyStep::Retry);
            }
            return Ok(InKeyStep::Unsplittable(format!(
                "a single sort-key tuple {} holds more than {} rows, the most one page can read \
                 within page_size={}, max_page_bytes={} and the query time budget",
                rows.describe(0),
                limit,
                self.page_size,
                self.max_page_bytes
            )));
        }
        self.limit = next;
        Ok(InKeyStep::Emit {
            rows: cut,
            cursor: rows.cursor(cut - 1),
        })
    }
}

/// The emitted part of an in-key page: its first `rows` rows (every row when
/// `None`) without the trailing `cursor_columns`.
pub fn payload(
    batches: Vec<RecordBatch>,
    rows: Option<usize>,
    cursor_columns: usize,
) -> Result<Vec<RecordBatch>> {
    let mut remaining = rows.unwrap_or(usize::MAX);
    let mut page = Vec::with_capacity(batches.len());
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let take = batch.num_rows().min(remaining);
        remaining -= take;
        let keep: Vec<usize> = (0..batch.num_columns() - cursor_columns).collect();
        page.push(
            batch
                .slice(0, take)
                .project(&keep)
                .streamling_context("failed to drop in-key cursor columns")?,
        );
    }
    Ok(page)
}

/// Exact byte size of the first `rows` rows of a page.
fn prefix_bytes(batches: &[RecordBatch], mut rows: usize) -> Result<u64> {
    let mut bytes = 0u64;
    for batch in batches {
        if rows == 0 {
            break;
        }
        let take = batch.num_rows().min(rows);
        rows -= take;
        for column in batch.slice(0, take).columns() {
            bytes += column
                .to_data()
                .get_slice_memory_size()
                .streamling_context("failed to size an in-key page prefix")?
                as u64;
        }
    }
    Ok(bytes)
}

/// The cursor columns of a page split across several batches, compared in
/// place row by row.
struct TupleRows<'a> {
    /// Per batch, the cursor column of each remaining sort key.
    batches: Vec<Vec<&'a StringArray>>,
    len: usize,
}

impl<'a> TupleRows<'a> {
    fn new(batches: &'a [RecordBatch], rest_keys: &[String]) -> Result<Self> {
        let k = rest_keys.len();
        if k == 0 {
            return Err(streamling_err!(
                "paging within a first-sort-key value needs a second sorting key"
            ));
        }
        let columns = batches
            .iter()
            .map(|batch| {
                let first = batch.num_columns().checked_sub(k).ok_or_else(|| {
                    streamling_err!("in-key page has fewer columns than cursor columns")
                })?;
                (0..k)
                    .map(|i| {
                        let name = cursor_column(i);
                        let column = batch.column(first + i);
                        match column.as_any().downcast_ref::<StringArray>() {
                            Some(cursor) if batch.schema().field(first + i).name() == &name => {
                                Ok(cursor)
                            }
                            _ => Err(streamling_err!(
                                "in-key page has no Utf8 cursor column '{}' for sorting key '{}'",
                                name,
                                rest_keys[i]
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let len = batches.iter().map(|b| b.num_rows()).sum();
        Ok(Self {
            batches: columns,
            len,
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    fn cell(column: &StringArray, row: usize) -> Option<&str> {
        column.is_valid(row).then(|| column.value(row))
    }

    fn same(&self, a: (usize, usize), b: (usize, usize)) -> bool {
        self.batches[a.0]
            .iter()
            .zip(&self.batches[b.0])
            .all(|(x, y)| Self::cell(x, a.1) == Self::cell(y, b.1))
    }

    /// Every (batch, row) position, in page order.
    fn positions(&self) -> impl DoubleEndedIterator<Item = (usize, usize)> + '_ {
        self.batches
            .iter()
            .enumerate()
            .flat_map(|(b, columns)| (0..columns[0].len()).map(move |row| (b, row)))
    }

    /// Rows in the first tuple group of a non-empty page.
    fn first_group_len(&self) -> usize {
        let mut positions = self.positions();
        let head = positions.next().expect("in-key page is not empty");
        1 + positions.take_while(|&p| self.same(head, p)).count()
    }

    /// Row where the last tuple group of a non-empty page starts.
    fn last_group_start(&self) -> usize {
        let mut positions = self.positions().rev();
        let tail = positions.next().expect("in-key page is not empty");
        self.len - 1 - positions.take_while(|&p| self.same(tail, p)).count()
    }

    fn position(&self, mut row: usize) -> (usize, usize) {
        for (b, columns) in self.batches.iter().enumerate() {
            if row < columns[0].len() {
                return (b, row);
            }
            row -= columns[0].len();
        }
        unreachable!("row index is within the page")
    }

    /// Remaining sort-key tuple at `row`, as its cursor strings; NULL stays NULL.
    fn cursor(&self, row: usize) -> Vec<ScalarValue> {
        let (b, i) = self.position(row);
        self.batches[b]
            .iter()
            .map(|column| ScalarValue::Utf8(Self::cell(column, i).map(str::to_string)))
            .collect()
    }

    /// The tuple at `row`, for errors.
    fn describe(&self, row: usize) -> String {
        let (b, i) = self.position(row);
        let values: Vec<&str> = self.batches[b]
            .iter()
            .map(|column| Self::cell(column, i).unwrap_or("NULL"))
            .collect();
        format!("({})", values.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::datatypes::Field;
    use std::sync::Arc;

    const BUDGET: Duration = Duration::from_secs(10);

    /// A page whose remaining sort key `id` has the given cursor strings.
    fn batch_of(ids: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("_gs_op", DataType::Utf8, false),
            Field::new(cursor_column(0), DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![7u64; ids.len()])),
                Arc::new(StringArray::from(vec!["i"; ids.len()])),
                Arc::new(StringArray::from(ids.to_vec())),
            ],
        )
        .unwrap()
    }

    fn batch(ids: &[&str]) -> RecordBatch {
        batch_of(&ids.iter().map(|s| Some(*s)).collect::<Vec<_>>())
    }

    fn keys() -> Vec<String> {
        vec!["id".to_string()]
    }

    fn pager(page_size: u64, max_page_bytes: u64) -> InKeyPager {
        InKeyPager::new(page_size, max_page_bytes, BUDGET)
    }

    fn utf8(s: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(s.to_string()))
    }

    fn plan(p: &mut InKeyPager, batches: &[RecordBatch], bytes: u64) -> InKeyStep {
        p.plan(batches, &keys(), bytes, Duration::ZERO).unwrap()
    }

    #[test]
    fn full_page_cuts_before_trailing_tuple_group_across_batches() {
        // limit 4 -> LIMIT 5 read. 'c' straddles the batch boundary and may have
        // more versions past the LIMIT, so the cut drops both 'c' rows.
        let mut p = pager(4, u64::MAX);
        let step = plan(&mut p, &[batch(&["a", "b", "c"]), batch(&["c", "c"])], 0);
        assert_eq!(
            step,
            InKeyStep::Emit {
                rows: 2,
                cursor: vec![utf8("b")]
            }
        );
    }

    #[test]
    fn short_page_finishes_the_key() {
        let mut p = pager(4, u64::MAX);
        assert_eq!(
            plan(&mut p, &[batch(&["a", "a", "b"])], 0),
            InKeyStep::Finish
        );
    }

    #[test]
    fn empty_page_finishes_the_key() {
        let mut p = pager(4, u64::MAX);
        assert_eq!(plan(&mut p, &[], 0), InKeyStep::Finish);
    }

    #[test]
    fn null_key_values_group_and_cut_like_values() {
        // NULLs sort first (NULLS FIRST); a cursor on a NULL tuple stays NULL so
        // the seek resumes with `IS NULL` / `IS NOT NULL`.
        let mut p = pager(3, u64::MAX);
        let step = plan(&mut p, &[batch_of(&[None, None, Some("a"), Some("a")])], 0);
        assert_eq!(
            step,
            InKeyStep::Emit {
                rows: 2,
                cursor: vec![ScalarValue::Utf8(None)]
            }
        );
    }

    #[test]
    fn single_tuple_over_page_size_is_unsplittable() {
        let mut p = pager(4, u64::MAX);
        let step = plan(&mut p, &[batch(&["a"; 5])], 0);
        assert!(matches!(step, InKeyStep::Unsplittable(m) if m.contains("page_size=4")));
    }

    #[test]
    fn byte_overflow_shrinks_limit_but_not_below_first_group() {
        // 5 rows at 100 bytes each against a 200-byte cap: the byte target
        // allows 1 row, but the first tuple group holds 2 rows.
        let mut p = pager(4, 200);
        let step = plan(&mut p, &[batch(&["a", "a", "b", "c", "d"])], 500);
        assert_eq!(step, InKeyStep::Retry);
        assert_eq!(p.limit(), 2);
    }

    #[test]
    fn byte_overflow_of_one_tuple_is_unsplittable() {
        let mut p = pager(4, 200);
        let step = plan(&mut p, &[batch(&["a"; 5])], 500);
        assert!(matches!(step, InKeyStep::Unsplittable(m) if m.contains("max_page_bytes")));
    }

    #[test]
    fn complete_first_group_that_fits_is_emitted() {
        // limit == first group: the page is [a, a] plus one lookahead row [b],
        // which proves the group complete. The group alone fits max_page_bytes.
        let mut p = pager(4, 200);
        plan(&mut p, &[batch(&["a", "a", "b", "c", "d"])], 500);
        assert_eq!(p.limit(), 2);
        let step = plan(&mut p, &[batch(&["a", "a", "b"])], 300);
        assert_eq!(
            step,
            InKeyStep::Emit {
                rows: 2,
                cursor: vec![utf8("a")]
            }
        );
    }

    #[test]
    fn complete_first_group_over_max_bytes_is_unsplittable() {
        let mut p = pager(4, 20);
        plan(&mut p, &[batch(&["a", "a", "b", "c", "d"])], 500);
        assert_eq!(p.limit(), 2);
        let step = plan(&mut p, &[batch(&["a", "a", "b"])], 300);
        assert!(matches!(step, InKeyStep::Unsplittable(m) if m.contains("max_page_bytes=20")));
    }

    #[test]
    fn single_group_after_byte_shrink_regrows_within_the_byte_budget() {
        let mut p = pager(4, 200);
        plan(&mut p, &[batch(&["a", "a", "b", "c", "d"])], 500);
        assert_eq!(p.limit(), 2);
        // Next cursor lands on a 3-row tuple at 50 bytes a row: LIMIT 3 reads
        // only that tuple, and 3 rows is all the byte target allows.
        let step = plan(&mut p, &[batch(&["x", "x", "x"])], 150);
        assert_eq!(step, InKeyStep::Retry);
        assert_eq!(p.limit(), 3);
    }

    #[test]
    fn slow_page_shrinks_the_next_limit_to_the_time_budget() {
        let ids: Vec<String> = (0..1001).map(|i| format!("{i:05}")).collect();
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
        let mut p = pager(1000, u64::MAX);
        let step = p.plan(&[batch(&ids)], &keys(), 0, BUDGET * 2).unwrap();
        assert!(
            matches!(step, InKeyStep::Emit { rows: 1000, .. }),
            "{step:?}"
        );
        assert_eq!(p.limit(), 500);
    }

    #[test]
    fn timeout_halving_is_not_undone_by_a_single_group_regrow() {
        // A tuple group larger than every limit that completes in time must fail,
        // not loop between the limit that timed out and its half.
        let mut p = pager(8, u64::MAX);
        assert!(p.on_timeout());
        assert_eq!(p.limit(), 4);
        let step = plan(&mut p, &[batch(&["x"; 5])], 0);
        assert!(
            p.limit() < 8,
            "regrew to the limit that timed out: {step:?}"
        );
    }

    #[test]
    fn a_tuple_group_that_only_fits_limits_that_time_out_fails() {
        // Group of 7 rows; every limit >= 5 times out. The pager must give up.
        let mut p = pager(8, u64::MAX);
        for _ in 0..20 {
            let step = if p.limit() >= 5 {
                assert!(p.on_timeout());
                continue;
            } else {
                let rows = (p.limit() + 1) as usize;
                plan(&mut p, &[batch(&vec!["x"; rows])], 0)
            };
            match step {
                InKeyStep::Retry => {}
                InKeyStep::Unsplittable(_) => return,
                other => panic!("unexpected {other:?}"),
            }
        }
        panic!("the pager kept retrying a tuple group it cannot read");
    }

    #[test]
    fn emit_after_timeout_does_not_return_to_the_timed_out_limit() {
        let mut p = pager(8, u64::MAX);
        p.on_timeout();
        let step = plan(&mut p, &[batch(&["a", "b", "c", "d", "e"])], 0);
        assert!(matches!(step, InKeyStep::Emit { .. }), "{step:?}");
        assert!(
            p.limit() < 8,
            "the next page re-tries the limit that timed out"
        );
    }

    #[test]
    fn a_one_row_page_that_times_out_cannot_shrink() {
        let mut p = pager(2, u64::MAX);
        assert!(p.on_timeout());
        assert_eq!(p.limit(), 1);
        assert!(!p.on_timeout());
    }

    #[test]
    fn fit_to_sizes_the_first_page_from_an_overflowing_read() {
        let mut p = pager(1_000, 1_000);
        p.fit_to(500, 2_000);
        assert_eq!(p.limit(), 225);
        p.fit_to(500, 100);
        assert_eq!(p.limit(), 1_000);
    }

    #[test]
    fn page_without_cursor_column_is_an_error() {
        let mut p = pager(4, u64::MAX);
        let page = batch(&["a"]).project(&[0, 1]).unwrap();
        let err = p.plan(&[page], &keys(), 0, Duration::ZERO).unwrap_err();
        assert!(err.to_string().contains("no Utf8 cursor column"), "{err}");
    }

    #[test]
    fn payload_takes_the_prefix_without_cursor_columns() {
        let pages = vec![batch(&["a", "b", "c"]), batch(&["d", "e", "f"])];
        let page = payload(pages.clone(), Some(4), 1).unwrap();
        assert_eq!(
            page.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![3, 1]
        );
        assert!(page.iter().all(|b| b.num_columns() == 2));
        assert_eq!(payload(pages.clone(), None, 1).unwrap().len(), 2);
        assert!(payload(pages, Some(0), 1).unwrap().is_empty());
    }

    #[test]
    fn float_and_nested_sort_keys_cannot_be_paged_within() {
        let k = vec!["a".to_string(), "b".to_string()];
        let types = |dt: DataType| {
            Schema::new(vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", dt, true),
            ])
        };
        assert_eq!(unsupported_reason(&k, &types(DataType::UInt64)), None);
        assert_eq!(
            unsupported_reason(&k, &types(DataType::FixedSizeBinary(66))),
            None
        );
        assert!(unsupported_reason(&k, &types(DataType::Float64)).is_some());
        let list = DataType::List(Arc::new(Field::new("item", DataType::UInt8, true)));
        assert!(unsupported_reason(&k, &types(list)).is_some());
        assert!(unsupported_reason(&[], &Schema::empty()).is_some());
    }

    #[test]
    fn cursor_validation_checks_arity_and_renderable_types() {
        let k = keys();
        assert!(validate_cursor(&[utf8("a")], &k).is_ok());
        assert!(validate_cursor(&[ScalarValue::UInt64(Some(1))], &k).is_ok());
        assert!(validate_cursor(&[ScalarValue::Utf8(None)], &k).is_ok());
        assert!(validate_cursor(&[ScalarValue::Float64(Some(1.0))], &k).is_err());
        assert!(validate_cursor(&[utf8("a"), utf8("b")], &k).is_err());
    }
}
