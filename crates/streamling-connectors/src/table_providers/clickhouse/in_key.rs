use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use streamling_core::error::{Result, ResultExt};
use streamling_core::streamling_err;

/// Paging inside ONE first-sort-key value that alone overflows a page.
///
/// Range pagination cannot split a single first-key value, so such a key is
/// read ordered by the full sorting key with `LIMIT limit + 1`, keyset-paged on
/// the remaining keys (`(k1, ..) > cursor`). Pages are cut only at a full
/// sort-key tuple boundary: the trailing tuple group of a full page is dropped
/// and re-read by the next page, so every version of one tuple lands in the
/// same page and version-aware dedup stays correct.
///
/// `limit` is the row budget of the next page. It starts at `page_size`,
/// shrinks under the byte tripwire, and never shrinks below the first tuple
/// group of the page it re-reads, so every re-read makes progress.
#[derive(Debug, Clone)]
pub struct InKeyPager {
    page_size: u64,
    max_page_bytes: u64,
    limit: u64,
}

/// An in-progress scan within the first-key value at the range cursor.
#[derive(Debug, Clone)]
pub struct InKeyScan {
    /// Remaining sort-key tuple to read strictly after; empty on the first page.
    pub after: Vec<ScalarValue>,
    pub pager: InKeyPager,
}

impl InKeyScan {
    pub fn new(page_size: u64, max_page_bytes: u64) -> Self {
        Self::resume(Vec::new(), InKeyPager::new(page_size, max_page_bytes))
    }

    pub fn resume(after: Vec<ScalarValue>, pager: InKeyPager) -> Self {
        Self { after, pager }
    }
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
    /// A single full sort-key tuple alone exceeds the page limits.
    Unsplittable(String),
}

impl InKeyPager {
    /// Fraction of `max_page_bytes` a byte-sized limit targets, as in
    /// `RangeController::BYTE_TARGET_RATIO`.
    const BYTE_TARGET_RATIO: f64 = 0.9;

    pub fn new(page_size: u64, max_page_bytes: u64) -> Self {
        Self {
            page_size,
            max_page_bytes,
            limit: page_size,
        }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// A timed-out page is re-read with half the rows.
    pub fn on_timeout(&mut self) {
        self.limit = (self.limit / 2).max(1);
    }

    /// Largest limit whose page stays under the byte target, at the observed
    /// bytes-per-row density.
    fn byte_limit(&self, rows: usize, bytes: u64) -> u64 {
        if bytes == 0 || rows == 0 {
            return self.page_size;
        }
        let per_row = bytes as f64 / rows as f64;
        ((self.max_page_bytes as f64 * Self::BYTE_TARGET_RATIO) / per_row) as u64
    }

    /// Decide what to do with a buffered page of `bytes` bytes read at the
    /// current limit. `rest_keys` are the sorting keys after the first; the
    /// batches are ordered by the full sorting key.
    pub fn plan(
        &mut self,
        batches: &[RecordBatch],
        rest_keys: &[String],
        bytes: u64,
    ) -> Result<InKeyStep> {
        let rows = TupleRows::new(batches, rest_keys)?;
        let n = rows.len();
        let limit = self.limit;

        if bytes > self.max_page_bytes {
            let first_group = rows.first_group_len()?;
            if first_group == n {
                return Ok(InKeyStep::Unsplittable(format!(
                    "a single sort-key tuple {:?} holds more than max_page_bytes={} ({} rows read, {} bytes)",
                    rows.tuple(0)?,
                    self.max_page_bytes,
                    n,
                    bytes
                )));
            }
            let shrunk = self
                .byte_limit(n, bytes)
                .min(limit.saturating_sub(1))
                .max(first_group as u64);
            if shrunk >= limit {
                return Ok(InKeyStep::Unsplittable(format!(
                    "sort-key tuple {:?} ({} rows) plus one lookahead row exceeds max_page_bytes={} ({} bytes)",
                    rows.tuple(0)?,
                    first_group,
                    self.max_page_bytes,
                    bytes
                )));
            }
            self.limit = shrunk.max(1);
            return Ok(InKeyStep::Retry);
        }

        if n as u64 <= limit {
            self.limit = self.byte_limit(n, bytes).clamp(1, self.page_size);
            return Ok(InKeyStep::Finish);
        }

        // Full page (`limit + 1` rows): more of this key remains. Cut before the
        // trailing tuple group, whose remaining versions may lie past the LIMIT.
        let cut = rows.last_group_start()?;
        if cut == 0 {
            if limit < self.page_size {
                self.limit = self.page_size;
                return Ok(InKeyStep::Retry);
            }
            return Ok(InKeyStep::Unsplittable(format!(
                "a single sort-key tuple {:?} holds more than page_size={} rows",
                rows.tuple(0)?,
                self.page_size
            )));
        }
        self.limit = self.byte_limit(n, bytes).clamp(1, self.page_size);
        Ok(InKeyStep::Emit {
            rows: cut,
            cursor: rows.tuple(cut - 1)?,
        })
    }
}

/// Row-indexed access to the remaining sort-key tuple of a page split across
/// several batches.
struct TupleRows<'a> {
    batches: &'a [RecordBatch],
    /// Column index of each remaining sort key (shared schema across batches).
    columns: Vec<usize>,
    len: usize,
}

impl<'a> TupleRows<'a> {
    fn new(batches: &'a [RecordBatch], rest_keys: &[String]) -> Result<Self> {
        let columns = match batches.first() {
            Some(first) => rest_keys
                .iter()
                .map(|k| {
                    first.schema().index_of(k).map_err(|_| {
                        streamling_err!(
                            "sorting key '{}' is not a selected column; paging within a \
                             first-sort-key value needs every sorting key in the scan",
                            k
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            None => Vec::new(),
        };
        let len = batches.iter().map(|b| b.num_rows()).sum();
        Ok(Self {
            batches,
            columns,
            len,
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    fn tuple(&self, mut row: usize) -> Result<Vec<ScalarValue>> {
        let batch = self
            .batches
            .iter()
            .find(|b| {
                if row < b.num_rows() {
                    true
                } else {
                    row -= b.num_rows();
                    false
                }
            })
            .ok_or_else(|| streamling_err!("row index out of range"))?;
        self.columns
            .iter()
            .map(|&c| {
                ScalarValue::try_from_array(batch.column(c), row)
                    .streamling_context("failed to read sort-key value")
            })
            .collect()
    }

    fn first_group_len(&self) -> Result<usize> {
        let head = self.tuple(0)?;
        for i in 1..self.len {
            if self.tuple(i)? != head {
                return Ok(i);
            }
        }
        Ok(self.len)
    }

    fn last_group_start(&self) -> Result<usize> {
        let tail = self.tuple(self.len - 1)?;
        for i in (0..self.len - 1).rev() {
            if self.tuple(i)? != tail {
                return Ok(i + 1);
            }
        }
        Ok(0)
    }
}

/// A cursor value must be a non-null scalar that both the checkpoint serde and
/// the SQL literal formatter render losslessly.
pub fn validate_cursor(cursor: &[ScalarValue], rest_keys: &[String]) -> Result<()> {
    for (value, key) in cursor.iter().zip(rest_keys) {
        let supported = matches!(
            value,
            ScalarValue::Int8(Some(_))
                | ScalarValue::Int16(Some(_))
                | ScalarValue::Int32(Some(_))
                | ScalarValue::Int64(Some(_))
                | ScalarValue::UInt8(Some(_))
                | ScalarValue::UInt16(Some(_))
                | ScalarValue::UInt32(Some(_))
                | ScalarValue::UInt64(Some(_))
                | ScalarValue::Utf8(Some(_))
                | ScalarValue::LargeUtf8(Some(_))
        );
        if !supported {
            return Err(streamling_err!(
                "cannot page within a first-sort-key value on sorting key '{}': value {:?} is \
                 not a non-null integer or string",
                key,
                value
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{StringArray, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(ids: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("id", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![7u64; ids.len()])),
                Arc::new(StringArray::from(ids.to_vec())),
            ],
        )
        .unwrap()
    }

    fn keys() -> Vec<String> {
        vec!["id".to_string()]
    }

    fn utf8(s: &str) -> ScalarValue {
        ScalarValue::Utf8(Some(s.to_string()))
    }

    #[test]
    fn full_page_cuts_before_trailing_tuple_group_across_batches() {
        // limit 4 -> LIMIT 5 read. 'c' straddles the batch boundary and may have
        // more versions past the LIMIT, so the cut drops both 'c' rows.
        let mut p = InKeyPager::new(4, u64::MAX);
        let step = p
            .plan(&[batch(&["a", "b", "c"]), batch(&["c", "c"])], &keys(), 0)
            .unwrap();
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
        let mut p = InKeyPager::new(4, u64::MAX);
        let step = p.plan(&[batch(&["a", "a", "b"])], &keys(), 0).unwrap();
        assert_eq!(step, InKeyStep::Finish);
    }

    #[test]
    fn empty_page_finishes_the_key() {
        let mut p = InKeyPager::new(4, u64::MAX);
        assert_eq!(p.plan(&[], &keys(), 0).unwrap(), InKeyStep::Finish);
    }

    #[test]
    fn single_tuple_over_page_size_is_unsplittable() {
        let mut p = InKeyPager::new(4, u64::MAX);
        let step = p.plan(&[batch(&["a"; 5])], &keys(), 0).unwrap();
        assert!(matches!(step, InKeyStep::Unsplittable(m) if m.contains("page_size=4")));
    }

    #[test]
    fn byte_overflow_shrinks_limit_but_not_below_first_group() {
        // 5 rows at 100 bytes each against a 200-byte cap: the byte target
        // allows 1 row, but the first tuple group holds 2 rows.
        let mut p = InKeyPager::new(4, 200);
        let step = p
            .plan(&[batch(&["a", "a", "b", "c", "d"])], &keys(), 500)
            .unwrap();
        assert_eq!(step, InKeyStep::Retry);
        assert_eq!(p.limit(), 2);
    }

    #[test]
    fn byte_overflow_of_one_tuple_is_unsplittable() {
        let mut p = InKeyPager::new(4, 200);
        let step = p.plan(&[batch(&["a"; 5])], &keys(), 500).unwrap();
        assert!(matches!(step, InKeyStep::Unsplittable(m) if m.contains("max_page_bytes")));
    }

    #[test]
    fn byte_overflow_without_progress_is_unsplittable() {
        // limit already equals the first group: no smaller page can help.
        let mut p = InKeyPager::new(4, 200);
        p.plan(&[batch(&["a", "a", "b", "c", "d"])], &keys(), 500)
            .unwrap();
        assert_eq!(p.limit(), 2);
        let step = p.plan(&[batch(&["a", "a", "b"])], &keys(), 300).unwrap();
        assert!(matches!(step, InKeyStep::Unsplittable(m) if m.contains("lookahead")));
    }

    #[test]
    fn single_group_after_byte_shrink_regrows_to_page_size() {
        let mut p = InKeyPager::new(4, 200);
        p.plan(&[batch(&["a", "a", "b", "c", "d"])], &keys(), 500)
            .unwrap();
        assert_eq!(p.limit(), 2);
        // Next cursor lands on a 3-row tuple: LIMIT 3 reads only that tuple.
        let step = p.plan(&[batch(&["x", "x", "x"])], &keys(), 150).unwrap();
        assert_eq!(step, InKeyStep::Retry);
        assert_eq!(p.limit(), 4);
    }

    #[test]
    fn missing_sort_key_column_is_an_error() {
        let mut p = InKeyPager::new(4, u64::MAX);
        let err = p
            .plan(&[batch(&["a"])], &["missing".to_string()], 0)
            .unwrap_err();
        assert!(err.to_string().contains("not a selected column"));
    }

    #[test]
    fn cursor_validation_rejects_null_and_unsupported_types() {
        let k = keys();
        assert!(validate_cursor(&[utf8("a")], &k).is_ok());
        assert!(validate_cursor(&[ScalarValue::UInt64(Some(1))], &k).is_ok());
        assert!(validate_cursor(&[ScalarValue::Utf8(None)], &k).is_err());
        assert!(validate_cursor(&[ScalarValue::Float64(Some(1.0))], &k).is_err());
    }
}
