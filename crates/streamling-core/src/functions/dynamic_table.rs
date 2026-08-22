use crate::dynamic_table::{DynamicTableBackend, DynamicTableRegistry};
use crate::error::{ResultExt, StreamlingError};
use crate::{streamling_user_bail, streamling_user_err};
use datafusion::arrow::array::builder::{BooleanBuilder, StringBuilder};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, GenericListArray, LargeListArray, ListArray, OffsetSizeTrait,
    StringArray,
};
use datafusion::arrow::datatypes::DataType::Boolean;
use datafusion::arrow::datatypes::DataType::Utf8;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
use tracing::debug;

#[derive(Debug)]
pub struct DynamicTableCheckFunc {
    registry: DynamicTableRegistry,
    signature: Signature,
}

// `DynamicTableRegistry` wraps runtime state (`Arc<RwLock<HashMap<..>>>`) that is
// not comparable, so UDF identity (required by df54's `ScalarUDFImpl: DynEq + DynHash`)
// is defined purely by the signature.
impl PartialEq for DynamicTableCheckFunc {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}
impl Eq for DynamicTableCheckFunc {}
impl std::hash::Hash for DynamicTableCheckFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl DynamicTableCheckFunc {
    pub fn new(registry: DynamicTableRegistry) -> Self {
        Self {
            registry,
            // Accept `(Utf8, Utf8)` plus any list-of-strings as the value arg.
            // `coerce_types` normalises the value arg to a canonical scalar
            // `Utf8` or `List(Utf8)`/`LargeList(Utf8)` so the exact child field
            // name / nullability of the input (e.g. Avro `array<string>` uses a
            // different child field) never prevents the list overload from
            // matching and does not fall back to casting the list to a string.
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }

    fn get_backend(&self, name: &str) -> Result<Arc<dyn DynamicTableBackend>> {
        let backend = self.registry.get(name)?.ok_or_else(|| {
            streamling_user_err!("No dynamic table backend found for name '{}'", name)
        })?;
        Ok(backend)
    }

    /// Validate that all table names in the array are the same
    fn validate_table_names_and_extract_first(
        &self,
        table_names_arr: &StringArray,
    ) -> Result<String> {
        if table_names_arr.is_empty() {
            streamling_user_bail!("Table names array cannot be empty");
        }

        if table_names_arr.is_null(0) {
            streamling_user_bail!("Table name cannot be null");
        }

        let first_table_name = table_names_arr.value(0);

        // Validate all table names are the same
        for i in 1..table_names_arr.len() {
            if !table_names_arr.is_null(i) && table_names_arr.value(i) != first_table_name {
                streamling_user_bail!(
                    "All table names must be the same. Found '{}' and '{}'",
                    first_table_name,
                    table_names_arr.value(i)
                );
            }
        }

        Ok(String::from(first_table_name))
    }

    fn execute(
        &self,
        table_names: &ColumnarValue,
        values: &ColumnarValue,
    ) -> Result<ColumnarValue> {
        let mut table_name: Option<String> = None;

        if let ColumnarValue::Array(tn_arr) = table_names
            && let Some(table_names_arr) = tn_arr.as_any().downcast_ref::<StringArray>()
        {
            table_name = Some(self.validate_table_names_and_extract_first(table_names_arr)?);
        } else if let ColumnarValue::Scalar(ScalarValue::Utf8(value)) = table_names {
            table_name = value.clone();
        }

        if table_name.is_none() {
            streamling_user_bail!(
                "dynamic_table_check requires string array or string scalar as first argument"
            );
        }
        let table_name = table_name.unwrap();

        if let ColumnarValue::Array(v_arr) = values {
            if let Some(list) = v_arr.as_any().downcast_ref::<ListArray>() {
                return self.check_list(table_name.as_str(), list);
            }
            if let Some(list) = v_arr.as_any().downcast_ref::<LargeListArray>() {
                return self.check_list(table_name.as_str(), list);
            }
            // Dictionary-encoded strings (common from parquet/avro sources):
            // probe only the K distinct dictionary values, then map the answers
            // back onto the N rows through the keys. A hand-rolled per-batch
            // dedup was measured slower than just reusing the encoding.
            use arrow::array::downcast_dictionary_array;
            use arrow::compute::{cast, take};

            let mut dict_result: Option<ArrayRef> = None;
            let mut decoded_result: Option<ArrayRef> = None;
            downcast_dictionary_array! {
                v_arr => {
                    let values_arr = v_arr.values();
                    if values_arr.as_any().downcast_ref::<StringArray>().is_some() {
                        // The dictionary path trades K probes + one `take` for N
                        // probes. It only wins when K is meaningfully smaller than
                        // N: measured 1.39x faster at K=N/10 but 1.25-1.39x SLOWER
                        // at K=N, because `take` costs ~11ns/row regardless.
                        // `values()` is the WHOLE dictionary, not just the entries
                        // this batch references, so a small slice of a large
                        // row-group dictionary would otherwise probe far more
                        // values than it has rows.
                        let values_len = values_arr.len();
                        let rows_len = v_arr.len();
                        let worth_it = values_len.saturating_mul(2) <= v_arr.keys().len();
                        let ratio = values_len as f64 / rows_len.max(1) as f64;
                        debug!(
                            "dynamic_table_check '{}': dictionary path {} (K={} distinct values, N={} rows, K/N={:.2})",
                            table_name,
                            if worth_it { "taken (fast path)" } else { "skipped (decoding to StringArray)" },
                            values_len,
                            rows_len,
                            ratio,
                        );
                        if worth_it {
                            // Probe the K distinct dictionary values, then map the
                            // answers back onto the N rows through the keys.
                            let values_contains =
                                self.lookup(table_name.as_str(), Arc::clone(values_arr))?;
                            dict_result = Some(take(&values_contains, v_arr.keys(), None)?);
                        } else {
                            // Not actually compressing: decode once and probe the N
                            // rows like the plain StringArray path. Casting a
                            // dictionary to its value type is `take` over the keys,
                            // so null keys and null dictionary values produce the
                            // same null rows as the fast path.
                            decoded_result = Some(cast(v_arr, &Utf8)?);
                        }
                    }
                }
                _ => {}
            }
            if let Some(result) = dict_result {
                return Ok(ColumnarValue::Array(result));
            }
            if let Some(decoded) = decoded_result {
                let result_array_ref = self.lookup(table_name.as_str(), decoded)?;
                // The result is already a BooleanArray wrapped in ArrayRef
                return Ok(ColumnarValue::Array(result_array_ref));
            }
            if let Some(values_arr) = v_arr.as_any().downcast_ref::<StringArray>() {
                debug!(
                    "Using dynamic table backend for table '{}' with {} values",
                    table_name,
                    values_arr.len()
                );
                let result_array_ref = self.lookup(table_name.as_str(), Arc::clone(v_arr))?;
                // The result is already a BooleanArray wrapped in ArrayRef
                return Ok(ColumnarValue::Array(result_array_ref));
            }
        }

        Err(streamling_user_err!("dynamic_table_check requires string array arguments").into())
    }

    /// Block on the async `backend.contains()` lookup for the given table.
    ///
    /// Prefers an existing Tokio runtime via `block_in_place`; otherwise falls
    /// back to a temporary runtime so the UDF works outside a Tokio context.
    fn lookup(&self, table_name: &str, values: ArrayRef) -> Result<ArrayRef> {
        let backend = self.get_backend(table_name)?;

        let result_array_ref = match Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(backend.contains(values))),
            Err(_) => {
                let rt = Runtime::new().streamling_context("failed to create Tokio runtime")?;
                rt.block_on(backend.contains(values))
            }
        }
        .map_err(StreamlingError::from)
        .streamling_context(format!("failed to check dynamic table '{}'", table_name))?;

        Ok(result_array_ref)
    }

    /// Any-match membership over a `List`/`LargeList` of `Utf8` values.
    ///
    /// Returns `true` for a row iff any element of its list is present in the
    /// dynamic table. Null lists yield null, empty lists yield false, and null
    /// elements are skipped. Distinct non-null values across the whole batch are
    /// deduplicated so the backend lookup runs once per unique string.
    fn check_list<O: OffsetSizeTrait>(
        &self,
        table_name: &str,
        list: &GenericListArray<O>,
    ) -> Result<ColumnarValue> {
        let child = list.values();
        let child_strings = child
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                streamling_user_err!("dynamic_table_check list elements must be of type text")
            })?;

        // Deduplicate distinct non-null strings across the whole batch, keeping a
        // value -> index map into the unique array so the fold stays O(elements).
        let mut unique_index: HashMap<&str, usize> = HashMap::new();
        let mut unique_builder = StringBuilder::new();
        for i in 0..child_strings.len() {
            if child_strings.is_null(i) {
                continue;
            }
            let value = child_strings.value(i);
            if !unique_index.contains_key(value) {
                let idx = unique_index.len();
                unique_index.insert(value, idx);
                unique_builder.append_value(value);
            }
        }
        let unique_values: ArrayRef = Arc::new(unique_builder.finish());

        debug!(
            "Using dynamic table backend for table '{}' with {} unique values across {} rows",
            table_name,
            unique_values.len(),
            list.len()
        );

        let unique_hits_ref = self.lookup(table_name, unique_values)?;
        let unique_hits = unique_hits_ref
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                streamling_user_err!("dynamic table backend did not return a boolean array")
            })?;

        let offsets = list.value_offsets();
        let mut result = BooleanBuilder::with_capacity(list.len());
        for row in 0..list.len() {
            if list.is_null(row) {
                result.append_null();
                continue;
            }
            let start = offsets[row].as_usize();
            let end = offsets[row + 1].as_usize();
            let mut any_hit = false;
            for j in start..end {
                if child_strings.is_null(j) {
                    continue;
                }
                let value = child_strings.value(j);
                if let Some(&idx) = unique_index.get(value)
                    && unique_hits.value(idx)
                {
                    any_hit = true;
                    break;
                }
            }
            result.append_value(any_hit);
        }

        Ok(ColumnarValue::Array(Arc::new(result.finish())))
    }
}

impl ScalarUDFImpl for DynamicTableCheckFunc {
    fn name(&self) -> &str {
        "dynamic_table_check"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            streamling_user_bail!("dynamic_table_check requires exactly two arguments");
        }

        fn is_stringish(dt: &DataType) -> bool {
            matches!(dt, Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
        }

        // Table name: any string type → Utf8.
        let name_type = if is_stringish(&arg_types[0]) {
            Utf8
        } else {
            streamling_user_bail!(
                "dynamic_table_check table name must be a string, got {:?}",
                arg_types[0]
            );
        };

        // Value: a scalar string, or a list/fixed-size-list/large-list of
        // strings. Normalise lists to a canonical `List(Utf8)` / `LargeList(Utf8)`
        // (child field name / nullability agnostic) so the list overload always
        // matches and DataFusion never falls back to casting the list to a string.
        let list_field = || Arc::new(Field::new_list_field(Utf8, true));
        let value_type = match &arg_types[1] {
            dt if is_stringish(dt) => Utf8,
            DataType::List(f) | DataType::FixedSizeList(f, _) if is_stringish(f.data_type()) => {
                DataType::List(list_field())
            }
            DataType::LargeList(f) if is_stringish(f.data_type()) => {
                DataType::LargeList(list_field())
            }
            // Keep the dictionary encoding: returning the dictionary type means
            // DataFusion inserts no cast, so `execute` can probe the K distinct
            // values instead of all N rows. Normalising only the VALUE type to
            // Utf8 costs at most a cast of the K dictionary values.
            DataType::Dictionary(key, value) if is_stringish(value) => {
                DataType::Dictionary(key.clone(), Box::new(Utf8))
            }
            other => streamling_user_bail!(
                "dynamic_table_check value must be a string or array of strings, got {:?}",
                other
            ),
        };

        Ok(vec![name_type, value_type])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            streamling_user_bail!("dynamic_table_check requires exactly two arguments");
        }

        self.execute(&args.args[0], &args.args[1])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamic_table::InMemoryDynamicTableBackend;
    use datafusion::arrow::array::builder::{LargeListBuilder, ListBuilder, StringBuilder};
    use datafusion::arrow::array::{DictionaryArray, Int32Array};

    async fn make_func(entries: &[&str]) -> DynamicTableCheckFunc {
        let registry = DynamicTableRegistry::new();
        let backend = Arc::new(InMemoryDynamicTableBackend::new(
            "tbl".to_string(),
            None,
            None,
            None,
            1024,
        ));
        let arr: ArrayRef = Arc::new(StringArray::from(entries.to_vec()));
        backend.append(arr).await.expect("append failed");
        registry
            .register("tbl".to_string(), backend)
            .expect("register failed");
        DynamicTableCheckFunc::new(registry)
    }

    fn table_name() -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("tbl".to_string())))
    }

    fn build_list(rows: Vec<Option<Vec<Option<&str>>>>) -> ColumnarValue {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                None => builder.append(false),
                Some(elems) => {
                    for e in elems {
                        match e {
                            Some(s) => builder.values().append_value(s),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                }
            }
        }
        ColumnarValue::Array(Arc::new(builder.finish()))
    }

    fn build_large_list(rows: Vec<Option<Vec<Option<&str>>>>) -> ColumnarValue {
        let mut builder = LargeListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                None => builder.append(false),
                Some(elems) => {
                    for e in elems {
                        match e {
                            Some(s) => builder.values().append_value(s),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                }
            }
        }
        ColumnarValue::Array(Arc::new(builder.finish()))
    }

    fn assert_bools(result: ColumnarValue, expected: &[Option<bool>]) {
        let arr = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array result"),
        };
        let bools = arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("expected boolean array");
        assert_eq!(bools.len(), expected.len(), "row count mismatch");
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                None => assert!(bools.is_null(i), "row {i} expected null"),
                Some(v) => {
                    assert!(!bools.is_null(i), "row {i} expected non-null");
                    assert_eq!(bools.value(i), *v, "row {i} value mismatch");
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scalar_overload_unchanged() {
        let func = make_func(&["hit"]).await;
        let values = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("hit"),
            Some("miss"),
            None,
        ])));
        let result = func
            .execute(&table_name(), &values)
            .expect("execute failed");
        assert_bools(result, &[Some(true), Some(false), None]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_any_match_and_all_miss() {
        let func = make_func(&["hit"]).await;
        let values = build_list(vec![
            Some(vec![Some("miss"), Some("hit")]),
            Some(vec![Some("nope"), Some("miss")]),
        ]);
        let result = func
            .execute(&table_name(), &values)
            .expect("execute failed");
        assert_bools(result, &[Some(true), Some(false)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_null_and_empty_semantics() {
        let func = make_func(&["hit"]).await;
        let values = build_list(vec![None, Some(vec![]), Some(vec![Some("hit")])]);
        let result = func
            .execute(&table_name(), &values)
            .expect("execute failed");
        assert_bools(result, &[None, Some(false), Some(true)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_null_elements_skipped() {
        let func = make_func(&["hit"]).await;
        let values = build_list(vec![
            Some(vec![None, Some("hit")]),
            Some(vec![None, Some("miss")]),
            Some(vec![None]),
        ]);
        let result = func
            .execute(&table_name(), &values)
            .expect("execute failed");
        assert_bools(result, &[Some(true), Some(false), Some(false)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_dedup_does_not_change_results() {
        let func = make_func(&["hit"]).await;
        let values = build_list(vec![
            // duplicates within one row
            Some(vec![Some("hit"), Some("hit"), Some("miss")]),
            // duplicate value across rows
            Some(vec![Some("miss"), Some("miss")]),
        ]);
        let result = func
            .execute(&table_name(), &values)
            .expect("execute failed");
        assert_bools(result, &[Some(true), Some(false)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn large_list_any_match() {
        let func = make_func(&["hit"]).await;
        let values = build_large_list(vec![
            Some(vec![Some("miss"), Some("hit")]),
            None,
            Some(vec![]),
        ]);
        let result = func
            .execute(&table_name(), &values)
            .expect("execute failed");
        assert_bools(result, &[Some(true), None, Some(false)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dictionary_matches_plain_string_results() {
        let func = make_func(&["a", "b"]).await;

        // Dictionary values ["a", "b", "zzz"], keys cover repeats: logical rows
        // are ["a", "b", "a", "zzz", "b", "b"].
        let keys = Int32Array::from(vec![Some(0), Some(1), Some(0), Some(2), Some(1), Some(1)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "zzz"]));
        let dict: ArrayRef = Arc::new(DictionaryArray::new(keys, values));

        let result = func
            .execute(&table_name(), &ColumnarValue::Array(dict))
            .expect("execute failed");
        assert_bools(
            result,
            &[
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(true),
            ],
        );

        // The equivalent plain StringArray over the same logical rows must
        // produce the same answers (and the same nulls).
        let plain = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            Some("zzz"),
            Some("b"),
            Some("b"),
        ])));
        let result = func.execute(&table_name(), &plain).expect("execute failed");
        assert_bools(
            result,
            &[
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(true),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dictionary_null_key_yields_null() {
        let func = make_func(&["a", "b"]).await;
        // Logical rows: ["a", null, "b"] — the null key must yield a null row.
        let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let dict: ArrayRef = Arc::new(DictionaryArray::new(keys, values));

        let result = func
            .execute(&table_name(), &ColumnarValue::Array(dict))
            .expect("execute failed");
        assert_bools(result, &[Some(true), None, Some(true)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dictionary_null_value_yields_null() {
        let func = make_func(&["a", "b"]).await;
        // Dictionary values entry 1 is null; every row referencing it (row 1)
        // must be null, matching the plain-string path's null semantics.
        let keys = Int32Array::from(vec![Some(0), Some(1), Some(2)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
        let dict: ArrayRef = Arc::new(DictionaryArray::new(keys, values));

        let result = func
            .execute(&table_name(), &ColumnarValue::Array(dict))
            .expect("execute failed");
        assert_bools(result, &[Some(true), None, Some(true)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dictionary_low_cardinality_uses_fast_path() {
        let func = make_func(&["a"]).await;
        // K=2 distinct values, N=6 rows: K <= N/2, so the dictionary fast path
        // is taken. The null key (row 1) must stay null through `take`.
        let keys = Int32Array::from(vec![Some(0), None, Some(1), Some(0), Some(1), Some(0)]);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "zzz"]));
        let dict: ArrayRef = Arc::new(DictionaryArray::new(keys, values));

        let result = func
            .execute(&table_name(), &ColumnarValue::Array(dict))
            .expect("execute failed");
        let expected: &[Option<bool>] = &[
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(false),
            Some(true),
        ];
        assert_bools(result, expected);

        // The equivalent plain StringArray over the same logical rows must
        // produce the same answers (and the same nulls).
        let plain = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("zzz"),
            Some("a"),
            Some("zzz"),
            Some("a"),
        ])));
        let result = func.execute(&table_name(), &plain).expect("execute failed");
        assert_bools(result, expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dictionary_high_cardinality_falls_back() {
        let func = make_func(&["a", "b"]).await;
        // K=3 distinct values, N=3 rows: K > N/2, so the fast path is skipped
        // and the input is decoded before probing. The null dictionary value
        // (entry 1) must yield null for the row referencing it, and the null
        // key must yield null — both preserved by the decode.
        let keys = Int32Array::from(vec![Some(0), Some(1), None]);
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("b")]));
        let dict: ArrayRef = Arc::new(DictionaryArray::new(keys, values));

        let result = func
            .execute(&table_name(), &ColumnarValue::Array(dict))
            .expect("execute failed");
        let expected: &[Option<bool>] = &[Some(true), None, None];
        assert_bools(result, expected);

        // The equivalent plain StringArray over the same logical rows.
        let plain = ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("a"), None, None])));
        let result = func.execute(&table_name(), &plain).expect("execute failed");
        assert_bools(result, expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dictionary_all_distinct_matches_plain() {
        let func = make_func(&["a"]).await;
        // K == N == 3 with every key distinct — the worst case for the fast
        // path (measured slower than probing rows directly), so it falls back.
        // Must still be exactly correct, including the null key and the miss.
        let keys = Int32Array::from(vec![Some(0), None, Some(2)]);
        let values: ArrayRef =
            Arc::new(StringArray::from(vec![Some("a"), Some("miss"), Some("b")]));
        let dict: ArrayRef = Arc::new(DictionaryArray::new(keys, values));

        let result = func
            .execute(&table_name(), &ColumnarValue::Array(dict))
            .expect("execute failed");
        let expected: &[Option<bool>] = &[Some(true), None, Some(false)];
        assert_bools(result, expected);

        // The equivalent plain StringArray over the same logical rows.
        let plain = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
        ])));
        let result = func.execute(&table_name(), &plain).expect("execute failed");
        assert_bools(result, expected);
    }

    /// Guards against a future "simplification" back to plain `Utf8` in
    /// `coerce_types`, which would silently reintroduce a full-column decode on
    /// every batch instead of probing only the K dictionary values.
    #[test]
    fn dictionary_coerce_types_preserves_encoding() {
        let func = DynamicTableCheckFunc::new(DynamicTableRegistry::new());
        let value_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        let coerced = func
            .coerce_types(&[Utf8, value_type])
            .expect("coerce failed");
        assert_eq!(
            coerced[1],
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(Utf8))
        );
    }

    #[test]
    fn dictionary_with_large_utf8_values_is_accepted() {
        let func = DynamicTableCheckFunc::new(DynamicTableRegistry::new());
        let value_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8));

        let coerced = func
            .coerce_types(&[Utf8, value_type])
            .expect("coerce failed");
        // Value type normalised to Utf8, dictionary encoding preserved.
        assert_eq!(
            coerced[1],
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(Utf8))
        );
    }

    /// End-to-end through DataFusion planning/execution: confirms the `one_of`
    /// signature coerces a `List(Utf8)` column argument to the list overload and
    /// filters rows by any-match. This retires the coercion open question.
    #[tokio::test(flavor = "multi_thread")]
    async fn list_overload_coerces_and_runs_via_sql() {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::logical_expr::ScalarUDF;
        use datafusion::prelude::SessionContext;

        let func = make_func(&["hit"]).await;

        let ids: ArrayRef = Arc::new(StringArray::from(vec!["r1", "r2"]));
        let arr = match build_list(vec![
            Some(vec![Some("miss"), Some("hit")]),
            Some(vec![Some("nope")]),
        ]) {
            ColumnarValue::Array(a) => a,
            _ => unreachable!(),
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "arr",
                DataType::List(Arc::new(Field::new_list_field(Utf8, true))),
                true,
            ),
        ]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![ids, arr]).expect("batch build failed");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable build failed");

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(table))
            .expect("register table failed");
        ctx.register_udf(ScalarUDF::from(func));

        let batches = ctx
            .sql("SELECT id FROM t WHERE dynamic_table_check('tbl', arr) ORDER BY id")
            .await
            .expect("planning failed")
            .collect()
            .await
            .expect("execution failed");

        let mut ids_out = Vec::new();
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id column is Utf8");
            for i in 0..col.len() {
                ids_out.push(col.value(i).to_string());
            }
        }
        assert_eq!(ids_out, vec!["r1"]);
    }

    /// Regression for STRM-6445: a list column whose child field name /
    /// nullability differs from the canonical `List(Field("item", Utf8, true))`
    /// (e.g. Avro `array<string>`, which uses a non-nullable "element" child)
    /// must still hit the list overload. Before the `coerce_types` fix,
    /// DataFusion fell back to casting the list to a string and this returned
    /// zero rows.
    #[tokio::test(flavor = "multi_thread")]
    async fn list_overload_matches_alternate_child_field() {
        use datafusion::arrow::array::ListArray;
        use datafusion::arrow::buffer::OffsetBuffer;
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::logical_expr::ScalarUDF;
        use datafusion::prelude::SessionContext;

        let func = make_func(&["hit"]).await;
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["r1", "r2"]));
        // r1 => [miss, hit], r2 => [nope]. Non-canonical child: name "element",
        // non-nullable — mirrors what the Avro reader produces.
        let values = StringArray::from(vec![Some("miss"), Some("hit"), Some("nope")]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
        let field = Arc::new(Field::new("element", Utf8, false));
        let list = ListArray::new(field.clone(), offsets, Arc::new(values), None);
        let arr: ArrayRef = Arc::new(list);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("arr", DataType::List(field), true),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![ids, arr]).expect("batch build failed");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable build failed");
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(table))
            .expect("register table failed");
        ctx.register_udf(ScalarUDF::from(func));

        let batches = ctx
            .sql("SELECT id FROM t WHERE dynamic_table_check('tbl', arr) ORDER BY id")
            .await
            .expect("planning failed")
            .collect()
            .await
            .expect("execution failed");

        let mut ids_out = Vec::new();
        for b in &batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("id column is Utf8");
            for i in 0..col.len() {
                ids_out.push(col.value(i).to_string());
            }
        }
        assert_eq!(ids_out, vec!["r1"]);
    }
}
