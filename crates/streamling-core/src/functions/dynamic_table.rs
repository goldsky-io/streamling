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
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
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
            signature: Signature::one_of(
                vec![
                    // dynamic table name, scalar value
                    TypeSignature::Exact(vec![Utf8, Utf8]),
                    // dynamic table name, array of values (any-match)
                    TypeSignature::Exact(vec![
                        Utf8,
                        DataType::List(Arc::new(Field::new_list_field(Utf8, true))),
                    ]),
                    TypeSignature::Exact(vec![
                        Utf8,
                        DataType::LargeList(Arc::new(Field::new_list_field(Utf8, true))),
                    ]),
                ],
                Volatility::Volatile,
            ),
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
}
