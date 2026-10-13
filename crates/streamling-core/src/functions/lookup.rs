//! `lookup_eq(table, key, value, column)`: `column` of the row in `table`
//! whose primary key `key` equals `value`, or null.

use crate::table::{InMemoryTable, TableRegistry};
use crate::{streamling_err, streamling_user_bail, streamling_user_err};
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamling_common::types::decimal_arb::DecimalArbType;

pub const LOOKUP_EQ_NAME: &str = "lookup_eq";
const ARGUMENT_COUNT: usize = 4;

/// See the module docs. The table, key and column must be string literals.
#[derive(Debug)]
pub struct LookupEqFunc {
    tables: TableRegistry,
    signature: Signature,
}

// Same identity rule as `DynamicTableCheckFunc`: the registry is runtime
// state, so the UDF is identified by its signature.
impl PartialEq for LookupEqFunc {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}
impl Eq for LookupEqFunc {}
impl std::hash::Hash for LookupEqFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl LookupEqFunc {
    pub fn new(tables: TableRegistry) -> Self {
        Self {
            tables,
            // Volatile: the result changes with the table, so it must never be
            // constant-folded or deduplicated.
            signature: Signature::any(ARGUMENT_COUNT, Volatility::Volatile),
        }
    }

    fn table(&self, name: &str) -> Result<Arc<InMemoryTable>> {
        self.tables.get(name).ok_or_else(|| {
            streamling_user_err!("{}: no table named '{}'", LOOKUP_EQ_NAME, name).into()
        })
    }
}

fn string_literal<'a>(value: Option<&'a ScalarValue>, argument: &str) -> Result<&'a str> {
    value
        .and_then(ScalarValue::try_as_str)
        .flatten()
        .ok_or_else(|| {
            streamling_user_err!(
                "{}: the {} must be a string literal",
                LOOKUP_EQ_NAME,
                argument
            )
            .into()
        })
}

fn decimal_arb_scale(field: &Field) -> Option<u32> {
    DecimalArbType::precision_scale_from_field(field).map(|(_, scale)| scale)
}

fn type_name(field: &Field) -> String {
    match DecimalArbType::precision_scale_from_field(field) {
        Some((precision, scale)) => format!("decimal_arb({precision}, {scale})"),
        None => field.data_type().to_string(),
    }
}

impl ScalarUDFImpl for LookupEqFunc {
    fn name(&self) -> &str {
        LOOKUP_EQ_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(streamling_err!(
            "{}: the return type depends on the table; return_field_from_args resolves it",
            LOOKUP_EQ_NAME
        )
        .into())
    }

    /// Resolves the table, key and column while planning, so a bad call fails
    /// the pipeline before it starts.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let ([table, key, _, column], [_, _, value, _]) = (args.scalar_arguments, args.arg_fields)
        else {
            streamling_user_bail!(
                "{} takes {} arguments: table, key, value, column",
                LOOKUP_EQ_NAME,
                ARGUMENT_COUNT
            );
        };
        let table = self.table(string_literal(*table, "table name")?)?;
        let key = string_literal(*key, "key")?;
        let key_field = table.lookup_key_field(key)?;
        // A decimal_arb key holds unscaled bytes: a value of another type or
        // scale casts without error to bytes that never match.
        if !can_cast_types(value.data_type(), key_field.data_type())
            || decimal_arb_scale(value) != decimal_arb_scale(key_field)
        {
            streamling_user_bail!(
                "{}: cannot compare {} with key '{}' of type {}",
                LOOKUP_EQ_NAME,
                type_name(value),
                key,
                type_name(key_field)
            );
        }
        Ok(table.lookup_field(string_literal(*column, "column")?)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [
            ColumnarValue::Scalar(table_name),
            _,
            keys,
            ColumnarValue::Scalar(column),
        ] = args.args.as_slice()
        else {
            streamling_user_bail!(
                "{}: the table name and column must be string literals",
                LOOKUP_EQ_NAME
            );
        };
        let table = self.table(string_literal(Some(table_name), "table name")?)?;
        let keys = keys.to_array(args.number_rows)?;
        let column = string_literal(Some(column), "column")?;
        Ok(ColumnarValue::Array(table.lookup(&keys, column)?))
    }
}

#[cfg(test)]
mod tests {
    use crate::data::COLUMN_NAME_OP;
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::session::SessionManager;
    use crate::table::InMemoryTable;
    use arrow::array::{AsArray, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;
    use streamling_common::types::decimal_arb::DecimalArbType;

    fn rates_schema(amount: Field) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("address", DataType::Utf8, false),
            amount,
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]))
    }

    /// A session with table `rates` (key `address`) holding `0xa → 10`, and
    /// a source `t` with one `address` column.
    fn session(addresses: Vec<Option<&str>>) -> SessionManager {
        let schema = rates_schema(Field::new("amount", DataType::Int64, false));
        let rates = InMemoryTable::try_new(
            "rates",
            &schema,
            &["address".to_string()],
            "app::rates".to_string(),
            crate::topology::DEFAULT_MAX_DEAD_RATIO,
        )
        .unwrap();
        rates
            .apply(
                &RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringArray::from(vec!["0xa"])),
                        Arc::new(Int64Array::from(vec![10])),
                        Arc::new(StringArray::from(vec!["i"])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        session_manager.tables().register(Arc::new(rates));

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "address",
            DataType::Utf8,
            true,
        )]));
        let input = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![Arc::new(StringArray::from(addresses))],
        )
        .unwrap();
        session_manager
            .session_context()
            .register_table(
                "t",
                Arc::new(MemTable::try_new(input_schema, vec![vec![input]]).unwrap()),
            )
            .unwrap();
        session_manager
    }

    #[tokio::test]
    async fn looks_up_a_column_by_primary_key() {
        let session_manager = session(vec![Some("0xa"), Some("0xb"), None]);
        let batches = session_manager
            .session_context()
            .sql("SELECT lookup_eq('rates', 'address', address, 'amount') FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let amounts: Vec<Option<i64>> = batches
            .iter()
            .flat_map(|batch| batch.column(0).as_primitive::<Int64Type>().iter())
            .collect();
        assert_eq!(amounts, [Some(10), None, None]);
    }

    #[tokio::test]
    async fn rejects_bad_calls_at_planning() {
        let session_manager = session(vec![Some("0xa")]);
        for (call, expected) in [
            (
                "lookup_eq('missing', 'address', address, 'amount')",
                "no table named 'missing'",
            ),
            (
                "lookup_eq(address, 'address', address, 'amount')",
                "the table name must be a string literal",
            ),
            (
                "lookup_eq('rates', 'amount', address, 'amount')",
                "lookups must use the primary key 'address', got 'amount'",
            ),
            (
                "lookup_eq('rates', 'address', address, 'missing')",
                "table 'rates' has no column 'missing'",
            ),
            (
                "lookup_eq('rates', 'address', named_struct('a', 1), 'amount')",
                "cannot compare",
            ),
        ] {
            let err = session_manager
                .session_context()
                .sql(&format!("SELECT {call} FROM t"))
                .await
                .err()
                .unwrap_or_else(|| panic!("{call} must fail"))
                .to_string();
            assert!(err.contains(expected), "{call}: {err}");
        }
    }

    /// decimal_arb keys are stored as unscaled bytes, so a value of another
    /// type or scale casts to bytes that never match.
    #[tokio::test]
    async fn rejects_decimal_arb_type_or_scale_mismatches_at_planning() {
        let id = DecimalArbType::field("id", 78, 2, false).unwrap();
        let ids_schema = Arc::new(Schema::new(vec![
            id.clone(),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        let session_manager = session(vec![Some("0xa")]);
        session_manager.tables().register(Arc::new(
            InMemoryTable::try_new(
                "ids",
                &ids_schema,
                &["id".to_string()],
                "app::ids".to_string(),
                crate::topology::DEFAULT_MAX_DEAD_RATIO,
            )
            .unwrap(),
        ));
        let values_schema = Arc::new(Schema::new(vec![
            DecimalArbType::field("same_scale", 78, 2, true).unwrap(),
            DecimalArbType::field("other_scale", 78, 0, true).unwrap(),
        ]));
        session_manager
            .session_context()
            .register_table(
                "d",
                Arc::new(MemTable::try_new(values_schema, vec![vec![]]).unwrap()),
            )
            .unwrap();
        let plan = |value: &str| {
            let session_manager = session_manager.clone();
            let sql = format!("SELECT lookup_eq('ids', 'id', {value}, 'id') FROM d, t");
            async move { session_manager.session_context().sql(&sql).await }
        };

        plan("same_scale")
            .await
            .expect("a decimal_arb value with the key's scale must plan");
        for value in ["other_scale", "address", "1"] {
            let err = plan(value)
                .await
                .err()
                .unwrap_or_else(|| panic!("{value} must fail"))
                .to_string();
            assert!(
                err.contains("cannot compare") && err.contains("decimal_arb(78, 2)"),
                "{value}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn returns_the_column_field_nullable_with_its_metadata() {
        let amount = DecimalArbType::field("amount", 78, 0, false).unwrap();
        let session_manager = session(vec![Some("0xa")]);
        session_manager.tables().register(Arc::new(
            InMemoryTable::try_new(
                "decimals",
                &rates_schema(amount.clone()),
                &["address".to_string()],
                "app::decimals".to_string(),
                crate::topology::DEFAULT_MAX_DEAD_RATIO,
            )
            .unwrap(),
        ));
        let df = session_manager
            .session_context()
            .sql("SELECT lookup_eq('decimals', 'address', address, 'amount') AS amount FROM t")
            .await
            .unwrap();
        let field = df.schema().field(0);
        assert_eq!(field.metadata(), amount.metadata());
        assert!(field.is_nullable());
    }
}
