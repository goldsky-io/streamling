//! Reads of a table's row by primary key. `key` names the table's whole
//! primary key, comma-separated like its `primary_key`, and one value follows
//! per key column, in order:
//! - `lookup_eq(table, key, value_1, ..., value_n, column)`: `column` of the
//!   row, or null.
//! - `lookup_row(table, key, value_1, ..., value_n)`: a struct of the row's
//!   columns, or null.
//! - `lookup_exists(table, key, value_1, ..., value_n)`: whether the row
//!   exists.

use crate::table::read::TableFunction;
use crate::table::{InMemoryTable, Mark, TableRegistry};
use crate::{streamling_user_bail, streamling_user_err};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, Signature, Volatility,
};
use std::ops::Range;
use std::sync::Arc;
use streamling_common::types::decimal_arb::DecimalArbType;

pub const LOOKUP_EQ_NAME: &str = "lookup_eq";
pub const LOOKUP_ROW_NAME: &str = "lookup_row";
pub const LOOKUP_EXISTS_NAME: &str = "lookup_exists";
/// Every function that reads a table by primary key.
pub const LOOKUP_NAMES: [&str; 3] = [LOOKUP_EQ_NAME, LOOKUP_ROW_NAME, LOOKUP_EXISTS_NAME];

/// The table and key arguments that precede the values.
const LEADING_ARGUMENTS: usize = 2;

/// What a lookup returns per key.
#[derive(Debug, Clone, Copy)]
pub enum LookupResult {
    /// `lookup_eq`: one column of the row.
    Column,
    /// `lookup_row`: the whole row.
    Row,
    /// `lookup_exists`: whether there is a row.
    Exists,
}

impl LookupResult {
    pub const ALL: [Self; 3] = [Self::Column, Self::Row, Self::Exists];

    fn name(self) -> &'static str {
        match self {
            Self::Column => LOOKUP_EQ_NAME,
            Self::Row => LOOKUP_ROW_NAME,
            Self::Exists => LOOKUP_EXISTS_NAME,
        }
    }

    /// The arguments after the values: `lookup_eq`'s column.
    fn trailing_arguments(self) -> usize {
        match self {
            Self::Column => 1,
            Self::Row | Self::Exists => 0,
        }
    }

    fn arguments(self) -> &'static str {
        match self {
            Self::Column => "table, key, value_1, ..., value_n, column",
            Self::Row | Self::Exists => "table, key, value_1, ..., value_n",
        }
    }
}

/// See the module docs. The table, key and column must be string literals.
#[derive(Debug)]
pub struct LookupFunc {
    tables: TableRegistry,
    result: LookupResult,
    signature: Signature,
}

impl LookupFunc {
    pub fn new(tables: TableRegistry, result: LookupResult) -> Self {
        Self {
            tables,
            result,
            // Volatile: the result changes with the table, so it must never be
            // constant-folded or deduplicated.
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }

    fn table_named(&self, name: &str) -> Result<Arc<InMemoryTable>> {
        self.tables.get(name).ok_or_else(|| {
            streamling_user_err!("{}: no table named '{}'", self.result.name(), name).into()
        })
    }

    fn string_literal<'a>(
        &self,
        value: Option<&'a ScalarValue>,
        argument: &str,
    ) -> Result<&'a str> {
        value
            .and_then(ScalarValue::try_as_str)
            .flatten()
            .ok_or_else(|| {
                streamling_user_err!(
                    "{}: the {} must be a string literal",
                    self.result.name(),
                    argument
                )
                .into()
            })
    }

    /// The positions of the values among a call's `argument_count`
    /// arguments.
    fn values(&self, argument_count: usize) -> Result<Range<usize>> {
        let trailing = self.result.trailing_arguments();
        if argument_count <= LEADING_ARGUMENTS + trailing {
            streamling_user_bail!("{} takes {}", self.result.name(), self.result.arguments());
        }
        Ok(LEADING_ARGUMENTS..argument_count - trailing)
    }
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

impl TableFunction for LookupFunc {
    fn name(&self) -> &str {
        self.result.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Resolves the table, key and column while planning, so a bad call fails
    /// the pipeline before it starts.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let literals = args.scalar_arguments;
        let values = &args.arg_fields[self.values(literals.len())?];
        let table = self.table_named(self.string_literal(literals[0], "table name")?)?;
        let key = self.string_literal(literals[1], "key")?;
        let key_fields = table.lookup_key_fields(key)?;
        if values.len() != key_fields.len() {
            streamling_user_bail!(
                "{}: key '{}' has {} columns, got {} values",
                self.result.name(),
                key,
                key_fields.len(),
                values.len()
            );
        }
        for (value, key_field) in values.iter().zip(key_fields.iter()) {
            // A decimal_arb key holds unscaled bytes: a value of another type
            // or scale casts without error to bytes that never match.
            if !can_cast_types(value.data_type(), key_field.data_type())
                || decimal_arb_scale(value) != decimal_arb_scale(key_field)
            {
                streamling_user_bail!(
                    "{}: cannot compare {} with key '{}' of type {}",
                    self.result.name(),
                    type_name(value),
                    key_field.name(),
                    type_name(key_field)
                );
            }
        }
        match self.result {
            LookupResult::Column => {
                let column = self.string_literal(literals.last().copied().flatten(), "column")?;
                Ok(table.lookup_field(column)?)
            }
            LookupResult::Row => Ok(table.lookup_row_field()),
            LookupResult::Exists => Ok(Arc::new(Field::new(
                self.result.name(),
                DataType::Boolean,
                false,
            ))),
        }
    }

    fn table(&self, literals: &[Option<&ScalarValue>]) -> crate::error::Result<Arc<InMemoryTable>> {
        let name = self.string_literal(literals.first().copied().flatten(), "table name")?;
        Ok(self.table_named(name)?)
    }

    fn invoke(&self, args: ScalarFunctionArgs, mark: Option<Mark>) -> Result<ColumnarValue> {
        let values = self.values(args.args.len())?;
        let ColumnarValue::Scalar(table_name) = &args.args[0] else {
            streamling_user_bail!(
                "{}: the table name must be a string literal",
                self.result.name()
            );
        };
        let table = self.table_named(self.string_literal(Some(table_name), "table name")?)?;
        let keys = args.args[values]
            .iter()
            .map(|value| value.to_array(args.number_rows))
            .collect::<Result<Vec<_>>>()?;
        let result: ArrayRef = match self.result {
            LookupResult::Column => {
                let Some(ColumnarValue::Scalar(column)) = args.args.last() else {
                    streamling_user_bail!(
                        "{}: the column must be a string literal",
                        self.result.name()
                    );
                };
                let column = self.string_literal(Some(column), "column")?;
                table.lookup(&keys, column, mark)?
            }
            LookupResult::Row => Arc::new(table.lookup_rows(&keys, mark)?),
            LookupResult::Exists => Arc::new(table.lookup_exists(&keys, mark)?),
        };
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use crate::data::COLUMN_NAME_OP;
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::session::SessionManager;
    use crate::table::{InMemoryTable, VersionRetention};
    use arrow::array::{Array, AsArray, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Fields, Int64Type, Schema, SchemaRef};
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

    /// A session with table `rates` (key `address`) holding `0xa → 10`,
    /// table `balances` (key `address,token`) holding `(0xa, usdc) → 10` and
    /// `(0xb, usdc) → 20`, and a source `t` with one `address` column.
    fn session(addresses: Vec<Option<&str>>) -> SessionManager {
        let schema = rates_schema(Field::new("amount", DataType::Int64, false));
        let rates = InMemoryTable::try_new(
            "rates",
            &schema,
            &["address".to_string()],
            "app::rates".to_string(),
            crate::topology::DEFAULT_MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
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
                &[],
            )
            .unwrap();
        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        session_manager.tables().register(Arc::new(rates));
        session_manager.tables().register(Arc::new(balances()));

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

    fn balances_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("address", DataType::Utf8, false),
            Field::new("token", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]))
    }

    fn balances() -> InMemoryTable {
        let balances = InMemoryTable::try_new(
            "balances",
            &balances_schema(),
            &["address".to_string(), "token".to_string()],
            "app::balances".to_string(),
            crate::topology::DEFAULT_MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
        )
        .unwrap();
        balances
            .apply(
                &RecordBatch::try_new(
                    balances_schema(),
                    vec![
                        Arc::new(StringArray::from(vec!["0xa", "0xb"])),
                        Arc::new(StringArray::from(vec!["usdc", "usdc"])),
                        Arc::new(Int64Array::from(vec![10, 20])),
                        Arc::new(StringArray::from(vec!["i", "i"])),
                    ],
                )
                .unwrap(),
                &[],
            )
            .unwrap();
        balances
    }

    /// `(address, token)` pairs: two hits, a miss, and a NULL in each key
    /// position.
    const BALANCE_KEYS: &str = "(VALUES ('0xb', 'usdc'), ('0xa', 'usdc'), ('0xa', 'dai'), (NULL, 'usdc'), ('0xa', NULL)) AS v(address, token)";

    #[tokio::test]
    async fn looks_up_by_a_composite_primary_key() {
        let session_manager = session(vec![]);
        let batches = session_manager
            .session_context()
            .sql(&format!(
                "SELECT lookup_eq('balances', 'address, token', address, token, 'amount') FROM {BALANCE_KEYS}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let amounts: Vec<Option<i64>> = batches
            .iter()
            .flat_map(|batch| batch.column(0).as_primitive::<Int64Type>().iter())
            .collect();
        assert_eq!(amounts, [Some(20), Some(10), None, None, None]);
    }

    #[tokio::test]
    async fn looks_up_rows_and_their_existence() {
        let session_manager = session(vec![]);
        let df = session_manager
            .session_context()
            .sql(&format!(
                "SELECT lookup_row('balances', 'address,token', address, token) AS row, \
                 lookup_row('balances', 'address,token', address, token)['amount'] AS amount, \
                 lookup_exists('balances', 'address,token', address, token) AS found \
                 FROM {BALANCE_KEYS}"
            ))
            .await
            .unwrap();
        let schema = df.schema().as_arrow().clone();
        let row = schema.field_with_name("row").unwrap();
        assert!(row.is_nullable());
        assert_eq!(
            row.data_type(),
            &DataType::Struct(Fields::from(balances_schema().fields()[..3].to_vec()))
        );
        assert!(schema.field_with_name("amount").unwrap().is_nullable());
        let found = schema.field_with_name("found").unwrap();
        assert_eq!(found.data_type(), &DataType::Boolean);
        assert!(!found.is_nullable());

        let batches = df.collect().await.unwrap();
        let column = |name: &str| -> Vec<_> {
            batches
                .iter()
                .map(|batch| Arc::clone(batch.column_by_name(name).unwrap()))
                .collect()
        };
        let rows: Vec<Option<(String, i64)>> = column("row")
            .iter()
            .flat_map(|rows| {
                let rows = rows.as_struct();
                let tokens = rows.column_by_name("token").unwrap().as_string::<i32>();
                let amounts = rows
                    .column_by_name("amount")
                    .unwrap()
                    .as_primitive::<Int64Type>();
                (0..rows.len())
                    .map(|row| {
                        rows.is_valid(row)
                            .then(|| (tokens.value(row).to_string(), amounts.value(row)))
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            rows,
            [
                Some(("usdc".to_string(), 20)),
                Some(("usdc".to_string(), 10)),
                None,
                None,
                None
            ]
        );
        let amounts: Vec<Option<i64>> = column("amount")
            .iter()
            .flat_map(|amounts| amounts.as_primitive::<Int64Type>().iter())
            .collect();
        assert_eq!(amounts, [Some(20), Some(10), None, None, None]);
        let found: Vec<Option<bool>> = column("found")
            .iter()
            .flat_map(|found| found.as_boolean().iter())
            .collect();
        assert_eq!(
            found,
            [
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false)
            ]
        );
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
            (
                "lookup_eq('rates', 'address', 'amount')",
                "lookup_eq takes table, key, value_1, ..., value_n, column",
            ),
            (
                "lookup_row('rates', 'address')",
                "lookup_row takes table, key, value_1, ..., value_n",
            ),
            (
                "lookup_exists(address, 'address', address)",
                "lookup_exists: the table name must be a string literal",
            ),
            (
                "lookup_row('rates', 'address', address, 'amount')",
                "key 'address' has 1 columns, got 2 values",
            ),
            (
                "lookup_eq('balances', 'token,address', address, address, 'amount')",
                "lookups must use the primary key 'address,token', got 'token,address'",
            ),
            (
                "lookup_exists('balances', 'address', address)",
                "lookups must use the primary key 'address,token', got 'address'",
            ),
            (
                "lookup_eq('balances', 'address,token', address, 'amount')",
                "key 'address,token' has 2 columns, got 1 values",
            ),
            (
                "lookup_row('balances', 'address,token', address, named_struct('a', 1))",
                "cannot compare Struct(\"a\": Int64) with key 'token' of type Utf8",
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
                None,
                VersionRetention::Latest,
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
                None,
                VersionRetention::Latest,
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
