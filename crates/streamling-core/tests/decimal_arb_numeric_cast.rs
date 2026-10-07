//! `CAST` / `TRY_CAST` of decimal_arb to integer, float and `DECIMAL` types
//! through Streamling's supported-plan entry point (SQL preprocessor and
//! analyzer included).
//!
//! Arrow has no cast from decimal_arb's `LargeBinary` storage, so
//! `CAST(v AS BIGINT)` failed with "Unsupported CAST from LargeBinary to
//! Int64" — in a projection and in a `WHERE` clause alike — and the only way
//! out was a round trip through text (`TRY_CAST(CAST(v AS TEXT) AS BIGINT)`).
use arrow::array::{Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray};
use arrow::util::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    datasource::{MemTable, ViewTable, provider_as_source},
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, dml::InsertOp},
    prelude::SessionContext,
};
use std::sync::Arc;
use streamling_common::types::decimal_arb::{
    DecimalArbArray, DecimalArbArrayBuilder, DecimalArbType, NativeIntKind,
};
use streamling_core::{
    dynamic_table::DynamicTableRegistry,
    operators::{
        checkpointable::CheckpointableNode,
        wrapping::{WrappingNode, WrappingSourceTableProvider},
    },
    session::SessionManager,
};

const U256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";
const I256_MIN: &str =
    "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
const WIDE_AMOUNT: &str = "123456789012345678901234567890.123456789012345678";

fn column(
    name: &str,
    p: u32,
    s: u32,
    hint: Option<NativeIntKind>,
    values: &[Option<&str>],
) -> (Field, Arc<dyn Array>) {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), name, p, s).unwrap();
    for value in values {
        match value {
            Some(v) => b.append_str(v).unwrap(),
            None => b.append_null(),
        }
    }
    let field = DecimalArbType::field(name, p, s, true).unwrap();
    let field = match hint {
        Some(kind) => DecimalArbType::with_native_int_kind(field, kind).unwrap(),
        None => field,
    };
    (field, Arc::new(b.finish().into_inner().0))
}

/// `t(id, amount decimal_arb(100, 18), ts u256, gas u256, signed i256, _gs_op)`:
///
/// | id | amount               | ts         | gas                 | signed  |
/// |----|----------------------|------------|---------------------|---------|
/// | 1  | WIDE_AMOUNT          | 1700000000 | 21000               | -5      |
/// | 2  | -0.5                 | 1700007201 | U256_MAX            | I256_MIN|
/// | 3  | NULL                 | 1699999715 | NULL                | 0       |
/// | 4  | 1.000000000000000001 | NULL       | 9223372036854775808 | i64::MIN|
fn source_batch() -> RecordBatch {
    let columns = [
        column(
            "amount",
            100,
            18,
            None,
            &[
                Some(WIDE_AMOUNT),
                Some("-0.5"),
                None,
                Some("1.000000000000000001"),
            ],
        ),
        column(
            "ts",
            78,
            0,
            Some(NativeIntKind::U256),
            &[
                Some("1700000000"),
                Some("1700007201"),
                Some("1699999715"),
                None,
            ],
        ),
        column(
            "gas",
            78,
            0,
            Some(NativeIntKind::U256),
            &[
                Some("21000"),
                Some(U256_MAX),
                None,
                Some("9223372036854775808"),
            ],
        ),
        column(
            "signed",
            78,
            0,
            Some(NativeIntKind::I256),
            &[
                Some("-5"),
                Some(I256_MIN),
                Some("0"),
                Some("-9223372036854775808"),
            ],
        ),
    ];
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut arrays: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))];
    for (field, array) in columns {
        fields.push(field);
        arrays.push(array);
    }
    fields.push(Field::new("_gs_op", DataType::Utf8, false));
    arrays.push(Arc::new(StringArray::from(vec!["i"; 4])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn session() -> SessionManager {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    sm.session_context()
        .register_batch("t", source_batch())
        .unwrap();
    sm
}

/// Every value of `name`, rendered (decimal_arb as its decimal text), in row order.
fn texts(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = vec![];
    for batch in batches {
        let schema = batch.schema();
        let field = schema.field_with_name(name).unwrap();
        let array = batch.column_by_name(name).unwrap();
        if DecimalArbType::is_decimal_arb_field(field) {
            let lba = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let arb = DecimalArbArray::try_from_array_and_field(lba.clone(), field).unwrap();
            out.extend(
                (0..arb.len()).map(|i| arb.value(i).unwrap().map(|v| v.to_canonical_string())),
            );
        } else {
            out.extend(
                (0..array.len())
                    .map(|i| (!array.is_null(i)).then(|| array_value_to_string(array, i).unwrap())),
            );
        }
    }
    out
}

/// Plan `sql` like a transform and run it; batches sorted by `id`.
async fn run(sql: &str) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let sm = session();
    let (plan, _) = sm
        .create_supported_logical_plan(sql.to_owned())
        .await
        .map_err(|e| e.to_string())?;
    let declared = plan.schema().inner().clone();
    let df = sm
        .new_df(plan)
        .sort_by(vec![datafusion::prelude::col("id")]);
    let batches = df
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    Ok((declared, batches))
}

async fn column_of(sql: &str, name: &str) -> Vec<Option<String>> {
    let (_, batches) = run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    texts(&batches, name)
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

#[tokio::test]
async fn cast_as_bigint_in_where_filters_numerically() {
    let sql = "SELECT id FROM t \
               WHERE CAST(ts AS BIGINT) >= 1700000000 AND CAST(ts AS BIGINT) <= 1700007200";
    assert_eq!(column_of(sql, "id").await, some(&["1"]));
    let sql = "SELECT id FROM t WHERE CAST(ts AS BIGINT) < 1700007201";
    assert_eq!(column_of(sql, "id").await, some(&["1", "3"]));
    // `x::BIGINT` and TRY_CAST are the same cast.
    let sql = "SELECT id FROM t WHERE ts::BIGINT BETWEEN 1699999715 AND 1700000000";
    assert_eq!(column_of(sql, "id").await, some(&["1", "3"]));
    let sql = "SELECT id FROM t WHERE TRY_CAST(gas AS BIGINT) IS NULL";
    assert_eq!(column_of(sql, "id").await, some(&["2", "3", "4"]));
}

#[tokio::test]
async fn cast_as_bigint_in_a_projection_plans_as_int64() {
    let (declared, batches) = run(
        "SELECT id, CAST(ts AS BIGINT) AS ts, TRY_CAST(gas AS BIGINT) AS gas, \
         CAST(signed AS BIGINT) AS signed FROM t WHERE id <> 2",
    )
    .await
    .unwrap();
    for name in ["ts", "gas", "signed"] {
        assert_eq!(
            declared.field_with_name(name).unwrap().data_type(),
            &DataType::Int64
        );
    }
    assert_eq!(
        texts(&batches, "ts"),
        vec![Some("1700000000".into()), Some("1699999715".into()), None]
    );
    // 9223372036854775808 is i64::MAX + 1: NULL under TRY_CAST.
    assert_eq!(
        texts(&batches, "gas"),
        vec![Some("21000".into()), None, None]
    );
    assert_eq!(
        texts(&batches, "signed"),
        some(&["-5", "0", "-9223372036854775808"])
    );
}

/// The declared field of a cast is the executed one, metadata included: no
/// `Int64` declared with the operand's decimal_arb extension or u256 hint.
#[tokio::test]
async fn declared_cast_fields_match_the_executed_fields() {
    for sql in [
        "SELECT id, CAST(ts AS BIGINT) AS ts, CAST(amount AS DECIMAL(38, 2)) AS d, \
         TRY_CAST(amount AS DOUBLE) AS f, TRY_CAST(gas AS DECIMAL(60, 0)) AS g, \
         TRY_CAST(signed AS INT UNSIGNED) AS u FROM t WHERE id <> 2",
        // Unaliased, and through a UNION ALL and a CTE.
        "SELECT id, CAST(ts AS BIGINT) FROM t WHERE id = 1",
        "SELECT id, CAST(ts AS BIGINT) AS v FROM t WHERE id = 1 \
         UNION ALL SELECT id, CAST(gas AS BIGINT) AS v FROM t WHERE id = 1",
        "WITH c AS (SELECT id, CAST(ts AS BIGINT) AS v FROM t) SELECT id, v FROM c WHERE id = 1",
    ] {
        let (declared, batches) = run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let executed = batches[0].schema();
        for (d, e) in declared.fields().iter().zip(executed.fields()) {
            assert_eq!(d.data_type(), e.data_type(), "{sql}: {}", d.name());
            assert_eq!(d.metadata(), e.metadata(), "{sql}: {}", d.name());
            assert!(
                DecimalArbType::without_decimal_arb_metadata(d.metadata()).is_none(),
                "{sql}: {d:?}"
            );
        }
    }
}

#[tokio::test]
async fn cast_out_of_range_fails_and_names_the_value() {
    for (sql, value, target) in [
        (
            "SELECT id, CAST(gas AS BIGINT) AS g FROM t",
            U256_MAX,
            "Int64",
        ),
        (
            "SELECT id, CAST(signed AS INT) AS s FROM t",
            I256_MIN,
            "Int32",
        ),
        (
            "SELECT id, CAST(amount AS DECIMAL(10, 2)) AS a FROM t",
            WIDE_AMOUNT,
            "Decimal128(10, 2)",
        ),
    ] {
        let err = run(sql).await.expect_err(sql);
        assert!(
            err.contains(value) && err.contains(target) && err.contains("out of range"),
            "{sql}: {err}"
        );
    }
}

#[tokio::test]
async fn text_round_trip_workaround_still_works() {
    let direct = column_of("SELECT id, TRY_CAST(gas AS BIGINT) AS g FROM t", "g").await;
    let via_text = column_of(
        "SELECT id, TRY_CAST(CAST(gas AS TEXT) AS BIGINT) AS g FROM t",
        "g",
    )
    .await;
    assert_eq!(direct, via_text);
    assert_eq!(direct, vec![Some("21000".into()), None, None, None]);
}

#[tokio::test]
async fn decimal_targets_rescale_and_check_precision() {
    let (declared, batches) = run("SELECT id, CAST(amount AS DECIMAL(38, 2)) AS d128, \
         CAST(amount AS DECIMAL(60, 20)) AS d256, TRY_CAST(amount AS DECIMAL(10, 2)) AS small, \
         CAST(amount AS DECIMAL(100, 18)) AS wide FROM t")
    .await
    .unwrap();
    assert_eq!(
        declared.field_with_name("d128").unwrap().data_type(),
        &DataType::Decimal128(38, 2)
    );
    assert_eq!(
        declared.field_with_name("d256").unwrap().data_type(),
        &DataType::Decimal256(60, 20)
    );
    assert_eq!(
        texts(&batches, "d128"),
        vec![
            Some("123456789012345678901234567890.12".into()),
            Some("-0.50".into()),
            None,
            Some("1.00".into()),
        ]
    );
    assert_eq!(
        texts(&batches, "d256"),
        vec![
            Some("123456789012345678901234567890.12345678901234567800".into()),
            Some("-0.50000000000000000000".into()),
            None,
            Some("1.00000000000000000100".into()),
        ]
    );
    assert_eq!(
        texts(&batches, "small"),
        vec![None, Some("-0.50".into()), None, Some("1.00".into())]
    );
    // Beyond 76 digits the result stays decimal_arb, exact.
    assert!(DecimalArbType::is_decimal_arb_field(
        declared.field_with_name("wide").unwrap()
    ));
    assert_eq!(texts(&batches, "wide")[0].as_deref(), Some(WIDE_AMOUNT));
}

#[tokio::test]
async fn float_targets() {
    let v = column_of("SELECT id, CAST(amount AS DOUBLE) AS v FROM t", "v").await;
    assert_eq!(
        v[0].as_deref().map(|s| s.parse::<f64>().unwrap()),
        Some(WIDE_AMOUNT.parse::<f64>().unwrap())
    );
    assert_eq!(v[1].as_deref(), Some("-0.5"));
    let v = column_of("SELECT id, TRY_CAST(signed AS REAL) AS v FROM t", "v").await;
    assert_eq!(v[0].as_deref(), Some("-5.0"));
}

#[tokio::test]
async fn casts_nested_in_case_coalesce_and_subqueries() {
    let sql = "SELECT id, CASE WHEN CAST(ts AS BIGINT) > 1699999900 THEN TRY_CAST(signed AS BIGINT) \
               ELSE COALESCE(TRY_CAST(gas AS INT), -1) END AS v FROM t";
    assert_eq!(
        column_of(sql, "v").await,
        vec![
            Some("-5".into()),
            None,
            Some("-1".into()),
            Some("-1".into())
        ]
    );
    // The operand only becomes decimal_arb once the CASE is stamped.
    let sql = "SELECT id, CAST(CASE WHEN id = 1 THEN gas ELSE ts END AS BIGINT) AS v FROM t";
    assert_eq!(
        column_of(sql, "v").await,
        vec![
            Some("21000".into()),
            Some("1700007201".into()),
            Some("1699999715".into()),
            None
        ]
    );
    let sql = "SELECT id, COALESCE(TRY_CAST(gas AS BIGINT), CAST(ts AS BIGINT), 0) AS v FROM t";
    assert_eq!(
        column_of(sql, "v").await,
        some(&["21000", "1700007201", "1699999715", "0"])
    );
    let sql = "WITH c AS (SELECT id, CAST(ts AS BIGINT) AS ts_i FROM t) \
               SELECT id, ts_i FROM c WHERE ts_i < 1700000500";
    assert_eq!(column_of(sql, "id").await, some(&["1", "3"]));
    let sql = "SELECT id FROM (SELECT id, ts FROM t) s WHERE CAST(s.ts AS BIGINT) >= 1700000000";
    assert_eq!(column_of(sql, "id").await, some(&["1", "2"]));
}

// ---------- declared schema of a chained transform ----------

struct Pipeline {
    sm: SessionManager,
    ctx: SessionContext,
}

impl Pipeline {
    fn new() -> Self {
        let sm = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        let ctx = sm.session_context();
        let batch = source_batch();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
        let provider = WrappingSourceTableProvider::new(
            Arc::new(table),
            format!("numeric_cast_source_{}", uuid::Uuid::new_v4()),
            None,
            None,
        );
        ctx.register_table("t", Arc::new(provider)).unwrap();
        Pipeline { sm, ctx }
    }

    /// Plan `sql` and register it as the transform `name` the way the pipeline
    /// builder does; returns the declared output schema.
    async fn transform(&self, name: &str, sql: &str) -> Arc<Schema> {
        let (sql_plan, _source) = self
            .sm
            .create_supported_logical_plan(sql.into())
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let declared = sql_plan.schema().inner().clone();
        let checkpoint = LogicalPlan::Extension(Extension {
            node: Arc::new(CheckpointableNode::new(sql_plan, 10, name.into())),
        });
        let wrapped = LogicalPlan::Extension(Extension {
            node: Arc::new(WrappingNode::new_with_non_null_cols(
                checkpoint,
                format!("numeric_cast_{name}_{}", uuid::Uuid::new_v4()),
                false,
                vec!["id".into()],
                None,
            )),
        });
        self.ctx
            .register_table(name, Arc::new(ViewTable::new(wrapped, None)))
            .unwrap();
        declared
    }

    /// Drain the transform `name` into a sink table and return the rows.
    async fn drain(&self, name: &str) -> Vec<RecordBatch> {
        let view = self.ctx.table(name).await.unwrap().into_unoptimized_plan();
        let target =
            Arc::new(MemTable::try_new(view.schema().inner().clone(), vec![vec![]]).unwrap());
        let sink = format!("sink_{name}");
        self.ctx.register_table(&sink, target.clone()).unwrap();
        let insert = LogicalPlanBuilder::insert_into(
            view,
            &sink,
            provider_as_source(target),
            InsertOp::Append,
        )
        .unwrap()
        .build()
        .unwrap();
        let df = self.sm.new_df(insert);
        let physical = df.create_physical_plan().await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            datafusion::physical_plan::collect(physical, Arc::new(df.task_ctx())),
        )
        .await
        .expect("bounded plan must drain")
        .unwrap();
        self.ctx
            .table(&sink)
            .await
            .unwrap()
            .sort_by(vec![datafusion::prelude::col("id")])
            .unwrap()
            .collect()
            .await
            .unwrap()
    }
}

#[tokio::test]
async fn next_transform_sees_the_cast_type() {
    let p = Pipeline::new();
    let declared = p
        .transform(
            "up",
            "SELECT id, CAST(ts AS BIGINT) AS ts, amount, \
             TRY_CAST(gas AS DECIMAL(20, 0)) AS gas, CAST(signed AS DOUBLE) AS signed FROM t",
        )
        .await;
    let types: Vec<(&str, DataType)> = ["ts", "gas", "signed"]
        .into_iter()
        .map(|n| (n, declared.field_with_name(n).unwrap().data_type().clone()))
        .collect();
    assert_eq!(
        types,
        [
            ("ts", DataType::Int64),
            ("gas", DataType::Decimal128(20, 0)),
            ("signed", DataType::Float64)
        ]
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(declared.field_with_name("amount").unwrap()),
        Some((100, 18))
    );

    // Downstream, the columns are plain numbers (and amount still decimal_arb).
    let declared = p
        .transform(
            "down",
            "SELECT id, ts + 1 AS next_ts, gas * 2 AS gas2, amount FROM up WHERE ts > 1699999900",
        )
        .await;
    assert_eq!(
        declared.field_with_name("next_ts").unwrap().data_type(),
        &DataType::Int64
    );
    let rows = p.drain("down").await;
    assert_eq!(texts(&rows, "next_ts"), some(&["1700000001", "1700007202"]));
    assert_eq!(texts(&rows, "gas2"), vec![Some("42000".into()), None]);
    assert_eq!(
        texts(&rows, "amount"),
        vec![
            Some(WIDE_AMOUNT.into()),
            Some("-0.500000000000000000".into())
        ]
    );
}
