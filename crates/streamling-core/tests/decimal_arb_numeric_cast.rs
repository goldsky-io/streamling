//! `CAST` / `TRY_CAST` of decimal_arb to integer, float and `DECIMAL` types
//! through Streamling's supported-plan entry point (SQL preprocessor and
//! analyzer included).
//!
//! Arrow has no cast from decimal_arb's `LargeBinary` storage, so
//! `CAST(v AS BIGINT)` failed with "Unsupported CAST from LargeBinary to
//! Int64" — in a projection and in a `WHERE` clause alike — and the only way
//! out was a round trip through text (`TRY_CAST(CAST(v AS TEXT) AS BIGINT)`).
//! A bare `CAST(v AS NUMERIC)` planned as `Decimal128(38, 10)`; over
//! decimal_arb it now keeps the value as it is, and `DECIMAL(p > 76, s)`
//! rounds it like `DECIMAL(p <= 76, s)` — both decided by the operand's
//! type, not its name.
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

const AMOUNTS: [Option<&str>; 4] = [
    Some(WIDE_AMOUNT),
    Some("-0.500000000000000000"),
    None,
    Some("1.000000000000000001"),
];

fn owned(values: &[Option<&str>]) -> Vec<Option<String>> {
    values.iter().map(|v| v.map(str::to_string)).collect()
}

#[tokio::test]
async fn bare_numeric_keeps_the_exact_decimal_arb_value() {
    let (declared, batches) = run(
        "SELECT id, CAST(amount AS NUMERIC) AS amount, TRY_CAST(amount AS DECIMAL) AS a2, \
         amount::numeric AS a3, CAST(AMOUNT AS NUMERIC) AS a4, TRY_CAST(Amount AS NUMERIC) AS a5, \
         CAST(gas AS NUMERIC) AS gas FROM t",
    )
    .await
    .unwrap();
    // Every spelling of the column, and of the cast, is the same value.
    for name in ["amount", "a2", "a3", "a4", "a5"] {
        let field = declared.field_with_name(name).unwrap();
        assert_eq!(
            DecimalArbType::precision_scale_from_field(field),
            Some((100, 18)),
            "{name}: {field:?}"
        );
        assert_eq!(texts(&batches, name), owned(&AMOUNTS), "{name}");
    }
    // The value is unchanged, so is a u256 hint (a ClickHouse sink keeps UInt256).
    let gas = declared.field_with_name("gas").unwrap();
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(gas),
        Some(NativeIntKind::U256)
    );
    assert_eq!(texts(&batches, "gas")[1].as_deref(), Some(U256_MAX));

    // Unaliased, the column is named as any cast of it is (the name cannot
    // depend on the operand's type), declared and executed alike.
    let (declared, batches) = run("SELECT id, CAST(amount AS NUMERIC) FROM t")
        .await
        .unwrap();
    let (other_cast, _) = run("SELECT id, CAST(amount AS BIGINT) FROM t WHERE id = 3")
        .await
        .unwrap();
    let name = other_cast.field(1).name();
    assert!(DecimalArbType::is_decimal_arb_field(
        declared.field_with_name(name).unwrap()
    ));
    assert_eq!(batches[0].schema().field(1).name(), name);
}

#[tokio::test]
async fn bare_numeric_over_other_types_is_unchanged() {
    let (declared, batches) =
        run("SELECT id, CAST(id AS NUMERIC) AS n, CAST(CAST(ts AS BIGINT) AS NUMERIC) AS m FROM t")
            .await
            .unwrap();
    for name in ["n", "m"] {
        assert_eq!(
            declared.field_with_name(name).unwrap().data_type(),
            &DataType::Decimal128(38, 10)
        );
    }
    assert_eq!(texts(&batches, "n")[0].as_deref(), Some("1.0000000000"));
}

/// The cast is decided by the type the operand has where it is used, not by
/// its name: a CTE or derived table that rebinds a decimal_arb column's name
/// to another type gets DataFusion's `Decimal128(38, 10)`, as a column of any
/// other name does.
#[tokio::test]
async fn bare_numeric_over_a_rebound_name_is_a_decimal128_cast() {
    let decimal = DataType::Decimal128(38, 10);
    for (sql, expected) in [
        (
            "WITH c AS (SELECT id, CAST(ts AS VARCHAR) AS ts FROM t) \
             SELECT id, CAST(ts AS NUMERIC) AS v FROM c WHERE id = 1",
            "1700000000.0000000000",
        ),
        (
            "WITH c AS (SELECT id, CAST(ts AS BIGINT) AS ts FROM t) \
             SELECT id, CAST(ts AS NUMERIC) AS v FROM c WHERE id = 1",
            "1700000000.0000000000",
        ),
        (
            "SELECT id, CAST(s.ts AS NUMERIC) AS v \
             FROM (SELECT id, CAST(ts AS BIGINT) AS ts FROM t) s WHERE id = 1",
            "1700000000.0000000000",
        ),
        (
            "WITH c AS (SELECT id, CAST(amount AS DOUBLE) AS amount FROM t) \
             SELECT id, CAST(amount AS NUMERIC) AS v FROM c WHERE id = 2",
            "-0.5000000000",
        ),
    ] {
        let (declared, batches) = run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(
            declared.field_with_name("v").unwrap().data_type(),
            &decimal,
            "{sql}"
        );
        assert_eq!(texts(&batches, "v"), some(&[expected]), "{sql}");
    }

    // In arithmetic, decimal arithmetic: not integer division, and a
    // fractional literal next to it is not quoted to text.
    let sql = "WITH c AS (SELECT id, id AS amount FROM t) \
               SELECT id, CAST(amount AS NUMERIC) / 4 AS q, CAST(amount AS NUMERIC) * 1.5 AS m \
               FROM c WHERE id = 1";
    let (_, batches) = run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let q: f64 = texts(&batches, "q")[0].as_deref().unwrap().parse().unwrap();
    let m: f64 = texts(&batches, "m")[0].as_deref().unwrap().parse().unwrap();
    assert_eq!((q, m), (0.25, 1.5));
    let sql = "WITH c AS (SELECT id, CAST(ts AS VARCHAR) AS ts FROM t) \
               SELECT id, CAST(ts AS NUMERIC) * 2 AS v FROM c WHERE id = 1";
    let v: f64 = column_of(sql, "v").await[0]
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(v, 3_400_000_000.0);

    // The same TRY_CAST as one written over the text inline.
    let rebound = column_of(
        "WITH x AS (SELECT id, CAST(amount AS TEXT) AS amount FROM t) \
         SELECT id, TRY_CAST(amount AS NUMERIC) AS amount FROM x",
        "amount",
    )
    .await;
    let inline = column_of(
        "SELECT id, TRY_CAST(CAST(amount AS TEXT) AS NUMERIC) AS amount FROM t",
        "amount",
    )
    .await;
    assert_eq!(rebound, inline);
    assert_eq!(
        rebound,
        vec![
            None,
            Some("-0.5000000000".into()),
            None,
            Some("1.0000000000".into())
        ]
    );
}

#[tokio::test]
async fn bare_numeric_reaches_through_expressions_and_aliases() {
    let cases = [
        "SELECT id, CAST(NULLIF(amount, 0) AS NUMERIC) AS v FROM t",
        "SELECT id, CAST((amount) AS NUMERIC) AS v FROM t",
        "WITH c AS (SELECT id, amount AS amt FROM t) SELECT id, CAST(amt AS NUMERIC) AS v FROM c",
        "SELECT id, CAST(s.a AS NUMERIC) AS v FROM (SELECT id, amount AS a FROM t) s",
        // A struct field and a list element: no name to recognise.
        "SELECT id, CAST(named_struct('x', amount)['x'] AS NUMERIC) AS v FROM t",
        "SELECT id, CAST(make_array(amount, amount)[1] AS NUMERIC) AS v FROM t",
        "SELECT id, CAST(CASE WHEN id > 0 THEN amount END AS NUMERIC) AS v FROM t",
    ];
    for sql in cases {
        let (declared, batches) = run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(
            DecimalArbType::is_decimal_arb_field(declared.field_with_name("v").unwrap()),
            "{sql}: {declared:?}"
        );
        assert_eq!(texts(&batches, "v"), owned(&AMOUNTS), "{sql}");
    }
    // A function that yields decimal_arb.
    let sql = "SELECT id, CAST(to_decimal_arb_from_int(id, 20, 0) AS NUMERIC) AS v FROM t";
    let (declared, batches) = run(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(declared.field_with_name("v").unwrap()),
        Some((20, 0))
    );
    assert_eq!(texts(&batches, "v"), some(&["1", "2", "3", "4"]));
    // The operand keeps its grouping.
    let v = column_of("SELECT id, CAST(gas - 1 AS NUMERIC) * 2 AS v FROM t", "v").await;
    assert_eq!(v[0].as_deref(), Some("41998"));
    let v = column_of("SELECT id, CAST(amount AS NUMERIC) * 1.5 AS v FROM t", "v").await;
    assert_eq!(v[1].as_deref(), Some("-0.7500000000000000000"));
}

/// A bare `NUMERIC` over decimal_arb is still decimal_arb, so what refuses
/// decimal_arb refuses it too; an explicit precision is a native decimal.
#[tokio::test]
async fn bare_numeric_over_decimal_arb_stays_decimal_arb() {
    for sql in [
        "SELECT id, ROUND(CAST(ts AS NUMERIC), 2) AS v FROM t",
        "SELECT id, CAST(ts AS NUMERIC) * CAST(id AS DOUBLE) AS v FROM t",
    ] {
        assert!(run(sql).await.is_err(), "{sql}");
    }
    let sql =
        "SELECT id, CAST(ts AS NUMERIC(38, 10)) * CAST(id AS DOUBLE) AS v FROM t WHERE id = 1";
    assert_eq!(column_of(sql, "v").await, some(&["1700000000.0"]));
}

/// `DECIMAL(p > 76, s)` over decimal_arb rounds like `DECIMAL(p <= 76, s)`:
/// a dropped digit rounds half away from zero, then the precision is checked.
#[tokio::test]
async fn wide_decimal_targets_round_like_narrow_ones() {
    let (declared, batches) = run(
        "SELECT id, CAST(amount AS DECIMAL(76, 0)) AS n0, CAST(amount AS DECIMAL(77, 0)) AS w0, \
         TRY_CAST(amount AS DECIMAL(76, 2)) AS n2, TRY_CAST(amount AS DECIMAL(80, 2)) AS w2, \
         TRY_CAST(TRY_CAST(amount AS DECIMAL(80, 0)) AS BIGINT) AS b, \
         CAST(amount AS DECIMAL(78, 0)) AS u, CAST(id AS DECIMAL(80, 0)) AS i FROM t",
    )
    .await
    .unwrap();
    let rounded = vec![
        Some("123456789012345678901234567890".into()),
        Some("-1".into()),
        None,
        Some("1".into()),
    ];
    assert_eq!(texts(&batches, "n0"), rounded);
    assert_eq!(texts(&batches, "w0"), rounded);
    let rounded = vec![
        Some("123456789012345678901234567890.12".into()),
        Some("-0.50".into()),
        None,
        Some("1.00".into()),
    ];
    assert_eq!(texts(&batches, "n2"), rounded);
    assert_eq!(texts(&batches, "w2"), rounded);
    assert_eq!(
        texts(&batches, "b"),
        vec![None, Some("-1".into()), None, Some("1".into())]
    );
    let w2 = declared.field_with_name("w2").unwrap();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(w2),
        Some((80, 2))
    );
    // `DECIMAL(77..=78, 0)` keeps the u256 hint.
    let u = declared.field_with_name("u").unwrap();
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(u),
        Some(NativeIntKind::U256)
    );
    // Any other operand goes through its text, as before.
    assert_eq!(texts(&batches, "i"), some(&["1", "2", "3", "4"]));

    // Past the precision: CAST errors naming the value, TRY_CAST is NULL.
    let err = run("SELECT id, CAST(amount AS DECIMAL(80, 60)) AS v FROM t")
        .await
        .expect_err("30 integer digits do not fit DECIMAL(80, 60)");
    assert!(
        err.contains(WIDE_AMOUNT) && err.contains("out of range"),
        "{err}"
    );
    let v = column_of(
        "SELECT id, TRY_CAST(amount AS DECIMAL(80, 60)) AS v FROM t",
        "v",
    )
    .await;
    assert_eq!(v[0], None);
    assert!(v[1].as_deref().unwrap().starts_with("-0.5000"));
}

/// Known limitation, not specific to casts: a `CASE` over decimal_arb only
/// gets its metadata back in the analyzer, and an `unnest` of decimal_arb
/// elements not at all, so read through a CTE or derived table (or unnested)
/// the column is raw `LargeBinary` and does not cast. Comparisons and
/// arithmetic fail on it the same way. A `CASE` written inline casts (see
/// `casts_nested_in_case_coalesce_and_subqueries`).
#[tokio::test]
async fn cast_of_a_derived_case_or_unnest_column_is_not_supported_yet() {
    for sql in [
        "SELECT id FROM (SELECT id, CASE WHEN id > 1 THEN ts ELSE gas END AS x FROM t) s \
         WHERE CAST(x AS BIGINT) > 100000",
        "SELECT id, CAST(unnest(make_array(gas, ts)) AS BIGINT) AS v FROM t",
        "SELECT id, CAST(unnest(make_array(gas, ts)) AS NUMERIC) AS v FROM t",
    ] {
        let err = run(sql).await.expect_err(sql);
        assert!(err.contains("LargeBinary"), "{sql}: {err}");
    }
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
            "SELECT id, CAST(ts AS BIGINT) AS ts, CAST(amount AS NUMERIC) AS amount, \
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

/// The preprocessor writes the operand twice (as the value and inside the
/// cast kept as fallback); an `unnest` operand still expands once.
#[tokio::test]
async fn casts_of_an_unnest_expand_it_once() {
    for (sql, expected) in [
        (
            "SELECT id, CAST(unnest(make_array(id, id + 1)) AS NUMERIC) AS v FROM t WHERE id = 1",
            ["1.0000000000", "2.0000000000"],
        ),
        (
            "SELECT id, CAST(unnest(make_array(id, id + 1)) AS DECIMAL(80, 0)) AS v \
             FROM t WHERE id = 1",
            ["1", "2"],
        ),
    ] {
        let mut v = column_of(sql, "v").await;
        v.sort();
        assert_eq!(v, some(&expected), "{sql}");
    }
}
