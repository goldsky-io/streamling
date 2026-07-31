//! Adversarial coverage for DF54 binary-operator type coercion through
//! `DecimalArbExprPlanner` (`streamling-common/src/functions/decimal_arb_coercion.rs`).
//!
//! Focus: the full mixed-operand matrix — decimal_arb against Int8/16/32/64,
//! UInt8/16/32/64, Decimal128(p,s), Decimal256(p,s), Float32/64, Utf8, Boolean
//! and NULL — in BOTH operand orders, plus column-vs-column with mismatched
//! (precision, scale), nested/parenthesised arithmetic, unary minus, and the
//! WHERE / SELECT / HAVING / JOIN-ON / CAST contexts.
//!
//! Every arithmetic assertion checks BOTH the decoded numeric result AND that
//! the output field still carries `streamling.decimal_arb` metadata with the
//! (precision, scale) the E5 widening rules promise — a query that returns the
//! right digits but loses the metadata silently corrupts data at the sink.
//!
//! Expected output (precision, scale), from `output_precision_scale` in
//! `decimal_arb_ops.rs`:
//!   add/sub: s = max(s1,s2)              p = max(p1-s1, p2-s2) + s + 1
//!   mul:     s = min(s1+s2, p)           p = p1 + p2 + 1
//!   div:     s = max(s1, 18)             p = (p1-s1) + s2 + s
//!   mod:     s = max(s1,s2)              p = min(p1-s1, p2-s2) + s
//! Integers coerce to decimal_arb(20, 0) (`INT_COERCE_PRECISION`).

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, i256};
use datafusion::prelude::SessionContext;
use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::session::SessionManager;
use streamling_core::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};

// =====================================================================
// Harness
// =====================================================================

fn manager() -> SessionManager {
    SessionManager::new(8192, 10, DynamicTableRegistry::new()).expect("SessionManager::new")
}

/// Build a decimal_arb column array (LargeBinary storage) for `values`.
fn arb_array(name: &str, p: u32, s: u32, values: &[Option<&str>]) -> ArrayRef {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), name, p, s)
        .unwrap_or_else(|e| panic!("builder for {name}({p},{s}): {e}"));
    for v in values {
        match v {
            Some(x) => b
                .append_value(&DecimalArbValue::from_str(x).unwrap())
                .unwrap_or_else(|e| panic!("append {x} to {name}({p},{s}): {e}")),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish().into_inner().0)
}

/// The main operand-matrix table `t` (3 rows).
///
/// ```text
/// amt  decimal_arb(30,4) = 12.5, 100, -3
/// amt2 decimal_arb(10,2) = 0.25, 4,   2
/// amt0 decimal_arb(20,0) = 7,    5,  -2
/// amtn decimal_arb(30,4) = 12.5, NULL, -3
/// i8/i16/i32/i64/u8/u16/u32/u64   = 1, 2, 3
/// i64n Int64                      = 1, NULL, 3
/// d128 Decimal128(20,4)           = 0.25, 100, 7
/// d256 Decimal256(50,4)           = 0.25, 100, 7
/// f32  Float32 / f64 Float64      = 1.5, 2.5, 3.5
/// s    Utf8                       = "1", "2", "3"
/// bl   Boolean                    = true, false, true
/// lb   LargeBinary (NOT decimal_arb) = 0x00,0x01 / 0x00,0x02 / 0x00,0x03
/// ```
fn matrix_ctx() -> SessionContext {
    let ints: Vec<i64> = vec![1, 2, 3];
    let fields = vec![
        DecimalArbType::field("amt", 30, 4, true).unwrap(),
        DecimalArbType::field("amt2", 10, 2, true).unwrap(),
        DecimalArbType::field("amt0", 20, 0, true).unwrap(),
        DecimalArbType::field("amtn", 30, 4, true).unwrap(),
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("u8", DataType::UInt8, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("u32", DataType::UInt32, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("i64n", DataType::Int64, true),
        Field::new("d128", DataType::Decimal128(20, 4), true),
        Field::new("d256", DataType::Decimal256(50, 4), true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("bl", DataType::Boolean, true),
        Field::new("lb", DataType::LargeBinary, true),
    ];
    let schema = Arc::new(Schema::new(fields));

    let d128 = Decimal128Array::from(vec![Some(2_500_i128), Some(1_000_000), Some(70_000)])
        .with_precision_and_scale(20, 4)
        .unwrap();
    let d256 = Decimal256Array::from(vec![
        Some(i256::from_i128(2_500_i128)),
        Some(i256::from_i128(1_000_000)),
        Some(i256::from_i128(70_000)),
    ])
    .with_precision_and_scale(50, 4)
    .unwrap();

    let columns: Vec<ArrayRef> = vec![
        arb_array("amt", 30, 4, &[Some("12.5"), Some("100"), Some("-3")]),
        arb_array("amt2", 10, 2, &[Some("0.25"), Some("4"), Some("2")]),
        arb_array("amt0", 20, 0, &[Some("7"), Some("5"), Some("-2")]),
        arb_array("amtn", 30, 4, &[Some("12.5"), None, Some("-3")]),
        Arc::new(Int8Array::from(
            ints.iter().map(|v| *v as i8).collect::<Vec<_>>(),
        )),
        Arc::new(Int16Array::from(
            ints.iter().map(|v| *v as i16).collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            ints.iter().map(|v| *v as i32).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(ints.clone())),
        Arc::new(UInt8Array::from(
            ints.iter().map(|v| *v as u8).collect::<Vec<_>>(),
        )),
        Arc::new(UInt16Array::from(
            ints.iter().map(|v| *v as u16).collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            ints.iter().map(|v| *v as u32).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            ints.iter().map(|v| *v as u64).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)])),
        Arc::new(d128),
        Arc::new(d256),
        Arc::new(Float32Array::from(vec![1.5_f32, 2.5, 3.5])),
        Arc::new(Float64Array::from(vec![1.5_f64, 2.5, 3.5])),
        Arc::new(StringArray::from(vec!["1", "2", "3"])),
        Arc::new(BooleanArray::from(vec![true, false, true])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(&[0x00u8, 0x01][..]),
            Some(&[0x00u8, 0x02][..]),
            Some(&[0x00u8, 0x03][..]),
        ])),
    ];

    let batch = RecordBatch::try_new(schema, columns).expect("matrix batch");
    let ctx = manager().session_context();
    ctx.register_batch("t", batch).expect("register t");
    ctx
}

/// A decimal_arb(20,0) table whose canonical bytes happen to be valid UTF-8
/// (`[0x00, n]` for small non-negative n) — see FINDING F-G.
fn utf8_safe_ctx(table: &str) -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 20, 0, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![arb_array("a", 20, 0, &[Some("5"), Some("65"), Some("97")])],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch(table, batch).unwrap();
    ctx
}

/// Plan + execute, panicking with the SQL on failure.
async fn ok_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let df = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("PLANNING failed for `{sql}`: {e}"));
    df.collect()
        .await
        .unwrap_or_else(|e| panic!("EXECUTION failed for `{sql}`: {e}"))
}

/// Expect a planning or execution error; return its message.
async fn err_sql(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(e) => e.to_string(),
        Ok(df) => match df.collect().await {
            Err(e) => e.to_string(),
            Ok(batches) => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                panic!(
                    "expected an ERROR for `{sql}` but it succeeded with {rows} row(s); \
                     a silent result here means a lossy/incorrect coercion happened"
                )
            }
        },
    }
}

fn field_of(batches: &[RecordBatch], name: &str) -> Field {
    batches
        .first()
        .unwrap_or_else(|| panic!("no batches, cannot inspect field `{name}`"))
        .schema()
        .field_with_name(name)
        .unwrap_or_else(|e| panic!("no output column `{name}`: {e}"))
        .clone()
}

fn fmt(values: &[Option<DecimalArbValue>]) -> String {
    values
        .iter()
        .map(|v| match v {
            Some(x) => x.to_canonical_string(),
            None => "NULL".to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn expected(items: &[Option<&str>]) -> Vec<Option<DecimalArbValue>> {
    items
        .iter()
        .map(|o| o.map(|s| DecimalArbValue::from_str(s).unwrap()))
        .collect()
}

/// Decode column `name` as decimal_arb, asserting the field metadata survives.
/// Returns `((precision, scale), values)`.
fn arb_column(
    batches: &[RecordBatch],
    name: &str,
    sql: &str,
) -> ((u32, u32), Vec<Option<DecimalArbValue>>) {
    let field = field_of(batches, name);
    assert!(
        DecimalArbType::is_decimal_arb_field(&field),
        "output column `{name}` of `{sql}` LOST decimal_arb metadata \
         (a sink would treat it as raw BYTEA / hex): {field:?}"
    );
    let (p, s) = DecimalArbType::precision_scale_from_field(&field).unwrap_or_else(|| {
        panic!("output column `{name}` of `{sql}` has unreadable decimal_arb metadata: {field:?}")
    });
    let mut out = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("missing column `{name}`"))
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap_or_else(|| panic!("column `{name}` of `{sql}` is not LargeBinary storage"));
        for i in 0..batch.num_rows() {
            if col.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(
                    DecimalArbValue::from_canonical_bytes_at_scale(col.value(i), s)
                        .unwrap_or_else(|e| panic!("decode row {i} of `{name}` at scale {s}: {e}")),
                ));
            }
        }
    }
    ((p, s), out)
}

fn bool_column(batches: &[RecordBatch], name: &str, sql: &str) -> Vec<Option<bool>> {
    let field = field_of(batches, name);
    assert_eq!(
        field.data_type(),
        &DataType::Boolean,
        "comparison output `{name}` of `{sql}` must be Boolean, got {:?}",
        field.data_type()
    );
    let mut out = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.push(if col.is_null(i) {
                None
            } else {
                Some(col.value(i))
            });
        }
    }
    out
}

/// Run `sql`, assert the aliased column `alias` is decimal_arb with exactly
/// `(p, s)` and decodes to `values`.
async fn check_arb(
    ctx: &SessionContext,
    sql: &str,
    alias: &str,
    ps: (u32, u32),
    values: &[Option<&str>],
) {
    let batches = ok_sql(ctx, sql).await;
    let (got_ps, got) = arb_column(&batches, alias, sql);
    assert_eq!(
        got_ps, ps,
        "wrong output (precision, scale) for `{sql}`: got {got_ps:?}, want {ps:?}"
    );
    let want = expected(values);
    assert_eq!(
        got,
        want,
        "wrong values for `{sql}`: got [{}], want [{}]",
        fmt(&got),
        fmt(&want)
    );
}

/// Run `sql`, assert the aliased Boolean column equals `values`.
async fn check_bool(ctx: &SessionContext, sql: &str, alias: &str, values: &[Option<bool>]) {
    let batches = ok_sql(ctx, sql).await;
    let got = bool_column(&batches, alias, sql);
    assert_eq!(got, values, "wrong comparison result for `{sql}`");
}

// =====================================================================
// A. decimal_arb OP <integer column> — every width, both operand orders
// =====================================================================

macro_rules! arb_plus_int_col {
    ($name:ident, $col:literal) => {
        #[tokio::test]
        async fn $name() {
            let ctx = matrix_ctx();
            let sql = concat!("SELECT amt + ", $col, " AS r FROM t");
            check_arb(
                &ctx,
                sql,
                "r",
                (31, 4),
                &[Some("13.5"), Some("102"), Some("0")],
            )
            .await;
        }
    };
}

arb_plus_int_col!(arb_plus_int8_column_coerces_at_scale_zero, "i8");
arb_plus_int_col!(arb_plus_int16_column_coerces_at_scale_zero, "i16");
arb_plus_int_col!(arb_plus_int32_column_coerces_at_scale_zero, "i32");
arb_plus_int_col!(arb_plus_int64_column_coerces_at_scale_zero, "i64");
arb_plus_int_col!(arb_plus_uint8_column_coerces_at_scale_zero, "u8");
arb_plus_int_col!(arb_plus_uint16_column_coerces_at_scale_zero, "u16");
arb_plus_int_col!(arb_plus_uint32_column_coerces_at_scale_zero, "u32");
arb_plus_int_col!(arb_plus_uint64_column_coerces_at_scale_zero, "u64");

macro_rules! int_plus_arb_col {
    ($name:ident, $col:literal) => {
        #[tokio::test]
        async fn $name() {
            let ctx = matrix_ctx();
            let sql = concat!("SELECT ", $col, " + amt AS r FROM t");
            check_arb(
                &ctx,
                sql,
                "r",
                (31, 4),
                &[Some("13.5"), Some("102"), Some("0")],
            )
            .await;
        }
    };
}

int_plus_arb_col!(int8_column_plus_arb_coerces_left_operand, "i8");
int_plus_arb_col!(int16_column_plus_arb_coerces_left_operand, "i16");
int_plus_arb_col!(int32_column_plus_arb_coerces_left_operand, "i32");
int_plus_arb_col!(int64_column_plus_arb_coerces_left_operand, "i64");
int_plus_arb_col!(uint8_column_plus_arb_coerces_left_operand, "u8");
int_plus_arb_col!(uint16_column_plus_arb_coerces_left_operand, "u16");
int_plus_arb_col!(uint32_column_plus_arb_coerces_left_operand, "u32");
int_plus_arb_col!(uint64_column_plus_arb_coerces_left_operand, "u64");

#[tokio::test]
async fn arb_minus_int64_column_uses_add_sub_widening_rule() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt - i64 AS r FROM t",
        "r",
        (31, 4),
        &[Some("11.5"), Some("98"), Some("-6")],
    )
    .await;
}

#[tokio::test]
async fn int64_column_minus_arb_is_not_commuted() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT i64 - amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("-11.5"), Some("-98"), Some("6")],
    )
    .await;
}

#[tokio::test]
async fn arb_times_int64_column_widens_precision_to_p1_plus_p2_plus_one() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt * i64 AS r FROM t",
        "r",
        (51, 4),
        &[Some("12.5"), Some("200"), Some("-9")],
    )
    .await;
}

#[tokio::test]
async fn int64_column_times_arb_gives_same_shape_as_reverse_order() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT i64 * amt AS r FROM t",
        "r",
        (51, 4),
        &[Some("12.5"), Some("200"), Some("-9")],
    )
    .await;
}

#[tokio::test]
async fn arb_divided_by_int64_column_uses_default_div_scale_18() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt / i64 AS r FROM t",
        "r",
        (44, 18),
        &[Some("12.5"), Some("50"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn int64_column_divided_by_arb_takes_left_scale_into_div_rule() {
    let ctx = matrix_ctx();
    // s_out = max(s_left=0, 18) = 18; p_out = (20-0) + 4 + 18 = 42.
    check_arb(
        &ctx,
        "SELECT i64 / amt AS r FROM t",
        "r",
        (42, 18),
        &[Some("0.08"), Some("0.02"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn arb_modulo_int64_column_narrows_precision_to_min_integer_digits() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt % i64 AS r FROM t",
        "r",
        (24, 4),
        &[Some("0.5"), Some("0"), Some("0")],
    )
    .await;
}

#[tokio::test]
async fn int64_column_modulo_arb_keeps_truncated_division_semantics() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT i64 % amt AS r FROM t",
        "r",
        (24, 4),
        &[Some("1"), Some("2"), Some("0")],
    )
    .await;
}

#[tokio::test]
async fn uint64_column_divided_by_arb_coerces_left_side() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT u64 / amt AS r FROM t",
        "r",
        (42, 18),
        &[Some("0.08"), Some("0.02"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn int8_column_multiplied_by_arb_still_uses_precision_20_for_the_integer() {
    let ctx = matrix_ctx();
    // The planner always coerces integers at precision 20, regardless of width,
    // so an Int8 operand must widen exactly like an Int64 one.
    check_arb(
        &ctx,
        "SELECT i8 * amt AS r FROM t",
        "r",
        (51, 4),
        &[Some("12.5"), Some("200"), Some("-9")],
    )
    .await;
}

// =====================================================================
// B. decimal_arb OP <integer literal>, both orders
// =====================================================================

#[tokio::test]
async fn arb_plus_integer_literal_widens_and_keeps_metadata() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + 10 AS r FROM t",
        "r",
        (31, 4),
        &[Some("22.5"), Some("110"), Some("7")],
    )
    .await;
}

#[tokio::test]
async fn integer_literal_plus_arb_widens_and_keeps_metadata() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT 10 + amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("22.5"), Some("110"), Some("7")],
    )
    .await;
}

#[tokio::test]
async fn arb_minus_integer_literal() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt - 10 AS r FROM t",
        "r",
        (31, 4),
        &[Some("2.5"), Some("90"), Some("-13")],
    )
    .await;
}

#[tokio::test]
async fn integer_literal_minus_arb() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT 10 - amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("-2.5"), Some("-90"), Some("13")],
    )
    .await;
}

#[tokio::test]
async fn arb_times_integer_literal() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt * 2 AS r FROM t",
        "r",
        (51, 4),
        &[Some("25"), Some("200"), Some("-6")],
    )
    .await;
}

#[tokio::test]
async fn integer_literal_times_arb() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT 2 * amt AS r FROM t",
        "r",
        (51, 4),
        &[Some("25"), Some("200"), Some("-6")],
    )
    .await;
}

#[tokio::test]
async fn arb_divided_by_integer_literal() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt / 2 AS r FROM t",
        "r",
        (44, 18),
        &[Some("6.25"), Some("50"), Some("-1.5")],
    )
    .await;
}

#[tokio::test]
async fn integer_literal_divided_by_arb_rounds_half_even_at_scale_18() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT 100 / amt AS r FROM t",
        "r",
        (42, 18),
        &[Some("8"), Some("1"), Some("-33.333333333333333333")],
    )
    .await;
}

#[tokio::test]
async fn arb_modulo_integer_literal() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt % 3 AS r FROM t",
        "r",
        (24, 4),
        &[Some("0.5"), Some("1"), Some("0")],
    )
    .await;
}

#[tokio::test]
async fn integer_literal_modulo_arb() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT 7 % amt AS r FROM t",
        "r",
        (24, 4),
        &[Some("7"), Some("7"), Some("1")],
    )
    .await;
}

#[tokio::test]
async fn negative_integer_literal_operand_is_coerced_not_rejected() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + (-10) AS r FROM t",
        "r",
        (31, 4),
        &[Some("2.5"), Some("90"), Some("-13")],
    )
    .await;
}

#[tokio::test]
async fn zero_literal_addition_is_identity_on_value_but_still_widens_type() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + 0 AS r FROM t",
        "r",
        (31, 4),
        &[Some("12.5"), Some("100"), Some("-3")],
    )
    .await;
}

#[tokio::test]
async fn i64_max_literal_operand_does_not_overflow_the_coercion_precision() {
    let ctx = matrix_ctx();
    // 9223372036854775807 is 19 digits; the coercion precision is 20, so this
    // must round-trip exactly rather than tripping check_fits.
    check_arb(
        &ctx,
        "SELECT amt + 9223372036854775807 AS r FROM t",
        "r",
        (31, 4),
        &[
            Some("9223372036854775819.5"),
            Some("9223372036854775907"),
            Some("9223372036854775804"),
        ],
    )
    .await;
}

#[tokio::test]
async fn division_by_zero_literal_errors_instead_of_returning_a_value() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt / 0 AS r FROM t").await;
    assert!(
        msg.contains("division by zero") || msg.to_lowercase().contains("divide by zero"),
        "decimal_arb / 0 must surface a division-by-zero error, got: {msg}"
    );
}

#[tokio::test]
async fn modulo_by_zero_literal_errors_instead_of_returning_a_value() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt % 0 AS r FROM t").await;
    assert!(
        msg.to_lowercase().contains("zero"),
        "decimal_arb % 0 must surface a modulo-by-zero error, got: {msg}"
    );
}

// =====================================================================
// C. decimal_arb OP Decimal128 / Decimal256
// =====================================================================

#[tokio::test]
async fn arb_plus_decimal128_column_keeps_metadata_and_widens() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + d128 AS r FROM t",
        "r",
        (31, 4),
        &[Some("12.75"), Some("200"), Some("4")],
    )
    .await;
}

#[tokio::test]
async fn decimal128_column_plus_arb_coerces_left_operand() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT d128 + amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("12.75"), Some("200"), Some("4")],
    )
    .await;
}

#[tokio::test]
async fn arb_plus_decimal256_column_takes_the_wider_precision() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + d256 AS r FROM t",
        "r",
        (51, 4),
        &[Some("12.75"), Some("200"), Some("4")],
    )
    .await;
}

#[tokio::test]
async fn decimal256_column_plus_arb_coerces_left_operand() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT d256 + amt AS r FROM t",
        "r",
        (51, 4),
        &[Some("12.75"), Some("200"), Some("4")],
    )
    .await;
}

#[tokio::test]
async fn arb_minus_decimal128_column() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt - d128 AS r FROM t",
        "r",
        (31, 4),
        &[Some("12.25"), Some("0"), Some("-10")],
    )
    .await;
}

#[tokio::test]
async fn arb_times_decimal128_column_sums_scales() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt * d128 AS r FROM t",
        "r",
        (51, 8),
        &[Some("3.125"), Some("10000"), Some("-21")],
    )
    .await;
}

#[tokio::test]
async fn arb_divided_by_decimal128_column() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt / d128 AS r FROM t",
        "r",
        (48, 18),
        &[Some("50"), Some("1"), Some("-0.428571428571428571")],
    )
    .await;
}

#[tokio::test]
async fn arb_modulo_decimal128_column() {
    let ctx = matrix_ctx();
    // -3 % 7 = -3 (truncated division), 12.5 % 0.25 = 0, 100 % 100 = 0.
    check_arb(
        &ctx,
        "SELECT amt % d128 AS r FROM t",
        "r",
        (20, 4),
        &[Some("0"), Some("0"), Some("-3")],
    )
    .await;
}

#[tokio::test]
async fn decimal256_column_divided_by_arb_coerces_left_operand() {
    let ctx = matrix_ctx();
    // d256(50,4) / amt(30,4): s_out = max(4,18) = 18, p_out = 46 + 4 + 18 = 68.
    check_arb(
        &ctx,
        "SELECT d256 / amt AS r FROM t",
        "r",
        (68, 18),
        &[Some("0.02"), Some("1"), Some("-2.333333333333333333")],
    )
    .await;
}

/// FINDING F-A: under DataFusion 54 a bare fractional SQL literal is typed
/// `Float64` (`datafusion.sql_parser.parse_float_as_decimal` defaults to false),
/// NOT `Decimal128`. `DecimalArbExprPlanner::is_coercible` rejects floats, so
/// every `decimal_arb <op> <fractional literal>` expression fails to plan with
/// "Cannot coerce arithmetic expression LargeBinary + Float64".
///
/// The planner's own module docs and its `decimal_arb_plus_float_still_rejected`
/// unit test both assert the opposite ("A bare `1.5` literal is Decimal128 in
/// DataFusion and *would* coerce"), so this is a stale-under-df54 assumption,
/// not a deliberate design choice: `WHERE amount > 0.5` is unwritable.
#[tokio::test]
#[ignore = "FINDING: df54 types bare fractional literals as Float64, so `decimal_arb + 1.5` fails to plan"]
async fn arb_plus_decimal_literal_coerces_via_decimal128_path() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + 1.5 AS r FROM t",
        "r",
        (31, 4),
        &[Some("14"), Some("101.5"), Some("-1.5")],
    )
    .await;
}

#[tokio::test]
#[ignore = "FINDING: df54 types bare fractional literals as Float64, so `1.5 + decimal_arb` fails to plan"]
async fn decimal_literal_plus_arb_coerces_via_decimal128_path() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT 1.5 + amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("14"), Some("101.5"), Some("-1.5")],
    )
    .await;
}

#[tokio::test]
#[ignore = "FINDING: df54 types bare fractional literals as Float64, so `decimal_arb * 0.5` fails to plan"]
async fn arb_times_decimal_literal_sums_scales_of_literal_and_column() {
    let ctx = matrix_ctx();
    // `0.5` would be Decimal128(1, 1) -> decimal_arb(1, 1). mul: p = 30+1+1 = 32, s = 5.
    check_arb(
        &ctx,
        "SELECT amt * 0.5 AS r FROM t",
        "r",
        (32, 5),
        &[Some("6.25"), Some("50"), Some("-1.5")],
    )
    .await;
}

/// Documents the current (broken) shape of FINDING F-A so a fix flips this test
/// from "errors" to "works". Not ignored: it asserts today's behaviour.
#[tokio::test]
async fn fractional_literal_operand_currently_errors_as_a_float() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt + 1.5 AS r FROM t").await;
    assert!(
        msg.contains("Float64"),
        "expected the df54 Float64-literal rejection (FINDING F-A); got: {msg}"
    );
}

#[tokio::test]
async fn casting_a_fractional_literal_to_decimal_is_the_working_workaround() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + CAST(1.5 AS DECIMAL(2, 1)) AS r FROM t",
        "r",
        (31, 4),
        &[Some("14"), Some("101.5"), Some("-1.5")],
    )
    .await;
}

#[tokio::test]
async fn casting_a_fractional_literal_to_decimal_works_for_multiplication_too() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt * CAST(0.5 AS DECIMAL(1, 1)) AS r FROM t",
        "r",
        (32, 5),
        &[Some("6.25"), Some("50"), Some("-1.5")],
    )
    .await;
}

#[tokio::test]
async fn arb_plus_explicit_cast_to_decimal128_literal() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + CAST(2 AS DECIMAL(10, 2)) AS r FROM t",
        "r",
        (31, 4),
        &[Some("14.5"), Some("102"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn arb_plus_cast_of_int_column_to_decimal128() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        // add: s = max(4, 3) = 4, p = max(30-4, 12-3) + 4 + 1 = 31.
        "SELECT amt + CAST(i64 AS DECIMAL(12, 3)) AS r FROM t",
        "r",
        (31, 4),
        &[Some("13.5"), Some("102"), Some("0")],
    )
    .await;
}

// =====================================================================
// D. decimal_arb column-vs-column with mismatched (precision, scale)
// =====================================================================

#[tokio::test]
async fn arb_plus_arb_with_smaller_scale_takes_the_max_scale() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + amt2 AS r FROM t",
        "r",
        (31, 4),
        &[Some("12.75"), Some("104"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn arb_plus_arb_reversed_operand_order_is_shape_identical() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt2 + amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("12.75"), Some("104"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn arb_plus_arb_with_zero_scale_operand() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + amt0 AS r FROM t",
        "r",
        (31, 4),
        &[Some("19.5"), Some("105"), Some("-5")],
    )
    .await;
}

#[tokio::test]
async fn arb_zero_scale_plus_arb_four_scale() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt0 + amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("19.5"), Some("105"), Some("-5")],
    )
    .await;
}

#[tokio::test]
async fn arb_times_arb_with_mismatched_scales_sums_scales() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt2 * amt0 AS r FROM t",
        "r",
        (31, 2),
        &[Some("1.75"), Some("20"), Some("-4")],
    )
    .await;
}

#[tokio::test]
async fn arb_divided_by_arb_with_mismatched_scales() {
    let ctx = matrix_ctx();
    // amt0(20,0) / amt2(10,2): s_out = max(0,18) = 18, p_out = 20 + 2 + 18 = 40.
    check_arb(
        &ctx,
        "SELECT amt0 / amt2 AS r FROM t",
        "r",
        (40, 18),
        &[Some("28"), Some("1.25"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn arb_modulo_arb_with_mismatched_scales() {
    let ctx = matrix_ctx();
    // amt2(10,2) % amt0(20,0): s_out = 2, p_out = min(8, 20) + 2 = 10.
    check_arb(
        &ctx,
        "SELECT amt2 % amt0 AS r FROM t",
        "r",
        (10, 2),
        &[Some("0.25"), Some("4"), Some("0")],
    )
    .await;
}

#[tokio::test]
async fn arb_minus_itself_is_exactly_zero_for_every_row() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt - amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("0"), Some("0"), Some("0")],
    )
    .await;
}

#[tokio::test]
async fn numerically_equal_arb_columns_with_different_scales_compare_equal() {
    // Scale is column metadata, not part of the value: decimal_arb(30,4) 100.0000
    // and decimal_arb(10,2) 100.00 must compare equal even though their canonical
    // bytes differ.
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("x", 30, 4, true).unwrap(),
        DecimalArbType::field("y", 10, 2, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            arb_array("x", 30, 4, &[Some("100"), Some("2.5")]),
            arb_array("y", 10, 2, &[Some("100"), Some("2.5")]),
        ],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("sc", batch).unwrap();
    check_bool(
        &ctx,
        "SELECT x = y AS r FROM sc",
        "r",
        &[Some(true), Some(true)],
    )
    .await;
}

// =====================================================================
// E. Float operands must be rejected, never silently cast
// =====================================================================

macro_rules! float_op_rejected {
    ($name:ident, $sql:literal) => {
        #[tokio::test]
        async fn $name() {
            let ctx = matrix_ctx();
            let msg = err_sql(&ctx, $sql).await;
            assert!(
                !msg.is_empty(),
                "float/decimal_arb mix must produce a non-empty error for `{}`",
                $sql
            );
        }
    };
}

float_op_rejected!(
    arb_plus_float64_column_is_rejected,
    "SELECT amt + f64 AS r FROM t"
);
float_op_rejected!(
    float64_column_plus_arb_is_rejected,
    "SELECT f64 + amt AS r FROM t"
);
float_op_rejected!(
    arb_plus_float32_column_is_rejected,
    "SELECT amt + f32 AS r FROM t"
);
float_op_rejected!(
    float32_column_plus_arb_is_rejected,
    "SELECT f32 + amt AS r FROM t"
);
float_op_rejected!(
    arb_minus_float64_column_is_rejected,
    "SELECT amt - f64 AS r FROM t"
);
float_op_rejected!(
    arb_times_float64_column_is_rejected,
    "SELECT amt * f64 AS r FROM t"
);
float_op_rejected!(
    arb_divided_by_float64_column_is_rejected,
    "SELECT amt / f64 AS r FROM t"
);
float_op_rejected!(
    arb_modulo_float64_column_is_rejected,
    "SELECT amt % f64 AS r FROM t"
);
float_op_rejected!(
    arb_greater_than_float64_column_is_rejected,
    "SELECT amt > f64 AS r FROM t"
);
float_op_rejected!(
    arb_equals_float64_column_is_rejected,
    "SELECT amt = f64 AS r FROM t"
);
float_op_rejected!(
    float64_column_less_than_arb_is_rejected,
    "SELECT f64 < amt AS r FROM t"
);
float_op_rejected!(
    arb_plus_cast_double_literal_is_rejected,
    "SELECT amt + CAST(2 AS DOUBLE) AS r FROM t"
);
float_op_rejected!(
    arb_plus_cast_real_literal_is_rejected,
    "SELECT amt + CAST(2 AS REAL) AS r FROM t"
);
float_op_rejected!(
    arb_filter_against_float_column_is_rejected,
    "SELECT amt AS r FROM t WHERE amt > f64"
);

#[tokio::test]
async fn float_rejection_error_names_the_offending_types() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt + f64 AS r FROM t").await;
    let lower = msg.to_lowercase();
    assert!(
        lower.contains("float") || lower.contains("largebinary") || lower.contains("binary"),
        "the float rejection error should name the operand types so an author can \
         add an explicit cast; got: {msg}"
    );
}

// =====================================================================
// F. Utf8 / Boolean / NULL / plain-binary operands
// =====================================================================

#[tokio::test]
async fn arb_plus_utf8_column_is_rejected() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt + s AS r FROM t").await;
    assert!(!msg.is_empty());
}

#[tokio::test]
async fn utf8_column_plus_arb_is_rejected() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT s + amt AS r FROM t").await;
    assert!(!msg.is_empty());
}

/// FINDING F-C: `decimal_arb <cmp> Utf8` is NOT rejected. `is_coercible`
/// returns false for `Utf8`, so the planner returns `PlannerResult::Original`
/// "so DataFusion's default planner surfaces a clear type-mismatch error"
/// (planner source comment). DataFusion instead coerces `Utf8 -> LargeBinary`
/// and compares the decimal_arb canonical bytes against the raw UTF-8 bytes of
/// the text — silently returning FALSE for every row instead of erroring.
///
/// `WHERE amount = '1000000000000000000'` (the natural spelling for a wei value
/// that arrives as text) therefore silently drops every row rather than telling
/// the author to cast.
#[tokio::test]
#[ignore = "FINDING: decimal_arb = Utf8 silently bytewise-compares (all FALSE) instead of erroring"]
async fn arb_equals_utf8_column_must_not_silently_compare_bytes() {
    let ctx = matrix_ctx();
    match ctx.sql("SELECT amt = s AS r FROM t").await {
        Err(_) => {}
        Ok(df) => match df.collect().await {
            Err(_) => {}
            Ok(batches) => {
                let got = bool_column(&batches, "r", "SELECT amt = s AS r FROM t");
                panic!(
                    "decimal_arb = Utf8 silently produced {got:?} — this is a bytewise \
                     comparison of canonical decimal bytes against UTF-8 text, not a \
                     numeric comparison"
                );
            }
        },
    }
}

#[tokio::test]
#[ignore = "FINDING: decimal_arb = '<text literal>' silently bytewise-compares (all FALSE) instead of erroring"]
async fn arb_equals_string_literal_must_not_silently_compare_bytes() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt = '12.5' AS r FROM t";
    match ctx.sql(sql).await {
        Err(_) => {}
        Ok(df) => match df.collect().await {
            Err(_) => {}
            Ok(batches) => {
                let got = bool_column(&batches, "r", sql);
                panic!(
                    "decimal_arb = '12.5' silently produced {got:?}; a bytewise compare \
                     against the literal's UTF-8 bytes is never a correct numeric answer"
                );
            }
        },
    }
}

/// Documents the current (broken) shape of FINDING F-C; not ignored.
/// A textual predicate that *should* match row 0 silently matches nothing.
#[tokio::test]
async fn text_predicate_over_decimal_arb_currently_drops_every_row_silently() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt AS r FROM t WHERE amt = '12.5'";
    match ctx.sql(sql).await {
        Err(_) => { /* erroring would be the correct behaviour */ }
        Ok(df) => match df.collect().await {
            Err(_) => {}
            Ok(batches) => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(
                    rows, 0,
                    "FINDING F-C describes a silent all-FALSE bytewise compare; if this \
                     query now returns rows the behaviour changed and the finding needs \
                     re-triage"
                );
            }
        },
    }
}

#[tokio::test]
async fn arb_plus_boolean_column_is_rejected() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt + bl AS r FROM t").await;
    assert!(!msg.is_empty());
}

#[tokio::test]
async fn boolean_column_plus_arb_is_rejected() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT bl + amt AS r FROM t").await;
    assert!(!msg.is_empty());
}

#[tokio::test]
async fn arb_equals_boolean_column_is_rejected() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt = bl AS r FROM t").await;
    assert!(!msg.is_empty());
}

#[tokio::test]
async fn arb_greater_than_boolean_literal_is_rejected() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt > true AS r FROM t").await;
    assert!(!msg.is_empty());
}

#[tokio::test]
async fn arb_plus_plain_largebinary_column_is_rejected() {
    // `lb` is LargeBinary WITHOUT decimal_arb metadata; adding it to a
    // decimal_arb column is meaningless and must not be dispatched to
    // decimal_arb_add (which would decode arbitrary bytes as a decimal).
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt + lb AS r FROM t").await;
    assert!(!msg.is_empty());
}

/// Same root cause as FINDING F-C, other non-coercible byte-comparable type:
/// `decimal_arb = <plain LargeBinary>` is silently answered bytewise (all FALSE)
/// instead of being rejected.
#[tokio::test]
#[ignore = "FINDING: decimal_arb = plain LargeBinary silently bytewise-compares (all FALSE) instead of erroring"]
async fn arb_equals_plain_largebinary_column_is_rejected() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt = lb AS r FROM t").await;
    assert!(!msg.is_empty());
}

/// Documents the current (broken) shape; not ignored.
#[tokio::test]
async fn arb_equals_plain_largebinary_currently_answers_all_false() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt = lb AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let got = bool_column(&batches, "r", sql);
    assert_eq!(
        got,
        vec![Some(false), Some(false), Some(false)],
        "FINDING F-C describes a silent bytewise compare; behaviour changed"
    );
}

/// FINDING F-F (low): `decimal_arb + NULL` fails to plan. DataFusion coerces the
/// untyped NULL literal to the decimal_arb storage type and then rejects
/// `LargeBinary + LargeBinary`, whereas SQL three-valued logic (and DataFusion's
/// own `Int64 + NULL`) says the result is NULL.
#[tokio::test]
#[ignore = "FINDING: `decimal_arb + NULL` errors instead of yielding NULL (SQL three-valued logic)"]
async fn arb_plus_null_literal_yields_all_nulls() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt + NULL AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    for batch in &batches {
        let col = batch.column_by_name("r").expect("column r");
        for i in 0..batch.num_rows() {
            assert!(
                col.is_null(i),
                "`{sql}` produced a NON-NULL value at row {i}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "FINDING: `NULL + decimal_arb` errors instead of yielding NULL (SQL three-valued logic)"]
async fn null_literal_plus_arb_yields_all_nulls() {
    let ctx = matrix_ctx();
    let sql = "SELECT NULL + amt AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    for batch in &batches {
        let col = batch.column_by_name("r").expect("column r");
        for i in 0..batch.num_rows() {
            assert!(
                col.is_null(i),
                "`{sql}` produced a non-NULL value at row {i}"
            );
        }
    }
}

/// Documents the current (broken) shape of FINDING F-F; not ignored.
#[tokio::test]
async fn arb_plus_null_literal_currently_fails_to_plan() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt + NULL AS r FROM t").await;
    assert!(
        msg.contains("LargeBinary"),
        "expected the NULL-coerced-to-LargeBinary arithmetic rejection; got: {msg}"
    );
}

/// Comparison against NULL is handled correctly (unlike arithmetic): the
/// predicate is NULL for every row, so no row survives.
#[tokio::test]
async fn arb_compared_to_null_literal_never_matches_a_row() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt AS r FROM t WHERE amt > NULL";
    let batches = ok_sql(&ctx, sql).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 0,
        "`{sql}` must match no rows (comparison to NULL is NULL)"
    );
}

#[tokio::test]
async fn arb_equality_against_null_literal_never_matches_a_row() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt AS r FROM t WHERE amt = NULL";
    let batches = ok_sql(&ctx, sql).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "`{sql}` must match no rows");
}

#[tokio::test]
async fn null_values_in_the_decimal_arb_column_propagate_through_integer_coercion() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amtn + 1 AS r FROM t",
        "r",
        (31, 4),
        &[Some("13.5"), None, Some("-2")],
    )
    .await;
}

#[tokio::test]
async fn null_values_in_the_integer_column_propagate_through_coercion() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + i64n AS r FROM t",
        "r",
        (31, 4),
        &[Some("13.5"), None, Some("0")],
    )
    .await;
}

#[tokio::test]
async fn nulls_on_both_sides_stay_null_and_do_not_become_zero() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amtn * i64n AS r FROM t",
        "r",
        (51, 4),
        &[Some("12.5"), None, Some("-9")],
    )
    .await;
}

#[tokio::test]
async fn comparison_with_null_operand_row_is_null_not_false() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amtn > 0 AS r FROM t",
        "r",
        &[Some(true), None, Some(false)],
    )
    .await;
}

// =====================================================================
// G. All six comparison operators, both operand orders
// =====================================================================

#[tokio::test]
async fn eq_with_integer_literal() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt = 100 AS r FROM t",
        "r",
        &[Some(false), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn neq_with_integer_literal() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt != 100 AS r FROM t",
        "r",
        &[Some(true), Some(false), Some(true)],
    )
    .await;
}

#[tokio::test]
async fn angle_bracket_neq_with_integer_literal() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt <> 100 AS r FROM t",
        "r",
        &[Some(true), Some(false), Some(true)],
    )
    .await;
}

#[tokio::test]
async fn lt_with_integer_literal() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt < 100 AS r FROM t",
        "r",
        &[Some(true), Some(false), Some(true)],
    )
    .await;
}

#[tokio::test]
async fn lte_with_integer_literal() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt <= 100 AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(true)],
    )
    .await;
}

#[tokio::test]
async fn gt_with_integer_literal() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt > 0 AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn gte_with_integer_literal() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt >= 100 AS r FROM t",
        "r",
        &[Some(false), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn literal_on_the_left_reverses_the_comparison_direction() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT 100 > amt AS r FROM t",
        "r",
        &[Some(true), Some(false), Some(true)],
    )
    .await;
}

#[tokio::test]
async fn literal_lte_on_the_left() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT 100 <= amt AS r FROM t",
        "r",
        &[Some(false), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn literal_eq_on_the_left() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT 100 = amt AS r FROM t",
        "r",
        &[Some(false), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn comparison_against_int8_column() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt > i8 AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn comparison_against_uint64_column_in_reverse_order() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT u64 < amt AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn comparison_against_decimal128_column() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt >= d128 AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn comparison_against_decimal256_column_reversed() {
    let ctx = matrix_ctx();
    // d256 = {0.25, 100, 7}, amt = {12.5, 100, -3}.
    check_bool(
        &ctx,
        "SELECT d256 <= amt AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
#[ignore = "FINDING: df54 types bare fractional literals as Float64, so `decimal_arb > 12.4999` fails to plan"]
async fn comparison_against_decimal_literal_with_fraction() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt > 12.4999 AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
#[ignore = "FINDING: df54 types bare fractional literals as Float64, so `WHERE decimal_arb > 0.5` fails to plan"]
async fn where_clause_with_a_fractional_literal_threshold() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt AS r FROM t WHERE amt > 0.5",
        "r",
        (30, 4),
        &[Some("12.5"), Some("100")],
    )
    .await;
}

#[tokio::test]
async fn comparison_against_a_cast_fractional_literal_works() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt > CAST(12.4999 AS DECIMAL(6, 4)) AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn comparison_of_two_arb_columns_with_mismatched_scales() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt > amt2 AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn comparison_result_field_is_nullable_boolean() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt > 0 AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let field = field_of(&batches, "r");
    assert_eq!(field.data_type(), &DataType::Boolean);
    assert!(
        field.is_nullable(),
        "decimal_arb comparison output must stay nullable (NULL operands): {field:?}"
    );
}

// =====================================================================
// H. WHERE / SELECT / HAVING / JOIN-ON / ORDER BY / CAST contexts
// =====================================================================

#[tokio::test]
async fn coerced_comparison_in_where_clause_filters_correctly() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt AS r FROM t WHERE amt > 0",
        "r",
        (30, 4),
        &[Some("12.5"), Some("100")],
    )
    .await;
}

#[tokio::test]
async fn coerced_arithmetic_inside_a_where_predicate() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt AS r FROM t WHERE amt + 1 > 100",
        "r",
        (30, 4),
        &[Some("100")],
    )
    .await;
}

#[tokio::test]
async fn where_predicate_combining_two_coerced_comparisons() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt AS r FROM t WHERE amt > 0 AND amt < 50",
        "r",
        (30, 4),
        &[Some("12.5")],
    )
    .await;
}

#[tokio::test]
async fn where_predicate_with_or_of_coerced_comparisons() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt AS r FROM t WHERE amt < 0 OR amt = 100",
        "r",
        (30, 4),
        &[Some("100"), Some("-3")],
    )
    .await;
}

#[tokio::test]
async fn where_clause_comparing_arb_against_an_integer_column() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt AS r FROM t WHERE amt > i64",
        "r",
        (30, 4),
        &[Some("12.5"), Some("100")],
    )
    .await;
}

#[tokio::test]
async fn where_clause_with_no_matching_rows_does_not_panic_on_empty_batch() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt + 1 AS r FROM t WHERE 1 = 0";
    let batches = ok_sql(&ctx, sql).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "`{sql}` must return zero rows");
}

#[tokio::test]
async fn coerced_comparison_inside_a_case_expression() {
    let ctx = matrix_ctx();
    let sql = "SELECT CASE WHEN amt > 0 THEN 1 ELSE 0 END AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let col = batches[0]
        .column_by_name("r")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("CASE over an integer literal must stay Int64");
    assert_eq!(
        (0..3).map(|i| col.value(i)).collect::<Vec<_>>(),
        vec![1, 1, 0],
        "coerced comparison used as a CASE condition gave the wrong branch"
    );
}

#[tokio::test]
async fn coerced_arithmetic_in_a_case_branch_keeps_metadata() {
    let ctx = matrix_ctx();
    let sql = "SELECT CASE WHEN i64 = 1 THEN amt + 1 ELSE amt + 2 END AS r FROM t";
    check_arb(
        &ctx,
        sql,
        "r",
        (31, 4),
        &[Some("13.5"), Some("102"), Some("-1")],
    )
    .await;
}

/// FINDING F-B: the decimal_arb `sum`/`min`/`max`/`avg` UDAFs
/// (`decimal_arb_aggregates.rs`) do not implement `return_field_from_args`, so
/// their output `Field` is a bare `LargeBinary` with NO
/// `streamling.decimal_arb` metadata. Two consequences:
///
///  1. `DecimalArbExprPlanner::plan_binary_op` resolves `SUM(amt)` to a
///     non-decimal_arb field, so it bails out and DataFusion's `TypeCoercion`
///     fails: "Cannot infer common argument type for comparison operation
///     LargeBinary > Int64". This is exactly the F1 shape, one level up —
///     F1's ExprPlanner fix never reaches aggregate outputs.
///  2. Any query that projects an aggregate of a decimal_arb column emits a
///     metadata-less column (see
///     `sum_over_decimal_arb_output_field_keeps_metadata`), which is the F2
///     shape for aggregates.
#[tokio::test]
#[ignore = "FINDING: HAVING SUM(decimal_arb) > <int> fails to plan — aggregate output field carries no decimal_arb metadata"]
async fn coerced_comparison_in_having_over_decimal_arb_sum() {
    let ctx = matrix_ctx();
    let sql = "SELECT i64 AS g, SUM(amt) AS s FROM t GROUP BY i64 HAVING SUM(amt) > 50";
    let batches = ok_sql(&ctx, sql).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "HAVING SUM(decimal_arb) > 50 should keep exactly the group whose sum is 100"
    );
    let (_, values) = arb_column(&batches, "s", sql);
    assert_eq!(
        values,
        expected(&[Some("100")]),
        "HAVING kept the wrong group: [{}]",
        fmt(&values)
    );
}

#[tokio::test]
#[ignore = "FINDING: HAVING MAX(decimal_arb) >= <int> fails to plan — aggregate output field carries no decimal_arb metadata"]
async fn coerced_comparison_in_having_over_decimal_arb_max() {
    let ctx = matrix_ctx();
    let sql = "SELECT i64 AS g, MAX(amt) AS m FROM t GROUP BY i64 HAVING MAX(amt) >= 12";
    let batches = ok_sql(&ctx, sql).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 2,
        "HAVING MAX(amt) >= 12 must keep the 12.5 and 100 groups"
    );
}

#[tokio::test]
#[ignore = "FINDING: HAVING SUM(decimal_arb) > <int> fails to plan — aggregate output field carries no decimal_arb metadata"]
async fn having_with_coerced_comparison_that_excludes_everything() {
    let ctx = matrix_ctx();
    let sql = "SELECT i64 AS g, SUM(amt) AS s FROM t GROUP BY i64 HAVING SUM(amt) > 1000";
    let batches = ok_sql(&ctx, sql).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "no group sums above 1000");
}

/// Documents the current (broken) shape of FINDING F-B; not ignored.
#[tokio::test]
async fn having_over_a_decimal_arb_aggregate_currently_fails_type_coercion() {
    let ctx = matrix_ctx();
    let msg = err_sql(
        &ctx,
        "SELECT i64 AS g, SUM(amt) AS s FROM t GROUP BY i64 HAVING SUM(amt) > 50",
    )
    .await;
    assert!(
        msg.contains("LargeBinary") && msg.contains("Int64"),
        "expected the aggregate-output coercion failure (FINDING F-B); got: {msg}"
    );
}

#[tokio::test]
#[ignore = "FINDING: SUM(decimal_arb) output field drops streamling.decimal_arb metadata (F2 shape, aggregates)"]
async fn sum_over_decimal_arb_output_field_keeps_metadata() {
    let ctx = matrix_ctx();
    let sql = "SELECT SUM(amt) AS s FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let field = field_of(&batches, "s");
    assert!(
        DecimalArbType::is_decimal_arb_field(&field),
        "SUM(decimal_arb) lost the extension metadata — a Postgres sink would write \
         BYTEA and a JSON sink would render hex: {field:?}"
    );
}

#[tokio::test]
#[ignore = "FINDING: MIN(decimal_arb) output field drops streamling.decimal_arb metadata (F2 shape, aggregates)"]
async fn min_over_decimal_arb_output_field_keeps_metadata() {
    let ctx = matrix_ctx();
    let sql = "SELECT MIN(amt) AS m FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let field = field_of(&batches, "m");
    assert!(
        DecimalArbType::is_decimal_arb_field(&field),
        "MIN(decimal_arb) lost the extension metadata: {field:?}"
    );
}

#[tokio::test]
#[ignore = "FINDING: MAX(decimal_arb) output field drops streamling.decimal_arb metadata (F2 shape, aggregates)"]
async fn max_over_decimal_arb_output_field_keeps_metadata() {
    let ctx = matrix_ctx();
    let sql = "SELECT MAX(amt) AS m FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let field = field_of(&batches, "m");
    assert!(
        DecimalArbType::is_decimal_arb_field(&field),
        "MAX(decimal_arb) lost the extension metadata: {field:?}"
    );
}

/// Even though the metadata is lost, the aggregated *bytes* must still decode to
/// the right number at the input column's scale — otherwise the defect is a
/// value bug on top of a metadata bug.
#[tokio::test]
async fn sum_over_decimal_arb_still_produces_the_right_number() {
    let ctx = matrix_ctx();
    let sql = "SELECT SUM(amt) AS s FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let col = batches[0]
        .column_by_name("s")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("decimal_arb sum is LargeBinary storage");
    let v = DecimalArbValue::from_canonical_bytes_at_scale(col.value(0), 4).unwrap();
    assert_eq!(
        v,
        DecimalArbValue::from_str("109.5").unwrap(),
        "SUM(amt) decoded at the input scale is wrong: {}",
        v.to_canonical_string()
    );
}

#[tokio::test]
async fn coerced_equality_in_a_join_on_clause_between_two_arb_columns() {
    let ctx = matrix_ctx();
    let sql = "SELECT l.amt AS r FROM t AS l JOIN t AS rt ON l.amt = rt.amt WHERE l.amt > 0";
    let batches = ok_sql(&ctx, sql).await;
    let (_, values) = arb_column(&batches, "r", sql);
    let mut got: Vec<String> = values
        .iter()
        .map(|v| v.as_ref().unwrap().to_canonical_string())
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec!["100.0000".to_string(), "12.5000".to_string()],
        "self-join on decimal_arb equality produced the wrong rows"
    );
}

#[tokio::test]
async fn coerced_equality_in_a_join_on_clause_against_an_integer_column() {
    let ctx = matrix_ctx();
    // t.amt0 = {7, 5, -2}; t.i64 = {1, 2, 3}. No row pairs up, so the join is empty.
    let sql = "SELECT l.amt0 AS r FROM t AS l JOIN t AS rt ON l.amt0 = rt.i64";
    let batches = ok_sql(&ctx, sql).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "no amt0 value equals any i64 value");
}

#[tokio::test]
async fn join_on_arb_equals_integer_column_that_does_match() {
    let schema_l = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 20, 0, true).unwrap(),
    ]));
    let batch_l = RecordBatch::try_new(
        schema_l,
        vec![arb_array("a", 20, 0, &[Some("1"), Some("2"), Some("9")])],
    )
    .unwrap();
    let schema_r = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let batch_r =
        RecordBatch::try_new(schema_r, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();

    let ctx = manager().session_context();
    ctx.register_batch("jl", batch_l).unwrap();
    ctx.register_batch("jr", batch_r).unwrap();

    let sql = "SELECT jl.a AS r FROM jl JOIN jr ON jl.a = jr.k";
    let batches = ok_sql(&ctx, sql).await;
    let (_, values) = arb_column(&batches, "r", sql);
    let mut got: Vec<String> = values
        .iter()
        .map(|v| v.as_ref().unwrap().to_canonical_string())
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec!["1".to_string(), "2".to_string()],
        "JOIN ON decimal_arb = Int64 matched the wrong rows: [{}]",
        fmt(&values)
    );
}

#[tokio::test]
async fn join_on_coerced_inequality_produces_the_expected_pair_count() {
    let ctx = matrix_ctx();
    // amt = {12.5, 100, -3}; amt0 = {7, 5, -2}. Pairs with l.amt > r.amt0:
    // 12.5 > 7, 12.5 > 5, 12.5 > -2, 100 > 7, 100 > 5, 100 > -2 => 6, plus
    // -3 > -2 is false. Total 6.
    let sql = "SELECT COUNT(*) AS n FROM t AS l JOIN t AS rt ON l.amt > rt.amt0";
    let batches = ok_sql(&ctx, sql).await;
    let n = batches[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 6, "non-equi JOIN over coerced decimal_arb comparison");
}

#[tokio::test]
async fn coerced_arithmetic_survives_a_subquery_projection() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT r FROM (SELECT amt + 1 AS r FROM t)",
        "r",
        (31, 4),
        &[Some("13.5"), Some("101"), Some("-2")],
    )
    .await;
}

#[tokio::test]
async fn coerced_comparison_in_a_subquery_where_clause() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT r FROM (SELECT amt AS r FROM t WHERE amt > 0)",
        "r",
        (30, 4),
        &[Some("12.5"), Some("100")],
    )
    .await;
}

#[tokio::test]
async fn coerced_arithmetic_inside_an_order_by_expression() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt AS r FROM t ORDER BY amt + 1";
    check_arb(
        &ctx,
        sql,
        "r",
        (30, 4),
        &[Some("-3"), Some("12.5"), Some("100")],
    )
    .await;
}

#[tokio::test]
async fn coerced_arithmetic_projected_and_ordered_keeps_metadata() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt * 2 AS r FROM t ORDER BY r",
        "r",
        (51, 4),
        &[Some("-6"), Some("25"), Some("200")],
    )
    .await;
}

#[tokio::test]
async fn coerced_arithmetic_as_a_group_by_key_keeps_metadata() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt + 1 AS r, COUNT(*) AS n FROM t GROUP BY amt + 1 ORDER BY r";
    check_arb(
        &ctx,
        sql,
        "r",
        (31, 4),
        &[Some("-2"), Some("13.5"), Some("101")],
    )
    .await;
}

#[tokio::test]
#[ignore = "FINDING: SUM over a coerced decimal_arb expression drops streamling.decimal_arb metadata (F2 shape, aggregates)"]
async fn coerced_arithmetic_inside_an_aggregate_argument() {
    let ctx = matrix_ctx();
    let sql = "SELECT SUM(amt + 1) AS s FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let (_, values) = arb_column(&batches, "s", sql);
    assert_eq!(
        values,
        expected(&[Some("112.5")]),
        "SUM over a coerced expression: [{}]",
        fmt(&values)
    );
}

/// The coercion itself is fine — only the output field metadata is lost — so the
/// aggregated bytes must still decode to the right value.
#[tokio::test]
async fn coerced_arithmetic_inside_an_aggregate_argument_produces_the_right_number() {
    let ctx = matrix_ctx();
    let sql = "SELECT SUM(amt + 1) AS s FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let col = batches[0]
        .column_by_name("s")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("decimal_arb sum is LargeBinary storage");
    let v = DecimalArbValue::from_canonical_bytes_at_scale(col.value(0), 4).unwrap();
    assert_eq!(
        v,
        DecimalArbValue::from_str("112.5").unwrap(),
        "SUM(amt + 1) is numerically wrong: {}",
        v.to_canonical_string()
    );
}

#[tokio::test]
async fn coerced_arithmetic_under_select_distinct_keeps_metadata() {
    let ctx = matrix_ctx();
    let sql = "SELECT DISTINCT amt * 0 AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let (_, values) = arb_column(&batches, "r", sql);
    assert_eq!(
        values,
        expected(&[Some("0")]),
        "DISTINCT over `amt * 0` should collapse to a single zero: [{}]",
        fmt(&values)
    );
}

#[tokio::test]
async fn coerced_arithmetic_through_union_all_keeps_metadata() {
    let ctx = matrix_ctx();
    let sql = "SELECT amt + 1 AS r FROM t UNION ALL SELECT amt + 1 AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let field = field_of(&batches, "r");
    assert!(
        DecimalArbType::is_decimal_arb_field(&field),
        "UNION ALL of two identical decimal_arb expressions dropped the extension \
         metadata (sink would see raw BYTEA): {field:?}"
    );
}

#[tokio::test]
async fn coerced_arithmetic_with_limit_keeps_metadata_and_values() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + 1 AS r FROM t ORDER BY amt LIMIT 2",
        "r",
        (31, 4),
        &[Some("-2"), Some("13.5")],
    )
    .await;
}

/// `CAST(<decimal_arb expr> AS DECIMAL(p, s))` is not wired to the existing
/// `decimal_arb_to_decimal128` UDF; Arrow has no LargeBinary -> Decimal128
/// kernel, so it fails at execution. That is an honest error (no corruption),
/// but it means the narrowing cast is unreachable from SQL.
#[tokio::test]
async fn cast_of_a_coerced_expression_to_decimal128_fails_loudly_not_silently() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT CAST(amt + 1 AS DECIMAL(20, 4)) AS r FROM t").await;
    assert!(
        msg.contains("Unsupported CAST") || msg.contains("LargeBinary"),
        "CAST(decimal_arb AS DECIMAL) must fail with a type-specific message, \
         never return a reinterpreted value; got: {msg}"
    );
}

/// FINDING F-G: `CAST(<decimal_arb> AS VARCHAR)` reinterprets the canonical
/// decimal bytes as UTF-8 instead of routing to the existing
/// `decimal_arb_to_string` UDF. For most values Arrow rejects the bytes
/// ("Encountered non UTF-8 data"), but the canonical encoding of a small
/// non-negative integer is `[0x00, <byte>]`, which IS valid UTF-8 — so the cast
/// SILENTLY yields control characters instead of the decimal text.
/// `5` becomes "\u{0}\u{5}", `65` becomes "\u{0}A".
#[tokio::test]
#[ignore = "FINDING: CAST(decimal_arb AS VARCHAR) reinterprets canonical bytes as UTF-8 and silently yields mojibake for small values"]
async fn cast_decimal_arb_to_varchar_returns_the_decimal_text() {
    let ctx = utf8_safe_ctx("cs");
    let sql = "SELECT CAST(a AS VARCHAR) AS r FROM cs";
    let batches = ok_sql(&ctx, sql).await;
    let col = batches[0]
        .column_by_name("r")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("CAST AS VARCHAR must produce a string array");
    assert_eq!(
        (0..3).map(|i| col.value(i)).collect::<Vec<_>>(),
        vec!["5", "65", "97"],
        "CAST(decimal_arb AS VARCHAR) must produce the canonical decimal text"
    );
}

/// Documents the current (silent) shape of FINDING F-G; not ignored.
#[tokio::test]
async fn cast_decimal_arb_to_varchar_currently_leaks_raw_canonical_bytes() {
    let ctx = utf8_safe_ctx("cs2");
    let sql = "SELECT CAST(a AS VARCHAR) AS r FROM cs2";
    match ctx.sql(sql).await {
        Err(_) => {}
        Ok(df) => match df.collect().await {
            Err(_) => {}
            Ok(batches) => {
                let col = batches[0]
                    .column_by_name("r")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("CAST AS VARCHAR produces LargeUtf8 here");
                let got: Vec<&str> = (0..3).map(|i| col.value(i)).collect();
                assert_eq!(
                    got,
                    vec!["\u{0}\u{5}", "\u{0}A", "\u{0}a"],
                    "FINDING F-G describes a silent byte reinterpretation; behaviour changed"
                );
            }
        },
    }
}

/// FINDING F-H (low): casting a decimal_arb column away from its LargeBinary
/// storage keeps the `ARROW:extension:name = streamling.decimal_arb` metadata on
/// the *cast output* field. `is_decimal_arb_field` also checks the storage type
/// so it is not fooled, but the metadata-only helpers
/// (`is_decimal_arb_metadata` / `native_int_kind_from_field_metadata`, which
/// sinks call precisely for fields whose DataType has been transformed away
/// from LargeBinary) will claim a plain string column is decimal_arb.
#[tokio::test]
#[ignore = "FINDING: CAST away from decimal_arb keeps streamling.decimal_arb extension metadata on the output field"]
async fn cast_away_from_decimal_arb_drops_the_extension_metadata() {
    let ctx = utf8_safe_ctx("cm1");
    let sql = "SELECT CAST(a AS VARCHAR) AS r FROM cm1";
    let batches = ok_sql(&ctx, sql).await;
    let field = field_of(&batches, "r");
    assert!(
        !DecimalArbType::is_decimal_arb_metadata(field.metadata()),
        "a non-LargeBinary cast output still advertises streamling.decimal_arb: {field:?}"
    );
}

/// Documents the current shape of FINDING F-H; not ignored.
#[tokio::test]
async fn cast_away_from_decimal_arb_currently_keeps_the_extension_metadata() {
    let ctx = utf8_safe_ctx("cm2");
    let sql = "SELECT CAST(a AS VARCHAR) AS r FROM cm2";
    let batches = ok_sql(&ctx, sql).await;
    let field = field_of(&batches, "r");
    assert_ne!(
        field.data_type(),
        &DataType::LargeBinary,
        "the cast should have changed the storage type"
    );
    assert!(
        !DecimalArbType::is_decimal_arb_field(&field),
        "is_decimal_arb_field must still reject the cast output because the storage \
         type is no longer LargeBinary: {field:?}"
    );
}

/// The safe spelling: the dedicated UDF returns the canonical decimal text.
#[tokio::test]
async fn decimal_arb_to_string_udf_returns_the_decimal_text() {
    let ctx = matrix_ctx();
    let sql = "SELECT decimal_arb_to_string(amt) AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let col = batches[0]
        .column_by_name("r")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("decimal_arb_to_string must produce Utf8");
    assert_eq!(
        (0..3).map(|i| col.value(i)).collect::<Vec<_>>(),
        vec!["12.5000", "100.0000", "-3.0000"]
    );
}

#[tokio::test]
async fn cast_of_the_integer_operand_still_routes_through_the_planner() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + CAST(i32 AS BIGINT) AS r FROM t",
        "r",
        (31, 4),
        &[Some("13.5"), Some("102"), Some("0")],
    )
    .await;
}

#[tokio::test]
async fn cast_to_tinyint_operand_coerces_at_scale_zero() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + CAST(2 AS TINYINT) AS r FROM t",
        "r",
        (31, 4),
        &[Some("14.5"), Some("102"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn cast_to_smallint_operand_coerces_at_scale_zero() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + CAST(2 AS SMALLINT) AS r FROM t",
        "r",
        (31, 4),
        &[Some("14.5"), Some("102"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn cast_to_int_operand_coerces_at_scale_zero() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + CAST(2 AS INT) AS r FROM t",
        "r",
        (31, 4),
        &[Some("14.5"), Some("102"), Some("-1")],
    )
    .await;
}

#[tokio::test]
async fn cast_to_unsigned_operand_coerces_at_scale_zero() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + CAST(2 AS BIGINT UNSIGNED) AS r FROM t",
        "r",
        (31, 4),
        &[Some("14.5"), Some("102"), Some("-1")],
    )
    .await;
}

// =====================================================================
// I. Nested / parenthesised arithmetic and unary minus
// =====================================================================

#[tokio::test]
async fn nested_arithmetic_respects_operator_precedence() {
    let ctx = matrix_ctx();
    // amt + amt2 * amt0 -> mul first: amt2*amt0 = (31,2) values {1.75, 20, -4};
    // then add with amt(30,4): s=4, p = max(26, 29) + 4 + 1 = 34.
    check_arb(
        &ctx,
        "SELECT amt + amt2 * amt0 AS r FROM t",
        "r",
        (34, 4),
        &[Some("14.25"), Some("120"), Some("-7")],
    )
    .await;
}

#[tokio::test]
async fn parentheses_change_the_widening_shape_and_the_value() {
    let ctx = matrix_ctx();
    // (amt + amt2) = (31,4); then * amt0(20,0): p = 31+20+1 = 52, s = 4.
    check_arb(
        &ctx,
        "SELECT (amt + amt2) * amt0 AS r FROM t",
        "r",
        (52, 4),
        &[Some("89.25"), Some("520"), Some("2")],
    )
    .await;
}

#[tokio::test]
async fn nested_arithmetic_mixing_integer_literals_and_columns() {
    let ctx = matrix_ctx();
    // i64 * 2 stays Int64 (planner untouched), then amt + Int64 -> (31,4).
    check_arb(
        &ctx,
        "SELECT amt + i64 * 2 AS r FROM t",
        "r",
        (31, 4),
        &[Some("14.5"), Some("104"), Some("3")],
    )
    .await;
}

#[tokio::test]
async fn parenthesised_chain_with_coercion_on_both_sides() {
    let ctx = matrix_ctx();
    // (amt + 1) = (31,4); (amt2 - 2) = (11,2) [max(8,20)+2+1 = 23? no:
    // amt2(10,2) - int(20,0): s=2, p = max(8,20)+2+1 = 23]. mul: p = 31+23+1 = 55, s = 6.
    check_arb(
        &ctx,
        "SELECT (amt + 1) * (amt2 - 2) AS r FROM t",
        "r",
        (55, 6),
        &[Some("-23.625"), Some("202"), Some("0")],
    )
    .await;
}

#[tokio::test]
async fn left_associative_chain_of_three_coercions() {
    let ctx = matrix_ctx();
    // ((amt + 1) + 2) + 3 : each step add(prev, int20/0)
    // step1 (31,4); step2 max(27,20)+4+1 = 32; step3 max(28,20)+4+1 = 33.
    check_arb(
        &ctx,
        "SELECT amt + 1 + 2 + 3 AS r FROM t",
        "r",
        (33, 4),
        &[Some("18.5"), Some("106"), Some("3")],
    )
    .await;
}

#[tokio::test]
async fn right_nested_parenthesised_chain() {
    let ctx = matrix_ctx();
    // amt0 + 1 -> (22,0); amt2 + that -> max(8, 22) + 2 + 1 = 25, s=2;
    // amt + that -> max(26, 23) + 4 + 1 = 31, s=4.
    check_arb(
        &ctx,
        // row 2: amt0+1 = -1, amt2 + (-1) = 1, amt + 1 = -2.
        "SELECT amt + (amt2 + (amt0 + 1)) AS r FROM t",
        "r",
        (31, 4),
        &[Some("20.75"), Some("110"), Some("-2")],
    )
    .await;
}

#[tokio::test]
async fn mixed_width_integer_chain_all_coerce_through_the_same_path() {
    let ctx = matrix_ctx();
    // amt + i8 + i16 + i32 + u8 = amt + 4*[1,2,3]
    // widening: (31,4) -> (32,4) -> (33,4) -> (34,4).
    check_arb(
        &ctx,
        "SELECT amt + i8 + i16 + i32 + u8 AS r FROM t",
        "r",
        (34, 4),
        &[Some("16.5"), Some("108"), Some("9")],
    )
    .await;
}

/// FINDING F-E: SQL unary minus over a decimal_arb column fails to plan —
/// "Negation only supports numeric, interval and timestamp types" from
/// `TypeCoercion`. `DecimalArbNegFunc` (`decimal_arb_neg`) exists and is
/// registered, but nothing rewrites `Expr::Negative` to call it: `ExprPlanner`
/// only hooks binary ops and `DecimalArbExprRewrite` only handles
/// Between/InList/Case/coalesce. Authors must spell it `0 - amount`.
#[tokio::test]
#[ignore = "FINDING: unary minus (`SELECT -decimal_arb_col`) fails to plan; decimal_arb_neg is never wired to Expr::Negative"]
async fn unary_minus_on_a_decimal_arb_column() {
    let ctx = matrix_ctx();
    let sql = "SELECT -amt AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let (_, values) = arb_column(&batches, "r", sql);
    assert_eq!(
        values,
        expected(&[Some("-12.5"), Some("-100"), Some("3")]),
        "unary minus produced the wrong values: [{}]",
        fmt(&values)
    );
}

#[tokio::test]
#[ignore = "FINDING: unary minus inside a coerced binary expression also fails to plan"]
async fn unary_minus_inside_a_coerced_binary_expression() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + -amt2 AS r FROM t",
        "r",
        (31, 4),
        &[Some("12.25"), Some("96"), Some("-5")],
    )
    .await;
}

/// Documents the current (broken) shape of FINDING F-E; not ignored.
#[tokio::test]
async fn unary_minus_over_decimal_arb_currently_fails_type_coercion() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT -amt AS r FROM t").await;
    assert!(
        msg.contains("Negation"),
        "expected the negation type-coercion failure (FINDING F-E); got: {msg}"
    );
}

#[tokio::test]
async fn zero_minus_column_is_the_supported_negation_spelling() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT 0 - amt AS r FROM t",
        "r",
        (31, 4),
        &[Some("-12.5"), Some("-100"), Some("3")],
    )
    .await;
}

#[tokio::test]
async fn multiply_by_negative_one_literal_negates() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt * -1 AS r FROM t",
        "r",
        (51, 4),
        &[Some("-12.5"), Some("-100"), Some("3")],
    )
    .await;
}

#[tokio::test]
async fn subtracting_a_negative_literal_adds() {
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt - -1 AS r FROM t",
        "r",
        (31, 4),
        &[Some("13.5"), Some("101"), Some("-2")],
    )
    .await;
}

#[tokio::test]
async fn nested_expression_feeding_a_comparison() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt + amt2 > 100 AS r FROM t",
        "r",
        &[Some(false), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn comparison_of_two_coerced_expressions() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT amt + 1 > amt2 * 2 AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn deeply_nested_division_and_multiplication_stay_exact() {
    let ctx = matrix_ctx();
    // (amt * 4) / 2 : mul -> (51,4); div by int(20,0) -> s = max(4,18)=18,
    // p = (51-4) + 0 + 18 = 65.
    check_arb(
        &ctx,
        "SELECT (amt * 4) / 2 AS r FROM t",
        "r",
        (65, 18),
        &[Some("25"), Some("200"), Some("-6")],
    )
    .await;
}

// =====================================================================
// J. Operators the planner deliberately does NOT override
// =====================================================================

#[tokio::test]
async fn string_concat_operator_on_decimal_arb_is_not_hijacked() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt || 'x' AS r FROM t").await;
    assert!(
        !msg.is_empty(),
        "`decimal_arb || text` must not silently succeed by concatenating raw bytes"
    );
}

#[tokio::test]
async fn bitwise_and_on_decimal_arb_is_not_hijacked() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt & 1 AS r FROM t").await;
    assert!(!msg.is_empty());
}

#[tokio::test]
async fn logical_and_with_a_decimal_arb_operand_is_rejected() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt AND bl AS r FROM t").await;
    assert!(!msg.is_empty());
}

/// FINDING F-D: `IS [NOT] DISTINCT FROM` is `Expr::IsNotDistinctFrom`, not a
/// `BinaryOperator`, so `DecimalArbExprPlanner::plan_binary_op` never sees it
/// and `DecimalArbExprRewrite` doesn't handle it either. DataFusion falls back
/// to raw `LargeBinary` equality — a BYTEWISE comparison of canonical decimal
/// bytes. Two numerically equal decimal_arb values stored at different scales
/// (100 @ scale 4 vs 100 @ scale 2) encode to different bytes, so
/// `x IS NOT DISTINCT FROM y` answers FALSE while `x = y` answers TRUE.
#[tokio::test]
#[ignore = "FINDING: IS NOT DISTINCT FROM over decimal_arb compares raw bytes and disagrees with `=`"]
async fn is_not_distinct_from_must_agree_with_the_equality_operator() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("x", 30, 4, true).unwrap(),
        DecimalArbType::field("y", 10, 2, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            arb_array("x", 30, 4, &[Some("100")]),
            arb_array("y", 10, 2, &[Some("100")]),
        ],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("nd", batch).unwrap();

    let sql = "SELECT x IS NOT DISTINCT FROM y AS r FROM nd";
    if let Ok(df) = ctx.sql(sql).await
        && let Ok(batches) = df.collect().await
    {
        let got = bool_column(&batches, "r", sql);
        assert_eq!(
            got,
            vec![Some(true)],
            "`x IS NOT DISTINCT FROM y` disagrees with `x = y` for numerically equal \
             decimal_arb values stored at different scales — a bytewise comparison leaked through"
        );
    }
}

#[tokio::test]
async fn non_decimal_arb_integer_arithmetic_is_untouched_by_the_planner() {
    let ctx = matrix_ctx();
    let sql = "SELECT i64 + i32 AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let col = batches[0]
        .column_by_name("r")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 + Int32 must stay an integer type, not become decimal_arb");
    assert_eq!(
        (0..3).map(|i| col.value(i)).collect::<Vec<_>>(),
        vec![2, 4, 6]
    );
}

#[tokio::test]
async fn non_decimal_arb_float_arithmetic_is_untouched_by_the_planner() {
    let ctx = matrix_ctx();
    let sql = "SELECT f64 + 1.5 AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let col = batches[0]
        .column_by_name("r")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 + literal must stay Float64");
    assert_eq!(col.value(0), 3.0);
}

#[tokio::test]
async fn non_decimal_arb_string_concat_is_untouched_by_the_planner() {
    let ctx = matrix_ctx();
    let sql = "SELECT s || 'x' AS r FROM t";
    let batches = ok_sql(&ctx, sql).await;
    let col = batches[0]
        .column_by_name("r")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 || Utf8 must stay Utf8");
    assert_eq!(col.value(0), "1x");
}

#[tokio::test]
async fn non_decimal_arb_comparison_is_untouched_by_the_planner() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT i64 > 1 AS r FROM t",
        "r",
        &[Some(false), Some(true), Some(true)],
    )
    .await;
}

#[tokio::test]
async fn unknown_column_operand_produces_a_planning_error_not_a_panic() {
    let ctx = matrix_ctx();
    let msg = err_sql(&ctx, "SELECT amt + nope AS r FROM t").await;
    assert!(
        msg.to_lowercase().contains("nope") || msg.to_lowercase().contains("column"),
        "unresolvable operand should surface a column-resolution error; got: {msg}"
    );
}

// =====================================================================
// K. Boundary values through the integer-coercion path
// =====================================================================

fn boundary_ctx() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 40, 0, true).unwrap(),
        Field::new("i8min", DataType::Int8, true),
        Field::new("i64min", DataType::Int64, true),
        Field::new("u64max", DataType::UInt64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            arb_array("a", 40, 0, &[Some("0")]),
            Arc::new(Int8Array::from(vec![i8::MIN])),
            Arc::new(Int64Array::from(vec![i64::MIN])),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
        ],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("b", batch).unwrap();
    ctx
}

#[tokio::test]
async fn int8_min_coerces_without_sign_loss() {
    let ctx = boundary_ctx();
    check_arb(
        &ctx,
        "SELECT a + i8min AS r FROM b",
        "r",
        (41, 0),
        &[Some("-128")],
    )
    .await;
}

#[tokio::test]
async fn int64_min_coerces_without_overflow() {
    let ctx = boundary_ctx();
    check_arb(
        &ctx,
        "SELECT a + i64min AS r FROM b",
        "r",
        (41, 0),
        &[Some("-9223372036854775808")],
    )
    .await;
}

#[tokio::test]
async fn uint64_max_fits_the_twenty_digit_coercion_precision() {
    let ctx = boundary_ctx();
    check_arb(
        &ctx,
        "SELECT a + u64max AS r FROM b",
        "r",
        (41, 0),
        &[Some("18446744073709551615")],
    )
    .await;
}

#[tokio::test]
async fn uint64_max_compares_correctly_against_a_wider_decimal_arb() {
    let ctx = boundary_ctx();
    check_bool(&ctx, "SELECT u64max > a AS r FROM b", "r", &[Some(true)]).await;
}

#[tokio::test]
async fn int64_min_compares_correctly_against_zero_valued_decimal_arb() {
    let ctx = boundary_ctx();
    check_bool(&ctx, "SELECT i64min < a AS r FROM b", "r", &[Some(true)]).await;
}

#[tokio::test]
async fn uint64_max_multiplied_by_decimal_arb_keeps_full_precision() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 40, 0, true).unwrap(),
        Field::new("u", DataType::UInt64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            arb_array("a", 40, 0, &[Some("2")]),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
        ],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("m", batch).unwrap();
    check_arb(
        &ctx,
        "SELECT a * u AS r FROM m",
        "r",
        (61, 0),
        &[Some("36893488147419103230")],
    )
    .await;
}

#[tokio::test]
async fn very_large_decimal_arb_plus_small_integer_stays_exact() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 80, 0, true).unwrap(),
    ]));
    let huge = "1".repeat(78);
    let batch = RecordBatch::try_new(schema, vec![arb_array("a", 80, 0, &[Some(&huge)])]).unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("h", batch).unwrap();
    let mut want = huge.clone();
    want.pop();
    want.push('2');
    check_arb(
        &ctx,
        "SELECT a + 1 AS r FROM h",
        "r",
        (81, 0),
        &[Some(&want)],
    )
    .await;
}

#[tokio::test]
async fn high_scale_decimal_arb_plus_integer_keeps_all_fractional_digits() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 40, 30, true).unwrap(),
    ]));
    // 30 fractional digits declared; the value has 29 significant ones.
    let batch = RecordBatch::try_new(
        schema,
        vec![arb_array(
            "a",
            40,
            30,
            &[Some("1.23456789012345678901234567890")],
        )],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("hs", batch).unwrap();
    check_arb(
        &ctx,
        "SELECT a + 1 AS r FROM hs",
        "r",
        (51, 30),
        &[Some("2.23456789012345678901234567890")],
    )
    .await;
}

#[tokio::test]
async fn high_scale_decimal_arb_divided_by_integer_keeps_the_input_scale() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 40, 30, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![arb_array("a", 40, 30, &[Some("1")])]).unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("hd", batch).unwrap();
    // div: s_out = max(30, 18) = 30; p_out = (40-30) + 0 + 30 = 40.
    check_arb(
        &ctx,
        "SELECT a / 4 AS r FROM hd",
        "r",
        (40, 30),
        &[Some("0.25")],
    )
    .await;
}

#[tokio::test]
async fn integer_divided_by_high_scale_decimal_arb_uses_default_scale_18() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 40, 30, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![arb_array("a", 40, 30, &[Some("4")])]).unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("hi", batch).unwrap();
    // s_out = max(0, 18) = 18; p_out = 20 + 30 + 18 = 68.
    check_arb(
        &ctx,
        "SELECT 1 / a AS r FROM hi",
        "r",
        (68, 18),
        &[Some("0.25")],
    )
    .await;
}

#[tokio::test]
async fn scale_one_decimal_arb_against_integer_rounds_half_even_on_division() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 10, 1, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![arb_array("a", 10, 1, &[Some("1")])]).unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("r1", batch).unwrap();
    // 1 / 3 at scale 18 = 0.333333333333333333
    check_arb(
        &ctx,
        "SELECT a / 3 AS r FROM r1",
        "r",
        (27, 18),
        &[Some("0.333333333333333333")],
    )
    .await;
}

#[tokio::test]
async fn decimal128_with_negative_scale_operand_errors_rather_than_mis_scaling() {
    let d = Decimal128Array::from(vec![Some(5_i128)])
        .with_precision_and_scale(10, -2)
        .expect("arrow allows negative Decimal128 scale");
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 20, 0, true).unwrap(),
        Field::new("n", DataType::Decimal128(10, -2), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![arb_array("a", 20, 0, &[Some("1")]), Arc::new(d)],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("ns", batch).unwrap();
    let msg = err_sql(&ctx, "SELECT a + n AS r FROM ns").await;
    assert!(
        msg.to_lowercase().contains("scale"),
        "a negative Decimal128 scale must be rejected with a scale-specific error \
         (silently treating 500 as 5 would corrupt the value); got: {msg}"
    );
}

#[tokio::test]
async fn decimal_arb_scale_equal_to_precision_still_coerces_integers() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 4, 4, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![arb_array("a", 4, 4, &[Some("0.5")])]).unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("sp", batch).unwrap();
    // add: s = 4, p = max(0, 20) + 4 + 1 = 25.
    check_arb(
        &ctx,
        "SELECT a + 1 AS r FROM sp",
        "r",
        (25, 4),
        &[Some("1.5")],
    )
    .await;
}

#[tokio::test]
async fn modulo_where_both_integer_parts_are_zero_keeps_a_valid_precision() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 4, 4, true).unwrap(),
        DecimalArbType::field("b", 4, 4, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            arb_array("a", 4, 4, &[Some("0.75")]),
            arb_array("b", 4, 4, &[Some("0.5")]),
        ],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("mz", batch).unwrap();
    check_arb(
        &ctx,
        "SELECT a % b AS r FROM mz",
        "r",
        (4, 4),
        &[Some("0.25")],
    )
    .await;
}

#[tokio::test]
async fn rounding_of_a_division_result_is_half_even_not_half_up() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 30, 0, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(schema, vec![arb_array("a", 30, 0, &[Some("2")])]).unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("he", batch).unwrap();
    // 2 / 3 = 0.666...; at scale 18 the 19th digit is 6 -> rounds up to ...667.
    check_arb(
        &ctx,
        "SELECT a / 3 AS r FROM he",
        "r",
        (48, 18),
        &[Some("0.666666666666666667")],
    )
    .await;
}

// =====================================================================
// L. Metadata retention audit (one test per operator + coercion path)
// =====================================================================

macro_rules! metadata_kept {
    ($name:ident, $sql:literal) => {
        #[tokio::test]
        async fn $name() {
            let ctx = matrix_ctx();
            let batches = ok_sql(&ctx, $sql).await;
            let field = field_of(&batches, "r");
            assert!(
                DecimalArbType::is_decimal_arb_field(&field),
                "`{}` produced an output field WITHOUT decimal_arb metadata: {field:?} \
                 — a sink would emit raw BYTEA / hex instead of NUMERIC",
                $sql
            );
        }
    };
}

metadata_kept!(metadata_kept_add_int_literal, "SELECT amt + 1 AS r FROM t");
metadata_kept!(metadata_kept_sub_int_literal, "SELECT amt - 1 AS r FROM t");
metadata_kept!(metadata_kept_mul_int_literal, "SELECT amt * 1 AS r FROM t");
metadata_kept!(metadata_kept_div_int_literal, "SELECT amt / 1 AS r FROM t");
metadata_kept!(metadata_kept_mod_int_literal, "SELECT amt % 7 AS r FROM t");
metadata_kept!(
    metadata_kept_add_int_literal_left,
    "SELECT 1 + amt AS r FROM t"
);
metadata_kept!(
    metadata_kept_add_decimal128_column,
    "SELECT amt + d128 AS r FROM t"
);
metadata_kept!(
    metadata_kept_add_decimal128_column_left,
    "SELECT d128 + amt AS r FROM t"
);
metadata_kept!(
    metadata_kept_add_decimal256_column,
    "SELECT amt + d256 AS r FROM t"
);
metadata_kept!(
    metadata_kept_add_decimal256_column_left,
    "SELECT d256 + amt AS r FROM t"
);
metadata_kept!(
    metadata_kept_add_uint64_column,
    "SELECT amt + u64 AS r FROM t"
);
metadata_kept!(
    metadata_kept_add_int8_column_left,
    "SELECT i8 + amt AS r FROM t"
);
metadata_kept!(
    metadata_kept_nested_expression,
    "SELECT amt + amt2 * amt0 AS r FROM t"
);
metadata_kept!(
    metadata_kept_through_filter,
    "SELECT amt + 1 AS r FROM t WHERE amt > 0"
);
metadata_kept!(
    metadata_kept_through_order_by,
    "SELECT amt + 1 AS r FROM t ORDER BY r"
);
metadata_kept!(
    metadata_kept_through_limit,
    "SELECT amt + 1 AS r FROM t LIMIT 1"
);
metadata_kept!(
    metadata_kept_through_subquery,
    "SELECT r FROM (SELECT amt * 2 AS r FROM t)"
);
metadata_kept!(
    metadata_kept_through_distinct,
    "SELECT DISTINCT amt + 1 AS r FROM t"
);
metadata_kept!(
    metadata_kept_through_group_by_key,
    "SELECT amt + 1 AS r, COUNT(*) AS n FROM t GROUP BY amt + 1"
);
metadata_kept!(
    metadata_kept_through_coalesce_over_coerced_expr,
    "SELECT COALESCE(amt + 1, amt + 2) AS r FROM t"
);
metadata_kept!(
    metadata_kept_through_case_over_coerced_expr,
    "SELECT CASE WHEN i64 = 1 THEN amt + 1 ELSE amt + 2 END AS r FROM t"
);
metadata_kept!(
    metadata_kept_for_plain_column_projection,
    "SELECT amt AS r FROM t"
);
metadata_kept!(
    metadata_kept_through_non_equi_self_join_projection,
    "SELECT l.amt + 1 AS r FROM t AS l JOIN t AS rt ON l.amt > rt.amt0"
);

/// Not a decimal_arb defect: any *equi*-join executed through `SessionManager`
/// dies with "Invalid HashJoinExec, unsupported PartitionMode Auto in
/// execute()", because `StreamlingPhysicalOptimizerRules` omits the DataFusion
/// rule that resolves `PartitionMode::Auto`. The control test below shows the
/// same failure with no decimal_arb column in sight. Kept as an ignored probe so
/// the decimal_arb-side assertion is ready once joins execute.
#[tokio::test]
#[ignore = "FINDING: equi-joins fail to execute under SessionManager (HashJoinExec PartitionMode::Auto) — not decimal_arb-specific"]
async fn metadata_kept_through_equi_self_join_projection() {
    let ctx = matrix_ctx();
    let sql = "SELECT l.amt + 1 AS r FROM t AS l JOIN t AS rt ON l.i64 = rt.i64";
    let batches = ok_sql(&ctx, sql).await;
    let field = field_of(&batches, "r");
    assert!(DecimalArbType::is_decimal_arb_field(&field), "{field:?}");
}

/// Control for the above: no decimal_arb anywhere, same failure.
#[tokio::test]
async fn equi_join_failure_is_not_caused_by_decimal_arb() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("plain", batch).unwrap();
    let sql = "SELECT a.k AS r FROM plain AS a JOIN plain AS b ON a.k = b.k";
    let df = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning an all-Int64 equi-join failed: {e}"));
    match df.collect().await {
        Err(e) => assert!(
            e.to_string().contains("PartitionMode"),
            "an all-Int64 equi-join failed for an unexpected reason: {e}"
        ),
        Ok(batches) => {
            let mut got: Vec<i64> = batches
                .iter()
                .flat_map(|b| {
                    let c = b
                        .column_by_name("r")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    (0..b.num_rows()).map(|i| c.value(i)).collect::<Vec<_>>()
                })
                .collect();
            got.sort();
            assert_eq!(
                got,
                vec![1, 2],
                "equi-joins now execute; re-check the ignored decimal_arb join test"
            );
        }
    }
}

#[tokio::test]
async fn coerced_expression_output_field_precision_is_never_zero() {
    let ctx = matrix_ctx();
    for sql in [
        "SELECT amt + 1 AS r FROM t",
        "SELECT amt - 1 AS r FROM t",
        "SELECT amt * 1 AS r FROM t",
        "SELECT amt / 1 AS r FROM t",
        "SELECT amt % 7 AS r FROM t",
        "SELECT amt2 % amt0 AS r FROM t",
    ] {
        let batches = ok_sql(&ctx, sql).await;
        let field = field_of(&batches, "r");
        let (p, s) = DecimalArbType::precision_scale_from_field(&field)
            .unwrap_or_else(|| panic!("`{sql}` lost metadata"));
        assert!(p > 0, "`{sql}` produced precision 0 (invalid decimal_arb)");
        assert!(
            s <= p,
            "`{sql}` produced scale {s} > precision {p} (invalid decimal_arb)"
        );
    }
}

#[tokio::test]
async fn integer_coercion_agrees_with_the_pure_decimal_arb_form_for_every_operator() {
    // For each operator, `amt <op> 2` (integer literal, coerced) must produce the
    // same NUMBERS as `amt <op> two` where `two` is a decimal_arb column holding
    // 2. Only the declared (precision, scale) may differ.
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("amt", 30, 4, true).unwrap(),
        DecimalArbType::field("two", 20, 0, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            arb_array("amt", 30, 4, &[Some("12.5"), Some("100"), Some("-3")]),
            arb_array("two", 20, 0, &[Some("2"), Some("2"), Some("2")]),
        ],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("eq", batch).unwrap();

    for op in ["+", "-", "*", "/", "%"] {
        let via_int = format!("SELECT amt {op} 2 AS r FROM eq");
        let via_arb = format!("SELECT amt {op} two AS r FROM eq");
        let a = ok_sql(&ctx, &via_int).await;
        let b = ok_sql(&ctx, &via_arb).await;
        let (aps, av) = arb_column(&a, "r", &via_int);
        let (bps, bv) = arb_column(&b, "r", &via_arb);
        assert_eq!(
            aps, bps,
            "`{via_int}` and `{via_arb}` should widen identically \
             (an integer literal coerces to decimal_arb(20, 0), same as `two`)"
        );
        assert_eq!(
            av,
            bv,
            "coercing the integer literal changed the answer for `{op}`: \
             literal form [{}] vs decimal_arb form [{}]",
            fmt(&av),
            fmt(&bv)
        );
    }
}

#[tokio::test]
async fn integer_column_coercion_agrees_with_the_pure_decimal_arb_form() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("amt", 30, 4, true).unwrap(),
        DecimalArbType::field("two", 20, 0, true).unwrap(),
        Field::new("k", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            arb_array("amt", 30, 4, &[Some("12.5"), Some("100"), Some("-3")]),
            arb_array("two", 20, 0, &[Some("2"), Some("7"), Some("-5")]),
            Arc::new(Int32Array::from(vec![2_i32, 7, -5])),
        ],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("eqc", batch).unwrap();

    for op in ["+", "-", "*", "/", "%"] {
        let via_int = format!("SELECT amt {op} k AS r FROM eqc");
        let via_arb = format!("SELECT amt {op} two AS r FROM eqc");
        let a = ok_sql(&ctx, &via_int).await;
        let b = ok_sql(&ctx, &via_arb).await;
        let (aps, av) = arb_column(&a, "r", &via_int);
        let (bps, bv) = arb_column(&b, "r", &via_arb);
        assert_eq!(aps, bps, "`{via_int}` vs `{via_arb}` widening");
        assert_eq!(
            av,
            bv,
            "coercing the Int32 column changed the answer for `{op}`: [{}] vs [{}]",
            fmt(&av),
            fmt(&bv)
        );
    }
}

#[tokio::test]
async fn integer_coercion_agrees_with_the_pure_decimal_arb_form_for_every_comparison() {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("amt", 30, 4, true).unwrap(),
        DecimalArbType::field("two", 20, 0, true).unwrap(),
        Field::new("k", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            arb_array("amt", 30, 4, &[Some("12.5"), Some("100"), Some("-3")]),
            arb_array("two", 20, 0, &[Some("12"), Some("100"), Some("-3")]),
            Arc::new(Int64Array::from(vec![12_i64, 100, -3])),
        ],
    )
    .unwrap();
    let ctx = manager().session_context();
    ctx.register_batch("cmpq", batch).unwrap();

    for op in ["=", "!=", "<", "<=", ">", ">="] {
        let via_int = format!("SELECT amt {op} k AS r FROM cmpq");
        let via_arb = format!("SELECT amt {op} two AS r FROM cmpq");
        let a = ok_sql(&ctx, &via_int).await;
        let b = ok_sql(&ctx, &via_arb).await;
        assert_eq!(
            bool_column(&a, "r", &via_int),
            bool_column(&b, "r", &via_arb),
            "coercing the Int64 column changed the comparison answer for `{op}`"
        );
    }
}

#[tokio::test]
async fn integer_coercion_uses_scale_zero_so_fractional_digits_are_not_invented() {
    // A scale-0 coercion must not turn `3` into `0.0003` (a scale mix-up would
    // show up as a wildly wrong sum).
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt0 + 3 AS r FROM t",
        "r",
        (21, 0),
        &[Some("10"), Some("8"), Some("1")],
    )
    .await;
}

#[tokio::test]
async fn coercion_does_not_truncate_the_decimal_arb_side_to_the_integer_scale() {
    // The dangerous inverse of the previous test: coercing the *decimal_arb* side
    // down to scale 0 would silently drop `.5`.
    let ctx = matrix_ctx();
    check_arb(
        &ctx,
        "SELECT amt + 0 AS r FROM t",
        "r",
        (31, 4),
        &[Some("12.5"), Some("100"), Some("-3")],
    )
    .await;
}

#[tokio::test]
async fn coercion_preserves_fraction_in_comparisons_against_integers() {
    let ctx = matrix_ctx();
    // 12.5 > 12 must be TRUE; a scale-0 truncation of the left side would make it FALSE.
    check_bool(
        &ctx,
        "SELECT amt > 12 AS r FROM t",
        "r",
        &[Some(true), Some(true), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn coercion_preserves_fraction_in_equality_against_integers() {
    let ctx = matrix_ctx();
    // 12.5 = 12 must be FALSE; truncation would make it TRUE.
    check_bool(
        &ctx,
        "SELECT amt = 12 AS r FROM t",
        "r",
        &[Some(false), Some(false), Some(false)],
    )
    .await;
}

#[tokio::test]
async fn integer_column_equality_against_arb_is_not_reflexively_true() {
    let ctx = matrix_ctx();
    check_bool(
        &ctx,
        "SELECT i64 = amt AS r FROM t",
        "r",
        &[Some(false), Some(false), Some(false)],
    )
    .await;
}
