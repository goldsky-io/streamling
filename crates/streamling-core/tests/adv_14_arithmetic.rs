//! Adversarial coverage for `decimal_arb` ARITHMETIC through the full
//! in-process SQL stack (`SessionManager` — ExprPlanner + FunctionRewrite +
//! UDFs + sort rewrite).
//!
//! Focus (agent 14): `+ - * / %` over every sign combination, operands with
//! differing scales, repeating-decimal division and its rounding rule,
//! division/modulo by zero, identity/commutativity/associativity/
//! distributivity laws, chained arithmetic at the precision boundary, and
//! NULL operands.
//!
//! EVERY arithmetic assertion checks BOTH:
//!   * the decoded numeric result rendered with its full declared scale
//!     (`to_canonical_string()` keeps trailing scale digits, so a wrong
//!     output scale changes the string), AND
//!   * the output field's `streamling.decimal_arb` (precision, scale).
//!
//! A wrong result scale is data corruption at the sink just as much as a
//!
//! wrong digit, and a dropped extension metadata is the F2 failure shape.
//!
//! Widening rules under test (`output_precision_scale` in
//! `streamling-common/src/functions/decimal_arb_ops.rs`, spec data-model E5):
//!   add/sub: s = max(s1,s2)          p = max(p1-s1, p2-s2) + s + 1
//!   mul:     s = min(s1+s2, p)       p = p1 + p2 + 1
//!   div:     s = max(s1, 18)         p = (p1-s1) + s2 + s
//!   mod:     s = max(s1,s2)          p = min(p1-s1, p2-s2) + s
//! Integer literals coerce to `decimal_arb(20, 0)` (`INT_COERCE_PRECISION`).
//! Division rounds excess fractional digits half-to-even.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, LargeBinaryArray, RecordBatch};
use arrow::datatypes::Schema;
use datafusion::datasource::MemTable;
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

#[derive(Clone)]
struct ArbCol {
    name: String,
    p: u32,
    s: u32,
    vals: Vec<Option<String>>,
}

fn col(name: &str, p: u32, s: u32, vals: &[Option<&str>]) -> ArbCol {
    ArbCol {
        name: name.to_string(),
        p,
        s,
        vals: vals.iter().map(|v| v.map(|x| x.to_string())).collect(),
    }
}

/// Non-null convenience constructor.
fn colv(name: &str, p: u32, s: u32, vals: &[&str]) -> ArbCol {
    ArbCol {
        name: name.to_string(),
        p,
        s,
        vals: vals.iter().map(|x| Some(x.to_string())).collect(),
    }
}

fn build_array(c: &ArbCol, build_p: u32, build_s: u32) -> ArrayRef {
    let mut b =
        DecimalArbArrayBuilder::with_capacity(c.vals.len(), c.name.clone(), build_p, build_s)
            .unwrap_or_else(|e| panic!("builder {}({build_p},{build_s}): {e}", c.name));
    for v in &c.vals {
        match v {
            Some(x) => b
                .append_value(&DecimalArbValue::from_str(x).expect("parse literal"))
                .unwrap_or_else(|e| panic!("append `{x}` to {}({build_p},{build_s}): {e}", c.name)),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish().into_inner().0)
}

/// Register a MemTable whose columns are all `decimal_arb`, declared exactly
/// as built.
fn register(sm: &SessionManager, table: &str, cols: &[ArbCol]) {
    let fields: Vec<_> = cols
        .iter()
        .map(|c| DecimalArbType::field(&c.name, c.p, c.s, true).expect("decimal_arb field"))
        .collect();
    let arrays: Vec<ArrayRef> = cols.iter().map(|c| build_array(c, c.p, c.s)).collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("RecordBatch::try_new");
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable")),
    )
    .expect("register_table");
}

/// Register a table whose DECLARED precision is deliberately narrower than the
/// data actually encoded in the array (malformed upstream schema). Used to
/// prove arithmetic surfaces a clean error rather than panicking or silently
/// truncating.
fn register_mismatched(sm: &SessionManager, table: &str, c: &ArbCol, declared_p: u32) {
    let array = build_array(c, c.p, c.s);
    let field = DecimalArbType::field(&c.name, declared_p, c.s, true).expect("field");
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).expect("batch");
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable")),
    )
    .expect("register_table");
}

async fn try_run(sm: &SessionManager, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let plan = sm
        .create_logical_plan(sql.to_string())
        .await
        .map_err(|e| format!("plan: {e}"))?;
    sm.new_df(plan)
        .collect()
        .await
        .map_err(|e| format!("exec: {e}"))
}

async fn run(sm: &SessionManager, sql: &str) -> Vec<RecordBatch> {
    match try_run(sm, sql).await {
        Ok(b) => b,
        Err(e) => panic!("query failed unexpectedly: `{sql}`\n  {e}"),
    }
}

async fn expect_err(sm: &SessionManager, sql: &str) -> String {
    match try_run(sm, sql).await {
        Ok(b) => {
            let rows: usize = b.iter().map(|x| x.num_rows()).sum();
            panic!("expected an error from `{sql}`, but it returned {rows} row(s)")
        }
        Err(e) => e,
    }
}

/// The output column's `(precision, scale)`. Fails loudly if the extension
/// metadata was dropped (F2 shape).
fn meta(batches: &[RecordBatch], name: &str) -> (u32, u32) {
    let field = batches[0]
        .schema()
        .field_with_name(name)
        .unwrap_or_else(|e| panic!("no output column `{name}`: {e}"))
        .clone();
    assert!(
        DecimalArbType::is_decimal_arb_field(&field),
        "output column `{name}` lost its streamling.decimal_arb extension metadata \
         (sinks would treat it as raw BYTEA): {field:?}"
    );
    DecimalArbType::precision_scale_from_field(&field)
        .unwrap_or_else(|| panic!("output column `{name}` carries no (precision, scale)"))
}

/// Decode the output column AT ITS DECLARED OUTPUT SCALE and render it with
/// `to_canonical_string()`, which preserves trailing scale digits — so the
/// returned strings pin the value AND the scale.
fn vals(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let (_, scale) = meta(batches, name);
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap_or_else(|e| panic!("{e}"));
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap_or_else(|| {
                panic!(
                    "output column `{name}` is not LargeBinary decimal_arb storage (got {:?})",
                    b.column(idx).data_type()
                )
            });
        for i in 0..b.num_rows() {
            if arr.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(
                    DecimalArbValue::from_canonical_bytes_at_scale(arr.value(i), scale)
                        .unwrap_or_else(|e| {
                            panic!("decode `{name}` row {i} at declared scale {scale}: {e}")
                        })
                        .to_canonical_string(),
                ));
            }
        }
    }
    out
}

fn raw_bytes(batches: &[RecordBatch], name: &str, row: usize) -> Vec<u8> {
    let b = &batches[0];
    let idx = b.schema().index_of(name).unwrap();
    let arr = b
        .column(idx)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("LargeBinary");
    arr.value(row).to_vec()
}

fn s(v: &[&str]) -> Vec<Option<String>> {
    v.iter().map(|x| Some(x.to_string())).collect()
}

/// `a` = ±7.00, `b` = ±3.00 over all four sign combinations, both
/// `decimal_arb(20, 2)`.
fn signs_sm() -> SessionManager {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("a", 20, 2, &["7.00", "7.00", "-7.00", "-7.00"]),
            colv("b", 20, 2, &["3.00", "-3.00", "3.00", "-3.00"]),
        ],
    );
    sm
}

/// One row of operands with deliberately different scales.
/// c2 = 1.25 (10,2), c5 = 2.50000 (12,5), c0 = 4 (20,0), c8 = 0.00000125 (30,8)
fn scales_sm() -> SessionManager {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("c2", 10, 2, &["1.25"]),
            colv("c5", 12, 5, &["2.50000"]),
            colv("c0", 20, 0, &["4"]),
            colv("c8", 30, 8, &["0.00000125"]),
        ],
    );
    sm
}

/// Division fixtures: `n / d`, both `decimal_arb(30, 0)`.
fn div_sm() -> SessionManager {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv(
                "n",
                30,
                0,
                &[
                    "1", "2", "1", "10", "1", "-1", "-2", "1", "3", "-1", "-3", "5", "1", "0",
                    "100",
                ],
            ),
            colv(
                "d",
                30,
                0,
                &[
                    "3", "3", "7", "3", "6", "3", "3", "524288", "524288", "524288", "524288", "2",
                    "1024", "5", "8",
                ],
            ),
        ],
    );
    sm
}

fn idt_sm() -> SessionManager {
    let sm = manager();
    register(
        &sm,
        "t",
        &[colv("a", 12, 3, &["7.250", "-7.250", "0", "0.001"])],
    );
    sm
}

fn abc_sm() -> SessionManager {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("a", 10, 2, &["1.25"]),
            colv("b", 10, 3, &["2.005"]),
            colv("c", 10, 1, &["3.5"]),
        ],
    );
    sm
}

fn null_sm() -> SessionManager {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            col("a", 10, 2, &[Some("1.25"), None, None, Some("3.00")]),
            col("b", 10, 2, &[None, Some("2.00"), None, Some("4.00")]),
        ],
    );
    sm
}

fn zero_sm() -> SessionManager {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("a", 10, 2, &["5.00"]),
            colv("z", 10, 2, &["0.00"]),
            col("an", 10, 2, &[None]),
        ],
    );
    sm
}

fn rep(c: char, n: usize) -> String {
    std::iter::repeat_n(c, n).collect()
}

// =====================================================================
// 1. Sign matrix — every op, all four sign combinations
// =====================================================================

#[tokio::test]
async fn add_is_exact_for_all_four_sign_combinations() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a + b AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["10.00", "4.00", "-4.00", "-10.00"]),
        "decimal_arb addition must be exact and scale-preserving across signs"
    );
}

#[tokio::test]
async fn add_output_precision_scale_follows_e5_rule() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a + b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (21, 2),
        "add over (20,2)+(20,2) must widen to max(p1-s1,p2-s2)+max(s1,s2)+1 = 21, scale 2"
    );
}

#[tokio::test]
async fn sub_is_exact_for_all_four_sign_combinations() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a - b AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["4.00", "10.00", "-10.00", "-4.00"]),
        "decimal_arb subtraction must be exact across signs"
    );
}

#[tokio::test]
async fn sub_output_precision_scale_follows_e5_rule() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a - b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (21, 2),
        "sub widening must match add widening"
    );
}

#[tokio::test]
async fn mul_is_exact_for_all_four_sign_combinations() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a * b AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["21.0000", "-21.0000", "-21.0000", "21.0000"]),
        "decimal_arb multiplication must be exact at scale s1+s2 across signs"
    );
}

#[tokio::test]
async fn mul_output_precision_scale_follows_e5_rule() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a * b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (41, 4),
        "mul over (20,2)*(20,2) must be (p1+p2+1, s1+s2) = (41, 4)"
    );
}

#[tokio::test]
async fn div_is_correct_for_all_four_sign_combinations() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a / b AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&[
            "2.333333333333333333",
            "-2.333333333333333333",
            "-2.333333333333333333",
            "2.333333333333333333",
        ]),
        "7/3 at the documented div scale (18) across signs"
    );
}

#[tokio::test]
async fn div_output_precision_scale_follows_e5_rule() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a / b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (38, 18),
        "div over (20,2)/(20,2) must be ((p1-s1)+s2+max(s1,18), max(s1,18)) = (38, 18)"
    );
}

#[tokio::test]
async fn mod_is_correct_for_all_four_sign_combinations() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a % b AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["1.00", "1.00", "-1.00", "-1.00"]),
        "SQL modulo sign must follow the DIVIDEND (Postgres semantics)"
    );
}

#[tokio::test]
async fn mod_output_precision_scale_follows_e5_rule() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a % b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (20, 2),
        "mod over (20,2)%(20,2) must be (min(p1-s1,p2-s2)+max(s1,s2), max(s1,s2)) = (20, 2)"
    );
}

#[tokio::test]
async fn mod_sign_is_independent_of_divisor_sign() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a % b AS r FROM t").await;
    let v = vals(&b, "r");
    assert_eq!(
        v[0], v[1],
        "7%3 and 7%-3 must agree: divisor sign is ignored"
    );
    assert_eq!(
        v[2], v[3],
        "-7%3 and -7%-3 must agree: divisor sign is ignored"
    );
}

#[tokio::test]
async fn div_with_operands_swapped_is_the_reciprocal_quotient() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT b / a AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&[
            "0.428571428571428571",
            "-0.428571428571428571",
            "-0.428571428571428571",
            "0.428571428571428571",
        ]),
        "3/7 rounded half-even at scale 18"
    );
}

#[tokio::test]
async fn mod_with_divisor_larger_than_dividend_returns_dividend() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT b % a AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["3.00", "-3.00", "3.00", "-3.00"]),
        "|b| < |a| so b % a == b, keeping b's sign"
    );
}

#[tokio::test]
async fn add_does_not_round_when_scales_already_match() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT (a + b) - (a + b) AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["0", "0", "0", "0"]),
        "add must be exact: x - x over the same expression is exactly zero"
    );
}

// =====================================================================
// 2. Operands with differing scales
// =====================================================================

#[tokio::test]
async fn add_mixed_scales_uses_max_scale_and_exact_value() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 + c5 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (14, 5), "s=max(2,5)=5, p=max(8,7)+5+1=14");
    assert_eq!(vals(&b, "r"), s(&["3.75000"]), "1.25 + 2.50000 = 3.75000");
}

#[tokio::test]
async fn add_mixed_scales_is_type_and_value_commutative() {
    let sm = scales_sm();
    let l = run(&sm, "SELECT c2 + c5 AS r FROM t").await;
    let r = run(&sm, "SELECT c5 + c2 AS r FROM t").await;
    assert_eq!(
        meta(&l, "r"),
        meta(&r, "r"),
        "add widening must be symmetric in the operand order"
    );
    assert_eq!(vals(&l, "r"), vals(&r, "r"), "add must be commutative");
}

#[tokio::test]
async fn sub_mixed_scales_keeps_max_scale() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 - c5 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (14, 5));
    assert_eq!(vals(&b, "r"), s(&["-1.25000"]), "1.25 - 2.50000");
}

#[tokio::test]
async fn sub_mixed_scales_reversed_is_negated() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c5 - c2 AS r FROM t").await;
    assert_eq!(vals(&b, "r"), s(&["1.25000"]), "2.50000 - 1.25");
}

#[tokio::test]
async fn mul_mixed_scales_scale_is_sum_of_scales() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 * c5 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (23, 7), "p=10+12+1=23, s=2+5=7");
    assert_eq!(
        vals(&b, "r"),
        s(&["3.1250000"]),
        "1.25 * 2.50000 = 3.125 rendered at scale 7"
    );
}

#[tokio::test]
async fn div_mixed_scales_scale_is_max_of_left_scale_and_18() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 / c5 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (31, 18), "p=(10-2)+5+18=31, s=max(2,18)=18");
    assert_eq!(vals(&b, "r"), s(&["0.500000000000000000"]));
}

#[tokio::test]
async fn div_scale_follows_the_left_operand_when_it_exceeds_18() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[colv("hi", 40, 25, &["1"]), colv("lo", 10, 2, &["4.00"])],
    );
    let b = run(&sm, "SELECT hi / lo AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (42, 25),
        "s=max(25,18)=25, p=(40-25)+2+25=42"
    );
    assert_eq!(
        vals(&b, "r"),
        s(&["0.2500000000000000000000000"]),
        "1/4 rendered at the wider left-operand scale"
    );
}

#[tokio::test]
async fn div_mixed_scales_reversed_changes_output_type() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c5 / c2 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (27, 18), "p=(12-5)+2+18=27");
    assert_eq!(vals(&b, "r"), s(&["2.000000000000000000"]));
}

#[tokio::test]
async fn mod_mixed_scales_uses_max_scale_and_min_integer_digits() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 % c5 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (12, 5), "s=max(2,5)=5, p=min(8,7)+5=12");
    assert_eq!(
        vals(&b, "r"),
        s(&["1.25000"]),
        "1.25 % 2.5 = 1.25 (dividend smaller than divisor)"
    );
}

#[tokio::test]
async fn mod_exactly_divisible_mixed_scales_is_canonical_zero() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c5 % c2 AS r FROM t").await;
    assert_eq!(vals(&b, "r"), s(&["0"]), "2.5 % 1.25 == 0");
}

#[tokio::test]
async fn add_scale_zero_column_keeps_the_fractional_operand_scale() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 + c0 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (23, 2), "s=max(2,0)=2, p=max(8,20)+2+1=23");
    assert_eq!(vals(&b, "r"), s(&["5.25"]));
}

#[tokio::test]
async fn sub_scale_zero_minus_fractional() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c0 - c2 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (23, 2));
    assert_eq!(vals(&b, "r"), s(&["2.75"]));
}

#[tokio::test]
async fn mul_by_scale_zero_column_keeps_scale() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 * c0 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (31, 2), "p=10+20+1=31, s=2+0=2");
    assert_eq!(vals(&b, "r"), s(&["5.00"]));
}

#[tokio::test]
async fn div_scale_zero_by_fractional_promotes_to_scale_18() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c0 / c2 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (40, 18), "p=(20-0)+2+18=40, s=max(0,18)=18");
    assert_eq!(
        vals(&b, "r"),
        s(&["3.200000000000000000"]),
        "4 / 1.25 = 3.2"
    );
}

#[tokio::test]
async fn mod_scale_zero_by_fractional_keeps_fractional_scale() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c0 % c2 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (10, 2), "s=max(0,2)=2, p=min(20,8)+2=10");
    assert_eq!(vals(&b, "r"), s(&["0.25"]), "4 % 1.25 = 0.25");
}

#[tokio::test]
async fn add_with_very_small_scale8_operand_is_exact() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c8 + c2 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (31, 8), "s=max(8,2)=8, p=max(22,8)+8+1=31");
    assert_eq!(vals(&b, "r"), s(&["1.25000125"]));
}

#[tokio::test]
async fn mul_with_very_small_scale8_operand_sums_scales() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c8 * c2 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (41, 10), "p=30+10+1=41, s=8+2=10");
    assert_eq!(
        vals(&b, "r"),
        s(&["0.0000015625"]),
        "0.00000125 * 1.25 = 0.0000015625, exact at scale 10"
    );
}

#[tokio::test]
async fn div_by_a_very_small_value_scales_up_exactly() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 / c8 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (34, 18), "p=(10-2)+8+18=34");
    assert_eq!(
        vals(&b, "r"),
        s(&["1000000.000000000000000000"]),
        "1.25 / 0.00000125 = 1000000 exactly"
    );
}

#[tokio::test]
async fn div_of_a_very_small_value_stays_small() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c8 / c2 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (42, 18), "p=(30-8)+2+18=42, s=max(8,18)=18");
    assert_eq!(vals(&b, "r"), s(&["0.000001000000000000"]));
}

#[tokio::test]
async fn mod_of_a_smaller_value_returns_it_unchanged() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c8 % c2 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (16, 8), "s=max(8,2)=8, p=min(22,8)+8=16");
    assert_eq!(vals(&b, "r"), s(&["0.00000125"]));
}

#[tokio::test]
async fn mod_by_a_very_small_exact_divisor_is_zero() {
    let sm = scales_sm();
    let b = run(&sm, "SELECT c2 % c8 AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["0"]),
        "1.25 is an exact multiple of 1.25e-6"
    );
}

// =====================================================================
// 3. Division: repeating decimals + the documented rounding rule
// =====================================================================

#[tokio::test]
async fn div_output_scale_for_scale_zero_operands_is_the_default_18() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (48, 18),
        "div over (30,0)/(30,0) must be ((30-0)+0+18, 18) = (48, 18)"
    );
}

#[tokio::test]
async fn div_one_third_is_rounded_to_18_fractional_digits() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(vals(&b, "r")[0], Some("0.333333333333333333".to_string()));
}

#[tokio::test]
async fn div_two_thirds_rounds_the_last_digit_up() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(
        vals(&b, "r")[1],
        Some("0.666666666666666667".to_string()),
        "2/3 must round the 18th digit up, not truncate"
    );
}

#[tokio::test]
async fn div_one_seventh_repeats_correctly() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(vals(&b, "r")[2], Some("0.142857142857142857".to_string()));
}

#[tokio::test]
async fn div_repeating_with_integer_part() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(vals(&b, "r")[3], Some("3.333333333333333333".to_string()));
}

#[tokio::test]
async fn div_one_sixth_rounds_up() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(vals(&b, "r")[4], Some("0.166666666666666667".to_string()));
}

#[tokio::test]
async fn div_negative_repeating_is_symmetric_with_positive() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    let v = vals(&b, "r");
    assert_eq!(v[5], Some("-0.333333333333333333".to_string()));
    assert_eq!(
        v[6],
        Some("-0.666666666666666667".to_string()),
        "rounding must be sign-symmetric (half-even, not half-up-toward-+inf)"
    );
}

#[tokio::test]
async fn div_exact_tie_rounds_half_to_even_downward() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    // 1/524288 == 0.0000019073486328125 exactly; digit 19 is a lone 5 and the
    // 18th digit is 2 (even) -> half-even keeps it. Half-up would give ...813.
    assert_eq!(
        vals(&b, "r")[7],
        Some("0.000001907348632812".to_string()),
        "division must round half-to-EVEN at the output scale"
    );
}

#[tokio::test]
async fn div_exact_tie_rounds_half_to_even_upward() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    // 3/524288 == 0.0000057220458984375; 18th digit is 7 (odd) -> half-even
    // rounds up to 8. Half-down would give ...437.
    assert_eq!(
        vals(&b, "r")[8],
        Some("0.000005722045898438".to_string()),
        "division must round half-to-EVEN at the output scale"
    );
}

#[tokio::test]
async fn div_negative_exact_tie_rounds_half_to_even_symmetrically() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    let v = vals(&b, "r");
    assert_eq!(v[9], Some("-0.000001907348632812".to_string()));
    assert_eq!(v[10], Some("-0.000005722045898438".to_string()));
}

#[tokio::test]
async fn div_exact_quotient_is_padded_to_the_declared_scale() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    let v = vals(&b, "r");
    assert_eq!(
        v[11],
        Some("2.500000000000000000".to_string()),
        "an exact quotient must still be rendered at the declared scale 18"
    );
    assert_eq!(v[14], Some("12.500000000000000000".to_string()));
}

#[tokio::test]
async fn div_terminating_binary_fraction_is_exact() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(
        vals(&b, "r")[12],
        Some("0.000976562500000000".to_string()),
        "1/1024 = 0.0009765625 exactly, padded to 18"
    );
}

#[tokio::test]
async fn div_zero_dividend_is_canonical_zero() {
    let sm = div_sm();
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(vals(&b, "r")[13], Some("0".to_string()));
    assert_eq!(
        raw_bytes(&b, "r", 13),
        vec![0x00u8],
        "0/x must encode as the single canonical zero byte"
    );
}

#[tokio::test]
async fn div_rounding_is_deterministic_within_one_query() {
    let sm = div_sm();
    let b = run(&sm, "SELECT (n / d) - (n / d) AS r FROM t").await;
    assert!(
        vals(&b, "r").iter().all(|v| v.as_deref() == Some("0")),
        "the same division expression must produce byte-identical results"
    );
}

#[tokio::test]
async fn div_rounding_is_deterministic_across_queries() {
    let sm = div_sm();
    let a = run(&sm, "SELECT n / d AS r FROM t").await;
    let c = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(
        vals(&a, "r"),
        vals(&c, "r"),
        "division must be deterministic across executions"
    );
}

#[tokio::test]
async fn div_negation_commutes_with_the_rounding() {
    let sm = div_sm();
    let b = run(&sm, "SELECT (n / d) + ((0 - n) / d) AS r FROM t").await;
    assert!(
        vals(&b, "r").iter().all(|v| v.as_deref() == Some("0")),
        "x/y + (-x)/y must be exactly zero: half-even rounding is sign-symmetric"
    );
}

// =====================================================================
// 4. Division / modulo by zero
// =====================================================================

#[tokio::test]
async fn div_by_zero_column_is_a_clean_error() {
    let sm = zero_sm();
    let e = expect_err(&sm, "SELECT a / z AS r FROM t").await;
    assert!(
        e.to_lowercase().contains("division by zero"),
        "expected a division-by-zero error, got: {e}"
    );
}

#[tokio::test]
async fn mod_by_zero_column_is_a_clean_error() {
    let sm = zero_sm();
    let e = expect_err(&sm, "SELECT a % z AS r FROM t").await;
    assert!(
        e.to_lowercase().contains("modulo by zero"),
        "expected a modulo-by-zero error, got: {e}"
    );
}

#[tokio::test]
async fn div_by_zero_literal_is_a_clean_error() {
    let sm = zero_sm();
    let e = expect_err(&sm, "SELECT a / 0 AS r FROM t").await;
    assert!(
        e.to_lowercase().contains("zero"),
        "expected a division-by-zero error, got: {e}"
    );
}

#[tokio::test]
async fn mod_by_zero_literal_is_a_clean_error() {
    let sm = zero_sm();
    let e = expect_err(&sm, "SELECT a % 0 AS r FROM t").await;
    assert!(
        e.to_lowercase().contains("zero"),
        "expected a modulo-by-zero error, got: {e}"
    );
}

#[tokio::test]
async fn zero_divided_by_zero_is_a_clean_error() {
    let sm = zero_sm();
    let e = expect_err(&sm, "SELECT z / z AS r FROM t").await;
    assert!(
        e.to_lowercase().contains("zero"),
        "0/0 must error, not return 0 or NaN, got: {e}"
    );
}

#[tokio::test]
async fn div_by_zero_error_does_not_leak_a_panic() {
    let sm = zero_sm();
    let e = expect_err(&sm, "SELECT a / z AS r FROM t").await;
    assert!(
        !e.contains("panicked") && !e.contains("Panic"),
        "division by zero must be a typed error, never a panic: {e}"
    );
}

#[tokio::test]
async fn zero_divided_by_nonzero_is_zero() {
    let sm = zero_sm();
    let b = run(&sm, "SELECT z / a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (28, 18), "p=(10-2)+2+18=28");
    assert_eq!(vals(&b, "r"), s(&["0"]));
}

#[tokio::test]
async fn zero_mod_nonzero_is_zero() {
    let sm = zero_sm();
    let b = run(&sm, "SELECT z % a AS r FROM t").await;
    assert_eq!(vals(&b, "r"), s(&["0"]));
}

#[tokio::test]
async fn null_divided_by_zero_is_null_not_an_error() {
    let sm = zero_sm();
    let b = run(&sm, "SELECT an / z AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        vec![None],
        "NULL must short-circuit before the divide-by-zero check (SQL 3VL)"
    );
}

#[tokio::test]
async fn null_mod_zero_is_null_not_an_error() {
    let sm = zero_sm();
    let b = run(&sm, "SELECT an % z AS r FROM t").await;
    assert_eq!(vals(&b, "r"), vec![None]);
}

#[tokio::test]
async fn div_by_zero_error_does_not_leak_operand_bytes() {
    let sm = zero_sm();
    let e = expect_err(&sm, "SELECT a / z AS r FROM t").await;
    assert!(
        !e.contains("LargeBinary["),
        "the error must not dump raw column bytes: {e}"
    );
}

// =====================================================================
// 5. Identity properties
// =====================================================================

#[tokio::test]
async fn add_zero_is_the_identity_on_value() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a + 0 AS r FROM t").await;
    assert_eq!(vals(&b, "r"), s(&["7.250", "-7.250", "0", "0.001"]));
}

#[tokio::test]
async fn add_zero_widens_precision_but_keeps_scale() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a + 0 AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (24, 3),
        "integer literal is decimal_arb(20,0): s=3, p=max(9,20)+3+1=24"
    );
}

#[tokio::test]
async fn sub_zero_is_the_identity() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a - 0 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (24, 3));
    assert_eq!(vals(&b, "r"), s(&["7.250", "-7.250", "0", "0.001"]));
}

#[tokio::test]
async fn mul_one_is_the_identity_on_value() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a * 1 AS r FROM t").await;
    assert_eq!(vals(&b, "r"), s(&["7.250", "-7.250", "0", "0.001"]));
}

#[tokio::test]
async fn mul_one_keeps_the_scale_unchanged() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a * 1 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (33, 3), "p=12+20+1=33, s=3+0=3");
}

#[tokio::test]
async fn div_one_preserves_value_but_promotes_scale_to_18() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a / 1 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (27, 18), "p=(12-3)+0+18=27, s=max(3,18)=18");
    assert_eq!(
        vals(&b, "r"),
        s(&[
            "7.250000000000000000",
            "-7.250000000000000000",
            "0",
            "0.001000000000000000",
        ])
    );
}

#[tokio::test]
async fn self_subtraction_is_exactly_zero() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a - a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (13, 3), "s=3, p=(12-3)+3+1=13");
    assert_eq!(vals(&b, "r"), s(&["0", "0", "0", "0"]));
}

#[tokio::test]
async fn self_subtraction_of_a_negative_encodes_canonical_zero_bytes() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a - a AS r FROM t").await;
    assert_eq!(
        raw_bytes(&b, "r", 1),
        vec![0x00u8],
        "(-7.250) - (-7.250) must encode as [0x00], never as a negative zero"
    );
}

#[tokio::test]
async fn mul_by_zero_never_produces_negative_zero_bytes() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a * 0 AS r FROM t").await;
    assert_eq!(vals(&b, "r"), s(&["0", "0", "0", "0"]));
    for row in 0..4 {
        assert_eq!(
            raw_bytes(&b, "r", row),
            vec![0x00u8],
            "row {row}: x*0 must encode as the canonical zero byte (a 0xFF sign \
             byte with an empty magnitude is rejected on decode and would split \
             GROUP BY keys)"
        );
    }
}

#[tokio::test]
async fn self_division_is_one_at_the_declared_scale() {
    let sm = manager();
    register(&sm, "t", &[colv("a", 12, 3, &["7.250", "-7.250", "0.001"])]);
    let b = run(&sm, "SELECT a / a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (30, 18), "p=(12-3)+3+18=30");
    assert_eq!(
        vals(&b, "r"),
        s(&[
            "1.000000000000000000",
            "1.000000000000000000",
            "1.000000000000000000",
        ])
    );
}

#[tokio::test]
async fn zero_minus_x_is_the_negation_of_x() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT 0 - a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (24, 3));
    assert_eq!(vals(&b, "r"), s(&["-7.250", "7.250", "0", "-0.001"]));
}

#[tokio::test]
async fn x_plus_negation_of_x_is_zero() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a + (0 - a) AS r FROM t").await;
    assert!(
        vals(&b, "r").iter().all(|v| v.as_deref() == Some("0")),
        "x + (-x) must be exactly zero"
    );
}

#[tokio::test]
async fn mod_by_one_returns_the_fractional_part_with_the_dividend_sign() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT a % 1 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (12, 3), "s=3, p=min(9,20)+3=12");
    assert_eq!(vals(&b, "r"), s(&["0.250", "-0.250", "0", "0.001"]));
}

#[tokio::test]
async fn add_then_subtract_round_trips_exactly() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT (a + 12345) - 12345 AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["7.250", "-7.250", "0", "0.001"]),
        "(x + k) - k must return x exactly for decimal arithmetic"
    );
}

#[tokio::test]
async fn mul_then_div_by_the_same_integer_round_trips() {
    let sm = idt_sm();
    let b = run(&sm, "SELECT (a * 8) / 8 AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&[
            "7.250000000000000000",
            "-7.250000000000000000",
            "0",
            "0.001000000000000000",
        ]),
        "(x * 8) / 8 is exactly representable at scale 18 and must round-trip"
    );
}

// =====================================================================
// 6. Commutativity / associativity / distributivity
// =====================================================================

#[tokio::test]
async fn add_is_commutative_in_value_and_type() {
    let sm = abc_sm();
    let l = run(&sm, "SELECT a + b AS r FROM t").await;
    let r = run(&sm, "SELECT b + a AS r FROM t").await;
    assert_eq!(meta(&l, "r"), (12, 3), "s=3, p=max(8,7)+3+1=12");
    assert_eq!(meta(&l, "r"), meta(&r, "r"));
    assert_eq!(vals(&l, "r"), s(&["3.255"]));
    assert_eq!(vals(&l, "r"), vals(&r, "r"));
}

#[tokio::test]
async fn mul_is_commutative_in_value_and_type() {
    let sm = abc_sm();
    let l = run(&sm, "SELECT a * b AS r FROM t").await;
    let r = run(&sm, "SELECT b * a AS r FROM t").await;
    assert_eq!(meta(&l, "r"), (21, 5), "p=10+10+1=21, s=2+3=5");
    assert_eq!(meta(&l, "r"), meta(&r, "r"));
    assert_eq!(vals(&l, "r"), s(&["2.50625"]));
    assert_eq!(vals(&l, "r"), vals(&r, "r"));
}

#[tokio::test]
async fn add_is_associative_in_value() {
    let sm = abc_sm();
    let l = run(&sm, "SELECT (a + b) + c AS r FROM t").await;
    let r = run(&sm, "SELECT a + (b + c) AS r FROM t").await;
    assert_eq!(vals(&l, "r"), s(&["6.755"]));
    assert_eq!(
        vals(&l, "r"),
        vals(&r, "r"),
        "decimal addition is exact, so it must be associative in value"
    );
    assert_eq!(
        meta(&l, "r").1,
        meta(&r, "r").1,
        "both groupings must land on the same output scale"
    );
}

#[tokio::test]
async fn add_associativity_precision_grows_with_nesting_depth() {
    let sm = abc_sm();
    let l = run(&sm, "SELECT (a + b) + c AS r FROM t").await;
    let r = run(&sm, "SELECT a + (b + c) AS r FROM t").await;
    assert_eq!(
        meta(&l, "r"),
        (13, 3),
        "((10,2)+(10,3))=(12,3); +(10,1)->(13,3)"
    );
    assert_eq!(
        meta(&r, "r"),
        (14, 3),
        "((10,3)+(10,1))=(13,3); (10,2)+ ->(14,3)"
    );
}

#[tokio::test]
async fn mul_is_associative_in_value_and_type() {
    let sm = abc_sm();
    let l = run(&sm, "SELECT (a * b) * c AS r FROM t").await;
    let r = run(&sm, "SELECT a * (b * c) AS r FROM t").await;
    assert_eq!(meta(&l, "r"), (32, 6));
    assert_eq!(meta(&l, "r"), meta(&r, "r"));
    assert_eq!(vals(&l, "r"), s(&["8.771875"]));
    assert_eq!(vals(&l, "r"), vals(&r, "r"));
}

#[tokio::test]
async fn mul_distributes_over_add_exactly() {
    let sm = abc_sm();
    let l = run(&sm, "SELECT a * (b + c) AS r FROM t").await;
    let r = run(&sm, "SELECT (a * b) + (a * c) AS r FROM t").await;
    assert_eq!(vals(&l, "r"), s(&["6.88125"]));
    assert_eq!(
        vals(&l, "r"),
        vals(&r, "r"),
        "exact decimal * and + must distribute"
    );
    assert_eq!(meta(&l, "r"), (24, 5));
    assert_eq!(meta(&r, "r"), (24, 5));
}

#[tokio::test]
async fn sub_anticommutes() {
    let sm = abc_sm();
    let l = run(&sm, "SELECT a - b AS r FROM t").await;
    let r = run(&sm, "SELECT 0 - (b - a) AS r FROM t").await;
    assert_eq!(vals(&l, "r"), s(&["-0.755"]));
    assert_eq!(
        vals(&l, "r"),
        vals(&r, "r"),
        "a - b must equal -(b - a) exactly"
    );
}

#[tokio::test]
async fn div_is_not_commutative() {
    let sm = abc_sm();
    let l = run(&sm, "SELECT a / b AS r FROM t").await;
    let r = run(&sm, "SELECT b / a AS r FROM t").await;
    assert_ne!(
        vals(&l, "r"),
        vals(&r, "r"),
        "a/b and b/a must not silently collapse to the same value"
    );
}

#[tokio::test]
async fn chained_add_of_three_columns_has_the_expected_type() {
    let sm = abc_sm();
    let b = run(&sm, "SELECT a + b + c AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (13, 3));
    assert_eq!(vals(&b, "r"), s(&["6.755"]));
}

#[tokio::test]
async fn mixed_precedence_expression_respects_sql_precedence() {
    let sm = abc_sm();
    let b = run(&sm, "SELECT a + b * c AS r FROM t").await;
    // b*c = (21,4) 7.0175 ; a + (21,4): s=4, p=max(8,17)+4+1=22
    assert_eq!(meta(&b, "r"), (22, 4));
    assert_eq!(
        vals(&b, "r"),
        s(&["8.2675"]),
        "a + b*c must bind * first: 1.25 + 7.0175"
    );
}

#[tokio::test]
async fn parenthesised_expression_differs_from_precedence_default() {
    let sm = abc_sm();
    let b = run(&sm, "SELECT (a + b) * c AS r FROM t").await;
    // (a+b) = (12,3) 3.255 ; *(10,1): p=12+10+1=23, s=4
    assert_eq!(meta(&b, "r"), (23, 4));
    assert_eq!(vals(&b, "r"), s(&["11.3925"]));
}

// =====================================================================
// 7. NULL operands
// =====================================================================

#[tokio::test]
async fn null_propagates_through_add() {
    let sm = null_sm();
    let b = run(&sm, "SELECT a + b AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (11, 2));
    assert_eq!(
        vals(&b, "r"),
        vec![None, None, None, Some("7.00".to_string())]
    );
}

#[tokio::test]
async fn null_propagates_through_sub() {
    let sm = null_sm();
    let b = run(&sm, "SELECT a - b AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        vec![None, None, None, Some("-1.00".to_string())]
    );
}

#[tokio::test]
async fn null_propagates_through_mul() {
    let sm = null_sm();
    let b = run(&sm, "SELECT a * b AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (21, 4));
    assert_eq!(
        vals(&b, "r"),
        vec![None, None, None, Some("12.0000".to_string())]
    );
}

#[tokio::test]
async fn null_propagates_through_div() {
    let sm = null_sm();
    let b = run(&sm, "SELECT a / b AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (28, 18));
    assert_eq!(
        vals(&b, "r"),
        vec![None, None, None, Some("0.750000000000000000".to_string())]
    );
}

#[tokio::test]
async fn null_propagates_through_mod() {
    let sm = null_sm();
    let b = run(&sm, "SELECT a % b AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (10, 2));
    assert_eq!(
        vals(&b, "r"),
        vec![None, None, None, Some("3.00".to_string())]
    );
}

#[tokio::test]
async fn null_arithmetic_still_carries_decimal_arb_metadata() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[col("a", 10, 2, &[None]), col("b", 10, 2, &[None])],
    );
    for (sql, want) in [
        ("SELECT a + b AS r FROM t", (11u32, 2u32)),
        ("SELECT a - b AS r FROM t", (11, 2)),
        ("SELECT a * b AS r FROM t", (21, 4)),
        ("SELECT a / b AS r FROM t", (28, 18)),
        ("SELECT a % b AS r FROM t", (10, 2)),
    ] {
        let b = run(&sm, sql).await;
        assert_eq!(meta(&b, "r"), want, "metadata dropped for `{sql}`");
        assert_eq!(vals(&b, "r"), vec![None], "`{sql}` must yield NULL");
    }
}

#[tokio::test]
async fn all_null_column_arithmetic_does_not_error() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            col("a", 10, 2, &[None, None, None]),
            colv("b", 10, 2, &["0.00", "1.00", "-1.00"]),
        ],
    );
    let b = run(&sm, "SELECT a / b AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        vec![None, None, None],
        "an all-NULL dividend must not trip the zero-divisor check"
    );
}

#[tokio::test]
async fn null_inside_a_chained_expression_propagates_to_the_end() {
    let sm = null_sm();
    let b = run(&sm, "SELECT ((a + b) * 3) - 1 AS r FROM t").await;
    let v = vals(&b, "r");
    assert_eq!(v[0], None);
    assert_eq!(v[1], None);
    assert_eq!(v[2], None);
    assert_eq!(v[3], Some("20.00".to_string()), "(3+4)*3 - 1 = 20");
}

#[tokio::test]
async fn null_operand_with_integer_literal_propagates() {
    let sm = null_sm();
    let b = run(&sm, "SELECT a + 10 AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        vec![
            Some("11.25".to_string()),
            None,
            None,
            Some("13.00".to_string())
        ]
    );
}

// =====================================================================
// 8. Metadata survival in the surrounding query shapes
// =====================================================================

#[tokio::test]
async fn arithmetic_inside_a_subquery_keeps_metadata_and_value() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT r FROM (SELECT a + b AS r FROM t) x").await;
    assert_eq!(meta(&b, "r"), (21, 2));
    assert_eq!(vals(&b, "r"), s(&["10.00", "4.00", "-4.00", "-10.00"]));
}

#[tokio::test]
async fn arithmetic_feeding_case_keeps_metadata() {
    let sm = signs_sm();
    let b = run(
        &sm,
        "SELECT CASE WHEN a > b THEN a - b ELSE b - a END AS r FROM t",
    )
    .await;
    assert_eq!(
        meta(&b, "r"),
        (21, 2),
        "CASE over two identically-typed arithmetic branches must keep (21,2)"
    );
    assert_eq!(vals(&b, "r"), s(&["4.00", "10.00", "10.00", "4.00"]));
}

#[tokio::test]
async fn coalesce_over_arithmetic_keeps_metadata() {
    let sm = null_sm();
    let b = run(&sm, "SELECT COALESCE(a + b, b + a) AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (11, 2));
    assert_eq!(
        vals(&b, "r"),
        vec![None, None, None, Some("7.00".to_string())]
    );
}

#[tokio::test]
async fn arithmetic_in_where_filters_on_the_computed_value() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a AS r FROM t WHERE a + b > 0").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["7.00", "7.00"]),
        "only the rows whose sum is positive survive"
    );
}

#[tokio::test]
async fn arithmetic_result_compares_against_an_integer_literal() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a AS r FROM t WHERE a * b = 21").await;
    assert_eq!(vals(&b, "r"), s(&["7.00", "-7.00"]));
}

#[tokio::test]
async fn arithmetic_result_compares_against_another_arithmetic_result() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a AS r FROM t WHERE a + b > a - b").await;
    // a+b > a-b  <=>  b > 0  -> rows 0 and 2
    assert_eq!(vals(&b, "r"), s(&["7.00", "-7.00"]));
}

#[tokio::test]
async fn group_by_an_arithmetic_result_groups_numerically() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("a", 10, 2, &["1.00", "1.0", "2.00", "-1.00"]),
            colv("b", 10, 2, &["1.00", "1.00", "0.00", "1.00"]),
        ],
    );
    let b = run(&sm, "SELECT a + b AS r FROM t GROUP BY a + b").await;
    let mut got: Vec<String> = vals(&b, "r").into_iter().map(|v| v.unwrap()).collect();
    got.sort();
    assert_eq!(
        got,
        vec!["0".to_string(), "2.00".to_string()],
        "1.00+1.00, 1.0+1.00 and 2.00+0.00 must collapse into ONE group (2.00): \
         numerically-equal sums written at different scales must not split the \
         grouping key"
    );
}

#[tokio::test]
async fn order_by_an_arithmetic_result_is_numeric_across_signs() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("a", 10, 2, &["-5.00", "3.00", "-1.00"]),
            colv("b", 10, 2, &["1.00", "1.00", "1.00"]),
        ],
    );
    let b = run(&sm, "SELECT a + b AS r FROM t ORDER BY a + b").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["-4.00", "0", "4.00"]),
        "ORDER BY over an arithmetic decimal_arb result must sort numerically, \
         not bytewise (0xFF sign byte would push negatives last)"
    );
}

#[tokio::test]
async fn arithmetic_output_column_name_alias_does_not_lose_metadata() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a + b AS a_totally_different_name FROM t").await;
    assert_eq!(meta(&b, "a_totally_different_name"), (21, 2));
}

#[tokio::test]
async fn two_arithmetic_columns_in_one_projection_keep_distinct_types() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a + b AS s1, a * b AS s2 FROM t").await;
    assert_eq!(meta(&b, "s1"), (21, 2));
    assert_eq!(meta(&b, "s2"), (41, 4));
    assert_eq!(vals(&b, "s1"), s(&["10.00", "4.00", "-4.00", "-10.00"]));
    assert_eq!(
        vals(&b, "s2"),
        s(&["21.0000", "-21.0000", "-21.0000", "21.0000"])
    );
}

#[tokio::test]
async fn arithmetic_over_an_empty_result_set_returns_no_rows_and_keeps_type() {
    let sm = signs_sm();
    let b = run(&sm, "SELECT a + b AS r FROM t WHERE a > 1000000").await;
    let rows: usize = b.iter().map(|x| x.num_rows()).sum();
    assert_eq!(rows, 0, "no row satisfies the filter");
    // DataFusion may or may not emit an empty batch; when it does, the batch
    // must still carry the decimal_arb type (the F1 empty-batch path).
    if !b.is_empty() {
        assert_eq!(
            meta(&b, "r"),
            (21, 2),
            "an empty batch must still carry the widened decimal_arb type"
        );
        assert_eq!(vals(&b, "r"), Vec::<Option<String>>::new());
    }
}

// =====================================================================
// 9. Boundaries, chaining and large magnitudes
// =====================================================================

fn boundary_sm() -> SessionManager {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("a", 20, 18, &["99.999999999999999999"]),
            colv("b", 20, 18, &["0.000000000000000001"]),
        ],
    );
    sm
}

#[tokio::test]
async fn div_at_the_exact_precision_boundary_fits() {
    let sm = boundary_sm();
    let b = run(&sm, "SELECT a / b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (38, 18),
        "p=(20-18)+18+18=38 -> 20 integer digits"
    );
    assert_eq!(
        vals(&b, "r"),
        s(&["99999999999999999999.000000000000000000"]),
        "the maximum quotient must fit EXACTLY in the declared integer digits"
    );
}

#[tokio::test]
async fn chained_div_stays_inside_the_declared_precision() {
    let sm = boundary_sm();
    let b = run(&sm, "SELECT (a / b) / b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (56, 18),
        "p=(38-18)+18+18=56 -> 38 integer digits"
    );
    let want = format!("{}{}.{}", rep('9', 20), rep('0', 18), rep('0', 18));
    assert_eq!(
        vals(&b, "r"),
        vec![Some(want)],
        "the second division must remain exact and inside the widened precision"
    );
}

#[tokio::test]
async fn mul_of_extreme_scales_is_exact() {
    let sm = boundary_sm();
    let b = run(&sm, "SELECT a * b AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (41, 36), "p=20+20+1=41, s=min(36,41)=36");
    let want = format!("0.{}{}", rep('0', 16), rep('9', 20));
    assert_eq!(vals(&b, "r"), vec![Some(want)]);
}

#[tokio::test]
async fn add_at_extreme_scale_carries_into_a_new_integer_digit() {
    let sm = boundary_sm();
    let b = run(&sm, "SELECT a + b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (21, 18),
        "p=max(2,2)+18+1=21 -> 3 integer digits"
    );
    assert_eq!(vals(&b, "r"), s(&["100.000000000000000000"]));
}

#[tokio::test]
async fn sub_at_extreme_scale_is_exact_in_the_last_digit() {
    let sm = boundary_sm();
    let b = run(&sm, "SELECT a - b AS r FROM t").await;
    assert_eq!(vals(&b, "r"), s(&["99.999999999999999998"]));
}

#[tokio::test]
async fn add_carry_into_a_new_integer_digit_fits_the_widened_precision() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[colv("a", 5, 2, &["999.99"]), colv("b", 5, 2, &["0.01"])],
    );
    let b = run(&sm, "SELECT a + b AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (6, 2),
        "the +1 in the add rule buys the carry digit"
    );
    assert_eq!(vals(&b, "r"), s(&["1000.00"]));
}

fn big_sm() -> SessionManager {
    let sm = manager();
    let v = format!("1{}", rep('0', 99));
    register(&sm, "t", &[colv("a", 100, 0, &[&v])]);
    sm
}

#[tokio::test]
async fn large_integer_add_is_exact() {
    let sm = big_sm();
    let b = run(&sm, "SELECT a + a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (101, 0));
    assert_eq!(vals(&b, "r"), vec![Some(format!("2{}", rep('0', 99)))]);
}

#[tokio::test]
async fn large_integer_mul_is_exact() {
    let sm = big_sm();
    let b = run(&sm, "SELECT a * a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (201, 0), "p=100+100+1");
    assert_eq!(vals(&b, "r"), vec![Some(format!("1{}", rep('0', 198)))]);
}

#[tokio::test]
async fn chained_mul_precision_keeps_growing_and_stays_exact() {
    let sm = big_sm();
    let b = run(&sm, "SELECT a * a * a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (302, 0), "p=(201)+100+1");
    assert_eq!(vals(&b, "r"), vec![Some(format!("1{}", rep('0', 297)))]);
}

#[tokio::test]
async fn large_integer_self_subtraction_is_zero() {
    let sm = big_sm();
    let b = run(&sm, "SELECT a - a AS r FROM t").await;
    assert_eq!(vals(&b, "r"), s(&["0"]));
    assert_eq!(raw_bytes(&b, "r", 0), vec![0x00u8]);
}

#[tokio::test]
async fn large_integer_self_division_is_one() {
    let sm = big_sm();
    let b = run(&sm, "SELECT a / a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (118, 18));
    assert_eq!(vals(&b, "r"), s(&["1.000000000000000000"]));
}

#[tokio::test]
async fn large_integer_self_modulo_is_zero() {
    let sm = big_sm();
    let b = run(&sm, "SELECT a % a AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (100, 0));
    assert_eq!(vals(&b, "r"), s(&["0"]));
}

#[tokio::test]
async fn hundred_digit_value_divided_by_a_small_integer_keeps_all_digits() {
    let sm = big_sm();
    let b = run(&sm, "SELECT a / 3 AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (118, 18),
        "integer literal is decimal_arb(20,0): s=max(0,18)=18, p=(100-0)+0+18=118"
    );
    let want = format!("{}.{}", rep('3', 99), rep('3', 18));
    assert_eq!(
        vals(&b, "r"),
        vec![Some(want)],
        "1e99/3 must keep 99 integer digits AND 18 fractional digits"
    );
}

#[tokio::test]
async fn arithmetic_on_a_column_whose_declared_precision_is_too_small_errors_cleanly_for_add() {
    let sm = manager();
    let v = format!("1{}", rep('0', 50));
    register_mismatched(&sm, "t", &colv("a", 100, 0, &[&v]), 20);
    let e = expect_err(&sm, "SELECT a + a AS r FROM t").await;
    assert!(
        !e.contains("panicked"),
        "malformed upstream precision must produce a typed error, not a panic: {e}"
    );
    assert!(
        e.contains("integer digit") || e.to_lowercase().contains("exceeds"),
        "the error should name the precision overflow: {e}"
    );
}

#[tokio::test]
async fn arithmetic_on_a_column_whose_declared_precision_is_too_small_errors_cleanly_for_mul() {
    let sm = manager();
    let v = format!("1{}", rep('0', 50));
    register_mismatched(&sm, "t", &colv("a", 100, 0, &[&v]), 20);
    let e = expect_err(&sm, "SELECT a * a AS r FROM t").await;
    assert!(!e.contains("panicked"), "must not panic: {e}");
}

#[tokio::test]
async fn a_column_whose_declared_precision_is_too_small_still_projects() {
    let sm = manager();
    let v = format!("1{}", rep('0', 50));
    register_mismatched(&sm, "t", &colv("a", 100, 0, &[&v]), 20);
    let b = run(&sm, "SELECT a AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        vec![Some(v)],
        "a plain projection must not silently rewrite the value"
    );
}

// =====================================================================
// 10. Division precision cap (findings)
// =====================================================================

/// `a` = 1e90 at (100,0); divisors of the same magnitude but different
/// declared scales.
fn jb_sm() -> SessionManager {
    let sm = manager();
    let a = format!("1{}", rep('0', 90));
    let a2 = format!("1{}", rep('0', 70));
    let b18 = format!("0.{}3", rep('0', 17));
    let b30 = format!("0.{}3", rep('0', 29));
    register(
        &sm,
        "t",
        &[
            colv("a", 100, 0, &[&a]),
            colv("a2", 100, 0, &[&a2]),
            colv("b0", 100, 0, &["3"]),
            colv("b18", 100, 18, &[&b18]),
            colv("b30", 100, 30, &[&b30]),
        ],
    );
    sm
}

#[tokio::test]
async fn div_of_a_huge_value_by_a_scale_zero_divisor_keeps_every_declared_digit() {
    // CONTROL for the three tests below: same quotient magnitude, divisor scale
    // equals dividend scale -> the result is exact to the declared scale 18.
    let sm = jb_sm();
    let b = run(&sm, "SELECT a / b0 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (118, 18), "p=(100-0)+0+18=118");
    let want = format!("{}.{}", rep('3', 90), rep('3', 18));
    assert_eq!(vals(&b, "r"), vec![Some(want)]);
}

#[tokio::test]
#[ignore = "FINDING: decimal_arb division silently zeroes the declared fractional \
            digits when the divisor's scale exceeds the dividend's and the quotient \
            needs >100 significant digits (bigdecimal DEFAULT_PRECISION cap)"]
async fn div_by_a_higher_scale_divisor_must_not_zero_the_fractional_digits() {
    let sm = jb_sm();
    let b = run(&sm, "SELECT a / b18 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (136, 18), "p=(100-0)+18+18=136");
    // 1e90 / 3e-18 = 1e108/3 = 333...3.333333333333333333 (108 integer threes)
    let want = format!("{}.{}", rep('3', 108), rep('3', 18));
    assert_eq!(
        vals(&b, "r"),
        vec![Some(want)],
        "the declared scale-18 fractional digits must be real digits, not zeros"
    );
}

#[tokio::test]
#[ignore = "FINDING: decimal_arb division truncates to 100 significant digits, so a \
            divisor whose scale exceeds the dividend's by more than 18 zeroes real \
            INTEGER digits of the quotient"]
async fn div_by_a_scale_30_divisor_must_not_zero_integer_digits() {
    let sm = jb_sm();
    let b = run(&sm, "SELECT a / b30 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (148, 18), "p=(100-0)+30+18=148");
    // 1e90 / 3e-30 = 1e120/3 = 120 integer threes, then .333...
    let want = format!("{}.{}", rep('3', 120), rep('3', 18));
    assert_eq!(
        vals(&b, "r"),
        vec![Some(want)],
        "the last 12 integer digits must be 3s, not zeros"
    );
}

#[tokio::test]
#[ignore = "FINDING: decimal_arb division loses the tail of the declared scale as soon \
            as (integer digits + scale) exceeds bigdecimal's 100-digit division cap"]
async fn div_partial_loss_at_the_hundred_significant_digit_cap() {
    let sm = jb_sm();
    let b = run(&sm, "SELECT a2 / b18 AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (136, 18));
    // 1e70 / 3e-18 = 1e88/3 -> 88 integer threes, then 18 fractional threes.
    let want = format!("{}.{}", rep('3', 88), rep('3', 18));
    assert_eq!(
        vals(&b, "r"),
        vec![Some(want)],
        "only the first 100 significant digits survive; the last 6 fractional \
         digits come back as zeros"
    );
}

#[tokio::test]
#[ignore = "FINDING: the quotient depends on the DIVISOR COLUMN'S DECLARED SCALE — \
            dividing by decimal_arb(100,0) `3` and by decimal_arb(100,18) \
            `3.000000000000000000` (numerically identical) yields different results"]
async fn div_result_must_not_depend_on_the_divisor_declared_scale() {
    let sm = manager();
    let a = format!("1{}", rep('0', 90));
    register(
        &sm,
        "t",
        &[
            colv("a", 100, 0, &[&a]),
            colv("d0", 100, 0, &["3"]),
            colv("d18", 100, 18, &["3.000000000000000000"]),
        ],
    );
    let l = run(&sm, "SELECT a / d0 AS r FROM t").await;
    let r = run(&sm, "SELECT a / d18 AS r FROM t").await;
    assert_eq!(meta(&l, "r").1, 18, "both quotients are declared scale 18");
    assert_eq!(meta(&r, "r").1, 18);
    assert_eq!(
        vals(&l, "r"),
        vals(&r, "r"),
        "d0 and d18 hold the SAME number (3); the quotient must not change just \
         because the divisor column declares a different storage scale"
    );
}

#[tokio::test]
async fn div_precision_loss_is_absent_for_ordinary_magnitudes() {
    // Same operand scales as the failing cases, but a quotient well under the
    // 100-significant-digit cap: this must be exact.
    let sm = manager();
    let b18 = format!("0.{}3", rep('0', 17));
    register(
        &sm,
        "t",
        &[colv("small", 100, 0, &["5"]), colv("b18", 100, 18, &[&b18])],
    );
    let b = run(&sm, "SELECT small / b18 AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        vec![Some(format!("1{}.{}", rep('6', 18), rep('6', 17) + "7"))],
        "5 / 3e-18 = 1.666...e18 must be exact to the declared scale"
    );
}

// =====================================================================
// 11. Unary minus and misc operator surface
// =====================================================================

#[tokio::test]
#[ignore = "FINDING: SQL unary minus over a decimal_arb column fails to plan \
            (\"Negation only supports numeric, interval and timestamp types\") even \
            though a decimal_arb_neg UDF exists — DecimalArbExprPlanner only \
            implements plan_binary_op, so `SELECT -amount` kills the pipeline"]
async fn unary_minus_on_a_decimal_arb_column() {
    let sm = manager();
    register(&sm, "t", &[colv("a", 10, 2, &["1.25", "-1.25", "0.00"])]);
    match try_run(&sm, "SELECT -a AS r FROM t").await {
        Ok(b) => {
            assert_eq!(meta(&b, "r"), (10, 2), "unary minus must preserve (p,s)");
            assert_eq!(vals(&b, "r"), s(&["-1.25", "1.25", "0"]));
        }
        Err(e) => panic!(
            "unary minus over decimal_arb is not supported (a decimal_arb_neg UDF \
             exists but nothing binds SQL unary `-` to it): {e}"
        ),
    }
}

#[tokio::test]
async fn float_operand_is_rejected_not_silently_coerced() {
    let sm = manager();
    register(&sm, "t", &[colv("a", 10, 2, &["1.25"])]);
    // 1.5 parses as Float64 by default; float x decimal is lossy and must not
    // be auto-coerced (FR-013).
    let e = expect_err(&sm, "SELECT a + 1.5 AS r FROM t").await;
    assert!(
        !e.contains("panicked"),
        "a float operand must yield a typed planning error, never a panic: {e}"
    );
}

/// (JOIN / comma-join SQL cannot reach the planner at all — the streamling
/// bigint SQL preprocessor rejects it with "JOIN queries not supported" — so
/// cross-column arithmetic is exercised within a single table.)
#[tokio::test]
async fn add_precision_is_driven_by_the_wider_integer_part() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("wide", 30, 2, &["1.25"]),
            colv("narrow", 8, 2, &["2.50"]),
        ],
    );
    let b = run(&sm, "SELECT wide + narrow AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (31, 2),
        "s=max(2,2)=2, p=max(30-2, 8-2)+2+1 = 28+2+1 = 31"
    );
    assert_eq!(vals(&b, "r"), s(&["3.75"]));
}

#[tokio::test]
async fn div_precision_is_driven_by_the_left_integer_part_and_right_scale() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("wide", 30, 2, &["1.25"]),
            colv("narrow", 8, 2, &["2.50"]),
        ],
    );
    let l = run(&sm, "SELECT wide / narrow AS r FROM t").await;
    let r = run(&sm, "SELECT narrow / wide AS r FROM t").await;
    assert_eq!(meta(&l, "r"), (48, 18), "p=(30-2)+2+18=48");
    assert_eq!(meta(&r, "r"), (26, 18), "p=(8-2)+2+18=26");
    assert_eq!(vals(&l, "r"), s(&["0.500000000000000000"]));
    assert_eq!(vals(&r, "r"), s(&["2.000000000000000000"]));
}

#[tokio::test]
async fn repeated_subtraction_reaches_exact_zero() {
    let sm = manager();
    register(&sm, "t", &[colv("a", 12, 4, &["0.0001"])]);
    let b = run(&sm, "SELECT ((a + a) + a) - (a + (a + a)) AS r FROM t").await;
    assert_eq!(
        vals(&b, "r"),
        s(&["0"]),
        "different groupings of the same sum must cancel exactly"
    );
}

#[tokio::test]
async fn modulo_and_division_agree_with_the_euclid_identity() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("a", 20, 0, &["17", "-17", "17", "-17"]),
            colv("b", 20, 0, &["5", "5", "-5", "-5"]),
        ],
    );
    let q = run(&sm, "SELECT a % b AS r FROM t").await;
    assert_eq!(
        vals(&q, "r"),
        s(&["2", "-2", "2", "-2"]),
        "truncated-division remainder: sign follows the dividend"
    );
    let recon = run(&sm, "SELECT (a - (a % b)) % b AS r FROM t").await;
    assert!(
        vals(&recon, "r").iter().all(|v| v.as_deref() == Some("0")),
        "a - (a % b) must be an exact multiple of b"
    );
}

#[tokio::test]
async fn modulo_of_a_fractional_pair_is_exact() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[colv("a", 10, 1, &["7.5"]), colv("b", 10, 1, &["2.2"])],
    );
    let b = run(&sm, "SELECT a % b AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (10, 1), "s=1, p=min(9,9)+1=10");
    assert_eq!(vals(&b, "r"), s(&["0.9"]), "7.5 - 3*2.2 = 0.9");
}

#[tokio::test]
async fn tiny_negative_quotient_rounds_to_canonical_zero_not_negative_zero() {
    let sm = manager();
    register(
        &sm,
        "t",
        &[
            colv("n", 20, 0, &["-5", "-15", "-25"]),
            colv("d", 20, 0, &["10000000000000000000"; 3]),
        ],
    );
    let b = run(&sm, "SELECT n / d AS r FROM t").await;
    assert_eq!(meta(&b, "r"), (38, 18));
    let v = vals(&b, "r");
    assert_eq!(
        v[0],
        Some("0".to_string()),
        "-5e-19 is an exact half of the last representable unit; half-even \
         rounds to zero and must encode as canonical +0"
    );
    assert_eq!(
        raw_bytes(&b, "r", 0),
        vec![0x00u8],
        "rounding a negative to zero must NOT emit a 0xFF negative-zero encoding"
    );
    assert_eq!(
        v[1],
        Some("-0.000000000000000002".to_string()),
        "-1.5 ULP is a tie: half-even picks the even neighbour (-2)"
    );
    assert_eq!(
        v[2],
        Some("-0.000000000000000002".to_string()),
        "-2.5 ULP is a tie: half-even picks the even neighbour (-2)"
    );
}

#[tokio::test]
async fn division_result_feeding_further_arithmetic_keeps_the_rounded_value() {
    let sm = div_sm();
    let b = run(&sm, "SELECT (n / d) * 3 AS r FROM t").await;
    let v = vals(&b, "r");
    // (1/3 rounded to 18) * 3 = 0.999999999999999999, NOT 1 — rounding is
    // committed at the division, exactly once.
    assert_eq!(
        v[0],
        Some("0.999999999999999999".to_string()),
        "the rounding must be applied once, at the division, and then be exact"
    );
}

#[tokio::test]
async fn scale_of_a_chained_division_does_not_grow_past_max_of_left_and_18() {
    let sm = div_sm();
    let b = run(&sm, "SELECT (n / d) / d AS r FROM t").await;
    assert_eq!(
        meta(&b, "r"),
        (48, 18),
        "left operand is (48,18): s=max(18,18)=18, p=(48-18)+0+18=48 — the scale \
         must NOT accumulate 18 per division"
    );
    // 1/3/3 == 1/9 == 0.111111111111111111 (the intermediate was already
    // rounded to 18, so this is 0.333333333333333333 / 3).
    assert_eq!(
        vals(&b, "r")[0],
        Some("0.111111111111111111".to_string()),
        "chained division must round exactly once per division"
    );
}
