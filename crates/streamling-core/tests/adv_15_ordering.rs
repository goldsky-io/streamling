//! Adversarial coverage for **ordering and comparison correctness** over the
//! `streamling.decimal_arb` extension type.
//!
//! `decimal_arb` sorts through a hand-written sort-key rewrite
//! (`DecimalArbSortRewriteRule` → `decimal_arb_to_sort_key`) over the canonical
//! `[sign][BE magnitude]` bytes, because a plain bytewise sort of those bytes is
//! numerically wrong. That makes lexicographic-vs-numeric divergence the prime
//! suspect, so every corpus here is chosen so that **byte order and numeric
//! order actively disagree**:
//!
//!   * `-1000` vs `-9`   — the more-negative value has the *larger* first
//!     magnitude byte and a *longer* magnitude.
//!   * `9` vs `100`      — the smaller value has the larger first byte.
//!   * `0.5` vs `0.05`   — same digits, different magnitudes.
//!   * `255` vs `256`, `65535` vs `65536`, `2^64-1` vs `2^64` — the exact
//!     points where the canonical magnitude gains a byte.
//!
//! Probed surfaces:
//!   * `decimal_arb_to_sort_key` as a pure function — the total-order property
//!     (`bytewise_cmp(key(a), key(b)) == numeric_cmp(a, b)`) proved over full
//!     cross-products, plus the key's structural invariants.
//!   * the six comparison UDFs (`decimal_arb_{eq,neq,lt,lte,gt,gte}`) invoked
//!     directly — reflexivity, irreflexivity, antisymmetry, totality,
//!     transitivity, three-valued NULL logic, scalar broadcast, cross-scale
//!     operands.
//!   * SQL `ORDER BY` through the full `SessionManager` stack — ASC/DESC,
//!     NULLS FIRST/LAST, multi-column and mixed-direction sorts, `LIMIT`
//!     (top-k), `ORDER BY` over an expression, `DISTINCT` + `ORDER BY`, and
//!     ordering across batch boundaries.
//!   * the **consistency triangle**: SQL row order, the comparison UDFs, and
//!     `DecimalArbValue::cmp` must all agree on the same corpus. A disagreement
//!     between any two is a silent data-corruption bug even if each looks
//!     self-consistent.
//!
//! Where a decimal_arb column reaches the output, the value assertion is paired
//! with a metadata assertion: a sort that returns the right rows but drops
//! `streamling.decimal_arb` from the output field silently corrupts the column
//! at the sink (F2's shape).

use std::cmp::Ordering;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, LargeBinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;

use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::functions::decimal_arb_ops::{
    DecimalArbEqFunc, DecimalArbGtFunc, DecimalArbGteFunc, DecimalArbLtFunc, DecimalArbLteFunc,
    DecimalArbNeqFunc,
};
use streamling_core::session::SessionManager;
use streamling_core::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, decimal_arb_to_sort_key,
};

// =====================================================================
// Corpora — every one has byte order != numeric order somewhere
// =====================================================================

/// `(id, text)` at scale 4. Hand-verified ascending order below.
const CORPUS_S4: &[(i64, &str)] = &[
    (1, "-1000"),
    (2, "-9"),
    (3, "9"),
    (4, "100"),
    (5, "0.5"),
    (6, "0.05"),
    (7, "0"),
    (8, "-0.05"),
    (9, "-0.5"),
    (10, "-100"),
    (11, "1000"),
    (12, "-9999.9999"),
    (13, "9999.9999"),
    (14, "6.5535"),
    (15, "6.5536"),
];

/// Hand-computed numeric ascending order of `CORPUS_S4` ids:
/// -9999.9999 < -1000 < -100 < -9 < -0.5 < -0.05 < 0 < 0.05 < 0.5
///            < 6.5535 < 6.5536 < 9 < 100 < 1000 < 9999.9999
const CORPUS_S4_ASC: &[i64] = &[12, 1, 10, 2, 9, 8, 7, 6, 5, 14, 15, 3, 4, 11, 13];

/// `(id, text)` at scale 0, sitting on the byte-length boundaries.
const CORPUS_S0: &[(i64, &str)] = &[
    (1, "-1000"),
    (2, "-9"),
    (3, "9"),
    (4, "100"),
    (5, "0"),
    (6, "255"),
    (7, "256"),
    (8, "-255"),
    (9, "-256"),
    (10, "65535"),
    (11, "65536"),
    (12, "-65536"),
    (13, "1"),
    (14, "-1"),
];

/// -65536 < -1000 < -256 < -255 < -9 < -1 < 0 < 1 < 9 < 100 < 255 < 256
///        < 65535 < 65536
const CORPUS_S0_ASC: &[i64] = &[12, 1, 9, 8, 2, 14, 5, 13, 3, 4, 6, 7, 10, 11];

/// Power-of-two magnitude boundaries at scale 0 (precision 100).
const CORPUS_P2: &[(i64, &str)] = &[
    (1, "18446744073709551616"),  // 2^64
    (2, "18446744073709551615"),  // 2^64 - 1
    (3, "-18446744073709551616"), // -2^64
    (4, "-18446744073709551615"), // -(2^64 - 1)
    (5, "0"),
    (6, "1"),
    (7, "-1"),
    (8, "340282366920938463463374607431768211456"), // 2^128
    (9, "-340282366920938463463374607431768211456"), // -2^128
];

/// -2^128 < -2^64 < -(2^64-1) < -1 < 0 < 1 < 2^64-1 < 2^64 < 2^128
const CORPUS_P2_ASC: &[i64] = &[9, 3, 4, 7, 5, 6, 2, 1, 8];

// =====================================================================
// Harness
// =====================================================================

fn arb_array(name: &str, p: u32, s: u32, vals: &[Option<&str>]) -> ArrayRef {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), name, p, s).unwrap();
    for v in vals {
        match v {
            Some(x) => b
                .append_value(&DecimalArbValue::from_str(x).unwrap())
                .unwrap_or_else(|e| panic!("`{x}` should fit decimal_arb({p},{s}): {e}")),
            None => b.append_null(),
        }
    }
    let (raw, _, _) = b.finish().into_inner();
    Arc::new(raw)
}

fn arb_storage(p: u32, s: u32, vals: &[Option<&str>]) -> LargeBinaryArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), "x", p, s).unwrap();
    for v in vals {
        match v {
            Some(x) => b.append_str(x).unwrap(),
            None => b.append_null(),
        }
    }
    b.finish().into_inner().0
}

fn int_array(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}

fn val(text: &str) -> DecimalArbValue {
    DecimalArbValue::from_str(text).unwrap()
}

/// The sort key a `decimal_arb(_, scale)` column would produce for `text`.
fn key_of(text: &str, scale: u32) -> Vec<u8> {
    decimal_arb_to_sort_key(&val(text).to_canonical_bytes_at_scale(scale))
}

/// The raw canonical bytes (what a naive bytewise sort would compare).
fn canon(text: &str, scale: u32) -> Vec<u8> {
    val(text).to_canonical_bytes_at_scale(scale)
}

fn ids_of(corpus: &[(i64, &str)]) -> Vec<i64> {
    corpus.iter().map(|(i, _)| *i).collect()
}

fn texts_of<'a>(corpus: &[(i64, &'a str)]) -> Vec<Option<&'a str>> {
    corpus.iter().map(|(_, t)| Some(*t)).collect()
}

fn text_for<'a>(corpus: &[(i64, &'a str)], id: i64) -> &'a str {
    corpus.iter().find(|(i, _)| *i == id).unwrap().1
}

/// A `SessionContext` carrying the full decimal_arb stack plus every table the
/// SQL sections below use. All tables are single-partition so that the sorts
/// under test are not confounded by partition-merge behaviour.
fn session() -> SessionContext {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
    let ctx = sm.session_context();

    // c(id, v decimal_arb(40,4))  — CORPUS_S4
    let c_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 40, 4, true).unwrap(),
    ]));
    let c = RecordBatch::try_new(
        c_schema,
        vec![
            int_array(&ids_of(CORPUS_S4)),
            arb_array("v", 40, 4, &texts_of(CORPUS_S4)),
        ],
    )
    .unwrap();
    ctx.register_batch("c", c).unwrap();

    // z(id, v decimal_arb(40,0))  — CORPUS_S0
    let z_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 40, 0, true).unwrap(),
    ]));
    let z = RecordBatch::try_new(
        z_schema,
        vec![
            int_array(&ids_of(CORPUS_S0)),
            arb_array("v", 40, 0, &texts_of(CORPUS_S0)),
        ],
    )
    .unwrap();
    ctx.register_batch("z", z).unwrap();

    // p(id, v decimal_arb(100,0))  — CORPUS_P2
    let p_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 100, 0, true).unwrap(),
    ]));
    let p = RecordBatch::try_new(
        p_schema,
        vec![
            int_array(&ids_of(CORPUS_P2)),
            arb_array("v", 100, 0, &texts_of(CORPUS_P2)),
        ],
    )
    .unwrap();
    ctx.register_batch("p", p).unwrap();

    // n(id, v decimal_arb(40,4) with NULLs)
    let n_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 40, 4, true).unwrap(),
    ]));
    let n = RecordBatch::try_new(
        n_schema,
        vec![
            int_array(&[1, 2, 3, 4, 5]),
            arb_array("v", 40, 4, &[Some("5"), None, Some("-5"), None, Some("0")]),
        ],
    )
    .unwrap();
    ctx.register_batch("n", n).unwrap();

    // m(id, a decimal_arb(40,4), b decimal_arb(40,4), g Int64)
    let m_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("a", 40, 4, true).unwrap(),
        DecimalArbType::field("b", 40, 4, true).unwrap(),
        Field::new("g", DataType::Int64, false),
    ]));
    let m = RecordBatch::try_new(
        m_schema,
        vec![
            int_array(&[1, 2, 3, 4, 5, 6]),
            arb_array(
                "a",
                40,
                4,
                &[
                    Some("-1000"),
                    Some("-1000"),
                    Some("9"),
                    Some("9"),
                    Some("100"),
                    Some("-9"),
                ],
            ),
            arb_array(
                "b",
                40,
                4,
                &[
                    Some("5"),
                    Some("-5"),
                    Some("1"),
                    Some("-1"),
                    Some("0"),
                    Some("3"),
                ],
            ),
            int_array(&[1, 1, 2, 2, 1, 2]),
        ],
    )
    .unwrap();
    ctx.register_batch("m", m).unwrap();

    // d(id, v decimal_arb(40,4)) with numerically duplicated values, for DISTINCT
    let d_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 40, 4, true).unwrap(),
    ]));
    let d = RecordBatch::try_new(
        d_schema,
        vec![
            int_array(&[1, 2, 3, 4, 5, 6, 7]),
            arb_array(
                "v",
                40,
                4,
                &[
                    Some("-1000"),
                    Some("-1000.0000"),
                    Some("9"),
                    Some("9.0"),
                    Some("-9"),
                    Some("0.05"),
                    Some("0.0500"),
                ],
            ),
        ],
    )
    .unwrap();
    ctx.register_batch("d", d).unwrap();

    // mb: CORPUS_S4 split across three batches inside ONE partition, so the
    // sort has to order across batch boundaries.
    let mb_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 40, 4, true).unwrap(),
    ]));
    let mut batches = Vec::new();
    for chunk in CORPUS_S4.chunks(5) {
        let ids: Vec<i64> = chunk.iter().map(|(i, _)| *i).collect();
        let txt: Vec<Option<&str>> = chunk.iter().map(|(_, t)| Some(*t)).collect();
        batches.push(
            RecordBatch::try_new(
                mb_schema.clone(),
                vec![int_array(&ids), arb_array("v", 40, 4, &txt)],
            )
            .unwrap(),
        );
    }
    ctx.register_table(
        "mb",
        Arc::new(MemTable::try_new(mb_schema, vec![batches]).unwrap()),
    )
    .unwrap();

    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> (SchemaRef, Vec<RecordBatch>) {
    let df = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("PLANNING FAILED for `{sql}`: {e}"));
    let logical: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("EXECUTION FAILED for `{sql}`: {e} / {e:?}"));
    let schema = batches.first().map(|b| b.schema()).unwrap_or(logical);
    (schema, batches)
}

fn field_of(schema: &SchemaRef, name: &str) -> Field {
    schema
        .field_with_name(name)
        .unwrap_or_else(|_| {
            panic!(
                "output has no column `{name}` (columns: {:?})",
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )
        })
        .clone()
}

fn assert_arb_meta(schema: &SchemaRef, name: &str, p: u32, s: u32, ctx_msg: &str) {
    let f = field_of(schema, name);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "{ctx_msg}: output column `{name}` LOST streamling.decimal_arb metadata \
         (a sink would treat it as raw BYTEA); field = {f:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((p, s)),
        "{ctx_msg}: output column `{name}` has the wrong (precision, scale)"
    );
}

fn ints(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let c = b
            .column_by_name(name)
            .unwrap_or_else(|| panic!("batch has no column `{name}`"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column `{name}` is not Int64"));
        for i in 0..c.len() {
            assert!(!c.is_null(i), "unexpected NULL id");
            out.push(c.value(i));
        }
    }
    out
}

fn opt_ints(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let c = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..c.len() {
            out.push(if c.is_null(i) { None } else { Some(c.value(i)) });
        }
    }
    out
}

/// Decode a decimal_arb column using the scale declared on the *output* field.
fn arb_values(schema: &SchemaRef, batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let f = field_of(schema, name);
    let (_, scale) = DecimalArbType::precision_scale_from_field(&f)
        .unwrap_or_else(|| panic!("column `{name}` is not decimal_arb in the output: {f:?}"));
    let mut out = Vec::new();
    for b in batches {
        let c = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap_or_else(|| panic!("column `{name}` is not LargeBinary storage"));
        for i in 0..c.len() {
            if c.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(norm(
                    DecimalArbValue::from_canonical_bytes_at_scale(c.value(i), scale)
                        .unwrap_or_else(|e| panic!("row {i} of `{name}` undecodable: {e}"))
                        .to_canonical_string(),
                )));
            }
        }
    }
    out
}

fn bools(batches: &[RecordBatch], idx: usize) -> Vec<Option<bool>> {
    let mut out = Vec::new();
    for b in batches {
        let c = b
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap_or_else(|| panic!("column {idx} is not Boolean"));
        for i in 0..c.len() {
            out.push(if c.is_null(i) { None } else { Some(c.value(i)) });
        }
    }
    out
}

/// Trim scale-padding zeros so numeric comparisons read cleanly; the declared
/// scale is asserted separately by `assert_arb_meta`.
fn norm(s: String) -> String {
    if !s.contains('.') {
        return s;
    }
    let t = s.trim_end_matches('0');
    let t = t.strip_suffix('.').unwrap_or(t);
    if t.is_empty() || t == "-" {
        "0".to_string()
    } else {
        t.to_string()
    }
}

async fn ordered_ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
    let (_, b) = run(ctx, sql).await;
    ints(&b, "id")
}

/// Invoke a comparison UDF over two decimal_arb arrays with declared scales.
fn invoke_cmp(
    func: &dyn ScalarUDFImpl,
    lhs: (LargeBinaryArray, u32, u32),
    rhs: (LargeBinaryArray, u32, u32),
) -> BooleanArray {
    let (lhs_arr, p1, s1) = lhs;
    let (rhs_arr, p2, s2) = rhs;
    let lhs_field = DecimalArbType::field("l", p1, s1, true).unwrap();
    let rhs_field = DecimalArbType::field("r", p2, s2, true).unwrap();
    let arg_fields = vec![Arc::new(lhs_field), Arc::new(rhs_field)];
    let ret_args = ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &[None, None],
    };
    let return_field = func.return_field_from_args(ret_args).unwrap();
    let n = lhs_arr.len().max(rhs_arr.len());
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(lhs_arr)),
            ColumnarValue::Array(Arc::new(rhs_arr)),
        ],
        arg_fields,
        number_rows: n,
        return_field,
        config_options: Arc::new(datafusion::config::ConfigOptions::default()),
    };
    match func.invoke_with_args(args).unwrap() {
        ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("comparison UDF must return BooleanArray")
            .clone(),
        other => panic!("expected an array result, got {other:?}"),
    }
}

/// Run one comparison UDF over the full cross-product of `corpus` at `scale`.
/// Returns a vector of `((i, j), Option<bool>)`.
fn cross_cmp(
    func: &dyn ScalarUDFImpl,
    corpus: &[(i64, &str)],
    p: u32,
    s: u32,
) -> Vec<((usize, usize), Option<bool>)> {
    let n = corpus.len();
    let mut left: Vec<Option<&str>> = Vec::with_capacity(n * n);
    let mut right: Vec<Option<&str>> = Vec::with_capacity(n * n);
    for a in corpus {
        for b in corpus {
            left.push(Some(a.1));
            right.push(Some(b.1));
        }
    }
    let out = invoke_cmp(
        func,
        (arb_storage(p, s, &left), p, s),
        (arb_storage(p, s, &right), p, s),
    );
    let mut res = Vec::with_capacity(n * n);
    for i in 0..n {
        for j in 0..n {
            let k = i * n + j;
            res.push((
                (i, j),
                if out.is_null(k) {
                    None
                } else {
                    Some(out.value(k))
                },
            ));
        }
    }
    res
}

// =====================================================================
// Section 0 — the hand-computed expected orders are themselves correct
//
// Everything downstream compares against these constants, so they are
// validated first against BigDecimal semantics (a path that shares no code
// with the canonical byte encoding or the sort key).
// =====================================================================

fn assert_corpus_order_matches_bigdecimal(corpus: &[(i64, &str)], expected: &[i64]) {
    let mut ids: Vec<i64> = corpus.iter().map(|(i, _)| *i).collect();
    ids.sort_by_key(|a| val(text_for(corpus, *a)));
    assert_eq!(
        ids, expected,
        "the hand-written expected ascending order disagrees with \
         DecimalArbValue::cmp (BigDecimal) — fix the constant, not the product"
    );
}

#[test]
fn corpus_s4_expected_ascending_order_is_numerically_correct() {
    assert_corpus_order_matches_bigdecimal(CORPUS_S4, CORPUS_S4_ASC);
}

#[test]
fn corpus_s0_expected_ascending_order_is_numerically_correct() {
    assert_corpus_order_matches_bigdecimal(CORPUS_S0, CORPUS_S0_ASC);
}

#[test]
fn corpus_p2_expected_ascending_order_is_numerically_correct() {
    assert_corpus_order_matches_bigdecimal(CORPUS_P2, CORPUS_P2_ASC);
}

#[test]
fn corpus_s4_actually_contains_byte_order_numeric_order_divergence() {
    // If the corpus did not diverge, every ordering test below would pass
    // trivially even with a bytewise sort. Prove the divergence exists.
    let mut diverged = 0usize;
    for (_, a) in CORPUS_S4 {
        for (_, b) in CORPUS_S4 {
            let numeric = val(a).cmp(&val(b));
            let bytewise = canon(a, 4).cmp(&canon(b, 4));
            if numeric != bytewise {
                diverged += 1;
            }
        }
    }
    assert!(
        diverged > 0,
        "CORPUS_S4 must contain pairs where raw bytewise order != numeric order, \
         otherwise the ordering tests cannot distinguish a correct sort from a \
         bytewise one"
    );
}

#[test]
fn corpus_s0_actually_contains_byte_order_numeric_order_divergence() {
    let mut diverged = 0usize;
    for (_, a) in CORPUS_S0 {
        for (_, b) in CORPUS_S0 {
            if val(a).cmp(&val(b)) != canon(a, 0).cmp(&canon(b, 0)) {
                diverged += 1;
            }
        }
    }
    assert!(diverged > 0, "CORPUS_S0 must diverge from bytewise order");
}

// =====================================================================
// Section A — `decimal_arb_to_sort_key` as a pure function
// =====================================================================

/// The single load-bearing property: bytewise order of keys == numeric order.
fn assert_sort_key_total_order(corpus: &[(i64, &str)], scale: u32) {
    for (_, a) in corpus {
        for (_, b) in corpus {
            let numeric = val(a).cmp(&val(b));
            let by_key = key_of(a, scale).cmp(&key_of(b, scale));
            assert_eq!(
                by_key,
                numeric,
                "sort key breaks numeric order at scale {scale}: `{a}` vs `{b}` \
                 — key order {by_key:?} but numeric order {numeric:?} \
                 (keys {:02x?} / {:02x?})",
                key_of(a, scale),
                key_of(b, scale)
            );
        }
    }
}

#[test]
fn sort_key_reproduces_numeric_order_over_scale_4_corpus() {
    assert_sort_key_total_order(CORPUS_S4, 4);
}

#[test]
fn sort_key_reproduces_numeric_order_over_scale_0_corpus() {
    assert_sort_key_total_order(CORPUS_S0, 0);
}

#[test]
fn sort_key_reproduces_numeric_order_over_power_of_two_corpus() {
    assert_sort_key_total_order(CORPUS_P2, 0);
}

#[test]
fn sort_key_reproduces_numeric_order_over_dense_signed_integer_range() {
    // -300..=300 at scale 0 crosses the 1-byte/2-byte magnitude boundary in
    // both directions, which is exactly where a length-prefixed key can break.
    let texts: Vec<String> = (-300i64..=300).map(|i| i.to_string()).collect();
    for a in &texts {
        for b in &texts {
            let numeric = val(a).cmp(&val(b));
            let by_key = key_of(a, 0).cmp(&key_of(b, 0));
            assert_eq!(
                by_key, numeric,
                "sort key breaks numeric order for `{a}` vs `{b}`"
            );
        }
    }
}

#[test]
fn sort_key_reproduces_numeric_order_across_the_65536_boundary() {
    let texts = [
        "-65537", "-65536", "-65535", "-256", "-255", "-1", "0", "1", "255", "256", "65535",
        "65536", "65537",
    ];
    for a in texts {
        for b in texts {
            assert_eq!(
                key_of(a, 0).cmp(&key_of(b, 0)),
                val(a).cmp(&val(b)),
                "sort key breaks numeric order for `{a}` vs `{b}`"
            );
        }
    }
}

#[test]
fn sort_key_orders_more_negative_before_less_negative_with_longer_magnitude() {
    // -1000 (magnitude 0x03E8, 2 bytes) vs -9 (0x09, 1 byte): the more-negative
    // value has the LONGER magnitude, which the key's inverted length prefix
    // must order first.
    assert!(
        key_of("-1000", 0) < key_of("-9", 0),
        "-1000 must sort before -9"
    );
}

#[test]
fn sort_key_fixes_the_bytewise_inversion_for_two_same_length_negatives() {
    // -100 -> [FF,64], -9 -> [FF,09]. Raw bytewise puts -9 first, which is
    // numerically backwards; the bit-flipped magnitude must invert it.
    assert!(
        canon("-100", 0) > canon("-9", 0),
        "premise: raw canonical bytes order -9 before -100"
    );
    assert!(
        key_of("-100", 0) < key_of("-9", 0),
        "the sort key must correct the bytewise inversion: -100 < -9"
    );
}

#[test]
fn sort_key_orders_9_before_100() {
    assert!(key_of("9", 0) < key_of("100", 0), "9 must sort before 100");
}

#[test]
fn sort_key_fixes_the_bytewise_inversion_for_positives_of_different_length() {
    // 100 -> [00,64], 256 -> [00,01,00]. Raw bytewise puts 256 first because
    // 0x01 < 0x64; the key's length prefix must order 100 first.
    assert!(
        canon("100", 0) > canon("256", 0),
        "premise: raw canonical bytes order 256 before 100"
    );
    assert!(
        key_of("100", 0) < key_of("256", 0),
        "the sort key must correct the bytewise inversion: 100 < 256"
    );
}

#[test]
fn sort_key_orders_005_before_05_at_scale_4() {
    assert!(
        key_of("0.05", 4) < key_of("0.5", 4),
        "0.05 must sort before 0.5"
    );
}

#[test]
fn sort_key_orders_negative_05_before_negative_005_at_scale_4() {
    assert!(
        key_of("-0.5", 4) < key_of("-0.05", 4),
        "-0.5 must sort before -0.05"
    );
}

#[test]
fn sort_key_places_every_negative_before_zero() {
    for (_, t) in CORPUS_S4 {
        if val(t).cmp(&val("0")) == Ordering::Less {
            assert!(
                key_of(t, 4) < key_of("0", 4),
                "negative `{t}` must sort before 0"
            );
        }
    }
}

#[test]
fn sort_key_places_zero_before_every_positive() {
    for (_, t) in CORPUS_S4 {
        if val(t).cmp(&val("0")) == Ordering::Greater {
            assert!(
                key_of("0", 4) < key_of(t, 4),
                "0 must sort before positive `{t}`"
            );
        }
    }
}

#[test]
fn sort_key_prefix_byte_is_zero_for_negatives_and_one_for_non_negatives() {
    for (_, t) in CORPUS_S4 {
        let k = key_of(t, 4);
        let expected = if val(t).cmp(&val("0")) == Ordering::Less {
            0u8
        } else {
            1u8
        };
        assert_eq!(
            k[0], expected,
            "sort key sign prefix wrong for `{t}` (key {k:02x?})"
        );
    }
}

#[test]
fn sort_key_length_is_five_plus_magnitude_length() {
    for (_, t) in CORPUS_S4 {
        let c = canon(t, 4);
        let k = key_of(t, 4);
        assert_eq!(
            k.len(),
            5 + (c.len() - 1),
            "sort key for `{t}` should be 1 sign byte + 4 length bytes + magnitude"
        );
    }
}

#[test]
fn sort_key_of_zero_is_the_documented_five_byte_form() {
    assert_eq!(
        key_of("0", 0),
        vec![1u8, 0, 0, 0, 0],
        "zero must key as [1,0,0,0,0] (non-negative prefix, zero-length magnitude)"
    );
}

#[test]
fn sort_key_of_negative_zero_equals_sort_key_of_zero() {
    // `-0` canonicalizes to `0`, so the two must be indistinguishable in the
    // sort order — otherwise ORDER BY splits a single value into two groups.
    assert_eq!(
        key_of("-0", 4),
        key_of("0", 4),
        "-0 and 0 must produce identical sort keys"
    );
}

#[test]
fn sort_key_of_zero_is_scale_independent() {
    for s in [0u32, 1, 4, 9, 18] {
        assert_eq!(
            key_of("0", s),
            vec![1u8, 0, 0, 0, 0],
            "zero must key identically at every scale (scale {s})"
        );
    }
}

#[test]
fn sort_key_is_deterministic() {
    for (_, t) in CORPUS_S4 {
        assert_eq!(
            key_of(t, 4),
            key_of(t, 4),
            "sort key for `{t}` is not deterministic"
        );
    }
}

#[test]
fn sort_key_is_injective_over_distinct_values_at_one_scale() {
    let mut seen: Vec<(Vec<u8>, &str)> = Vec::new();
    for (_, t) in CORPUS_S4 {
        let k = key_of(t, 4);
        if let Some((_, other)) = seen.iter().find(|(kk, _)| *kk == k) {
            panic!(
                "sort key collision at scale 4 between `{t}` and `{other}` — two distinct values would sort as equal"
            );
        }
        seen.push((k, t));
    }
}

#[test]
fn sort_key_treats_numerically_equal_spellings_identically() {
    // GROUP BY / DISTINCT / ORDER BY tie-breaking all depend on this.
    for (a, b) in [
        ("5", "5.0"),
        ("5", "05"),
        ("5", "+5"),
        ("-3", "-3.0000"),
        ("0.05", "0.0500"),
        ("0", "0.0"),
    ] {
        assert_eq!(
            key_of(a, 4),
            key_of(b, 4),
            "`{a}` and `{b}` are numerically equal and must produce identical sort keys"
        );
    }
}

#[test]
fn sort_key_of_smallest_representable_positive_is_above_zero() {
    assert!(
        key_of("0.0001", 4) > key_of("0", 4),
        "the smallest positive representable at scale 4 must sort above zero"
    );
}

#[test]
fn sort_key_of_smallest_representable_negative_is_below_zero() {
    assert!(
        key_of("-0.0001", 4) < key_of("0", 4),
        "the smallest negative representable at scale 4 must sort below zero"
    );
}

#[test]
fn sort_key_orders_adjacent_ulps_at_scale_18() {
    let a = "0.000000000000000001";
    let b = "0.000000000000000002";
    assert!(
        key_of(a, 18) < key_of(b, 18),
        "one-ulp difference at scale 18 must be ordered"
    );
    assert!(
        key_of(&format!("-{b}"), 18) < key_of(&format!("-{a}"), 18),
        "negative one-ulp difference at scale 18 must be ordered"
    );
}

#[test]
fn sort_key_orders_hundred_digit_magnitudes() {
    let big = format!("1{}", "0".repeat(99)); // 10^99
    let just_below = "9".repeat(99); // 10^99 - 1
    assert!(
        key_of(&just_below, 0) < key_of(&big, 0),
        "10^99-1 must sort below 10^99"
    );
    assert!(
        key_of(&format!("-{big}"), 0) < key_of(&format!("-{just_below}"), 0),
        "-10^99 must sort below -(10^99-1)"
    );
}

#[test]
fn sort_key_orders_across_a_magnitude_byte_length_change_for_positives() {
    // 255 -> 1 byte, 256 -> 2 bytes. The length prefix must not invert them.
    assert!(
        key_of("255", 0) < key_of("256", 0),
        "255 must sort below 256"
    );
}

#[test]
fn sort_key_orders_across_a_magnitude_byte_length_change_for_negatives() {
    assert!(
        key_of("-256", 0) < key_of("-255", 0),
        "-256 must sort below -255"
    );
}

#[test]
fn sort_key_orders_same_length_magnitudes_by_trailing_byte() {
    // 0x0100 vs 0x01FF — identical first magnitude byte, differ at the last.
    assert!(
        key_of("256", 0) < key_of("511", 0),
        "256 must sort below 511"
    );
    assert!(
        key_of("-511", 0) < key_of("-256", 0),
        "-511 must sort below -256"
    );
}

#[test]
fn sort_key_ordering_matches_decimal_arb_value_ordering_pairwise() {
    // Cross-check the key against the type's own Ord, over the union corpus.
    for (_, a) in CORPUS_S4 {
        for (_, b) in CORPUS_S4 {
            assert_eq!(
                key_of(a, 4).cmp(&key_of(b, 4)),
                val(a).cmp(&val(b)),
                "sort key and DecimalArbValue::cmp disagree on `{a}` vs `{b}`"
            );
        }
    }
}

#[test]
fn sort_key_order_is_transitive_over_the_corpus() {
    let ts: Vec<&str> = CORPUS_S4.iter().map(|(_, t)| *t).collect();
    for a in &ts {
        for b in &ts {
            for c in &ts {
                if key_of(a, 4) < key_of(b, 4) && key_of(b, 4) < key_of(c, 4) {
                    assert!(
                        key_of(a, 4) < key_of(c, 4),
                        "sort key order is not transitive: `{a}` < `{b}` < `{c}` but not `{a}` < `{c}`"
                    );
                }
            }
        }
    }
}

#[test]
fn sort_key_order_is_antisymmetric_over_the_corpus() {
    for (_, a) in CORPUS_S4 {
        for (_, b) in CORPUS_S4 {
            let ab = key_of(a, 4).cmp(&key_of(b, 4));
            let ba = key_of(b, 4).cmp(&key_of(a, 4));
            assert_eq!(
                ab,
                ba.reverse(),
                "sort key order is not antisymmetric for `{a}` vs `{b}`"
            );
        }
    }
}

#[test]
fn sorting_the_corpus_by_sort_key_yields_the_hand_verified_order() {
    let mut ids = ids_of(CORPUS_S4);
    ids.sort_by_key(|a| key_of(text_for(CORPUS_S4, *a), 4));
    assert_eq!(
        ids, CORPUS_S4_ASC,
        "sorting by sort key must reproduce the numeric ascending order"
    );
}

#[test]
fn sorting_the_scale_zero_corpus_by_sort_key_yields_the_hand_verified_order() {
    let mut ids = ids_of(CORPUS_S0);
    ids.sort_by_key(|a| key_of(text_for(CORPUS_S0, *a), 0));
    assert_eq!(ids, CORPUS_S0_ASC);
}

#[test]
fn sorting_the_power_of_two_corpus_by_sort_key_yields_the_hand_verified_order() {
    let mut ids = ids_of(CORPUS_P2);
    ids.sort_by_key(|a| key_of(text_for(CORPUS_P2, *a), 0));
    assert_eq!(ids, CORPUS_P2_ASC);
}

#[test]
fn sort_key_of_defensive_empty_input_matches_zero() {
    // Documented defensive branch: empty canonical bytes key as zero rather
    // than panicking or producing a key that sorts before all negatives.
    assert_eq!(
        decimal_arb_to_sort_key(&[]),
        vec![1u8, 0, 0, 0, 0],
        "the empty-input defensive branch must not produce an out-of-band key"
    );
}

#[test]
fn sort_key_never_panics_on_arbitrary_two_byte_payloads() {
    // A malformed payload must not panic the sort path (the agent must not
    // crash on corrupted upstream bytes).
    for sign in [0x00u8, 0xFF, 0x01, 0x7F, 0x80] {
        for mag in 0u8..=255 {
            let k = decimal_arb_to_sort_key(&[sign, mag]);
            assert_eq!(
                k.len(),
                6,
                "key length wrong for payload [{sign:02x},{mag:02x}]"
            );
        }
    }
}

#[test]
fn sort_key_treats_unknown_sign_bytes_as_non_negative() {
    // Only 0xFF selects the negative branch; anything else takes the positive
    // branch. Pinning this documents that a corrupted sign byte cannot make a
    // value sort into the negative region.
    for sign in [0x00u8, 0x01, 0x7F, 0x80, 0xFE] {
        assert_eq!(
            decimal_arb_to_sort_key(&[sign, 0x05])[0],
            1u8,
            "sign byte 0x{sign:02x} must take the non-negative key branch"
        );
    }
    assert_eq!(decimal_arb_to_sort_key(&[0xFF, 0x05])[0], 0u8);
}

// =====================================================================
// Section B — comparison UDFs, invoked directly
// =====================================================================

#[test]
fn eq_is_reflexive_over_the_corpus() {
    let res = cross_cmp(&DecimalArbEqFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        let v = res[i * n + i].1;
        assert_eq!(
            v,
            Some(true),
            "decimal_arb_eq is not reflexive for `{}`",
            CORPUS_S4[i].1
        );
    }
}

#[test]
fn lte_is_reflexive_over_the_corpus() {
    let res = cross_cmp(&DecimalArbLteFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        assert_eq!(
            res[i * n + i].1,
            Some(true),
            "decimal_arb_lte is not reflexive for `{}`",
            CORPUS_S4[i].1
        );
    }
}

#[test]
fn gte_is_reflexive_over_the_corpus() {
    let res = cross_cmp(&DecimalArbGteFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        assert_eq!(
            res[i * n + i].1,
            Some(true),
            "decimal_arb_gte not reflexive"
        );
    }
}

#[test]
fn lt_is_irreflexive_over_the_corpus() {
    let res = cross_cmp(&DecimalArbLtFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        assert_eq!(
            res[i * n + i].1,
            Some(false),
            "decimal_arb_lt must be irreflexive for `{}`",
            CORPUS_S4[i].1
        );
    }
}

#[test]
fn gt_is_irreflexive_over_the_corpus() {
    let res = cross_cmp(&DecimalArbGtFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        assert_eq!(
            res[i * n + i].1,
            Some(false),
            "decimal_arb_gt not irreflexive"
        );
    }
}

#[test]
fn neq_is_irreflexive_over_the_corpus() {
    let res = cross_cmp(&DecimalArbNeqFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        assert_eq!(
            res[i * n + i].1,
            Some(false),
            "decimal_arb_neq not irreflexive"
        );
    }
}

#[test]
fn eq_agrees_with_decimal_arb_value_equality_over_the_cross_product() {
    let res = cross_cmp(&DecimalArbEqFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for ((i, j), v) in res {
        let expected = val(CORPUS_S4[i].1) == val(CORPUS_S4[j].1);
        assert_eq!(
            v,
            Some(expected),
            "decimal_arb_eq(`{}`, `{}`) disagrees with DecimalArbValue equality",
            CORPUS_S4[i].1,
            CORPUS_S4[j].1
        );
        let _ = n;
    }
}

#[test]
fn lt_agrees_with_decimal_arb_value_ordering_over_the_cross_product() {
    let res = cross_cmp(&DecimalArbLtFunc::new(), CORPUS_S4, 40, 4);
    for ((i, j), v) in res {
        let expected = val(CORPUS_S4[i].1).cmp(&val(CORPUS_S4[j].1)) == Ordering::Less;
        assert_eq!(
            v,
            Some(expected),
            "decimal_arb_lt(`{}`, `{}`) is wrong",
            CORPUS_S4[i].1,
            CORPUS_S4[j].1
        );
    }
}

#[test]
fn gt_agrees_with_decimal_arb_value_ordering_over_the_cross_product() {
    let res = cross_cmp(&DecimalArbGtFunc::new(), CORPUS_S4, 40, 4);
    for ((i, j), v) in res {
        let expected = val(CORPUS_S4[i].1).cmp(&val(CORPUS_S4[j].1)) == Ordering::Greater;
        assert_eq!(
            v,
            Some(expected),
            "decimal_arb_gt(`{}`, `{}`) is wrong",
            CORPUS_S4[i].1,
            CORPUS_S4[j].1
        );
    }
}

#[test]
fn lte_agrees_with_decimal_arb_value_ordering_over_the_cross_product() {
    let res = cross_cmp(&DecimalArbLteFunc::new(), CORPUS_S4, 40, 4);
    for ((i, j), v) in res {
        let expected = val(CORPUS_S4[i].1).cmp(&val(CORPUS_S4[j].1)) != Ordering::Greater;
        assert_eq!(v, Some(expected), "decimal_arb_lte is wrong at ({i},{j})");
    }
}

#[test]
fn gte_agrees_with_decimal_arb_value_ordering_over_the_cross_product() {
    let res = cross_cmp(&DecimalArbGteFunc::new(), CORPUS_S4, 40, 4);
    for ((i, j), v) in res {
        let expected = val(CORPUS_S4[i].1).cmp(&val(CORPUS_S4[j].1)) != Ordering::Less;
        assert_eq!(v, Some(expected), "decimal_arb_gte is wrong at ({i},{j})");
    }
}

#[test]
fn neq_is_the_exact_complement_of_eq() {
    let eq = cross_cmp(&DecimalArbEqFunc::new(), CORPUS_S4, 40, 4);
    let ne = cross_cmp(&DecimalArbNeqFunc::new(), CORPUS_S4, 40, 4);
    for k in 0..eq.len() {
        assert_eq!(
            ne[k].1,
            eq[k].1.map(|b| !b),
            "decimal_arb_neq must be the exact complement of decimal_arb_eq at {:?}",
            eq[k].0
        );
    }
}

#[test]
fn lt_and_gt_are_antisymmetric() {
    let lt = cross_cmp(&DecimalArbLtFunc::new(), CORPUS_S4, 40, 4);
    let gt = cross_cmp(&DecimalArbGtFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        for j in 0..n {
            assert_eq!(
                lt[i * n + j].1,
                gt[j * n + i].1,
                "lt(a,b) must equal gt(b,a) for `{}` / `{}`",
                CORPUS_S4[i].1,
                CORPUS_S4[j].1
            );
        }
    }
}

#[test]
fn lte_and_gte_are_antisymmetric() {
    let lte = cross_cmp(&DecimalArbLteFunc::new(), CORPUS_S4, 40, 4);
    let gte = cross_cmp(&DecimalArbGteFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        for j in 0..n {
            assert_eq!(
                lte[i * n + j].1,
                gte[j * n + i].1,
                "lte(a,b) must equal gte(b,a) at ({i},{j})"
            );
        }
    }
}

#[test]
fn exactly_one_of_lt_eq_gt_holds_for_every_pair() {
    let lt = cross_cmp(&DecimalArbLtFunc::new(), CORPUS_S4, 40, 4);
    let eq = cross_cmp(&DecimalArbEqFunc::new(), CORPUS_S4, 40, 4);
    let gt = cross_cmp(&DecimalArbGtFunc::new(), CORPUS_S4, 40, 4);
    for k in 0..lt.len() {
        let count = [lt[k].1, eq[k].1, gt[k].1]
            .iter()
            .filter(|v| **v == Some(true))
            .count();
        assert_eq!(
            count, 1,
            "trichotomy violated at {:?}: lt={:?} eq={:?} gt={:?}",
            lt[k].0, lt[k].1, eq[k].1, gt[k].1
        );
    }
}

#[test]
fn lte_equals_lt_or_eq_for_every_pair() {
    let lt = cross_cmp(&DecimalArbLtFunc::new(), CORPUS_S4, 40, 4);
    let eq = cross_cmp(&DecimalArbEqFunc::new(), CORPUS_S4, 40, 4);
    let lte = cross_cmp(&DecimalArbLteFunc::new(), CORPUS_S4, 40, 4);
    for k in 0..lt.len() {
        assert_eq!(
            lte[k].1,
            Some(lt[k].1.unwrap() || eq[k].1.unwrap()),
            "lte != (lt OR eq) at {:?}",
            lt[k].0
        );
    }
}

#[test]
fn gte_equals_gt_or_eq_for_every_pair() {
    let gt = cross_cmp(&DecimalArbGtFunc::new(), CORPUS_S4, 40, 4);
    let eq = cross_cmp(&DecimalArbEqFunc::new(), CORPUS_S4, 40, 4);
    let gte = cross_cmp(&DecimalArbGteFunc::new(), CORPUS_S4, 40, 4);
    for k in 0..gt.len() {
        assert_eq!(
            gte[k].1,
            Some(gt[k].1.unwrap() || eq[k].1.unwrap()),
            "gte != (gt OR eq) at {:?}",
            gt[k].0
        );
    }
}

#[test]
// i < j < k indexing is the natural statement of transitivity over the corpus;
// iterator adaptors would obscure the triple relation under test.
#[allow(clippy::needless_range_loop)]
fn lt_is_transitive_over_the_corpus() {
    let lt = cross_cmp(&DecimalArbLtFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    let b = |i: usize, j: usize| lt[i * n + j].1.unwrap();
    for i in 0..n {
        for j in 0..n {
            for k in 0..n {
                if b(i, j) && b(j, k) {
                    assert!(
                        b(i, k),
                        "decimal_arb_lt is not transitive: `{}` < `{}` < `{}`",
                        CORPUS_S4[i].1,
                        CORPUS_S4[j].1,
                        CORPUS_S4[k].1
                    );
                }
            }
        }
    }
}

#[test]
fn eq_is_transitive_over_the_corpus_including_alternate_spellings() {
    let corpus: Vec<(i64, &str)> = vec![
        (1, "5"),
        (2, "5.0"),
        (3, "05"),
        (4, "5.0000"),
        (5, "-3"),
        (6, "-3.00"),
        (7, "0"),
        (8, "-0"),
    ];
    let eq = cross_cmp(&DecimalArbEqFunc::new(), &corpus, 40, 4);
    let n = corpus.len();
    let b = |i: usize, j: usize| eq[i * n + j].1.unwrap();
    for i in 0..n {
        for j in 0..n {
            for k in 0..n {
                if b(i, j) && b(j, k) {
                    assert!(
                        b(i, k),
                        "decimal_arb_eq is not transitive: `{}` = `{}` = `{}`",
                        corpus[i].1,
                        corpus[j].1,
                        corpus[k].1
                    );
                }
            }
        }
    }
}

#[test]
fn eq_is_symmetric_over_the_corpus() {
    let eq = cross_cmp(&DecimalArbEqFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        for j in 0..n {
            assert_eq!(
                eq[i * n + j].1,
                eq[j * n + i].1,
                "decimal_arb_eq is not symmetric at ({i},{j})"
            );
        }
    }
}

#[test]
fn comparisons_agree_with_the_sort_key_byte_order() {
    // The two independent ordering implementations must not diverge: if
    // `lt(a,b)` is true then key(a) < key(b), for every pair.
    let lt = cross_cmp(&DecimalArbLtFunc::new(), CORPUS_S4, 40, 4);
    let n = CORPUS_S4.len();
    for i in 0..n {
        for j in 0..n {
            let by_udf = lt[i * n + j].1.unwrap();
            let by_key = key_of(CORPUS_S4[i].1, 4) < key_of(CORPUS_S4[j].1, 4);
            assert_eq!(
                by_udf, by_key,
                "decimal_arb_lt and the sort key disagree on `{}` vs `{}` — \
                 WHERE and ORDER BY would return inconsistent results",
                CORPUS_S4[i].1, CORPUS_S4[j].1
            );
        }
    }
}

#[test]
fn eq_matches_across_operands_declared_at_different_scales() {
    // Column scales differ; the UDF decodes each side at its own scale, so
    // numerically equal values must still compare equal.
    let lhs = arb_storage(40, 0, &[Some("5"), Some("-3"), Some("0")]);
    let rhs = arb_storage(40, 4, &[Some("5.0000"), Some("-3"), Some("0.0000")]);
    let out = invoke_cmp(&DecimalArbEqFunc::new(), (lhs, 40, 0), (rhs, 40, 4));
    for i in 0..3 {
        assert!(
            out.value(i),
            "row {i}: cross-scale operands that are numerically equal must compare equal"
        );
    }
}

#[test]
fn lt_is_correct_across_operands_declared_at_different_scales() {
    let lhs = arb_storage(40, 0, &[Some("5"), Some("-1000"), Some("0")]);
    let rhs = arb_storage(40, 6, &[Some("5.000001"), Some("-9"), Some("-0.000001")]);
    let out = invoke_cmp(&DecimalArbLtFunc::new(), (lhs, 40, 0), (rhs, 40, 6));
    assert!(out.value(0), "5 < 5.000001");
    assert!(
        out.value(1),
        "-1000 < -9 (cross-scale, byte order disagrees)"
    );
    assert!(!out.value(2), "0 is NOT < -0.000001");
}

#[test]
fn gt_is_correct_across_operands_declared_at_different_scales() {
    let lhs = arb_storage(40, 8, &[Some("0.00000001"), Some("-0.00000001")]);
    let rhs = arb_storage(40, 2, &[Some("0"), Some("0")]);
    let out = invoke_cmp(&DecimalArbGtFunc::new(), (lhs, 40, 8), (rhs, 40, 2));
    assert!(out.value(0), "1e-8 > 0");
    assert!(!out.value(1), "-1e-8 is not > 0");
}

#[test]
fn every_comparison_returns_null_when_the_left_operand_is_null() {
    let lhs = arb_storage(40, 4, &[None]);
    let rhs = arb_storage(40, 4, &[Some("1")]);
    for (name, f) in cmp_funcs() {
        let out = invoke_cmp(f.as_ref(), (lhs.clone(), 40, 4), (rhs.clone(), 40, 4));
        assert!(out.is_null(0), "{name}(NULL, 1) must be NULL, not a value");
    }
}

#[test]
fn every_comparison_returns_null_when_the_right_operand_is_null() {
    let lhs = arb_storage(40, 4, &[Some("1")]);
    let rhs = arb_storage(40, 4, &[None]);
    for (name, f) in cmp_funcs() {
        let out = invoke_cmp(f.as_ref(), (lhs.clone(), 40, 4), (rhs.clone(), 40, 4));
        assert!(out.is_null(0), "{name}(1, NULL) must be NULL");
    }
}

#[test]
fn every_comparison_returns_null_when_both_operands_are_null() {
    let lhs = arb_storage(40, 4, &[None]);
    let rhs = arb_storage(40, 4, &[None]);
    for (name, f) in cmp_funcs() {
        let out = invoke_cmp(f.as_ref(), (lhs.clone(), 40, 4), (rhs.clone(), 40, 4));
        assert!(out.is_null(0), "{name}(NULL, NULL) must be NULL, not TRUE");
    }
}

#[test]
fn comparison_null_handling_does_not_shift_the_surrounding_rows() {
    // A NULL in the middle must not desynchronize the output from the input.
    let lhs = arb_storage(40, 4, &[Some("-1000"), None, Some("9")]);
    let rhs = arb_storage(40, 4, &[Some("-9"), Some("1"), Some("100")]);
    let out = invoke_cmp(&DecimalArbLtFunc::new(), (lhs, 40, 4), (rhs, 40, 4));
    assert_eq!(out.len(), 3, "output length must match the input length");
    assert!(out.value(0), "-1000 < -9");
    assert!(out.is_null(1), "row 1 must be NULL");
    assert!(out.value(2), "9 < 100");
}

#[test]
fn comparison_broadcasts_a_length_one_left_operand() {
    let lhs = arb_storage(40, 4, &[Some("0")]);
    let rhs = arb_storage(40, 4, &[Some("-1000"), Some("-9"), Some("0"), Some("9")]);
    let out = invoke_cmp(&DecimalArbLtFunc::new(), (lhs, 40, 4), (rhs, 40, 4));
    assert_eq!(
        out.len(),
        4,
        "broadcast must produce one row per array element"
    );
    assert_eq!(
        (0..4).map(|i| out.value(i)).collect::<Vec<_>>(),
        vec![false, false, false, true],
        "0 < x should only hold for x = 9"
    );
}

#[test]
fn comparison_broadcasts_a_length_one_right_operand() {
    let lhs = arb_storage(40, 4, &[Some("-1000"), Some("-9"), Some("0"), Some("9")]);
    let rhs = arb_storage(40, 4, &[Some("0")]);
    let out = invoke_cmp(&DecimalArbLtFunc::new(), (lhs, 40, 4), (rhs, 40, 4));
    assert_eq!(
        (0..4).map(|i| out.value(i)).collect::<Vec<_>>(),
        vec![true, true, false, false],
        "x < 0 should hold only for the two negatives"
    );
}

#[test]
fn comparison_of_two_empty_arrays_yields_an_empty_result() {
    let lhs = arb_storage(40, 4, &[]);
    let rhs = arb_storage(40, 4, &[]);
    let out = invoke_cmp(&DecimalArbEqFunc::new(), (lhs, 40, 4), (rhs, 40, 4));
    assert_eq!(out.len(), 0, "empty inputs must yield an empty output");
}

#[test]
fn comparison_of_an_empty_array_against_a_broadcast_scalar_yields_empty() {
    let lhs = arb_storage(40, 4, &[]);
    let rhs = arb_storage(40, 4, &[Some("1")]);
    let out = invoke_cmp(&DecimalArbLtFunc::new(), (lhs, 40, 4), (rhs, 40, 4));
    assert_eq!(
        out.len(),
        0,
        "an empty batch against a broadcast scalar must stay empty"
    );
}

#[test]
fn comparison_rejects_a_left_field_without_decimal_arb_metadata() {
    let arg_fields = vec![
        Arc::new(Field::new("l", DataType::LargeBinary, true)),
        Arc::new(DecimalArbType::field("r", 40, 4, true).unwrap()),
    ];
    let ret = ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &[None, None],
    };
    assert!(
        DecimalArbLtFunc::new().return_field_from_args(ret).is_err(),
        "a bare LargeBinary left operand must be rejected at planning time, \
         not silently compared as raw bytes"
    );
}

#[test]
fn comparison_rejects_a_right_field_without_decimal_arb_metadata() {
    let arg_fields = vec![
        Arc::new(DecimalArbType::field("l", 40, 4, true).unwrap()),
        Arc::new(Field::new("r", DataType::LargeBinary, true)),
    ];
    let ret = ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &[None, None],
    };
    assert!(
        DecimalArbGteFunc::new()
            .return_field_from_args(ret)
            .is_err(),
        "a bare LargeBinary right operand must be rejected at planning time"
    );
}

#[test]
fn every_comparison_returns_a_non_decimal_arb_boolean_field() {
    let arg_fields = vec![
        Arc::new(DecimalArbType::field("l", 40, 4, true).unwrap()),
        Arc::new(DecimalArbType::field("r", 40, 4, true).unwrap()),
    ];
    for (name, f) in cmp_funcs() {
        let ret = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        };
        let field = f.return_field_from_args(ret).unwrap();
        assert_eq!(
            field.data_type(),
            &DataType::Boolean,
            "{name} must return Boolean"
        );
        assert!(
            !DecimalArbType::is_decimal_arb_field(field.as_ref()),
            "{name}'s Boolean output must NOT carry decimal_arb metadata"
        );
    }
}

#[test]
fn comparisons_are_correct_on_the_power_of_two_boundary_corpus() {
    let lt = cross_cmp(&DecimalArbLtFunc::new(), CORPUS_P2, 100, 0);
    for ((i, j), v) in lt {
        let expected = val(CORPUS_P2[i].1).cmp(&val(CORPUS_P2[j].1)) == Ordering::Less;
        assert_eq!(
            v,
            Some(expected),
            "decimal_arb_lt wrong for `{}` vs `{}` at the 2^k magnitude boundary",
            CORPUS_P2[i].1,
            CORPUS_P2[j].1
        );
    }
}

fn cmp_funcs() -> Vec<(&'static str, Box<dyn ScalarUDFImpl>)> {
    vec![
        ("decimal_arb_eq", Box::new(DecimalArbEqFunc::new())),
        ("decimal_arb_neq", Box::new(DecimalArbNeqFunc::new())),
        ("decimal_arb_lt", Box::new(DecimalArbLtFunc::new())),
        ("decimal_arb_lte", Box::new(DecimalArbLteFunc::new())),
        ("decimal_arb_gt", Box::new(DecimalArbGtFunc::new())),
        ("decimal_arb_gte", Box::new(DecimalArbGteFunc::new())),
    ]
}

// =====================================================================
// Section C — SQL ORDER BY through the full SessionManager stack
// =====================================================================

#[tokio::test]
async fn order_by_asc_over_a_byte_divergent_scale_4_corpus_is_numeric() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC").await,
        CORPUS_S4_ASC,
        "ORDER BY over decimal_arb must be numeric, not bytewise"
    );
}

#[tokio::test]
async fn order_by_desc_over_a_byte_divergent_scale_4_corpus_is_numeric() {
    let ctx = session();
    let mut expected = CORPUS_S4_ASC.to_vec();
    expected.reverse();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v DESC").await,
        expected,
        "ORDER BY ... DESC must be the exact reverse of ASC"
    );
}

#[tokio::test]
async fn order_by_default_direction_is_ascending() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v").await,
        CORPUS_S4_ASC,
        "the default ORDER BY direction must be ASC"
    );
}

#[tokio::test]
async fn order_by_asc_over_the_scale_zero_byte_length_corpus_is_numeric() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM z ORDER BY v ASC").await,
        CORPUS_S0_ASC,
        "ORDER BY must handle magnitudes that change byte length (255/256, 65535/65536)"
    );
}

#[tokio::test]
async fn order_by_desc_over_the_scale_zero_byte_length_corpus_is_numeric() {
    let ctx = session();
    let mut expected = CORPUS_S0_ASC.to_vec();
    expected.reverse();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM z ORDER BY v DESC").await,
        expected
    );
}

#[tokio::test]
async fn order_by_asc_over_power_of_two_magnitudes_is_numeric() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM p ORDER BY v ASC").await,
        CORPUS_P2_ASC,
        "ORDER BY must be correct across the 2^64 / 2^128 magnitude boundaries"
    );
}

#[tokio::test]
async fn order_by_desc_over_power_of_two_magnitudes_is_numeric() {
    let ctx = session();
    let mut expected = CORPUS_P2_ASC.to_vec();
    expected.reverse();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM p ORDER BY v DESC").await,
        expected
    );
}

#[tokio::test]
async fn order_by_sorts_correctly_across_batch_boundaries_within_a_partition() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM mb ORDER BY v ASC").await,
        CORPUS_S4_ASC,
        "a sort spanning three batches inside one partition must still be total"
    );
}

#[tokio::test]
async fn order_by_returns_every_input_row_exactly_once() {
    let ctx = session();
    let mut got = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC").await;
    got.sort_unstable();
    let mut expected = ids_of(CORPUS_S4);
    expected.sort_unstable();
    assert_eq!(got, expected, "ORDER BY must not drop or duplicate rows");
}

#[tokio::test]
async fn order_by_projected_decimal_arb_keeps_metadata_and_numeric_order() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v FROM c ORDER BY v ASC").await;
    assert_arb_meta(&schema, "v", 40, 4, "ORDER BY over the projected column");
    let vals: Vec<String> = arb_values(&schema, &b, "v")
        .into_iter()
        .map(|o| o.unwrap())
        .collect();
    let expected: Vec<String> = CORPUS_S4_ASC
        .iter()
        .map(|id| norm(val(text_for(CORPUS_S4, *id)).to_canonical_string()))
        .collect();
    assert_eq!(vals, expected, "sorted values must be in numeric order");
}

#[tokio::test]
async fn order_by_desc_projected_decimal_arb_keeps_metadata() {
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT v FROM c ORDER BY v DESC").await;
    assert_arb_meta(&schema, "v", 40, 4, "ORDER BY DESC");
}

#[tokio::test]
async fn order_by_does_not_leak_the_sort_key_column_into_the_output() {
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT id FROM c ORDER BY v ASC").await;
    assert_eq!(
        schema.fields().len(),
        1,
        "the rewritten sort key must not appear as an output column: {:?}",
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
    assert_eq!(schema.field(0).name(), "id");
}

#[tokio::test]
async fn select_star_order_by_decimal_arb_keeps_exactly_the_table_columns() {
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT * FROM c ORDER BY v ASC").await;
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "v"],
        "SELECT * with a rewritten ORDER BY must not gain a sort-key column"
    );
    assert_arb_meta(&schema, "v", 40, 4, "SELECT * ORDER BY");
}

#[tokio::test]
async fn order_by_nulls_last_places_nulls_after_all_values() {
    let ctx = session();
    let got = opt_ints(
        &run(&ctx, "SELECT id FROM n ORDER BY v ASC NULLS LAST")
            .await
            .1,
        "id",
    );
    assert_eq!(
        got,
        vec![Some(3), Some(5), Some(1), Some(2), Some(4)],
        "NULLS LAST: -5, 0, 5, then the two NULL rows"
    );
}

#[tokio::test]
async fn order_by_nulls_first_places_nulls_before_all_values() {
    let ctx = session();
    let got = opt_ints(
        &run(&ctx, "SELECT id FROM n ORDER BY v ASC NULLS FIRST")
            .await
            .1,
        "id",
    );
    assert_eq!(
        got,
        vec![Some(2), Some(4), Some(3), Some(5), Some(1)],
        "NULLS FIRST: the two NULL rows, then -5, 0, 5"
    );
}

#[tokio::test]
async fn order_by_desc_nulls_last_keeps_values_in_descending_numeric_order() {
    let ctx = session();
    let got = opt_ints(
        &run(&ctx, "SELECT id FROM n ORDER BY v DESC NULLS LAST")
            .await
            .1,
        "id",
    );
    assert_eq!(
        got,
        vec![Some(1), Some(5), Some(3), Some(2), Some(4)],
        "DESC NULLS LAST: 5, 0, -5, then NULLs"
    );
}

#[tokio::test]
async fn order_by_desc_nulls_first_keeps_values_in_descending_numeric_order() {
    let ctx = session();
    let got = opt_ints(
        &run(&ctx, "SELECT id FROM n ORDER BY v DESC NULLS FIRST")
            .await
            .1,
        "id",
    );
    assert_eq!(
        got,
        vec![Some(2), Some(4), Some(1), Some(5), Some(3)],
        "DESC NULLS FIRST: NULLs, then 5, 0, -5"
    );
}

#[tokio::test]
async fn null_rows_are_never_ordered_among_the_values() {
    // Whatever the default null placement, NULLs must form a contiguous block
    // at one end — never interleaved with real values.
    let ctx = session();
    let got = opt_ints(&run(&ctx, "SELECT id FROM n ORDER BY v ASC").await.1, "id");
    let null_positions: Vec<usize> = got
        .iter()
        .enumerate()
        .filter(|(_, v)| matches!(v, Some(2) | Some(4)))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(null_positions.len(), 2);
    assert_eq!(
        null_positions[1] - null_positions[0],
        1,
        "the two NULL rows must be adjacent, not interleaved with values: {got:?}"
    );
}

#[tokio::test]
async fn order_by_keeps_null_rows_in_the_result_set() {
    let ctx = session();
    let got = opt_ints(&run(&ctx, "SELECT id FROM n ORDER BY v ASC").await.1, "id");
    assert_eq!(got.len(), 5, "ORDER BY must not drop NULL-valued rows");
}

#[tokio::test]
async fn order_by_a_null_only_column_returns_all_rows() {
    let ctx = session();
    let got = opt_ints(
        &run(&ctx, "SELECT id FROM n WHERE v IS NULL ORDER BY v ASC")
            .await
            .1,
        "id",
    );
    let mut got = got;
    got.sort();
    assert_eq!(got, vec![Some(2), Some(4)]);
}

#[tokio::test]
async fn multi_column_order_decimal_arb_then_decimal_arb_ascending() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM m ORDER BY a ASC, b ASC").await,
        vec![2, 1, 6, 4, 3, 5],
        "both sort keys must be numeric: a ASC then b ASC"
    );
}

#[tokio::test]
async fn multi_column_order_decimal_arb_asc_then_decimal_arb_desc() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM m ORDER BY a ASC, b DESC").await,
        vec![1, 2, 6, 3, 4, 5],
        "mixed directions must apply per-key, not globally"
    );
}

#[tokio::test]
async fn multi_column_order_decimal_arb_desc_then_decimal_arb_asc() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM m ORDER BY a DESC, b ASC").await,
        vec![5, 4, 3, 6, 2, 1]
    );
}

#[tokio::test]
async fn multi_column_order_int_then_decimal_arb_then_decimal_arb() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM m ORDER BY g ASC, a ASC, b ASC").await,
        vec![2, 1, 5, 6, 4, 3],
        "a non-decimal_arb leading key must not disturb the decimal_arb keys"
    );
}

#[tokio::test]
async fn multi_column_order_decimal_arb_then_int_breaks_ties_numerically() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM m ORDER BY a ASC, id DESC").await,
        vec![2, 1, 6, 4, 3, 5],
        "a=-1000 ties resolve by id DESC (2 then 1); a=9 ties resolve to 4 then 3"
    );
}

#[tokio::test]
async fn multi_column_order_output_keeps_metadata_on_both_decimal_arb_columns() {
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT a, b FROM m ORDER BY a ASC, b DESC").await;
    assert_arb_meta(&schema, "a", 40, 4, "two-key sort");
    assert_arb_meta(&schema, "b", 40, 4, "two-key sort");
}

#[tokio::test]
async fn order_by_with_limit_one_returns_the_numeric_minimum() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC LIMIT 1").await,
        vec![CORPUS_S4_ASC[0]],
        "top-k with LIMIT 1 must return the numerically smallest row"
    );
}

#[tokio::test]
async fn order_by_desc_with_limit_one_returns_the_numeric_maximum() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v DESC LIMIT 1").await,
        vec![*CORPUS_S4_ASC.last().unwrap()],
        "top-k DESC with LIMIT 1 must return the numerically largest row"
    );
}

#[tokio::test]
async fn order_by_with_every_limit_prefix_matches_the_full_sort() {
    // The top-k path is a different physical operator from the full sort; for
    // every k the prefix must agree with the full ordering.
    let ctx = session();
    for k in 1..=CORPUS_S4.len() {
        let got = ordered_ids(&ctx, &format!("SELECT id FROM c ORDER BY v ASC LIMIT {k}")).await;
        assert_eq!(
            got,
            CORPUS_S4_ASC[..k].to_vec(),
            "top-{k} disagrees with the full ascending sort"
        );
    }
}

#[tokio::test]
async fn order_by_desc_with_every_limit_prefix_matches_the_full_sort() {
    let ctx = session();
    let mut desc = CORPUS_S4_ASC.to_vec();
    desc.reverse();
    for k in 1..=CORPUS_S4.len() {
        let got = ordered_ids(&ctx, &format!("SELECT id FROM c ORDER BY v DESC LIMIT {k}")).await;
        assert_eq!(got, desc[..k].to_vec(), "top-{k} DESC disagrees");
    }
}

#[tokio::test]
async fn order_by_limit_spanning_the_sign_boundary_is_numeric() {
    // CORPUS_S4 has 6 negatives; LIMIT 7 must be exactly those 6 plus zero.
    let ctx = session();
    let got = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC LIMIT 7").await;
    assert_eq!(
        got,
        CORPUS_S4_ASC[..7].to_vec(),
        "a top-k that straddles the sign boundary must not favour bytewise order"
    );
    assert!(
        got.contains(&7),
        "the zero row must be the 7th value, after all six negatives"
    );
}

#[tokio::test]
async fn order_by_with_limit_and_offset_windows_the_sorted_sequence() {
    let ctx = session();
    let got = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC LIMIT 4 OFFSET 5").await;
    assert_eq!(
        got,
        CORPUS_S4_ASC[5..9].to_vec(),
        "LIMIT/OFFSET must window the numerically sorted sequence"
    );
}

#[tokio::test]
async fn order_by_limit_zero_returns_no_rows() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM c ORDER BY v ASC LIMIT 0").await;
    assert_eq!(b.iter().map(|x| x.num_rows()).sum::<usize>(), 0);
}

#[tokio::test]
async fn order_by_limit_larger_than_the_input_returns_the_full_sort() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC LIMIT 1000").await,
        CORPUS_S4_ASC
    );
}

#[tokio::test]
async fn order_by_with_limit_keeps_decimal_arb_metadata_on_the_output() {
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT v FROM c ORDER BY v ASC LIMIT 3").await;
    assert_arb_meta(&schema, "v", 40, 4, "top-k projection");
}

#[tokio::test]
#[ignore = "FINDING O2: SQL unary minus over a decimal_arb column fails to plan \
            (`Negation only supports numeric, interval and timestamp types`). \
            DecimalArbExprPlanner hooks plan_binary_op only, so Expr::Negative \
            over LargeBinary reaches TypeCoercion unrewritten. `ORDER BY -amount` \
            and `SELECT -amount` are both unavailable; authors must spell \
            decimal_arb_neg(amount)."]
async fn order_by_a_negated_decimal_arb_expression_inverts_the_order() {
    let ctx = session();
    let mut expected = CORPUS_S4_ASC.to_vec();
    expected.reverse();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY -v ASC").await,
        expected,
        "ORDER BY -v ASC must equal ORDER BY v DESC"
    );
}

#[tokio::test]
#[ignore = "FINDING O2: SQL unary minus over a decimal_arb column fails to plan \
            (`Negation only supports numeric, interval and timestamp types`)."]
async fn unary_minus_over_a_decimal_arb_column_projects() {
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT -v AS neg FROM c").await;
    assert_arb_meta(&schema, "neg", 40, 4, "unary minus projection");
}

#[tokio::test]
async fn order_by_the_decimal_arb_neg_helper_inverts_the_order() {
    // The supported spelling of the previous two tests.
    let ctx = session();
    let mut expected = CORPUS_S4_ASC.to_vec();
    expected.reverse();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY decimal_arb_neg(v) ASC").await,
        expected,
        "ORDER BY decimal_arb_neg(v) ASC must equal ORDER BY v DESC"
    );
}

#[tokio::test]
async fn order_by_the_decimal_arb_abs_helper_sorts_by_magnitude() {
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM c ORDER BY decimal_arb_abs(v) ASC, id ASC",
    )
    .await;
    let abs = |id: i64| val(text_for(CORPUS_S4, id).trim_start_matches('-'));
    let mut expected = ids_of(CORPUS_S4);
    expected.sort_by(|a, b| abs(*a).cmp(&abs(*b)).then(a.cmp(b)));
    assert_eq!(
        got, expected,
        "ORDER BY decimal_arb_abs(v) must sort by absolute numeric value"
    );
}

#[tokio::test]
async fn order_by_an_addition_expression_over_decimal_arb_is_numeric() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v + v ASC").await,
        CORPUS_S4_ASC,
        "v + v is monotone in v, so the order must be unchanged"
    );
}

#[tokio::test]
async fn order_by_a_subtraction_expression_over_decimal_arb_is_numeric() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v - v + v ASC").await,
        CORPUS_S4_ASC
    );
}

#[tokio::test]
async fn order_by_an_aliased_decimal_arb_column_is_numeric() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id, v AS amount FROM c ORDER BY amount ASC").await,
        CORPUS_S4_ASC,
        "ordering by a projection alias must still take the numeric path"
    );
}

#[tokio::test]
async fn order_by_a_qualified_decimal_arb_column_is_numeric() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY c.v ASC").await,
        CORPUS_S4_ASC
    );
}

#[tokio::test]
async fn order_by_an_ordinal_referring_to_a_decimal_arb_column_is_numeric() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v, id FROM c ORDER BY 1 ASC").await;
    assert_arb_meta(&schema, "v", 40, 4, "ORDER BY ordinal");
    assert_eq!(
        ints(&b, "id"),
        CORPUS_S4_ASC,
        "ORDER BY 1 must resolve to the decimal_arb column and sort numerically"
    );
}

#[tokio::test]
async fn order_by_an_explicit_sort_key_call_matches_the_implicit_rewrite() {
    // Writing the helper by hand and letting the rule insert it must agree.
    let ctx = session();
    let implicit = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC").await;
    let explicit = ordered_ids(
        &ctx,
        "SELECT id FROM c ORDER BY decimal_arb_to_sort_key(v) ASC",
    )
    .await;
    assert_eq!(
        implicit, explicit,
        "the automatic ORDER BY rewrite must produce the same order as an \
         explicit decimal_arb_to_sort_key(...) call"
    );
}

#[tokio::test]
async fn order_by_inside_a_subquery_with_limit_selects_the_numeric_minimum() {
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM (SELECT id, v FROM c ORDER BY v ASC LIMIT 3) ORDER BY v ASC",
    )
    .await;
    assert_eq!(
        got,
        CORPUS_S4_ASC[..3].to_vec(),
        "the inner top-3 must be the three numerically smallest rows"
    );
}

#[tokio::test]
async fn order_by_in_a_cte_feeding_an_outer_order_by_is_numeric() {
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "WITH k AS (SELECT id, v FROM c ORDER BY v DESC LIMIT 4) \
         SELECT id FROM k ORDER BY v ASC",
    )
    .await;
    let mut desc = CORPUS_S4_ASC.to_vec();
    desc.reverse();
    let mut top4 = desc[..4].to_vec();
    top4.reverse();
    assert_eq!(
        got, top4,
        "the CTE's top-4 DESC re-sorted ASC must be the four largest, ascending"
    );
}

#[tokio::test]
async fn order_by_after_a_filter_keeps_numeric_order_of_the_survivors() {
    let ctx = session();
    let got = ordered_ids(&ctx, "SELECT id FROM c WHERE v < 0 ORDER BY v ASC").await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| val(text_for(CORPUS_S4, *id)) < val("0"))
        .collect();
    assert_eq!(
        got, expected,
        "WHERE v < 0 must select exactly the negatives, in numeric order"
    );
}

#[tokio::test]
async fn order_by_after_a_positive_filter_keeps_numeric_order() {
    let ctx = session();
    let got = ordered_ids(&ctx, "SELECT id FROM c WHERE v > 0 ORDER BY v ASC").await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| val(text_for(CORPUS_S4, *id)) > val("0"))
        .collect();
    assert_eq!(got, expected);
}

#[tokio::test]
async fn distinct_then_order_by_dedupes_numerically_and_orders_numerically() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT DISTINCT v FROM d ORDER BY v ASC").await;
    assert_arb_meta(&schema, "v", 40, 4, "DISTINCT + ORDER BY");
    let vals: Vec<String> = arb_values(&schema, &b, "v")
        .into_iter()
        .map(|o| o.unwrap())
        .collect();
    assert_eq!(
        vals,
        vec!["-1000", "-9", "0.05", "9"],
        "DISTINCT must fold `-1000`/`-1000.0000`, `9`/`9.0` and `0.05`/`0.0500`, \
         and ORDER BY must then sort numerically"
    );
}

#[tokio::test]
async fn distinct_then_order_by_desc_is_the_reverse_of_ascending() {
    let ctx = session();
    let (s1, b1) = run(&ctx, "SELECT DISTINCT v FROM d ORDER BY v ASC").await;
    let (s2, b2) = run(&ctx, "SELECT DISTINCT v FROM d ORDER BY v DESC").await;
    let mut asc = arb_values(&s1, &b1, "v");
    let desc = arb_values(&s2, &b2, "v");
    asc.reverse();
    assert_eq!(asc, desc, "DISTINCT + DESC must be the reverse of ASC");
}

#[tokio::test]
async fn distinct_over_two_decimal_arb_columns_then_order_by_is_numeric() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT DISTINCT a, b FROM m ORDER BY a ASC, b ASC").await;
    assert_eq!(
        b.iter().map(|x| x.num_rows()).sum::<usize>(),
        6,
        "all six (a, b) pairs are distinct"
    );
}

#[tokio::test]
async fn group_by_a_decimal_arb_key_then_order_by_that_key_is_numeric() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v FROM c GROUP BY v ORDER BY v ASC").await;
    assert_arb_meta(&schema, "v", 40, 4, "GROUP BY + ORDER BY the key");
    let vals: Vec<Option<String>> = arb_values(&schema, &b, "v");
    let expected: Vec<Option<String>> = CORPUS_S4_ASC
        .iter()
        .map(|id| Some(norm(val(text_for(CORPUS_S4, *id)).to_canonical_string())))
        .collect();
    assert_eq!(
        vals, expected,
        "grouping by a decimal_arb key then ordering by it must be numeric"
    );
}

#[tokio::test]
async fn order_by_a_decimal_arb_column_not_in_the_select_list_still_sorts() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC").await,
        CORPUS_S4_ASC
    );
}

#[tokio::test]
async fn order_by_a_second_decimal_arb_column_not_in_the_select_list() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM m ORDER BY b ASC").await,
        vec![2, 4, 5, 3, 6, 1],
        "b ascending: -5, -1, 0, 1, 3, 5"
    );
}

#[tokio::test]
async fn order_by_a_non_decimal_arb_column_is_unaffected_by_the_rewrite() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY id ASC").await,
        ids_of(CORPUS_S4),
        "an Int64 sort key must be untouched by DecimalArbSortRewriteRule"
    );
}

#[tokio::test]
async fn order_by_an_int_column_descending_is_unaffected() {
    let ctx = session();
    let mut expected = ids_of(CORPUS_S4);
    expected.reverse();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY id DESC").await,
        expected
    );
}

#[tokio::test]
async fn order_by_on_a_single_row_table_is_a_no_op() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c WHERE id = 7 ORDER BY v ASC").await,
        vec![7]
    );
}

#[tokio::test]
async fn order_by_on_an_empty_result_returns_no_rows_and_keeps_metadata() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v FROM c WHERE id = 999 ORDER BY v ASC").await;
    assert_eq!(b.iter().map(|x| x.num_rows()).sum::<usize>(), 0);
    assert_arb_meta(&schema, "v", 40, 4, "empty sorted result");
}

#[tokio::test]
async fn order_by_a_scale_zero_column_keeps_scale_zero_metadata() {
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT v FROM z ORDER BY v ASC").await;
    assert_arb_meta(&schema, "v", 40, 0, "scale-0 sort");
}

#[tokio::test]
async fn order_by_a_precision_100_column_keeps_its_metadata() {
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT v FROM p ORDER BY v ASC").await;
    assert_arb_meta(&schema, "v", 100, 0, "precision-100 sort");
}

#[tokio::test]
async fn order_by_the_same_column_twice_is_idempotent() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC, v ASC").await,
        CORPUS_S4_ASC,
        "a duplicated sort key must not change the order"
    );
}

#[tokio::test]
async fn order_by_a_column_then_its_negation_is_stable_on_the_first_key() {
    let ctx = session();
    assert_eq!(
        ordered_ids(
            &ctx,
            "SELECT id FROM c ORDER BY v ASC, decimal_arb_neg(v) ASC"
        )
        .await,
        CORPUS_S4_ASC,
        "a redundant second key must not disturb the primary ordering"
    );
}

#[tokio::test]
async fn nested_order_by_twice_yields_the_outer_ordering() {
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM (SELECT id, v FROM c ORDER BY v ASC) ORDER BY v DESC",
    )
    .await;
    let mut expected = CORPUS_S4_ASC.to_vec();
    expected.reverse();
    assert_eq!(
        got, expected,
        "the outer ORDER BY must win over the inner one"
    );
}

// =====================================================================
// Section D — cross-consistency: SQL order vs comparison operators
// =====================================================================

#[tokio::test]
#[allow(clippy::needless_range_loop)]
async fn sql_less_than_agrees_with_the_sql_sort_position_for_every_pair() {
    // For every ordered pair of corpus rows, `a.v < b.v` must be TRUE exactly
    // when a precedes b in the ORDER BY result. Disagreement means WHERE and
    // ORDER BY use different orderings — silent corruption in windowed or
    // range-partitioned pipelines.
    let ctx = session();
    let order = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC").await;
    let pos = |id: i64| order.iter().position(|x| *x == id).unwrap();

    let n = CORPUS_S4.len();
    let mut left: Vec<Option<&str>> = Vec::new();
    let mut right: Vec<Option<&str>> = Vec::new();
    for a in CORPUS_S4 {
        for b in CORPUS_S4 {
            left.push(Some(a.1));
            right.push(Some(b.1));
        }
    }
    let out = invoke_cmp(
        &DecimalArbLtFunc::new(),
        (arb_storage(40, 4, &left), 40, 4),
        (arb_storage(40, 4, &right), 40, 4),
    );
    for i in 0..n {
        for j in 0..n {
            let lt = out.value(i * n + j);
            let before = pos(CORPUS_S4[i].0) < pos(CORPUS_S4[j].0);
            let equal = val(CORPUS_S4[i].1) == val(CORPUS_S4[j].1);
            if !equal {
                assert_eq!(
                    lt, before,
                    "`{}` < `{}` is {lt} but its ORDER BY position says {before}",
                    CORPUS_S4[i].1, CORPUS_S4[j].1
                );
            }
        }
    }
}

/// The corpus entries that can be written as a bare SQL integer literal.
/// (Fractional literals parse as Float64 and hit FINDING O1 below.)
const INTEGER_PIVOTS: &[&str] = &["-1000", "-100", "-9", "0", "9", "100", "1000"];

#[tokio::test]
async fn sql_where_less_than_an_integer_pivot_selects_exactly_the_sorted_prefix() {
    let ctx = session();
    let order = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC").await;
    for pivot in INTEGER_PIVOTS {
        let sql = format!("SELECT id FROM c WHERE v < {pivot} ORDER BY v ASC");
        let got = ordered_ids(&ctx, &sql).await;
        let expected: Vec<i64> = order
            .iter()
            .copied()
            .filter(|id| val(text_for(CORPUS_S4, *id)) < val(pivot))
            .collect();
        assert_eq!(
            got, expected,
            "WHERE v < {pivot} must return exactly the sorted prefix below the pivot"
        );
    }
}

#[tokio::test]
async fn sql_where_greater_than_an_integer_pivot_selects_exactly_the_sorted_suffix() {
    let ctx = session();
    let order = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC").await;
    for pivot in INTEGER_PIVOTS {
        let sql = format!("SELECT id FROM c WHERE v > {pivot} ORDER BY v ASC");
        let got = ordered_ids(&ctx, &sql).await;
        let expected: Vec<i64> = order
            .iter()
            .copied()
            .filter(|id| val(text_for(CORPUS_S4, *id)) > val(pivot))
            .collect();
        assert_eq!(got, expected, "WHERE v > {pivot} disagrees with the sort");
    }
}

#[tokio::test]
#[ignore = "FINDING O1: comparing a decimal_arb column to a FRACTIONAL SQL literal \
            fails to plan — the literal parses as Float64 and \
            DecimalArbExprPlanner::is_coercible deliberately excludes floats, so \
            planning dies with `Cannot infer common argument type for comparison \
            operation LargeBinary < Float64`. `WHERE amount < 0.5` is the most \
            natural predicate on a decimal column and is unavailable, while \
            `WHERE amount < 1` works."]
async fn sql_where_less_than_a_fractional_literal_selects_the_sorted_prefix() {
    let ctx = session();
    let got = ordered_ids(&ctx, "SELECT id FROM c WHERE v < -0.5 ORDER BY v ASC").await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| val(text_for(CORPUS_S4, *id)) < val("-0.5"))
        .collect();
    assert_eq!(got, expected);
}

#[tokio::test]
#[ignore = "FINDING O1: `decimal_arb_col = <fractional literal>` fails to plan \
            (LargeBinary = Float64)."]
async fn sql_equality_against_a_fractional_literal_matches_one_row() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c WHERE v = 0.5").await,
        vec![5]
    );
}

#[tokio::test]
#[ignore = "FINDING O1: even a whole-number literal written with a decimal point \
            (`0.0`) parses as Float64 and fails the same way, so the failure is \
            about the literal's spelling, not about losing precision."]
async fn sql_comparison_against_a_whole_number_written_with_a_decimal_point_plans() {
    let ctx = session();
    let got = ordered_ids(&ctx, "SELECT id FROM c WHERE v > 0.0 ORDER BY v ASC").await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| val(text_for(CORPUS_S4, *id)) > val("0"))
        .collect();
    assert_eq!(got, expected);
}

#[test]
fn session_parses_fractional_sql_literals_as_float_not_decimal() {
    // Root cause of FINDING O1, pinned as its own fact: SessionManager leaves
    // `parse_float_as_decimal` at DataFusion's default (false), so `0.5` in SQL
    // becomes a Float64 literal — and Float64 is exactly the type
    // `DecimalArbExprPlanner::is_coercible` refuses. Flipping this option (or
    // teaching the planner about exact float literals) is what would fix O1.
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
    let state = sm.session_state();
    assert!(
        !state.config_options().sql_parser.parse_float_as_decimal,
        "if this ever flips to true, FINDING O1 should be re-checked: fractional \
         literals would become Decimal128 and the comparison would start planning"
    );
}

#[tokio::test]
async fn sql_comparison_against_a_cast_decimal_literal_agrees_with_the_sort() {
    // The documented workaround for FINDING O1: an explicit DECIMAL cast makes
    // the literal Decimal128, which IS coercible. If this ever breaks, the
    // fractional-predicate path has no escape hatch at all.
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM c WHERE v < CAST(-0.5 AS DECIMAL(20, 4)) ORDER BY v ASC",
    )
    .await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| val(text_for(CORPUS_S4, *id)) < val("-0.5"))
        .collect();
    assert_eq!(
        got, expected,
        "CAST(<fractional> AS DECIMAL) must give a working fractional predicate"
    );
}

#[tokio::test]
async fn sql_equality_against_a_cast_decimal_literal_matches_exactly_one_row() {
    let ctx = session();
    assert_eq!(
        ordered_ids(
            &ctx,
            "SELECT id FROM c WHERE v = CAST(0.05 AS DECIMAL(20, 4))"
        )
        .await,
        vec![6],
        "0.05 must match exactly the 0.05 row, not the 0.5 row"
    );
}

#[tokio::test]
async fn sql_greater_than_a_cast_decimal_literal_agrees_with_the_sort() {
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM c WHERE v > CAST(6.5535 AS DECIMAL(20, 4)) ORDER BY v ASC",
    )
    .await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| val(text_for(CORPUS_S4, *id)) > val("6.5535"))
        .collect();
    assert_eq!(
        got, expected,
        "a pivot one ulp below 6.5536 must exclude 6.5535 and include 6.5536"
    );
}

#[tokio::test]
async fn sql_between_selects_a_contiguous_window_of_the_sorted_order() {
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM c WHERE v BETWEEN -100 AND 9 ORDER BY v ASC",
    )
    .await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| {
            let v = val(text_for(CORPUS_S4, *id));
            v >= val("-100") && v <= val("9")
        })
        .collect();
    assert_eq!(
        got, expected,
        "BETWEEN must select a contiguous window of the numeric order"
    );
}

#[tokio::test]
async fn sql_equality_matches_exactly_one_row_for_every_integer_valued_entry() {
    let ctx = session();
    for (id, t) in CORPUS_S4 {
        if !INTEGER_PIVOTS.contains(t) {
            continue;
        }
        let got = ordered_ids(&ctx, &format!("SELECT id FROM c WHERE v = {t}")).await;
        assert_eq!(got, vec![*id], "`v = {t}` must match exactly row {id}");
    }
}

#[tokio::test]
async fn sql_lte_and_gte_partition_the_corpus_at_every_integer_pivot() {
    let ctx = session();
    for t in INTEGER_PIVOTS {
        let (_, lo) = run(&ctx, &format!("SELECT id FROM c WHERE v <= {t}")).await;
        let (_, hi) = run(&ctx, &format!("SELECT id FROM c WHERE v > {t}")).await;
        let total = lo.iter().map(|b| b.num_rows()).sum::<usize>()
            + hi.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(
            total,
            CORPUS_S4.len(),
            "`v <= {t}` and `v > {t}` must partition the corpus exactly"
        );
    }
}

#[tokio::test]
async fn sql_lt_and_gte_partition_the_corpus_at_every_integer_pivot() {
    let ctx = session();
    for t in INTEGER_PIVOTS {
        let (_, lo) = run(&ctx, &format!("SELECT id FROM c WHERE v < {t}")).await;
        let (_, hi) = run(&ctx, &format!("SELECT id FROM c WHERE v >= {t}")).await;
        let total = lo.iter().map(|b| b.num_rows()).sum::<usize>()
            + hi.iter().map(|b| b.num_rows()).sum::<usize>();
        assert_eq!(
            total,
            CORPUS_S4.len(),
            "`v < {t}` and `v >= {t}` must partition the corpus exactly"
        );
    }
}

#[tokio::test]
async fn sql_between_with_fractional_bounds_selects_a_contiguous_window() {
    // BETWEEN goes through the F1b FunctionRewrite rather than the ExprPlanner,
    // so it is a separate path from the bare `<` / `>` operators.
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM c WHERE v BETWEEN CAST(-0.5 AS DECIMAL(20,4)) \
         AND CAST(0.5 AS DECIMAL(20,4)) ORDER BY v ASC",
    )
    .await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| {
            let v = val(text_for(CORPUS_S4, *id));
            v >= val("-0.5") && v <= val("0.5")
        })
        .collect();
    assert_eq!(
        got, expected,
        "BETWEEN with fractional bounds must select the contiguous numeric window"
    );
}

#[tokio::test]
async fn sql_in_list_of_integer_literals_matches_the_expected_rows() {
    let ctx = session();
    let mut got = ordered_ids(&ctx, "SELECT id FROM c WHERE v IN (-1000, 0, 1000)").await;
    got.sort_unstable();
    assert_eq!(
        got,
        vec![1, 7, 11],
        "IN over decimal_arb must match numerically, not bytewise"
    );
}

#[tokio::test]
async fn sql_comparison_of_two_decimal_arb_columns_agrees_with_the_value_type() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT a < b AS lt FROM m ORDER BY id ASC").await;
    let got = bools(&b, 0);
    let expected: Vec<Option<bool>> = vec![
        Some(val("-1000") < val("5")),
        Some(val("-1000") < val("-5")),
        Some(val("9") < val("1")),
        Some(val("9") < val("-1")),
        Some(val("100") < val("0")),
        Some(val("-9") < val("3")),
    ];
    assert_eq!(
        got, expected,
        "column-to-column `<` must match DecimalArbValue ordering"
    );
}

#[tokio::test]
async fn sql_min_and_max_agree_with_the_order_by_endpoints() {
    let ctx = session();
    let first = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC LIMIT 1").await[0];
    let last = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v DESC LIMIT 1").await[0];
    let (s, b) = run(&ctx, "SELECT MIN(v) AS mn, MAX(v) AS mx FROM c").await;
    let mn = arb_values_at_scale(&b, "mn", declared_scale(&s, "mn").unwrap_or(4));
    let mx = arb_values_at_scale(&b, "mx", declared_scale(&s, "mx").unwrap_or(4));
    assert_eq!(
        mn[0].as_deref(),
        Some(norm(val(text_for(CORPUS_S4, first)).to_canonical_string()).as_str()),
        "MIN must equal the first row of ORDER BY ... ASC"
    );
    assert_eq!(
        mx[0].as_deref(),
        Some(norm(val(text_for(CORPUS_S4, last)).to_canonical_string()).as_str()),
        "MAX must equal the first row of ORDER BY ... DESC"
    );
}

/// Read the declared scale of a column if it still carries decimal_arb metadata.
fn declared_scale(schema: &SchemaRef, name: &str) -> Option<u32> {
    let f = schema.field_with_name(name).ok()?;
    DecimalArbType::precision_scale_from_field(f).map(|(_, s)| s)
}

fn arb_values_at_scale(batches: &[RecordBatch], name: &str, scale: u32) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let c = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        for i in 0..c.len() {
            if c.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(norm(
                    DecimalArbValue::from_canonical_bytes_at_scale(c.value(i), scale)
                        .unwrap()
                        .to_canonical_string(),
                )));
            }
        }
    }
    out
}

#[tokio::test]
async fn sql_order_matches_a_rust_side_sort_by_decimal_arb_value() {
    // The final consistency check: the engine's row order and the value type's
    // own Ord must produce the same permutation.
    let ctx = session();
    let sql_order = ordered_ids(&ctx, "SELECT id FROM c ORDER BY v ASC").await;
    let mut rust_order = ids_of(CORPUS_S4);
    rust_order.sort_by_key(|a| val(text_for(CORPUS_S4, *a)));
    assert_eq!(
        sql_order, rust_order,
        "SQL ORDER BY and DecimalArbValue::cmp must produce the same permutation"
    );
}

#[tokio::test]
async fn sql_order_matches_a_rust_side_sort_for_the_scale_zero_corpus() {
    let ctx = session();
    let sql_order = ordered_ids(&ctx, "SELECT id FROM z ORDER BY v ASC").await;
    let mut rust_order = ids_of(CORPUS_S0);
    rust_order.sort_by_key(|a| val(text_for(CORPUS_S0, *a)));
    assert_eq!(sql_order, rust_order);
}

#[tokio::test]
async fn sql_order_matches_a_rust_side_sort_for_the_power_of_two_corpus() {
    let ctx = session();
    let sql_order = ordered_ids(&ctx, "SELECT id FROM p ORDER BY v ASC").await;
    let mut rust_order = ids_of(CORPUS_P2);
    rust_order.sort_by_key(|a| val(text_for(CORPUS_P2, *a)));
    assert_eq!(sql_order, rust_order);
}

#[tokio::test]
async fn ascending_and_descending_orders_are_exact_reverses_for_every_corpus() {
    let ctx = session();
    for table in ["c", "z", "p", "mb"] {
        let asc = ordered_ids(&ctx, &format!("SELECT id FROM {table} ORDER BY v ASC")).await;
        let mut desc = ordered_ids(&ctx, &format!("SELECT id FROM {table} ORDER BY v DESC")).await;
        desc.reverse();
        assert_eq!(
            asc, desc,
            "ASC and reversed DESC must agree for table `{table}` \
             (all values are distinct, so there are no tie-breaking degrees of freedom)"
        );
    }
}

#[tokio::test]
async fn sorted_output_is_monotone_when_decoded_from_the_output_bytes() {
    // Decode the sorted column and verify monotonicity directly, so the test
    // does not rely on the id mapping at all.
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v FROM c ORDER BY v ASC").await;
    let vals: Vec<DecimalArbValue> = arb_values(&schema, &b, "v")
        .into_iter()
        .map(|o| val(&o.unwrap()))
        .collect();
    for w in vals.windows(2) {
        assert!(
            w[0] < w[1],
            "sorted output is not strictly increasing: {} then {}",
            w[0],
            w[1]
        );
    }
}

#[tokio::test]
async fn descending_output_is_monotone_when_decoded_from_the_output_bytes() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v FROM c ORDER BY v DESC").await;
    let vals: Vec<DecimalArbValue> = arb_values(&schema, &b, "v")
        .into_iter()
        .map(|o| val(&o.unwrap()))
        .collect();
    for w in vals.windows(2) {
        assert!(
            w[0] > w[1],
            "descending output is not strictly decreasing: {} then {}",
            w[0],
            w[1]
        );
    }
}

#[tokio::test]
async fn sorted_output_of_the_scale_zero_corpus_is_monotone() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v FROM z ORDER BY v ASC").await;
    let vals: Vec<DecimalArbValue> = arb_values(&schema, &b, "v")
        .into_iter()
        .map(|o| val(&o.unwrap()))
        .collect();
    for w in vals.windows(2) {
        assert!(w[0] < w[1], "not increasing: {} then {}", w[0], w[1]);
    }
}

#[tokio::test]
async fn sorted_output_of_the_power_of_two_corpus_is_monotone() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v FROM p ORDER BY v ASC").await;
    let vals: Vec<DecimalArbValue> = arb_values(&schema, &b, "v")
        .into_iter()
        .map(|o| val(&o.unwrap()))
        .collect();
    for w in vals.windows(2) {
        assert!(w[0] < w[1], "not increasing: {} then {}", w[0], w[1]);
    }
}

#[tokio::test]
async fn sorting_is_a_permutation_of_the_input_multiset_of_values() {
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v FROM c ORDER BY v ASC").await;
    let mut got: Vec<String> = arb_values(&schema, &b, "v")
        .into_iter()
        .map(|o| o.unwrap())
        .collect();
    got.sort();
    let mut expected: Vec<String> = CORPUS_S4
        .iter()
        .map(|(_, t)| norm(val(t).to_canonical_string()))
        .collect();
    expected.sort();
    assert_eq!(
        got, expected,
        "the sort must be a pure permutation — no value may be altered"
    );
}

// =====================================================================
// Section E — scale interactions, derived expressions, and defaults
// =====================================================================

/// A session with `x(id, a decimal_arb(20,0), b decimal_arb(20,4), flag Boolean)`
/// — two decimal_arb columns whose **scales differ**, which is where a
/// scale-blind sort key would show up.
fn cross_scale_session() -> SessionContext {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
    let ctx = sm.session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("a", 20, 0, true).unwrap(),
        DecimalArbType::field("b", 20, 4, true).unwrap(),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            int_array(&[1, 2, 3, 4]),
            arb_array(
                "a",
                20,
                0,
                &[Some("5"), Some("-3"), Some("0"), Some("1000")],
            ),
            arb_array(
                "b",
                20,
                4,
                &[Some("0.5"), Some("-0.03"), Some("0"), Some("9.9999")],
            ),
            Arc::new(arrow::array::BooleanArray::from(vec![
                true, false, true, false,
            ])),
        ],
    )
    .unwrap();
    ctx.register_batch("x", batch).unwrap();
    ctx
}

#[test]
fn sort_key_is_scale_blind_so_keys_are_only_comparable_within_one_column() {
    // Pinned invariant, not a bug: the key encodes the *unscaled* magnitude, so
    // `5` at scale 0 and `0.5` at scale 1 produce the SAME key. Any future code
    // that compares sort keys across columns of differing scale is therefore
    // wrong by construction, and this test is the tripwire.
    assert_eq!(
        key_of("5", 0),
        key_of("0.5", 1),
        "sort keys carry no scale; this equality documents why they must never \
         be compared across columns with different declared scales"
    );
    assert_ne!(
        val("5"),
        val("0.5"),
        "the two values are of course NOT numerically equal"
    );
}

#[test]
fn sort_key_of_one_value_changes_with_the_encoding_scale() {
    assert_ne!(
        key_of("1", 0),
        key_of("1", 4),
        "the same value at different scales has different keys — a sort key is \
         only meaningful relative to its column's declared scale"
    );
}

#[tokio::test]
async fn cross_scale_column_comparison_is_numerically_correct() {
    // The comparison UDF decodes each operand at ITS OWN declared scale, so a
    // scale-0 column compared against a scale-4 column must be numeric.
    let ctx = cross_scale_session();
    let mut got = ordered_ids(&ctx, "SELECT id FROM x WHERE a > b").await;
    got.sort_unstable();
    assert_eq!(
        got,
        vec![1, 4],
        "a > b across differing scales: 5>0.5 and 1000>9.9999 only"
    );
}

#[tokio::test]
async fn cross_scale_column_equality_is_numerically_correct() {
    let ctx = cross_scale_session();
    let mut got = ordered_ids(&ctx, "SELECT id FROM x WHERE a = b").await;
    got.sort_unstable();
    assert_eq!(
        got,
        vec![3],
        "only row 3 has a = b = 0 across the two scales"
    );
}

#[tokio::test]
async fn cross_scale_column_less_than_is_numerically_correct() {
    let ctx = cross_scale_session();
    let mut got = ordered_ids(&ctx, "SELECT id FROM x WHERE a < b").await;
    got.sort_unstable();
    assert_eq!(got, vec![2], "-3 < -0.03 is the only row where a < b");
}

#[tokio::test]
async fn order_by_each_of_two_differently_scaled_columns_is_numeric() {
    let ctx = cross_scale_session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM x ORDER BY a ASC").await,
        vec![2, 3, 1, 4],
        "scale-0 column: -3, 0, 5, 1000"
    );
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM x ORDER BY b ASC").await,
        vec![2, 3, 1, 4],
        "scale-4 column: -0.03, 0, 0.5, 9.9999"
    );
}

#[tokio::test]
#[ignore = "FINDING O3: a CASE whose decimal_arb branches have DIFFERENT scales \
            plans and executes with no error but comes back in raw bytewise \
            LargeBinary order — every negative sorts AFTER every positive. \
            `SELECT id FROM x ORDER BY CASE WHEN flag THEN a ELSE b END ASC` \
            returns [3,4,1,2] (bytewise on [00] < [00,01,86,9F] < [00,05] < \
            [FF,01,2C]) instead of the numeric [2,3,1,4]. The F2 metadata \
            re-stamp (decimal_arb_with_meta) only fires when all branches share \
            a scale, so the mixed-scale CASE stays bare LargeBinary and \
            DecimalArbSortRewriteRule never sees it. Silent, not an error."]
async fn order_by_a_case_mixing_two_differently_scaled_decimal_arb_branches_is_numeric() {
    let ctx = cross_scale_session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM x ORDER BY CASE WHEN flag THEN a ELSE b END ASC",
    )
    .await;
    assert_eq!(
        got,
        vec![2, 3, 1, 4],
        "CASE picks a=5, b=-0.03, a=0, b=9.9999 for ids 1..4, so the numeric \
         ascending order is -0.03, 0, 5, 9.9999"
    );
}

#[tokio::test]
#[ignore = "FINDING O3 (root cause): a CASE mixing decimal_arb branches of \
            different scales yields a bare LargeBinary output field with no \
            streamling.decimal_arb metadata — which is what disables the ORDER BY \
            rewrite and would also make a sink emit the column as raw BYTEA."]
async fn case_over_two_differently_scaled_decimal_arb_branches_keeps_metadata() {
    let ctx = cross_scale_session();
    let (schema, _) = run(
        &ctx,
        "SELECT CASE WHEN flag THEN a ELSE b END AS chosen FROM x",
    )
    .await;
    let f = field_of(&schema, "chosen");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "mixed-scale CASE dropped decimal_arb metadata: {f:?}"
    );
}

#[tokio::test]
async fn order_by_a_case_over_same_scale_decimal_arb_branches_is_numeric() {
    // Regression guard for the F2 fix: when the branches DO share a scale the
    // metadata survives, the sort rewrite fires, and the order is numeric.
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM m ORDER BY CASE WHEN g = 1 THEN a ELSE b END ASC, id ASC",
    )
    .await;
    // g=1 rows take a: id1 -1000, id2 -1000, id5 100.
    // g=2 rows take b: id3 1, id4 -1, id6 3.
    // ascending: -1000(1), -1000(2), -1(4), 1(3), 3(6), 100(5)
    assert_eq!(
        got,
        vec![1, 2, 4, 3, 6, 5],
        "a same-scale CASE must keep its decimal_arb metadata and sort numerically"
    );
}

#[tokio::test]
async fn order_by_a_coalesce_over_same_scale_decimal_arb_columns_is_numeric() {
    let ctx = session();
    let got = ordered_ids(&ctx, "SELECT id FROM m ORDER BY COALESCE(a, b) ASC, id ASC").await;
    assert_eq!(
        got,
        vec![1, 2, 6, 3, 4, 5],
        "COALESCE(a, b) is just `a` here (no NULLs), so the order must match \
         ORDER BY a ASC, id ASC"
    );
}

#[tokio::test]
async fn order_by_a_doubled_decimal_arb_expression_preserves_the_order() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v * 2 ASC").await,
        CORPUS_S4_ASC,
        "multiplying by a positive constant is order-preserving"
    );
}

#[tokio::test]
async fn order_by_a_decimal_arb_expression_scaled_by_minus_one_reverses_the_order() {
    let ctx = session();
    let mut expected = CORPUS_S4_ASC.to_vec();
    expected.reverse();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v * -1 ASC").await,
        expected,
        "multiplying by a negative constant must reverse the order"
    );
}

#[tokio::test]
async fn order_by_a_divided_decimal_arb_expression_preserves_the_order() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v / 3 ASC").await,
        CORPUS_S4_ASC,
        "dividing by a positive constant (which widens the scale to 18) must \
         still be order-preserving"
    );
}

#[tokio::test]
async fn order_by_a_shifted_decimal_arb_expression_preserves_the_order() {
    let ctx = session();
    assert_eq!(
        ordered_ids(&ctx, "SELECT id FROM c ORDER BY v + 1000000 ASC").await,
        CORPUS_S4_ASC,
        "adding a constant must not change the relative order"
    );
}

#[tokio::test]
async fn order_by_an_expression_whose_result_scale_differs_from_the_input() {
    // v / 3 has scale 18 while v has scale 4; the sort key must be built from
    // the *result* column's bytes, consistently for every row.
    let ctx = session();
    let (schema, b) = run(&ctx, "SELECT v / 3 AS q FROM c ORDER BY v / 3 ASC").await;
    let f = field_of(&schema, "q");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "the divided column must stay decimal_arb: {f:?}"
    );
    let vals: Vec<DecimalArbValue> = arb_values(&schema, &b, "q")
        .into_iter()
        .map(|o| val(&o.unwrap()))
        .collect();
    for w in vals.windows(2) {
        assert!(
            w[0] <= w[1],
            "quotient column is not monotonically increasing: {} then {}",
            w[0],
            w[1]
        );
    }
}

#[tokio::test]
async fn explicit_sort_key_projection_is_plain_large_binary_without_metadata() {
    // The sort key is not a value: projecting it must NOT claim to be
    // decimal_arb, or a sink would try to render the key as a number.
    let ctx = session();
    let (schema, _) = run(&ctx, "SELECT decimal_arb_to_sort_key(v) AS k FROM c").await;
    let f = field_of(&schema, "k");
    assert_eq!(f.data_type(), &DataType::LargeBinary);
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "a sort key must not carry decimal_arb metadata: {f:?}"
    );
}

#[tokio::test]
async fn explicit_sort_key_values_match_the_rust_side_encoder() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT decimal_arb_to_sort_key(v) AS k FROM c ORDER BY id ASC",
    )
    .await;
    let mut got: Vec<Vec<u8>> = Vec::new();
    for batch in &b {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        for i in 0..col.len() {
            got.push(col.value(i).to_vec());
        }
    }
    let expected: Vec<Vec<u8>> = CORPUS_S4.iter().map(|(_, t)| key_of(t, 4)).collect();
    assert_eq!(
        got, expected,
        "the SQL sort-key UDF must produce byte-identical keys to the Rust helper"
    );
}

#[tokio::test]
async fn sort_key_udf_emits_null_for_null_input_rows() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT decimal_arb_to_sort_key(v) AS k FROM n ORDER BY id ASC",
    )
    .await;
    let mut nulls = Vec::new();
    for batch in &b {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        for i in 0..col.len() {
            nulls.push(col.is_null(i));
        }
    }
    assert_eq!(
        nulls,
        vec![false, true, false, true, false],
        "a NULL value must key as NULL, never as the zero key (which would sort \
         NULLs in among the values)"
    );
}

#[tokio::test]
async fn top_k_with_nulls_does_not_let_nulls_displace_real_values() {
    let ctx = session();
    let got = opt_ints(
        &run(&ctx, "SELECT id FROM n ORDER BY v ASC NULLS LAST LIMIT 3")
            .await
            .1,
        "id",
    );
    assert_eq!(
        got,
        vec![Some(3), Some(5), Some(1)],
        "top-3 with NULLS LAST must be the three real values -5, 0, 5"
    );
}

#[tokio::test]
async fn top_k_with_nulls_first_returns_the_nulls_first() {
    let ctx = session();
    let got = opt_ints(
        &run(&ctx, "SELECT id FROM n ORDER BY v ASC NULLS FIRST LIMIT 3")
            .await
            .1,
        "id",
    );
    assert_eq!(
        got,
        vec![Some(2), Some(4), Some(3)],
        "top-3 with NULLS FIRST must be the two NULLs then -5"
    );
}

#[tokio::test]
async fn ascending_default_null_placement_is_last() {
    let ctx = session();
    let default = opt_ints(&run(&ctx, "SELECT id FROM n ORDER BY v ASC").await.1, "id");
    let explicit = opt_ints(
        &run(&ctx, "SELECT id FROM n ORDER BY v ASC NULLS LAST")
            .await
            .1,
        "id",
    );
    assert_eq!(
        default, explicit,
        "ASC without an explicit NULLS clause must behave as NULLS LAST"
    );
}

#[tokio::test]
async fn descending_default_null_placement_is_first() {
    let ctx = session();
    let default = opt_ints(&run(&ctx, "SELECT id FROM n ORDER BY v DESC").await.1, "id");
    let explicit = opt_ints(
        &run(&ctx, "SELECT id FROM n ORDER BY v DESC NULLS FIRST")
            .await
            .1,
        "id",
    );
    assert_eq!(
        default, explicit,
        "DESC without an explicit NULLS clause must behave as NULLS FIRST"
    );
}

#[tokio::test]
async fn order_by_a_column_of_all_equal_values_returns_every_row() {
    let ctx = session();
    let got = ordered_ids(&ctx, "SELECT id FROM c WHERE v = 0 OR v = 0 ORDER BY v ASC").await;
    assert_eq!(got, vec![7]);
}

#[tokio::test]
async fn order_by_decimal_arb_then_a_string_column_resolves_ties_on_the_string() {
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM m ORDER BY a ASC, CAST(id AS VARCHAR) ASC",
    )
    .await;
    assert_eq!(
        got,
        vec![1, 2, 6, 3, 4, 5],
        "a=-1000 ties break on '1' < '2'; a=9 ties break on '3' < '4'"
    );
}

#[tokio::test]
async fn order_by_a_decimal_arb_column_from_a_join_free_self_reference() {
    let ctx = session();
    let got = ordered_ids(
        &ctx,
        "SELECT id FROM (SELECT id, v FROM c WHERE v <> 0) ORDER BY v ASC",
    )
    .await;
    let expected: Vec<i64> = CORPUS_S4_ASC
        .iter()
        .copied()
        .filter(|id| *id != 7)
        .collect();
    assert_eq!(got, expected, "`v <> 0` must exclude only the zero row");
}

#[tokio::test]
async fn order_by_survives_a_having_clause_over_a_decimal_arb_group_key() {
    let ctx = session();
    let (schema, b) = run(
        &ctx,
        "SELECT v FROM c GROUP BY v HAVING COUNT(*) = 1 ORDER BY v ASC",
    )
    .await;
    assert_arb_meta(&schema, "v", 40, 4, "GROUP BY + HAVING + ORDER BY");
    let vals: Vec<DecimalArbValue> = arb_values(&schema, &b, "v")
        .into_iter()
        .map(|o| val(&o.unwrap()))
        .collect();
    assert_eq!(
        vals.len(),
        CORPUS_S4.len(),
        "every value occurs exactly once"
    );
    for w in vals.windows(2) {
        assert!(w[0] < w[1], "HAVING must not disturb the numeric order");
    }
}
