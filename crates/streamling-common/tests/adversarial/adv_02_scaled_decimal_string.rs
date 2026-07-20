//! Adversarial integration tests for the high-precision *scaled* decimal path:
//! avro `decimal` with precision > 76 AND scale > 0 maps to an Arrow `Utf8` field carrying
//! `AVRO_DECIMAL_SCALE_META`, and the decoded bytes are formatted as a scale-aware decimal string
//! (via `BigDecimal(unscaled, scale)`, trailing fractional zeros trimmed) rather than reinterpreted
//! as raw text.
//!
//! Two exercise paths:
//!   * `coerce_batch_to_target` fed a hand-built `Binary`/`LargeBinary` source column (directly
//!     targets `binary_to_decimal_string` / `format_decimal_bytes_with_scale`), and
//!   * full `ConfluentAvroDecoder` round-trips (positive/zero only, to avoid any ambiguity in how
//!     `apache_avro::Decimal` re-encodes negative magnitudes).
//!
//! Expected strings are computed by an INDEPENDENT formatter (`oracle`) that splits the unscaled
//! decimal digits manually — it does not call `BigDecimal`, so a divergence between it and the
//! implementation is a real signal. The canonical spec examples are additionally pinned as string
//! literals for the strongest cross-check.

use apache_avro::types::{Record, Value};
use apache_avro::{Decimal, Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{Array, ArrayRef, BinaryArray, LargeBinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bigdecimal::num_bigint::BigInt;
use std::str::FromStr;
use std::sync::Arc;

use streamling_common::formats::avro::arrow_avro::{
    AVRO_DECIMAL_SCALE_META, ConfluentAvroDecoder, coerce_batch_to_target,
};
use streamling_common::formats::avro::convert_avro_schema_to_arrow;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn confluent_frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0x00];
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(body);
    f
}

fn bi(v: i128) -> BigInt {
    BigInt::from_str(&v.to_string()).unwrap()
}

fn bi_str(s: &str) -> BigInt {
    BigInt::from_str(s).unwrap()
}

/// Big-endian two's-complement bytes for an unscaled value (what the avro wire carries).
fn sbytes(v: &BigInt) -> Vec<u8> {
    v.to_signed_bytes_be()
}

/// Independent oracle for the correct scaled-decimal string: split the unscaled digit string
/// manually, insert the decimal point `scale` places from the right, then trim trailing fractional
/// zeros (and a bare trailing '.'). Deliberately does NOT use `BigDecimal`.
fn oracle(unscaled: &BigInt, scale: u64) -> String {
    let full = unscaled.to_str_radix(10); // includes leading '-' for negatives
    let (neg, digits) = match full.strip_prefix('-') {
        Some(rest) => (true, rest.to_string()),
        None => (false, full),
    };
    let s = scale as usize;
    let mut result = if s == 0 {
        digits
    } else if digits.len() > s {
        let (int_part, frac_part) = digits.split_at(digits.len() - s);
        format!("{int_part}.{frac_part}")
    } else {
        let zeros = "0".repeat(s - digits.len());
        format!("0.{zeros}{digits}")
    };
    if s > 0 && result.contains('.') {
        result = result
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string();
    }
    if result.is_empty() {
        result = "0".to_string();
    }
    if neg && result != "0" {
        format!("-{result}")
    } else {
        result
    }
}

/// Nullable single-field avro record whose field `v` is a `["null", decimal(p,s)]`. Its Arrow
/// target (via `convert_avro_schema_to_arrow`) is a nullable `Utf8` field carrying the scale meta.
fn nullable_decimal_json(precision: u64, scale: u64) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"v","type":["null",{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":{scale}}}]}}]}}"#
    )
}

/// Non-nullable single-field avro record with a bare `decimal(p,s)` field.
fn plain_decimal_json(precision: u64, scale: u64) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"v","type":{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":{scale}}}}}]}}"#
    )
}

fn target_schema(precision: u64, scale: u64) -> SchemaRef {
    let schema = AvroWriterSchema::parse_str(&nullable_decimal_json(precision, scale)).unwrap();
    convert_avro_schema_to_arrow(schema)
}

/// Run the coerce path over a hand-built `Binary` (or `LargeBinary`) source column and return the
/// resulting `Utf8` column. This exercises `binary_to_decimal_string` directly.
fn coerce_decimal_strings(
    precision: u64,
    scale: u64,
    rows: &[Option<Vec<u8>>],
    large: bool,
) -> StringArray {
    let target = target_schema(precision, scale);
    let opt_slices: Vec<Option<&[u8]>> = rows.iter().map(|o| o.as_deref()).collect();
    let (src_field, col): (Field, ArrayRef) = if large {
        (
            Field::new("v", DataType::LargeBinary, true),
            Arc::new(LargeBinaryArray::from(opt_slices)),
        )
    } else {
        (
            Field::new("v", DataType::Binary, true),
            Arc::new(BinaryArray::from(opt_slices)),
        )
    };
    let src_schema = Arc::new(Schema::new(vec![src_field]));
    let batch = RecordBatch::try_new(src_schema, vec![col]).unwrap();
    let out = coerce_batch_to_target(&batch, &target).unwrap();
    out.column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("high-precision scaled decimal must coerce to a Utf8 column")
        .clone()
}

/// Convenience: coerce a single unscaled value and return the formatted string.
fn fmt(precision: u64, scale: u64, unscaled: &BigInt) -> String {
    let arr = coerce_decimal_strings(precision, scale, &[Some(sbytes(unscaled))], false);
    arr.value(0).to_string()
}

/// Full ConfluentAvroDecoder round-trip for a nullable decimal column (positive/zero values only).
fn decode_nullable_decimal(precision: u64, scale: u64, rows: &[Option<BigInt>]) -> StringArray {
    let json = nullable_decimal_json(precision, scale);
    let schema = AvroWriterSchema::parse_str(&json).unwrap();
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(7, &json).unwrap();
    for r in rows {
        let mut rec = Record::new(&schema).unwrap();
        match r {
            Some(v) => rec.put(
                "v",
                Value::Union(1, Box::new(Value::Decimal(Decimal::from(sbytes(v))))),
            ),
            None => rec.put("v", Value::Union(0, Box::new(Value::Null))),
        }
        let body = to_avro_datum(&schema, rec).unwrap();
        d.decode(&confluent_frame(7, &body)).unwrap();
    }
    let batch = d.flush().unwrap().expect("batch");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 column")
        .clone()
}

// ---------------------------------------------------------------------------
// Broad table-driven cross-check against the independent oracle
// ---------------------------------------------------------------------------

#[test]
fn table_scaled_decimal_matches_independent_oracle() {
    // scale > 0 across the assigned spread of scales.
    let scales: [u64; 6] = [1, 2, 4, 10, 18, 30];
    // A spread of unscaled values: zero, tiny positive/negative, mid, huge positive/negative.
    let values: Vec<BigInt> = vec![
        bi(0),
        bi(1),
        bi(5),
        bi(-1),
        bi(-5),
        bi(123),
        bi(-123),
        bi(1000),
        bi(-1000),
        bi(12345),
        bi(-12345),
        bi(100000),
        bi(999999999),
        bi_str("123456789012345678901234567890"),
        bi_str("-98765432109876543210987654321"),
        bi_str("1000000000000000000000000000000"),
    ];
    // precision 100 (the max streamling accepts) comfortably holds every value + scale.
    for &scale in &scales {
        for v in &values {
            let expected = oracle(v, scale);
            let got = fmt(100, scale, v);
            assert_eq!(
                got, expected,
                "scale={scale} unscaled={v}: formatted decimal string mismatch"
            );
        }
    }
}

#[test]
fn table_precision_77_matches_oracle() {
    // Same breadth but at the *lower* boundary precision (77) to make sure the metadata-driven
    // formatting is independent of precision.
    let scales: [u64; 5] = [1, 2, 4, 10, 18];
    let values: Vec<BigInt> = vec![
        bi(0),
        bi(7),
        bi(-7),
        bi(4200),
        bi(-4200),
        bi(1000000000000000000),
        bi_str("9".repeat(40).as_str()),
    ];
    for &scale in &scales {
        for v in &values {
            let expected = oracle(v, scale);
            let got = fmt(77, scale, v);
            assert_eq!(got, expected, "p=77 scale={scale} unscaled={v}");
        }
    }
}

// ---------------------------------------------------------------------------
// Canonical spec examples pinned as string literals (strongest cross-check)
// ---------------------------------------------------------------------------

#[test]
fn literal_sub_one_scale4_unscaled5() {
    // value < 1 gains a leading "0." — the exact example in the spec.
    assert_eq!(fmt(100, 4, &bi(5)), "0.0005");
}

#[test]
fn literal_trailing_zeros_trimmed_1_23() {
    // "1.2300" -> "1.23"
    assert_eq!(fmt(100, 4, &bi(12300)), "1.23");
}

#[test]
fn literal_integer_valued_100_scale3() {
    // "100.000" -> "100"
    assert_eq!(fmt(100, 3, &bi(100000)), "100");
}

#[test]
fn literal_hundred_scale2_trims() {
    // 10000 @ scale 2 == "100.00" -> "100"
    assert_eq!(fmt(100, 2, &bi(10000)), "100");
}

#[test]
fn literal_half_scale1() {
    assert_eq!(fmt(100, 1, &bi(5)), "0.5");
}

#[test]
fn literal_one_scale1() {
    assert_eq!(fmt(100, 1, &bi(10)), "1");
}

#[test]
fn literal_ten_scale1() {
    assert_eq!(fmt(100, 1, &bi(100)), "10");
}

#[test]
fn literal_negative_sub_one_scale4() {
    assert_eq!(fmt(100, 4, &bi(-5)), "-0.0005");
}

#[test]
fn literal_negative_trim_scale4() {
    assert_eq!(fmt(100, 4, &bi(-12300)), "-1.23");
}

#[test]
fn literal_negative_integer_scale3() {
    assert_eq!(fmt(100, 3, &bi(-1000)), "-1");
}

#[test]
fn literal_mixed_fractional_zeros_scale4() {
    // 120500 @ scale 4 == "12.0500" -> "12.05" (only *trailing* zeros trimmed)
    assert_eq!(fmt(100, 4, &bi(120500)), "12.05");
}

#[test]
fn literal_high_bit_negative_byte_scale2() {
    // single byte 0x80 == -128 -> "-1.28"
    let arr = coerce_decimal_strings(100, 2, &[Some(vec![0x80])], false);
    assert_eq!(arr.value(0), "-1.28");
}

#[test]
fn literal_max_positive_byte_scale2() {
    // single byte 0x7F == 127 -> "1.27"
    let arr = coerce_decimal_strings(100, 2, &[Some(vec![0x7F])], false);
    assert_eq!(arr.value(0), "1.27");
}

#[test]
fn literal_256_scale4_is_clean_utf8() {
    // 0x0100 == 256 @ scale 4 -> "0.0256"; verifies bytes are formatted, not cast to raw text.
    let arr = coerce_decimal_strings(100, 4, &[Some(vec![0x01, 0x00])], false);
    assert_eq!(arr.value(0), "0.0256");
    assert!(
        arr.value(0)
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.' || c == '-'),
        "output must be a clean decimal string, got {:?}",
        arr.value(0)
    );
}

#[test]
fn literal_zero_scale4_is_bare_zero() {
    assert_eq!(fmt(100, 4, &bi(0)), "0");
}

#[test]
fn literal_zero_various_scales_all_bare_zero() {
    for &scale in &[1u64, 2, 4, 10, 18, 30] {
        assert_eq!(fmt(100, scale, &bi(0)), "0", "zero at scale {scale}");
    }
}

#[test]
fn literal_one_wei_scale18() {
    // 1 @ scale 18 -> "0." + 17 zeros + "1"
    let expected = format!("0.{}1", "0".repeat(17));
    assert_eq!(fmt(100, 18, &bi(1)), expected);
}

#[test]
fn literal_1_5_scale18() {
    assert_eq!(fmt(100, 18, &bi(1_500_000_000_000_000_000)), "1.5");
}

#[test]
fn literal_1_23_scale18() {
    assert_eq!(fmt(100, 18, &bi(1_230_000_000_000_000_000)), "1.23");
}

#[test]
fn literal_whole_one_scale18() {
    assert_eq!(fmt(100, 18, &bi(1_000_000_000_000_000_000)), "1");
}

#[test]
fn literal_one_smallest_scale30() {
    let expected = format!("0.{}1", "0".repeat(29));
    assert_eq!(fmt(100, 30, &bi(1)), expected);
}

#[test]
fn literal_ten_pow_30_scale30_is_one() {
    let v = bi_str(&format!("1{}", "0".repeat(30)));
    assert_eq!(fmt(100, 30, &v), "1");
}

#[test]
fn literal_1_23_scale10() {
    assert_eq!(fmt(100, 10, &bi(12_300_000_000)), "1.23");
}

#[test]
fn literal_sub_one_scale10() {
    let expected = format!("0.{}5", "0".repeat(9));
    assert_eq!(fmt(100, 10, &bi(5)), expected);
}

// ---------------------------------------------------------------------------
// Byte-encoding edge cases (sign extension, leading zeros, empty, over-32-bytes)
// ---------------------------------------------------------------------------

#[test]
fn edge_leading_zero_bytes_positive() {
    // Redundant leading 0x00 bytes must not change the value.
    let arr = coerce_decimal_strings(100, 2, &[Some(vec![0x00, 0x00, 0x7B])], false);
    assert_eq!(arr.value(0), "1.23");
}

#[test]
fn edge_sign_extended_negative_bytes() {
    // Redundant leading 0xFF sign-extension bytes must not change the value.
    let arr = coerce_decimal_strings(100, 4, &[Some(vec![0xFF, 0xFF, 0xFB])], false);
    assert_eq!(arr.value(0), "-0.0005");
}

#[test]
fn edge_empty_bytes_is_zero() {
    // avro may encode a zero-valued decimal as empty bytes; from_signed_bytes_be(&[]) == 0.
    let arr = coerce_decimal_strings(100, 4, &[Some(vec![])], false);
    assert_eq!(arr.value(0), "0");
}

#[test]
fn edge_value_needing_more_than_32_bytes() {
    // The scaled-string path has NO 32-byte cap (unlike u256/i256). A value wider than 32 bytes
    // must still format correctly.
    let v = bi_str(&format!("1{}", "0".repeat(80))); // 10^80, ~34 bytes
    let expected = oracle(&v, 10);
    assert_eq!(fmt(100, 10, &v), expected);
    // Sanity: it really did need > 32 bytes.
    assert!(sbytes(&v).len() > 32, "test value should exceed 32 bytes");
}

#[test]
fn edge_huge_positive_scale18() {
    let v = bi_str(&format!("{}", "1234567890".repeat(4))); // 40 digits
    assert_eq!(fmt(100, 18, &v), oracle(&v, 18));
}

#[test]
fn edge_huge_negative_scale30() {
    let v = bi_str(&format!("-{}", "9".repeat(60)));
    assert_eq!(fmt(100, 30, &v), oracle(&v, 30));
}

#[test]
fn edge_hundred_digit_value_scale30() {
    // precision exactly 100.
    let v = bi_str(&"7".repeat(99));
    assert_eq!(fmt(100, 30, &v), oracle(&v, 30));
}

#[test]
fn edge_same_bytes_different_scale_reads_metadata() {
    // The scale must come from field metadata, not a hardcoded assumption: the SAME unscaled bytes
    // formatted at different scales must differ accordingly.
    let v = bi(123456);
    assert_eq!(fmt(100, 1, &v), "12345.6");
    assert_eq!(fmt(100, 2, &v), "1234.56");
    assert_eq!(fmt(100, 4, &v), "12.3456");
    assert_eq!(fmt(100, 10, &v), oracle(&v, 10));
}

#[test]
fn edge_large_binary_source_column() {
    // as_binary_iter also accepts LargeBinary; the coerce path must handle it identically.
    let arr = coerce_decimal_strings(100, 4, &[Some(sbytes(&bi(12300)))], true);
    assert_eq!(arr.value(0), "1.23");
}

#[test]
fn edge_large_binary_negative_and_null() {
    let rows = vec![Some(sbytes(&bi(-5))), None, Some(sbytes(&bi(10000)))];
    let arr = coerce_decimal_strings(100, 4, &rows, true);
    assert_eq!(arr.value(0), "-0.0005");
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), "1");
}

// ---------------------------------------------------------------------------
// Null handling / batches
// ---------------------------------------------------------------------------

#[test]
fn null_rows_preserved_in_coerce() {
    let rows = vec![Some(sbytes(&bi(5))), None, Some(sbytes(&bi(-12300))), None];
    let arr = coerce_decimal_strings(100, 4, &rows, false);
    assert_eq!(arr.len(), 4);
    assert_eq!(arr.value(0), "0.0005");
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2), "-1.23");
    assert!(arr.is_null(3));
    assert_eq!(arr.null_count(), 2);
}

#[test]
fn all_null_column() {
    let rows = vec![None, None, None];
    let arr = coerce_decimal_strings(100, 4, &rows, false);
    assert_eq!(arr.len(), 3);
    assert_eq!(arr.null_count(), 3);
    assert!((0..3).all(|i| arr.is_null(i)));
}

#[test]
fn mixed_batch_many_values_scale4() {
    let cases: Vec<(BigInt, &str)> = vec![
        (bi(5), "0.0005"),
        (bi(-5), "-0.0005"),
        (bi(0), "0"),
        (bi(10000), "1"),
        (bi(15000), "1.5"),
        (bi(12300), "1.23"),
        (bi(-12300), "-1.23"),
        (bi(100000), "10"),
        (bi(123456), "12.3456"),
        (bi(1), "0.0001"),
        (bi(-1), "-0.0001"),
    ];
    let rows: Vec<Option<Vec<u8>>> = cases.iter().map(|(v, _)| Some(sbytes(v))).collect();
    let arr = coerce_decimal_strings(100, 4, &rows, false);
    assert_eq!(arr.len(), cases.len());
    for (i, (_, expected)) in cases.iter().enumerate() {
        assert_eq!(arr.value(i), *expected, "row {i}");
    }
}

#[test]
fn row_count_preserved_large_batch() {
    let rows: Vec<Option<Vec<u8>>> = (0..500)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some(sbytes(&bi(i as i128 * 137 - 40000)))
            }
        })
        .collect();
    let arr = coerce_decimal_strings(100, 6, &rows, false);
    assert_eq!(arr.len(), 500);
    for (i, r) in rows.iter().enumerate() {
        match r {
            None => assert!(arr.is_null(i), "row {i} should be null"),
            Some(b) => {
                let v = BigInt::from_signed_bytes_be(b);
                assert_eq!(arr.value(i), oracle(&v, 6), "row {i}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Target-schema (metadata / dtype) oracle checks
// ---------------------------------------------------------------------------

#[test]
fn dtype_is_utf8_for_p77_s1() {
    let t = target_schema(77, 1);
    assert_eq!(t.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn dtype_is_utf8_for_p100_s30() {
    let t = target_schema(100, 30);
    assert_eq!(t.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn field_carries_scale_metadata() {
    for &scale in &[1u64, 2, 4, 10, 18, 30] {
        let t = target_schema(100, scale);
        let meta = t.field(0).metadata();
        assert_eq!(
            meta.get(AVRO_DECIMAL_SCALE_META),
            Some(&scale.to_string()),
            "scale meta must equal the avro scale (scale={scale})"
        );
    }
}

#[test]
fn field_is_nullable_when_union_wrapped() {
    let t = target_schema(100, 4);
    assert!(t.field(0).is_nullable());
}

#[test]
fn boundary_p76_is_decimal256_not_utf8() {
    // precision 76 with scale > 0 is Decimal256, NOT the Utf8 path.
    let schema = AvroWriterSchema::parse_str(&nullable_decimal_json(76, 5)).unwrap();
    let t = convert_avro_schema_to_arrow(schema);
    assert_eq!(t.field(0).data_type(), &DataType::Decimal256(76, 5));
    assert!(!t.field(0).metadata().contains_key(AVRO_DECIMAL_SCALE_META));
}

#[test]
fn boundary_p77_s0_is_fixed_binary_not_utf8() {
    // precision 77 with scale 0 is the u256 FixedSizeBinary path, NOT the scaled-string path.
    let schema = AvroWriterSchema::parse_str(&nullable_decimal_json(77, 0)).unwrap();
    let t = convert_avro_schema_to_arrow(schema);
    assert_eq!(t.field(0).data_type(), &DataType::FixedSizeBinary(32));
    assert!(
        !t.field(0).metadata().contains_key(AVRO_DECIMAL_SCALE_META),
        "scale-0 high-precision decimal must not be tagged with decimal scale meta"
    );
}

#[test]
fn plain_string_field_has_no_scale_metadata() {
    // A genuine string field must NOT carry the scale meta, so plain strings pass through untouched.
    const JSON: &str = r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#;
    let schema = AvroWriterSchema::parse_str(JSON).unwrap();
    let t = convert_avro_schema_to_arrow(schema);
    assert_eq!(t.field(0).data_type(), &DataType::Utf8);
    assert!(!t.field(0).metadata().contains_key(AVRO_DECIMAL_SCALE_META));
}

// ---------------------------------------------------------------------------
// Full ConfluentAvroDecoder round-trips (positive / zero only)
// ---------------------------------------------------------------------------

#[test]
fn e2e_positive_1_23_scale2() {
    let arr = decode_nullable_decimal(100, 2, &[Some(bi(123))]);
    assert_eq!(arr.value(0), "1.23");
}

#[test]
fn e2e_trailing_zero_trims_to_100_scale2() {
    let arr = decode_nullable_decimal(100, 2, &[Some(bi(10000))]);
    assert_eq!(arr.value(0), "100");
}

#[test]
fn e2e_sub_one_scale4() {
    let arr = decode_nullable_decimal(100, 4, &[Some(bi(5))]);
    assert_eq!(arr.value(0), "0.0005");
}

#[test]
fn e2e_zero_scale4() {
    let arr = decode_nullable_decimal(100, 4, &[Some(bi(0))]);
    assert_eq!(arr.value(0), "0");
}

#[test]
fn e2e_integer_100_scale3() {
    let arr = decode_nullable_decimal(100, 3, &[Some(bi(100000))]);
    assert_eq!(arr.value(0), "100");
}

#[test]
fn e2e_big_value_scale18() {
    let v = bi(1_230_000_000_000_000_000);
    let arr = decode_nullable_decimal(100, 18, &[Some(v.clone())]);
    assert_eq!(arr.value(0), "1.23");
    assert_eq!(arr.value(0), oracle(&v, 18));
}

#[test]
fn e2e_precision_77_scale1() {
    let arr = decode_nullable_decimal(77, 1, &[Some(bi(125))]);
    assert_eq!(arr.value(0), "12.5");
}

#[test]
fn e2e_nullable_with_null_row() {
    let arr = decode_nullable_decimal(100, 2, &[None, Some(bi(123)), None]);
    assert_eq!(arr.len(), 3);
    assert!(arr.is_null(0));
    assert_eq!(arr.value(1), "1.23");
    assert!(arr.is_null(2));
}

#[test]
fn e2e_many_values_one_batch() {
    let cases: Vec<(i128, &str)> = vec![
        (123, "1.23"),
        (100, "1"),
        (1, "0.01"),
        (999, "9.99"),
        (10000, "100"),
        (10050, "100.5"),
        (0, "0"),
    ];
    let rows: Vec<Option<BigInt>> = cases.iter().map(|(v, _)| Some(bi(*v))).collect();
    let arr = decode_nullable_decimal(100, 2, &rows);
    assert_eq!(arr.len(), cases.len());
    for (i, (_, expected)) in cases.iter().enumerate() {
        assert_eq!(arr.value(i), *expected, "row {i}");
    }
}

#[test]
fn e2e_field_dtype_is_utf8_with_scale_meta() {
    // The decoded batch's schema field must be Utf8 and carry the scale metadata end-to-end.
    let json = nullable_decimal_json(100, 4);
    let schema = AvroWriterSchema::parse_str(&json).unwrap();
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(9, &json).unwrap();
    let mut rec = Record::new(&schema).unwrap();
    rec.put(
        "v",
        Value::Union(
            1,
            Box::new(Value::Decimal(Decimal::from(sbytes(&bi(12300))))),
        ),
    );
    let body = to_avro_datum(&schema, rec).unwrap();
    d.decode(&confluent_frame(9, &body)).unwrap();
    let batch = d.flush().unwrap().expect("batch");
    let field = batch.schema().field(0).clone();
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert_eq!(
        field.metadata().get(AVRO_DECIMAL_SCALE_META),
        Some(&"4".to_string())
    );
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "1.23");
}
