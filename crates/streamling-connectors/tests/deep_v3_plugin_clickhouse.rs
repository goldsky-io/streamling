//! Actual sink path for a companion-plugin legacy U256 source batch.
use arrow::{
    array::{Array, FixedSizeBinaryArray, Int32Array, LargeBinaryArray, StringArray},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    catalog::TableProvider,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, dml::InsertOp},
    physical_plan::collect,
    prelude::SessionContext,
};
use std::{collections::HashMap, sync::Arc};
use streamling_config::{ClickHouseCompression, ClickHouseConfig, GzipCompressionLevel};
use streamling_connectors::table_providers::clickhouse::{
    ClickHouseClient, ClickHouseTableProvider, clickhouse_native_to_decimal_arb,
};
use streamling_core::functions::byte_reverse::ReverseBytes32Func;
use streamling_core::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, NativeIntKind,
};

fn review_clickhouse_config() -> ClickHouseConfig {
    ClickHouseConfig {
        url: std::env::var("STREAMLING_REVIEW_CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:30123".into()),
        database: std::env::var("STREAMLING_REVIEW_CLICKHOUSE_DATABASE")
            .unwrap_or_else(|_| "default".into()),
        user: std::env::var("STREAMLING_REVIEW_CLICKHOUSE_USER")
            .unwrap_or_else(|_| "default".into()),
        password: std::env::var("STREAMLING_REVIEW_CLICKHOUSE_PASSWORD").unwrap_or_default(),
        compression: ClickHouseCompression::None,
        compression_level: GzipCompressionLevel::default(),
        columns: None,
    }
}

#[tokio::test]
#[ignore = "requires ClickHouse; set STREAMLING_REVIEW_CLICKHOUSE_URL (default localhost:30123); creates/removes UUID Memory table"]
async fn v3_existing_plugin_u256_survives_actual_clickhouse_sink() {
    let config = review_clickhouse_config();
    let client = ClickHouseClient::new(config.clone());
    let table = format!("pr37_v3_plugin_sink_{}", uuid::Uuid::new_v4().simple());
    client
        .send_query(
            reqwest::Method::POST,
            &format!("CREATE TABLE {table} (idx Int32, value UInt256) ENGINE=Memory"),
        )
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let result = async {
        let values = [1_u128, 16, 256, 1000, 1_000_000_000_000_000_000];
        let bytes: Vec<_> = values
            .iter()
            .map(|v| {
                let mut b = [0_u8; 32];
                b[16..].copy_from_slice(&v.to_be_bytes());
                b
            })
            .collect();
        let value_field = Field::new("value", DataType::FixedSizeBinary(32), false).with_metadata(
            HashMap::from([(
                "ARROW:extension:name".to_owned(),
                "streamling.u256".to_owned(),
            )]),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int32, false),
            value_field,
            Field::new("_gs_op", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(
                    (0..values.len() as i32).collect::<Vec<_>>(),
                )),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|v| v.as_slice()))
                        .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["c"; values.len()])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let input = ctx
            .read_batch(batch)
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let sink = ClickHouseTableProvider::new_sink(
            "v3_plugin_sink".into(),
            &table,
            config,
            None,
            "idx".into(),
            Some(true),
            Some(false),
            None,
            None,
            None,
            None,
            "v3_plugin_sink".into(),
            None,
        )
        .unwrap();
        let plan = sink
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await
            .unwrap();
        let write_result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            collect(plan, ctx.task_ctx()),
        )
        .await;
        match write_result {
            Ok(Err(e)) => return Err(format!("sink explicitly rejected: {e}")),
            Err(e) => return Err(format!("sink timed out: {e}")),
            Ok(Ok(_)) => {}
        }
        let actual = client
            .send_query(
                reqwest::Method::GET,
                &format!("SELECT toString(value) FROM {table} ORDER BY idx FORMAT TSV"),
            )
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();
        Ok((
            values.iter().map(ToString::to_string).collect::<Vec<_>>(),
            actual.lines().map(str::to_owned).collect::<Vec<_>>(),
        ))
    }
    .await;
    client
        .send_query(
            reqwest::Method::POST,
            &format!("DROP TABLE IF EXISTS {table}"),
        )
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let (expected, actual) =
        result.expect("test must reach the actual ClickHouse write to assess value preservation");
    eprintln!("actual legacy plugin -> production sink: expected={expected:?}; actual={actual:?}");
    assert_eq!(
        actual, expected,
        "existing plugin integer must be preserved by the actual sink path"
    );
}

const SIGNED_CASES: &[Option<&str>] = &[
    None,
    Some("0"),
    Some("1"),
    Some("-1"),
    Some("2"),
    Some("-2"),
    Some("127"),
    Some("128"),
    Some("255"),
    Some("256"),
    Some("257"),
    Some("-127"),
    Some("-128"),
    Some("-255"),
    Some("-256"),
    Some("-257"),
    Some("9223372036854775807"),
    Some("-9223372036854775808"),
    Some("170141183460469231731687303715884105727"),
    Some("-170141183460469231731687303715884105728"),
    Some("57896044618658097711785492504343953926634992332820282019728792003956564819967"),
    Some("-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
];

fn signed_batch(
    values: &[Option<&str>],
    legacy: bool,
    repair_legacy_endianness: bool,
) -> RecordBatch {
    let (value_field, array): (Field, arrow::array::ArrayRef) = if legacy {
        // The base's I256 contract is signed two's-complement, 32 bytes BE.
        // This is a legacy-contract diagnostic: current companion production
        // source code only emits U256, not I256.
        let bytes: Vec<Option<Vec<u8>>> = values
            .iter()
            .map(|value| {
                value.map(|v| {
                    let n = v
                        .parse::<DecimalArbValue>()
                        .unwrap()
                        .as_bigdecimal()
                        .as_bigint_and_exponent()
                        .0;
                    let signed = n.to_signed_bytes_be();
                    assert!(signed.len() <= 32);
                    let mut out = vec![if v.starts_with('-') { 255 } else { 0 }; 32];
                    out[32 - signed.len()..].copy_from_slice(&signed);
                    out
                })
            })
            .collect();
        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            bytes.iter().map(|v| v.as_deref()),
            32,
        )
        .unwrap();
        let field = Field::new("value", DataType::FixedSizeBinary(32), true).with_metadata(
            HashMap::from([("ARROW:extension:name".into(), "streamling.i256".into())]),
        );
        let array: arrow::array::ArrayRef = Arc::new(array);
        let array = if repair_legacy_endianness {
            // Execute the actual unchanged UDF invoked by the deleted base
            // normalizer arm, rather than duplicating its byte conversion.
            ReverseBytes32Func::new()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(array)],
                    arg_fields: vec![Arc::new(field.clone())],
                    number_rows: values.len(),
                    return_field: Arc::new(field.clone()),
                    config_options: Arc::default(),
                })
                .unwrap()
                .into_array(values.len())
                .unwrap()
        } else {
            array
        };
        (field, array)
    } else {
        let mut builder =
            DecimalArbArrayBuilder::with_capacity(values.len(), "value", 78, 0).unwrap();
        for v in values {
            match v {
                Some(v) => builder.append_str(v).unwrap(),
                None => builder.append_null(),
            }
        }
        (
            DecimalArbType::with_native_int_kind(
                DecimalArbType::field("value", 78, 0, true).unwrap(),
                NativeIntKind::I256,
            )
            .unwrap(),
            Arc::new(builder.finish().into_inner().0),
        )
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int32, false),
            value_field,
            Field::new("_gs_op", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(
                (0..values.len() as i32).collect::<Vec<_>>(),
            )),
            array,
            Arc::new(StringArray::from(vec!["c"; values.len()])),
        ],
    )
    .unwrap()
}

async fn actual_signed_sink(batch: RecordBatch) -> (Result<(), String>, Vec<String>) {
    let config = review_clickhouse_config();
    let client = ClickHouseClient::new(config.clone());
    let table = format!("pr37_v3_signed_sink_{}", uuid::Uuid::new_v4().simple());
    client
        .send_query(
            reqwest::Method::POST,
            &format!("CREATE TABLE {table} (idx Int32, value Nullable(Int256)) ENGINE=Memory"),
        )
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let result = async {
        let ctx = SessionContext::new();
        let input = ctx
            .read_batch(batch)
            .map_err(|e| e.to_string())?
            .create_physical_plan()
            .await
            .map_err(|e| e.to_string())?;
        let sink = ClickHouseTableProvider::new_sink(
            "v3_signed_sink".into(),
            &table,
            config,
            None,
            "idx".into(),
            Some(true),
            Some(false),
            None,
            None,
            None,
            None,
            "v3_signed_sink".into(),
            None,
        )
        .map_err(|e| e.to_string())?;
        let plan = sink
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await
            .map_err(|e| e.to_string())?;
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            collect(plan, ctx.task_ctx()),
        )
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        Ok(())
    }
    .await;
    let text = client
        .send_query(
            reqwest::Method::GET,
            &format!("SELECT toString(value) FROM {table} ORDER BY idx FORMAT TSV"),
        )
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    // Independently read the server's actual Arrow bytes through the current
    // native signed read adapter. This is the metadata-directed read helper
    // used by backfill, not a claim that generic ClickHouse sources infer the
    // signed hint automatically.
    let wire = client
        .send_query(
            reqwest::Method::GET,
            &format!("SELECT value FROM {table} ORDER BY idx FORMAT Arrow"),
        )
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let read_field = Arc::new(
        DecimalArbType::with_native_int_kind(
            DecimalArbType::field("value", 78, 0, true).unwrap(),
            NativeIntKind::I256,
        )
        .unwrap(),
    );
    let mut arrow_values = Vec::new();
    let read_result = (|| -> Result<(), String> {
        let reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(wire), None)
            .map_err(|e| e.to_string())?;
        for batch in reader {
            let batch = batch.map_err(|e| e.to_string())?;
            let decoded = clickhouse_native_to_decimal_arb(batch.column(0).as_ref(), &read_field)
                .map_err(|e| e.to_string())?;
            let array = decoded.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            for row in 0..array.len() {
                arrow_values.push(if array.is_null(row) {
                    "\\N".to_owned()
                } else {
                    DecimalArbValue::from_canonical_bytes_at_scale(array.value(row), 0)
                        .map_err(|e| e.to_string())?
                        .to_canonical_string()
                });
            }
        }
        Ok(())
    })();
    client
        .send_query(
            reqwest::Method::POST,
            &format!("DROP TABLE IF EXISTS {table}"),
        )
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    read_result.unwrap();
    assert_eq!(
        arrow_values,
        text.lines().map(str::to_owned).collect::<Vec<_>>(),
        "native Arrow read adapter must agree with the independent server decimal text"
    );
    (result, text.lines().map(str::to_owned).collect())
}

#[tokio::test]
#[ignore = "requires STREAMLING_REVIEW_CLICKHOUSE_URL or localhost:30123; legacy I256 diagnostic, no current companion producer"]
async fn v3_legacy_signed_contract_diagnostic_actual_clickhouse_sink() {
    let (result, actual) = actual_signed_sink(signed_batch(SIGNED_CASES, true, false)).await;
    result.unwrap();
    let expected: Vec<_> = SIGNED_CASES
        .iter()
        .map(|v| v.unwrap_or("\\N").to_owned())
        .collect();
    let mismatches: Vec<_> = expected
        .iter()
        .zip(&actual)
        .filter(|(e, a)| e != a)
        .collect();
    eprintln!(
        "legacy I256 contract diagnostic: {} mismatches / {} rows: {mismatches:?}",
        mismatches.len(),
        expected.len()
    );
    assert_eq!(
        actual, expected,
        "legacy I256 must preserve sign and magnitude or be rejected explicitly"
    );
}

#[tokio::test]
#[ignore = "requires STREAMLING_REVIEW_CLICKHOUSE_URL or localhost:30123; signed new-representation and restored-endian controls"]
async fn v3_new_signed_decimal_and_repaired_legacy_control_match_actual_sink() {
    let expected: Vec<_> = SIGNED_CASES
        .iter()
        .map(|v| v.unwrap_or("\\N").to_owned())
        .collect();
    for (legacy, repair, label) in [
        (false, false, "new decimal_arb I256"),
        (true, true, "legacy + removed endian reversal"),
    ] {
        let (result, actual) = actual_signed_sink(signed_batch(SIGNED_CASES, legacy, repair)).await;
        result.unwrap();
        assert_eq!(actual, expected, "{label}");
        eprintln!(
            "{label}: {} exact rows including NULL, min/max and signed byte boundaries",
            expected.len()
        );
    }
}

#[tokio::test]
#[ignore = "requires STREAMLING_REVIEW_CLICKHOUSE_URL or localhost:30123; verifies Int256 overflow rejection and no writes"]
async fn v3_new_signed_decimal_rejects_out_of_range_without_partial_write() {
    for value in [
        "57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "-57896044618658097711785492504343953926634992332820282019728792003956564819969",
        "115792089237316195423570985008687907853269984665640564039457584007913129639935",
        "-115792089237316195423570985008687907853269984665640564039457584007913129639935",
    ] {
        // Put valid data before and after the bad value to check that no
        // prefix of this batch is silently committed when normalization fails.
        let (result, actual) = actual_signed_sink(signed_batch(
            &[Some("1"), Some(value), Some("-1")],
            false,
            false,
        ))
        .await;
        let error = result.expect_err("unrepresentable signed value must reject");
        assert!(
            error.contains("signed range"),
            "expected precise native range error, got {error}"
        );
        assert!(
            actual.is_empty(),
            "part of rejected batch was written: {actual:?}"
        );
        eprintln!("native signed overflow {value}: explicit range error, zero rows committed");
    }
}
