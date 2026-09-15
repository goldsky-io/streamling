//! Exact numeric agreement between bounded ClickHouse and live Kafka phases.
use apache_avro::{types::Value, Decimal};
use num_bigint::BigInt;
use std::{collections::BTreeMap, str::FromStr, time::Duration};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

#[derive(serde::Deserialize, clickhouse::Row)]
struct InputRow {
    id: String,
    amount: Option<String>,
}

fn canonical(text: &str) -> String {
    let text = if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        text
    };
    if text == "-0" {
        "0".into()
    } else {
        text.into()
    }
}

fn decimal_text(coefficient: &str, scale: u32) -> String {
    if scale == 0 {
        return coefficient.into();
    }
    let sign = if coefficient.starts_with('-') {
        "-"
    } else {
        ""
    };
    let digits = coefficient.trim_start_matches('-');
    let width = usize::max(digits.len(), scale as usize + 1);
    let padded = format!("{digits:0>width$}");
    let point = padded.len() - scale as usize;
    format!("{sign}{}.{}", &padded[..point], &padded[point..])
}

async fn hybrid(ch_type: &str, precision: u32, scale: u32, coefficients: Vec<Option<String>>) {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();
    let ch = ctx.clickhouse.as_ref().unwrap();
    ch.execute(&format!("CREATE TABLE history(block Int64,id String,amount Nullable({ch_type}),is_deleted UInt8) ENGINE=MergeTree ORDER BY (block,id)")).await.unwrap();
    ch.execute("CREATE TABLE offsets(topic String,partition Int32,offset UInt32) ENGINE=MergeTree ORDER BY (topic,partition)").await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE results(block BIGINT,id TEXT PRIMARY KEY,amount NUMERIC)")
        .await
        .unwrap();
    let schema = format!(
        r#"{{"type":"record","name":"V3Hybrid","fields":[
      {{"name":"block","type":"long"}},
      {{"name":"id","type":"string"}},
      {{"name":"amount","type":["null",{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":{scale}}}],"default":null}}
    ]}}"#
    );
    let mut expected = BTreeMap::new();
    for (index, coefficient) in coefficients.iter().enumerate() {
        let text = coefficient.as_ref().map(|c| decimal_text(c, scale));
        let literal = text
            .as_ref()
            .map(|s| format!("'{s}'"))
            .unwrap_or_else(|| "NULL".into());
        ch.execute(&format!(
            "INSERT INTO history VALUES({},'ch_{index}',{literal},0)",
            index + 1
        ))
        .await
        .unwrap();
        let value = match coefficient {
            None => Value::Union(0, Box::new(Value::Null)),
            Some(c) => Value::Union(
                1,
                Box::new(Value::Decimal(Decimal::from(
                    BigInt::from_str(c).unwrap().to_signed_bytes_be(),
                ))),
            ),
        };
        ctx.kafka
            .produce_avro_value(
                &schema,
                Value::Record(vec![
                    ("block".into(), Value::Long((1000 + index) as i64)),
                    ("id".into(), Value::String(format!("kafka_{index}"))),
                    ("amount".into(), value),
                ]),
            )
            .await
            .unwrap();
        for prefix in ["ch", "kafka"] {
            expected.insert(
                format!("{prefix}_{index}"),
                text.as_ref().map(|s| canonical(s)),
            );
        }
    }
    // Check the fixture itself before attributing any pipeline mismatch.
    let raw: Vec<InputRow> = ch
        .query("SELECT id,toString(amount) AS amount FROM history ORDER BY id")
        .await
        .unwrap();
    for row in raw {
        assert_eq!(
            row.amount.as_deref().map(canonical),
            expected[&row.id],
            "ClickHouse input fixture for{}",
            row.id
        );
    }
    let yaml = format!(
        r#"
sources:
  src:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: history
        columns: block,id,amount
    unbounded_source:
      source_type: kafka
      topic: {topic}
      start_at: earliest
    offset_table:
      topic_name: {topic}
      table_name: offsets
    primary_key: id
transforms: {{}}
sinks:
  out:
    type: postgres
    from: src
    table: results
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        topic = ctx.kafka_topic
    );
    let opts = PipelineOpts::new()
        .record_limit((coefficients.len() * 2) as u64)
        .timeout(Duration::from_secs(60))
        .env("RUST_LOG", "info")
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
        .env("STREAMLING__RECORD_BATCH_SIZE", "1");
    let out = ctx.run_pipeline_with_opts(&yaml, opts).await;
    eprintln!("V3 HYBRID type={ch_type} precision={precision} scale={scale} status={out:?}");
    let rows: Vec<(String, Option<String>)> = ctx
        .postgres
        .query("SELECT id,amount::text FROM results ORDER BY id")
        .await
        .unwrap();
    let actual = rows
        .into_iter()
        .map(|(id, v)| (id, v.as_deref().map(canonical)))
        .collect::<BTreeMap<_, _>>();
    eprintln!(
        "V3 HYBRID verified_rows={} expected_rows={}",
        actual.len(),
        expected.len()
    );
    assert!(out.is_ok(), "pipeline must complete successfully: {out:?}");
    assert_eq!(
        actual, expected,
        "both source phases must preserve the same exact numbers"
    );
}

fn signed_values(max_digits: usize) -> Vec<Option<String>> {
    let mut values = vec![None, Some("0".into())];
    for s in [
        "1".into(),
        "127".into(),
        "128".into(),
        "255".into(),
        "256".into(),
        "1000".into(),
        "18446744073709551617".into(),
        "9".repeat(max_digits),
    ] {
        values.push(Some(s.clone()));
        values.push(Some(format!("-{s}")));
    }
    values
}

#[tokio::test]
async fn v3_hybrid_wide_fraction_string() {
    hybrid("String", 100, 18, signed_values(99)).await;
}
#[tokio::test]
async fn v3_hybrid_native_decimal_to_wide() {
    hybrid("Decimal(76,18)", 100, 18, signed_values(76)).await;
}
#[tokio::test]
async fn v3_hybrid_native_decimal_scale38_to_wide() {
    hybrid("Decimal(76,38)", 100, 38, signed_values(76)).await;
}
#[tokio::test]
async fn v3_hybrid_tiny_fraction_string() {
    hybrid("String", 100, 100, signed_values(99)).await;
}
#[tokio::test]
async fn v3_hybrid_unsigned_native_hint() {
    let max = (BigInt::from(1_u8) << 256_usize) - 1_u8;
    let mut values = vec![None, Some("0".into()), Some(max.to_string())];
    for bit in [0_usize, 7, 8, 63, 64, 127, 128, 191, 192, 254, 255] {
        let n = BigInt::from(1_u8) << bit;
        values.push(Some(n.to_string()));
        values.push(Some((&n + 1_u8).to_string()));
    }
    hybrid("UInt256", 78, 0, values).await;
}
#[tokio::test]
async fn v3_hybrid_signed_native_to_wide() {
    let limit = BigInt::from(1_u8) << 255_usize;
    let mut values = signed_values(75);
    values.push(Some((-&limit).to_string()));
    values.push(Some((&limit - 1_u8).to_string()));
    hybrid("Int256", 100, 0, values).await;
}
