//! Complete pipeline proofs for nullable-script and decimal-key failures.
use apache_avro::{types::Value, Decimal};
use num_bigint::BigInt;
use std::time::Duration;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

fn opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(Duration::from_secs(20))
        .record_limit(2)
        .env("RUST_LOG", "info")
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
        .env("STREAMLING__RECORD_BATCH_SIZE", "2")
}
fn decimal(n: i64) -> Value {
    Value::Decimal(Decimal::from(BigInt::from(n).to_signed_bytes_be()))
}

async fn key(precision: u32, deduplicate: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE results(id NUMERIC PRIMARY KEY,payload BIGINT)")
        .await
        .unwrap();
    let schema = format!(
        r#"{{"type":"record","name":"V3Key","fields":[
      {{"name":"id","type":{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":0}}}},
      {{"name":"payload","type":"long"}}
    ]}}"#
    );
    for (n, payload) in [(1_i64, 10_i64), (1, 11), (2, 20)] {
        ctx.kafka
            .produce_avro_value(
                &schema,
                Value::Record(vec![
                    ("id".into(), decimal(n)),
                    ("payload".into(), Value::Long(payload)),
                ]),
            )
            .await
            .unwrap();
    }
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
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
    batch_size: 2
    deduplicate: {deduplicate}
"#,
        topic = ctx.kafka_topic
    );
    let out = ctx
        .run_pipeline_raw(
            &yaml,
            opts()
                .record_limit(2)
                .env("STREAMLING__RECORD_BATCH_SIZE", "3"),
        )
        .await
        .unwrap();
    if !out.status.success() {
        eprintln!("V3 KEY precision={precision}: {}", out.stderr);
    }
    assert!(
        out.status.success(),
        "decimal keys should preserve supported deduplication"
    );
    let actual: Vec<(String, i64)> = ctx
        .postgres
        .query("SELECT id::text,payload FROM results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(actual, vec![("1".into(), 11), ("2".into(), 20)]);
}

#[tokio::test]
async fn native_decimal_key_control() {
    key(30, true).await;
}
#[tokio::test]
async fn decimal_arb_primary_key_deduplication() {
    key(100, true).await;
}

async fn null_script(precision: u32, all_null: bool) {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    ctx.postgres
        .execute("CREATE TABLE results(id BIGINT PRIMARY KEY,amount NUMERIC)")
        .await
        .unwrap();
    let schema = format!(
        r#"{{"type":"record","name":"V3Null","fields":[
      {{"name":"id","type":"long"}},
      {{"name":"amount","type":["null",{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":0}}],"default":null}}
    ]}}"#
    );
    for id in [1_i64, 2] {
        let v = if all_null || id == 2 {
            Value::Union(0, Box::new(Value::Null))
        } else {
            Value::Union(1, Box::new(decimal(1000)))
        };
        ctx.kafka
            .produce_avro_value(
                &schema,
                Value::Record(vec![("id".into(), Value::Long(id)), ("amount".into(), v)]),
            )
            .await
            .unwrap();
    }
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  identity:
    type: script
    from: src
    language: javascript
    primary_key: id
    batch_size: 2
    script: "row => ({{id: row.id, amount: row.amount}})"
sinks:
  out:
    type: postgres
    from: identity
    table: results
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 2
"#,
        topic = ctx.kafka_topic
    );
    let out = ctx.run_pipeline_raw(&yaml, opts()).await.unwrap();
    if !out.status.success() {
        eprintln!(
            "V3 NULL precision={precision} all_null={all_null}: {}",
            out.stderr
        );
    }
    assert!(
        out.status.success(),
        "a valid all-null batch must remain typed and preserve its rows"
    );
    let actual: Vec<(i64, Option<String>)> = ctx
        .postgres
        .query("SELECT id,amount::text FROM results ORDER BY id")
        .await
        .unwrap();
    let first = if all_null { None } else { Some("1000".into()) };
    assert_eq!(actual, vec![(1, first), (2, None)]);
}

#[tokio::test]
async fn native_all_null_script_control() {
    null_script(30, true).await;
}
#[tokio::test]
async fn decimal_arb_mixed_null_script_control() {
    null_script(100, false).await;
}
#[tokio::test]
async fn decimal_arb_all_null_script_batch() {
    null_script(100, true).await;
}
