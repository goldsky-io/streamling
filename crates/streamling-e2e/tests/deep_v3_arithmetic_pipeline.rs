//! Exact Fraction-oracle checks through the complete binary and real services.
use apache_avro::{types::Value, Decimal, Schema};
use num_bigint::BigInt;
use rdkafka::{
    config::ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use serde::Deserialize;
use sqlx::FromRow;
use std::{collections::BTreeMap, str::FromStr, time::Duration};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

#[derive(Deserialize)]
struct Group {
    sa: u32,
    sb: u32,
    precision: u32,
    rows: Vec<OracleRow>,
}
#[derive(Deserialize)]
struct OracleRow {
    id: i64,
    a: Option<String>,
    b: Option<String>,
    expected: BTreeMap<String, Option<String>>,
}
#[derive(Debug, FromRow)]
struct Stored {
    id: i64,
    added: Option<String>,
    subtracted: Option<String>,
    multiplied: Option<String>,
    divided: Option<String>,
    remainder: Option<String>,
    recovered: Option<String>,
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
fn dec(value: &Option<String>) -> Value {
    match value {
        None => Value::Union(0, Box::new(Value::Null)),
        Some(value) => Value::Union(
            1,
            Box::new(Value::Decimal(Decimal::from(
                BigInt::from_str(value).unwrap().to_signed_bytes_be(),
            ))),
        ),
    }
}
fn opts(batch: usize) -> PipelineOpts {
    PipelineOpts::new()
        .timeout(Duration::from_secs(60))
        .env("RUST_LOG", "info")
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
        .env("STREAMLING__RECORD_BATCH_SIZE", batch.to_string())
}

async fn exercise(mode: &str, batch: usize) {
    init_tracing();
    let groups: Vec<Group> =
        serde_json::from_str(include_str!("deep_v3_pipeline_oracle.json")).unwrap();
    let mut checked = 0;
    for group in groups {
        let ctx = TestContext::new().await.unwrap();
        ctx.postgres.execute("CREATE TABLE results(id BIGINT PRIMARY KEY,added NUMERIC,subtracted NUMERIC,multiplied NUMERIC,divided NUMERIC,remainder NUMERIC,recovered NUMERIC)").await.unwrap();
        let schema_text = format!(
            r#"{{"type":"record","name":"V3Arithmetic","fields":[
          {{"name":"id","type":"long"}},
          {{"name":"a","type":["null",{{"type":"bytes","logicalType":"decimal","precision":{p},"scale":{sa}}}],"default":null}},
          {{"name":"b","type":["null",{{"type":"bytes","logicalType":"decimal","precision":{p},"scale":{sb}}}],"default":null}}
        ]}}"#,
            p = group.precision,
            sa = group.sa,
            sb = group.sb
        );
        let schema_id = ctx.kafka.register_schema(&schema_text).await.unwrap();
        let schema = Schema::parse_str(&schema_text).unwrap();
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &ctx.config.kafka_broker)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        for row in &group.rows {
            let value = Value::Record(vec![
                ("id".into(), Value::Long(row.id)),
                ("a".into(), dec(&row.a)),
                ("b".into(), dec(&row.b)),
            ]);
            let mut payload = vec![0];
            payload.extend(schema_id.to_be_bytes());
            payload.extend(apache_avro::to_avro_datum(&schema, value).unwrap());
            producer
                .send(
                    FutureRecord::to(&ctx.kafka_topic)
                        .payload(&payload)
                        .key(&row.id.to_string())
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "dbz.op",
                            value: Some("c"),
                        })),
                    Timeout::After(Duration::from_secs(10)),
                )
                .await
                .unwrap();
        }
        producer
            .flush(Timeout::After(Duration::from_secs(10)))
            .unwrap();
        let output_topic = ctx.create_kafka_topic("math_out").await.unwrap();
        let extra_transform = if mode == "script" {
            r#"
  identity:
    type: script
    from: calculated
    language: javascript
    primary_key: id
    batch_size: 32
    script: |
      row => ({id: row.id, added: row.added, subtracted: row.subtracted, multiplied: row.multiplied, divided: row.divided, remainder: row.remainder, recovered: row.recovered})
"#
        } else {
            ""
        };
        let from = if mode == "script" {
            "identity"
        } else {
            "calculated"
        };
        let sink = if mode == "avro" {
            format!("type: kafka\n    from: {from}\n    topic: {}\n    topic_partitions: 1\n    data_format: avro",output_topic.topic)
        } else {
            format!("type: postgres\n    from: {from}\n    table: results\n    schema: public\n    primary_key: id\n    on_conflict: update\n    batch_size: 37\n    batch_flush_interval: 100ms")
        };
        let yaml = format!(
            r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  calculated:
    type: sql
    primary_key: id
    sql: "SELECT id,a+b AS added,a-b AS subtracted,a*b AS multiplied,a/b AS divided,a%b AS remainder,(a+b)-b AS recovered FROM src"
{extra_transform}
sinks:
  out:
    {sink}
"#,
            topic = ctx.kafka_topic
        );
        let out = ctx
            .run_pipeline_raw(&yaml, opts(batch).record_limit(group.rows.len() as u64))
            .await
            .unwrap();
        eprintln!(
            "ARITHMETIC PIPELINE mode={mode} batch={batch} scales={}/{} status={:?}",
            group.sa, group.sb, out.status
        );
        assert!(out.status.success(), "{}", out.stderr);
        if mode == "avro" {
            let readback = format!(
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
    batch_size: 37
    batch_flush_interval: 100ms
"#,
                topic = output_topic.topic
            );
            let out = ctx
                .run_pipeline_raw(&readback, opts(batch).record_limit(group.rows.len() as u64))
                .await
                .unwrap();
            assert!(out.status.success(), "{}", out.stderr);
        }
        let actual:Vec<Stored>=ctx.postgres.query("SELECT id,added::text AS added,subtracted::text AS subtracted,multiplied::text AS multiplied,divided::text AS divided,remainder::text AS remainder,recovered::text AS recovered FROM results ORDER BY id").await.unwrap();
        assert_eq!(
            actual.len(),
            group.rows.len(),
            "mode={mode},scales={}/{}",
            group.sa,
            group.sb
        );
        for (actual, expected) in actual.iter().zip(&group.rows) {
            assert_eq!(actual.id, expected.id);
            for (name, value) in [
                ("added", &actual.added),
                ("subtracted", &actual.subtracted),
                ("multiplied", &actual.multiplied),
                ("divided", &actual.divided),
                ("remainder", &actual.remainder),
                ("recovered", &actual.recovered),
            ] {
                assert_eq!(
                    value.as_deref().map(canonical),
                    expected.expected[name],
                    "mode={mode},batch={batch},scales={}/{},id={},op={name},a={:?},b={:?}",
                    group.sa,
                    group.sb,
                    actual.id,
                    expected.a,
                    expected.b
                );
                checked += 1;
            }
        }
        eprintln!(
            "ARITHMETIC VERIFIED mode={mode} batch={batch} scales={}/{} rows={} outputs={}",
            group.sa,
            group.sb,
            actual.len(),
            actual.len() * 6
        );
    }
    eprintln!("ARITHMETIC TOTAL mode={mode} batch={batch} exact_checks={checked}");
}

#[tokio::test]
async fn v3_exact_arithmetic_direct_batch1() {
    exercise("direct", 1).await;
}
#[tokio::test]
async fn v3_exact_arithmetic_script_batch17() {
    exercise("script", 17).await;
}
#[tokio::test]
async fn v3_exact_arithmetic_avro_batch128() {
    exercise("avro", 128).await;
}
