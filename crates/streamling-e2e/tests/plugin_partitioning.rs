//! Partitioned plugins: one plugin instance per physical stream.
//!
//! Uses the in-repo example plugin (`plugin_examples/basic`). Its partitioned
//! source emits `rows` rows per partition with ids `partition * 1_000_000 + n`
//! and completes, so every pipeline here ends on its own.

use std::collections::BTreeSet;
use std::time::Duration;
use streamling_e2e::{build_basic_example_plugin, init_tracing, PipelineOpts, TestContext};

const PIPELINE_TIMEOUT: Duration = Duration::from_secs(120);
/// Source partition `p` emits ids `p * ID_STRIDE ..`.
const ID_STRIDE: i64 = 1_000_000;

async fn run_with_plugin(ctx: &TestContext, pipeline: &str) {
    let plugin_lib = build_basic_example_plugin().await;
    let status = ctx
        .run_pipeline_with_opts(
            pipeline,
            PipelineOpts::new()
                .timeout(PIPELINE_TIMEOUT)
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
                .env(
                    "STREAMLING__PLUGIN__PATH",
                    plugin_lib.to_string_lossy().as_ref(),
                ),
        )
        .await
        .expect("pipeline must run to completion");
    assert!(status.success(), "pipeline must exit 0");
}

/// Every id the source emits: `rows` per partition.
fn expected_ids(partitions: i64, rows: i64) -> BTreeSet<i64> {
    (0..partitions)
        .flat_map(|p| (0..rows).map(move |n| p * ID_STRIDE + n))
        .collect()
}

/// Without `parallelism`, the source runs as many instances as the plugin
/// prefers, each emitting its own partition.
#[tokio::test]
async fn test_partitioned_plugin_source_runs_one_instance_per_stream() {
    init_tracing();
    let ctx = TestContext::new().await.expect("Failed to create context");

    let pipeline = r#"
sources:
  numbers:
    type: basic_plugin.partitioned_source
    rows: "50"
    preferred_partitions: "3"
    primary_key: id

transforms: {}

sinks:
  pg:
    type: postgres
    from: numbers
    table: partitioned_source_rows
    schema: public
    primary_key: id
"#;
    run_with_plugin(&ctx, pipeline).await;

    let postgres = &ctx.postgres;
    assert_eq!(
        postgres
            .count("SELECT COUNT(*) FROM public.partitioned_source_rows")
            .await
            .unwrap(),
        150
    );
    for partition in 0..3 {
        let rows = postgres
            .count(&format!(
                "SELECT COUNT(*) FROM public.partitioned_source_rows \
                 WHERE source_partition = {partition} \
                 AND id >= {start} AND id < {end}",
                start = partition * ID_STRIDE,
                end = partition * ID_STRIDE + 50,
            ))
            .await
            .unwrap();
        assert_eq!(rows, 50, "partition {partition} must emit its own rows");
    }
}

/// An explicit `parallelism` widens the transform through the placement the
/// plugin asked for; without it the transform inherits its input's width.
#[tokio::test]
async fn test_partitioned_plugin_transform_runs_one_instance_per_stream() {
    init_tracing();
    let ctx = TestContext::new().await.expect("Failed to create context");

    let pipeline = r#"
sources:
  numbers:
    type: basic_plugin.partitioned_source
    rows: "40"
    parallelism: 2
    primary_key: id

transforms:
  widened:
    type: basic_plugin.partitioned_transform
    from: numbers
    parallelism: 4
    primary_key: id
  inherited:
    type: basic_plugin.partitioned_transform
    from: numbers
    primary_key: id

sinks:
  widened_rows:
    type: postgres
    from: widened
    table: widened_rows
    schema: public
    primary_key: id
  inherited_rows:
    type: postgres
    from: inherited
    table: inherited_rows
    schema: public
    primary_key: id
"#;
    run_with_plugin(&ctx, pipeline).await;

    let postgres = &ctx.postgres;
    for (table, width) in [("widened_rows", 4), ("inherited_rows", 2)] {
        assert_eq!(
            postgres
                .count(&format!("SELECT COUNT(*) FROM public.{table}"))
                .await
                .unwrap(),
            80,
            "{table} must hold every row"
        );
        assert_eq!(
            postgres
                .count(&format!(
                    "SELECT COUNT(DISTINCT transform_partition) FROM public.{table}"
                ))
                .await
                .unwrap(),
            width,
            "{table} must run {width} instances"
        );
        assert_eq!(
            postgres
                .count(&format!(
                    "SELECT COUNT(*) FROM public.{table} \
                     WHERE transform_partition < 0 OR transform_partition >= {width}"
                ))
                .await
                .unwrap(),
            0
        );
    }
}

/// Each write stream goes to its own sink instance, which writes its own
/// file. Rows are placed by primary key, so every id lands in exactly one.
#[tokio::test]
async fn test_partitioned_plugin_sink_runs_one_instance_per_stream() {
    init_tracing();
    let ctx = TestContext::new().await.expect("Failed to create context");
    let output_dir = ctx.temp_dir.path().join("partitioned_sink");
    std::fs::create_dir_all(&output_dir).unwrap();

    let pipeline = format!(
        r#"
sources:
  numbers:
    type: basic_plugin.partitioned_source
    rows: "30"
    parallelism: 3
    primary_key: id

transforms: {{}}

sinks:
  out:
    type: basic_plugin.partitioned_file_sink
    from: numbers
    parallelism: 2
    output_dir: "{output_dir}"
"#,
        output_dir = output_dir.display()
    );
    run_with_plugin(&ctx, &pipeline).await;

    let mut seen = BTreeSet::new();
    for partition in 0..2 {
        let file = output_dir.join(format!("out-{partition}.csv"));
        let written = std::fs::read_to_string(&file)
            .unwrap_or_else(|e| panic!("instance {partition} must write {file:?}: {e}"));
        let ids: Vec<i64> = written.lines().map(|l| l.parse().unwrap()).collect();
        assert!(!ids.is_empty(), "instance {partition} must receive rows");
        for id in ids {
            assert!(seen.insert(id), "id {id} was written by two instances");
        }
    }
    assert!(
        !output_dir.join("out-2.csv").exists(),
        "the sink runs 2 instances"
    );
    assert_eq!(seen, expected_ids(3, 30));
}
