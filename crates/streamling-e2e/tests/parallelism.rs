//! Repartitioning and coalescing across widths.
//!
//! The other parallelism tests all *widen* — a narrow source feeding a wider
//! sink. These cover the rest of the matrix: widening at a transform, narrowing
//! at a transform, and collapsing back to one stream. Every exchange must be
//! lossless in both directions, and hash placement must survive being applied
//! twice in one plan.
//!
//! These use a bounded file source and a print sink deliberately: the source
//! reaches EOF and the pipeline terminates on its own, so there is no
//! `num_records_before_stop` to interact with in-batch deduplication, and the
//! captured output *is* the exact row set the sink received.

use std::fs;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

/// Rows per generated CSV file.
const ROWS_PER_FILE: i64 = 3;

/// Writes `files` CSV files of [`ROWS_PER_FILE`] rows each and returns the
/// directory plus the total row count.
///
/// A bounded file scan yields `min(parallelism, file_count)` partitions — one
/// file cannot be split across partitions — so the file count is what actually
/// caps the source's width.
fn write_csv_files(ctx: &TestContext, dir_name: &str, files: i64) -> (std::path::PathBuf, i64) {
    let data_dir = ctx.temp_dir.path().join(dir_name);
    fs::create_dir_all(&data_dir).expect("create data dir");

    let mut next_id = 1i64;
    for file in 0..files {
        let mut csv = String::from("id,value\n");
        for _ in 0..ROWS_PER_FILE {
            csv.push_str(&format!("{next_id},value_{next_id}\n"));
            next_id += 1;
        }
        fs::write(data_dir.join(format!("part_{file}.csv")), csv).expect("write csv");
    }
    (data_dir, files * ROWS_PER_FILE)
}

/// Builds a `file -> [sql] -> print` pipeline, with optional parallelism at each
/// stage.
fn pipeline(
    path: &std::path::Path,
    source_parallelism: usize,
    transform_parallelism: Option<usize>,
    sink_parallelism: Option<usize>,
) -> String {
    // An empty `transforms:` key deserializes as null, not an empty map, so the
    // no-transform case has to spell out `{}`.
    let (transforms, from) = match transform_parallelism {
        Some(parallelism) => (
            format!(
                r#"transforms:
  widen:
    type: sql
    primary_key: id
    parallelism: {parallelism}
    sql: select * from file_src
"#
            ),
            "widen",
        ),
        None => ("transforms: {}\n".to_string(), "file_src"),
    };
    let sink_parallelism = sink_parallelism
        .map(|p| format!("    parallelism: {p}\n"))
        .unwrap_or_default();

    format!(
        r#"
sources:
  file_src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    parallelism: {source_parallelism}
    mode:
      type: bounded

{transforms}
sinks:
  print_sink:
    type: print
    from: {from}
    sample_every: 1
{sink_parallelism}"#,
        path = path.display(),
    )
}

fn script_pipeline(path: &std::path::Path, parallelism: usize) -> String {
    format!(
        r#"
sources:
  file_src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    parallelism: 1
    mode:
      type: bounded

transforms:
  normalize:
    type: script
    from: file_src
    language: javascript
    primary_key: id
    parallelism: {parallelism}
    batch_size: 2
    script: |
      function(input) {{
        input.value = input.value.toUpperCase();
        return input;
      }}

sinks:
  print_sink:
    type: print
    from: normalize
    sample_every: 1
"#,
        path = path.display(),
    )
}

/// Collects the `id` column from a captured print-sink run, sorted.
async fn run_and_collect_ids(ctx: &TestContext, pipeline: &str) -> Vec<i64> {
    let output = ctx
        .run_pipeline_with_capture(
            pipeline,
            PipelineOpts::new().timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Pipeline should complete successfully");

    let mut ids: Vec<i64> = output
        .column_values("id")
        .iter()
        .filter_map(|v| v.as_i64())
        .collect();
    ids.sort();
    ids
}

/// A transform's `parallelism` widens a source narrower than it: the exchange is
/// planned *under* the transform's SQL, so the transform's own work runs at the
/// requested width and everything above it inherits that width.
///
/// The property under test is that the 1 -> 4 hash exchange is lossless.
#[tokio::test]
async fn transform_parallelism_widens_a_narrow_source() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let (dir, total) = write_csv_files(&ctx, "widen", 4);

    // `parallelism: 1` on the source pins it to a single partition even though
    // four files are present, so the transform is genuinely widening.
    let ids = run_and_collect_ids(&ctx, &pipeline(&dir, 1, Some(4), None)).await;

    assert_eq!(
        ids,
        (1..=total).collect::<Vec<i64>>(),
        "every row must survive the 1 -> 4 exchange exactly once"
    );
}

/// Script parallelism is physical plan width, not row slicing inside a single
/// stream. This covers the complete 1 -> 4 path, including per-stream
/// rebatching, one WASM instance per partition, and terminal checkpoint drain.
#[tokio::test]
async fn script_parallelism_widens_a_narrow_source() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let (dir, total) = write_csv_files(&ctx, "script_widen", 4);

    let ids = run_and_collect_ids(&ctx, &script_pipeline(&dir, 4)).await;

    assert_eq!(
        ids,
        (1..=total).collect::<Vec<i64>>(),
        "every row must pass through one of the four WASM streams exactly once"
    );
}

/// The reverse direction: a wide source narrowed by a transform. Rows are
/// re-placed by key onto fewer streams, which is the same hash exchange running
/// 4 -> 2.
#[tokio::test]
async fn transform_parallelism_narrows_a_wide_source() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let (dir, total) = write_csv_files(&ctx, "narrow", 4);

    let ids = run_and_collect_ids(&ctx, &pipeline(&dir, 4, Some(2), None)).await;

    assert_eq!(
        ids,
        (1..=total).collect::<Vec<i64>>(),
        "every row must survive the 4 -> 2 exchange exactly once"
    );
}

/// `parallelism: 1` on a sink collapses a parallel source back to one write
/// stream. That path is a `StreamingCoalesceExec` rather than a hash exchange —
/// one stream trivially holds every key — and it is where checkpoint markers
/// from the parallel branches are aligned back down to a single copy.
#[tokio::test]
async fn sink_parallelism_one_coalesces_a_parallel_source() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let (dir, total) = write_csv_files(&ctx, "coalesce", 4);

    let ids = run_and_collect_ids(&ctx, &pipeline(&dir, 4, None, Some(1))).await;

    assert_eq!(
        ids,
        (1..=total).collect::<Vec<i64>>(),
        "coalescing 4 streams into 1 must not drop or duplicate a row"
    );
}

/// Two exchanges in one plan, in opposite directions: the source is widened by
/// the transform and then narrowed again at the sink. The sink's exchange is
/// round-robin (a print sink has no ordering requirement), so this also covers a
/// hash exchange feeding a round-robin one.
#[tokio::test]
async fn widen_then_narrow_preserves_every_row() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let (dir, total) = write_csv_files(&ctx, "widen_narrow", 4);

    let ids = run_and_collect_ids(&ctx, &pipeline(&dir, 2, Some(4), Some(2))).await;

    assert_eq!(
        ids,
        (1..=total).collect::<Vec<i64>>(),
        "rows must survive 2 -> 4 -> 2 without loss or duplication"
    );
}

/// A source already at the requested width needs no exchange at all — the
/// planner elides it. Nothing observable should change, which is the point: this
/// pins that the elision path still delivers every row.
#[tokio::test]
async fn matching_widths_need_no_exchange() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let (dir, total) = write_csv_files(&ctx, "elided", 3);

    let ids = run_and_collect_ids(&ctx, &pipeline(&dir, 3, Some(3), None)).await;

    assert_eq!(
        ids,
        (1..=total).collect::<Vec<i64>>(),
        "an elided exchange must still deliver every row"
    );
}
