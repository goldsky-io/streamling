//! File source e2e tests.
//!
//! Streamling reads files from a temp directory, a print sink captures output to assert.
//! Verifies schema inference, the synthesized `_gs_op = 'i'` column, Hive-partition
//! inference, and fail-fast on bad paths.
//!
//! The file source uses neither Kafka nor Postgres, but `TestContext` still
//! provisions them, so these tests require the e2e Docker stack like the rest.

use std::fs;
use std::time::Duration;

use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

/// CSV file source: schema is inferred and a constant `_gs_op = 'i'` column is
/// synthesized for the append-only rows.
#[tokio::test]
async fn file_source_csv_synthesizes_gs_op() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let data_dir = ctx.temp_dir.path().join("csv_data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("data.csv"),
        "id,name\n1,alice\n2,bob\n3,carol\n",
    )
    .expect("write csv");

    // Point at the directory (trailing slash) so the listing matches by `.csv`.
    let pipeline = format!(
        r#"
sources:
  file_src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded

transforms: {{}}

sinks:
  print_sink:
    type: print
    from: file_src
    sample_every: 1
"#,
        path = data_dir.display()
    );

    // No record limit: the bounded file source reaches EOF and the pipeline
    // terminates on its own.
    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new())
        .await
        .expect("Pipeline should complete successfully");

    assert_eq!(output.len(), 3, "expected 3 rows from the CSV file");
    assert!(
        output.has_column("id"),
        "inferred id column; got {:?}",
        output.column_names()
    );
    assert!(output.has_column("name"), "inferred name column");

    for row in output.rows() {
        assert_eq!(
            row.row_kind, "Insert",
            "append-only file rows must be inserts"
        );
    }
    let ops = output.column_values("_gs_op");
    assert_eq!(ops.len(), 3, "every row should carry a synthesized _gs_op");
    for op in ops {
        assert_eq!(op.as_str(), Some("i"), "synthesized _gs_op must be 'i'");
    }
}

/// Continuous file source: ingests the files present at startup, then picks up a
/// new file dropped in after the pipeline is already running. The watermark stops
/// the initial files from being re-read, so the run only reaches the record limit
/// once the new file is discovered on a later poll.
#[tokio::test]
async fn file_source_continuous_picks_up_new_files() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let data_dir = ctx.temp_dir.path().join("continuous_data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    // Three rows are present at startup; two more arrive while the source polls.
    fs::write(
        data_dir.join("initial.csv"),
        "id,name\n1,alice\n2,bob\n3,carol\n",
    )
    .expect("write initial csv");

    let pipeline = format!(
        r#"
sources:
  file_src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: continuous
      poll_interval: 1s

transforms: {{}}

sinks:
  print_sink:
    type: print
    from: file_src
    sample_every: 1
"#,
        path = data_dir.display()
    );

    // The continuous source never self-terminates, so bound the run by record
    // count. The limit (5) exceeds the 3 startup rows, so the run can only finish
    // after the 2-row file that is dropped in mid-run is discovered.
    let new_file = data_dir.join("new.csv");
    let writer = async {
        tokio::time::sleep(Duration::from_secs(3)).await;
        fs::write(&new_file, "id,name\n4,dave\n5,erin\n").expect("write new csv");
    };

    let (result, ()) = tokio::join!(
        ctx.run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(5)),
        writer,
    );
    let output = result.expect("Pipeline should complete successfully");

    // Reaching the limit of 5 is itself proof the mid-run file was ingested: the
    // 3 startup rows are read once (the watermark blocks re-reads), so the run
    // cannot finish without the 2 rows from `new.csv`.
    assert_eq!(
        output.len(),
        5,
        "expected 3 startup rows plus 2 from the file added mid-run"
    );

    for row in output.rows() {
        assert_eq!(
            row.row_kind, "Insert",
            "append-only file rows must be inserts"
        );
    }

    let ops = output.column_values("_gs_op");
    assert_eq!(ops.len(), 5, "every row should carry a synthesized _gs_op");
    for op in ops {
        assert_eq!(op.as_str(), Some("i"), "synthesized _gs_op must be 'i'");
    }
}

/// Hive-style partition columns encoded in the path are inferred into the schema.
#[tokio::test]
async fn file_source_infers_hive_partition_columns() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let root = ctx.temp_dir.path().join("hive_data");
    let p1 = root.join("dt=2024-01-01");
    let p2 = root.join("dt=2024-01-02");
    fs::create_dir_all(&p1).expect("create partition 1");
    fs::create_dir_all(&p2).expect("create partition 2");
    fs::write(p1.join("data.csv"), "id,name\n1,alice\n2,bob\n").expect("write partition 1");
    fs::write(p2.join("data.csv"), "id,name\n3,carol\n").expect("write partition 2");

    let pipeline = format!(
        r#"
sources:
  file_src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded

transforms: {{}}

sinks:
  print_sink:
    type: print
    from: file_src
    sample_every: 1
"#,
        path = root.display()
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new())
        .await
        .expect("Pipeline should complete successfully");

    assert_eq!(output.len(), 3, "expected 3 rows across both partitions");
    assert!(
        output.has_column("dt"),
        "Hive partition column 'dt' should be inferred from the path; got {:?}",
        output.column_names()
    );
}

/// A path matching no files fails fast instead of producing a silent empty source.
#[tokio::test]
async fn file_source_empty_directory_fails_fast() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let empty_dir = ctx.temp_dir.path().join("empty_data");
    fs::create_dir_all(&empty_dir).expect("create empty dir");

    let pipeline = format!(
        r#"
sources:
  file_src:
    type: file
    path: {path}/
    format: parquet

transforms: {{}}

sinks:
  print_sink:
    type: print
    from: file_src
    sample_every: 1
"#,
        path = empty_dir.display()
    );

    let output = ctx
        .run_pipeline_raw(&pipeline, PipelineOpts::new())
        .await
        .expect("should capture output even on failure");

    assert!(
        !output.status.success(),
        "a path matching no files should fail, not produce a silent empty source"
    );
    assert!(
        output.stderr.contains("no files matching"),
        "stderr should explain no files were found, got: {}",
        output.stderr
    );
}

/// An unsupported remote scheme is rejected with a clear error.
#[tokio::test]
async fn file_source_unsupported_scheme_fails() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let pipeline = r#"
sources:
  file_src:
    type: file
    path: ftp://example.com/data
    format: parquet

transforms: {}

sinks:
  print_sink:
    type: print
    from: file_src
    sample_every: 1
"#;

    let output = ctx
        .run_pipeline_raw(pipeline, PipelineOpts::new())
        .await
        .expect("should capture output even on failure");

    assert!(
        !output.status.success(),
        "an unsupported remote scheme should fail, not be silently accepted"
    );
    assert!(
        output.stderr.contains("unsupported scheme"),
        "stderr should explain the scheme is unsupported, got: {}",
        output.stderr
    );
}
