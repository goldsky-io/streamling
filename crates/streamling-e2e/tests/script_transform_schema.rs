//! Permanent e2e coverage for the schema-aware `type: script` output path
//! (see `crates/streamling-core/wasm/runtime.js`'s `runSchemaAware` and
//! `crates/streamling-core/src/operators/wasm_runner.rs`'s
//! `decode_json_output`): the production traces transform and its declared
//! `schema:` against a Kafka Avro source and a Postgres sink, checked against
//! exact expected rows for the real production payload, a handful of
//! hand-checkable edge cases, and a small type-coercion script.
//!
//! Runs against a single, normally-built `streamling` binary (whatever
//! `E2E_STREAMLING_BIN`/the default build points at).

use serde::Serialize;
use streamling_e2e::{PipelineOpts, TestContext};

// ============================================================================
// Kafka input: same Avro schema as the traces source used elsewhere, a JSON
// string of trace objects in the `traces` field.
// ============================================================================

const TRACES_AVRO_SCHEMA: &str = r#"{"type":"record","name":"TracesByTransaction","fields":[{"name":"id","type":"string"},{"name":"block_number","type":["null","long"],"default":null},{"name":"block_hash","type":["null","string"],"default":null},{"name":"block_timestamp","type":"long"},{"name":"transaction_hash","type":["null","string"],"default":null},{"name":"transaction_index","type":["null","long"],"default":null},{"name":"traces","type":["null","string"],"default":null}]}"#;

#[derive(Debug, Clone, Serialize)]
struct TracesMsg {
    id: String,
    block_number: Option<i64>,
    block_hash: Option<String>,
    block_timestamp: i64,
    transaction_hash: Option<String>,
    transaction_index: Option<i64>,
    traces: Option<String>,
}

fn msg(id: &str, block_hash: &str, tx_hash: &str, traces: Option<String>) -> TracesMsg {
    TracesMsg {
        id: id.to_string(),
        block_number: Some(51_666_906),
        block_hash: Some(block_hash.to_string()),
        block_timestamp: 1_790_123_159,
        transaction_hash: Some(tx_hash.to_string()),
        transaction_index: None,
        traces,
    }
}

/// The verbatim real production payload: id
/// "geth_traces_by_transaction_0xfa585a755a7ccd7c79c2acd548dc5effa86b309a40c6784a35dff638e3f8bd6e_22",
/// block_number 51666906, block_timestamp 1790123159, two traces (a root
/// call and its one delegatecall child).
const M1_TRACES: &str = r#"[{"type": "geth_trace", "block_number": 51666906, "block_hash": "0xfa585a755a7ccd7c79c2acd548dc5effa86b309a40c6784a35dff638e3f8bd6e", "transaction_hash": "0x6ba8a84bf36840f62f5d4319ec82f2f19466016385ebc8da86ee42f0a0a3d41c", "transaction_index": null, "from_address": "0xee6cb1eeb0a141a54b5c5faa94a50859c288e120", "to_address": "0xede940cdf2a9c5620cbf97e45947594723e29c14", "value": 0, "input": "0x1667d87500000000000000000000000000000000000000002152431b2152411022461eca", "output": null, "trace_type": "call", "call_type": "call", "reward_type": null, "gas": 60000, "gas_used": 31985, "subtraces": 1, "trace_address": [], "error": null, "status": 1, "trace_id": "call_0x6ba8a84bf36840f62f5d4319ec82f2f19466016385ebc8da86ee42f0a0a3d41c_", "trace_index": null, "before_evm_transfers": null, "after_evm_transfers": null, "tx_from_address": "0xee6cb1eeb0a141a54b5c5faa94a50859c288e120", "tx_to_address": "0xede940cdf2a9c5620cbf97e45947594723e29c14"}, {"type": "geth_trace", "block_number": 51666906, "block_hash": "0xfa585a755a7ccd7c79c2acd548dc5effa86b309a40c6784a35dff638e3f8bd6e", "transaction_hash": "0x6ba8a84bf36840f62f5d4319ec82f2f19466016385ebc8da86ee42f0a0a3d41c", "transaction_index": null, "from_address": "0xede940cdf2a9c5620cbf97e45947594723e29c14", "to_address": "0x6d9dd143e42b6338f4f6a7c0c26d124658f641cb", "value": 0, "input": "0x1667d87500000000000000000000000000000000000000002152431b2152411022461eca", "output": null, "trace_type": "call", "call_type": "delegatecall", "reward_type": null, "gas": 32825, "gas_used": 5301, "subtraces": 0, "trace_address": [0], "error": null, "status": 1, "trace_id": "call_0x6ba8a84bf36840f62f5d4319ec82f2f19466016385ebc8da86ee42f0a0a3d41c_0", "trace_index": null, "before_evm_transfers": null, "after_evm_transfers": null, "tx_from_address": "0xee6cb1eeb0a141a54b5c5faa94a50859c288e120", "tx_to_address": "0xede940cdf2a9c5620cbf97e45947594723e29c14"}]"#;

const M1_ID: &str =
    "geth_traces_by_transaction_0xfa585a755a7ccd7c79c2acd548dc5effa86b309a40c6784a35dff638e3f8bd6e_22";

// ============================================================================
// Production TS traces transform and its declared output schema.
// ============================================================================

const TRACES_TS: &str = include_str!("fixtures/traces_transform.ts");

const P1_SCHEMA: &str = r#"      _gs_op: string
      block_number: int64
      block_timestamp: int64
      call_depth: int64
      call_type: int64
      callee: string
      caller: string
      function_signature: string
      id: string
      parent_function_signature: string
      success: boolean
      trace_address: string
      tx_from: string
      tx_to: string
      txn_hash: string"#;

fn yaml_indent(text: &str, indent: usize) -> String {
    let pad = " ".repeat(indent);
    text.lines()
        .map(|l| {
            if l.is_empty() {
                String::new()
            } else {
                format!("{pad}{l}")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn script_pipeline_yaml(topic: &str, table: &str, script: &str, schema_block: &str) -> String {
    let script_indented = yaml_indent(script, 6);
    format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  script_transform:
    type: script
    from: kafka_source
    language: typescript
    primary_key: id
    parallelism: 1
    schema:
{schema_block}
    script: |
{script_indented}

sinks:
  pg_sink:
    type: postgres
    from: script_transform
    table: {table}
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#
    )
}

/// Small batches keep the run deterministic and fast for a handful of rows
/// (see AGENTS.md's "Tests hang" pitfall).
fn small_batch_opts(record_limit: u64) -> PipelineOpts {
    PipelineOpts::new()
        .record_limit(record_limit)
        .env("STREAMLING__RECORD_BATCH_SIZE", "1")
        .env("STREAMLING__INTERNAL_BUFFER_SIZE", "1")
        .timeout(std::time::Duration::from_secs(60))
}

#[tokio::test]
async fn test_script_transform_schema_traces_production_and_edges() {
    streamling_e2e::init_tracing();

    let ctx = TestContext::new().await.expect("create test context");
    ctx.kafka
        .register_schema(TRACES_AVRO_SCHEMA)
        .await
        .expect("register avro schema");

    // M1: the real production payload (2 traces -> 2 rows).
    let m1 = msg(
        M1_ID,
        "0xfa585a755a7ccd7c79c2acd548dc5effa86b309a40c6784a35dff638e3f8bd6e",
        "0x6ba8a84bf36840f62f5d4319ec82f2f19466016385ebc8da86ee42f0a0a3d41c",
        Some(M1_TRACES.to_string()),
    );

    // Null to_address -> callee must be "" (not null, not "null").
    let null_to_address_traces = serde_json::json!([{
        "transaction_hash": "0xNullToTxHash00000000000000000000000000000000000001",
        "block_number": 1,
        "from_address": "0xfrom0000000000000000000000000000000001",
        "to_address": null,
        "input": "0x12345678",
        "trace_type": "call",
        "call_type": "call",
        "status": 1,
        "trace_address": [],
        "tx_from_address": "0xfrom0000000000000000000000000000000001",
        "tx_to_address": "0xto00000000000000000000000000000000001",
    }])
    .to_string();
    let null_to_address_msg = msg(
        "null_to_address_msg",
        "0xblockhash",
        "0xNullToTxHash00000000000000000000000000000000000001",
        Some(null_to_address_traces),
    );

    // Input under 10 chars -> function_signature must default to "0x00000000".
    let short_input_traces = serde_json::json!([{
        "transaction_hash": "0xShortInputTxHash0000000000000000000000000000000001",
        "block_number": 1,
        "from_address": "0xfrom0000000000000000000000000000000001",
        "to_address": "0xto00000000000000000000000000000000001",
        "input": "0x1234567", // 9 chars, one short of the 10-char selector length
        "trace_type": "call",
        "call_type": "call",
        "status": 1,
        "trace_address": [],
        "tx_from_address": "0xfrom0000000000000000000000000000000001",
        "tx_to_address": "0xto00000000000000000000000000000000001",
    }])
    .to_string();
    let short_input_msg = msg(
        "short_input_msg",
        "0xblockhash",
        "0xShortInputTxHash0000000000000000000000000000000001",
        Some(short_input_traces),
    );

    // Orphan child: trace_address [0, 0, 5] exists but its immediate parent
    // [0, 0] does not (only the root [] and the [0, 0, 5] child are present).
    // The transform must fall back to the child's own from_address as caller
    // instead of failing.
    let orphan_tx = "0xOrphanTxHash000000000000000000000000000000000000001";
    let orphan_traces = serde_json::json!([
        {
            "transaction_hash": orphan_tx,
            "block_number": 1,
            "from_address": "0xfrom0000000000000000000000000000000001",
            "to_address": "0xto00000000000000000000000000000000001",
            "input": "0x12345678",
            "trace_type": "call",
            "call_type": "call",
            "status": 1,
            "trace_address": [],
            "tx_from_address": "0xfrom0000000000000000000000000000000001",
            "tx_to_address": "0xto00000000000000000000000000000000001",
        },
        {
            "transaction_hash": orphan_tx,
            "block_number": 1,
            "from_address": "0xorphanfrom000000000000000000000000000001",
            "to_address": "0xorphanto0000000000000000000000000000001",
            "input": "0x99999999",
            "trace_type": "call",
            "call_type": "call",
            "status": 1,
            "trace_address": [0, 0, 5],
            "tx_from_address": "0xfrom0000000000000000000000000000000001",
            "tx_to_address": "0xto00000000000000000000000000000000001",
        },
    ])
    .to_string();
    let orphan_msg = msg("orphan_msg", "0xblockhash", orphan_tx, Some(orphan_traces));

    // Unicode in the `error` field: the transform never reads or emits
    // `error`, so it must be silently ignored (not surfaced, not corrupting
    // the row, not crashing the script).
    let hostile_tx = "0xHostileTxHash00000000000000000000000000000000000001";
    let hostile_traces = serde_json::json!([{
        "transaction_hash": hostile_tx,
        "block_number": 1,
        "from_address": "0xfrom0000000000000000000000000000000001",
        "to_address": "0xto00000000000000000000000000000000001",
        "input": "0x12345678",
        "trace_type": "call",
        "call_type": "call",
        "status": 1,
        "trace_address": [],
        "error": "emoji \u{1F389} and accent \u{00e9}",
        "tx_from_address": "0xfrom0000000000000000000000000000000001",
        "tx_to_address": "0xto00000000000000000000000000000000001",
    }])
    .to_string();
    let hostile_msg = msg(
        "hostile_error_msg",
        "0xblockhash",
        hostile_tx,
        Some(hostile_traces),
    );

    ctx.kafka
        .produce_avro_records(&[
            m1,
            null_to_address_msg,
            short_input_msg,
            orphan_msg,
            hostile_msg,
        ])
        .await
        .expect("produce avro records");

    let table = "script_schema_traces";
    let pipeline = script_pipeline_yaml(&ctx.kafka_topic, table, TRACES_TS, P1_SCHEMA);

    // 5 messages -> 2 (M1) + 1 + 1 + 2 (orphan) + 1 = 7 output rows.
    let status = ctx
        .run_pipeline_with_opts(&pipeline, small_batch_opts(7))
        .await
        .expect("run pipeline");
    assert!(status.success(), "pipeline exited with {status:?}");

    let count = ctx
        .postgres
        .count(&format!("SELECT COUNT(*) FROM public.{table}"))
        .await
        .expect("count rows");
    assert_eq!(count, 7, "expected 7 output rows");

    async fn row_by_id(
        postgres: &streamling_e2e::resources::PostgresResource,
        table: &str,
        id: &str,
    ) -> Vec<Vec<String>> {
        postgres
            .query_rows_as_text(&format!(
                "SELECT caller, callee, function_signature, parent_function_signature, \
                 call_depth::text, trace_address, success::text, call_type::text, \
                 block_number::text, block_timestamp::text, txn_hash, tx_from, tx_to \
                 FROM public.{table} WHERE id = '{id}'"
            ))
            .await
            .expect("query row")
    }

    // M1 root: trace_address [] -> "0", caller/callee from the top call.
    let root_id = format!("{M1_ID}_0");
    let root_rows = row_by_id(&ctx.postgres, table, &root_id).await;
    assert_eq!(root_rows.len(), 1, "expected exactly one root row");
    assert_eq!(
        root_rows[0],
        vec![
            "0xee6cb1eeb0a141a54b5c5faa94a50859c288e120".to_string(), // caller
            "0xede940cdf2a9c5620cbf97e45947594723e29c14".to_string(), // callee
            "0x1667d875".to_string(),                                 // function_signature
            "0x00000000".to_string(),                                 // parent_function_signature
            "0".to_string(),                                          // call_depth
            "0".to_string(),                                          // trace_address
            "true".to_string(),                                       // success
            "0".to_string(),                                          // call_type (CALL)
            "51666906".to_string(),                                   // block_number
            "1790123159".to_string(),                                 // block_timestamp
            "0x6ba8a84bf36840f62f5d4319ec82f2f19466016385ebc8da86ee42f0a0a3d41c".to_string(),
            "0xee6cb1eeb0a141a54b5c5faa94a50859c288e120".to_string(), // tx_from
            "0xede940cdf2a9c5620cbf97e45947594723e29c14".to_string(), // tx_to
        ]
    );

    // M1 child: trace_address [0] -> "0,0", one level deeper, delegatecall.
    let child_id = format!("{M1_ID}_0,0");
    let child_rows = row_by_id(&ctx.postgres, table, &child_id).await;
    assert_eq!(child_rows.len(), 1, "expected exactly one child row");
    assert_eq!(
        child_rows[0],
        vec![
            "0xede940cdf2a9c5620cbf97e45947594723e29c14".to_string(), // caller
            "0x6d9dd143e42b6338f4f6a7c0c26d124658f641cb".to_string(), // callee
            "0x1667d875".to_string(),                                 // function_signature
            "0x1667d875".to_string(),                                 // parent_function_signature
            "1".to_string(),                                          // call_depth
            "0,0".to_string(),                                        // trace_address
            "true".to_string(),                                       // success
            "3".to_string(),                                          // call_type (DELEGATECALL)
            "51666906".to_string(),
            "1790123159".to_string(),
            "0x6ba8a84bf36840f62f5d4319ec82f2f19466016385ebc8da86ee42f0a0a3d41c".to_string(),
            "0xee6cb1eeb0a141a54b5c5faa94a50859c288e120".to_string(),
            "0xede940cdf2a9c5620cbf97e45947594723e29c14".to_string(),
        ]
    );

    // Null to_address -> callee "".
    let null_rows = ctx
        .postgres
        .query_rows_as_text(&format!(
            "SELECT callee FROM public.{table} WHERE id = 'null_to_address_msg_0'"
        ))
        .await
        .expect("query null-to_address row");
    assert_eq!(null_rows, vec![vec!["".to_string()]]);

    // Short input -> function_signature defaults to "0x00000000".
    let short_input_rows = ctx
        .postgres
        .query_rows_as_text(&format!(
            "SELECT function_signature FROM public.{table} WHERE id = 'short_input_msg_0'"
        ))
        .await
        .expect("query short-input row");
    assert_eq!(short_input_rows, vec![vec!["0x00000000".to_string()]]);

    // Orphan child [0, 0, 5]: parent [0, 0] is missing, so the transform
    // falls back to the child's own from_address as caller, and
    // parent_function_signature defaults to "0x00000000".
    let orphan_rows = ctx
        .postgres
        .query_rows_as_text(&format!(
            "SELECT caller, parent_function_signature, call_depth::text, trace_address \
             FROM public.{table} WHERE id = 'orphan_msg_0,0,0,5'"
        ))
        .await
        .expect("query orphan row");
    assert_eq!(
        orphan_rows,
        vec![vec![
            "0xorphanfrom000000000000000000000000000001".to_string(),
            "0x00000000".to_string(),
            "3".to_string(),
            "0,0,0,5".to_string(),
        ]]
    );

    // Unicode in `error` is ignored: the row is produced normally, and the
    // schema has no `error` column at all to leak into.
    let hostile_rows = ctx
        .postgres
        .query_rows_as_text(&format!(
            "SELECT caller, callee FROM public.{table} WHERE id = 'hostile_error_msg_0'"
        ))
        .await
        .expect("query hostile-error row");
    assert_eq!(
        hostile_rows,
        vec![vec![
            "0xfrom0000000000000000000000000000000001".to_string(),
            "0xto00000000000000000000000000000000001".to_string(),
        ]]
    );
}

// ============================================================================
// Coercion script: every declared column gets one consistently mismatched
// JS kind, exercising the fallback-to-IPC/Arrow-cast coercion path.
// ============================================================================

const COERCION_TS: &str = r#"
function transformCoercion(data: any): any {
  return {
    id: String(data.id),
    _gs_op: 'i',
    c_int_from_string: "7",
    c_int_from_float: 2.9,
    c_bool_from_string: "true",
    c_float_from_string: "2.5",
    c_string_from_number: 42,
    c_always_null: null,
  };
}
"#;

const P2_SCHEMA: &str = r#"      id: string
      _gs_op: string
      c_int_from_string: int64
      c_int_from_float: int64
      c_bool_from_string: boolean
      c_float_from_string: float64
      c_string_from_number: string
      c_always_null: int64
      c_always_missing: string"#;

#[tokio::test]
async fn test_script_transform_schema_coercion() {
    streamling_e2e::init_tracing();

    let ctx = TestContext::new().await.expect("create test context");
    ctx.kafka
        .register_schema(TRACES_AVRO_SCHEMA)
        .await
        .expect("register avro schema");

    let messages = vec![
        msg("coerce_1", "0xblockhash", "0xCoerceTxHash1", None),
        msg("coerce_2", "0xblockhash", "0xCoerceTxHash2", None),
    ];
    ctx.kafka
        .produce_avro_records(&messages)
        .await
        .expect("produce avro records");

    let table = "script_schema_coercion";
    let pipeline = script_pipeline_yaml(&ctx.kafka_topic, table, COERCION_TS, P2_SCHEMA);

    let status = ctx
        .run_pipeline_with_opts(&pipeline, small_batch_opts(2))
        .await
        .expect("run pipeline");
    assert!(status.success(), "pipeline exited with {status:?}");

    // Every column except `c_always_missing` (a declared column the script
    // never returns -- deliberately not asserted here, since its value is
    // the same order-dependent positional fallback the Arrow-IPC/inferred
    // path has always produced for a missing column, not a fixed value).
    let rows = ctx
        .postgres
        .query_rows_as_text(&format!(
            "SELECT c_int_from_string::text, c_int_from_float::text, \
             c_bool_from_string::text, c_float_from_string::text, \
             c_string_from_number, c_always_null::text \
             FROM public.{table} ORDER BY id"
        ))
        .await
        .expect("query coercion rows");

    assert_eq!(rows.len(), 2, "expected 2 output rows");
    for row in rows {
        assert_eq!(
            row,
            vec![
                "7".to_string(),    // "7" -> 7
                "2".to_string(),    // 2.9 -> 2 (cast truncates, doesn't round)
                "true".to_string(), // "true" -> true
                "2.5".to_string(),  // "2.5" -> 2.5
                "42".to_string(),   // 42 -> "42"
                "NULL".to_string(), // explicit null
            ]
        );
    }
}
