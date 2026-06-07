# streamling-e2e

End-to-end test framework for streamling pipelines.

## Purpose

This crate provides black-box testing of the `streamling` binary against a real k3s environment with PostgreSQL, Kafka (Redpanda), ClickHouse, and Prometheus.

## Key Design Principles

1. **No internal dependencies** - This crate does NOT depend on other streamling crates. It executes `streamling` as an external binary.
2. **Resource isolation** - Each test creates unique databases/topics using UUID prefixes to prevent test interference.
3. **Automatic cleanup** - Resources are cleaned up when `TestContext` is dropped.

## Structure

```
src/
├── lib.rs           # TestContext, PipelineOpts, E2eConfig
├── streamling.rs    # Binary execution helpers
└── resources/
    ├── postgres.rs  # PostgreSQL database management
    ├── kafka.rs     # Kafka topic/schema management
    └── clickhouse.rs # ClickHouse database management

tests/
├── postgres_sink.rs  # PostgreSQL sink tests (7 scenarios)
├── clickhouse_sink.rs # ClickHouse sink tests
└── plugin_udf_bridge.rs # plugin UDF bridge scalar-literal regression (Kafka → SQL → blackhole)
```

## Writing Tests

### Basic Pattern

```rust
#[tokio::test]
async fn test_my_scenario() {
    init_tracing();

    // 1. Create isolated test context
    let ctx = TestContext::new().await.expect("Failed to create context");

    // 2. Register schema and produce test data
    ctx.kafka.register_schema(SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&records).await.unwrap();

    // 3. Define pipeline YAML (must include `transforms: {}`)
    let pipeline = format!(r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: my_table
    schema: public
    primary_key: id
    on_conflict: update
"#, topic = ctx.kafka_topic);

    // 4. Run pipeline with record limit
    let status = ctx.run_pipeline(&pipeline, 10).await.unwrap();
    assert!(status.success());

    // 5. Verify results
    let count = ctx.pg_count("SELECT COUNT(*) FROM public.my_table").await.unwrap();
    assert_eq!(count, 10);
}
```

### Important Notes

- **Always include `transforms: {}`** in pipeline YAML (required field)
- **Use `run_pipeline_with_opts`** with `record_limit()` and `env("STREAMLING__RECORD_BATCH_SIZE", "1")` for tests that need precise record counting
- **Delete operations** use `ctx.kafka.produce_avro_records_with_op(&records, "d")`
- **ClickHouse tests** require `TestContext::with_options(TestContextOptions::new().with_clickhouse())`

## Running Tests

```bash
# Setup k3s environment (first time)
just env-setup

# Check environment status
just env-status

# Run all e2e tests
just e2e-test

# Run specific test
just e2e-test test_basic_postgres_sink

# Run with debug output
just e2e-test-debug

# List available tests
just e2e-list
```

## Environment Variables

Set automatically by `just e2e-test`:

| Variable                  | Description                         |
| ------------------------- | ----------------------------------- |
| `E2E_POSTGRES_URL`        | PostgreSQL connection string        |
| `E2E_KAFKA_BROKER`        | Kafka broker address                |
| `E2E_SCHEMA_REGISTRY_URL` | Schema registry URL                 |
| `E2E_CLICKHOUSE_URL`      | ClickHouse HTTP URL                 |
| `E2E_STREAMLING_BIN`      | Path to pre-built streamling binary |

## Test Scenarios (postgres_sink.rs)

1. **test_basic_postgres_sink** - Basic data flow
2. **test_multiple_batches** - Multiple batch processing
3. **test_jsonb_types** - Nested struct/array as JSONB
4. **test_deduplication** - Primary key upsert behavior
5. **test_delete_operations** - Delete via `dbz.op='d'` header
6. **test_composite_primary_key** - Multi-column primary key
7. **test_on_conflict_nothing** - Skip duplicates behavior

## Troubleshooting

- **Tests hang**: Add `batch_size: 1` to sink config and `env("STREAMLING__RECORD_BATCH_SIZE", "1")` to pipeline opts
- **Context errors**: Run `just env-status` to check cluster state
- **Wrong kubectl context**: Run `kubectl config use-context k3d-streamling-e2e`
