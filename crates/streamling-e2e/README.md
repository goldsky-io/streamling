# streamling-e2e

End-to-end test framework for Streamling pipelines.

## Overview

This crate provides a lightweight e2e testing framework that runs Streamling pipelines against real services (PostgreSQL, Kafka, ClickHouse) in a local k3s cluster. This framework:

- **Runs the Streamling binary** as an external process (no internal crate dependencies)
- **Uses a persistent k3s cluster** (no container startup per test)
- **Isolates tests** via unique databases/topics per test run
- **Builds once, runs many** - the binary is built once before all tests

## Prerequisites

- [k3d](https://k3d.io/) - Lightweight Kubernetes in Docker
- [kubectl](https://kubernetes.io/docs/tasks/tools/) - Kubernetes CLI
- [cargo-nextest](https://nexte.st/) - Fast test runner (`cargo install cargo-nextest`)

## Quick Start

```bash
# 1. Set up the k3s cluster with all services
just env-setup

# 2. Run e2e tests
just e2e-test

# 3. (Optional) Tear down when done
just env-teardown
```

## Commands

### Environment Management

```bash
just env-setup      # Create k3s cluster and deploy services
just env-status     # Check cluster and service status
just env-teardown   # Delete the k3s cluster
just env-vars       # Print environment variables (use: eval $(just env-vars))
```

### Running Tests

```bash
just e2e-test                           # Run all e2e tests
just e2e-test postgres_sink             # Run tests matching "postgres_sink"
just e2e-test test_deduplication        # Run a specific test
just e2e-test --no-capture              # Run with output visible
just e2e-test-debug <test_name>         # Run a specific test with full output
just e2e-list                           # List available tests
just e2e-inspect <test_id>              # Inspect resources for a test (e.g., test_abc123)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        k3s Cluster                               │
│  ┌──────────┐  ┌──────────┐  ┌────────────┐  ┌──────────────┐  │
│  │PostgreSQL│  │ Redpanda │  │ ClickHouse │  │  Prometheus  │  │
│  │  :30432  │  │  :30092  │  │   :30123   │  │    :30090    │  │
│  └──────────┘  │  :30081  │  └────────────┘  └──────────────┘  │
│                │ (schema) │                                      │
│                └──────────┘                                      │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │ NodePort
                              │
┌─────────────────────────────┼───────────────────────────────────┐
│  E2E Test                   │                                    │
│  ┌──────────────────────────┴───────────────────────────────┐  │
│  │                     TestContext                           │  │
│  │  • Creates isolated DB/topic per test (UUID-based)        │  │
│  │  • Produces test data to Kafka                            │  │
│  │  • Runs streamling binary with pipeline YAML              │  │
│  │  • Verifies results in PostgreSQL/ClickHouse              │  │
│  │  • Cleans up resources on drop                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                    │
│                              ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              target/release/streamling                    │  │
│  │  (executed as subprocess with pipeline YAML + env vars)   │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Writing Tests

```rust
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};
use serde::Serialize;

#[derive(Serialize)]
struct MyRecord {
    id: i64,
    value: String,
}

const SCHEMA: &str = r#"{
    "type": "record",
    "name": "MyRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"}
    ]
}"#;

#[tokio::test]
async fn test_my_pipeline() {
    init_tracing();

    // Create isolated test context (unique DB + topic)
    let ctx = TestContext::new().await.unwrap();

    // Register schema and produce test data
    ctx.kafka.register_schema(SCHEMA).await.unwrap();
    ctx.kafka.produce_avro_records(&[
        MyRecord { id: 1, value: "hello".into() },
    ]).await.unwrap();

    // Define pipeline YAML
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

    // Run pipeline (stops after 1 record)
    let status = ctx.run_pipeline(&pipeline, 1).await.unwrap();
    assert!(status.success());

    // Verify results
    let count = ctx.postgres.count("SELECT COUNT(*) FROM public.my_table").await.unwrap();
    assert_eq!(count, 1);
}
```

## Environment Variables

The test framework reads these from the k3s setup:

| Variable                  | Description                  | Default                                                                 |
| ------------------------- | ---------------------------- | ----------------------------------------------------------------------- |
| `E2E_POSTGRES_URL`        | PostgreSQL connection string | `postgres://postgres:postgres@localhost:30432/postgres?sslmode=disable` |
| `E2E_KAFKA_BROKER`        | Kafka broker address         | `localhost:30092`                                                       |
| `E2E_SCHEMA_REGISTRY_URL` | Schema Registry URL          | `http://localhost:30081`                                                |
| `E2E_CLICKHOUSE_URL`      | ClickHouse HTTP URL          | `http://localhost:30123`                                                |
| `E2E_PROMETHEUS_URL`      | Prometheus URL               | `http://localhost:30090`                                                |
| `E2E_STREAMLING_BIN`      | Path to streamling binary    | Set by `just e2e-test`                                                  |

## Debugging

```bash
# Run a single test with full output
just e2e-test-debug test_basic_postgres_sink

# Inspect resources for a failed test
just e2e-inspect test_abc123

# Check k3s services
just env-status
kubectl get pods -n streamling-e2e
kubectl logs -n streamling-e2e deployment/postgres

# Watch logs from a service
stern postgres -n streamling-e2e
stern redpanda -n streamling-e2e
```

## Design Principles

1. **No internal dependencies** - Tests execute the streamling binary, not internal crate code
2. **Resource isolation** - Each test gets unique database/topic names via UUIDs
3. **Automatic cleanup** - Resources are cleaned up when `TestContext` is dropped
4. **Fast iteration** - Build binary once, run many tests against persistent services
5. **Real services** - Tests run against actual PostgreSQL, Kafka, ClickHouse (not mocks)
