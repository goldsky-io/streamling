# Streamling

```

      ███████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗██╗     ██╗███╗   ██╗ ██████╗
      ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║██║     ██║████╗  ██║██╔════╝
      ███████╗   ██║   ██████╔╝█████╗  ███████║██╔████╔██║██║     ██║██╔██╗ ██║██║  ███╗
      ╚════██║   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║██║     ██║██║╚██╗██║██║   ██║
      ███████║   ██║   ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║███████╗██║██║ ╚████║╚██████╔╝
      ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝╚═╝╚═╝  ╚═══╝ ╚═════╝

```

Streamling is a columnar streaming runtime for real-time and historical data processing, extended through dynamically linked plugins.

It powers [Goldsky Turbo](https://goldsky.com/products/turbo-pipelines), where thousands of user-created pipelines run in production for banks, hedge funds, and prediction markets.

Install with one command (see [Quick start](#quick-start)) or read more at [streamling.dev](https://streamling.dev).

## Contents

- [Why Streamling](#why-streamling)
- [Quick start](#quick-start)
- [Common patterns](#common-patterns)
- [Development setup](#development-setup)
- [Reference](#reference)
- [Topology](#topology)
- [Dynamic Tables](#dynamic-tables)
- [Side Outputs](#side-outputs)
- [Checkpointing](#checkpointing)
- [State Backends](#state-backends)
- [Error Handling](#error-handling)
- [Upsert Semantics](#upsert-semantics)
- [Plugin System](#plugin-system)
- [Profiling](#profiling)
- [Benchmarking](#benchmarking)
- [Telemetry and Metrics](#telemetry-and-metrics)

## Why Streamling

- **Plugin system on a columnar data plane.** Custom sources, transforms, and sinks are separate Rust crates dynamically linked at runtime. No forking or recompiling the engine. Every stage exchanges Arrow RecordBatches and processes millions of rows per second on one node. In [benchmarks against Flink](https://www.streamling.dev/blog/streamling-0-3-0), Streamling delivered roughly 40x the throughput per GB of memory on a Kafka workload.
- **Transactional correctness.** At-least-once delivery through checkpointing. Checkpoint state lives in any Postgres database.
- **Declarative pipelines, built-in connectors.** Define topologies in YAML. Kafka, Postgres, ClickHouse, files, and webhooks ship in a single binary with fast startup.
- **Lean in production.** Allocator and Arrow-native decode work cut one customer workload writing million-row batches into ClickHouse from 31 GB of memory to 4 GB.

A pipeline has three sections: `sources`, `transforms`, and `sinks`.

```yaml
sources:
  raw.transactions:
    type: kafka
    topic: raw.event.transaction
transforms:
  large_transactions:
    type: sql
    primary_key: id
    sql: |
      SELECT *
      FROM raw.transactions
      WHERE amount > 1000
sinks:
  pg.large_transactions:
    from: large_transactions
    type: postgres
    schema: public
    table: large_transactions
    primary_key: id
```

**Streamling is a good fit when you need to:**

- **Write your own operators and reuse them**: implement a source, transform, or sink once in Rust (or a transform in WASM/TypeScript), then drop it into any pipeline with runtime-enforced checkpointing and schema validation
- **Run ongoing data processes** over continuous ordered inputs: event streams, database changelogs, polled APIs, or any plugin source
- **Build multi-stage flows on one columnar data plane**: plugins, SQL, WASM, HTTP enrichment, and [dynamic tables](#dynamic-tables) chained in one topology, all exchanging Arrow `RecordBatch`es
- **Move data with built-in connectors**: Kafka, Postgres, ClickHouse, files, and webhooks
- **Run bounded batch jobs** with the same logic as real-time pipelines, including upserts (INSERT/UPDATE/DELETE via `_gs_op`) into Postgres or ClickHouse

**Streamling is probably not the right fit when you need:**

- **Distributed stateful processing**: cross-partition joins, windowed aggregations, and coordinated checkpointing across nodes (custom plugins can cover some of these)
- **An embeddable library**: Streamling is a standalone runtime you deploy and configure

Streamling is a single-node engine. Scale horizontally with Kafka consumer groups and independent instances, each checkpointing on its own.

## Quick start

This is the simplest path to a running pipeline. Most production flows add plugin stages and multiple transforms. See [Common patterns](#common-patterns).

```bash
# 1. Install the runtime (macOS/Linux)
curl -fsSL https://www.streamling.dev/install.sh | bash

# 2. Create some data to read
mkdir -p data
cat > data/transactions.csv <<'EOF'
id,amount
1,500
2,1500
3,2500
4,750
EOF

# 3. Write a pipeline
cat > pipeline.yaml <<'EOF'
sources:
  raw_transactions:
    type: file
    path: ./data/
    format: csv
    primary_key: id
    mode:
      type: bounded
transforms:
  large_transactions:
    type: sql
    primary_key: id
    sql: |
      SELECT *
      FROM raw_transactions
      WHERE amount > 1000
sinks:
  print_large:
    type: print
    from: large_transactions
EOF

# 4. Run it
streamling pipeline.yaml
```

The bounded file source reads `data/transactions.csv` once, the SQL transform keeps the rows with `amount > 1000`, and the print sink writes them to stdout before the pipeline terminates on its own. Change `type: bounded` to `type: continuous` to keep polling the folder for new files.

You can also swap the print sink for [Postgres](#postgres-sink) or [webhook](#webhook-http-sink), or switch the source to [Kafka](#kafka-source) for a live stream, in production.

To **build from source** or run against local Kafka/Postgres/ClickHouse, see [Development setup](#development-setup).

## Common patterns

Mix the built-in connectors with [plugins](#plugin-system), SQL, WASM, and HTTP handlers; the runtime orchestrates them with the same delivery guarantees.


| Flow                                 | Stages                                                                                                       | What the runtime provides                                                                                   |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| Plugin → SQL → handler → WASM → sink | plugin source + sql + [HTTP handler](#http-handler-transform) + [WASM](#webassembly-script-transform) + sink | Ordered processing and at-least-once delivery through all stages. See [Plugin examples](#plugin-examples). |
| Plugin → plugin → sink               | custom source + custom transform + custom sink                                                               | Same guarantees on fully custom I/O (e.g. poll an API, apply domain logic, push to a partner system)        |
| Kafka → plugin transform → sink      | built-in source + plugin transform + built-in sink                                                           | Built-in source convenience + custom compute (decoding, external lookups, multi-record logic)               |
| Multi-source via hybrid              | bounded backfill in a datalake + live stream through kafka                                                   | Phase-ordered processing with checkpoint continuity. See [Hybrid Source](#hybrid-source).                   |
| Kafka → SQL → Postgres        | source + sql + postgres sink      | Simple movement and filter with no custom logic. See [Kafka Source](#kafka-source), [SQL Transform](#sql-transform), [Postgres Sink](#postgres-sink).         |
| ClickHouse → Postgres (batch) | clickhouse source + postgres sink | One-time or job-mode backfill. See [ClickHouse Source](#clickhouse-source).                                                                                   |
| Kafka → WASM → webhook        | source + script + webhook sink    | Lightweight scripted transform, push to an API. See [WebAssembly Script Transform](#webassembly-script-transform), [Webhook (HTTP) Sink](#webhook-http-sink). |
| Backfill then stream          | hybrid source + transforms + sink | Historical backfill then live streaming. See [Hybrid Source](#hybrid-source).                                                                                 |

### Plugin examples

Plugin ids appear as the `type` in pipeline YAML (`namespace.operator_name`). The examples below are building blocks of custom data flows. Each plugin stage is where custom logic lives, while the runtime wires them into the topology and applies the same checkpointing and delivery rules as built-in operators. They use the bundled [`basic_plugin`](plugin_examples/basic/); swap in your own plugin ids for production pipelines.

**Custom source**: generate or poll data the runtime doesn't have a built-in connector for:

```yaml
sources:
  orders:
    type: basic_plugin.random_source # e.g. acme_api.orders_source in production
    max_rows: 10000
    record_batch_size: 1000
transforms:
  filtered:
    type: sql
    primary_key: id
    sql: SELECT * FROM orders WHERE num_field > 100
sinks:
  pg_orders:
    type: postgres
    from: filtered
    schema: public
    table: orders
    primary_key: id
```

**Custom transform**: enrich or reshape records between built-in operators:

```yaml
sources:
  raw_events:
    type: kafka
    topic: raw.events
    primary_key: id
transforms:
  enriched:
    type: basic_plugin.filter_transform # e.g. enrichment.normalize_events in production
    from: raw_events
sinks:
  pg_events:
    type: postgres
    from: enriched
    schema: public
    table: events
    primary_key: id
```

**Custom sink**: write to a destination without a built-in connector:

```yaml
sources:
  raw_events:
    type: kafka
    topic: app.events
    primary_key: id
transforms: {}
sinks:
  partner_out:
    type: print_sink # e.g. partner_api.webhook_sink in production
    from: raw_events
    options:
      mode: batch
```

**Community plugins are available in the [streamling-community-plugins](https://github.com/goldsky-io/streamling-community-plugins) repository — see [Including Plugins](#including-plugins) for setup**.

## Development setup

This section is for **contributing to or building Streamling**. To run a pipeline, see [Quick start](#quick-start).

- Get [Rust and Cargo](https://www.rust-lang.org/learn/get-started)
- Install [Kubernetes](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/) and [OpenSSL](https://formulae.brew.sh/formula/openssl@3)
- Recommended IDE: [RustRover](https://www.jetbrains.com/rust/)

This project uses [just](https://github.com/casey/just) as a command runner. To see all available commands:

```bash
just
```

Common commands:

```bash
just build      # Build the project
just test       # Run unit tests
just lint       # Format and lint the project
just fix        # Auto-fix cargo issues
```

### Using k3s for Local Development

For a production-like environment, you can use k3d (k3s in Docker):

1. Run the setup script:

   ```bash
   ./scripts/k3s-setup.sh
   ```

   This creates a k3d cluster with PostgreSQL, ClickHouse, Redpanda (Kafka), and Prometheus.

2. Source the environment variables:

   ```bash
   source infra/local-k8s/env.sh
   ```

3. Verify services are running:

   ```bash
   kubectl get pods -n streamling-e2e
   ```

To tear down the cluster:

```bash
k3d cluster delete streamling-e2e
```

Default configuration is compiled into the binary (from
`crates/streamling-config/default_config.yaml`), so `streamling` runs as a standalone binary with no external files.
You can override the defaults by placing a `config.yaml` in the working directory, or using environment variables.
The repo root ships a `config.yaml` with local-development overrides (docker-compose hosts, throwaway credentials),
so running from the repo root behaves like local development always has.
The format of environment variables is the following: `STREAMLING__<property_name>`. If a property is nested,
use an additional double underscore (`__`) to separate the levels, for example
`STREAMLING__EXTERNAL_HTTP_HANDLER__TRIGGER_MAX_COUNT`. Precedence is: embedded defaults < `config.yaml` < environment
variables.

To run an e2e test (requires k3s cluster running):

```bash
cargo test -p streamling-e2e --test print_sink
```

Recommended environment variables when running locally:

- `RUST_LOG=debug`. Seeing debug logs is essential.
- `STREAMLING__PIPELINE_DEFINITION_LOCATION=pipeline.yaml`. You can switch between existing sample pipelines (or create
  your own).

## Reference

The sections below are the full connector and runtime reference. New here? Start with [Quick start](#quick-start).

## Topology

As you'll see below, all sources, transforms and sinks can be abstracted as nodes exchanging Arrow's `RecordBatch`es
using `SendableRecordBatchStream`. Most sources and transforms have a concept of an internal buffer, which is used to
accumulate `RecordBatch`es before passing them to the next operator. This is done to improve performance. The buffer
size is configurable via the `internal_buffer_size` parameter.

### Sources

Sources return a `SendableRecordBatchStream` as a result.

#### Kafka Source

The Kafka source allows consuming data from Kafka topics using Avro (with Schema Registry integration) or JSON
serialization. It's implemented as a custom DataFusion Table Provider (`TableProvider`) with the following key features:

- **Schema Management**:
  - For Avro, automatically fetches and converts Avro schemas from Schema Registry to Arrow schemas.
  - For JSON, uses the input schema declared in the source's `schema` field (Schema Registry is not used).
- **Message Processing**:
  - Converts Avro- or JSON-encoded messages to Arrow `RecordBatch`es.
    - The size of the batches is controlled by the `record_batch_size` and `record_batch_interval_ms` parameters.
  - Adds operation type column (`_gs_op`) to track INSERT/UPDATE/DELETE operations (see `Upsert Semantics` section
    below). The operation type is determined by the `dbz.op` header value.
  - Tombstone records (null/empty payloads, e.g. CDC deletes-as-tombstones) are **not supported** in any format
    and fail the source. Represent deletes with a non-empty payload plus a `dbz.op=d` header instead.

Kafka Source uses
high-level [StreamConsumer](https://docs.rs/rdkafka/latest/rdkafka/consumer/struct.StreamConsumer.html) which handles
everything related to partition assignment, rebalancing, etc. As a result, it doesn't support any parallelism (meaning
one partition from the DataFusion perspective). So, in order to scale, increase the number of pods in the Kubernetes
deployment (which will coordinate using the same consumer group name).

**Reliability**:

- Integrates with Streamling's checkpointing system (see `Checkpointing` section below).
- Maintains offset tracking per checkpoint epoch.
- Commits offsets after successful processing of all records in a checkpoint epoch.
- Skips committing offsets if the values haven't changed since the last epoch.

Sample configuration:

```yaml
sources:
  raw_events:
    type: kafka
    topic: app.events
```

**JSON format.** Set `data_format: json` (the default is `avro`) and declare the input schema as a `column → type` map.
Each Kafka message payload is treated as a single UTF-8 JSON object (one row); Schema Registry is not used.

```yaml
sources:
  raw_events:
    type: kafka
    topic: app.events
    data_format: json
    schema:
      id: int64
      name: string
      amount: float64
```

Supported type names match Arrow's (`int8`..`int64`, `uint8`..`uint64`, `float32`/`float64`, `string`, `boolean`,
`timestamp`, etc.). Decimals are written as `decimal128(precision, scale)` or `decimal256(precision, scale)`; the bare
names `decimal128`/`decimal256` (alias `decimal`) default to `(38, 9)`. All declared columns are nullable. If the
payload does not already contain `_gs_op`, it is added from the `dbz.op` header (defaulting to insert), exactly as for
Avro.

**Connection settings** — override the embedded defaults (local Kafka and Schema Registry) with these environment variables:

| Environment variable | Default | Description |
| --- | --- | --- |
| `STREAMLING__KAFKA_SOURCE__BROKERS` | `localhost:9092` | Comma-separated bootstrap broker list |
| `STREAMLING__KAFKA_SOURCE__SECURITY_PROTOCOL` | `plaintext` | Kafka security protocol (e.g. `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`) |
| `STREAMLING__KAFKA_SOURCE__SASL_MECHANISM` | _(unset)_ | SASL mechanism (applied when the protocol contains `sasl`) |
| `STREAMLING__KAFKA_SOURCE__SASL_USERNAME` | _(unset)_ | SASL username |
| `STREAMLING__KAFKA_SOURCE__SASL_PASSWORD` | _(unset)_ | SASL password |
| `STREAMLING__KAFKA_SOURCE__SCHEMA_REGISTRY_URL` | `http://localhost:18081` | Schema Registry URL (required for Avro) |
| `STREAMLING__KAFKA_SOURCE__SCHEMA_REGISTRY_USERNAME` | _(unset)_ | Schema Registry basic-auth username |
| `STREAMLING__KAFKA_SOURCE__SCHEMA_REGISTRY_PASSWORD` | _(unset)_ | Schema Registry basic-auth password |
| `STREAMLING__KAFKA_SOURCE__CONSUMER_GROUP_ID` | _(unset)_ | Consumer group id (coordinates scaling across instances) |
| `STREAMLING__KAFKA_SOURCE__CLIENT_ID` | _(unset)_ | Kafka client id |

#### Hybrid Source

A hybrid source runs one or more **bounded** phases followed by an **unbounded** phase. Bounded phases read a finite dataset (e.g. a ClickHouse table for backfill) and finish; the unbounded phase (e.g. Kafka) then takes over for live streaming.

With `STREAMLING__JOB_MODE=true` (or `job_mode: true` in `config.yaml`), the pipeline terminates after all bounded phases complete instead of transitioning to the unbounded phase. All sources in the pipeline must be hybrid sources when job mode is enabled.

Sample configuration:

```yaml
sources:
  orders:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: orders_archive
    unbounded_source:
      source_type: kafka
      topic: orders_live
      start_at: earliest
    primary_key: id
```

Connection settings for each phase come from the same environment variables as the standalone [ClickHouse Source](#clickhouse-source) (bounded phases) and [Kafka Source](#kafka-source) (unbounded phase).

#### ClickHouse Source

A standalone ClickHouse source reads a table in paginated chunks and terminates when the table is fully consumed — a bounded source suitable for batch pipelines.

```yaml
sources:
  ch_orders:
    type: clickhouse
    table_name: orders
    primary_key: id
```

**Connection settings** — override the embedded defaults with these environment variables:

| Environment variable | Default | Description |
| --- | --- | --- |
| `STREAMLING__CLICKHOUSE_SOURCE__URL` | `http://localhost:8123` | ClickHouse HTTP endpoint |
| `STREAMLING__CLICKHOUSE_SOURCE__USER` | `default` | Username |
| `STREAMLING__CLICKHOUSE_SOURCE__PASSWORD` | _(empty)_ | Password |
| `STREAMLING__CLICKHOUSE_SOURCE__DATABASE` | `default` | Database name |
| `STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE` | `10000000` | Rows fetched per pagination chunk |

#### File Source

Reads files from a local path or object store (`s3://`, `gs://`) into the pipeline. Formats: `csv`, `json` (newline-delimited / NDJSON — aliases `ndjson`, `jsonl`), `parquet`, `avro`. The schema is inferred at startup. Because file reads are append-only, a constant `_gs_op = 'i'` column is synthesized when the data doesn't already carry one.

**Layouts** — all of the following work in both modes below:

- flat directories,
- plain nested subfolders (read recursively),
- Hive-style partition directories (`…/dt=2024-01-01/`), whose `key=value` columns (e.g. `dt`) are inferred and added to the schema.

The `mode` field selects between two modes (default **continuous**):

**Bounded** — lists the matching files once at startup, reads them to EOF, and the pipeline terminates on its own (so it works with `STREAMLING__JOB_MODE=true`).

```yaml
sources:
  events:
    type: file
    path: s3://my-bucket/events/
    format: parquet
    primary_key: id
    mode:
      type: bounded
```

**Continuous** (default) — keeps polling `path` every `poll_interval`, ingesting newly-arrived files; never self-terminates.

```yaml
sources:
  events:
    type: file
    path: /data/events/
    format: csv
    primary_key: id
    mode:
      type: continuous
      poll_interval: 10s   # optional; defaults to 5s
```

**Discovery semantics & caveats (continuous mode).** Discovery is **polling-based** and tracked with a **scalar `last_modified` watermark** plus a small set of paths already ingested at that exact timestamp. Understand these limits before relying on it:

- Each poll lists the path and ingests every file whose object `last_modified` is **greater than** the watermark, plus any file **sharing the watermark's exact timestamp** that hasn't been ingested yet — so new files landing in the same second the watermark sits on are not lost (common with second-granularity object-store mtimes). The watermark then advances to the newest `last_modified` seen, and its boundary set resets when a newer timestamp appears.
- **Files with an older timestamp can be missed.** Any file that becomes visible with a `last_modified` **strictly below** the current watermark is skipped permanently — there is no per-file bookkeeping below the boundary.
- **Updated files are reprocessed.** A rewritten file is re-ingested if its new `last_modified` reaches or exceeds the watermark. Reprocessed rows are emitted as inserts (`_gs_op = 'i'`) at the moment.
- **Deletions are not detected** — removing a file has no effect on already-ingested data.
- **At-least-once across restarts.** The watermark (and its boundary set) is persisted to the state backend only on a checkpoint **finalize** (mirroring the Kafka source). On restart the source reloads the last finalized watermark and re-reads any files from polls that had not finalized, so downstream may see duplicates (deduplicated by `primary_key` + an upsert sink).
- Idle polls emit empty heartbeat batches so checkpoint markers keep propagating even when no new files arrive.

For guaranteed no-miss discovery, ensure new data always lands as immutable, newly-named files whose `last_modified` never goes backwards (atomic writes), or use **bounded** mode for one-shot reads.

**Credentials** for `s3://` / `gs://` come from the standard environment (`AWS_*`, `GOOGLE_APPLICATION_CREDENTIALS`, etc.) — there are no `STREAMLING__` overrides for the file source, and only `s3`/`s3a`/`gs` remote schemes are supported.

### Transforms

All transforms except the SQL one are implemented as custom DataFusion operators (`UserDefinedLogicalNode` +
`ExtensionPlanner` + `ExecutionPlan`). They accept `ExecutionPlan` (which is converted to a `SendableRecordBatchStream`)
as input and construct a new `SendableRecordBatchStream` as an output.

#### SQL Transform

The SQL transform allows executing SQL queries on input streams.

- **DataFusion Integration**:
  - Uses DataFusion's built-in SQL parser and analyzer.
  - Leverages DataFusion's query optimization capabilities.
  - SQL queries are converted to logical plans.

- **Query Support**:
  - Supports standard SQL SELECT statements.
  - Input streams can be referenced by their reference names.
  - Allows filtering, projections, and column transformations. NOTE: stateful operations (e.g. joins and aggregations)
    have been explicitly disabled. They can be enabled when a proper distributed shuffle is supported.

The transformation preserves the streaming nature of the data while allowing SQL-based modifications to the record
contents. Each input batch is processed through the SQL query and produces a corresponding output batch.

Sample configuration:

```yaml
transforms:
  filtered_events:
    type: sql
    sql: SELECT id, event_type, amount, _gs_op FROM raw_events WHERE amount > 0
    primary_key: id
```

#### HTTP Handler Transform

The HTTP Handler transform allows sending records to an external HTTP endpoint for processing. The transformed data is
then returned to the pipeline. It's implemented as a custom DataFusion operator (`ExternalHandlerExec`) with the
following key features:

- **Request Processing**:
  - Can send records individually or in batches.
  - Supports configurable batching with `external_http_handler.trigger_max_count` parameter.
  - Converts Arrow `RecordBatch`es to JSON before sending.
  - Handles retries for transient failures using exponential backoff.
  - Automatic timeout handling for requests.
  - Supports HTTP headers customization.

- **Schema Management**:
  - Allows dynamic schema modification through overrides.
  - Can add new fields, modify existing field types, or remove fields.
  - Preserves nullability constraints from the original schema.

- **Response Handling**:
  - Converts JSON responses back to Arrow `RecordBatch`es.
  - Supports two payload envelope versions (0 and 1) for different formatting needs.
  - Can operate in output-optional mode where responses are ignored (it's used for the Webhook sink).

- **Batching and Performance**:
  - Uses non-blocking I/O.
  - Supports concurrent request execution with configurable buffer size (via `external_http_handler.buffer_size`
    parameter).
  - Integrates with Streamling's checkpointing system.

Sample configuration:

```yaml
transforms:
  enriched_accounts:
    type: handler
    from: raw_accounts
    url: http://localhost:8087/enrich
    one_row_per_request: false
    primary_key: id
```

#### WebAssembly Script Transform

The WebAssembly script transformation allows executing custom JavaScript code using WebAssembly for each record in the
input stream. It's implemented as a custom DataFusion operator (`WasmRunnerExec`) with the following key features:

- **Runtime Environment**: Uses Extism's [js-pdk](https://github.com/extism/js-pdk) to execute JavaScript code in a
  sandboxed WebAssembly environment.
- **Record Processing**:
  - Converts Arrow `RecordBatch`es to JSON strings.
  - Executes the provided JavaScript code for each record.
  - Uses JavaScript's `eval` function within the WebAssembly sandbox.
  - Converts the results back to Arrow `RecordBatch`es.
- **Batching**:
  - Processes multiple records in a batch for better performance.
  - Uses newline character as a separator between records.
  - Maintains checkpointing for reliable processing.
- **Language Support**:
  - Supports any valid **browser** JavaScript and TypeScript (no Node APIs).
  - TypeScript is transpiled to JavaScript in runtime using [swc](https://swc.rs/).

The transformation preserves the schema of the input stream while allowing modifications to the record contents. Each
record is passed to the JavaScript code as a parsed JSON object, and the code must return a new object that will be
converted back to the Arrow format.

Sample configuration:

```yaml
transforms:
  updated_account:
    type: script
    from: raw_accounts
    language: javascript
    script: >
      function process(input) {
        input.tier = input.amount > 1000 ? 'premium' : 'standard';
        input.status = input.status + '_processed';
        return input;
      }
    primary_key: id
```

Note: `script` field accepts any valid **browser** JavaScript or TypeScript snippet inside a `process` function with a
single `input` argument. The input record is passed as `input` argument, and the transformed record must be returned
from the function.

### Sinks

All sinks are implemented as custom DataFusion Table Providers (`TableProvider`) returning a `DataSinkExec`. Sinks
accept `SendableRecordBatchStream` as input. Streamling supports several sink types for outputting processed data.
All sinks support checkpointing to ensure reliable data processing.

#### Print Sink

Prints records to stdout in JSON format. Each record is prefixed with its row kind (INSERT/UPDATE/DELETE). Useful for
debugging and development.

Sample configuration:

```yaml
sinks:
  sink:
    type: print
    from: filtered_events
```

The sampling rate is configurable via `STREAMLING__PRINT_SINK__SAMPLE_EVERY` (default: `1`, prints every record; set to `N` to print only every Nth record).

#### Blackhole Sink

Discards all records without performing any operation. Useful for testing or when you want to measure throughput without
actual output.

Sample configuration:

```yaml
sinks:
  blackhole_events:
    type: blackhole
    from: raw_events
```

#### Webhook (HTTP) Sink

Sends records to an HTTP endpoint as JSON payloads. Supports batching and customization options.

See the `HTTP Handler Transform` section for more info.

Sample configuration:

```yaml
sinks:
  webhook_accounts:
    type: webhook
    from: updated_account
    url: http://localhost:8087/notify
```

#### Postgres Sink

The PostgreSQL sink allows writing data to PostgreSQL tables. It create sink table automically.

When a `primary_key` is set, each batch is collapsed to the latest row per key before writing; set `deduplicate: false`
to disable that (see `Batch Deduplication` below).

Sample configuration:

```yaml
sinks:
  postgres_blocks:
    type: postgres
    primary_key: id
    schema: eth
    table: blocks
```

**Connection settings** — override the embedded defaults with these environment variables:

| Environment variable | Default | Description |
| --- | --- | --- |
| `STREAMLING__POSTGRES_SINK__HOST` | `127.0.0.1` | Database host |
| `STREAMLING__POSTGRES_SINK__PORT` | `5432` | Database port |
| `STREAMLING__POSTGRES_SINK__USER` | `graph-node` | Username for authentication |
| `STREAMLING__POSTGRES_SINK__PASS` | _(empty)_ | Password for authentication |
| `STREAMLING__POSTGRES_SINK__DB` | `postgres` | Database name |
| `STREAMLING__POSTGRES_SINK__SSLMODE` | `disable` | SSL mode (`disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`) |

**Per-sink connections** — two `postgres` sinks in one pipeline can write to two
different databases. Prefix a sink's connection with
`STREAMLING__POSTGRES_SINK_CONNECTIONS__<SINK_NAME>__`, where `<SINK_NAME>` is
the sink's key in `sinks:` with non-alphanumeric characters replaced by `_`; any
field left unset falls back to the table above, and a sink with no entry uses it
verbatim. For the sample above:

```
STREAMLING__POSTGRES_SINK_CONNECTIONS__POSTGRES_BLOCKS__HOST=blocks.example.com
STREAMLING__POSTGRES_SINK_CONNECTIONS__POSTGRES_BLOCKS__DB=blocks
```

Behavior for 256-bit integers (U256/I256):

- Columns annotated as U256/I256 in the Arrow schema (FixedSizeBinary(32) with Streamling metadata) are created in Postgres as `NUMERIC(78,0)`.
- During writes, these columns are stringified in-flight (`u256_to_string` / `i256_to_string`) so the sink sends textual decimal values that Postgres parses into `NUMERIC(78,0)`.
- If an existing destination table has incompatible types (e.g., insufficient precision or non-numeric), the sink errors instead of coercing or dropping data.

#### Postgres Aggregation Sink

The PostgreSQL aggregation sink enables real-time aggregations directly in PostgreSQL using database triggers. Data flows into a landing table, and a trigger function automatically maintains aggregated values in a separate aggregation table.

- **Landing Table**: Stores raw records in append-only mode with a composite primary key (`primary_key` + `_gs_op`).
- **Deduplication**: each batch is collapsed to the latest row per `primary_key` before it reaches the landing table;
  set `deduplicate: false` to disable that (see `Batch Deduplication` below).
- **Aggregation Table**: Stores aggregated results, updated incrementally by the trigger.
- **Operation Handling**: For `sum`, `count`, and `avg`, delete operations negate values. `count` also correctly handles updates (contributes 0 to the count). `min` and `max` do not support deletes or updates (use only for insert-only streams).

Sample configuration:

```yaml
sinks:
  balances:
    type: postgres_aggregate
    from: transfers
    schema: finance
    landing_table: transfer_log
    agg_table: account_balances
    primary_key: transfer_id
    batch_flush_interval: 1s
    batch_size: 100
    group_by:
      account:
        type: text
    aggregate:
      balance:
        from: amount
        fn: sum
```

Column configuration:

- `group_by`: Defines grouping columns (composite key for the aggregation table).
- `aggregate`: Defines aggregation columns. At least one is required. Supported functions: `sum`, `count`, `avg`, `min`, `max`.
- `from`: Source column name (defaults to the key name if omitted).
- `type`: Optional PostgreSQL type override (e.g., `numeric(30,5)`).

When no grouping columns are defined (global aggregation), a sentinel `key` column is added automatically.

For `avg`, the sink stores two columns (`<name>_sum` and `<name>_count`) to support incremental updates.

The sink uses the same PostgreSQL connection parameters as the regular Postgres sink.

**Deduplication and Data Management:**

The landing table uses append-only mode with a composite primary key (`primary_key` + `_gs_op`). This enables:

- **Deduplication within checkpoint window**: Duplicate records (same primary key and operation type) arriving within the same checkpoint epoch are deduplicated via upsert (`ON CONFLICT DO UPDATE`). Records with the same key but different operations (e.g., insert and delete) are stored separately.
- **Checkpoint-based truncation**: The landing table includes a `_gs_checkpoint_epoch` column. When checkpoint epoch N is finalized, all data from epoch N-1 is deleted, keeping the landing table small. Deduplication only works within this window.

**Limitations:**

- **SUM/AVG do not support updates**: These functions cannot correctly handle updates because they would need the old value to compute the delta. Updates are treated as new values, causing incorrect results. Use `sum` and `avg` only with insert/delete streams.
- **COUNT supports all operations**: Inserts add +1, updates add 0 (no change), deletes add -1.
- **MIN/MAX do not support deletes or updates**: These functions cannot retract values without rescanning the entire landing table. Use only with insert-only streams.

##### Examples

**Multiple aggregations with type override:**

```yaml
sinks:
  market_stats:
    type: postgres_aggregate
    from: trades
    schema: trading
    landing_table: trade_log
    agg_table: market_stats
    primary_key: trade_id
    group_by:
      market:
        type: varchar(100)
    aggregate:
      trade_count:
        fn: count
      total_volume:
        from: volume
        fn: sum
        type: numeric(30,5)
      avg_price:
        from: price
        fn: avg
```

**Global aggregation (no GROUP BY):**

```yaml
sinks:
  totals:
    type: postgres_aggregate
    from: events
    schema: analytics
    landing_table: event_log
    agg_table: global_totals
    primary_key: event_id
    aggregate:
      total_count:
        fn: count
```

**Generated Trigger Example:**

The sink automatically creates the trigger function and trigger. Here's an example of the generated SQL:

```sql
CREATE OR REPLACE FUNCTION "finance"."transfer_log_to_account_balances_fn"()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO "finance"."account_balances" ("account", "balance")
  SELECT new_table."account" AS "account",
         SUM(CASE WHEN new_table."_gs_op" = 'd' THEN -new_table."amount"
                  ELSE new_table."amount" END) AS "balance"
  FROM new_table
  GROUP BY new_table."account"
  ON CONFLICT ("account")
  DO UPDATE SET "balance" = "finance"."account_balances"."balance" + EXCLUDED."balance";
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS "transfer_log_to_account_balances_trigger" ON "finance"."transfer_log";
CREATE TRIGGER "transfer_log_to_account_balances_trigger"
AFTER INSERT ON "finance"."transfer_log"
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE FUNCTION "finance"."transfer_log_to_account_balances_fn"();
```

#### Kafka Sink

The Kafka sink allows emitting data to Kafka topics using Avro serialization with Schema Registry integration. It's
implemented as a custom DataFusion Table Provider (`TableProvider`) with the following key features:

- **Schema Management**: Automatically converts Arrow schemas into Avro schemas, registers them with the Schema Registry.
- **Message Processing**:
  - Converts Arrow `RecordBatch`es to Avro-encoded messages.
  - Adds operation type column (`_gs_op`) to track INSERT/UPDATE/DELETE operations (see `Upsert Semantics` section
    below) as a message header with the `dbz.op` key.
- **Deduplication**: when a `primary_key` is set, each batch is collapsed to the latest row per key before it is
  produced; set `deduplicate: false` to disable that (see `Batch Deduplication` below). Message keying is unaffected.

Kafka Sink uses
high-level [FutureProducer](https://docs.rs/rdkafka/latest/rdkafka/producer/future_producer/struct.FutureProducer.html)
to create and write records. The producer is flushed at the end of every batch.

Sample configuration:

```yaml
sinks:
  custom_kafka_sink:
    type: kafka
    topic: custom_topic
    topic_partitions: 10
    from: updated_account
    data_format: avro
```

**Connection settings** — override the embedded defaults with these environment variables:

| Environment variable | Default | Description |
| --- | --- | --- |
| `STREAMLING__KAFKA_SINK__BROKERS` | `localhost:9092` | Comma-separated bootstrap broker list |
| `STREAMLING__KAFKA_SINK__SECURITY_PROTOCOL` | `plaintext` | Kafka security protocol (e.g. `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`) |
| `STREAMLING__KAFKA_SINK__SASL_MECHANISM` | _(unset)_ | SASL mechanism (applied when the protocol contains `sasl`) |
| `STREAMLING__KAFKA_SINK__SASL_USERNAME` | _(unset)_ | SASL username |
| `STREAMLING__KAFKA_SINK__SASL_PASSWORD` | _(unset)_ | SASL password |
| `STREAMLING__KAFKA_SINK__SCHEMA_REGISTRY_URL` | `http://localhost:18081` | Schema Registry URL (required for Avro) |
| `STREAMLING__KAFKA_SINK__SCHEMA_REGISTRY_USERNAME` | _(unset)_ | Schema Registry basic-auth username |
| `STREAMLING__KAFKA_SINK__SCHEMA_REGISTRY_PASSWORD` | _(unset)_ | Schema Registry basic-auth password |
| `STREAMLING__KAFKA_SINK__CLIENT_ID` | _(unset)_ | Kafka client id |

#### ClickHouse Sink

The ClickHouse sink writes records to a ClickHouse table over the HTTP interface. The destination table must already exist; the sink reads its schema and sorting keys at startup rather than creating the table.

- **Upsert Semantics**: INSERT/UPDATE/DELETE operations are derived from the `_gs_op` column (see `Upsert Semantics` section below). With `append_only_mode: true` (the default), the sink targets a `ReplacingMergeTree(insert_time, is_deleted)` table and derives the `is_deleted`/`insert_time` columns automatically. With `append_only_mode: false`, it uses `INSERT` for upserts and `ALTER TABLE ... DELETE` for deletes.
- **Compression**: INSERT request bodies are compressed with zstd by default. gzip and lz4 are also available. The global default comes from `clickhouse_sink.compression`/`clickhouse_sink.compression_level` and can be overridden per sink with the `compression` and `compression_level` fields.
- **Deduplication**: each batch is collapsed to the latest row per `primary_key` before writing. Set `deduplicate: false` for append-only tables whose rows are deltas rather than states (e.g. `SummingMergeTree`) — see `Batch Deduplication` below.

Sample configuration:

```yaml
sinks:
  ch_output:
    type: clickhouse
    from: filtered_events
    table: test_output
    primary_key: id
    compression: gzip # optional, overrides clickhouse_sink.compression
```

**Connection settings** — override the embedded defaults with these environment variables:

| Environment variable | Default | Description |
| --- | --- | --- |
| `STREAMLING__CLICKHOUSE_SINK__URL` | `http://localhost:8123` | ClickHouse HTTP endpoint |
| `STREAMLING__CLICKHOUSE_SINK__USER` | `default` | Username |
| `STREAMLING__CLICKHOUSE_SINK__PASSWORD` | _(empty)_ | Password |
| `STREAMLING__CLICKHOUSE_SINK__DATABASE` | `default` | Database name |
| `STREAMLING__CLICKHOUSE_SINK__COMPRESSION` | `zstd` | Wire compression for INSERTs (`none`, `gzip`, `zstd`, or `lz4`); the per-sink `compression` field overrides it |
| `STREAMLING__CLICKHOUSE_SINK__COMPRESSION_LEVEL` | `6` | gzip compression level (0–9); ignored unless compression resolves to `gzip` |

## Dynamic Tables

Dynamic tables provide a mechanism for maintaining stateful data structures that can be queried during stream processing. They act as persistent, queryable data stores that can be populated from streaming data and used for lookups, deduplication, and other operations within SQL transforms. Most importantly, these tables can be updated externally by simply changing the underlying datastore (e.g. Postgres) without needing to restart the pipeline.

Optionally, dynamic tables can also be populated by Streamling in runtime by providing a SQL query. At the moment, this SQL query only supports projections and filters. Also, it doesn't have a requirement to return `_gs_op` column. SQL queries defined for dynamic tables are executed as side outputs (see the `Side Outputs` section below) and are guaranteed to be in sync with the main data flow.

### Overview

- **Persistent Storage**: Data persists across pipeline restarts and checkpoints
- **Fast Lookups**: Optimized for checking if values exist in the table
- **Multiple Backends**: Support for in-memory and PostgreSQL.
- **SQL Integration**: Seamless integration with SQL transforms through UDF functions

### Configuration

Dynamic tables are configured in the `transforms` section of the pipeline topology, even though they function as both a data store and a sink. The backend storage is configured at the application level.

Example configuration:

```yaml
transforms:
  # Dynamic table without SQL - acts as a simple key-value store
  user_seen_table:
    type: dynamic_table
    backend_type: postgres # Options: in-memory, postgres
    backend_entity_name: user_tracking_table

  # Dynamic table with SQL - automatically populated from a source
  vip_customers_table:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: vip_customers
    sql: |
      SELECT customer_id
      FROM crm.accounts
      WHERE tier = 'vip'
```

### Backend Types

#### In-Memory Backend

- **Use Case**: Development, testing, and low-latency scenarios
- **Persistence**: Data is lost on restart
- **Performance**: Fastest option with lowest latency
- **Configuration**: No additional setup required

```yaml
transforms:
  temp_table:
    type: dynamic_table
    backend_type: InMemory
    backend_entity_name: temp_data
```

#### PostgreSQL Backend

- **Use Case**: Production deployments requiring persistence
- **Persistence**: Data survives restarts and failures
- **Performance**: Good performance with network overhead
- **Configuration**: Requires PostgreSQL connection settings

```yaml
transforms:
  persistent_table:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: persistent_data
```

Enable the in-memory cache in the application config (or with
`STREAMLING__DYNAMIC_TABLE_BACKEND__POSTGRES__CACHE_ENABLED=true`):

```yaml
dynamic_table_backend:
  postgres:
    cache_enabled: true
```

Then set `time_column` on each backing table whose timestamp advances on every append:

```yaml
transforms:
  persistent_table:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: persistent_data
    time_column: updated_at
    # Optional: how long to trust the cache before re-checking table freshness
    # (`SELECT MAX(time_column)`). When omitted (and unset globally), a 5000ms
    # default applies; 0 re-checks on every batch — one database round trip per
    # batch. Raising it trades staleness for round trips: rows written by OTHER
    # writers may be missed for up to this long, while this pipeline's own
    # writes stay visible immediately.
    cache_refresh_debounce_ms: 5000
```

The cache is off by default and is used only when both settings are present. The initial lookup
loads the full table through bounded PostgreSQL cursor pages. Each later `dynamic_table_check`
batch reads `MAX(time_column)` and appends only rows newer than the cached maximum. Index the time
column so these checks and range reads stay cheap.

PostgreSQL dynamic tables are append-only when the in-memory cache is enabled. Updating or deleting
existing membership, including removals, is not supported in this mode. Keeping `time_column` current
is a user requirement and part of the cache's correctness contract. Every process that appends table
membership must use the same serialized-writer protocol:

```sql
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('public.persistent_data', 0));
INSERT INTO public.persistent_data (value, updated_at)
VALUES ('new-value', clock_timestamp())
ON CONFLICT (value) DO NOTHING;
COMMIT;
```

Use the schema-qualified table name as the lock key, exactly as shown. Transaction-scoped advisory
locks coordinate only writers that follow this protocol. Each successful append must atomically make
`MAX(time_column)` different by assigning a non-null timestamp strictly newer than the previous
maximum. `NOW()` and `CURRENT_TIMESTAMP` are unsafe here because PostgreSQL fixes them at transaction
start, which can precede waiting for the writer lock; use `clock_timestamp()` after the lock instead.

Streamling takes this lock automatically for its own cached appends and creates new backing tables
with `updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()`. It does not alter existing tables.
Tables created by older Streamling versions with nullable `TIMESTAMPTZ DEFAULT NOW()` need no schema
or data migration: cached Streamling appends explicitly write `clock_timestamp()` after taking the
lock. Future appends from other writers must still follow the protocol above. Cache initialization
checks that the configured column exists and supports `MAX`, but it cannot verify that other writers
obey the protocol. If that cannot be guaranteed, leave caching disabled; uncached tables continue to
use batched PostgreSQL lookups.

### Usage with SQL Transforms

Dynamic tables are accessed within SQL transforms using the `dynamic_table_check` UDF (User Defined Function). This function checks if a value exists in the specified dynamic table.

#### Example: Deduplication

```yaml
transforms:
  # First, define a dynamic table
  seen_users:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: user_dedup
    sql: |
      SELECT user_id
      FROM user.events

  # Then use it in a SQL transform
  filtered_events:
    type: sql
    sql: |
      SELECT *
      FROM user.events
      WHERE NOT dynamic_table_check('seen_users', user_id)
    primary_key: event_id
```

#### Example: Lookup filter

```yaml
transforms:
  vip_customers:
    type: dynamic_table
    backend_type: Postgres
    backend_entity_name: vip_customers
    sql: |
      SELECT customer_id
      FROM crm.accounts
      WHERE tier = 'vip'
  vip_orders:
    type: sql
    sql: |
      SELECT id, customer_id, amount, created_at
      FROM orders.events
      WHERE dynamic_table_check('vip_customers', customer_id)
    primary_key: id
```

#### UDF Function Signature

The `dynamic_table_check` function has the following signature:

- **Parameters**:
  - `table_name` (string): Name of the dynamic table to query
  - `value` (string **or** `text[]`): Value(s) to check for existence
- **Returns**: `boolean` - `true` if the value exists in the table, `false` otherwise

`value` accepts two forms:

- **Scalar `text`** — returns `true` iff the single value is present in the table.
- **Array `text[]`** (`List` or `LargeList` of `text`) — **any-match**: returns `true` iff **any** element of the array is present in the table. Null handling is explicit: a **null array yields null**, an **empty array yields `false`**, and **null elements are skipped**. Distinct values are deduplicated per batch before the lookup. Nested arrays (`text[][]`) are not supported — dynamic tables are a flat set of strings.

```sql
-- any element of the array present in the table?
SELECT * FROM src WHERE dynamic_table_check('tracked_wallets', accounts)
```

### Source Validation

Dynamic tables include built-in validation to ensure data consistency. When a dynamic table with SQL is used in a SQL transform,
**both must reference the same input**. This prevents issues where the dynamic table and the transform are out of sync.
This input can be a source or another transform.

### Performance Considerations

- **Batch Operations**: The UDF processes batches of values
- **Backend Choice**: Choose backends based on your latency and persistence requirements
- **Indexing**: PostgreSQL backend automatically creates an index for optimal lookup performance

## Side Outputs

Side outputs allow emitting data to a "side" processor without modifying the main data flow. They're not part of the topology definition
(not visible in the LogicalPlan). The implementation can slightly differ between operators, but the general idea is that side outputs are executed
right before the data is emitted (by receiving a `RecordBatch`).

Currently, the only use case for side outputs is populating dynamic tables (when a SQL query is defined).

## Checkpointing

Streamling supports lightweight, local checkpointing. Local checkpointing
means that each instance of Streamling performs checkpointing independently, so no coordination is needed. Checkpoint
markers with monotonically increasing epochs are emitted by the checkpoint coordinator, then they're passed between the
operators (sources to transforms to sinks) and finally delivered to the checkpoint coordinator. Once the coordinator
receives the expected number of marker acknowledgements, it advances the epoch and sends the finalization message.

Here's the end-to-end flow with expectations from each operator:

```
Marker phase:
1. Coordinator broadcasts Marker → Sources receive them
2. Sources record offsets (DON'T commit yet), propagate Marker downstream in batches
3. Sinks receive batches with Marker, FLUSH/COMMIT their data, then send Ack
4. Coordinator receives all Acks → knows all sinks have flushed

Finalizer phase:
5. Coordinator sends Finalizer → Sources receive them
6. Sources COMMIT their state (e.g., Kafka offsets)
7. Sources propagate Finalizer downstream
8. Sinks ignore Finalizer
```

This guarantees at-least-once because sources only commit after all sinks have confirmed they flushed.

For example, the Kafka source uses this message to commit previously persisted offsets. Here are the logs
for the `Kafka -> SQL Transform -> Print` topology:

```
2025-12-03T18:10:20.399911Z  INFO tokio-runtime-worker ThreadId(03) streamling_core::checkpoints::checkpoint_management: Sending checkpoint with epoch: 1
2025-12-03T18:10:20.400262Z DEBUG tokio-runtime-worker ThreadId(07) streamling_connectors::table_providers::kafka: Received epoch marker: CheckpointEpoch(1)
2025-12-03T18:10:20.400465Z DEBUG tokio-runtime-worker ThreadId(07) streamling_connectors::table_providers::kafka: Current position: TPL {app.events.account/0: offset=Offset(87) metadata="", error=Ok(()); app.events.account/1: offset=Offset(19) metadata="", error=Ok(()); app.events.account/2: offset=Offset(241) metadata="", error=Ok(()); app.events.account/3: offset=Offset(2) metadata="", error=Ok(())}
2025-12-03T18:10:20.508865Z DEBUG tokio-runtime-worker ThreadId(11) streamling_core::checkpoints::checkpoint_management: [CheckpointCoordinator] Received checkpoint ACK for epoch: 1 from sink
2025-12-03T18:10:20.508998Z  INFO tokio-runtime-worker ThreadId(11) streamling_core::checkpoints::checkpoint_management: Epoch finalized: 1
2025-12-03T18:10:20.613465Z DEBUG tokio-runtime-worker ThreadId(11) streamling_connectors::table_providers::kafka: Received epoch finalizer: CheckpointEpoch(1)
2025-12-03T18:10:20.613503Z DEBUG tokio-runtime-worker ThreadId(11) streamling_connectors::table_providers::kafka: Committing position: TPL {app.events.account/0: offset=Offset(87) metadata="", error=Ok(()); app.events.account/1: offset=Offset(19) metadata="", error=Ok(()); app.events.account/2: offset=Offset(241) metadata="", error=Ok(()); app.events.account/3: offset=Offset(2) metadata="", error=Ok(())}
2025-12-03T18:10:20.620278Z DEBUG tokio-runtime-worker ThreadId(11) streamling_connectors::table_providers::kafka: Saving position to state backend: TopicPartition { topic: "app.events.account", partition: 0, offset: TopicPartitionOffset { offset: 87, updated_at: 1764785420620 } }
2025-12-03T18:10:20.620308Z DEBUG tokio-runtime-worker ThreadId(11) streamling_connectors::table_providers::kafka: Saving position to state backend: TopicPartition { topic: "app.events.account", partition: 1, offset: TopicPartitionOffset { offset: 19, updated_at: 1764785420620 } }
2025-12-03T18:10:20.620317Z DEBUG tokio-runtime-worker ThreadId(11) streamling_connectors::table_providers::kafka: Saving position to state backend: TopicPartition { topic: "app.events.account", partition: 2, offset: TopicPartitionOffset { offset: 241, updated_at: 1764785420620 } }
2025-12-03T18:10:20.620326Z DEBUG tokio-runtime-worker ThreadId(11) streamling_connectors::table_providers::kafka: Saving position to state backend: TopicPartition { topic: "app.events.account", partition: 3, offset: TopicPartitionOffset { offset: 2, updated_at: 1764785420620 } }
2025-12-03T18:10:21.466247Z  INFO tokio-runtime-worker ThreadId(03) streamling_core::checkpoints::checkpoint_management: Sending checkpoint with epoch: 2
2025-12-03T18:10:21.573524Z DEBUG tokio-runtime-worker ThreadId(05) streamling_connectors::table_providers::kafka: Received epoch marker: CheckpointEpoch(2)
2025-12-03T18:10:21.573603Z DEBUG tokio-runtime-worker ThreadId(05) streamling_connectors::table_providers::kafka: Current position: TPL {app.events.account/0: offset=Offset(87) metadata="", error=Ok(()); app.events.account/1: offset=Offset(19) metadata="", error=Ok(()); app.events.account/2: offset=Offset(241) metadata="", error=Ok(()); app.events.account/3: offset=Offset(2) metadata="", error=Ok(())}
2025-12-03T18:10:21.681840Z DEBUG tokio-runtime-worker ThreadId(03) streamling_core::checkpoints::checkpoint_management: [CheckpointCoordinator] Received checkpoint ACK for epoch: 2 from sink
2025-12-03T18:10:21.681873Z  INFO tokio-runtime-worker ThreadId(03) streamling_core::checkpoints::checkpoint_management: Epoch finalized: 2
2025-12-03T18:10:21.787167Z DEBUG tokio-runtime-worker ThreadId(03) streamling_connectors::table_providers::kafka: Received epoch finalizer: CheckpointEpoch(2)
2025-12-03T18:10:21.787226Z DEBUG tokio-runtime-worker ThreadId(03) streamling_connectors::table_providers::kafka: Current position already committed, so skipping commit
```

You can see that once the epoch 1 is created, the Kafka source immediately receives the epoch marker and saves the
current offsets. Then it propagates the marker to the SQL transform, which propagates it to the Print sink.
Finally, the print sink sends the checkpoint ack back to the coordinator. Once the coordinator receives the expected
number of acks (just 1 in this case, since there is only one sink), it finalizes the epoch and sends the finalizer
message to the Kafka source. The source then commits the offsets.

## State Backends

Streamling supports multiple state backend implementations for persisting operator state.

**NOTE**: `application_id` (`STREAMLING__APPLICATION_ID`) is used as a namespace for the state backend. It must be unique
across all applications using the same state backend. By default, it is set to the `app_name` from infra config.

### In-Memory State Backend

- Simple HashMap-based implementation
- Suited for testing and local development
- No persistence between restarts
- Lowest latency but no durability guarantees

### SQLite State Backend

- Uses SQLite for state storage
- Good for single-node deployments or local development
- Persists data to a local file
- Schema:
  ```sql
  CREATE TABLE state (
    namespace TEXT,
    key TEXT,
    data TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(namespace, key)
  );
  ```
- Sqlite database file is created automatically on startup if it doesn't exist.
- The `created_at` field is automatically set to the current timestamp when new state records are created and preserved on updates.

### PostgreSQL State Backend

- Uses PostgreSQL for state storage
- Recommended for production deployments
- Supports high availability
- Schema:
  ```sql
  CREATE TABLE streamling.state (
    namespace TEXT,
    key TEXT,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY(namespace, key)
  );
  ```
- Postgres schema and table are created automatically on startup if they don't exist.
- The `created_at` field is automatically set to the current timestamp when new state records are created and preserved on updates.

You can choose a state backend by setting the `STREAMLING__STATE_BACKEND__BACKEND_TYPE` environment variable to one of the following values:

- `InMemory` (default, a no-op)
- `Postgres`
- `Sqlite`

### Usage with Kafka Source Checkpoints

The state backend plays a crucial role in Kafka source reliability:

- **Offset Management**:
  - Stores Kafka partition offsets using the format `{reference_name}:{topic}:{partition}` as keys
  - Each offset entry contains the offset position and last update timestamp
  - Used to resume processing from the last committed position after restarts
  - **NOTE**: offsets in the state always have preference over the Kafka consumer group ones

- **Checkpoint Process**:
  - When a checkpoint epoch is finalized, Kafka source commits offsets to both Kafka and state backend
  - State backend persists the offsets in a durable way
  - On startup, Kafka source:
    - Queries state backend for stored offsets
    - Seeks to stored positions if found
    - Starts from configured position (earliest/latest) for new partitions

## Error Handling

Streamling uses `StreamlingError`, a custom error type that wraps `anyhow::Error` and integrates with DataFusion while providing rich error context and semantic flags.

### Why Context Matters

Errors are messages meant for humans (debugging) or machines (automated recovery). Without context, errors become useless for both. Consider a common antipattern where we catch errors and throw them up the stack as fast as possible:

```rust
// Bad: error forwarding strips away meaning
fn process_batch(&self) -> Result<RecordBatch> {
    let data = self.fetch_data()?;      // "connection refused"
    let parsed = parse_avro(data)?;      // just forwards the error
    Ok(transform(parsed)?)
}
```

When this fails in production, you get `"connection refused"`. But connection to _what_? During _which operation_? In _which pipeline_? The error shows where it originated but not the logical path through the application—the path you actually need to debug the issue.

Backtraces don't solve this either. In async Rust, they're dominated by noise (dozens of `GenFuture::poll()` frames) and still only show _origin_, not _intent_.

**Context captures intent.** Each `.context()` call answers: "What was I trying to do when this failed?"

```rust
// Good: context captures the logical path
fn process_batch(&self) -> Result<RecordBatch> {
    let data = self.fetch_data()
        .streamling_context("failed to fetch data from schema registry")?;
    let parsed = parse_avro(data)
        .streamling_context("failed to parse Avro payload")?;
    transform(parsed)
        .streamling_context("failed to transform batch")
}
```

Now the error reads: `"failed to transform batch: failed to parse Avro payload: invalid schema version"`. You know exactly what the code was trying to do at each level.

For more on this philosophy, see [Stop Forwarding Errors, Start Designing Them](https://fast.github.io/blog/stop-forwarding-errors-start-designing-them/).

### StreamlingError

`StreamlingError` is the standard error type with two semantic flags:

- **`internal`**: Internal errors are not likely to be caused by user input and may contain implementation details. User-facing errors (e.g., invalid configuration) should have this flag unset.
- **`retriable`**: Indicates the operation can be retried (e.g., transient network errors like connection refused or timeout).

These flags enable automated error handling (retry logic) and safe error display to end users.

### Creating Errors

#### Macros (preferred for simple cases)

```rust
use streamling_core::{streamling_err, streamling_bail, streamling_user_err, streamling_user_bail, streamling_retriable_err};

// Internal, non-retriable error
let err = streamling_err!("failed to process item {}", item_id);

// Early return with internal, non-retriable error
if value < 0 {
    streamling_bail!("value must be non-negative, got {}", value);
}

// User-facing, non-retriable error (safe to show to users)
let err = streamling_user_err!("invalid configuration: {}", reason);

// Early return with user-facing error
if input.is_empty() {
    streamling_user_bail!("input cannot be empty");
}

// Internal, retriable error (for transient failures)
let err = streamling_retriable_err!("connection to {} timed out", host);
```

#### Constructors (for more control)

```rust
use streamling_core::error::StreamlingError;

// Basic constructors
let err = StreamlingError::new("internal error");           // internal, non-retriable
let err = StreamlingError::user("user-facing error");       // user-facing, non-retriable
let err = StreamlingError::retriable("transient failure");  // internal, retriable

// With cause (wraps another error)
let err = StreamlingError::with_cause("write failed", io_error);           // internal, non-retriable
let err = StreamlingError::user_with_cause("config error", parse_error);   // user-facing, non-retriable
let err = StreamlingError::retriable_with_cause("connection failed", e);   // internal, retriable
```

#### Chainable Modifiers

Errors can be modified after creation:

```rust
let err = StreamlingError::new("error")
    .mark_retriable()      // mark as retriable
    .mark_user_facing()    // mark as user-facing (not internal)
    .context("additional context");  // add context, preserves flags
```

### Adding Context

Use the `ResultExt` trait to add context to any `Result`:

```rust
use streamling_core::error::ResultExt;

fn process() -> Result<()> {
    // Eager context (always evaluated)
    some_operation()
        .streamling_context("failed to perform operation")?;

    // Lazy context (only evaluated on error)
    expensive_operation()
        .streamling_with_context(|| format!("failed for item {}", compute_id()))?;

    Ok(())
}
```

When applied to `Result<T, StreamlingError>`, the `internal` and `retriable` flags are preserved. For other error types, creates an internal, non-retriable error.

### Automatic Conversions

`StreamlingError` provides `From` implementations for common error types:

| Source Type                | `internal` | `retriable`             |
| -------------------------- | ---------- | ----------------------- |
| `anyhow::Error`            | `true`     | `false`                 |
| `arrow_schema::ArrowError` | `true`     | `false`                 |
| `DataFusionError`          | `true`     | `false`                 |
| `std::io::Error`           | `true`     | Depends on error kind\* |

\*I/O errors are marked retriable for: `ConnectionRefused`, `ConnectionReset`, `ConnectionAborted`, `NotConnected`, `TimedOut`, `Interrupted`.

`StreamlingError` also converts to `DataFusionError` automatically, enabling seamless use with the `?` operator at trait boundaries.

### Checking Error Properties

```rust
if err.is_retriable() {
    // Implement retry logic
}

if !err.is_internal() {
    // Safe to display to user
    show_to_user(&err.to_string());
}
```

### DataFusion Integration

DataFusion traits require `datafusion::error::Result<T>`. Use `StreamlingError` or the `IntoStreamlingResult` trait:

```rust
use streamling_core::error::{Result, ResultExt, IntoStreamlingResult};

impl TableProvider for MyProvider {
    async fn scan(&self, ...) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // StreamlingError converts to DataFusionError automatically
        let schema = fetch_schema(&self.url)
            .streamling_context("failed to fetch schema")?;

        // Or convert anyhow::Result explicitly
        let data = some_anyhow_function()
            .into_streamling()?;

        Ok(Arc::new(MyExec::new(schema)))
    }
}
```

### Error Output Format

When errors are displayed, the full chain is preserved:

```
Error: kafka source 'raw_transactions': failed to create Kafka source

Caused by:
    Execution error: Error: failed to fetch Avro schema from Schema Registry for topic raw.event.transaction

    Caused by:
        Error: Could not get id from response had no other cause, it's retriable: false, it's cached: false
```

This replaces the previous nested `"Execution error: Execution error:"` pattern with clear, hierarchical error messages.

## Upsert Semantics

By default, DataFusion passes data in a columnar format using `RecordBatch` without any additional metadata.
So, in order to support upsert semantics, an additional `_gs_op` column is added to the schema. Unfortunately, this
comes with some challenges mentioned in https://github.com/goldsky-io/streamling/pull/2.

### Batch Deduplication

Every sink configured with a `primary_key` — Postgres, Kafka and ClickHouse — collapses each
batch to the **latest row per key** before writing it. Only the newest state of a row matters to an upsert, so this
saves redundant writes. Sinks with no primary key never deduplicate.

That assumption breaks when rows are **deltas rather than states**. Delta rows are meant to accumulate, so several
of them legitimately share a key and every one has to be written — a retraction and its addition, or two different
entities rolling up to the same aggregate key. Collapsing them silently drops rows and the destination drifts. Set
`deduplicate: false` on those sinks:

```yaml
sinks:
  revenue_rollup:
    type: clickhouse
    from: revenue_deltas
    table: product_revenue_daily
    primary_key: revenue_date,product_id,currency
    append_only_mode: false
    deduplicate: false
```

The primary key still drives everything else it is used for — `ALTER TABLE ... DELETE` keying and `CREATE TABLE`
ordering in ClickHouse, `ON CONFLICT` targeting in Postgres, message keying in Kafka. `deduplicate: false` turns off
the batch collapse and nothing else.

## Plugin System

### Overview

Streamling's plugin system provides a flexible way to extend the platform with custom functionality. The plugin system is built using FFI (Foreign Function Interface) with the [abi_stable](https://docs.rs/abi_stable) crate, ensuring that plugins built against one version of Streamling continue to work with future versions. While theoretically any language that can interface with Rust can be used, the primary focus is on Rust-based plugins.

The plugin architecture consists of these key components:

1. **Host Environment**: The Streamling core that loads and manages plugins
2. **Plugin Interface**: A stable ABI for communication between host and plugins
3. **Message Channels**: Bidirectional communication channels for data exchange (input, output, and metrics)

Plugins can implement any of the following types:

- **Source plugins**: Generate data to be processed by the pipeline
- **Transform plugins**: Modify data flowing through the pipeline
- **Sink plugins**: Consume data from the pipeline and write it to external systems
- **Preprocessor plugins**: Transform topology configuration before parsing
- **UDF plugins**: Custom scalar functions usable in SQL transforms
- **Side output plugins**: Observe data from sources without modifying the pipeline

### Plugin Pipeline Configuration

Plugins location is configured via the application config (`STREAMLING__PLUGIN__PATH` env var). It can either point to
an individual plugin file or a directory containing multiple plugin files. These plugins will be loaded and initialized
on application startup.

Plugins are configured in pipeline definitions through JSON or YAML. NOTE: plugin ids (explained below) are used as a
type. This makes plugins first-class citizens in the topology definition.

Here's an example showing a complete plugin-based pipeline with source, transform and sink:

```yaml
sources:
  plugin_data:
    type: basic_plugin.random_source
    options:
      max_rows: "50"
transforms:
  updated_plugin_data:
    from: plugin_data
    type: basic_plugin.filter_transform
    options:
      _gs_op: i
sinks:
  print.updated_plugin_data:
    from: updated_plugin_data
    type: print_sink
    options:
      mode: batch
```

Each plugin configuration requires:

- `type`: Unique plugin id, consisting of the plugin namespace (optional) and an operator name, separated by a dot (e.g., `basic_plugin.random_source`)
- `options`: Key-value pairs for plugin-specific configuration

### Including Plugins

A plugin is a shared library (`.so` on Linux, `.dylib` on macOS, `.dll` on Windows) built against the Streamling plugin ABI. To make a plugin usable in a pipeline, point the runtime at it before startup via `STREAMLING__PLUGIN__PATH`. The path may be a single library file or a directory — every file in a directory is loaded:

```bash
# a single plugin library
export STREAMLING__PLUGIN__PATH=/opt/streamling-plugins/libcommunity_plugins.so

# or a directory holding several plugins (all files are loaded)
export STREAMLING__PLUGIN__PATH=/opt/streamling-plugins
```

Loaded plugins are then referenced by id as the `type` of any source, transform, or sink (see [Plugin Pipeline Configuration](#plugin-pipeline-configuration)). Plugin ids may be bare (`s3_sink`) or namespaced (`basic_plugin.random_source`).

#### Community plugins

The [streamling-community-plugins](https://github.com/goldsky-io/streamling-community-plugins) repository is a single workspace crate that compiles several sinks — `s3_sink`, `mysql_sink`, `sqs`, and `s2_sink` — into one shared library (`libcommunity_plugins.so` / `.dylib` / `community_plugins.dll`). Each release publishes prebuilt binaries for common platforms, so prefer those over building from source.

1. Get the prebuilt library for your platform from the [releases page](https://github.com/goldsky-io/streamling-community-plugins/releases) and extract it:
   ```bash
   # latest release — swap linux-x86_64 for darwin-aarch64, darwin-x86_64,
   # linux-aarch64, or windows-x86_64 (.zip) as needed
   curl -L -o community-plugins.tar.gz \
     https://github.com/goldsky-io/streamling-community-plugins/releases/latest/download/community-plugins-linux-x86_64.tar.gz
   tar xzf community-plugins.tar.gz   # → libcommunity_plugins.so
   ```
   No prebuilt binary for your platform? Build from source: `git clone https://github.com/goldsky-io/streamling-community-plugins && cd streamling-community-plugins && just build-release` → `target/release/libcommunity_plugins.so`.
2. Point the runtime at the library:
   ```bash
   export STREAMLING__PLUGIN__PATH=./libcommunity_plugins.so
   ```
3. Reference a plugin by its id in your pipeline. For example, write to S3 as Parquet:
   ```yaml
   sinks:
     to_s3:
       type: s3_sink
       from: my_source
       options:
         bucket: my-bucket
         region: us-east-1
   ```
   AWS credentials are required and best passed via the `STREAMLING__PLUGIN__S3_SINK__ACCESS_KEY_ID` / `...SECRET_ACCESS_KEY` environment variables (they take precedence over YAML). See the community-plugins README for the full option tables of each sink.

To write and distribute your own plugin, see [Building Plugins](#building-plugins).

### Message-based Interface

Communication between Streamling and plugins happens through a message-based interface built on crossbeam channels. Each plugin receives three channels:

1. **Input Channel**: Receives messages from the host
2. **Output Channel**: Sends messages back to the host
3. **Metrics Channel**: Sends metrics from the plugin to the host

The message protocol is defined by the `PluginMsg` enum:

```rust
pub enum PluginMsg {
    Init,
    NextBatch { data: SafeArrowArray },
    CheckpointMarker { epoch: PluginCheckpointEpoch },
    CheckpointAck { epoch: PluginCheckpointEpoch },
    CheckpointFinalizer { epoch: PluginCheckpointEpoch },
    Terminate,
    Topology { config: RString },
    Error { message: RString },
}
```

Key messages include:

- `Init`: Sent to initialize the plugin
- `NextBatch`: Contains an Arrow RecordBatch for processing
- `CheckpointMarker`/`CheckpointAck`/`CheckpointFinalizer`: Support Streamling's checkpointing system
- `Terminate`: Graceful shutdown signal
- `Topology`: Sends topology configuration to preprocessor plugins
- `Error`: Error response from a plugin

This design allows plugins to process data incrementally and supports the full range of Streamling's reliability features.

### FFI

The core plugin interface is defined by the `PluginModule` struct:

```rust
pub struct PluginModule {
  /// Function to initialize the plugin, called only once per plugin module.
  pub init: extern "C" fn(
    logging: PluginLogging,
  ) -> RResult<PluginRuntimeConfiguration, PluginInitializationError>,

  /// Function to create a plugin instance, e.g. a source, transform, or sink.
  pub create: extern "C" fn(
    plugin_id: RString,
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
  ) -> RResult<PluginResult, PluginInitializationError>,

  /// Returns UDF descriptors provided by this plugin. Can return an empty vector.
  pub udf_descriptors:
    extern "C" fn() -> RResult<RVec<PluginUdfDescriptor>, PluginInitializationError>,

  /// Returns side output descriptors provided by this plugin. Can return an empty vector.
  pub side_output_descriptors:
    extern "C" fn() -> RResult<RVec<PluginSideOutputDescriptor>, PluginInitializationError>,
}
```

Plugins must implement these functions:

1. `init`: Called once when the plugin is loaded. It returns a `PluginRuntimeConfiguration`, which contains available plugin ids and optional per-plugin channel capacity hints.
2. `create`: Called to create a new plugin instance with a specific plugin id.
3. `udf_descriptors`: Returns descriptors for any UDFs the plugin provides.
4. `side_output_descriptors`: Returns descriptors for any side outputs the plugin provides.

### Async Runtime Support

Tokio's runtime can't be passed over FFI, so it can't be shared with the plugins directly. Plugins may choose to either:

- Use the provided `PluginAsyncRuntime` (described below) trait, as well as the corresponding `PluginAsyncRuntimeObj` trait object,
  to perform asynchronous operations. This runtime is created on the host side and shared by all plugins. It allows to use
  resources efficiently, but it also limits async operations to the provided interface.
- Initialize and use their own Tokio async runtime.

The `PluginAsyncRuntime` trait uses [async-ffi](https://crates.io/crates/async-ffi) looks like this:

```rust
pub trait PluginAsyncRuntime: Send + Sync + Clone {
    fn spawn(&self, fut: FfiFuture<()>);
    fn sleep(&self, dur: RDuration) -> FfiFuture<()>;
    fn timeout(&self, dur: RDuration, fut: FfiFuture<()>) -> FfiFuture<RResult<(), ()>>;
    fn block_on(&self, fut: FfiFuture<()>);
    fn yield_now(&self) -> FfiFuture<()>;
}
```

It's also possible to use plugin-specific Tokio runtime via the `PluginAsyncRuntime` trait. The high-level API described
below provides `init_plugin_with_async_runtime!()` macro just for that.

### Data Handling with Arrow

Data flows between Streamling and plugins as Arrow RecordBatches, providing efficient columnar data processing.
The `SafeArrowArray` type enables safe passage of Arrow data across the FFI boundary:

```rust
pub struct SafeArrowArray {
    pub array: FFI_ArrowArray,
    pub schema: SafeArrowSchema,
}
```

Conversion functions handle the transformation between Arrow RecordBatches and the FFI-safe representation.

### State Backend Support

`PluginStateBackendConfig` containing the application namespace and the serialized state backend configuration is passed
to the plugins. From there, `PluginStateBackendFactory` can be used to create a state backend factory for plugins. After
that, the factory can be used just like a regular state backend factory (refer to the `State Backends` section above).

NOTE: state backends typically require a Tokio runtime to be initialized, so plugins should enable async support before
using them.

### Plugin Metrics

Plugins receive a `PluginMetricsRecorder` that allows emitting metrics back to the host via a dedicated metrics channel. The host processes these metrics and integrates them with the Streamling telemetry system.

Available methods:

- `record_count(name, value)` / `record_count_w_tags(name, value, tags)`: Record counter metrics.
- `record_latency(name, duration)` / `record_latency_w_tags(name, duration, tags)`: Record latency/histogram metrics.
- `record_gauge(name, value)` / `record_gauge_w_tags(name, value, tags)`: Record gauge metrics.

Tags are provided as `Vec<(&str, &str)>` key-value pairs.

### High-Level API

While plugins can work directly with the message-based interface, Streamling provides convenient higher-level traits that simplify plugin development.

#### Core Operator Traits

```rust
#[async_trait]
pub trait SourcePlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    fn output_schema(&self) -> Result<SchemaRef, PluginError>;
    /// Return an empty batch to indicate missing data if needed.
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError>;
    /// Returning a successful result indicates that the checkpoint marker was processed
    /// successfully, and the mark should be propagated downstream.
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
}

#[async_trait]
pub trait TransformPlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    fn output_schema(&self) -> Result<SchemaRef, PluginError>;
    /// Return an empty batch to indicate missing data if needed.
    async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError>;
    /// Returning a successful result indicates that the checkpoint marker was processed
    /// successfully, and the mark should be propagated downstream.
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
}

#[async_trait]
pub trait SinkPlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    async fn process_batch(&self, data: RecordBatch) -> Result<(), PluginError>;
    /// Returning a successful result indicates that the checkpoint marker was processed
    /// successfully, and an acknowledgment should be sent back to the source.
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
}
```

`SupportsGracefulShutdown` is a small trait that allows plugins to handle graceful shutdown by implementing these methods:

```rust
#[async_trait]
pub trait SupportsGracefulShutdown {
    /// Returns true if the plugin is still running.
    fn is_running(&self) -> bool;
    /// Attempts to gracefully shut down the plugin.
    async fn terminate(&self) -> Result<(), PluginError>;
}
```

Typical implementation would rely on an `AtomicBool` to track the running state.

#### Preprocessor Trait

Preprocessors transform the raw topology configuration string before it is parsed. This enables dynamic configuration generation.

```rust
#[async_trait]
pub trait PreprocessorPlugin: Send + Sync {
    async fn preprocess_topology(&self, config: String) -> Result<String, PluginError>;
}
```

Preprocessors are configured via the application config:

```yaml
plugin:
  preprocessor_ids:
    - my_preprocessor
  preprocessor_options:
    my_preprocessor:
      key: value
```

Preprocessors run in the configured order. An id that is not registered by the
loaded plugin bundle is skipped with a warning. This allows shared configuration
to add preprocessors without breaking older images that do not include them.
Under `--validate`, the warning appears in the JSON `warnings` array.

A pipeline that depends on a skipped preprocessor may still fail later when its
topology is validated. Missing-plugin errors list the ids available in the loaded
bundle to make version mismatches easier to diagnose.

#### Side Output Trait

Side outputs observe data from sources without modifying the pipeline. Unlike other plugin types, side outputs use direct FFI invocation (no channels). One instance is created per source.

```rust
pub trait SideOutputPlugin: Send + Sync {
    fn process_batch(&self, batch: &RecordBatch) -> Result<(), String>;
    fn shutdown(&self);
}
```

Side outputs are configured via the application config:

```yaml
plugin:
  side_output_ids:
    - my_side_output
  side_output_options:
    my_side_output:
      key: value
```

#### UDF Plugins

UDF (User Defined Function) plugins provide custom scalar functions that can be used in SQL transforms. They implement DataFusion's `ScalarUDFImpl` trait and are registered globally when the plugin is loaded.

#### Dispatchers

Dispatcher types automatically handle the message loop and channel communication, allowing plugin developers to focus on business logic:

- `SourcePluginDispatcher`: Manages message flow for source plugins
- `TransformPluginDispatcher`: Manages message flow for transform plugins
- `SinkPluginDispatcher`: Manages message flow for sink plugins
- `PreprocessorPluginDispatcher`: Manages message flow for preprocessor plugins

#### Registration Macros

Helper macros make it easy to implement the plugin interface:

- `register_plugin_source!("<plugin_id>", <SourcePlugin>);` to register a source plugin.
- `register_plugin_transform!("<plugin_id>", <TransformPlugin>);` to register a transform plugin.
- `register_plugin_sink!("<plugin_id>", <SinkPlugin>);` to register a sink plugin.
- `register_plugin_preprocessor!("<plugin_id>", <PreprocessorPlugin>);` to register a preprocessor plugin.
- `register_plugin_udf!(<ScalarUDFImpl>);` to register a UDF from a type implementing `ScalarUDFImpl` + `Default`.
- `register_plugin_udf_fn!(<factory_fn>);` to register a UDF from a factory function returning `ScalarUDF`.
- `register_plugin_side_output!("<id>", <SideOutputPlugin>);` to register a side output plugin.
- `init_plugin!();` or `init_plugin_with_async_runtime!();` to finalize the plugin definition. The latter creates a plugin-specific Tokio runtime.

Plugin id consists of a namespace (optional) and a name, separated by a dot (e.g., `basic_plugin.random_source`). The namespace allows grouping related plugins together, which can help avoid naming conflicts and organize plugins logically.

Any number of `register_plugin_*` macros can be called, e.g. to register multiple transforms.

When using macros, `SourcePlugin` constructor (`new`) will receive the async runtime (see `Async Runtime Support` section),
state backend factory (see `State Backend Support` section), metrics recorder (see `Plugin Metrics` section), and the
options map as parameters. `TransformPlugin` and `SinkPlugin` constructors will also receive the input schema as the
first parameter.

```rust
// Source constructor signature
fn new(
    rt: PluginAsyncRuntimeObj,
    state_backend_factory: PluginStateBackendFactory,
    metrics_recorder: PluginMetricsRecorder,
    options: HashMap<String, String>,
) -> Self

// Transform and Sink constructor signature (adds input_schema as first param)
fn new(
    schema: SchemaRef,
    rt: PluginAsyncRuntimeObj,
    state_backend_factory: PluginStateBackendFactory,
    metrics_recorder: PluginMetricsRecorder,
    options: HashMap<String, String>,
) -> Self
```

### Examples

The project includes example plugins that demonstrate different implementation approaches:

#### Basic Example

A complete plugin implementation using the high-level API:

```rust
// From plugin_examples/basic/src/lib.rs
mod sink;
mod source;
mod transform;

use crate::sink::PrintSink;
use crate::source::RandomSource;
use crate::transform::FilterTransform;
use streamling_plugin::{
  init_plugin_with_async_runtime, register_plugin_sink, register_plugin_source,
  register_plugin_transform,
};

// using namespace and name; this would be "basic_plugin.random_source" in the topology
register_plugin_source!("basic_plugin", "random_source", RandomSource);
register_plugin_transform!("basic_plugin", "filter_transform", FilterTransform);
// using name only
register_plugin_sink!("print_sink", PrintSink);
init_plugin_with_async_runtime!();
```

#### Source Plugin Example

The RandomSource example shows how to implement a data generator:

```rust
// From plugin_examples/basic/src/source.rs
pub struct RandomSource {
    rt: PluginAsyncRuntimeObj,
    schema: SchemaRef,
    options: HashMap<String, String>,
    max_rows: usize,
    row_count: AtomicUsize,
}

#[async_trait]
impl SourcePlugin for RandomSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        self.rt.sleep(RDuration::from_secs(1)).await;

        // Generate random data as a RecordBatch
        // ...

        Ok(batch)
    }

    // Checkpoint handling methods
    // ...
}
```

#### Transform Plugin Example

The FilterTransform example demonstrates data transformation:

```rust
// From plugin_examples/basic/src/transform.rs
pub struct FilterTransform {
    schema: SchemaRef,
    options: HashMap<String, String>,
}

#[async_trait]
impl TransformPlugin for FilterTransform {
    async fn process_batch(&self, batch: RecordBatch) -> Result<RecordBatch, PluginError> {
        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        // Apply filtering logic based on options
        // ...

        Ok(filtered_batch)
    }

    // Other required methods
    // ...
}
```

#### Low-Level Example

For developers who need more control, direct interaction with the message channels is possible:

```rust
// From plugin_examples/low_level/src/sink.rs
pub struct PrintSink {
    schema: SchemaRef,
    options: HashMap<String, String>,
    channels: PluginChannels,
}

impl PrintSink {
    pub async fn start(&self, runtime: PluginAsyncRuntimeObj) -> Result<(), DataFusionError> {
        loop {
            match self.channels.input.receiver.recv().map(|m| m.into_enum()) {
                Ok(Ok(PluginMsg::Init)) => {
                    // Handle initialization
                }
                Ok(Ok(PluginMsg::NextBatch { data })) => {
                    let batch: RecordBatch = data.into();
                    // Process the batch
                }
                // Handle other message types
                // ...
            }
        }
    }
}
```

### Building Plugins

To build a plugin (using the high-level API):

1. Create a new Rust crate with `crate-type = ["cdylib"]` in Cargo.toml
2. Implement one or more plugin traits: `SourcePlugin`, `TransformPlugin`, `SinkPlugin`, `PreprocessorPlugin`, `SideOutputPlugin`, or `ScalarUDFImpl` (for UDFs).
3. Use registration macros to register plugin components.
4. Call `init_plugin!()` or `init_plugin_with_async_runtime!()` to finalize.
5. Build with `cargo build`

The resulting shared library (`.so`, `.dylib`, `.dll`) can be loaded by Streamling via the `STREAMLING__PLUGIN__PATH` configuration.

### AI Authoring Skills

Streamling ships a pack of AI agent skills (in [`skills/`](skills/)) that teach a coding agent how to build plugins correctly — the registration macros, constructor contracts, checkpoint/exactly-once lifecycle, error types, and option/secret patterns, all grounded in this repo's plugin API. Install them once and an agent can scaffold a new source/sink/transform/preprocessor/UDF without re-deriving the FFI contract.

| Skill | Covers |
|---|---|
| `streamling-plugin-basics` | crate setup, registration macros, constructor contracts, lifecycle, async runtime, errors, options/secrets, metrics, state — **start here** |
| `streamling-source-plugin` | `SourcePlugin`, `_gs_op`, `generate_batch`, resumable pagination, backpressure |
| `streamling-sink-plugin` | `SinkPlugin`, lazy client init, batched writes with retry + partial-failure handling, checkpoint acks |
| `streamling-transform-plugin` | `TransformPlugin`, `process_batch`, arrow-compute filtering |
| `streamling-udf-plugin` | DataFusion scalar UDFs — `ScalarUDFImpl`, `invoke_with_args`, calling custom functions from SQL |
| `streamling-advanced-plugins` | preprocessors, side outputs, multi-kind crates, low-level FFI |

#### Installing the skills

Each skill is `<name>/SKILL.md`. Symlink the ones you want into your coding agent's personal skills directory (they then stay in sync with the repo):

| Agent | Skills directory |
|---|---|
| Claude Code | `~/.claude/skills/` |
| Codex, GitHub Copilot CLI, Gemini CLI | `~/.agents/skills/` (cross-runtime alias) |

```bash
cd /path/to/streamling
for d in skills/streamling-*; do
  for dir in ~/.claude/skills ~/.agents/skills; do
    mkdir -p "$dir" && ln -sf "$(pwd)/$d" "$dir/$(basename "$d")"
  done
done
```

Skills are discovered at agent **startup** — restart your session after installing. See [`skills/README.md`](skills/README.md) for the full per-agent path table (incl. `~/.codex`, `~/.copilot`, `~/.gemini`, Antigravity), a copy/freeze option, and verification steps.

### Checkpointing Support

Plugins fully integrate with Streamling's checkpointing system, allowing exactly-once processing semantics. The checkpointing messages flow through the same channels as data:

1. `CheckpointMarker`: Indicates a checkpoint should be created
2. `CheckpointAck`: Acknowledges receipt of a checkpoint marker (only from sinks)
3. `CheckpointFinalizer`: Indicates a checkpoint has been completed across the pipeline

By properly handling these messages, plugins can participate in Streamling's reliability guarantees.

### Channel Capacity Tuning

Plugins can declare default channel capacities using macros:

- `set_plugin_input_buffer!("<plugin_id>", <capacity>);`: Set input channel capacity.
- `set_plugin_output_buffer!("<plugin_id>", <capacity>);`: Set output channel capacity.

The global default can also be set via the `STREAMLING__PLUGIN__CHANNEL_CAPACITY` environment variable.

### Performance Considerations

- **Batch Size**: Adjust the batch size for optimal performance. Larger batches reduce overhead but increase latency.
- **Message Handling**: Process messages efficiently to avoid blocking the pipeline.
- **Data Conversion**: Minimize conversions between Arrow and native formats.
- **Resource Usage**: Be mindful of memory and CPU usage in plugin implementations.

## Profiling

### Profiling locally

You can profile application runs, unit and integration tests with [flamegraph](https://github.com/flamegraph-rs/flamegraph) locally.

[Install it](https://github.com/flamegraph-rs/flamegraph?tab=readme-ov-file#installation) first, then [run many scenarios](https://github.com/flamegraph-rs/flamegraph?tab=readme-ov-file#examples), e.g. profiling an integration test:

```bash
cargo flamegraph --test pipeline
```

### Profiling in Kubernetes

The project can be configured to be profiled with `perf`:

- If running in Kubernetes, you may need to update `securityContext`:
  - `capabilities.add: SYS_ADMIN` to allow using `perf` inside the container
  - `privileged: true` to allow access to kernel symbols (required for call graphs)
- Uncomment the following section in the Cargo.toml file:
  ```toml
  [profile.release]
  debug = true
  ```
  and wait until the build is ready. Ideally, this should be automated (e.g. we should have a separate Github Action for
  this).
- (Recommended) deploy without memory or ephemeral storage limits.
- Connect to a pod.
- Run `echo -1 | tee /proc/sys/kernel/perf_event_paranoid` and `echo 0 | tee /proc/sys/kernel/kptr_restrict`.
- Run `perf`: `perf record -p 1 --call-graph dwarf -F 99 -- sleep 15` (where `1` is the PID of the process). Time and
  frequency can be adjusted.
- Run `perf script --no-inline > out.perf`.
- Copy the `out.perf` file locally with `kubectl cp`.
- Checkout https://github.com/brendangregg/FlameGraph repo.
- Run `$PATH_TO_REPO/FlameGraph/stackcollapse-perf.pl out.perf > out.folded`.
- Run `$PATH_TO_REPO/FlameGraph/flamegraph.pl out.folded > out.svg`.
- Open `out.svg` in a browser.
- Enjoy the flamegraphs!

#### perf binary

Note: the `perf` binary may need to be compiled from scratch for the version of the linux kernel used in the Kubernetes cluster.

Instructions for compiling can be found [here](https://gist.github.com/sap1ens/827558883e1bd01709a52a4dedd2f10a).

## Benchmarking

An end-to-end throughput benchmark drives the real `streamling` binary through `Kafka (Avro/CDC) → SQL transform → blackhole sink` against the local k3s stack and reports records/sec. It is meant to catch performance regressions between releases, not to microbenchmark individual operators.

The harness lives in the `streamling-bench` crate and reuses the e2e stack (Redpanda + Prometheus). It runs two scenarios:

- `avro_cdc_projection` — projects/computes every row (selectivity 1.0).
- `avro_cdc_filter` — a predicate that passes ~10% of rows (selectivity 0.1).

### Running locally

First bring up the local environment (see [Using k3s for Local Development](#using-k3s-for-local-development)):

```bash
just env-setup
```

Then run the benchmark. It is **report-only** — it prints a comparison against the baseline but never fails on a regression:

```bash
# Both scenarios with defaults (5M records, 5 measured iterations + 1 warmup).
just bench

# A quick run with fewer records/iterations.
just bench --records 500000 --iterations 3 --warmup 1

# A single scenario.
just bench --scenario avro_cdc_filter
```

Each measured iteration re-reads the topic and reports:

- **Input throughput** — `records / wall_clock` (the headline) plus MB/s.
- **`compute_us_per_input_record`** — per-record compute time (microseconds) derived from `streamling_elapsed_compute_milliseconds_sum`. It excludes process startup and Kafka I/O wait, making it the most noise-resistant regression signal.

Pass `--out-dir <dir>` to also write a `<scenario>.json` result file per scenario.

### Baselines

Baselines are committed JSON files, one per scenario, under `bench/baselines/<runner_label>/<scenario>.json`. A run compares its median against the matching baseline and flags any metric that moved past `--regression-threshold` (default `0.10`, i.e. 10%).

Absolute throughput is machine-specific, so baselines are keyed by a **runner label** (`BENCH_RUNNER_LABEL`, defaults to `local`) — only compare runs from the same machine. If no baseline exists yet, the run just prints its numbers. To (re)seed the local baseline from a fresh run:

```bash
# Writes bench/baselines/local/<scenario>.json for every scenario.
just bench-update-baseline

# Or a single scenario.
just bench-update-baseline --scenario avro_cdc_projection
```

Commit the generated files to make them the reference for future local runs.

### In CI

The `streamling-bench.yml` workflow runs the same benchmark manually on a fixed runner (`blacksmith-8vcpu`) so its numbers are comparable across releases:

```bash
gh workflow run streamling-bench.yml --ref main
```

It writes the comparison to the job summary and uploads the results JSON as an artifact. To update the CI baseline, download that artifact and commit its files to `bench/baselines/blacksmith-8vcpu/`.

## Telemetry and Metrics

Streamling uses with Datafusion metrics system to record the metrics and has OpenTelemetry integration to export it to metrics-backend.

### Configuration

Telemetry is configured through the application configuration:

```yaml
open_telemetry_metrics:
  ingestion_endpoint: "http://localhost:4318/v1/metrics" # OTLP endpoint
  endpoint_protocol: "http/protobuf" # Protocol: http/json, http/protobuf, or grpc
  batch_interval_secs: 30 # How often to export metrics
  global_tags: "environment:production,image_tag:v1.0.0" # Global tags for all metrics
```

By default, service name is set to `streamling` and `service.instance.id` is set to `application_id`.

Environment variables can also be used:

- `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`: Override the ingestion endpoint
- `OTEL_EXPORTER_OTLP_PROTOCOL`: Override the protocol
- `STREAMLING__OPEN_TELEMETRY_METRICS__GLOBAL_TAGS`: Set global tags

### Supported Components and Metrics

The following table shows all supported components and the metrics they emit:

| Component                  | Tags/Labels | Metric Name           | Metric Type | Description                                          |
| -------------------------- | ----------- | --------------------- | ----------- | ---------------------------------------------------- |
| **Kafka Source**           | `topic`     | `output_rows`         | Counter     | Total number of rows consumed from Kafka topics      |
|                            |             | `output_rows_rate`    | Gauge       | Current rate of rows being consumed per time window  |
|                            |             | `output_rows_latency` | Histogram   | Time taken to process Kafka message batches (ms)     |
| **HTTP Handler Transform** |             | `output_rows`         | Counter     | Total number of rows processed by HTTP handlers      |
|                            |             | `output_rows_rate`    | Gauge       | Current rate of rows being processed per time window |
|                            |             | `output_rows_latency` | Histogram   | Time taken for HTTP request/response cycles (ms)     |
| **Blackhole Sink**         |             | `output_rows`         | Counter     | Total number of rows discarded                       |
|                            |             | `output_rows_rate`    | Gauge       | Current rate of rows being discarded per time window |
|                            |             | `output_rows_latency` | Histogram   | Time taken to process and discard batches (ms)       |

**Note**

All metrics include the following standard tags for filtering and aggregation:

- `id`: The key of the structs inside of `sources|transforms|sinks` in the pipeline-config
- `topology_node_type`: The topology type ("source", "transform", or "sink")
- `operator_type`: The specific implementation (e.g., "kafka", "sql", "webhook")
- Component-specific tags (e.g., `topic` for Kafka sources)

#### Per-node labels (`telemetry.labels`)

Every source, transform, and sink accepts an optional `telemetry.labels` map. Each key/value attaches as an additional Prometheus label dimension on every metric the node emits:

```yaml
sources:
  raw_events:
    type: kafka
    topic: app.events
    telemetry:
      labels:
        tier: critical
        dataset: app.events
        team: platform
```

Query: `streamling_output_rows_total{dataset="app.events",tier="critical"}`.

**Constraints (validated at pipeline load):**

- Up to 20 labels per node.
- Keys match Prometheus label-name grammar (`^[a-zA-Z_][a-zA-Z0-9_]*$`) and cannot start with `__`.
- Keys cannot shadow built-in tags (`id`, `topology_node_type`, `operator_type`, `service_instance_id`) or the per-type identity tag for the node's kind (`topic` on Kafka, `table` on ClickHouse/Postgres, `url` on webhooks, `type` on plugins, `language` on script transforms; hybrid sources reserve both `table` and `topic`).
- Values up to 256 bytes; control characters (newline, null, etc.) are rejected to protect Prometheus text-format output. Tab is allowed.

**Precedence with plugin-declared labels:** plugins can return their own identity labels via `PluginResult::labels`. When a plugin-declared key collides with a YAML-declared key on the same node, the plugin value wins and a WARN log names the colliding key and both values (plugins are authoritative about their own identity, but the WARN surfaces misconfigurations rather than hiding them).

**Hybrid sources:** `telemetry.labels` on a hybrid source automatically propagate to both the bounded and unbounded phase-child metric series.

#### Metric Temporality

Streamling exports two variants of the `output_rows` metric with different temporality semantics:

- **`output_rows_total`** (Cumulative): Reports the cumulative total of rows since process start. Use this for rate calculations in Prometheus/Grafana with functions like `rate()` and `increase()`.

- **`output_rows_delta`** (Delta): Reports incremental rows since the last export interval. Use this for billing and aggregation use cases in time-series databases like ClickHouse, where you can simply `SUM()` the values without worrying about resets or lag compensation.

### Adding Telemetry to Custom Operators

#### For Execution Plans (Sources and Transforms)

Wrap your execution plan with `TelemetryExec`. See `kafka.rs`, `external_handlers.rs` for inspiration.

#### For Sinks

Wrap your sink with `TelemetryDataSink`. See `blackhole.rs` for inspiration.

### Troubleshooting

If metrics are not appearing:

1. **Check Configuration**: Ensure the OTLP endpoint is reachable and configured correctly
2. **Verify Protocol**: Make sure the endpoint supports the configured protocol
3. **Review Logs**: Look for telemetry initialization errors in the application logs
4. **Test Connectivity**: Verify network connectivity to the metrics endpoint

The telemetry system will log warnings if initialization fails, but the application will continue running without metrics export.
