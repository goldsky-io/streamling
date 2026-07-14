# Streamling Development Guide

Streamling is a Rust-based data processing engine built on Arrow and DataFusion. It reads from sources (Kafka), applies transforms (SQL, HTTP, WASM), and writes to sinks (Postgres, ClickHouse, Kafka, SQS, webhook, etc.).

## Project Layout

```
crates/
├── streamling/              # Main binary
├── streamling-core/         # Core topology, operators, pipeline engine
├── streamling-config/       # Pipeline YAML parsing and configuration
├── streamling-connectors/   # Table providers (sources/sinks)
├── streamling-plugin/       # FFI-based plugin system
├── streamling-state/        # State backend (checkpointing)
├── streamling-flink-compat/ # Flink state compatibility
└── streamling-e2e/          # End-to-end test framework (see crates/streamling-e2e/AGENT.md)

plugin_examples/             # Example plugins (basic, low_level)
scripts/                     # k3s setup/teardown scripts
```

## Command Runner (just)

All common tasks use `just`. Run `just --list` to see everything.

| Command               | Purpose                                            |
| --------------------- |----------------------------------------------------|
| `just build`          | Build the workspace                                |
| `just fix`            | Auto-fix cargo issues                              |
| `just lint`           | Format + clippy for the main workspace             |
| `just test`           | Run unit tests                                     |
| `just env-setup`      | Spin up k3s with Postgres, Kafka, ClickHouse, etc. |
| `just env-status`     | Check k3s cluster health                           |
| `just env-teardown`   | Tear down k3s cluster                              |
| `just e2e-test`       | Build + run all e2e tests                          |
| `just e2e-test-debug` | E2e tests with full output                         |
| `just e2e-list`       | List available e2e tests                           |

## Development Workflow

### After every task

1. Run `just fix` to auto-fix warnings and issues.
2. Run `just lint` to verify formatting and clippy pass.

### Fixing a bug

1. Write a **unit test** that reproduces the issue first.
2. Watch it fail.
3. Implement the fix — keep the change as small and easy to understand as possible.
4. Verify the test passes.
5. Run `just fix && just lint`.

### Adding a feature

1. Prefer unit tests for quick iteration during development.
2. If the feature involves pipeline-level behavior (source -> transform -> sink), add an **e2e test** once the implementation is complete.
3. Run `just fix && just lint`.

### Before pushing

Check that CI/CD has passed for the PR:

```bash
gh pr checks 494   # use the relevant PR number
```

## Testing Strategy

### Unit tests (preferred for iteration)

```bash
just test                       # Run all unit tests
cargo test -p streamling-core   # Run tests for a specific crate
```

Unit tests are fast and should be the first choice for verifying logic changes.

### E2E tests (always run locally)

E2E tests exercise the full `streamling` binary as a black box against real infrastructure (Kafka, Postgres, ClickHouse, SQS via ElasticMQ) running in a local k3s cluster.

**E2E tests must always be created and run locally before pushing.** They are the final verification that a pipeline-level change works end-to-end.

```bash
# First time: set up the local k3s environment
just env-setup

# Run all e2e tests
just e2e-test

# Run a specific test
just e2e-test test_basic_postgres_sink

# Run with full debug output
just e2e-test-debug

# Use debug build (faster compile, slower runtime)
PROFILE=debug just e2e-test
```

See `crates/streamling-e2e/AGENT.md` for detailed guidance on writing e2e tests (test patterns, resource isolation, available helpers).

## Pipeline YAML

Pipelines are defined in YAML with three sections: `sources`, `transforms`, and `sinks`. The `transforms` key is **always required**, even if empty (`transforms: {}`).

```yaml
sources:
  my_source:
    type: kafka
    topic: my_topic
    starting_offsets: earliest
    primary_key: id
    telemetry:
      event_time:
        column: block_timestamp
        unit: seconds
      labels:
        tier: critical
        dataset: v2.evm.blocks

transforms: {}

sinks:
  my_sink:
    type: postgres
    from: my_source
    table: output_table
    schema: public
    primary_key: id
    on_conflict: update
```

### `telemetry` block (optional)

Every source, transform, and sink accepts an optional `telemetry` block:

- **`event_time`** — column that carries the record's event time; drives `streamling_event_time_watermark_milliseconds` and `streamling_event_time_lag_milliseconds` series. `unit` required for integer columns (`seconds`, `milliseconds`, `microseconds`).
- **`labels`** — map of `key: value` identity labels attached to every metric this node emits. Use for dataset identity, tier/team tagging, destination tagging.

Label constraints (enforced at config load):

| Rule | Reason |
|------|--------|
| Max 20 labels per node | Each label is a Prometheus dimension; unbounded maps blow up storage. |
| Keys match `^[a-zA-Z_][a-zA-Z0-9_]*$` | Prometheus label-name grammar. |
| Keys cannot start with `__` | Prometheus reserves that prefix for internal use. |
| Keys cannot be `id`, `topology_node_type`, `operator_type`, `service_instance_id` | These collide with built-in metric dimensions. |
| Keys cannot shadow a node kind's per-type tag | `topic` on Kafka, `table` on ClickHouse/Postgres, `url` on webhooks, `type` on plugins, `language` on script transforms. Hybrid sources reserve both `table` and `topic`. Overriding would silently replace the real identity on every emitted metric. |
| Values max 256 bytes, no control characters (except tab) | A raw newline in a value can corrupt Prometheus scrape output. |

**Precedence when labels collide with plugin-declared labels:** plugin wins, and a WARN log names the colliding key and both values. Plugins are authoritative about their own identity (`chain_slug`, `topic`, etc.); YAML cannot override them, but the WARN surfaces misconfigurations rather than letting them go silent.

**Hybrid sources:** `telemetry.labels` on a hybrid source automatically propagate to both the bounded and unbounded phase-child metric series — declare them once on the parent.

### Node-wait metric (starved / blocked)

A node's idle time — time spent *not* doing useful work — is exported as a single counter, `streamling_node_wait_milliseconds_total`, split by a **`state`** tag into the two idle states of the utilization triad. The third state, **busy**, is derived from the existing `streamling_elapsed_compute_milliseconds`; together the three localize a bottleneck at a glance:

> **Measurement model:** `blocked` is measured at async polling/channel
> boundaries, not as downstream service latency. Prefetch, bounded queues, and
> concurrency can move observed wait between `blocked`, `starved`, and adjacent
> edges. Read [`docs/node-wait-measurement-model.md`](docs/node-wait-measurement-model.md)
> before changing buffering, connector scheduling, or plugin channels.

| State | Series | Meaning |
|-------|--------|---------|
| **starved** | `node_wait{state="starved"}` | waiting on upstream for input (slow source, or backpressure arriving from below) |
| **busy** | `elapsed_compute - node_wait{state="starved"}` | this node is the CPU/service bottleneck |
| **blocked** | `node_wait{state="blocked", downstream_id=...}` | held back by a specific downstream consumer |

> **`elapsed_compute` compatibility.** For backward compatibility, a `WrappingExec` node's input-wait (`data.next().await`) is *still folded into* `elapsed_compute` in addition to being emitted as `node_wait{state="starved"}`. So `elapsed_compute` retains its historical (input-wait + compute) meaning — existing dashboards/alerts are unchanged — and pure compute is `elapsed_compute - node_wait{state="starved"}`. The double-fold is deprecated; a future release will drop the `elapsed_compute` input-wait contribution so it means compute only. (Sink `elapsed_compute` is connector-recorded service time and is unaffected either way.)

Every `node_wait` series carries `id=<node>`, `state`, and `downstream_id` (empty for `starved` and for unresolved `blocked`), so the two states share an identical label key set and cross-state math needs no per-series label surgery.

**`blocked`** is modeled as a property of an **edge** (`producer → consumer`): the `downstream_id` names the consumer, so a linear chain is a sequence of single edges and a fan-out is several edges. The core invariant is **exactly one blocked emitter per edge**:

- A single-downstream node's `WrappingExec` emits its one edge (`id=self, downstream_id=the_consumer`), measured as yield→resume suspension.
- A **fan-out producer** (feeding a multi-sink or scan-sharing `BroadcastStream`) has its `WrappingExec` blocked emission **suppressed**; the `BroadcastStream` emits one edge per consumer (`id=producer, downstream_id=each_consumer`), measured as blocked-send time on each consumer's channel. `each_consumer` is the **immediate** downstream: for a multi-sink fan-out that is each terminal sink; for a scan-sharing fan-out that is each consuming transform reading the shared source (falling back to the sink only when a sink reads the shared source directly, with no transform in between).

Because the two blocked layers never coexist for the same node, no single edge is ever counted twice, and `... by (downstream_id)` is always the correct per-consumer signal (which consumer is slow). **Aggregating across edges needs care for fan-outs, though:** a fan-out producer sends to all consumers concurrently (`join_all`), so their per-edge blocked spans overlap in wall-clock. The producer's real block on a batch is the *slowest* edge (`join_all == max`), so `sum by (id) (node_wait{state="blocked"})` measures aggregate per-edge blocking *effort* and can exceed the producer's wall-clock blocked time — up to N× for N simultaneously-full channels. Use **`max by (id)`** for a wall-clock "is this node backpressured?" signal; `sum`/`by (downstream_id)` for per-edge attribution. For a linear single-edge node the two coincide. The only `downstream_id=""` blocked series is a rare fallback where a linear node's downstream name can't be resolved (still one series for that node).

**`starved`** is node-local (not an edge property): it is the `WrappingExec`'s time in `data.next().await`, always emitted with `downstream_id=""` regardless of the node's blocked-attribution role. For a source it is upstream I/O wait; for a channel-decoupled SQL transform it is input starvation.

Blocked attribution is stamped by the `DownstreamAttributionRule` physical-optimizer pass (`optimizer.rs`); scan-sharing producers are suppressed at construction (they are unreachable by the pass). Both states share the remainder-carrying `MillisAccumulator` (`telemetry/accumulator.rs`) so sub-millisecond spans are not truncated to zero at high throughput.

## Key Architecture Concepts

- **Arrow RecordBatches** flow between operators as the internal data format.
- **DataFusion** provides the SQL engine for transforms and table providers for sources/sinks.
- **Checkpointing** persists Kafka offsets and operator state for exactly-once semantics.
- **Plugin system** uses FFI (`cdylib` + `abi_stable`) to load external sources, sinks, and transforms at runtime.

## Common Pitfalls

- **Tests hang**: Add `batch_size: 1` to sink config and `env("STREAMLING__RECORD_BATCH_SIZE", "1")` to pipeline opts.
- **k3s not running**: Run `just env-status` to check, `just env-setup` to start.
- **Linker issues with nextest on macOS**: Set `export DYLD_LIBRARY_PATH="$HOME/.rustup/toolchains/1.89.0-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/lib:$DYLD_LIBRARY_PATH"`.

## AI Review Rules

This section defines rules enforced by the automated AI code review system
(`.github/workflows/ai-review.yml`). Each rule has a severity that determines
how it appears in review output. The review system also computes a per-file
risk score — see `scripts/ai-review/` for configuration and scoring logic.

Configuration (model, weights, thresholds) lives in `scripts/ai-review/review_config.json`.

### Convention Rules

| Rule ID  | Severity  | Rule                                                                                  |
| -------- | --------- | ------------------------------------------------------------------------------------- |
| CONV-001 | Block     | Bug fix PRs MUST include a failing unit test that reproduces the issue                |
| CONV-002 | Block     | `just fix && just lint` must pass (no clippy warnings with `-D warnings`)             |
| CONV-004 | Recommend | Pipeline-level features should have an e2e test                                       |
| CONV-005 | Recommend | Changes should be minimal and easy to understand — prefer small, focused PRs          |
| CONV-006 | Minor     | Commit messages should use conventional format (`feat:`, `fix:`, `chore:`, etc.)      |

### Rust Rules

| Rule ID  | Severity  | Rule                                                                                  |
| -------- | --------- | ------------------------------------------------------------------------------------- |
| RUST-001 | Block     | No `.unwrap()` in non-test production code — use `?`, `.expect("reason")`, or handle  |
| RUST-002 | Block     | `unsafe` blocks must have a `// SAFETY:` comment explaining soundness                 |
| RUST-003 | Recommend | Prefer `StreamlingError` over `anyhow::Error` for public APIs                        |
| RUST-004 | Recommend | New public APIs should have doc comments                                              |
| RUST-005 | Minor     | Avoid `clone()` where a reference or `Cow` would suffice                              |

### Connector Rules (kafka, clickhouse, postgres)

| Rule ID  | Severity  | Rule                                                                                  |
| -------- | --------- | ------------------------------------------------------------------------------------- |
| CONN-001 | Block     | Connection errors must be retriable (`StreamlingError::retriable`)                    |
| CONN-002 | Recommend | New sink configs should use `#[serde(deny_unknown_fields)]`                           |
| CONN-003 | Recommend | Resource cleanup (connections, cursors) must happen in `Drop` impls or explicit close |

### Pipeline / Topology Rules

| Rule ID  | Severity  | Rule                                                                                  |
| -------- | --------- | ------------------------------------------------------------------------------------- |
| PIPE-001 | Block     | Arrow schema changes must be backward-compatible or versioned                         |
| PIPE-002 | Recommend | New transforms must handle empty `RecordBatch`es gracefully                           |
| PIPE-003 | Recommend | Checkpoint-related changes must preserve exactly-once semantics                       |

### Tuning the Reviewer

When the AI reviewer gets something wrong, update this section with a new rule or
adjust an existing rule's severity. This is the primary correction mechanism — treat
it like a living document the team edits to steer the reviewer.

To adjust risk scoring weights and thresholds, edit `scripts/ai-review/review_config.json`.
To change the model (e.g. switch between `claude-opus-4-20250514` and `claude-sonnet-4-20250514`),
update the `model` field in that same config file.
