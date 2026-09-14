# Node Flow Metrics

Per-node flow metrics for diagnosing idleness, backpressure, empty batches, and
local in-flight buffering.

These answer the question a throughput graph cannot: when a node is not
producing rows, *why* — is it waiting on its input (idle), waiting on its output
(backpressure), or busily processing batches that happen to be empty?

Registered in `crates/streamling-core/src/telemetry/recorder.rs`; the shared
emission helpers live in `crates/streamling-core/src/telemetry/node_flow.rs`.

## Metrics

| Metric | Type | Unit | Definition |
| --- | --- | --- | --- |
| `node_idle_wait` | Histogram | ms | Time spent waiting for the next input batch before processing continues. |
| `node_backpressure_wait` | Histogram | ms | Time spent blocked while sending data or control messages downstream. |
| `node_backpressure_events` | Counter | count | Incremented when a single `node_backpressure_wait` sample is at or above the configured threshold. |
| `node_empty_batch` | Counter | count | Incremented when a processed or emitted batch has `num_rows == 0`. |
| `node_empty_streak` | Gauge | count | Current consecutive empty-batch streak for the node. Resets to `0` after a non-empty batch. |
| `node_inflight_buffered` | Gauge | count | Work buffered at the instrumented node boundary right now. |

`node_backpressure_wait` uses its own bucket layout: the shared duration
boundaries start at 100ms, which is far above a typical channel send, so it
prepends fine-grained sub-100ms buckets while keeping the same high end (a
wedged sink can block for minutes).

## Backpressure threshold

`node_backpressure_events` fires when one wait sample is at or above:

- `STREAMLING__BACKPRESSURE_EVENT_THRESHOLD_MS` (default `100`)

A non-numeric or zero value falls back to the default.

## Tags

Node-flow metrics carry the standard node tags resolved from
`metric_metadata_id`, plus `execution_kind`:

- `execution_kind=native` — native operator and connector paths.
- `execution_kind=plugin` — the plugin bridge.

## Emission sites

| Path | Metrics emitted |
| --- | --- |
| `operators/wrapping.rs` (`WrappingExec`) | `node_idle_wait`, `node_empty_batch`, `node_empty_streak` |
| `operators/wrapping.rs` (`WrappingDataSink`) | `node_idle_wait`, `node_empty_batch`, `node_empty_streak` |
| `operators/mod.rs` (`spawn_marker_preserving_forwarder`, used by `CheckpointableExec`) | `node_inflight_buffered`, `node_backpressure_wait`, `node_backpressure_events` |
| `operators/external_handlers.rs` (`ExternalHandlerExec`) | `node_inflight_buffered`, `node_backpressure_wait`, `node_backpressure_events` |
| `operators/wasm_runner.rs` (`WasmRunnerExec`) | `node_inflight_buffered`, `node_backpressure_wait`, `node_backpressure_events` |
| `plugin/operator.rs` (`PluginExec`) | all six |
| `streamling-connectors` `table_providers/hybrid.rs` (`HybridSourceExec`) | `node_inflight_buffered`, `node_backpressure_wait`, `node_backpressure_events` |

## Reading `node_inflight_buffered`

The gauge is deliberately **local to one boundary** — it is not a global
pipeline queue depth. Two nodes' values are not comparable as a queue; each says
how much that node is holding at that moment.

| Path | Value |
| --- | --- |
| `spawn_marker_preserving_forwarder` | `output_batch.num_rows()` while sending, `0` after |
| `ExternalHandlerExec` | `modified_batch.num_rows()` while sending, `0` after |
| `WasmRunnerExec` | `batch.num_rows()` while sending, `0` after |
| `HybridSourceExec` | rows of the batch being relayed, `0` after |
| `PluginExec` | `checkpoint_buffer.len() + epoch_created_at.len()` during checkpoint relay; `epoch_created_at.len()` around a data-batch send |

## Notes

- Wrapper metrics are intentionally limited to idleness and empty-batch
  behavior: the wrapper does not own the downstream send, so a backpressure
  sample taken there would attribute another node's stall to it.
- Every helper takes `Option<&MetricsRecorder>` and is a no-op when telemetry
  was never initialized, so instrumented paths stay silent in tests and tooling.
