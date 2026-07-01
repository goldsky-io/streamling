---
name: streamling-transform-plugin
description: Use when implementing a streamling TransformPlugin — process_batch with RecordBatch in and RecordBatch out, declaring output_schema, filtering or projecting with Arrow compute (boolean masks, filter_record_batch), handling empty batches, and passing checkpoints through. Use this instead of the built-in sql transform when row logic needs custom Rust or external calls. Assumes streamling-plugin-basics.
---

# streamling transform plugin — Agent Skill

A **transform** sits in the middle of a pipeline: it receives an upstream `RecordBatch`, returns a (possibly reshaped) `RecordBatch`, and propagates checkpoints. Transforms are stateless-by-default row pipelines — use one when the built-in `sql` transform can't express the logic (custom Rust, per-row external calls, non-SQL projections).

**Prerequisite:** [`streamling-plugin-basics`](skill://streamling-plugin-basics) (crate setup, registration, lifecycle, errors).

**When NOT to write a transform:** if the work is column projection/renaming, a `UNION ALL`, or a `JOIN`, the built-in `type: sql` transform (DataFusion) is simpler and faster — see [`sql-transform`](skill://sql-transform). Reach for a plugin transform only when you need imperative Rust or side effects the SQL engine can't do.

## The trait

```rust
#[async_trait]
pub trait TransformPlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    fn output_schema(&self) -> Result<SchemaRef, PluginError>;
    fn labels(&self) -> Vec<PluginLabel> { Vec::new() }
    async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError>;
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
}
```

Constructor (called by `register_plugin_transform!` — **schema first**):
```rust
pub fn new(
    schema: SchemaRef,                       // upstream input schema
    rt: PluginAsyncRuntimeObj,
    state_backend_factory: PluginStateBackendFactory,
    metrics_recorder: PluginMetricsRecorder,
    options: HashMap<String, String>,
) -> Result<Self, PluginInitializationError>   // or Self
```

## `output_schema` — pass-through vs reshape

- **Pass-through** (filter, enrich-in-place): return the input schema unchanged.
- **Reshape** (drop/add/rename columns): return a *different* schema, built once in `new()`. Downstream stages see whatever you return here.

```rust
fn output_schema(&self) -> Result<SchemaRef, PluginError> {
    Ok(self.output_schema.clone())     // precomputed in new()
}
```

## `process_batch` — the core contract

- Receive a `RecordBatch`, return a `RecordBatch`.
- **Empty in → empty out.** Always handle `num_rows() == 0` first (return the empty batch unchanged) so you never build zero-length arrays by accident.
- Returning `Err` fails the pipeline. For per-row soft failures, emit a null or drop the row rather than erroring the whole batch.

## Pattern: row filter with a boolean mask

Filtering is the canonical transform. Build a `BooleanArray` mask and apply it with `filter_record_batch`:

```rust
use arrow::array::{Array, BooleanArray, StringArray, RecordBatch};
use arrow::compute::{and, filter_record_batch};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, TransformPlugin};

pub struct FilterTransform {
    schema: SchemaRef,
    column: String,                       // parsed from options
    keep_value: String,
    running: Arc<AtomicBool>,
}

impl FilterTransform {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state: PluginStateBackendFactory,
        _metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Self {
        Self {
            schema,
            column: options.get("column").cloned().unwrap_or_default(),
            keep_value: options.get("keep").cloned().unwrap_or_default(),
            running: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for FilterTransform {
    fn is_running(&self) -> bool { self.running.load(Ordering::SeqCst) }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl TransformPlugin for FilterTransform {
    async fn initialize(&self) -> Result<(), PluginError> { Ok(()) }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> { Ok(self.schema.clone()) }

    async fn process_batch(&self, batch: RecordBatch) -> Result<RecordBatch, PluginError> {
        if batch.num_rows() == 0 { return Ok(batch); }            // empty in → empty out

        let idx = batch.schema().index_of(&self.column)
            .map_err(|e| PluginError::Execution(format!("missing column {}: {e}", self.column)))?;
        let col = batch.column(idx);

        // Start with all-true, AND in each predicate (extend to multiple conditions).
        let mut mask = BooleanArray::from(vec![true; batch.num_rows()]);
        if let Some(strings) = col.as_any().downcast_ref::<StringArray>() {
            let values: Vec<bool> = (0..strings.len())
                .map(|i| strings.value(i) == self.keep_value.as_str())
                .collect();
            let pred = BooleanArray::from(values);
            mask = and(&mask, &pred).map_err(PluginError::ArrowError)?;
        }
        // For other column types, downcast to Int64Array/Float64Array/etc. and compare.

        filter_record_batch(&batch, &mask).map_err(PluginError::ArrowError)
    }

    async fn process_checkpoint_marker(&self, _e: CheckpointEpoch) -> Result<(), PluginError> { Ok(()) }
    async fn process_checkpoint_finalizer(&self, _e: CheckpointEpoch) -> Result<(), PluginError> { Ok(()) }
}
```

Register it:
```rust
register_plugin_transform!("my_plugin", "filter", FilterTransform);
set_plugin_output_buffer!("my_plugin.filter", 1);   // optional backpressure cap
```

### Multi-condition masks

Start from all-true and fold each predicate with `and(&mask, &pred)`. Build each predicate by downcasting the column to its concrete array type (`StringArray`, `Int64Array`, `Float64Array`, `BooleanArray`, …) and producing a `Vec<bool>` per row. Nulls are falsy by default — handle them explicitly if your logic differs.

## Pattern: project / add a derived column

To produce a **different** schema, build new arrays and assemble a fresh `RecordBatch` against your `output_schema`. Common builders: `StringBuilder`, `Int64Builder`, `Float64Builder`, `BooleanBuilder`, `ListBuilder`. For derived columns, iterate the source columns, compute, append, then `RecordBatch::try_new(self.output_schema.clone(), vec![...])`.

## Pattern: stateful transforms

If the transform must remember something across batches (a running aggregate, a de-dup set), use the state backend (see [`streamling-plugin-basics.md`](./streamling-plugin-basics.md#state-backend)). Restore in `initialize`, mutate in `process_batch`, and persist anything that must survive restart in `process_checkpoint_finalizer`. Keep in-memory state behind the same `Arc<AtomicBool>` shutdown discipline as sources/sinks.

## Checkpoints: pass them through

Unless your transform owns durable state, both checkpoint methods are no-ops — just return `Ok(())`. Returning `Ok` propagates the marker downstream; the transform is transparent to the checkpoint protocol.

## Backpressure

If a downstream stage is slower than the transform (e.g. the transform fans out to a heavy enricher), cap the output channel:
```rust
set_plugin_output_buffer!("my_plugin.filter", 1);
```

## Common mistakes

- **Forgetting the empty-batch guard.** Building arrays from a zero-row batch can panic or produce schema-mismatched output. Return it unchanged.
- **Wrong output schema.** If you drop/add columns, `output_schema()` must match the batches you actually return, or the downstream stage errors at runtime.
- **Panicking on a bad row.** Downcast failures and index lookups should become `PluginError::Execution(..)`, not panics — a panic crashes the worker.
- **Erroring the whole batch for one bad row.** Prefer nulling/skipping the row; reserve batch-level errors for unrecoverable problems.
- **Re-implementing SQL.** If it's a projection/union/join, use [`sql-transform`](skill://sql-transform).

## Related skills

- [`streamling-plugin-basics`](skill://streamling-plugin-basics) — registration, lifecycle, options, state backend, errors.
- [`sql-transform`](skill://sql-transform) — the built-in DataFusion transform; prefer it for projection/union/join.
- [`streamling-udf-plugin`](skill://streamling-udf-plugin) — custom SQL functions; a lighter alternative to a full transform for a single derived column.
