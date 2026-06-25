---
name: streamling-source-plugin
description: Use when implementing a streamling SourcePlugin — defining the output schema and _gs_op column, generate_batch and the empty-batch means-no-data contract, decomposing a source into fetcher/buffer/runner, resumable cursor or sequential pagination that survives restart via the checkpoint state backend, and output-channel backpressure. Assumes streamling-plugin-basics.
---

# streamling source plugin — Agent Skill

A **source** is the head of a pipeline: it owns its schema, produces `RecordBatch`es on demand, and drives checkpointing. Sources are pull-based — streamling calls `generate_batch()` repeatedly and stops when `is_running()` is false or the pipeline drains.

**Prerequisite:** [`streamling-plugin-basics`](skill://streamling-plugin-basics) (crate setup, registration, lifecycle, errors).

## The trait

```rust
#[async_trait]
pub trait SourcePlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    fn output_schema(&self) -> Result<SchemaRef, PluginError>;
    fn labels(&self) -> Vec<PluginLabel> { Vec::new() }            // optional identity
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError>;
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
}
```

Constructor (called by `register_plugin_source!` — **no schema arg**, the source owns it):
```rust
pub fn new(
    rt: PluginAsyncRuntimeObj,
    state_backend_factory: PluginStateBackendFactory,
    metrics_recorder: PluginMetricsRecorder,
    options: HashMap<String, String>,
) -> Result<Self, PluginInitializationError>
```

## Output schema and the `_gs_op` column

A source emits change-data with a per-row operation column. Import the canonical name and include it:

```rust
use streamling_plugin::api::STREAMLING_COLUMN_NAME_OP; // == "_gs_op"

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Utf8, false),
    Field::new("value", DataType::Int64, false),
    Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),  // "i" | "u" | "d"
]));
```

`_gs_op` values: `"i"` insert, `"u"` update, `"d"` delete. Downstream sinks use it to apply upsert/delete semantics. If your source is append-only, emit `"i"` for every row — but the column is still expected by the engine.

`output_schema()` must return the **same** schema for the life of the instance. Build it once in `new()` and clone it.

## `generate_batch` — the pull contract

- Return a **non-empty** `RecordBatch` when you have data.
- Return an **empty batch** (`RecordBatch::new_empty(self.schema.clone())`) when there is no data right now. The engine treats this as "not done, just idle" and will poll again. Do **not** block indefinitely inside `generate_batch` — sleep briefly then return empty if still waiting, so shutdown stays responsive.
- Return `Err(PluginError::..)` to fail the pipeline.

Building a batch with Arrow builders:
```rust
async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
    let rows = self.buffer.next_batch().await;
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(self.schema.clone()));
    }
    let mut ids = arrow::array::StringBuilder::new();
    let mut vals = arrow::array::Int64Builder::new();
    let mut ops = arrow::array::StringBuilder::new();
    for r in &rows {
        ids.append_value(&r.id);
        vals.append_value(r.value);
        ops.append_value("u");                 // or "i"/"d"
    }
    Ok(RecordBatch::try_new(self.schema.clone(), vec![
        Arc::new(ids.finish()), Arc::new(vals.finish()), Arc::new(ops.finish()),
    ]).map_err(|e| PluginError::ArrowError(e))?)
}
```

## Fetcher / buffer / runner decomposition

Anything beyond a toy source should be split into three pieces (this is the shape every non-trivial production source converges on):

- **Fetcher** — pure async `fetch_page(cursor) -> (Vec<Item>, next_cursor)`. No streamling types; trivially unit-testable against a mock HTTP client.
- **Buffer** — an ordered, bounded queue that the runner fills and `generate_batch` drains. Caps memory and provides backpressure.
- **Runner** (the `SourcePlugin` impl) — wires fetcher + buffer + checkpoint state to the streamling lifecycle: starts a background poll loop in `initialize`, drains in `generate_batch`, persists progress in `process_checkpoint_finalizer`, stops in `terminate`.

Keep the streamling trait impl thin; push logic into the fetcher/buffer so you can test it without the FFI.

## Resumable pagination via the state backend

The durable-resume pattern: store a cursor/offset in `PluginStateBackend`, advance it only on `process_checkpoint_finalizer` (the commit point), and restore it in `initialize`.

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Cursor { last_ts_ms: i64, token: Option<String> }

pub struct ApiSource {
    schema: SchemaRef,
    client: ApiClient,
    buffer: Arc<OrderedBuffer<Row>>,        // shared with the poll loop
    state: Arc<PluginStateBackend<Cursor>>,
    running: Arc<AtomicBool>,
    cursor: OnceLock<Cursor>,               // set in initialize
}

#[async_trait]
impl SourcePlugin for ApiSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        // Restore committed progress; fall back to the configured start.
        let mut cursor = self.state.get().await.map_err(PluginError::State)?
            .unwrap_or_else(|| Cursor { last_ts_ms: self.start_ts, token: None });
        // never regress past the configured start
        if cursor.last_ts_ms < self.start_ts { cursor.last_ts_ms = self.start_ts; }

        let buf = self.buffer.clone();
        let client = self.client.clone();
        let running = self.running.clone();
        // background poll loop (detached; Arcs keep it alive)
        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                let page = match client.fetch_page(&cursor).await {
                    Ok(p) => p,
                    Err(e) => { tracing::warn!(error=%e, "fetch failed; will retry"); continue; }
                };
                if page.items.is_empty() {
                    tokio::time::sleep(Duration::from_millis(60_000)).await;
                    continue;
                }
                cursor = page.next_cursor();          // monotonic advance
                buf.push(page.items).await;
            }
        });
        let _ = self.cursor.set(cursor);
        Ok(())
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let rows = self.buffer.next_batch().await;    // drains up to batch_size
        if rows.is_empty() { return Ok(RecordBatch::new_empty(self.schema.clone())); }
        // advance an in-memory high-water mark (never regress)
        if let Some(last) = rows.last() {
            self.last_ts_ms.fetch_max(last.ts_ms, Ordering::AcqRel);
        }
        build_batch(&self.schema, &rows)
    }

    async fn process_checkpoint_marker(&self, _e: CheckpointEpoch) -> Result<(), PluginError> { Ok(()) }

    async fn process_checkpoint_finalizer(&self, _e: CheckpointEpoch) -> Result<(), PluginError> {
        // THE commit point: persist durable progress here, never in generate_batch.
        let cursor = Cursor {
            last_ts_ms: self.last_ts_ms.load(Ordering::Acquire),
            token: self.cursor.get().and_then(|c| c.token.clone()),
        };
        self.state.put(cursor).await.map_err(PluginError::State)
    }
}
```

**Why finalizer, not `process_batch`:** the finalizer fires only after the epoch is durably committed across the whole pipeline. Writing the cursor here gives exactly-once resume — a restart replays from the last committed cursor, never skipping data or emitting it twice durably.

**Monotonic cursor:** always `fetch_max`/max — never let a late or reordered page move the high-water mark backwards.

### Two pagination flavors

- **Numeric/height pagination** (keyed by a monotonic `u64`, e.g. a sequence number or block height): use a *sequential* source — pages are `height..height+N`, dedup by height, resume stores the last height.
- **Opaque cursor + timestamp** (server-issued `next_page` token plus a monotonic timestamp): use a *cursor* source — resume stores `{last_ts_ms, token}`, and on restart you re-request from the token or re-scan from `last_ts_ms`. Catalog/reference APIs that change in place (a row's fields update over time) benefit from **content-hash dedup**: hash each row excluding its observed-at field, and only forward rows whose hash changed since last emit (in-process map, cleared on restart → one at-least-once re-emit, which is the documented tradeoff).

### Snapshot-then-tail (hybrid) sources

When a dataset has a finite historical snapshot and then a live tail, run the snapshot to completion first, record the handoff point in state, then switch to polling the tail from that point. Keep both phases writing the same schema and `_gs_op`. The same source can expose both phases under one `SourcePlugin`; streamling also has a native `hybrid` source type if you'd rather compose two providers.

## Backpressure

If your source can produce faster than the downstream can consume, bound the output channel:

```rust
register_plugin_source!("my_plugin", "api_source", ApiSource);
set_plugin_output_buffer!("my_plugin.api_source", 1);   // capacity 1: strict backpressure
```

A capacity of `1` makes the source block (cooperative, via the channel) until the downstream drains — useful for heavy transforms that can't keep up. Larger capacities decouple producer/consumer at the cost of memory. Pair with a bounded internal buffer so your poll loop naturally stalls instead of allocating unbounded memory.

## Shutdown

```rust
#[async_trait]
impl SupportsGracefulShutdown for ApiSource {
    fn is_running(&self) -> bool { self.running.load(Ordering::SeqCst) }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);   // poll loop exits on next check
        Ok(())
    }
}
```
Keep `is_running()` returning `true` until `initialize` has run (e.g. gate on a `OnceLock` being set), so the engine doesn't bail before you've started.

## Heavy row-building: use `spawn_blocking`

If converting fetched items into an Arrow batch is CPU-heavy (wide rows, many columns), move it off the async thread:
```rust
let schema = self.schema.clone();
let batch = tokio::task::spawn_blocking(move || build_batch(&schema, &items))
    .await
    .map_err(|e| PluginError::Internal(format!("blocking task: {e}")))??;
```

## Common mistakes

- **Blocking forever in `generate_batch`.** Sleep-then-empty when idle, so `terminate` is prompt.
- **Persisting cursor in `process_batch`.** Breaks exactly-once. Use `process_checkpoint_finalizer`.
- **Regressing the cursor.** Use `fetch_max`/max on restore and on each batch.
- **No `_gs_op` column.** The engine and CDC-aware sinks expect it.
- **Schema drift.** `output_schema()` must be stable for the instance's lifetime.
- **Unbounded buffer under backpressure.** Cap the channel with `set_plugin_output_buffer!` and the internal buffer together.

## Related skills

- [`streamling-plugin-basics`](skill://streamling-plugin-basics) — registration, lifecycle, options/secrets, state backend API.
- [`streamling-sink-plugin`](skill://streamling-sink-plugin) — the consuming half; sink checkpoint acks mirror source finalizers.
