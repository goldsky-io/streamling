---
name: streamling-sink-plugin
description: Use when implementing a streamling SinkPlugin — lazy client initialization in initialize(), the empty-batch guard, converting RecordBatch to NDJSON for outbound writes, batched writes with retry and exponential backoff, partial-batch-failure handling that avoids silent data loss, and checkpoint-ack semantics for exactly-once. Assumes streamling-plugin-basics.
---

# streamling sink plugin — Agent Skill

A **sink** consumes `RecordBatch`es at the tail of a pipeline and writes them to an external system (a database, queue, object store, webhook). Sinks receive an upstream schema, do heavy I/O, and must preserve exactly-once semantics through the checkpoint protocol.

**Prerequisite:** [`streamling-plugin-basics`](skill://streamling-plugin-basics) (crate setup, registration, lifecycle, errors).

## The trait

```rust
#[async_trait]
pub trait SinkPlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    fn labels(&self) -> Vec<PluginLabel> { Vec::new() }            // optional identity
    async fn process_batch(&self, data: RecordBatch) -> Result<(), PluginError>;
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
}
```

Constructor (called by `register_plugin_sink!` — **schema first**, it's the upstream input schema):
```rust
pub fn new(
    schema: SchemaRef,
    rt: PluginAsyncRuntimeObj,
    state_backend_factory: PluginStateBackendFactory,
    metrics_recorder: PluginMetricsRecorder,
    options: HashMap<String, String>,
) -> Result<Self, PluginInitializationError>   // or Self
```

Note: sinks do **not** implement `output_schema` — they produce no batches.

## Lazy client init in `initialize()`

Construct network clients (DB pools, queue clients, HTTP clients) in `initialize()`, **not** `new()`. `new()` should only parse options and store handles. Use `OnceLock` so `initialize` is idempotent and so the client lives for the instance:

```rust
pub struct QueueSink {
    opts: PluginOptions,
    client: OnceLock<QueueClient>,
    queue_url: OnceLock<String>,
    running: Arc<AtomicBool>,
}

impl QueueSink {
    pub fn new(schema: SchemaRef, _rt: PluginAsyncRuntimeObj, _state: PluginStateBackendFactory,
               _metrics: PluginMetricsRecorder, options: HashMap<String, String>) -> Self {
        Self {
            opts: PluginOptions::new(options, "queue_sink", "STREAMLING__PLUGIN__QUEUE_SINK"),
            client: OnceLock::new(),
            queue_url: OnceLock::new(),
            running: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl SinkPlugin for QueueSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        if self.client.get().is_some() { return Ok(()); }          // idempotent

        let endpoint = self.opts.get("endpoint_url")?;              // env var > YAML > error
        let queue_url = self.opts.get("queue_url")?;
        let access_key = self.opts.get_secret("access_key_id");    // env var only
        let secret = self.opts.get_secret("secret_access_key");

        let mut client = QueueClient::builder().endpoint(endpoint);
        if let (Some(id), Some(sec)) = (access_key, secret) {
            client = client.credentials(id, sec);
        }
        let client = client.build().await
            .map_err(|e| PluginError::Internal(format!("client build: {e}")))?;

        let _ = self.client.set(client);
        let _ = self.queue_url.set(queue_url.clone());
        Ok(())
    }
    // ...
}
```
Rationale: `new()` is cheap and synchronous-ish (config parsing); `initialize()` is the async, fallible, network-touching setup. This also makes the struct unit-testable without a live network.

## The empty-batch guard

Always short-circuit empty batches first — the engine legitimately sends them (idle upstream, checkpoint-only epochs):
```rust
async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
    if !self.is_running() {
        return Err(PluginError::Internal("sink not running".into()));
    }
    if batch.num_rows() == 0 {
        return Ok(());                   // nothing to do — do NOT treat as error
    }
    // ... actual write
}
```

## RecordBatch → NDJSON

Most sinks serialize rows to newline-delimited JSON. Use `arrow_json::LineDelimitedWriter` over a projected schema:

```rust
use arrow_json::LineDelimitedWriter;

/// One JSON object per row, returned as owned byte buffers.
pub fn record_batch_to_ndjson(batch: &RecordBatch) -> Result<Vec<Vec<u8>>, arrow_schema::ArrowError> {
    let mut buf = Vec::new();
    {
        let mut w = LineDelimitedWriter::new(&mut buf);
        w.write_batch(batch)?;
        w.finish()?;
    }
    // split the single buffer into per-row slices at newlines
    Ok(buf.split(|&b| b == b'\n')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_vec())
        .collect())
}
```

**Gotcha — wide integers:** `arrow_json` renders `FixedSizeBinary(32)` (the streamling U256 extension type) as hex. If your dataset carries large numeric columns tagged with the `streamling.u256` extension metadata, pre-cast those columns to decimal-string `Utf8` before writing, so downstream JSON consumers get decimal strings. For ordinary (`Int*`, `Utf8`, `Float64`, `Boolean`, timestamps) columns, the default writer output is correct.

## Batched writes with retry + partial-failure

External APIs cap batch size (e.g. message-queue APIs commonly cap at 10 entries per call) and may fail *some* entries of a batch while succeeding others. A correct sink chunks, retries the failed subset with exponential backoff, and treats unrecoverable failures as fatal. This pattern — mined from production queue sinks — is the single most important thing to get right, because naive handling silently loses data.

```rust
const MAX_BATCH_SIZE: usize = 10;          // service limit
const MAX_RETRIES: u32 = 5;

async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
    if batch.num_rows() == 0 { return Ok(()); }

    let client = self.client.get()
        .ok_or_else(|| PluginError::Internal("client not initialized".into()))?;
    let queue_url = self.queue_url.get().unwrap();

    let rows = record_batch_to_ndjson(&batch)
        .map_err(|e| PluginError::Internal(format!("to json: {e}")))?;
    let messages: Vec<String> = rows.into_iter()
        .map(|b| String::from_utf8(b)
            .map_err(|e| PluginError::Internal(format!("utf8: {e}"))).unwrap())
        .collect::<Result<_, _>>()?;
    if messages.is_empty() { return Ok(()); }

    self.send_all(client, queue_url, &messages).await?;
    Ok(())
}

/// Send every message, chunking to the service limit, retrying only failures.
async fn send_all(&self, client: &QueueClient, url: &str, msgs: &[String]) -> Result<usize, PluginError> {
    let mut total = 0;
    // track original index with each message so partial-failure results map back
    let mut pending: Vec<(usize, String)> = msgs.iter().enumerate().map(|(i, s)| (i, s.clone())).collect();
    while !pending.is_empty() {
        let chunk: Vec<_> = pending.drain(..std::cmp::min(MAX_BATCH_SIZE, pending.len())).collect();
        let (sent, failed) = self.send_chunk_with_retry(client, url, &chunk).await?;
        total += sent;
        pending.extend(failed);       // only the still-failing subset loops again
    }
    Ok(total)
}

async fn send_chunk_with_retry(
    &self, client: &QueueClient, url: &str, chunk: &[(usize, String)],
) -> Result<(usize, Vec<(usize, String)>), PluginError> {
    let chunk_len = chunk.len();
    let mut to_retry: Vec<(usize, String)> = chunk.to_vec();
    let mut backoff_ms: u64 = 100;

    for attempt in 0..=MAX_RETRIES {
        let entries = to_retry.iter().enumerate().map(|(i, (_, body))| {
            client.build_entry(format!("msg_{i}"), body.clone())   // stable id = position in to_retry
        }).collect::<Result<Vec<_>, _>>()?;

        let result = client.send_batch(url, entries).await
            .map_err(|e| PluginError::Internal(format!("send: {e}")))?;

        let failed = result.failed_entries();
        if failed.is_empty() {
            return Ok((chunk_len, vec![]));                         // all sent
        }

        // Sender faults are non-retryable — the request itself is malformed.
        for f in failed.iter() {
            if f.sender_fault() {
                return Err(PluginError::Internal(format!(
                    "unrecoverable sender fault: {}", f.message())));
            }
        }

        // Map failed ids back to the messages they referred to.
        let mut retry = Vec::new();
        for f in failed.iter() {
            let i = f.id().strip_prefix("msg_").and_then(|s| s.parse::<usize>().ok());
            match i.filter(|&i| i < to_retry.len()) {
                Some(i) => retry.push(to_retry[i].clone()),
                None => return Err(PluginError::Internal(format!(
                    "could not map failed entry id {} back to a message — possible data loss", f.id()))),
            }
        }

        if attempt == MAX_RETRIES {
            return Err(PluginError::Internal(format!(
                "{} messages failed after {MAX_RETRIES} retries", retry.len())));
        }

        tracing::warn!(failed = retry.len(), attempt, "partial failure; retrying");
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = std::cmp::min(backoff_ms * 2, 5_000);          // capped exponential
        to_retry = retry;
    }
    Ok((0, vec![]))
}
```

The three things that make this safe:
1. **Stable per-attempt ids** (`msg_{index-into-to_retry}`) so a partial-failure result can be mapped back to exact messages.
2. **Unmapped id → hard error.** If a failed entry's id can't be mapped back, you don't know which message failed — treat it as data-loss and error out rather than silently dropping it.
3. **Sender fault → fatal.** A sender fault means the request is malformed; retrying wastes the budget and masks a bug.

For idempotent sinks (upsert by primary key), you can retry whole batches on transient errors without dedup worry. For append-only sinks (queues, webhooks), per-entry tracking as above is what prevents loss.

## Checkpoint acks (exactly-once)

Sinks ack upstream through the checkpoint protocol:
- `process_checkpoint_marker(epoch)` — a checkpoint is starting. Return `Ok` to ack; this tells the source its data is safe to advance.
- `process_checkpoint_finalizer(epoch)` — the epoch is durably committed. For a sink that buffers writes internally, **flush here** so a committed epoch is genuinely durable before the source advances past it.

```rust
async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
    tracing::info!(epoch = epoch.0, "checkpoint marker acked");
    Ok(())
}
async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
    // If you buffer writes, flush/commit them now so the acked epoch is durable.
    self.flush().await?;
    Ok(())
}
```
If `process_batch` already writes synchronously and the destination is durable per-call, the finalizer can be a no-op. The rule: **by the time the finalizer returns Ok, everything up to that epoch must survive a crash.**

## Upsert sinks (DB-style)

For database sinks with a primary key and `on_conflict: update`:
- Read `primary_key` and `on_conflict` from options.
- Use the `_gs_op` column to decide insert/update vs delete: rows with `"d"` issue a delete by key; `"i"`/`u"` issue an upsert.
- Batch into multi-row `INSERT ... ON CONFLICT` statements chunked to a safe size; reuse the retry helper above for transient DB errors (deadlocks, connection blips). Distinguish retriable (connection, serialization) from fatal (constraint violation) errors.

## Identity labels & metrics

```rust
fn labels(&self) -> Vec<PluginLabel> {
    vec![PluginLabel::new("destination", self.opts.get_or("table", "default"))]
}
```
Emit timing for the slow path:
```rust
let start = Instant::now();
self.send_all(client, url, &messages).await?;
self.metrics_recorder.record_latency_w_tags(
    "sink_send_latency", start.elapsed(), vec![("table", &table)]);
```

## Common mistakes

- **Building the client in `new()`.** Do it in `initialize()` behind a `OnceLock`. `new()` must stay cheap.
- **Treating empty batches as errors.** They're normal; guard and return `Ok(())`.
- **Retrying the whole batch on partial failure.** Re-sends the successful entries; for non-idempotent destinations that's duplicates or loss. Retry only the failed subset with stable id mapping.
- **Silently dropping unmappable failures.** Error out — "possible data loss" must be loud.
- **Flushing in `process_batch` only.** If you buffer, the checkpoint finalizer is the commit point; not flushing there can lose buffered data on crash.
- **Hex-encoded wide integers in JSON.** Pre-cast U256 extension columns to decimal strings.

## Related skills

- [`streamling-plugin-basics`](skill://streamling-plugin-basics) — registration, options/secrets (`PluginOptions`), state backend, error types.
- [`streamling-source-plugin`](skill://streamling-source-plugin) — the producing half; source finalizers and sink acks are the two ends of the exactly-once contract.
