---
name: streamling-plugin-basics
description: Use when creating a new streamling plugin crate in Rust (source, sink, transform, preprocessor, UDF, or side output), or when wiring up registration macros, constructor contracts, the plugin lifecycle, the async runtime, error types, or option/secret handling. Start here before the type-specific skills.
---

# streamling plugin basics — Agent Skill

A streamling **plugin** is an independently compiled `cdylib` loaded at runtime via a stable ABI (`abi_stable`). A plugin crate can register any mix of six component kinds: **source**, **transform**, **sink**, **preprocessor**, **UDF**, and **side output**. This skill covers the crate skeleton, registration, the lifecycle every component shares, and the cross-cutting utilities (options, metrics, state, async runtime, errors). For the per-kind trait patterns, see the sibling skills.

**Read this first**, then jump to the specific skill:
- [`streamling-source-plugin`](skill://streamling-source-plugin)
- [`streamling-sink-plugin`](skill://streamling-sink-plugin)
- [`streamling-transform-plugin`](skill://streamling-transform-plugin)
- [`streamling-udf-plugin`](skill://streamling-udf-plugin) — custom SQL functions (DataFusion UDFs).
- [`streamling-advanced-plugins`](skill://streamling-advanced-plugins) — preprocessor, UDF, side output, multi-plugin crate, low-level FFI.

## Crate setup

A plugin is a Cargo crate compiled to `cdylib` (+ `rlib` so tests work) that depends on `streamling-plugin`.

```toml
# Cargo.toml
[package]
name = "my_plugins"
version = "0.1.0"
edition = "2024"

[lib]
name = "my_plugins"
crate-type = ["cdylib", "rlib"]

[dependencies]
streamling-plugin = "0.2.0"          # or path = "../crates/streamling-plugin"
abi_stable = "0.11.3"                # required by the init macro
async-trait = "0.1.83"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tracing = "0.1"
arrow = "55.2.0"
arrow-schema = "55.2.0"
serde = { version = "1", features = ["derive"] }
```

`crate-type = ["cdylib", "rlib"]` is mandatory — `cdylib` is what streamling loads; `rlib` lets unit tests link against your code.

## Registration (lib.rs)

Register every component with a macro, then invoke **exactly one** init macro at the bottom:

```rust
mod sink;
mod source;

use crate::sink::MySink;
use crate::source::MySource;
use streamling_plugin::{
    init_plugin_with_async_runtime, register_plugin_sink, register_plugin_source,
};

// id can be "namespace.name" (two string args) or just "name" (one string arg).
register_plugin_source!("my_plugin", "rest_source", MySource);
register_plugin_sink!("my_sink", MySink);

init_plugin_with_async_runtime!();
```

### Macro reference

| Macro | Form | Notes |
|---|---|---|
| `register_plugin_source!` | `("ns", "name", T)` or `("name", T)` | `T::new(rt, state, metrics, opts)` |
| `register_plugin_transform!` | `("ns", "name", T)` or `("name", T)` | `T::new(schema, rt, state, metrics, opts)` |
| `register_plugin_sink!` | `("ns", "name", T)` or `("name", T)` | `T::new(schema, rt, state, metrics, opts)` |
| `register_plugin_preprocessor!` | `("ns", "name", T)` or `("name", T)` | `T::new(opts)` |
| `register_plugin_udf!` | `(T)` | `T: ScalarUDFImpl + Default` |
| `register_plugin_udf_fn!` | `(create_fn)` | `fn() -> ScalarUDF` |
| `register_plugin_side_output!` | `("id", T)` or `(T)` | `T: SideOutputPlugin` |
| `set_plugin_output_buffer!` | `("plugin.id", capacity)` | backpressure cap on the output channel |
| `set_plugin_input_buffer!` | `("plugin.id", capacity)` | cap on the input channel |
| `init_plugin_with_async_runtime!` | `()` | **required, exactly once**, at the bottom |

**The `plugin.id` string** is `namespace.name` when you pass two args, else `name`. The buffer-setter macros take that exact full id (e.g. `set_plugin_output_buffer!("my_plugin.rest_source", 1)`).

**Two init macros exist.** Use `init_plugin_with_async_runtime!()` — it embeds a real Tokio runtime (`DirectTokioProxy`), so your plugin can call `tokio::time::sleep`, `tokio::task::spawn_blocking`, etc. directly. The bare `init_plugin!()` variant expects the host to supply the runtime and is rarely what a standalone plugin wants.

## Constructor contracts (exact)

The registration macros generate a closure that calls your `new()` with a fixed argument order. **Match it exactly** or the macro will fail to compile:

| Kind | `new(...)` arguments | Return |
|---|---|---|
| Source | `rt, state_backend_factory, metrics_recorder, options` | `Result<Self, PluginInitializationError>` or `Self` |
| Transform | `schema, rt, state_backend_factory, metrics_recorder, options` | `Result<Self, _>` or `Self` |
| Sink | `schema, rt, state_backend_factory, metrics_recorder, options` | `Result<Self, _>` or `Self` |
| Preprocessor | `options` | `Result<Self, _>` |

Where:
- `rt: PluginAsyncRuntimeObj`
- `state_backend_factory: PluginStateBackendFactory`
- `metrics_recorder: PluginMetricsRecorder`
- `schema: arrow_schema::SchemaRef` (the upstream input schema — **not** passed to sources, which define their own)
- `options: HashMap<String, String>` (parsed from the pipeline YAML `options:` map)

The `Result<Self, PluginInitializationError>` form is preferred — it lets you reject bad config at create time with a clear `Configuration(..)` message instead of panicking. (The generator already catches panics and converts them to `Configuration` errors, but an explicit `Result` is cleaner.)

## Lifecycle

Every source/transform/sink runs the same protocol, driven by streamling over message channels:

```
initialize()                          # once, before any data
  └─ process_batch(...)               # repeated: source→generate_batch, transform/sink→process_batch
       process_checkpoint_marker(E)   # a checkpoint epoch is starting
       process_checkpoint_finalizer(E)# that epoch is durably committed — safe to flush/advance
  terminate()                         # graceful shutdown
```

**Checkpoint semantics (load-bearing):**
- `process_checkpoint_marker(epoch)` — a checkpoint *beginning*. Returning `Ok` propagates the marker downstream. For sinks this means "an ack should be sent upstream."
- `process_checkpoint_finalizer(epoch)` — the epoch is **durably committed**. This is where you persist durable progress: advance a cursor, flush a buffer, save state. If you maintain resumable progress, do it here, not in `process_batch`.
- `process_batch` returning `Err` fails the pipeline. Empty batches are a normal "no data right now" signal — see the source skill.

`SupportsGracefulShutdown` is a required supertrait for source/transform/sink:
```rust
fn is_running(&self) -> bool;                 // false → engine stops polling
async fn terminate(&self) -> Result<(), PluginError>;  // release resources, set running=false
```
The standard idiom is an `Arc<AtomicBool>` flipped to `false` in `terminate()`.

## Async runtime

`rt: PluginAsyncRuntimeObj` exposes FFI-safe async primitives:
```rust
rt.spawn(fut);                       // spawn a detached task
rt.sleep(RDuration::from_millis(100)).await;   // non-blocking sleep
rt.timeout(RDuration::from_secs(5), fut).await;
rt.block_on(fut);                    // rare; block on a future
rt.yield_now().await;
```
`RDuration` comes from `abi_stable::std_types`. Because `init_plugin_with_async_runtime!()` installs a real Tokio runtime, you can also use `tokio::time::sleep`, `tokio::task::spawn_blocking`, and `tokio::sync` types directly inside your methods.

## Options & secrets

`options: HashMap<String, String>` arrives flat from the pipeline YAML. For anything non-trivial, centralize parsing in a small helper struct with typed accessors. **Secrets must come from environment variables, not YAML** — adopt this `PluginOptions` helper (it checks `ENV_PREFIX__KEY` before the YAML value and warns on plaintext secrets):

```rust
use std::collections::HashMap;
use streamling_plugin::PluginError;
use tracing::warn;

pub struct PluginOptions {
    options: HashMap<String, String>,
    env_prefix: String,
    plugin_name: String,
}

impl PluginOptions {
    pub fn new(options: HashMap<String, String>, plugin_name: &str, env_prefix: &str) -> Self {
        Self { options, env_prefix: env_prefix.to_string(), plugin_name: plugin_name.to_string() }
    }

    /// Required value: env var wins, then YAML, else error.
    pub fn get(&self, key: &str) -> Result<String, PluginError> {
        let env_key = format!("{}__{}", self.env_prefix, key.to_uppercase());
        if let Ok(v) = std::env::var(&env_key) { return Ok(v); }
        self.options.get(key).cloned().ok_or_else(|| {
            PluginError::Internal(format!("{}: required option '{}' is not set", self.plugin_name, key))
        })
    }

    /// Optional value with default.
    pub fn get_or(&self, key: &str, default: &str) -> String {
        let env_key = format!("{}__{}", self.env_prefix, key.to_uppercase());
        std::env::var(&env_key).ok().or_else(|| self.options.get(key).cloned())
            .unwrap_or_else(|| default.to_string())
    }

    /// Secret: env var only; warn if found in plaintext YAML.
    pub fn get_secret(&self, key: &str) -> Option<String> {
        let env_key = format!("{}__{}", self.env_prefix, key.to_uppercase());
        std::env::var(&env_key).ok().or_else(|| {
            self.options.get(key).map(|v| {
                warn!("{key} set in plaintext YAML — prefer env var {env_key}.");
                v.clone()
            })
        })
    }

    pub fn get_usize(&self, key: &str, default: usize) -> usize {
        self.options.get(key).and_then(|s| s.parse().ok()).unwrap_or(default)
    }
}
```
Convention: prefix is `STREAMLING__PLUGIN__<UPPER_NAME>` (e.g. `STREAMLING__PLUGIN__MY_SINK`). Parse numeric/bool options with `.parse()` and a sensible default — never `unwrap()` user input.

## Metrics & identity labels

`metrics_recorder: PluginMetricsRecorder` emits custom Prometheus series:

| Method | Series type |
|---|---|
| `record_count(name, u64)` / `record_count_w_tags(name, u64, vec![("k","v")])` | counter |
| `record_latency(name, Duration)` / `record_latency_w_tags(name, dur, tags)` | timing (ms) |
| `record_gauge(name, u64)` / `record_gauge_w_tags(name, u64, tags)` | gauge |

Metric dispatch is best-effort (channel `try_send`) — a full channel just logs a warning, never fails the batch.

**Identity labels** (`labels()` override on source/transform/sink) declare *what this instance is* and attach to every built-in metric the node emits. Derive them from options at construction:
```rust
fn labels(&self) -> Vec<PluginLabel> {
    vec![PluginLabel::new("dataset", self.dataset_name.clone())]
}
```
`PluginLabel::new(key, value)`. Keep keys Prometheus-legal and avoid the reserved built-ins (`topic`, `table`, `url`, `type`, `id`, `operator_type`, …).

## State backend

`state_backend_factory.create::<V>()` returns an `Arc<PluginStateBackend<V>>` for checkpointed, resumable state. `V: Serialize + Deserialize + Send + Sync + Clone + Debug + 'static`.

```rust
let state: Arc<PluginStateBackend<MyState>> = state_backend_factory.create();
let resume: Option<MyState> = state.get().await.map_err(PluginError::State)?;
state.put(MyState { cursor: 123 }).await.map_err(PluginError::State)?;
```
- `get()` / `put(v)` / `remove()` — single value under the default key (`{reference_name}`).
- `get_kv(k)` / `put_kv(k, v)` / `remove_kv(k)` — keyed values under `{prefix}:{k}`; `set_prefix(Some(""))` goes global.
- Persist durable progress in `process_checkpoint_finalizer`, restore in `initialize`. See the source skill for the full resume pattern.

## Errors

Two error enums:

```rust
// Construction-time (returned from new()):
pub enum PluginInitializationError {
    NotImplemented,
    Configuration(RString),   // bad options/config
    Execution(RString),
}

// Runtime (returned from trait methods):
pub enum PluginError {
    ArrowError(ArrowError),
    IoError(io::Error),
    Internal(String),         // config/setup/programming errors
    Execution(String),        // transient data-processing failures
    State(StateBackendError),
}
```
Rule of thumb: `Internal` for "this plugin is misconfigured / something is wrong with me," `Execution` for "this batch failed but the pipeline can retry." Map external errors with `.map_err(|e| PluginError::Internal(format!("..: {e}")))`.

## Minimal end-to-end skeleton (sink)

```rust
// src/sink.rs
use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, SinkPlugin};

pub struct PrintSink {
    schema: SchemaRef,
    running: Arc<AtomicBool>,
}

impl PrintSink {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state: PluginStateBackendFactory,
        _metrics: PluginMetricsRecorder,
        _options: HashMap<String, String>,
    ) -> Self {
        Self { schema, running: Arc::new(AtomicBool::new(true)) }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for PrintSink {
    fn is_running(&self) -> bool { self.running.load(Ordering::SeqCst) }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for PrintSink {
    async fn initialize(&self) -> Result<(), PluginError> { Ok(()) }
    async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if batch.num_rows() == 0 { return Ok(()); }
        tracing::info!(rows = batch.num_rows(), "received batch");
        Ok(())
    }
    async fn process_checkpoint_marker(&self, _e: CheckpointEpoch) -> Result<(), PluginError> { Ok(()) }
    async fn process_checkpoint_finalizer(&self, _e: CheckpointEpoch) -> Result<(), PluginError> { Ok(()) }
}
```

## Common mistakes

- **Wrong `new()` arity or order.** The macros call a fixed signature (see the table). A source that takes `schema` first, or a sink that omits it, won't compile.
- **Forgetting `init_plugin_with_async_runtime!()`.** Without it the module exports nothing loadable.
- **Panic in `process_batch`.** It crashes the worker thread. Return `Err(PluginError::..)` instead.
- **Plaintext secrets in YAML.** Use the `get_secret` env-var pattern.
- **`unwrap()` on user-supplied options.** Parse with defaults; reject bad config via `PluginInitializationError::Configuration`.
- **Doing durable work in `process_batch` instead of `process_checkpoint_finalizer`.** Breaks exactly-once — the finalizer is the commit point.

## Related skills

- [`streamling-source-plugin`](skill://streamling-source-plugin) — sources, `_gs_op`, resume, backpressure.
- [`streamling-sink-plugin`](skill://streamling-sink-plugin) — sinks, batching, retry, partial failure.
- [`streamling-transform-plugin`](skill://streamling-transform-plugin) — transforms, arrow compute.
- [`streamling-udf-plugin`](skill://streamling-udf-plugin) — custom SQL functions (DataFusion UDFs).
- [`streamling-advanced-plugins`](skill://streamling-advanced-plugins) — preprocessors, side outputs, multi-kind crates, low-level FFI.
