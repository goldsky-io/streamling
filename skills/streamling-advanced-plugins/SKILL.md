---
name: streamling-advanced-plugins
description: Use when implementing a streamling preprocessor (rewriting pipeline topology YAML before it parses), a DataFusion scalar UDF usable in SQL, a side output (observing every source without joining the pipeline), registering many component kinds in one crate, or dropping below the registration macros to the low-level manual FFI plugin module. Assumes streamling-plugin-basics.
---

# streamling advanced plugin kinds — Agent Skill

Beyond sources, transforms, and sinks, a plugin crate can register three more component kinds — **preprocessors**, **UDFs**, and **side outputs** — and can mix all six in one `cdylib`. This skill also covers the **low-level manual FFI** escape hatch for when the registration macros don't fit.

**Prerequisite:** [`streamling-plugin-basics`](skill://streamling-plugin-basics).

## Preprocessors — rewrite topology YAML before parsing

A preprocessor transforms the **raw topology config string** (the pipeline YAML) before streamling parses it. Use it to inject default options, expand shorthand dataset names into concrete source configs, or enforce per-source conventions — without touching streamling's core config parser.

```rust
use async_trait::async_trait;
use std::collections::HashMap;
use streamling_plugin::api::PluginError;
use streamling_plugin::{PluginInitializationError, PreprocessorPlugin};

pub struct DefaultsPreprocessor {
    default_region: String,
}

impl DefaultsPreprocessor {
    /// Constructor takes ONLY options (no schema/rt/state/metrics).
    pub fn new(options: HashMap<String, String>) -> Result<Self, PluginInitializationError> {
        Ok(Self {
            default_region: options.get("region").cloned().unwrap_or_else(|| "us-east-1".into()),
        })
    }
}

#[async_trait]
impl PreprocessorPlugin for DefaultsPreprocessor {
    async fn preprocess_topology(&self, config: String) -> Result<String, PluginError> {
        // Parse, mutate, re-serialize. serde_yaml is the usual tool.
        let mut doc: serde_yaml::Value = serde_yaml::from_str(&config)
            .map_err(|e| PluginError::Internal(format!("parse topology: {e}")))?;

        if let Some(sinks) = doc.get_mut("sinks").and_then(|v| v.as_mapping_mut()) {
            for (_, sink) in sinks.iter_mut() {
                if let Some(map) = sink.as_mapping_mut() {
                    let key = serde_yaml::Value::String("region".into());
                    map.entry(key).or_insert_with(|| serde_yaml::Value::String(self.default_region.clone()));
                }
            }
        }

        serde_yaml::to_string(&doc).map_err(|e| PluginError::Internal(format!("emit topology: {e}")))
    }
}
```

Register and **order matters** — preprocessors run in registration order, and later ones see earlier ones' output:
```rust
register_plugin_preprocessor!("my_plugin", "defaults", DefaultsPreprocessor);
```

**Discipline:**
- **Never overwrite a user-set field.** Check existence first (`entry().or_insert(..)` / `contains_key`) and only fill gaps.
- Preprocessors are async but should stay fast and deterministic — they run once at pipeline load.
- Parse failures must become `PluginError::Internal`, never panics.

## UDFs — custom functions usable in SQL

A **UDF** is a DataFusion scalar function your plugin exposes so pipeline SQL can call it (`SELECT my_func(col) FROM ...`) — the lightest way to add a derived column. It implements `ScalarUDFImpl` and registers with `register_plugin_udf!(MyFunc)` (or `register_plugin_udf_fn!(create_fn)` for the legacy `create_udf` closure form).

**Full coverage — the modern `invoke_with_args` / `ScalarFunctionArgs` API, a complete worked example, argument helpers, `return_type` vs `return_field_from_args`, volatility, and the legacy path — lives in its own skill:** [`streamling-udf-plugin`](skill://streamling-udf-plugin).

## Side outputs — observe every source

A **side output** observes data from **all sources** without joining the pipeline or modifying records — e.g. a monitor that logs batch counts, tracks a high-water mark, or emits a heartbeat. Unlike other kinds, side outputs use **direct FFI invocation** (no channels), and **one instance is created per source** (the macro manages a `HashMap<source_name, instance>`).

```rust
use arrow::array::RecordBatch;
use streamling_plugin::SideOutputPlugin;

pub struct MonitorSideOutput;

impl SideOutputPlugin for MonitorSideOutput {
    fn process_batch(&self, batch: &RecordBatch) -> Result<(), String> {
        tracing::info!(rows = batch.num_rows(), "observed source batch");
        Ok(())
    }
    fn shutdown(&self) { tracing::info!("side output shutting down"); }
}
```

Register with an id:
```rust
register_plugin_side_output!("monitor", MonitorSideOutput);
```

Note: `SideOutputPlugin` is sync and returns `Result<(), String>` (a plain `String` error, not `PluginError`). Instances are auto-registered against every source; you don't wire a `from:`.

## Registering many kinds in one crate

A single `cdylib` can register sources, transforms, sinks, preprocessors, UDFs, and side outputs together — this is how production plugin crates are structured. Group them by `mod`, then list every macro call above the **single** `init_plugin_with_async_runtime!()`:

```rust
mod sinks; mod sources; mod transforms; mod udfs; mod preprocessors;

use crate::preprocessors::DefaultsPreprocessor;
use crate::sinks::{MySink, QueueSink};
use crate::sources::ApiSource;
use crate::transforms::FilterTransform;
use crate::udfs::BytesToHexFunc;

use streamling_plugin::{
    init_plugin_with_async_runtime, register_plugin_preprocessor, register_plugin_sink,
    register_plugin_source, register_plugin_transform, register_plugin_udf,
    set_plugin_output_buffer,
};

register_plugin_source!("my_plugin", "api_source", ApiSource);
set_plugin_output_buffer!("my_plugin.api_source", 1);

register_plugin_transform!("my_plugin", "filter", FilterTransform);
set_plugin_output_buffer!("my_plugin.filter", 1);

register_plugin_sink!("my_sink", MySink);
register_plugin_sink!("queue_sink", QueueSink);

register_plugin_preprocessor!("defaults", DefaultsPreprocessor);
register_plugin_udf!(BytesToHexFunc);

init_plugin_with_async_runtime!();
```

Rules:
- **Exactly one** `init_plugin_with_async_runtime!()`, at the bottom, regardless of how many components you registered.
- `set_plugin_output_buffer!`/`set_plugin_input_buffer!` are optional, per-component, and take the **full** `plugin.id`.
- Keep each component in its own module/file; only the registration list lives in `lib.rs`.

## Low-level manual FFI (escape hatch)

When the registration macros can't express what you need (a component constructed outside the standard closure, custom channel wiring), drop to the raw `PluginModule` ABI. This is advanced — prefer the macros whenever they fit. Shape (from the `low_level` example):

```rust
mod sink;

use abi_stable::std_types::{RNone, ROption, RResult, RString, RVec};
use abi_stable::{export_root_module, prefix_type::PrefixTypeTrait};
use streamling_plugin::api::PluginStateBackendFactory;
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::SafeArrowSchema;
use streamling_plugin::{
    PluginChannels, PluginInitializationError, PluginLogging, PluginModule, PluginModuleRef,
    PluginOptions, PluginResult, PluginRuntimeConfiguration, PluginUdfDescriptor,
    PluginSideOutputDescriptor, RootModule, sink_generator,
};
use streamling_plugin::IntoSinkPluginResult;

use crate::sink::MySink;

extern "C" fn init(logging: PluginLogging) -> RResult<PluginRuntimeConfiguration, PluginInitializationError> {
    logging.init_logger();
    RResult::ROk(PluginRuntimeConfiguration { /* ... */ })
}

extern "C" fn create(
    plugin_id: RString,
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: streamling_plugin::PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    let schema: arrow_schema::SchemaRef = input_schema.into_rust().unwrap().into();  // for sinks/transforms
    sink_generator(
        plugin_id,
        |schema, rt, state, metrics, opts| {
            IntoSinkPluginResult::into_sink_result(MySink::new(schema, rt, state, metrics, opts))
        },
        options,
        runtime,
        state_backend_config,
        message_channels,
    )
}

extern "C" fn udf_descriptors() -> RResult<RVec<PluginUdfDescriptor>, PluginInitializationError> {
    RResult::ROk(RVec::new())
}
extern "C" fn side_output_descriptors() -> RResult<RVec<PluginSideOutputDescriptor>, PluginInitializationError> {
    RResult::ROk(RVec::new())
}

#[export_root_module]
pub fn get_module() -> PluginModuleRef {
    PluginModule::new(init, create, udf_descriptors, side_output_descriptors)
}
```

Use the low-level path only when you must hand-route `create` or channel handling; otherwise the macros generate this boilerplate correctly and keep you ABI-safe.

## Common mistakes

- **Preprocessor overwrites a user field.** Always check before writing; users must be able to override your defaults.
- **UDF `invoke` panics on bad arity/types.** Validate and return a `DataFusionError`.
- **Side output returns `PluginError`.** It returns `Result<(), String>` — different signature from the other kinds.
- **Two `init_plugin_with_async_runtime!()` calls, or none.** Exactly one per crate.
- **Reaching for low-level FFI when a macro works.** The macros handle ABI-stability details you can easily get wrong by hand.

## Related skills

- [`streamling-plugin-basics`](skill://streamling-plugin-basics) — crate setup, registration, lifecycle, options/secrets, errors.
- [`streamling-source-plugin`](skill://streamling-source-plugin) / [`streamling-sink-plugin`](skill://streamling-sink-plugin) / [`streamling-transform-plugin`](skill://streamling-transform-plugin) — the main three kinds.
- [`streamling-udf-plugin`](skill://streamling-udf-plugin) — custom SQL functions via DataFusion `ScalarUDFImpl`.
- [`sql-transform`](skill://sql-transform) — SQL transform, the no-code alternative to a transform plugin or a UDF.
