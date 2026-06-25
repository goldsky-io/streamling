---
name: streamling-udf-plugin
description: Use when implementing a streamling DataFusion scalar UDF — a custom function callable from pipeline SQL (SELECT my_func(col) FROM ...). Covers the ScalarUDFImpl trait, the modern invoke_with_args / ScalarFunctionArgs API, argument validation and downcasting, null propagation, return_type vs return_field_from_args, volatility, both registration macros, and the create_udf legacy path. Assumes streamling-plugin-basics.
---

# streamling UDF plugin — Agent Skill

A **UDF** (user-defined function) is a DataFusion scalar function your plugin exposes, so pipeline SQL can call it: `SELECT truncate(body, 280) FROM stream`. It is the lightest way to add a derived column — no batch pipeline of your own, no `process_batch`. DataFusion calls your function inside its plan.

**Prerequisites:** [`streamling-plugin-basics`](skill://streamling-plugin-basics) (crate setup, registration). To *use* the UDF once registered, see [`sql-transform`](skill://sql-transform).

**API version:** these skills target DataFusion **49** / Arrow **55** (the `invoke_with_args` + `ScalarFunctionArgs` era). If your `Cargo.toml` pins an older DataFusion, the method may be `invoke(&self, args: Vec<ColumnarValue>)` instead — adapt accordingly.

## Two ways to author a UDF

| Form | Register with | When |
|---|---|---|
| `struct MyFunc impl ScalarUDFImpl` (+ `Default`) | `register_plugin_udf!(MyFunc)` | Default — type-safe, structured, the modern recommendation |
| closure via `create_udf(..)` returning a `ScalarUDF` | `register_plugin_udf_fn!(create_my_func)` | Legacy / quick — a `fn() -> ScalarUDF` factory |

Both live alongside any source/sink/transform/preprocessor in the same `cdylib`.

## A complete UDF: `truncate(text, max_len)`

`truncate(text: Utf8, max_len: Int64) -> Utf8` — clip a string to `max_len` characters, char-boundary safe. Null text or null/egative limit → null.

```rust
use arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringBuilder};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct TruncateFunc {
    signature: Signature,
}

impl Default for TruncateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TruncateFunc {
    pub fn new() -> Self {
        // Exact signature: two arguments, fixed types, pure function.
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TruncateFunc {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { "truncate" }                       // the SQL name you call

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    /// Preferred on modern DataFusion: declare the nullable output field.
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    /// Modern entry point. `args.args` is a `Vec<ColumnarValue>`.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let inputs = args.args;
        if inputs.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "{} expects exactly 2 arguments, got {}",
                self.name(),
                inputs.len()
            )));
        }
        let text = columnar_to_array(&inputs[0])?;
        let max_len = columnar_to_array(&inputs[1])?;

        let text = text
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan(format!("{}: arg 0 must be Utf8", self.name())))?;
        let max_len = max_len.as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Plan(format!("{}: arg 1 must be Int64", self.name())))?;

        let n = text.len().max(max_len.len());
        let mut out = StringBuilder::with_capacity(n, n * 8);
        for i in 0..n {
            let t = (i < text.len() && !text.is_null(i)).then(|| text.value(i));
            let m = (i < max_len.len() && !max_len.is_null(i)).then(|| max_len.value(i));
            match (t, m) {
                (Some(s), Some(m)) if m >= 0 => {
                    let end = usize::try_from(m).unwrap_or(0).min(s.chars().count());
                    out.append_value(s.chars().take(end).collect::<String>());
                }
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Column arguments arrive as `ColumnarValue::Array`; a literal (scalar) needs
/// broadcasting to the batch length. `ScalarValue::to_array_of_size(len)` does that.
fn columnar_to_array(cv: &ColumnarValue) -> Result<ArrayRef> {
    match cv {
        ColumnarValue::Array(a) => Ok(a.clone()),
        ColumnarValue::Scalar(s) => Ok(s.to_array_of_size(1)?),
    }
}
```

## Register it

```rust
register_plugin_udf!(TruncateFunc);   // requires TruncateFunc: ScalarUDFImpl + Default
```

You do **not** call `init_plugin_with_async_runtime!()` per UDF — one call per crate, at the bottom of `lib.rs`, covers every component.

## Use it in a pipeline

Once the plugin cdylib is on `STREAMLING__PLUGIN__PATH`, the function is callable in any `type: sql` transform:

```yaml
transforms:
  clipped:
    type: sql
    primary_key: id
    sql: |
      SELECT id, truncate(body, 280) AS snippet FROM posts
```

No `from:` on the SQL transform; `posts` must be an existing source/transform name. See [`sql-transform`](skill://sql-transform).

## The `invoke_with_args` API in detail

```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue>;
```

- `args.args: Vec<ColumnarValue>` — one entry per call argument, in declared order.
- `ColumnarValue::Array(ArrayRef)` is the normal case (a column); `ColumnarValue::Scalar(ScalarValue)` appears for literals — broadcast with `to_array_of_size(len)`.
- Return `ColumnarValue::Array(Arc::new(your_array))` for column output, or `ColumnarValue::Scalar(..)` for a single value broadcast across the batch.

**Validate, don't panic.** Arity and type checks must return `Err(DataFusionError::Plan(..))`. A panic inside `invoke_with_args` aborts the whole query.

### Argument helpers (idiomatic toolkit)

Production UDF crates factor these into a `util` module. Adopt the same shape:

```rust
// util.rs
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use arrow::array::{Array, ArrayRef};

pub fn validate_arg_count(args: &ScalarFunctionArgs, min: usize, max: Option<usize>, name: &str) -> Result<()> {
    let n = args.args.len();
    if n < min || max.map_or(false, |mx| n > mx) {
        return Err(DataFusionError::Plan(format!("{name}: expected {min:?}..{max:?} args, got {n}")));
    }
    Ok(())
}

pub fn get_array_from_arg(arg: &ColumnarValue) -> Result<ArrayRef> {
    match arg {
        ColumnarValue::Array(a) => Ok(a.clone()),
        ColumnarValue::Scalar(s) => Ok(s.to_array_of_size(1)?),
    }
}

/// One typed array argument: validates arity (1) + downcasts in one call.
pub fn get_single_arg<T: Array + 'static>(args: &ScalarFunctionArgs, name: &str, type_label: &str) -> Result<std::sync::Arc<T>> {
    validate_arg_count(args, 1, Some(1), name)?;
    let a = get_array_from_arg(&args.args[0])?;
    a.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Plan(format!("{name}: argument must be {type_label}"))
    }).map(std::sync::Arc::clone) // note: clone the Arc, not the array
}
```

Then `invoke_with_args` shrinks to:
```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let text = get_single_arg::<StringArray>(&args, self.name(), "Utf8")?;
    // ...
}
```

> Note the downcast pitfall: `downcast_ref::<T>()` borrows; to hand back an `Arc<T>` you clone the **`Arc`**, never the underlying array.

## Return type

Implement **both** on modern DataFusion:
- `return_type(&self, arg_types) -> Result<DataType>` — legacy planner path.
- `return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef>` — preferred; lets you declare nullability and depend on input nullability. Return a `Field::new(name, dtype, nullable)`.

Keep them consistent (same `DataType`). For a fixed output type, both just return `Utf8`/`Int64`/etc.; for variable-shape outputs (e.g. `List<Struct<...>>`), build the nested `DataType`/`Field` once and reuse.

## Volatility

Set it in the `Signature`:
- **`Immutable`** — pure function of its inputs (default; planner may cache/constant-fold). Almost all UDFs.
- **`Stable`** — same result within a query but varies across (e.g. depends on session timezone).
- **`Volatile`** — may differ row-to-row (e.g. reads external state, network, `now()`). Disables caching; use sparingly.

## Nulls and allocation

- **Propagate nulls explicitly.** Check `arr.is_null(i)` each row and append a null to the output builder (`append_null()` / `append(false)` for nullable list entries). DataFusion does not auto-propagate.
- **Build, don't push.** Operate on whole arrays with builders (`StringBuilder`, `PrimitiveBuilder`, `ListBuilder`, `StructBuilder`), pre-sized with `with_capacity` — far cheaper than growing `Vec<String>` and converting.
- **Char-safe string ops.** Slice by `chars()`, never byte indices, to avoid UTF-8 boundary panics.

## The legacy `create_udf` path

When you'd rather write a closure than a struct:

```rust
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use arrow_schema::DataType;

pub fn create_double_udf() -> ScalarUDF {
    create_udf(
        "double",
        vec![DataType::Int64],            // input types
        DataType::Int64,                  // return type
        Volatility::Immutable,
        std::sync::Arc::new(|args: &[ColumnarValue]| {
            // older signature: closure takes &[ColumnarValue]
            use arrow::array::Int64Array;
            use datafusion::common::Result;
            use std::sync::Arc;
            let col = args[0].clone();
            let arr = col.into_array(1)?;
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            let out: Int64Array = a.iter().map(|v| v.map(|x| x * 2)).collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        }),
    )
}
```
Register with the factory form:
```rust
register_plugin_udf_fn!(create_double_udf);   // fn() -> ScalarUDF
```
Prefer the `ScalarUDFImpl` struct form for new code — it's typed, testable, and tracks the modern API.

## Common mistakes

- **Using the old `invoke(&self, args: Vec<ColumnarValue>)`.** On DataFusion 49+ it's `invoke_with_args(&self, args: ScalarFunctionArgs)`; access arguments via `args.args`.
- **Panicking on bad arity/types.** Return `DataFusionError::Plan(..)`; a panic aborts the query.
- **Forgetting null propagation.** Null inputs must yield null outputs — check `is_null(i)` per row.
- **Slicing strings by byte index.** Use `chars()` to stay UTF-8-safe.
- **Inconsistent `return_type` vs `return_field_from_args`.** They must agree on the `DataType`.
- **Marking a deterministic function `Volatile`.** It disables planner optimizations unnecessarily.

## Related skills

- [`streamling-plugin-basics`](skill://streamling-plugin-basics) — crate setup, registration, the single `init_plugin_with_async_runtime!()` per crate.
- [`sql-transform`](skill://sql-transform) — how to call your UDF from pipeline SQL.
- [`streamling-transform-plugin`](skill://streamling-transform-plugin) — when a UDF isn't enough and you need full `process_batch` control.
- [`streamling-advanced-plugins`](skill://streamling-advanced-plugins) — registering UDFs alongside other kinds in one crate.
