# Contract: Connector Capability

**Spec**: [../spec.md](../spec.md) FR-010, FR-011, FR-012, FR-018, FR-019 — **Plan**: [../plan.md](../plan.md) — **Data model**: [../data-model.md](../data-model.md) (E4)

This contract defines how each connector advertises its ability to carry a `decimal_arb` column. The pipeline configuration validator consults this at startup; pipelines whose declared schemas exceed any connector's capability are rejected before any rows flow.

## Trait

```rust
pub enum CapabilityResult {
    /// The connector handles (precision, scale) directly with no loss.
    Native,
    /// The connector cannot natively hold (precision, scale), but the user
    /// has set an explicit per-column opt-in directive. Carries the directive
    /// (e.g. CoercionDirective::CoerceToString) the connector will use.
    OptInOnly(CoercionDirective),
    /// The connector cannot carry this column. Pipeline is rejected at config load.
    /// The error MUST name the column, the connector, the declared (precision, scale),
    /// and an actionable suggestion (e.g. "set coerce_to: string on this column").
    Reject(StreamlingError),
}

pub trait DecimalArbCapability {
    fn supports_decimal_arb(
        &self,
        precision: u32,
        scale: u32,
        column_directives: &ColumnDirectives,
    ) -> CapabilityResult;
}

pub enum CoercionDirective {
    CoerceToString,
}
```

`ColumnDirectives` is the per-column YAML annotation block (per `contracts/yaml-schema.md`). The connector inspects `column_directives.coerce_to` to decide whether `OptInOnly` applies.

## Per-connector behavior

### Postgres source / sink

```rust
fn supports_decimal_arb(precision, scale, _) -> CapabilityResult {
    // Postgres NUMERIC has unbounded precision (subject to a 1000-digit
    // implementation cap which is well above MAX_PRECISION's practical use).
    if precision <= MAX_POSTGRES_NUMERIC_PRECISION { Native }
    else { Reject(...) }
}
```

`MAX_POSTGRES_NUMERIC_PRECISION = 1000` (Postgres's documented practical limit). Above that, reject with: "Postgres NUMERIC supports up to 1000 digits; column `<col>` declares precision <p>".

### ClickHouse source / sink

```rust
fn supports_decimal_arb(precision, _scale, dirs) -> CapabilityResult {
    if precision <= 76 { /* unreachable: caller would have used Decimal128/256 */ }
    if dirs.coerce_to == Some(CoerceToString) {
        OptInOnly(CoerceToString)  // emit/consume ClickHouse String column
    } else {
        Reject(error_with_hint("set `coerce_to: string` on this column or reduce declared precision to ≤76"))
    }
}
```

This is the change of behavior from the silent `String` fallback at `crates/streamling-connectors/src/table_providers/clickhouse.rs:1972-1992`, which is removed.

### Hybrid (ClickHouse-backed)

Same logic as ClickHouse (delegates to the same trait impl); `crates/streamling-connectors/src/table_providers/hybrid.rs:1083-1091` is updated.

### Kafka source / sink — JSON encoding

```rust
fn supports_decimal_arb(_precision, _scale, _) -> CapabilityResult {
    Native  // digit-string in JSON; arbitrary precision
}
```

### Kafka source / sink — Avro encoding

```rust
fn supports_decimal_arb(precision, scale, _) -> CapabilityResult {
    let bytes_needed = ((precision as f64) * 3.322).ceil() as usize / 8 + 1;
    let avro_bytes = avro_field.declared_bytes_or_unbounded();
    if avro_bytes.is_unbounded() || avro_bytes.fits(bytes_needed) {
        Native
    } else {
        Reject(error_with_hint("Avro field declares <bytes> bytes; <bytes_needed> required for declared precision <p>"))
    }
}
```

### Kafka source / sink — Protobuf encoding

```rust
fn supports_decimal_arb(_precision, _scale, dirs) -> CapabilityResult {
    if dirs.coerce_to == Some(CoerceToString) {
        OptInOnly(CoerceToString)
    } else {
        Reject(error_with_hint("Protobuf has no native decimal type; set `coerce_to: string` to encode as a string field"))
    }
}
```

### SQS / webhook (JSON-encoded payloads)

Same as Kafka JSON — `Native`.

### Plugin

```rust
fn supports_decimal_arb(precision, scale, dirs) -> CapabilityResult {
    // Delegates to the plugin's FFI method; default impl returns Reject.
    self.plugin_handle.supports_decimal_arb(precision, scale, dirs)
}
```

The plugin FFI ABI gains one method (research R10):

```rust
extern "C" fn supports_decimal_arb(
    self: &Self,
    precision: u32,
    scale: u32,
    coerce_to_string: bool,
) -> RCapabilityResult;
```

Default impl in the abi_stable trait returns `Reject` — existing plugins that don't override are unaffected and continue to work for non-decimal_arb columns.

## Error message contract

Every `Reject` MUST include:
- `column`: fully-qualified column name (e.g., `pipeline.sources.orders.amount`)
- `connector`: connector name and kind (e.g., `clickhouse sink "analytics"`)
- `declared`: declared `(precision, scale)` from the source-of-record
- `reason`: human-readable reason
- `hint`: a YAML edit (or the destination type-system limitation to address) that would unblock the rejection

Example:

```
config error: column `pipeline.sinks.analytics.amount` (declared decimal_arb(100, 18))
cannot be emitted to ClickHouse sink `analytics`: ClickHouse Decimal precision is capped at 76.
hint: add `coerce_to: string` under this column in the sink YAML to emit as a String column,
      or reduce declared precision to ≤76 if the source data fits.
```

Satisfies FR-012 (config-load rejection with clear, actionable error).
