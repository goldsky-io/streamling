# Data Model: Arbitrary-Precision Decimal Type

**Plan**: [plan.md](./plan.md) — **Spec**: [spec.md](./spec.md) — **Research**: [research.md](./research.md)

This document specifies the in-memory and on-the-wire representation of the new type, the artifacts the engine introduces around it, and the validation rules each artifact enforces. It is the source of truth for the contracts in `contracts/`.

---

## E1. `DecimalArbType` — Arrow extension type

**Purpose**: Identify Arrow `Field`s that carry an arbitrary-precision decimal column.

**Identity**:
- Extension name: `streamling.decimal_arb`
- Arrow metadata key (on `Field`): `ARROW:extension:name = streamling.decimal_arb`
- Arrow metadata key (on `Field`): `ARROW:extension:metadata = {"precision": <u32>, "scale": <u32>}` (JSON-encoded string)

**Storage type**: `DataType::LargeBinary` (resolved by T006 spike on 2026-04-30; see research R2).

**Validation rules** (enforced at `Field` construction and at config load):
- `precision` is a positive `u32` and `precision <= MAX_PRECISION`. `MAX_PRECISION` is a constant defaulting to `65535`; not a hard product requirement, just a sanity guard documented in spec Assumptions.
- `scale` is a non-negative `u32` and `scale <= precision`.
- `precision > 76` (otherwise the column should remain on `Decimal256` per FR-015 — the type is **not** intended to be selected for narrower columns).

**Relationships**:
- Held on every `Field` whose declared precision exceeds 76, in the schema of every `RecordBatch` carrying such a column. Travels through Arrow IPC unchanged.
- A `RecordBatch` cannot mix `decimal_arb` and non-`decimal_arb` payloads in the same column (column type is single-typed).

---

## E2. `DecimalArbValue` — in-memory value

**Purpose**: Single arbitrary-precision decimal value used inside operator implementations.

**Backing**: `bigdecimal::BigDecimal` (per research R1).

**Invariants**:
- Always stored in **canonical** form (no leading zeros in magnitude; no trailing zeros that would imply a wider scale than declared on the column).
- Knows nothing about column-level `precision`/`scale` — those live on `DecimalArbType` (E1). Validation against column declarations happens at boundaries (read into `DecimalArbArray`, write to a sink, cast).

**Equality / hash / ordering**:
- Equality and hash compute from the canonical `BigDecimal` value (so `BigDecimal("123")` and `BigDecimal("0123")` are equal and hash equal).
- Total order: numeric `Ord` from `bigdecimal`. `+0` and `-0` are equal.

**Constructors**:
- `from_str(&str)` — canonical decimal string parser; rejects on bad input.
- `from_bigint_and_scale(BigInt, u32)` — used by Avro/Postgres byte decoders.
- `null()` — sentinel; `DecimalArbArray` represents NULL in the validity buffer, not the value.

---

## E3. `DecimalArbArray` — Arrow array

**Purpose**: Columnar container holding `DecimalArbValue`s for one column.

**Layout**:
- Underlying: `LargeBinaryArray` (resolved by T006 spike).
- Per-value bytes: `[sign_byte (0x00 = non-negative | 0xFF = negative)][big-endian two's-complement magnitude bytes]`.
- Validity: standard Arrow null bitmap.

**Builder API** (in `streamling-common/src/types/decimal_arb.rs`):
- `DecimalArbArrayBuilder::with_capacity(usize, precision: u32, scale: u32)`
- `append_str(&mut self, &str) -> Result<()>` — parses, validates against `(precision, scale)`, canonicalizes, appends.
- `append_value(&mut self, BigDecimal) -> Result<()>` — same as above but skips parsing.
- `append_null(&mut self)`
- `finish(self) -> DecimalArbArray`

**Validation at append**:
- `value.digits() <= precision` — else FR-013 error naming the column and value.
- `value.fractional_digit_count() <= scale` — same.
- Both are runtime checks; pre-row-flow validation is per FR-012 at config load.

**Conversions** (for FR-009 casts):
- `from_decimal128(&Decimal128Array, target_precision, target_scale) -> DecimalArbArray` — always succeeds (widening).
- `from_decimal256(&Decimal256Array, target_precision, target_scale) -> DecimalArbArray` — always succeeds.
- `to_decimal128(&self, target_precision, target_scale) -> Result<Decimal128Array>` — fails on out-of-range value with FR-013 error; rounds fractional excess per session rounding mode (R8 — half-to-even).
- `to_decimal256(...)` — analogous.
- `to_string_array() -> StringArray` — canonical decimal strings.
- `from_string_array(...)` — strict parse; fails on bad input.

---

## E4. `ConnectorCapabilityMatrix` — config-load entity

**Purpose**: At pipeline configuration load, decide for each `(column, connector)` pair whether the connector can carry the column losslessly. Drives FR-010, FR-011, FR-012.

**Shape**:

```rust
pub enum CapabilityResult {
    Native,                              // connector handles (precision, scale) directly
    OptInOnly(CoercionDirective),        // requires per-column opt-in (e.g., coerce_to: string)
    Reject(StreamlingError),             // pipeline must be rejected at config load
}

pub trait DecimalArbCapability {
    fn supports_decimal_arb(&self, precision: u32, scale: u32) -> CapabilityResult;
}
```

**Per-connector concrete entries** (registered when the connector is constructed):

| Connector | precision ≤76 | precision >76 | precision >76 with `coerce_to: string` |
|---|---|---|---|
| Postgres source/sink | (existing path; not this type) | `Native` | n/a |
| ClickHouse source/sink | (existing path) | `Reject` | `OptInOnly` (emit/consume as `String`) |
| Hybrid (CH-backed) | (existing path) | `Reject` | `OptInOnly` |
| Kafka source/sink, JSON encoding | (existing path) | `Native` (digit-string) | n/a |
| Kafka source/sink, Avro encoding | (existing path) | `Native` iff Avro `decimal` declared `bytes` ≥ ⌈precision·log₂10/8⌉; else `Reject` | n/a |
| Kafka source/sink, Protobuf encoding | (existing path) | `Reject` (no native decimal in proto3) | `OptInOnly` (string field) |
| SQS / webhook (JSON) | n/a (no decimals today) | `Native` | n/a |
| Plugin | (delegates to plugin) | delegates to plugin via FFI method | delegates to plugin |

**Lifecycle**: built once per pipeline at startup; consulted by the config validator before any rows flow. Errors include: column name, source-of-declaration (e.g., "Postgres source `orders.amount` declared NUMERIC(100, 18)"), connector identity ("ClickHouse sink `analytics`"), and the directive that would unblock the rejection ("set `coerce_to: string` on the column").

---

## E5. `CoercionTable` — DataFusion type-coercion entity

**Purpose**: Resolve mixed-operand expressions involving `decimal_arb` so native operators (`+`, `<`, etc.) plan correctly. Drives FR-016, FR-020.

**Shape**: a function `coerce(BinaryOp, lhs: &DataType, rhs: &DataType) -> Option<(DataType, Cast<lhs>, Cast<rhs>)>` registered with the session via `ExprPlanner`.

**Rules**:

| Operator class | LHS / RHS | Result type | Casts |
|---|---|---|---|
| Arithmetic (`+`, `-`, `*`, `/`, `%`) | `decimal_arb(p1, s1)` × `decimal_arb(p2, s2)` | `decimal_arb` widened per standard SQL decimal-arith rules (`+/-`: precision = `max(p1-s1, p2-s2) + max(s1, s2) + 1`, scale = `max(s1, s2)`; `*`: precision = `p1 + p2 + 1`, scale = `s1 + s2`; `/`: precision = `p1 - s1 + s2 + max(s1, default_div_scale)`, scale = `max(s1, default_div_scale)`) | none |
| Arithmetic | `decimal_arb` × `Decimal128`/`Decimal256` | `decimal_arb` widened as above | RHS cast to `decimal_arb` |
| Arithmetic | `decimal_arb` × `Int*` / `Float*` | `decimal_arb` widened as above (integer treated as `decimal_arb(digits, 0)`; float rejected with a clear error per FR-013 unless explicitly cast — float ↔ decimal is lossy) | RHS cast |
| Comparison (`=`, `<`, etc.) | `decimal_arb` × any numeric | Boolean | both operands cast to a common `decimal_arb` (no float auto-cast — error if RHS is float) |
| Concat / string ops | `decimal_arb` × `Utf8` | `Utf8` | LHS cast to canonical decimal string |

**`default_div_scale`**: 18 (matches typical financial conventions; documented in spec Assumptions about rounding/precision).

---

## E6. Aggregate output schema

For each aggregate over a `decimal_arb` input column with declared `(p, s)`:

| Aggregate | Output type | Rule |
|---|---|---|
| `SUM` | `decimal_arb(p + 16, s)` | adds 16 digits headroom — supports up to ~10¹⁶ rows without overflow on the worst-case input; if a pipeline needs more, the assumption is documented. Cap at `MAX_PRECISION`. |
| `AVG` | `decimal_arb(p + 1, s + 1)` | matches Postgres `AVG(numeric)` widening |
| `MIN` | `decimal_arb(p, s)` | identity |
| `MAX` | `decimal_arb(p, s)` | identity |
| `COUNT` | `Int64` | identity |

These widening rules are encoded in `decimal_arb_aggregates.rs` and surfaced by the AggregateUDFImpl's `state_type`/`return_type` methods.

---

## State transitions

The type itself has no lifecycle — values are immutable. The only "transition" is at boundary conversions (E3 conversion methods), governed by the validation rules above.
