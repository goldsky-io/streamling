# Data Model — Retire U256/I256

This document captures the data-shape changes this feature introduces. There are no new entities — the change is the introduction of one optional metadata key on existing `decimal_arb` fields, plus the routing-table changes that consume it.

---

## E1. `decimal_arb` field metadata, extended

A `decimal_arb` field is an Arrow `Field` of `DataType::LargeBinary` whose metadata map carries the existing extension-type keys (from feature 001):

```text
ARROW:extension:name      = "streamling.decimal_arb"
ARROW:extension:metadata  = '{"precision":<u32>,"scale":<u32>}'
```

This feature **adds** one optional sibling key:

```text
streamling.native_int_kind = "u256" | "i256"     (optional)
```

### Field-level semantics

- **Identity**: `(extension:name, precision, scale)` — unchanged by this feature.
- **Operations**: All `decimal_arb_*` UDFs, the ExprPlanner, the sort optimizer, the aggregate UDAFs operate identically regardless of `native_int_kind`. The hint is *not* part of the type's operational identity.
- **Propagation through transforms — current behavior**: every `decimal_arb_*` binary-op UDF's `return_field_from_args` builds its output field via `DecimalArbType::field(name, p, s, true)`, which produces a fresh field with the extension keys and **no `native_int_kind` hint**. So *all* derived columns — same-hint inputs, mixed-hint inputs, hinted + plain inputs — drop the hint. The defensible reading is "the result is a new value, not the original ClickHouse `UInt256` bytes, so the round-trip guarantee no longer applies; fall back to the safe `Decimal(p, s)` sink emission." A consequence: `SELECT amount + 0 AS amount FROM src` causes a downstream ClickHouse sink to emit `Decimal(78, 0)` rather than `UInt256`, even though the value is identical. Identity-projection (`SELECT amount FROM src`) preserves the field metadata as-is, so identity-only transforms keep the hint. This behavior is locked by unit tests in `decimal_arb_ops::tests` (`add_drops_native_int_kind_when_*`).
- **Conflict on mixed operands**: same as above — the output has no hint, which happens to be the right answer for mixed-hint inputs (ambiguous origin → no native channel).

A future enhancement could teach `build_output_field` to propagate the hint when both inputs agree, restoring the round-trip guarantee through pass-through arithmetic. That's out of scope here; the current hint-dropping behavior is the documented contract.

### Connector-level semantics

`native_int_kind` is a *hint about the column's origin*, **not** a *constraint on its runtime values*. Specifically:

- A `native_int_kind=u256` column whose actual value happens to be negative MUST be detected and rejected by any sink that has a matching unsigned native channel — emit a clear "value-out-of-range for declared native_int_kind=u256" error. (This can happen when arithmetic on a u256-origin column produces a negative result, e.g. `amount - other_amount` where `other_amount > amount`.) Sinks without a matching native channel (Postgres, Kafka, generic ClickHouse `Decimal`) MAY emit such a value normally.

- Sinks **MUST NOT** mutate the hint when emitting. If they cannot honor the hint (e.g. a Postgres NUMERIC sink has no native UInt256), they emit using their normal `decimal_arb` path; the hint is observation, not coercion.

### Helpers

The `DecimalArbType` impl in `crates/streamling-common/src/types/decimal_arb.rs` gains three methods:

| Method | Purpose |
|---|---|
| `fn with_native_int_kind(field: Field, kind: NativeIntKind) -> Field` | Stamp the hint onto a decimal_arb field |
| `fn native_int_kind_from_field(field: &Field) -> Option<NativeIntKind>` | Read the hint back |
| `enum NativeIntKind { U256, I256 }` | New enum (sibling to `ConnectorKind` in the capability matrix) |

These are not part of the ExprPlanner / UDF surface — they're only consulted by:
- Source-side schema annotation (sets the hint)
- Sink-side wire-format adapters (reads the hint)
- The connector capability matrix (consults the hint to decide Native vs OptIn)

---

## E2. Source-side type routing table

| Source connector | Inbound shape | Arrow Output (today) | Arrow Output (after this feature) |
|---|---|---|---|
| Kafka Avro | `decimal(p, 0)`, `77 ≤ p ≤ 78` | `FixedSizeBinary(32)` + `U256Type` metadata (all `p > 76` historically) | `LargeBinary` + decimal_arb extension metadata `(p, 0)` + `native_int_kind=u256` |
| Kafka Avro | `decimal(p, 0)`, `p > 78` | `FixedSizeBinary(32)` + `U256Type` metadata (silently overflowed; UInt256 fits ≤ 78 digits) | `LargeBinary` + decimal_arb `(p, 0)` (no hint — lossless via `Decimal(p, 0)`) |
| Kafka Avro | `decimal(p, s)`, `p > 76, s > 0` | decimal_arb `(p, s)` (today) | decimal_arb `(p, s)` (unchanged, no hint) |
| Kafka Avro | `decimal(p, s)`, `p ≤ 76` | `Decimal128`/`Decimal256` (today) | unchanged |
| Postgres | `NUMERIC(78, 0)` | `FixedSizeBinary(32)` + U256 metadata | `LargeBinary` + decimal_arb `(78, 0)` + `native_int_kind=u256` |
| Postgres | `NUMERIC(p, s)`, `p > 76, s ≥ 0` | decimal_arb `(p, s)` (today) | decimal_arb `(p, s)` (unchanged, no hint) |
| Postgres | `NUMERIC(p, s)`, `p ≤ 76` | `Decimal128`/`Decimal256` | unchanged |
| ClickHouse | `UInt256` | `FixedSizeBinary(32)` (no hint stamped) | unchanged — see note below |
| ClickHouse | `Int256` | `FixedSizeBinary(32)` (no hint stamped) | unchanged — see note below |
| ClickHouse | `Decimal(p, s)`, `p > 76` | decimal_arb `(p, s)` | unchanged |
| ClickHouse | `Decimal(p, s)`, `p ≤ 76` | `Decimal128`/`Decimal256` | unchanged |

> **Note (ClickHouse source-side hint stamping is deferred)**: an earlier draft of this row asserted that today's ClickHouse source emits `FixedSizeBinary(32)` *with* `U256Type` / `I256Type` metadata, and that this feature would re-stamp it as `decimal_arb` + `native_int_kind`. Investigation under T009 (`tasks.md`) showed that neither `clickhouse.rs::fetch_schema` nor `normalize_schema_for_clickhouse` actually stamp that metadata today — a `UInt256` / `Int256` source column lands in the pipeline as a plain `FixedSizeBinary(32)` with no provenance. Building the source-side `system.columns` lookup that stamps `native_int_kind` is a meaningful new feature, deferred to a follow-up. This means a ClickHouse `UInt256` source paired with a ClickHouse `UInt256` sink does **not** round-trip natively today (it lands at the sink as `FixedString(32)` unless an explicit `schema_override` reasserts `UInt256`). This is pre-existing behavior, not a regression introduced or fixed by this feature. The Avro and Postgres source-side hint paths land in this release (those rows above).

---

## E3. Sink-side wire-format routing table

| Sink connector | Streamling column | Hint | Emitted as |
|---|---|---|---|
| ClickHouse | `decimal_arb(78, 0)` | `u256` | `UInt256` |
| ClickHouse | `decimal_arb(p, 0)` (any p) | `i256` | `Int256` |
| ClickHouse | `decimal_arb(p, s)` | absent, `p ≤ 76` | `Decimal(p, s)` (Native) |
| ClickHouse | `decimal_arb(p, s)` | absent, `p > 76`, `coerce_to=string` | `String` (OptInOnly) |
| ClickHouse | `decimal_arb(p, s)` | absent, `p > 76`, no opt-in | **Reject at config load** (FR-012 from feature 001) |
| Postgres | `decimal_arb(p, s)` | any | `NUMERIC(p, s)` (always Native; hint is ignored) |
| Kafka JSON | `decimal_arb(p, s)` | any | digit-string JSON literal (hint ignored) |
| Kafka Avro | `decimal_arb(p, s)` | any | Avro `decimal(p, s)` logical type (hint ignored) |
| Kafka Protobuf | `decimal_arb(p, s)` | any | `coerce_to: string` required (hint ignored) |

The hint *only* changes ClickHouse (and Hybrid) sink behavior — for all other connectors it's a no-op. This is the minimal surface change to preserve existing ClickHouse table-schema compatibility (US4) without complicating the connector matrix elsewhere.

---

## E4. Migration state shape

No new state-record format and no state migration. Streamling pipeline checkpoints record **source-side offsets only** — they do not carry the Arrow schema of in-flight data.

On restart under the post-migration streamling:

1. The pipeline reads the stored offset from the state backend.
2. It resumes consuming from the source at that offset.
3. The source decodes each record per its wire schema (Avro `decimal(p, 0)` bytes, Postgres `NUMERIC` text, ClickHouse `UInt256` LE bytes — none of which changed).
4. The new code routes the decoded record through `decimal_arb` with the `native_int_kind` hint, instead of `u256` / `i256`.
5. The sink emits to the same wire format as before.

No schema mismatch can arise from a checkpoint because no schema is stored in one. No operator action is required to upgrade. Rollback (post-002 → pre-002) is symmetric and equally clean.

---

## E5. Capability matrix delta

`capability_for_decimal_arb(kind, precision, scale, coerce_to_string)` becomes `capability_for_decimal_arb(kind, precision, scale, coerce_to_string, native_int_kind)`.

The decision changes only inside the `ClickHouse` and `Hybrid` arms:

```
For ClickHouse/Hybrid:
  if native_int_kind == Some(U256) && precision <= 78 && scale == 0:
    Native  (will be emitted as UInt256)
  else if native_int_kind == Some(I256) && precision <= 78 && scale == 0:
    Native  (will be emitted as Int256)
  else:
    (existing logic: Native for p <= 76; OptInOnly for p > 76 with coerce_to: string; Reject otherwise)
```

All other connectors ignore `native_int_kind` for capability decisions.

---

## E6. Invariants and validation rules

| Invariant | Where enforced |
|---|---|
| `native_int_kind` is only present on `decimal_arb` fields | At metadata-stamp helper; refuse if field is not decimal_arb |
| `native_int_kind=u256` field's runtime values must be ≥ 0 when emitting to ClickHouse `UInt256` | ClickHouse sink emit path; surface FR-013-shaped error on first violating row |
| Mixed-hint arithmetic strips the hint | ExprPlanner output-field synthesis (already handles other metadata mismatches) |
| A `decimal_arb(78, 0)` with `native_int_kind=u256` round-trips through ClickHouse `UInt256` byte-exact | Acceptance scenario US4#2 |

---

## What this data model does NOT change

- The decimal_arb canonical byte encoding (sign byte + BE magnitude at declared scale) — unchanged.
- The decimal_arb UDF set, the ExprPlanner, the sort optimizer, the aggregate UDAFs — unchanged.
- The Postgres / Kafka JSON / Kafka Avro wire formats for decimal_arb — unchanged.
- The connector capability matrix's existing decisions for non-hinted decimal_arb columns — unchanged.
- The bigint preprocessor's `CAST(x AS DECIMAL(>76, s))` → `to_decimal_arb_from_string(...)` rewrite — unchanged.
