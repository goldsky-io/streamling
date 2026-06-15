# Contract: ClickHouse Wide-Integer Wire-Format Adapter

This contract specifies how the ClickHouse connector translates between ClickHouse's native `UInt256` / `Int256` types and streamling's internal `decimal_arb` representation.

## Source side (ClickHouse → decimal_arb) — **deferred for this feature**

> **Status**: this section describes a contract the implementation does **not** satisfy in feature 002. Source-side hint stamping (and the accompanying byte-conversion adapter) is deferred to a follow-up — see tasks T009, T023–T025 (`[-]` deferred). The text below remains as the design target for that follow-up.

### Why deferred

Investigation in T009 confirmed that today's `clickhouse.rs::fetch_schema` and `normalize_schema_for_clickhouse` do **not** walk `system.columns` for source-side type annotation. A ClickHouse `UInt256` / `Int256` source column lands in the pipeline as a plain `FixedSizeBinary(32)` with no provenance. That is pre-existing behavior, not a regression introduced by this feature, so flipping the source-side adapter doesn't gate retiring `u256`/`i256`. The follow-up that adds this adapter is a meaningful new feature in its own right (new `system.columns` HTTP path + byte-conversion module + a real test surface).

### Target behavior (future)

When the connector probes a ClickHouse table's schema (today via `LIMIT 1 FORMAT Arrow`, then walked through `normalize_schema_for_clickhouse`), every column with ClickHouse type:

| ClickHouse type | Arrow IPC representation | Adapted to (future) |
|---|---|---|
| `UInt256` | `FixedSizeBinary(32)` | `decimal_arb(78, 0)` field with `native_int_kind=u256` |
| `Int256` | `FixedSizeBinary(32)` | `decimal_arb(78, 0)` field with `native_int_kind=i256` |

The adapter would learn each column's underlying ClickHouse type via a `SELECT name, type FROM system.columns WHERE database = ? AND table = ?` lookup, because the Arrow IPC alone shows only `FixedSizeBinary(32)` and cannot distinguish `UInt256` from `Int256` from a generic `FixedString(32)`.

### Target batch read (future)

ClickHouse emits 256-bit values as 32 little-endian bytes per row. The decimal_arb canonical encoding is `[sign_byte][big-endian magnitude bytes scaled to declared scale]`. The adapter would:

1. Read the FSB(32) bytes for each row.
2. Reverse byte order (LE → BE).
3. For `UInt256`: emit as `[0x00][BE magnitude]` (always non-negative).
4. For `Int256`: detect the sign bit (high bit of MSB before reverse); if negative, two's-complement the magnitude and emit as `[0xFF][BE magnitude]`; otherwise `[0x00][BE magnitude]`.

The output is a `LargeBinary` array carrying the canonical decimal_arb bytes.

### Workaround until the source-side adapter ships

For ClickHouse `UInt256` / `Int256` source columns whose values must round-trip through a downstream ClickHouse sink as the same native type, pair the ClickHouse source with a Kafka/Postgres-sourced version of the same column (the hint stamps on those source paths), or explicitly set `schema_override = "UInt256"` on the ClickHouse sink to override the CREATE TABLE column type. This is documented in `docs/decimal-arbitrary-precision.md` under "Known limitations".

## Sink side (decimal_arb → ClickHouse)

### CREATE TABLE

`ClickHouseClient::build_create_table_query` already consults `clickhouse_column_type(field, directive)` per column (from feature 001). That function gains the new logic:

```text
input: Arrow Field, optional ColumnDirective

if field is decimal_arb:
  read (precision, scale, native_int_kind_opt)
  match (precision, scale, native_int_kind_opt):
    (78, 0, Some(u256)) => emit "UInt256"
    (_p, 0, Some(i256)) where _p <= 78 => emit "Int256"
    (p, s, None) where p <= 76 => emit "Decimal(p, s)"
    (p, s, None) where p > 76 and coerce_to=string => emit "String"
    (p, s, None) where p > 76 and no opt-in => return FR-012 error
else:
  existing arrow_field_to_clickhouse path
```

### Batch insert (data conversion)

Today the ClickHouse sink uses `build_projection_for_clickhouse` to materialize columns before HTTP insert. For columns with `native_int_kind` matching the destination ClickHouse type:

1. Read the canonical decimal_arb bytes for each row.
2. Strip the sign byte; take the magnitude.
3. For `UInt256` target: verify sign byte is `0x00` (or magnitude is zero); if non-zero sign, return FR-013-shaped error "value out of range for declared native_int_kind=u256". Pad/truncate magnitude to exactly 32 bytes BE, reverse to LE, emit as FSB(32) for the HTTP insert.
4. For `Int256` target: pad magnitude to 32 bytes BE; if sign byte is `0xFF`, two's-complement the result; reverse to LE; emit as FSB(32).

The cast_map step that feature 001 uses for `decimal_arb` → `String` is bypassed for the `UInt256`/`Int256` path; those columns are inserted as bytes, not as text.

### Error semantics

- Row value exceeds `UInt256` range (negative or > 2^256−1) with `native_int_kind=u256`: pipeline fails with a row-attribute error naming the column and row index.
- Row value exceeds `Int256` range (|value| > 2^255−1) with `native_int_kind=i256`: same.
- Sink-side ClickHouse table has a column of incompatible type (e.g. table declares `String` but pipeline emits `UInt256`): the existing ClickHouse server-side type-check error surfaces; the operator updates either the YAML or the table.

## Invariants

1. **Round-trip identity (Avro/Postgres source → ClickHouse sink)**: a value read from an Avro `decimal(78, 0)` or Postgres `NUMERIC(78, 0)` source column and written to a ClickHouse `UInt256` sink column (via the unmodified pipeline, no transform) MUST be byte-identical to the source. Same for `decimal(77, 0)` → `Int256`. **ClickHouse → ClickHouse round-trip is NOT covered** by this invariant in feature 002 — see the "deferred" note in the Source side section.
2. **Sink storage preservation**: a sink emitting to a pre-existing table with `UInt256`/`Int256` columns MUST NOT change the column type during `CREATE TABLE IF NOT EXISTS`. (The "if not exists" guard naturally honors this — but the column generator must produce the same `UInt256`/`Int256` ClickHouse-side type so re-running CREATE doesn't drift.) This applies whenever the streamling-side column arrives at the sink hinted (i.e. from an Avro or Postgres source).
3. **Hint preservation through identity-only transforms**: a SQL `SELECT amount FROM src` transform on a hinted source column emits an output column whose Arrow metadata still carries the `native_int_kind` hint. Arithmetic / aggregation / CAST transforms drop the hint (see `data-model.md` E1 — `build_output_field` produces a fresh field without the hint).

## Out of scope for this contract

- ClickHouse's `Decimal128(p, s)` / `Decimal256(p, s)` source columns — these continue to map to `Decimal128`/`Decimal256` Arrow types as today.
- ClickHouse's other fixed-width types (`Int128`, `UInt128`, `FixedString(32)` — the bare-bytes case) — unaffected by this feature.
- Bulk transfer protocols other than HTTP Arrow IPC — the Native protocol is not in scope.
