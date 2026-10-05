# Arbitrary-precision decimal columns (`streamling.decimal_arb`)

Streamling supports a numeric type whose precision and scale are
user-declared and not bounded by the 76-digit ceiling of Arrow's
`Decimal256`. Use it when a column legitimately needs more digits than
`NUMERIC(76, *)` can hold — for example, very large token balances,
long-window accumulators, or any Postgres `NUMERIC(p, s)` with `p > 76`.

The type is **opt-in by precision**: where a column is declared with
`precision > 76` — a Postgres `NUMERIC(p, s)` column, an Avro
`decimal(p, s)` logical type, or a `CAST(… AS DECIMAL(p, s))` in a SQL
transform — streamling auto-promotes it to `decimal_arb`. Columns at or
below 76 keep using `Decimal128`/`Decimal256` exactly as they do today;
nothing about existing pipelines changes.

## What you can write today

```sql
-- Arithmetic, comparisons, aggregates — all native:
SELECT
  a + b           AS sum,
  a * b           AS product,
  a / b           AS quotient,           -- result at scale 18 (default_div_scale)
  a < threshold   AS small,
  SUM(amount)     AS total,
  AVG(amount)     AS mean,
  MIN(amount), MAX(amount), COUNT(*)
FROM src
WHERE amount > 0
GROUP BY entity_id;

-- Literals next to a decimal_arb column are exact, however wide or
-- fractional: the SQL preprocessor quotes a bare numeric literal that
-- DataFusion would otherwise plan as Float64, and the planner parses the
-- digits. A quoted literal works the same way anywhere.
SELECT * FROM src WHERE amount > 1000000000000000000000000 AND amount * 1.5 < '1e30';

-- Build decimal_arb literals from text at an explicit (precision, scale):
SELECT to_decimal_arb_from_string('1234567890.987654321098765432109876543210', 80, 30);

-- ORDER BY is sign-correct automatically (an optimizer rule rewrites it
-- to an order-preserving key; canonical bytes alone would sort every
-- negative above every positive). The key function is also callable
-- directly if you need it in an expression:
SELECT * FROM src ORDER BY amount;
```

The native `+`/`-`/`*`/`/`/`%`/`=`/`!=`/`<`/`<=`/`>`/`>=` surface is
wired via DataFusion's `ExprPlanner`. The standard SQL aggregate names
(`SUM`/`MIN`/`MAX`/`AVG`/`COUNT`) are wired via `register_udaf` and
override the built-ins for `decimal_arb` inputs only — pipelines using
only `Decimal128`/`Decimal256` are unaffected.

## How auto-promotion works

Where the column's declared precision lives drives the routing:

| Source of declaration               | What happens for `precision > 76`                                    |
|-------------------------------------|----------------------------------------------------------------------|
| Postgres `NUMERIC(p, s)`            | Auto-promoted to `decimal_arb`. For `NUMERIC(78, 0)` specifically, the `u256` native-int hint is set so a downstream ClickHouse sink can emit `UInt256` storage; a signed column is pinned to `Int256` with the sink's `schema_override` (see below). |
| Avro `decimal(p, s)` logical type   | Auto-promoted. Every integer-shaped `decimal(p, 0)` with `p > 76` gets the `u256` hint — the routing the retired `u256` type had (there is no Avro convention for signed vs. unsigned). A value above 2^256 − 1 fails the ClickHouse write loudly instead of being truncated. |
| `CAST(… AS DECIMAL(p, s))` in SQL   | Auto-promoted for `p > 76`; `TRY_CAST` yields NULL for a value that does not convert. `DECIMAL(77..=78, 0)` keeps the `u256` hint the retired `to_u256` rewrite gave it, so a ClickHouse sink still stores it as `UInt256`. |
| Kafka JSON, via a source `schema:` map | **Not** auto-promoted — see "Known limitations". Declare the column as `string` and convert in SQL. |
| Arrow IPC                           | Round-trips natively via the extension-type metadata (including the `native_int_kind` hint). |

`precision <= 76` keeps using the existing `Decimal128(p, s)` (≤38) or
`Decimal256(p, s)` (39–76) — no behavior change.

## Wide integers (Ethereum-style `uint256` / `int256`)

If you've worked with blockchain data, you've seen 256-bit unsigned
(`uint256`) and signed (`int256`) integers — gas, balances, token
amounts. There used to be dedicated `u256` / `i256` extension types
for these; those were **retired in favor of `decimal_arb`**. What
changed for you as a pipeline author:

- **Nothing in your YAML, and nothing in SQL that uses plain operators.**
  An Avro `decimal(p > 76, 0)` source column still works the same way. A
  Postgres `NUMERIC(78, 0)` source column still works the same way. A
  ClickHouse `UInt256` destination column still stores values as 256-bit
  native. `a + b`, `a * b`, comparisons, `CAST(col AS TEXT)` and
  `CAST(x AS DECIMAL(78, 0))` all plan as before. The type identity that
  streamling uses internally changes from `u256` to `decimal_arb(78, 0)`.

- **SQL that called the dedicated `u256_*` / `i256_*` functions must be
  rewritten.** `to_u256`, `to_i256`, `to_int64`, `u256_add` / `_sub` /
  `_mul` / `_div` / `_mod` / `_to_string`, their `i256_*` twins and
  `i256_neg` / `i256_abs` are gone, with no aliases. Use the plain operator
  (`a + b`), `CAST(x AS DECIMAL(78, 0))` and `CAST(col AS TEXT)` instead —
  the preprocessor and the planner route those to `decimal_arb` for you.
  A pipeline whose SQL still names one of the old functions fails at
  planning with "function not found".

- **SQL operations on wide-integer columns are richer.** Previously
  `SUM(gas_used)`, `MIN(balance)`, `ORDER BY i256_col` with
  negative values, and `CAST(col AS TEXT)` either failed outright
  or returned wrong results. After the migration they all work
  correctly — wide-integer columns inherit the full `decimal_arb`
  surface (aggregates, comparisons, sorts, casts).

- **ClickHouse storage compactness is preserved.** An integer-shaped
  `decimal_arb` column that originated from an Avro `decimal(p > 76, 0)`
  field, a Postgres `NUMERIC(78, 0)` column or a `CAST(… AS
  DECIMAL(77..=78, 0))` carries a `streamling.native_int_kind` hint on its
  Arrow field metadata. Every integer-shaped result derived from such a
  column keeps it: `+`/`-`/`*`/`%` (with a literal or an unhinted operand
  too — `balance + 1`), `ABS`, `SUM`, `MIN`, `MAX`, `GREATEST`/`LEAST`,
  `COALESCE`/`CASE`/`NULLIF`, `UNION` branches and list `min`/`max`;
  `-x` is `i256`-shaped. Two different hints (`u256` next to `i256`) and
  any fractional result carry none. `a - b` over two `u256` columns stays
  `u256`, so a negative difference fails the write — pin the column to
  `Int256` in `schema_override` when differences can be negative. The
  ClickHouse sink consults the hint and emits
  CREATE TABLE columns as `UInt256` (or `Int256`), not as `Decimal(78, 0)`
  or `String`, and range-checks every value against that type on write.
  Existing wide-integer ClickHouse tables don't need a schema change. To
  pin a column yourself — a signed `NUMERIC(78, 0)` that must land as
  `Int256`, or a wide integer column that arrived without a hint — set the
  type in the sink's `schema_override`; the sink reads `UInt256` /
  `Int256` there as the hint:

  ```yaml
  sinks:
    ch_sink:
      type: clickhouse
      schema_override:
        balance: "Int256"
  ```

## Connector capability matrix

| Connector                           | Native support                       | Without opt-in (`p > 76`)             |
|-------------------------------------|--------------------------------------|---------------------------------------|
| Postgres source / sink              | `NUMERIC(p, s)` up to 1000 digits    | Native                                |
| Kafka JSON                          | digit-string                         | Native                                |
| Kafka Avro                          | `decimal(p, s)` if declared bytes fit | `Reject` unless `coerce_to: string`   |
| Kafka Protobuf                      | (no native decimal)                  | `Reject` until `coerce_to: string`    |
| ClickHouse / Hybrid                 | `Decimal(p, s)` up to 76 digits; `UInt256`/`Int256` for an integer-shaped (scale 0) decimal_arb with a native-int hint or a `schema_override` pin, at any declared precision, range-checked on write | Hard reject without `coerce_to: string` or a `UInt256`/`Int256` `schema_override` (the silent String fallback was retired); `coerce_to: string` wins over a hint |
| webhook (JSON)                      | digit-string                         | Native                                |
| Plugins                             | pass-through: the plugin receives the column as `LargeBinary` + `decimal_arb` metadata and decides for itself | Native |

The capability decision function is exposed at
[`crates/streamling-common/src/types/decimal_arb_capability.rs`](../crates/streamling-common/src/types/decimal_arb_capability.rs)
— `capability_for_decimal_arb(kind, precision, scale, coerce_to_string, native_int_kind)`.
The pipeline-startup validator (`validate_pipeline_decimal_arb`) is
called from every sink-construction arm in `streamling/src/lib.rs`,
so misconfigured pipelines fail at config-load with an actionable
error naming the offending column and connector.

## Known limitations

What is not supported today:

- **ClickHouse-source-side native-int annotation** — when a pipeline
  reads from a ClickHouse `UInt256` / `Int256` source column, the
  resulting Arrow field is plain `FixedSizeBinary(32)` without the
  `native_int_kind` hint (the ClickHouse HTTP `FORMAT Arrow` probe
  doesn't tell us the underlying ClickHouse type). The hint is set
  for Avro and Postgres sources. Adding ClickHouse-source-side
  annotation is straightforward — a `system.columns` lookup after
  the schema probe — but is not implemented. Workaround:
  pair ClickHouse `UInt256` sources with a Kafka/Postgres source
  if you need the hint to propagate to a downstream ClickHouse sink.
  For the same reason the hybrid source reads a hinted (or wide)
  `decimal_arb` column from its ClickHouse history table as decimal
  text and parses it back: the hint says how a sink writes the
  column, and the history table of a `u256`-hinted stream may be
  `Int256`, `Decimal(100, 0)` or `String` — a `CAST(… AS UInt256)`
  there would wrap every negative value inside ClickHouse.
- **A source `schema:` map cannot declare a `decimal_arb` column** —
  a Kafka JSON source's `schema:` block maps a column name to an Arrow
  type string, and that grammar only produces `Decimal128(p, s)` or
  `Decimal256(p, s)`; there is no `decimal_arb` spelling, and a
  declared precision above the Arrow maximum (38 for `Decimal128`, 76
  for `Decimal256`) is not rejected at config load. Declare such a
  column as `string` and convert it in the SQL transform with
  `to_decimal_arb_from_string(col, p, s)` — that path is exact and
  validated per value.
- **In-pipeline SQL aggregates require `postgres_aggregate` sink** —
  streamling's streaming SQL transforms reject bare `Aggregate` /
  `WindowAggr` plan nodes (this is a general streamling constraint,
  not specific to `decimal_arb`). For `SUM` / `MIN` / `MAX` / `AVG`
  / `COUNT` over a decimal_arb column, route through the
  `postgres_aggregate` sink shape.
- **Plugins see `decimal_arb` as-is.** The host hands a plugin its
  batches unchanged, so a plugin sink or transform receives wide
  integers as `LargeBinary` columns carrying the `decimal_arb` extension
  metadata (where it received `FixedSizeBinary(32)` `u256` / `i256`
  before). A plugin that expects the old shape must be updated; to hand
  it text instead, convert in SQL with `CAST(col AS TEXT)` upstream of
  the plugin.
- **`decimal_arb` inside an Arrow `Union` is rejected at config load**
  for every connector — nothing serialises a decimal through a union.
  Flatten the union in a transform.
- **Pre-existing ClickHouse tables with `Decimal(78, 0)` columns** —
  the ClickHouse sink emits `UInt256` for a hinted decimal_arb
  column on `CREATE TABLE`. If a user's table was hand-rolled (or
  created by a much older streamling version) with the column typed
  as `Decimal(78, 0)`, `CREATE TABLE IF NOT EXISTS` is a no-op
  (table exists) and the subsequent INSERT will fail server-side
  with a type-mismatch error. Workaround: ALTER the table to
  `UInt256` / `Int256`, or pin the legacy ClickHouse type via the
  sink's `schema_override` map, e.g.

  ```yaml
  sinks:
    ch_sink:
      type: clickhouse
      schema_override:
        balance: "Decimal(78, 0)"
  ```

## Migration runbook (for operators)

If you're upgrading a pipeline from a streamling that still had the
`u256` / `i256` types, the type identity for wide-integer columns changes
from `u256`/`i256` to `decimal_arb(p, 0) + native_int_kind=u256/i256`.
Checkpoints are unaffected: they record source-side offsets only and do
not carry the Arrow schema of in-flight data, so on restart the pipeline
resumes from the stored Kafka / Postgres-CDC / ClickHouse offset and the
source decodes records exactly as before. Rolling the binary back is
clean for the same reason. What does change is below — check each
pipeline against this list before upgrading.

### What stays the same

- YAML pipeline configs.
- Postgres tables: `NUMERIC(78, 0)` on both sides, written as decimal
  text.
- ClickHouse tables: `UInt256` / `Int256` columns receive the same 32
  little-endian bytes; existing tables need no DDL change.
- The Avro *source*: `decimal(p, s)` fields decode as before.
- Plain-operator SQL (`a + b`, comparisons, `CAST(… AS TEXT)`,
  `CAST(… AS DECIMAL(78, 0))`).
- Pipeline state checkpoints (source offsets).
- Pipelines that don't use wide-integer columns, with two exceptions
  noted below (Parquet sources and Postgres decimals).

### What changes

- **SQL that names a retired function fails to plan.** Rewrite
  `to_u256` / `to_i256` / `to_int64`, `u256_*` and `i256_*` calls to the
  plain-operator forms (see "Wide integers" above). There are no aliases.
- **Kafka Avro sinks register a new schema version.** Wide-integer
  columns are now written as Avro `bytes` with a `decimal` logicalType
  and a minimal two's-complement payload, where the retired type wrote
  plain `bytes` holding 32 raw big-endian bytes. On startup the sink
  compares its schema with the subject's latest version and registers
  the new one when they differ; if the subject's compatibility level
  forbids it, the pipeline stops at startup with an error naming the
  subject. Consumers that read the raw bytes of those fields must handle
  the new encoding; consumers that use the Avro decimal logical type
  need no change. New topics get the new shape from the start.
- **JSON sinks (webhook, Kafka JSON) print every wide integer as a
  decimal string.** A top-level `u256` already did; a nested `u256`, and
  `i256` anywhere, printed as hex before.
- **Plugins receive `LargeBinary` + `decimal_arb` metadata** instead of
  `FixedSizeBinary(32)` `u256` / `i256`. Update plugins that match on the
  old Arrow type before upgrading the pipelines that feed them.
- **ClickHouse: a wide decimal without a hint is a config-load error**,
  not a silent `String` column. Hinted integer columns (Avro
  `decimal(p > 76, 0)`, `NUMERIC(78, 0)`, `CAST … DECIMAL(77..=78, 0)`,
  and arithmetic, `COALESCE` / `CASE`, `GREATEST` / `LEAST`, `SUM` /
  `MIN` / `MAX` over them) take the native route
  at any declared precision, and a value that does not fit `UInt256` /
  `Int256` now fails the write instead of being truncated. For anything
  else choose: `coerce_to: string`, or a `UInt256` / `Int256`
  `schema_override` pin.
- **Parquet file sources read string and binary columns as `Utf8` /
  `Binary`** (the file's declared types) rather than DataFusion's view
  types. The source has to keep Arrow field metadata to recognise
  `decimal_arb` columns, and DataFusion's view-type rewrite would
  otherwise change the storage type out from under that metadata. No
  sink DDL depends on the difference.
- **Postgres `Decimal(p, s > 0)` values are written correctly.** The
  previous binder appended `s` zeros to the *unscaled* integer, so a
  `Decimal128(10, 2)` holding `12.34` was stored as `1234.00` (and some
  shapes overflowed `NUMERIC` and never landed). Rows written before the
  upgrade keep the inflated values; an upsert on the same key writes the
  correct value alongside, it does not rewrite history. Audit tables fed
  by a `Decimal(p, s > 0)` column before relying on old rows.

## Performance

Arithmetic on `decimal_arb` values pays an inherent cost proportional
to the declared precision. Expect:

- For values that would have fit `Decimal128` or `Decimal256`, prefer
  declaring the smaller types — they take a fast Arrow primitive path.
- `decimal_arb` arithmetic goes through `bigdecimal::BigDecimal`. For
  precision 100–200 and typical pipeline volumes this is fast enough
  that it's rarely the bottleneck; for thousands of digits it can
  dominate. Profile before assuming.
- Pipelines that don't reference `decimal_arb` at all are unchanged.
