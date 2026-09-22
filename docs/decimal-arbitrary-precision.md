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
| Postgres `NUMERIC(p, s)`            | Auto-promoted to `decimal_arb`. For `NUMERIC(78, 0)` specifically, the `u256` native-int hint is set so a downstream ClickHouse sink can emit `UInt256` storage. |
| Avro `decimal(p, s)` logical type   | Auto-promoted. `decimal(p, 0)` with `p` in `77..=78` gets the `u256` hint (there is no native Avro convention for signed vs. unsigned wide decimals). Signed `Int256` round-trip from Avro requires a future opt-in. |
| `CAST(… AS DECIMAL(p, s))` in SQL   | Auto-promoted for `p > 76`; `TRY_CAST` yields NULL for a value that does not convert. |
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

- **Nothing in your YAML or your SQL.** An Avro `decimal(78, 0)`
  source column still works the same way. A Postgres `NUMERIC(78, 0)`
  source column still works the same way. A ClickHouse `UInt256`
  destination column still stores values as 256-bit native. The
  type identity that streamling uses internally changes from `u256`
  to `decimal_arb(78, 0)`, but the surface you touch is unchanged.

- **SQL operations on wide-integer columns are richer.** Previously
  `SUM(gas_used)`, `MIN(balance)`, `ORDER BY i256_col` with
  negative values, and `CAST(col AS TEXT)` either failed outright
  or returned wrong results. After the migration they all work
  correctly — wide-integer columns inherit the full `decimal_arb`
  surface (aggregates, comparisons, sorts, casts).

- **ClickHouse storage compactness is preserved.** A
  `decimal_arb(78, 0)` column that originated from an Avro
  `decimal(78, 0)` field or a ClickHouse `UInt256` table column
  carries a `streamling.native_int_kind` hint on its Arrow field
  metadata. The ClickHouse sink consults that hint and emits
  CREATE TABLE columns as `UInt256` (or `Int256` for signed), not
  as `Decimal(78, 0)` or `String`. Existing wide-integer ClickHouse
  tables don't need a schema change.

## Connector capability matrix

| Connector                           | Native support                       | Without opt-in (`p > 76`)             |
|-------------------------------------|--------------------------------------|---------------------------------------|
| Postgres source / sink              | `NUMERIC(p, s)` up to 1000 digits    | Native                                |
| Kafka JSON                          | digit-string                         | Native                                |
| Kafka Avro                          | `decimal(p, s)` if declared bytes fit | `Reject` unless `coerce_to: string`   |
| Kafka Protobuf                      | (no native decimal)                  | `Reject` until `coerce_to: string`    |
| ClickHouse / Hybrid                 | `Decimal(p, s)` up to 76 digits; `UInt256`/`Int256` for hinted `(≤78, 0)` decimal_arb | Hard reject without `coerce_to: string` (the silent String fallback was retired) |
| SQS / webhook (JSON)                | digit-string                         | Native                                |
| Plugins                             | per plugin                           | `Reject` unless plugin advertises    |

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
- **Plugin connector default** — the capability matrix returns
  `Reject` for `decimal_arb` columns flowing to a plugin sink (or
  source) unless the plugin advertises support via a
  `supports_decimal_arb` FFI hook. The hook itself is not yet
  implemented; plugins that need wide-integer support today must
  use `coerce_to: string` on the column.
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
`u256` / `i256` types, the type identity for wide-integer columns changes from `u256`/`i256` to
`decimal_arb(p, 0) + native_int_kind=u256/i256`. The wire formats are
unchanged (Avro decimal bytes, Postgres NUMERIC, ClickHouse
UInt256/Int256), so your tables and your Kafka topics don't need any
changes.

**No operator action is needed to upgrade an existing pipeline.**
Streamling checkpoints record source-side offsets only — they do not
carry the Arrow schema of in-flight data. On restart the pipeline
resumes reading from the stored Kafka / Postgres-CDC / ClickHouse
offset, the source decodes records exactly as before (the Avro decimal
bytes / NUMERIC text / UInt256 bytes haven't changed), and the new
code routes those records through `decimal_arb` instead of `u256` /
`i256`. The downstream sink emits the same wire bytes either way.

If you're keeping a deploy-time rollback option, the rollback is also
clean — both versions read the same checkpoints, because checkpoints
only carry offsets.

### What's *not* breaking

- YAML pipeline configs are unchanged.
- Kafka topic schemas (registered Avro) are unchanged.
- Postgres table schemas are unchanged.
- ClickHouse table schemas are unchanged.
- SQL transforms are unchanged.
- Pipeline state checkpoints (source offsets) are unchanged.
- Pipelines that don't use wide-integer columns are entirely
  unaffected.

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
