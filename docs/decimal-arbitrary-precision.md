# Arbitrary-precision decimal columns (`streamling.decimal_arb`)

Streamling supports a numeric type whose precision and scale are
user-declared and not bounded by the 76-digit ceiling of Arrow's
`Decimal256`. Use it when a column legitimately needs more digits than
`NUMERIC(76, *)` can hold — for example, very large token balances,
long-window accumulators, or any Postgres `NUMERIC(p, s)` with `p > 76`.

The type is **opt-in by precision**: declare `precision > 76` on a
column anywhere — at the source, in YAML, in a CAST — and streamling
auto-promotes it to `decimal_arb`. Columns at or below 76 keep using
`Decimal128`/`Decimal256` exactly as they do today; nothing about
existing pipelines changes.

> **Spec, plan, and contracts** live under
> [`specs/001-decimal-arbitrary-precision/`](../specs/001-decimal-arbitrary-precision/),
> with worked examples in
> [`quickstart.md`](../specs/001-decimal-arbitrary-precision/quickstart.md).

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

-- Build decimal_arb literals from text:
SELECT to_decimal_arb_from_string('1234567890.987654321098765432109876543210', 80, 30);

-- Sort with explicit signed-correct key (auto-rewrite is a follow-up):
SELECT * FROM src ORDER BY decimal_arb_to_sort_key(amount);
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
| Postgres `NUMERIC(p, s)` (sink-side type override) | Auto-promoted to `decimal_arb` (FR-018 — fixes a prior mis-mapping bug). |
| Avro `decimal(p, s)` logical type   | Auto-promoted; previously fell back to `Utf8` (lossy).               |
| Kafka JSON digit-string             | Uses YAML schema's declared `precision`; auto-promoted.              |
| Arrow IPC                           | Round-trips natively via the extension-type metadata.                |

`precision <= 76` keeps using the existing `Decimal128(p, s)` (≤38) or
`Decimal256(p, s)` (39–76) — no behavior change.

## Connector capability matrix

| Connector                           | Native support                       | Without opt-in (`p > 76`)             |
|-------------------------------------|--------------------------------------|---------------------------------------|
| Postgres source / sink              | `NUMERIC(p, s)` up to 1000 digits    | Native                                |
| Kafka JSON                          | digit-string                         | Native                                |
| Kafka Avro                          | `decimal(p, s)` if declared bytes fit | `Reject` (or `OptInOnly` in future)   |
| Kafka Protobuf                      | (no native decimal)                  | `Reject` until `coerce_to: string`    |
| ClickHouse / Hybrid                 | `Decimal(p, s)` up to 76 digits      | `String` fallback (logged); hard reject after `coerce_to: string` lands |
| SQS / webhook (JSON)                | digit-string                         | Native                                |
| Plugins                             | per plugin                           | `Reject` unless plugin advertises    |

The capability decision function is exposed at
[`crates/streamling-common/src/types/decimal_arb_capability.rs`](../crates/streamling-common/src/types/decimal_arb_capability.rs)
— `capability_for_decimal_arb(kind, precision, scale, coerce_to_string)`.
The pipeline-startup validator that consults it lives in
[`specs/.../tasks.md`](../specs/001-decimal-arbitrary-precision/tasks.md)
under T033 (deferred).

## Known limitations (the deferred bits)

These are documented in `tasks.md` as `[-]` deferred and have explicit
acceptance criteria for whoever picks them up:

- **`ORDER BY decimal_arb_col` direct (no function call)** — bytewise
  sort is wrong for negatives (sign byte `0xFF` byte-wise sorts after
  `0x00`). Workaround today:
  `ORDER BY decimal_arb_to_sort_key(col)`. The auto-rewrite needs a
  LogicalPlan-level `OptimizerRule` that wraps Sort exprs.
- **Mixed-operand expressions** — `decimal_arb_col + decimal128_col`
  does not yet auto-coerce. Workaround: cast the narrow side
  explicitly via `to_decimal_arb_from_*` (most directions still to
  ship — only `from_string` lands today).
- **ClickHouse hard-rejection at config load** — the silent `String`
  fallback for `precision > 76` still fires (with a WARN log). The
  `coerce_to: string` YAML directive infrastructure is the open piece
  in `streamling-config`.
- **Postgres pipeline-level overflow detection** at config load — the
  `NUMERIC` width is checked at the type-mapping layer but the
  capability matrix isn't yet consulted by the pipeline-startup
  validator (T033).
- **E2E tests** — three end-to-end scenarios in `tasks.md` (T020–T022)
  remain deferred until `just env-setup` is run; the unit-test layer
  fully covers each underlying conversion path.

## Performance

Per the spec's Assumptions: arithmetic on `decimal_arb` values pays an
inherent cost proportional to the declared precision. Expect:

- For values that would have fit `Decimal128` or `Decimal256`, prefer
  declaring the smaller types — they take a fast Arrow primitive path.
- `decimal_arb` arithmetic goes through `bigdecimal::BigDecimal`. For
  precision 100–200 and typical pipeline volumes this is fast enough
  that it's rarely the bottleneck; for thousands of digits it can
  dominate. Profile before assuming.
- Pipelines that don't reference `decimal_arb` at all are unchanged
  (SC-003).

## Implementation entry points

For deeper reading:

- [`spec.md`](../specs/001-decimal-arbitrary-precision/spec.md) — the
  user-visible requirements (FR-001 through FR-020).
- [`plan.md`](../specs/001-decimal-arbitrary-precision/plan.md) — the
  technical approach (Arrow extension type + UDFs + ExprPlanner).
- [`research.md`](../specs/001-decimal-arbitrary-precision/research.md)
  — Decision/Rationale/Alternatives for each technical choice.
- [`data-model.md`](../specs/001-decimal-arbitrary-precision/data-model.md)
  — `DecimalArbType`, `DecimalArbValue`, the capability matrix.
- [`contracts/`](../specs/001-decimal-arbitrary-precision/contracts/)
  — wire format, UDF/UDAF signatures, connector capability surface,
  YAML schema additions.
- [`quickstart.md`](../specs/001-decimal-arbitrary-precision/quickstart.md)
  — three runnable end-to-end pipelines.
- [`tasks.md`](../specs/001-decimal-arbitrary-precision/tasks.md) —
  the per-task implementation status, including everything still
  marked `[-]` deferred with acceptance criteria.
