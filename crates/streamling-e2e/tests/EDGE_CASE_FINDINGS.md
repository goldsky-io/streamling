# Adversarial e2e test suite — findings & learnings

A suite of ~100 adversarial end-to-end tests (`crates/streamling-e2e/tests/edge_*.rs`)
written to surface **runtime errors and silent failures** in the DataFusion-54 /
arrow-avro / `decimal_arb` work, by pushing the edges of precision, scale, byte
width, type coercion, schema evolution, primary-key/op semantics, and value content.

Run them with:

```
cargo nextest run -p streamling-e2e -E 'binary(/edge_/)' -j 6 --no-fail-fast
```

(Rebuild the streamling release binary first — `just e2e-build` — so it reflects
local source changes. Run ambiguous/failing tests with `-j1`: under high
parallelism the streamling subprocess logs of concurrent tests interleave, making
error attribution unreliable.)

## Files

| File | Theme |
| --- | --- |
| `edge_decimal_boundaries.rs` | decimal precision/scale boundaries (Decimal128 ≤38 < Decimal256 ≤76 < decimal_arb), byte-width edges (16/17/32/33B) → Postgres NUMERIC |
| `edge_decimal_sql.rs` | SQL transforms over `decimal_arb` (+ − * / %, CASE, COALESCE, casts, comparisons, literal mixing) |
| `edge_decimal_clickhouse.rs` | `decimal_arb` → ClickHouse (native UInt256/Int256, `coerce_to: string`, narrow Decimal) |
| `edge_avro_types.rs` | non-decimal avro primitive decode (int/long/double/bool/string boundaries, unicode, nulls, wide records) |
| `edge_schema_evolution2.rs` | avro schema evolution + the arrow-avro per-writer-id generation batching |
| `edge_pk_upsert_delete.rs` | primary-key / debezium op (`c`/`u`/`d`) / `on_conflict` semantics |
| `edge_values_general.rs` | value/throughput edges (large batches, unicode/empty PKs, int/float extremes, filters, SQL string ops) |
| `edge_robustness_misc.rs` | multi-sink fan-out, filters, SQL coercion, arithmetic overflow / divide-by-zero |
| `edge_boundary_sinks.rs` | decimal_arb across Kafka(Avro) sink (round-trip), Webhook (JSON), Print (JSON) — all top-level, all pass |
| `edge_complex_decimal.rs` | NESTED decimal_arb (struct / array) at the JSON and Avro boundaries — pins F6/F7 |
| `edge_sql_runnable.rs` | runnable scalar SQL over decimal_arb — BETWEEN, IN, IS [NOT] NULL (pass); literal-BETWEEN pins F1 |

## Confirmed findings (real issues these tests surfaced)

### F1 — `decimal_arb` mixed with an integer literal fails type coercion
**Tests:** `edge_decimal_sql::sql_add_integer_literal_coercion`, `sql_mod_literal`, `sql_filter_positive_only`.
**Symptom:** `amount + 1`, `amount % 10`, and `WHERE amount > 0` fail at planning:
`Error during planning: Cannot infer common argument type for comparison operation LargeBinary > Int64`.
**Root cause:** the `decimal_arb` `ExprPlanner` (`decimal_arb_coercion.rs`) rewrites
binary ops only when **both** operands are `decimal_arb`. A `decimal_arb` operand
against a plain integer/float literal is left to DataFusion, which cannot coerce
`LargeBinary` vs `Int64`. Any arithmetic/comparison/filter mixing a `decimal_arb`
column with a numeric literal therefore fails.
**Suggested fix:** extend the `ExprPlanner` to coerce a numeric literal (or other
numeric operand) to `decimal_arb` when the other side is `decimal_arb` (insert a
`to_decimal_arb_from_*` cast), for arithmetic **and** comparison operators.

### F2 — `CASE` over `decimal_arb` drops the extension metadata → sink fails/hangs
**Tests:** `edge_decimal_sql::sql_case_passthrough_metadata_tripwire`, `sql_nested_case`.
**Symptom:** the `CASE` output field is bare `LargeBinary` (no decimal_arb
metadata), so the Postgres NUMERIC insert fails and the sink retries to the
timeout (see F4). End-to-end confirmation of the gap already tracked in PR #37's
TODO and the `#[ignore]`d unit tripwire
`session::tests::case_over_decimal_arb_should_preserve_metadata`.
**Suggested fix:** a metadata-propagation rule so expression outputs (CASE,
COALESCE, …) whose inputs are `decimal_arb` retain the extension metadata.

### F3 — Some decimal shapes overflow Postgres NUMERIC even into an over-sized column
**Tests:** `edge_decimal_boundaries::dec128_scale_equals_precision` (decimal(10,10),
value `0.1234567890` → `NUMERIC(40,10)`), `dec128_negative_near_min`
(decimal(38,2), large negative → `NUMERIC(40,2)`), `dec256_negative_high_scale`
(decimal(60,30), negative → `NUMERIC(80,30)`).
**Symptom:** `error returned from database: numeric field overflow` on INSERT —
even though the target NUMERIC is comfortably wide enough for the *correct* value
(`0.1234567890` cannot need >30 integer digits). Reproduces serially (`-j1`), so
not a parallelism artifact. Indicates streamling materializes an out-of-range
value for these shapes — a likely scale/sign handling bug in the decimal →
Postgres bind path for all-fractional (scale == precision), large-negative, and
high-scale decimals.
**Status:** root cause not yet pinned; needs investigation in the Postgres sink
decimal path.

### F4 — Postgres sink retries non-retriable errors indefinitely → silent hang
**Symptom:** a non-retriable DB error (`numeric field overflow`, `column "x" does
not exist`) is retried by `streamling_core::retry` repeatedly; the pipeline never
exits and hangs until the caller's timeout. This is what turns F1/F2/F3 — and many
ordinary config/data mistakes — into multi-minute silent hangs instead of fast,
actionable failures.
**Suggested fix:** classify DB errors. `numeric field overflow`, undefined column,
type mismatch, and constraint violations are non-retriable and should fail the
pipeline fast with a clear message; reserve retries for transient/connection
errors.

### F5 — `decimal_arb * decimal_arb` near max precision → arithmetic overflow
**Test:** `edge_decimal_sql::sql_mul_self_precision_near_max`.
**Symptom:** `Arrow error: Arithmetic overflow: … * …` for a product whose
precision approaches the `decimal_arb` ceiling. Likely *correct* (the result
exceeds the precision cap), but it surfaces as an opaque sink error and (via F4)
can hang. Worth confirming the intended UX (clear overflow error vs widening).

### F6 — Nested decimal_arb at the JSON boundary — **FIXED**
**Tests:** `edge_complex_decimal::{nested_struct_decimal_arb_to_print_json, array_of_decimal_arb_to_print_json}` (now assert the value), plus unit tests
`formats::json::tests::{nested_struct_decimal_arb_serializes_value_not_hex, array_of_struct_decimal_arb_serializes_values_not_hex}`.
**Was:** a `decimal_arb` nested inside a struct or array was emitted by the JSON
converter as its **raw canonical bytes in hex** (e.g. `{"amt":"00018ee9…"}`), not its
decimal value, because `json.rs` special-cased only **top-level** decimal_arb columns.
**Fix:** `json.rs` now rewrites decimal_arb leaves **recursively by field metadata**
(`decimalize_for_json` walks Struct / List / LargeList / FixedSizeList / Map), so nested
decimal_arb serializes as its decimal value across print, webhook, and any JSON/external-handler
output. (Decode of nested decimal_arb from a JSON *source* is a separate, still-unhandled path —
the recursive rewrite is currently output-only.)

### F7 — Nested decimal_arb fails the Kafka Avro sink
**Test:** `edge_complex_decimal::nested_struct_decimal_arb_kafka_avro_sink_fails_f7` (pinned).
**Symptom:** the avro schema builder (`to_avro`) emits **nested** decimal_arb fields as
plain `Bytes` (only top-level fields get the `decimal` logicalType), so the writer's
`Value::Decimal` fails to encode — `apache_avro`: *"Unsupported value-schema combination:
Decimal vs Bytes"* — and the sink errors. Top-level decimal_arb → avro is correct.
(F6 + F7 together: **nested/complex decimal_arb is unsupported at every sink except,
at the byte level, ClickHouse/Postgres which have no nested column type anyway**;
only the Avro/JSON *top-level* path works. The Avro *decode* of nested decimals →
decimal_arb is correct, per C1 — it's the re-serialization that's missing.)

## Status of the suite

- **100 tests across the 8 files above. All 100 pass** (`cargo nextest run -p
  streamling-e2e -E 'binary(/edge_/)'`).
- Test-construction bugs found during bring-up were fixed: SQL transform alias must
  match the sink column; over-tight `NUMERIC` targets; missing `type: sql` /
  `primary_key` on transforms; `record_limit` vs in-pipeline dedup; and a fragile
  integer-valued decimal round-trip assertion.
- The genuine-finding tests (F1, F2, F3, F5) are encoded as **documented
  expected-failure tripwires** (`assert_known_gap_no_rows` / `assert_overflows_no_rows`
  / bounded-timeout guards), each tagged `KNOWN GAP (Fx)`. They assert the *current
  broken* behavior (typically: no row lands). **When a gap is fixed, a row will land
  and the tripwire fails** — that's the signal to flip the test to assert the
  now-correct value. So the green suite both guards working behavior and flags the
  day each gap is closed.

## Test-authoring pitfalls discovered (so future tests avoid them)

- **SQL alias must match the sink column.** A `sql` transform output column whose
  alias does not match the destination table column causes the Postgres sink to
  INSERT a non-existent column, which (via F4) retries forever and **hangs to the
  timeout** — it does *not* fail fast. Always align: `SELECT … AS foo` ↔ table
  column `foo` ↔ verification `SELECT foo::text AS foo`.
- **Read wide NUMERIC as `col::text`** — `sqlx` has no native `NUMERIC(100,18)`.
- **Producing input is constrained by the harness:**
  - decimals: only `produce_decimal_record(schema, id, field, unscaled)` — a record
    of `id long` + one `decimal(p,s)` field; cannot produce nulls or multiple
    decimal columns this way.
  - everything else: FLAT `#[derive(Serialize)]` structs via `register_schema` +
    `produce_avro_records` (or `produce_json_records`). Nested records / arrays /
    maps / enums are not reliably encodable through the harness encoder.
- **`record_limit` counts every produced record**, including `d` (delete) ops; a
  filter that drops all rows may never reach the limit and hang — bound such tests
  with a short timeout and assert on the table state instead.
