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

### F1 — `decimal_arb` mixed with an integer literal failed type coercion — **FIXED**
**Tests:** `edge_decimal_sql::{sql_add_integer_literal_coercion, sql_mod_literal, sql_filter_positive_only}`,
`edge_sql_runnable::between_int_literals` (now assert correct results), plus unit tests
`functions::decimal_arb_coercion::tests::{decimal_arb_plus_integer_literal_dispatches, decimal_arb_filter_with_integer_literal_works, mixed_decimal_arb_plus_int64_column_dispatches, decimal_arb_plus_float_still_rejected}`.
**Was:** `amount + 1`, `amount % 10`, `WHERE amount > 0`, `BETWEEN 0 AND 100` failed at
planning: `Cannot infer common argument type … LargeBinary > Int64`. The `ExprPlanner`
rewrote binary ops only when **both** operands were `decimal_arb`; a `decimal_arb` vs an
integer was left to DataFusion, which can't coerce `LargeBinary` vs `Int64`.
**Fix:** the `ExprPlanner` (`decimal_arb_coercion.rs`) now coerces an integer operand
(column or literal) to `decimal_arb` at scale 0 via `to_decimal_arb_from_int` (precision
20, covering any 64-bit int) for both arithmetic and comparison operators. Floats remain
rejected (lossy — explicit cast required); a bare `1.5` literal is `Decimal128` in
DataFusion and coerces fine, but a genuine `Float64` still errors. Fixing this also
surfaced and fixed a **latent empty-batch panic**: the binary/comparison ops computed
result length as `max(left.len(), right.len())`, so an empty column (0 rows) against a
broadcast scalar (len 1) gave length 1 and read `column.value(0)` out of bounds —
replaced with proper broadcast semantics (`broadcast_len`).

### F1b — `BETWEEN` / `IN` with literal bounds over `decimal_arb` — still open
**Test:** `edge_sql_runnable::between_int_literals_f1b` (pinned tripwire).
**Symptom:** `amount BETWEEN 0 AND 100` (and `amount IN (…)` with literals) fails to plan
/ lands nothing. **Distinct from F1**: `BETWEEN` is an `Expr::Between` and `IN` an
`Expr::InList` — not `BinaryExpr` — so the decimal_arb `ExprPlanner::plan_binary_op` hook
never intercepts them, and DataFusion's native coercion can't reconcile `LargeBinary` vs
the `Int64` bounds. (`x BETWEEN x AND x` over decimal_arb works only because it needs no
coercion — byte comparison of equal operands.)
**Suggested fix:** an `AnalyzerRule` that rewrites a decimal_arb `Between`/`InList` into the
decimal_arb comparison UDFs (`>=`/`<=`/`=` chains), or expands them to binary ops before
the ExprPlanner runs.

### F2 — `CASE` over `decimal_arb` drops the extension metadata → sink fails/hangs
**Tests:** `edge_decimal_sql::sql_case_passthrough_metadata_tripwire`, `sql_nested_case`.
**Symptom:** the `CASE` output field is bare `LargeBinary` (no decimal_arb
metadata), so the Postgres NUMERIC insert fails and the sink retries to the
timeout (see F4). End-to-end confirmation of the gap already tracked in PR #37's
TODO and the `#[ignore]`d unit tripwire
`session::tests::case_over_decimal_arb_should_preserve_metadata`.
**Suggested fix:** a metadata-propagation rule so expression outputs (CASE,
COALESCE, …) whose inputs are `decimal_arb` retain the extension metadata.

### F3 — Standard Decimal128/256 → Postgres bound with the decimal point misplaced — **FIXED**
**Tests:** `edge_decimal_boundaries::{dec128_scale_equals_precision, dec128_negative_near_min, dec256_negative_high_scale}` (now assert the correct value lands), plus unit test
`value_binding::tests::test_unscaled_to_numeric_string`.
**Was:** `error returned from database: numeric field overflow` on INSERT for
all-fractional (scale == precision), large-negative, and high-scale shapes — even
into an over-sized NUMERIC.
**Root cause:** `value_binding::format_decimal_string` received the **unscaled
integer** (`Decimal128/256::value()` → e.g. `12345` for `123.45` at scale 2) but
**appended `scale` trailing zeros** instead of placing the decimal point `scale`
digits from the right. This inflated every non-zero-scale value by 10^scale —
silently wrong even when it fit, and overflowing the column once the inflated
integer part exceeded `precision − scale` digits. (decimal_arb dodged this: it is
converted to a canonical decimal string upstream and binds via the Utf8 path.)
**Fix:** renamed to `unscaled_to_numeric_string`, which interprets the input as an
unscaled integer and places the point correctly (sign-aware, sub-1 magnitudes
padded as `0.00…d`). Affects any pipeline sinking a raw `Decimal128/256` (scale > 0)
to Postgres NUMERIC.

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

### F7 — Nested decimal at the Kafka Avro sink — **FIXED**
**Test:** `edge_complex_decimal::nested_struct_decimal_arb_kafka_avro_sink_round_trip`
(full round-trip: nested decimal_arb → avro sink → re-read → print JSON shows the value),
plus unit `formats::avro::writer::tests::nested_struct_decimal_keeps_logical_type_in_schema`.
**Was:** `apache_avro`: *"Unsupported value-schema combination: Decimal vs Bytes"* — the
avro schema builder emitted **nested** decimals as plain `Bytes`, so the writer's
`Value::Decimal` failed to encode.
**Root cause:** `arrow_to_avro`'s `Struct` branch built the nested record schema via
`Schema::canonical_form()`, and Avro's Parsing Canonical Form **strips `logicalType`** —
demoting every nested decimal (decimal_arb *and* standard Decimal128/256) to `bytes`.
**Fix:** the struct path now assembles the nested record JSON directly
(`record_schema_json`), preserving nested logicalType. Nested decimals now encode at the
Avro sink. (Combined with the F6 fix, nested/complex decimal_arb is now supported at both
the JSON and Avro output boundaries; ClickHouse/Postgres still have no nested column type.)

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
