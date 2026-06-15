# Quickstart — Retire U256/I256

**Spec**: [spec.md](./spec.md) — **Plan**: [plan.md](./plan.md) — **Contracts**: [contracts/](./contracts/)

After this feature lands, pipeline authors and platform operators see a single wide-numeric type — `decimal_arb` — at the streamling-internal layer. The on-wire representations (Avro `decimal`, Postgres `NUMERIC`, ClickHouse `UInt256`/`Int256`) are unchanged.

This walkthrough covers three scenarios:

1. **Avro source → Postgres sink** (uint256 stays unsigned on the wire, becomes `decimal_arb(78, 0)` internally, lands as `NUMERIC(78, 0)`)
2. **ClickHouse source → ClickHouse sink** (`UInt256` round-trips byte-exact through a streamling SQL transform — and a previously-broken `WHERE balance < 0` on an `Int256` column now returns correct results)
3. **Diagnosing a state-mismatch error on first restart after the migration**

All three are runnable on the existing `just env-setup` k3s cluster.

---

## Example 1 — Kafka Avro `decimal(78, 0)` → Postgres `NUMERIC(78, 0)`

### Setup

Postgres destination table (operator runs once):

```sql
CREATE TABLE balances (
  account_id BIGINT PRIMARY KEY,
  balance    NUMERIC(78, 0) NOT NULL
);
```

Kafka topic Avro schema (registered via Schema Registry):

```json
{
  "type": "record",
  "name": "BalanceEvent",
  "fields": [
    {"name": "account_id", "type": "long"},
    {"name": "balance",    "type": {
      "type": "bytes",
      "logicalType": "decimal",
      "precision": 78,
      "scale": 0
    }}
  ]
}
```

### Pipeline

```yaml
sources:
  events:
    type: kafka
    topic: balance-events
    starting_offsets: earliest
    primary_key: account_id

transforms: {}

sinks:
  out:
    type: postgres
    from: events
    table: balances
    schema: public
    primary_key: account_id
    on_conflict: update
```

### What happens internally

- `formats/avro/schema.rs` sees `decimal(78, 0)` → emits an Arrow field `balance: LargeBinary` with `decimal_arb(78, 0)` extension metadata and `native_int_kind=u256`.
- `formats/avro/arrow_array_reader.rs` decodes each row's bytes into the canonical decimal_arb encoding.
- `build_projection_for_postgres` projects the `decimal_arb` column to canonical text (via `DecimalArbToStringFunc`) before bind.
- The Postgres sink writes the canonical text into the `NUMERIC(78, 0)` column. The `native_int_kind=u256` hint is observed but ignored (Postgres has no native UInt256 alternative).

### Verification

```sql
SELECT balance FROM balances LIMIT 1;
-- Returns: 12345678901234567890... (up to 78 digits)
```

### Why this matters

Before this feature, the same pipeline ran via the u256 path. After: identical YAML, identical wire bytes in Kafka, identical Postgres column type, identical row values. The change is invisible to the operator.

---

## Example 2 — ClickHouse `Int256` round-trip with negative-aware sort

> **Status: deferred** — this example depends on ClickHouse source-side
> annotation (T009 in `tasks.md`) which is **not** implemented in
> feature 002. As of this feature, a ClickHouse `UInt256` / `Int256`
> source column lands as plain `FixedSizeBinary(32)` with no
> `native_int_kind` hint, so the sink can't auto-emit the matching
> native type. The byte-conversion logic for the eventual sink path
> is unit-tested in
> `streamling-connectors::table_providers::clickhouse::feature_002_byte_conversion_tests`,
> but the e2e shape below is aspirational and not validated by an
> automated test. The example remains here as a design reference for
> the follow-up that adds the source-side `system.columns` lookup.

### Setup

ClickHouse tables (operator runs once):

```sql
CREATE TABLE deltas
(
  id BIGINT,
  delta Int256
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE deltas_sorted
(
  id BIGINT,
  delta Int256
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO deltas VALUES
  (1, -100),
  (2, 0),
  (3, 1000),
  (4, -1),
  (5, 500);
```

### Pipeline (with the previously-broken sort)

```yaml
sources:
  src:
    type: clickhouse
    table_name: deltas
    primary_key: id

transforms:
  sorted:
    type: sql
    sql: "SELECT id, delta FROM src ORDER BY delta ASC"
    primary_key: id

sinks:
  out:
    type: clickhouse
    from: sorted
    table: deltas_sorted
    primary_key: id
```

### What happens internally

- ClickHouse source schema fetch sees the `Int256` column type, emits an Arrow field `delta: LargeBinary` with `decimal_arb(78, 0)` + `native_int_kind=i256`.
- The source-side batch read converts each ClickHouse `Int256` value (32 LE bytes, two's-complement) into canonical decimal_arb encoding (`[sign byte][BE magnitude]`).
- The SQL transform's `ORDER BY delta ASC` triggers the `DecimalArbSortRewriteRule` (feature 001's optimizer rule) which rewrites to `ORDER BY decimal_arb_to_sort_key(delta) ASC`. The sort-key encoder flips the sign byte on negatives so byte comparison produces numeric order.
- The ClickHouse sink sees `decimal_arb(78, 0) + native_int_kind=i256` and emits CREATE TABLE / INSERT as `Int256`. The batch insert converts canonical decimal_arb back to ClickHouse's 32 LE bytes two's-complement.

### Verification

```sql
SELECT id, delta FROM deltas_sorted ORDER BY id;
-- Returns rows in the order they landed — which is the sort order from the transform:
-- (1, -100), (4, -1), (2, 0), (5, 500), (3, 1000)
```

This is the **correctness fix** in action. The same pipeline today (pre-migration, running through u256/i256 + the bigint preprocessor) produces:

```
(2, 0), (5, 500), (3, 1000), (4, -1), (1, -100)
```

— negatives sorted *after* positives, because the two's-complement byte representation of `-1` (`0xFF...`) sorts byte-greater than `0` (`0x00...`).

### Why this matters

This is the silent correctness bug from US1. Any analytics computed on top of `ORDER BY i256_col` or `WHERE i256_col < N` today is wrong for mixed-sign data. After the migration, the same SQL produces correct results.

---

## Example 3 — Restarting a pre-migration pipeline resumes cleanly with no operator action

### Setup

A pipeline running on pre-migration streamling has been writing checkpoints. The operator deploys post-migration streamling without clearing state.

### What happens at start time

Streamling pipeline checkpoints record **source-side offsets only** — they carry no Arrow schema of in-flight data. So on restart, the pipeline:

1. Reads the stored offset from the state backend.
2. Resumes consuming from the source at that offset.
3. The source decodes each record per its unchanged wire schema (Avro `decimal(78, 0)` bytes / Postgres `NUMERIC(78, 0)` text / ClickHouse `UInt256` LE bytes — these formats are unchanged by this feature).
4. The new code routes the decoded record through `decimal_arb(78, 0)` + `native_int_kind=u256` instead of through the legacy `u256` type.
5. The sink emits to the same wire format as before.

No schema-mismatch error fires because no schema is stored in a checkpoint.

### Operator response

None required. Standard `kubectl rollout restart` (or operator-managed equivalent) is sufficient.

### Rollback

Symmetric. If the post-migration streamling needs to be replaced with the pre-migration streamling against the same checkpoint, the pre-migration code reads the offset, resumes from the source, decodes the unchanged wire formats, and routes through the legacy `u256` / `i256` path. Clean downgrade.

### Why this matters

FR-017 ("A pipeline restarted from a state checkpoint that pre-dates this migration MUST resume correctly without operator action") is satisfied trivially because the checkpoint format never represented in-flight schema. The wire formats this migration leaves untouched (Avro decimal bytes, Postgres NUMERIC, ClickHouse UInt256) carry no version skew either.

---

## What you do NOT need to do during this migration

- **No YAML edits**: source/sink configs are unchanged. Pipelines that were declaring `NUMERIC(78, 0)` or `decimal(78, 0)` or `UInt256` continue to work.
- **No ClickHouse schema edits**: existing tables with `UInt256` / `Int256` columns stay as-is. The sink continues to emit those types.
- **No streamling-side type knowledge in your SQL**: SQL transforms continue to write `+`, `-`, `*`, `<`, `>`, `SUM(col)`, `ORDER BY col` — all of which now work correctly through the decimal_arb implementation, including for mixed-sign signed values.
- **No new UDF invocations**: `u256_to_string(col)` and `i256_to_string(col)` continue to be parseable for backwards compatibility (they delegate to `decimal_arb_to_string`); but new pipelines should prefer the natural `CAST(col AS TEXT)` form per US3.

## What might surprise you

- **A `decimal_arb(78, 0)` column with `native_int_kind=u256` that ends up holding a negative value** (e.g. via subtraction in a SQL transform) lands successfully if the sink is Postgres (`NUMERIC(78, 0)` handles negatives) but rejects on a ClickHouse `UInt256` sink with a row-attribute error. This is correct behavior — the hint says "this column should round-trip as UInt256"; once a row violates that contract, the sink surfaces it.
  - To handle this safely, either route the sink to a `Decimal(78, 0)` column instead, or set `coerce_to: string`, or change the upstream SQL transform so subtractions can't produce negatives.

- **Performance**: BigDecimal-based arithmetic replaces native 256-bit math for wide-integer columns. Expected impact for I/O-bound pipelines: minimal. Expected impact for math-bound pipelines: 5–20%. If a real workload regresses outside that band, the follow-up is a fixed-width fast-path inside `decimal_arb_ops.rs` for the `(78, 0)` and `(77, 0)` shapes — not a re-introduction of the dedicated types.
