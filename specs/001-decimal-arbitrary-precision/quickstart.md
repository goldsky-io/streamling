# Quickstart: Arbitrary-Precision Decimal Type

**Spec**: [spec.md](./spec.md) — **Plan**: [plan.md](./plan.md) — **Contracts**: [contracts/](./contracts/)

A pipeline author who has shipped pipelines using existing fixed-width decimal columns can adopt the new type with no code or transform-rewrite work. This quickstart walks through three end-to-end pipelines that exercise the type:

1. **Postgres → SQL transform → Postgres** — the canonical lossless round-trip.
2. **Kafka (JSON) → Postgres** — the most common ingestion shape; JSON digit-strings in, NUMERIC out.
3. **Postgres → ClickHouse** — destination caps at 76; the FR-019 `coerce_to: string` opt-in is required.

All three are runnable on the existing `just env-setup` k3s cluster (per `AGENTS.md`).

## Prerequisites

- `just env-setup` succeeded (k3s with Postgres, ClickHouse, Kafka).
- The streamling binary is built (`just build`).
- A `psql` and a Kafka producer client of choice for inserting test data.

## Example 1 — Postgres → SQL → Postgres (lossless round-trip)

### Schema setup

```sql
-- in Postgres
CREATE TABLE balances (
  account_id BIGINT PRIMARY KEY,
  amount NUMERIC(100, 18) NOT NULL
);

CREATE TABLE adjusted_balances (
  account_id BIGINT PRIMARY KEY,
  amount NUMERIC(100, 18) NOT NULL
);

INSERT INTO balances VALUES
  (1, '12345678901234567890123456789012345678901234567890123456789012345678901234567890.123456789012345678'),
  (2, '-99.0'),
  (3, '0');
```

The `amount` column declares `NUMERIC(100, 18)` — precision exceeds the 76-digit fixed-width ceiling, so streamling auto-promotes it to `decimal_arb(100, 18)` (FR-015).

### Pipeline

```yaml
sources:
  src:
    type: postgres
    table: balances
    primary_key: account_id

transforms:
  doubled:
    type: sql
    sql: SELECT account_id, amount * 2 AS amount FROM src

sinks:
  out:
    type: postgres
    from: doubled
    table: adjusted_balances
    primary_key: account_id
    on_conflict: update
```

### Expected behavior

- The SQL transform's `amount * 2` widens per E5 multiplication rule: input `decimal_arb(100, 18)` × built-in integer `2` → `decimal_arb(102, 18)`. The Postgres sink accepts wider-than-declared input by storing into `NUMERIC(100, 18)` if and only if the runtime value fits — values that would not fit surface FR-013 errors.
- After `streamling run`, `SELECT * FROM adjusted_balances` shows the doubled values byte-for-byte (within the `(100, 18)` declaration).

### What to verify

```sql
SELECT amount FROM adjusted_balances WHERE account_id = 1;
-- Expect: 24691357802469135780246913578024691357802469135780246913578024691357802469135780.246913578024691356
```

(The trailing two digits demonstrate the half-to-even rounding rule applied at the `(100, 18)` cap during the multiply.)

## Example 2 — Kafka (JSON) → Postgres

### Topic setup

```bash
kafka-topics --create --topic payments --bootstrap-server localhost:9092
```

Producer message (JSON):

```json
{ "id": 1, "amount": "1234567890.987654321098765432109876543210" }
```

(40 fractional digits — well past `Decimal256`'s 76 total ceiling once you account for the integer side; the string-encoded JSON is the right wire format.)

### Pipeline

```yaml
sources:
  payments_in:
    type: kafka
    topic: payments
    starting_offsets: earliest
    primary_key: id
    encoding: json
    schema:
      columns:
        - name: id
          type: bigint
        - name: amount
          type: decimal
          precision: 80
          scale: 40

transforms: {}

sinks:
  payments_out:
    type: postgres
    from: payments_in
    table: payments
    primary_key: id
```

The Postgres `payments.amount` column must be declared `NUMERIC(80, 40)` (or wider). If it's narrower, the pipeline is rejected at config load with the error from `connector-capability.md`.

### What to verify

After producing one message and `streamling run`:

```sql
SELECT amount FROM payments WHERE id = 1;
-- Expect: 1234567890.987654321098765432109876543210
```

Bit-for-bit equal to the JSON digit-string (SC-001).

## Example 3 — Postgres → ClickHouse with opt-in

### Postgres source

Same `balances` table as Example 1.

### ClickHouse target

```sql
-- in ClickHouse
CREATE TABLE balances_ch (
  account_id Int64,
  amount String
) ENGINE = MergeTree() ORDER BY account_id;
```

The `amount` column is `String` because ClickHouse's `Decimal` caps at 76 (precision >76 is not native).

### Pipeline (without opt-in — REJECTED)

```yaml
sources:
  src:
    type: postgres
    table: balances
    primary_key: account_id

transforms: {}

sinks:
  ch:
    type: clickhouse
    from: src
    table: balances_ch
    primary_key: account_id
```

Running this pipeline produces a config-load error:

```
config error: column `pipeline.sinks.ch.amount` (declared decimal_arb(100, 18))
cannot be emitted to ClickHouse sink `ch`: ClickHouse Decimal precision is capped at 76.
hint: add `coerce_to: string` under this column in the sink YAML to emit as a String column,
      or reduce declared precision to ≤76 if the source data fits.
```

### Pipeline (with opt-in — succeeds)

```yaml
sources:
  src:
    type: postgres
    table: balances
    primary_key: account_id

transforms: {}

sinks:
  ch:
    type: clickhouse
    from: src
    table: balances_ch
    primary_key: account_id
    columns:
      - name: amount
        coerce_to: string
```

After `streamling run`:

```sql
-- in ClickHouse
SELECT amount FROM balances_ch WHERE account_id = 1;
-- Expect: '12345678901234567890123456789012345678901234567890123456789012345678901234567890.123456789012345678'
```

The string is the canonical decimal representation. Numeric semantics are forfeit on the ClickHouse side (no `SUM(amount)` etc.) — this is the documented tradeoff of the opt-in.

## Verifying SC-006 — no transform rewrites

A pipeline that today reads `Decimal256(70, 18)` (within the existing ceiling) can be migrated to `decimal_arb(100, 18)` by changing only the source schema declaration. None of the SQL transform code (arithmetic, ORDER BY, GROUP BY, aggregates) needs to change. This is the SC-006 acceptance — and Examples 1 and 2 demonstrate it: their SQL is plain `SELECT account_id, amount * 2 AS amount FROM src`, with no `decimal_arb_*` function calls.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Config load error mentioning `coerce_to: string` | Sink destination cannot natively hold the declared precision | Add the directive (Example 3) or reduce declared precision |
| Runtime error "value `<v>` exceeds declared precision" | A row's actual magnitude exceeds the column's `(precision, scale)` declaration | Widen the declared precision or filter the offending row |
| Postgres source still maps `NUMERIC(100, 18)` to a fixed-width type | Streamling not yet upgraded to the version including this feature | Verify `streamling --version`; auto-promotion lands as part of the FR-018 fix |
| ClickHouse sink emits column as `String` without an explicit `coerce_to` | Old streamling version with the now-retired silent fallback | Upgrade; absent the directive, the pipeline must be rejected (FR-011) |
