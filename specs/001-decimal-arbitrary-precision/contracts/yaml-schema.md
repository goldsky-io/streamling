# Contract: YAML Schema Additions

**Spec**: [../spec.md](../spec.md) FR-015, FR-019 — **Plan**: [../plan.md](../plan.md) — **Data model**: [../data-model.md](../data-model.md) (E4)

This contract enumerates every YAML change required to support the new type. There are **no new top-level keywords** for type selection — the new type is auto-promoted whenever the declared precision exceeds 76 (FR-015). The only YAML addition is a per-column directive on **sinks** for the FR-019 string-coercion opt-in.

## Auto-promotion: no YAML changes for the typical case

A pipeline like:

```yaml
sources:
  orders_pg:
    type: postgres
    table: orders
    primary_key: id
```

…where the Postgres `orders.amount` column is declared `NUMERIC(100, 18)`, automatically gets a `decimal_arb(100, 18)` Arrow column on the source side. Authors do not write any new YAML.

For YAML-declared schemas (e.g., a Kafka source whose payload schema is in YAML rather than registry-derived), declaring a column with `precision >76` is sufficient to opt in:

```yaml
sources:
  payments_kafka:
    type: kafka
    topic: payments
    encoding: json
    schema:
      columns:
        - name: amount
          type: decimal
          precision: 100   # >76 → auto-promoted to decimal_arb
          scale: 18
```

(The exact YAML schema-declaration grammar follows existing conventions for Kafka sources; this example illustrates the precision/scale fields.)

## FR-019 opt-in: `coerce_to`

A per-column directive on a **sink** that allows a `decimal_arb` column to be emitted to a destination that cannot natively hold it. The directive MUST be explicit — connectors do not infer it from the destination's capabilities.

### Grammar

```yaml
sinks:
  analytics_ch:
    type: clickhouse
    from: orders_pg
    table: orders_analytics
    primary_key: id
    columns:
      - name: amount
        coerce_to: string    # FR-019 opt-in: emit as ClickHouse String
```

### Allowed values

| Value | Effect |
|---|---|
| `string` | Emit the column to the destination as a string field, using canonical decimal text (per Arrow extension-type contract §3). The destination column type is a string type (e.g., ClickHouse `String`, Protobuf `string`). |

(`string` is the only value defined in v1. Future values like `bigint_truncated` could be added; this contract does not pre-allocate them.)

### Validation

At config load:
- If the column is `decimal_arb` and the connector returns `OptInOnly(CoerceToString)`, the directive `coerce_to: string` is REQUIRED. Absent, the pipeline is rejected per FR-011.
- If the column is **not** `decimal_arb`, the directive is rejected with: "`coerce_to: string` only applies to arbitrary-precision decimal columns; column `<name>` is `<type>`."
- If the connector returns `Native`, the directive is allowed but logged at INFO ("`coerce_to: string` set on a column the destination can carry natively; coercion is unnecessary but accepted").
- If the connector returns `Reject` even with the directive, the pipeline is rejected.

### Source-side: not allowed

`coerce_to` is a **sink-side** directive. It does not appear on sources. Sources accept whatever the underlying store advertises; if a source presents an `decimal_arb` column the engine cannot use, the rejection happens at config load via the connector capability matrix (E4) — without any YAML directive.

## Schema parsing (`streamling-config`)

The `streamling-config` crate's column-directives struct gains:

```rust
pub struct ColumnDirectives {
    pub coerce_to: Option<CoercionDirective>,
    // ... existing fields ...
}

pub enum CoercionDirective {
    String,
}
```

Parsed with `#[serde(deny_unknown_fields)]` per CONN-002 in `AGENTS.md` (so a typo like `coerce_to: stirng` is rejected at config load).

## Examples

### Postgres → Postgres (no YAML changes needed)

```yaml
sources:
  src:
    type: postgres
    table: balances
    primary_key: account_id

transforms:
  enriched:
    type: sql
    sql: SELECT account_id, balance * 1.05 AS adjusted FROM src

sinks:
  out:
    type: postgres
    from: enriched
    table: adjusted_balances
    primary_key: account_id
```

If `balances.balance` is `NUMERIC(120, 30)`, `adjusted` ends up as `decimal_arb(124, 32)` (per E5 multiplication widening), and `adjusted_balances.adjusted` must be a Postgres `NUMERIC` of equal-or-wider precision/scale or the pipeline is rejected at config load.

### Postgres → ClickHouse with opt-in

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
      - name: balance
        coerce_to: string
```

Without the `coerce_to: string` line, this pipeline is rejected at config load with the error message described in `connector-capability.md`.

### Kafka JSON → Postgres (no YAML changes needed)

JSON natively carries digit-strings; no opt-in needed on the Kafka source. The Postgres sink accepts the column natively because Postgres `NUMERIC` is unbounded.
