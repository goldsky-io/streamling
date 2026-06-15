# Contract: Aggregate UDF Signatures

**Spec**: [../spec.md](../spec.md) FR-007, FR-020 — **Plan**: [../plan.md](../plan.md) — **Data model**: [../data-model.md](../data-model.md) (E6)

These AggregateUDFs are registered with the `SessionContext` so that DataFusion's standard aggregate names (`SUM`, `MIN`, `MAX`, `AVG`, `COUNT`) resolve to them when the input column is `streamling.decimal_arb`. Authors invoke them via standard SQL syntax — no aliased names required.

## Resolution rule

`SUM(decarb_col)` resolves to `decimal_arb_sum_udaf` because the built-in `sum` cannot accept the `decimal_arb` extension type. DataFusion's aggregate resolver consults user-defined aggregates ahead of built-ins for unknown-to-built-in input types (research R4 OPEN — fallback is hooking `AggregateFunctionPlanner`).

## SUM

| Property | Value |
|---|---|
| Name | `sum` (registered as a UDAF; takes precedence over the built-in for `decimal_arb` inputs) |
| Signature | `decarb(p, s)` |
| Return type | `decarb(min(p + 16, MAX_PRECISION), s)` |
| Volatility | `Immutable` |
| State | `decarb(p_out, s)` accumulator |

Rationale for `+16` widening: supports up to ~10¹⁶ rows in the worst case before SUM overflows the declared precision. Pipelines summing larger volumes with the worst-case row magnitudes will surface FR-013 overflow errors at the offending row.

## MIN

| Property | Value |
|---|---|
| Name | `min` |
| Signature | `decarb(p, s)` |
| Return type | `decarb(p, s)` |
| Volatility | `Immutable` |
| State | `decarb(p, s)` (current minimum) |

Standard SQL: `MIN` returns NULL over an all-NULL group; otherwise the numerically smallest non-NULL value. Ordering uses the row converter from research R5.

## MAX

Symmetric to `MIN`.

## AVG

| Property | Value |
|---|---|
| Name | `avg` |
| Signature | `decarb(p, s)` |
| Return type | `decarb(p + 1, s + 1)` (capped at `MAX_PRECISION`) |
| Volatility | `Immutable` |
| State | `(decarb(p + 16, s), Int64)` — running sum and row count |
| Final | sum / count, rounded half-to-even to `(p+1, s+1)` |

Matches Postgres `AVG(numeric)` widening. Empty / all-NULL group → NULL (FR-008 edge case).

## COUNT

`COUNT(decarb_col)` and `COUNT(*)` continue to use the DataFusion built-in (returns `Int64`). No custom UDAF needed; DataFusion's `count` already handles arbitrary input types via its `Any`-typed input signature.

## Window functions

When the standard aggregates are used as window functions (`SUM(col) OVER (...)`), DataFusion's window operator dispatches the same `AggregateUDFImpl`. No additional registration required.

## Error surface

Same conventions as the scalar UDFs (FR-013 errors include column, value, declared precision/scale, and an actionable hint).
