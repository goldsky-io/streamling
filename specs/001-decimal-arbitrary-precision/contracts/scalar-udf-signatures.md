# Contract: Scalar UDF Signatures

**Spec**: [../spec.md](../spec.md) FR-003, FR-004, FR-009, FR-013, FR-014, FR-016, FR-017, FR-020 — **Plan**: [../plan.md](../plan.md) — **Data model**: [../data-model.md](../data-model.md) (E5)

These ScalarUDFs are registered on every `SessionContext` that streamling builds. They are **auxiliary**: the binding contract for authors is the native SQL operator surface (`a + b`, `a < b`, `CAST(a AS DECIMAL(...))`), routed to these UDFs by the `ExprPlanner` (research R3). Authors MAY call them by name for parity with `u256_*` / `i256_*`.

Notation: `decarb(p, s)` denotes the `decimal_arb` extension type with declared `(precision, scale)`. `decarb(?, ?)` is a wildcard for any declared instance.

## Arithmetic

| UDF name | Signature | Return type | Notes |
|---|---|---|---|
| `decimal_arb_add` | `decarb(p1, s1), decarb(p2, s2)` | `decarb(max(p1-s1, p2-s2) + max(s1, s2) + 1, max(s1, s2))` | rounds excess scale per R8 (half-to-even) |
| `decimal_arb_sub` | `decarb(p1, s1), decarb(p2, s2)` | same as `_add` | |
| `decimal_arb_mul` | `decarb(p1, s1), decarb(p2, s2)` | `decarb(p1 + p2 + 1, s1 + s2)` | overflow surfaces FR-013 error if cap exceeded |
| `decimal_arb_div` | `decarb(p1, s1), decarb(p2, s2)` | `decarb(p1 - s1 + s2 + max(s1, default_div_scale), max(s1, default_div_scale))` | division by zero → engine NULL/error (per existing decimal semantics, FR-008 edge case); `default_div_scale = 18` |
| `decimal_arb_mod` | `decarb(p1, s1), decarb(p2, s2)` | `decarb(min(p1-s1, p2-s2) + max(s1, s2), max(s1, s2))` | follows SQL standard for `MOD(NUMERIC)` |
| `decimal_arb_neg` | `decarb(p, s)` | `decarb(p, s)` | unary minus |
| `decimal_arb_abs` | `decarb(p, s)` | `decarb(p, s)` | absolute value |
| `decimal_arb_round` | `decarb(p, s), Int64 (target_scale)` | `decarb(p, target_scale)` | half-to-even; target_scale ≥ 0 |

Mixed-operand variants exist as overloads via the coercion table (E5); they cast the narrow operand to `decimal_arb` and dispatch to the same impl.

## Comparison

Each comparison returns `Boolean`. NULLs propagate per standard SQL three-valued logic (FR-008).

| UDF name | Signature |
|---|---|
| `decimal_arb_eq` | `decarb(?, ?), decarb(?, ?)` |
| `decimal_arb_neq` | `decarb(?, ?), decarb(?, ?)` |
| `decimal_arb_lt` | `decarb(?, ?), decarb(?, ?)` |
| `decimal_arb_lte` | `decarb(?, ?), decarb(?, ?)` |
| `decimal_arb_gt` | `decarb(?, ?), decarb(?, ?)` |
| `decimal_arb_gte` | `decarb(?, ?), decarb(?, ?)` |

Comparison ignores declared scale: `decimal_arb("1.0", scale=1) = decimal_arb("1.000", scale=3)` is `TRUE`. Canonicalization in the extension-type contract makes this byte-equal as well.

## Casts

Cast UDFs are registered both as named functions and as coercion entries so that DataFusion's `CAST(expr AS DECIMAL(p, s))` path resolves to them when `p > 76` (FR-009, FR-015 auto-promotion).

| UDF name | Source signature | Target signature | Notes |
|---|---|---|---|
| `to_decimal_arb_from_str` | `Utf8 \| LargeUtf8` | `decarb(p, s)` (where p, s declared by call site) | strict parse; FR-013 error on bad input or out-of-range |
| `to_decimal_arb_from_decimal128` | `Decimal128(p1, s1)` | `decarb(p, s)` | always lossless when widening; rounds when narrowing |
| `to_decimal_arb_from_decimal256` | `Decimal256(p1, s1)` | `decarb(p, s)` | same |
| `to_decimal_arb_from_int` | `Int8 \| Int16 \| Int32 \| Int64` | `decarb(p, s)` | always lossless; scale=0 input |
| `to_decimal_arb_from_float` | `Float32 \| Float64` | `decarb(p, s)` | lossy by nature; surfaces a WARN log once per call site (cast can introduce IEEE 754 imprecision) |
| `decimal_arb_to_string` | `decarb(p, s)` | `Utf8` | canonical decimal string per Arrow contract §3 / FR-017 |
| `decimal_arb_to_decimal128` | `decarb(p, s)` | `Decimal128(p_target, s_target)` | FR-013 error if out of range; rounds excess scale |
| `decimal_arb_to_decimal256` | `decarb(p, s)` | `Decimal256(p_target, s_target)` | same |
| `decimal_arb_to_int64` | `decarb(p, s)` | `Int64` | rounds (half-to-even) and FR-013 error if out of range |
| `decimal_arb_to_float64` | `decarb(p, s)` | `Float64` | lossy by nature; same WARN treatment |

## Volatility & determinism

All UDFs above are `Volatility::Immutable` and deterministic given identical inputs.

## Error surface

All UDFs return errors via `StreamlingError` (per `AGENTS.md` RUST-003 conventions) with:
- column name (when invoked with a column reference)
- the offending value (formatted via canonical string)
- the declared `(precision, scale)` of the result
- an actionable hint (e.g., "increase declared precision to ≥ N", "use `decimal_arb_round` to drop fractional digits")

This satisfies FR-013.
