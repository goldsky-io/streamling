# Contract: Postgres Wide-Integer Routing

## Source side (Postgres → decimal_arb)

### Type mapping

The Arrow type returned by `postgres_type_to_arrow_field(pg_type, name, nullable)` in `crates/streamling-core/src/utils/pg.rs` for `NUMERIC(p, s)` columns:

| Postgres `NUMERIC` shape | Today | After this feature |
|---|---|---|
| `NUMERIC(p, s)` where `p ≤ 38` | `Decimal128(p, s)` | unchanged |
| `NUMERIC(p, s)` where `38 < p ≤ 76` | `Decimal256(p, s)` | unchanged |
| `NUMERIC(78, 0)` | `FixedSizeBinary(32)` + U256 metadata | `LargeBinary` + decimal_arb `(78, 0)` + `native_int_kind=u256` |
| `NUMERIC(p, s)` where `p > 76` (general) | `LargeBinary` + decimal_arb `(p, s)` (no hint) | unchanged |

The `NUMERIC(78, 0)` special case preserves the existing convention that this shape historically meant "uint256 storage" — see research.md "Open questions deferred to implementation" §2, where the original "don't try; emit with no hint" decision was reversed during implementation to avoid silently regressing Postgres-sourced `NUMERIC(78, 0)` columns to `Decimal(78, 0)` storage on downstream ClickHouse sinks.

### Why no `i256` hint from Postgres source

Postgres `NUMERIC` is mathematically signed (can hold negative values). There is no `NUMERIC` shape that's exclusively signed-256-bit. As a result, the source can confidently hint `u256` (for the conventional 78-digit shape) but cannot confidently hint `i256` — that would require additional out-of-band knowledge (e.g. a column comment, an `init-options.json` directive, or a YAML override). Decision: don't try; a Postgres `NUMERIC(p, 0)` with `77 ≤ p ≤ 78` lands at downstream ClickHouse as `Decimal(78, 0)` rather than `Int256`. That's a niche regression in storage compactness (signed wide-int Postgres → ClickHouse pipelines are rare), not correctness.

If users hit this in practice, the follow-up is a YAML-level signedness override on the source — out of scope for this feature.

## Sink side (decimal_arb → Postgres)

No change from feature 001. The existing `decimal_arb` → `NUMERIC(p, s)` projection in `build_projection_for_postgres` + the `get_postgres_type_info` mapping in `pg.rs` continue to handle every `decimal_arb` column. The `native_int_kind` hint is ignored for Postgres — `NUMERIC` is the universal wide-decimal storage type, and no native fixed-width alternative exists in Postgres.

The only edit to the Postgres connector is **deletion**: the `U256Type::is_u256_metadata` / `I256Type::is_i256_metadata` branch in `get_postgres_type_info` (`pg.rs:104-113`) is removed once the source-side routing flips — there are no more `FixedSizeBinary(32)` + U256/I256 metadata fields in the pipeline.

## Invariants

1. **Existing Postgres → Postgres pipelines unchanged**: a `NUMERIC(78, 0)` source column flowing to a `NUMERIC(78, 0)` sink column round-trips byte-exact.
2. **Existing Postgres → ClickHouse uint256 pipelines preserved**: a `NUMERIC(78, 0)` source paired with a ClickHouse `UInt256` sink continues to work end-to-end, because the source emits `decimal_arb(78, 0)` with `native_int_kind=u256` and the ClickHouse sink's wire-format adapter emits `UInt256` for that hint.
