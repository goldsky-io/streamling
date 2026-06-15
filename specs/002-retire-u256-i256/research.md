# Research Notes — Retire U256/I256

This document consolidates Phase 0 findings. Each section ends with a **Decision / Rationale / Alternatives** triplet that informs the data model and contracts.

---

## R1. Where to store the unsigned/signed origin hint on a `decimal_arb` field

**Question**: A `decimal_arb` value is always signed in its canonical encoding (sign byte + BE magnitude). But pipelines that originated from a `UInt256` or `Int256` ClickHouse column — or an Avro `decimal(78, 0)` that conventionally meant uint256 — must round-trip back to the same native ClickHouse type on the sink. Where does that hint live?

**Existing Arrow metadata convention on decimal_arb fields** (per `crates/streamling-common/src/types/decimal_arb.rs`):

- `ARROW:extension:name = "streamling.decimal_arb"`
- `ARROW:extension:metadata = '{"precision":<u32>,"scale":<u32>}'`

Adding a third key — e.g. `streamling.native_int_kind` — is cheap; Arrow field metadata is an open `HashMap<String, String>`.

**Decision**: Add an optional sibling key on the field metadata map:

```
streamling.native_int_kind = "u256" | "i256"
```

Absent key = generic decimal_arb (no native sink preference). Present + value = a hint that a connector with a matching native channel may use to choose a fixed-width sink type.

**Rationale**:
- Keeps the *type identity* (precision, scale) separate from a *propagation hint* (origin native type). Two `decimal_arb(78, 0)` columns with different `native_int_kind` are still the same type for arithmetic, sort, aggregation, comparison purposes — just emitted differently if the sink has a native channel.
- Avoids overloading the existing `ARROW:extension:metadata` JSON, which would force every consumer to re-parse that JSON to see the hint.
- Survives Arrow IPC round-trips (Arrow preserves arbitrary field metadata keys natively — already verified in feature 001 via the IPC round-trip test for decimal_arb).

**Alternatives considered**:
- **Embed in extension metadata JSON**: rejected — forces every reader of `precision`/`scale` to also handle the new key, and bloats the JSON.
- **Two new extension types (`streamling.decimal_arb_u256`, `streamling.decimal_arb_i256`)**: rejected — would re-introduce the type-multiplication problem this feature is solving, and every UDF / ExprPlanner registration would need to handle three type-identity flavors.
- **Track signedness only via the value's sign bit**: rejected — a single-row sample doesn't tell you whether the column is *semantically* signed; you need the column-level hint to set sink storage correctly regardless of which row values happened to flow through.

---

## R2. How ClickHouse's HTTP `FORMAT Arrow` represents UInt256/Int256

**Question**: When the ClickHouse source executes `SELECT * FROM t LIMIT 1 FORMAT Arrow` against a table with a `UInt256` column, what `DataType` does the Arrow IPC schema carry?

**Finding** (corrected from an earlier draft): ClickHouse emits `UInt256` and `Int256` columns as Arrow `FixedSizeBinary(32)` in its Arrow IPC output. `clickhouse.rs::fetch_schema` returns the Arrow schema from a `LIMIT 1 FORMAT Arrow` probe and lands these columns as plain `FixedSizeBinary(32)`.

**An earlier draft of this section asserted that the current code performs a post-fetch `system.columns` lookup that stamps `U256Type::metadata()` / `I256Type::metadata()` onto these fields. That was wrong.** Neither `fetch_schema` nor `normalize_schema_for_clickhouse` walks `system.columns` for source-side annotation, and no production code path stamps that metadata. Verified by reading the actual code and noted in `tasks.md` T009 ("the ClickHouse source today returns `FixedSizeBinary(32)` for `UInt256`/`Int256` columns *without* attaching U256Type/I256Type metadata").

The practical consequence: today's ClickHouse `UInt256` source column flows downstream as plain bytes with no provenance hint. A ClickHouse-source-to-ClickHouse-sink pipeline that doesn't use a `schema_override` lands the column as `FixedString(32)` at the sink, not `UInt256`. This is pre-existing behavior, not introduced or "broken" by feature 002.

**Decision**: This feature explicitly **defers** source-side hint stamping for ClickHouse. The sink-side hint-honoring path *does* land (so an Avro- or Postgres-sourced `decimal_arb(78, 0) + native_int_kind=u256` column does emit as `UInt256` on a downstream ClickHouse sink). Adding a post-fetch `system.columns` lookup that stamps `native_int_kind` on ClickHouse source fields is a meaningful new feature, in scope for a follow-up. The known limitation is documented in `docs/decimal-arbitrary-precision.md` with a workaround (pair the ClickHouse source with a Kafka/Postgres source for the hint to propagate).

**Rationale**:
- Preserves the streamling-side type uniformity for the cases that *do* have a source-side hint (Avro, Postgres).
- Avoids scope creep — the `system.columns` annotation step is genuinely a new code path, not a rename of an existing one, and would benefit from its own design + test surface.
- The byte-conversion logic that would live at the ClickHouse source boundary (FSB(32) LE → decimal_arb canonical BE+sign) is straightforward but untested; deferring keeps the failure surface small for the routing-flip change.

**Alternatives considered**:
- **Land source-side annotation in this feature**: rejected — would add ~150 LOC of new behavior with no test coverage from feature 001 to lean on, and would gate the routing-flip on a feature unrelated to retiring `u256`/`i256`.
- **Keep ClickHouse columns as FSB(32) all the way through**: rejected for *Avro/Postgres-sourced* hinted columns — bypasses every decimal_arb capability (arithmetic, comparison, aggregation, sort, CAST) and re-introduces the BigIntKind preprocessor as the only way to use them. This rejection still holds; the deferred piece is only ClickHouse-source stamping.

---

## R3. Avro decimal logical-type signedness convention

**Question**: An Avro `decimal(p, s)` logical type encodes its unscaled value as a signed BigInt (`apache_avro::Decimal`). How do we decide whether an inbound `decimal(78, 0)` should be hinted `u256` or `i256` for sink purposes?

**Finding**: There is no native Avro convention for "this decimal is unsigned" — it's always mathematically signed. The existing streamling Avro reader (`formats/avro/schema.rs`) routes `decimal(p, 0)` columns by precision: `p ≥ 78` → U256Type, `p == 77` → I256Type, `p ≤ 76` → Decimal128/256. This precision-driven split matches the source semantic for the historical blockchain pipelines: Ethereum `uint256` values need 78 digits and arrive as `decimal(78, 0)`; signed 256-bit values fit in 77 digits and arrive as `decimal(77, 0)`.

**Decision**: Preserve the existing convention. Avro decimal routing:

| Avro shape | Arrow output | `native_int_kind` |
|---|---|---|
| `decimal(p, 0)` where `p ≥ 78` | `decimal_arb(p, 0)` | `u256` |
| `decimal(p, 0)` where `77 ≤ p < 78` | `decimal_arb(p, 0)` | `i256` |
| `decimal(p, 0)` where `p ≤ 76` | `Decimal128(p, 0)` or `Decimal256(p, 0)` | — (unchanged) |
| `decimal(p, s)` where `p > 76`, `s > 0` | `decimal_arb(p, s)` | — (no hint; fractional) |

The 77-vs-78 split mirrors the existing u256/i256 boundary today.

**Rationale**: Operationally identical to today's u256/i256 routing — no surprises for existing pipelines. The `native_int_kind` hint preserves "this came from a uint256-shaped source" semantics so the ClickHouse sink can still emit `UInt256` for it.

**Alternatives considered**:
- **Default to `i256` for all `decimal(p, 0)` with `p > 76`**: rejected — would silently promote every existing Ethereum pipeline to `Int256` storage, breaking US4.
- **Require user to specify signed/unsigned in YAML**: rejected — out of scope per the feature spec; existing pipelines must work unchanged.
- **Infer from sample values at runtime**: rejected — sample-based inference is unreliable (a uint256 column whose first values happen to be small looks the same as an int256 column with all-positive sample values).

---

## R4. Migration safety for in-flight pipeline state

**Question**: A streamling pipeline that's been running on the old code-base has stored checkpoint state. When that pipeline is restarted under the new code-base, what happens?

**Finding** (corrected from an earlier wrong claim in this section): streamling checkpoints record **source-side offsets only**. They do not carry the Arrow schema of in-flight data. On restart the pipeline reads the offset, resumes consuming from the source, and decodes records per the source's wire schema — which doesn't change with this feature (Avro `decimal` bytes / Postgres `NUMERIC` text / ClickHouse `UInt256` bytes are identical pre- and post-migration). The new code routes each decoded record through `decimal_arb` instead of `u256` / `i256`; the sink emits the same wire bytes either way.

**Decision**: **No migration step is required.** Pipelines transparently upgrade and downgrade across the feature boundary. The earlier draft of this research note claimed there was a schema-mismatch path; that was wrong (verified against the actual `streamling-state` backend, which only stores offset records, not schemas).

**Rationale**: Because the wire formats don't change and the checkpoint format doesn't carry schema, there's no incompatibility surface. FR-017 ("A pipeline restarted from a state checkpoint that pre-dates this migration MUST resume correctly without operator action") is satisfied trivially — there is no path that re-interprets stored state against a different schema, because the stored state is only an offset.

**Alternatives considered**:
- **Add schema-aware checkpointing as part of this feature** to make a future type retirement louder: rejected — out of scope; would also be a large behavioral change to the state backend with implications well beyond wide integers.

---

## R5. Surviving the "where does the BigIntKind preprocessor still help" question

**Question**: The bigint SQL preprocessor (`crates/streamling-core/src/types/bigint_sql_preprocessor.rs`) is 1,892 lines, of which the spec estimates ~1,500 are exclusively u256/i256 binary-op rewriting. Is that estimate accurate? And what's left after stripping the BigIntKind machinery?

**Finding** (from grepping the file):

| Concern | LOC (planned) | LOC (actual, post-implementation) | Disposition |
|---|---|---|---|
| `BigIntKind` trait + `U256Kind`/`I256Kind` impls | ~30 | ~30 | Deleted |
| `rewrite_expr_kind::<K>` and helpers | ~250 | ~250 | Deleted |
| `is_bigint_expr`, `is_kind_func_call`, `parse_wrapped_fn` machinery (generic over K) | ~250 | ~250 | Deleted |
| Binary-op AST walker that hardcodes BigIntKind dispatch | ~400 | ~400 | Deleted |
| `preprocess_bigint_binary_ops_with_schema` (the SessionContext-aware entry point) | ~150 | simplified to ~60 (now decimal_arb-CAST-only) | Trimmed, not deleted (decimal_arb CAST-rewrite stayed here) |
| CAST-to-DECIMAL(p > 76, s) → decimal_arb routing | ~150 | ~150 | Kept |
| Statement / SetExpr / CTE traversal scaffolding | ~250 | ~250 | Kept (used by the CAST rewrite path) |
| Decimal-arb CAST-to-string rewrite (new in T019) | — | ~120 | Added |
| Tests | ~400 | ~300 (migrated/deleted/added) | Reshaped |

**Actual outcome**: the surviving file is **905 LOC**, down from 1,892 (a ~52% reduction). The original ~600 LOC estimate was too aggressive because it undercounted (a) the traversal scaffolding and helper code that the decimal_arb CAST path also depends on, and (b) the new ~120-LOC decimal_arb CAST-to-string rewrite added by T019 that didn't exist when the estimate was made. The order-of-magnitude conclusion (drop the BigIntKind machinery, keep the decimal_arb CAST routing) is unchanged.

**Decision**: Retain the CAST-to-DECIMAL rewriting path (it's how pipelines declare wide-precision intent in their SQL); strip everything else.

**Rationale**: The CAST path is the only piece of the preprocessor that operates at the SQL-text level *before* DataFusion sees the query. It serves a fundamentally different role from the BigIntKind binary-op rewrites — those compensated for missing ExprPlanner integration, which decimal_arb already has.

**Alternatives considered**:
- **Delete the entire preprocessor file**: rejected — the CAST routing is needed by feature 001's wide-DECIMAL CAST acceptance test (`test_preprocess_decimal_100_to_decimal_arb`) and supports the natural pattern of `CAST(col AS DECIMAL(100, 18))` for migrating to wide precision.
- **Move the CAST routing into a DataFusion analyzer pass**: deferred — viable but adds a new code path; the existing regex-based pre-pass is simple and well-tested.

---

## R6. Connector capability matrix interaction

**Question**: Does the `native_int_kind` hint change the connector capability matrix from feature 001?

**Finding**: The capability matrix (`crates/streamling-common/src/types/decimal_arb_capability.rs`) decides per-(column, connector) whether a column can be carried `Native`, `OptInOnly` (with `coerce_to: string`), or `Reject`. The decision today is based on `(precision, scale, connector_kind, coerce_to_string)`.

**Decision**: Two changes:

1. The ClickHouse connector's branch in `capability_for_decimal_arb` learns to consider `native_int_kind`. If the hint is `u256` or `i256` and the precision is 78/77 respectively with scale 0, the result becomes `Native` *without needing `coerce_to: string`* — because the wire-format adapter emits `UInt256` / `Int256`, not `Decimal(78, 0)`.

2. The Hybrid connector (ClickHouse-backed) gets the same treatment.

For non-ClickHouse connectors, the hint is ignored (Postgres NUMERIC handles up to 1000 digits natively; Kafka JSON/Avro/Protobuf handle decimals via existing logical types).

**Rationale**: Maintains backwards compatibility for existing ClickHouse `UInt256`/`Int256` pipelines, which today are `Native` and must stay `Native` post-migration (FR-014).

**Alternatives considered**:
- **Always require `coerce_to: string` for `decimal_arb(78, 0)` on ClickHouse**: rejected — would break every existing wide-int ClickHouse pipeline on rollout day.

---

## Open questions deferred to implementation

1. **Test migration strategy**: The bigint preprocessor's ~400 LOC of tests pin behavior we're about to remove. Decision deferred to `/speckit-tasks`: some tests will migrate to decimal_arb-equivalents (where the underlying user-visible behavior persists), others will be deleted (where the user-visible behavior is now provided by ExprPlanner and the tests existed only to pin preprocessor-internal mechanics).

2. **Postgres source `NUMERIC` signed/unsigned hint** (decision changed during implementation): Postgres `NUMERIC` is always signed mathematically, but the historical streamling convention for `NUMERIC(78, 0)` has been to treat it as `u256`-shaped (this matches blockchain pipelines where Postgres-side balances mirror the Avro/Ethereum origin). The implementation in `pg.rs::postgres_type_to_arrow_field` therefore stamps `native_int_kind=u256` on the `decimal_arb(78, 0)` output for `NUMERIC(78, 0)` source columns, preserving downstream ClickHouse `UInt256` compactness. **The earlier "don't try; emit with no hint" decision in this slot was reversed during implementation** because it would have silently regressed Postgres-sourced wide-int columns flowing to ClickHouse — they would have landed as `Decimal(78, 0)` instead of `UInt256`, breaking storage-shape compatibility for any existing pipeline of that shape. No `i256` hint is inferred from Postgres (there's no widely-used 77-digit convention there); a YAML-level override is the future path for the rare signed case.

3. **Renaming the preprocessor file**: After ~70% reduction, the file might warrant a rename (`decimal_cast_preprocessor.rs`?). Deferred to a polish task; not required for this feature's user stories.
