# Implementation Plan: Arbitrary-Precision Decimal Type

**Branch**: `001-decimal-arbitrary-precision` | **Date**: 2026-04-29 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-decimal-arbitrary-precision/spec.md`

## Summary

Add a new arbitrary-precision decimal type (`streamling.decimal_arb`) to streamling's DataFusion-on-Arrow pipeline, alongside the existing `Decimal128` (≤38 digits) and `Decimal256` (≤76 digits). The type carries user-declared `(precision, scale)` with no fixed-width-derived ceiling and supports the full standard SQL surface (native `+`, `−`, `×`, `÷`, `%`, comparisons, `ORDER BY`, `GROUP BY`, `JOIN`, `SUM`, `MIN`, `MAX`, `AVG`, `COUNT`, casts) without requiring author-side function calls. Connectors that already carry decimals (Postgres, ClickHouse, Kafka with JSON/Avro, plugin) accept or reject the type per-(column, connector) at configuration load — never with silent runtime fallback. The existing Postgres-source mis-mapping bug (`pg.rs:255` mapping `NUMERIC(100,18)` to `Decimal128(100,18)`) is fixed in the same change via auto-promotion when declared precision exceeds 76.

The architectural approach reuses the **existing `streamling.u256` / `streamling.i256` extension-type pattern** (Arrow extension type via metadata key, `ScalarUDFImpl` per operation, per-connector type-mapping touchpoints) and extends it for: (a) variable-width payload, (b) declared scale, (c) custom DataFusion type-coercion so native operators dispatch automatically, (d) aggregate UDFs, and (e) sort correctness for signed values. The plan favors `bigdecimal` (already pinned at 0.4.8) for the core arithmetic to avoid introducing a new dependency family.

## Technical Context

**Language/Version**: Rust 1.89.0 (per `rust-toolchain.toml` and the macOS linker workaround in `AGENTS.md:168`).

**Primary Dependencies**:
- `datafusion = "49.0.2"`, `datafusion-expr = "49.0.2"`, `arrow = "55.2.0"`, `arrow-schema = "55.2.0"`, `arrow-data = "55.2.0"`, `arrow-json = "55.2.0"` (workspace, `Cargo.toml:29-34`).
- `bigdecimal = "0.4.8"` and `num-bigint = "0.4"` already pinned (workspace `Cargo.toml:79, 121`); used today only by `streamling-common/src/formats/avro/arrow_array_reader.rs:1030` for Avro decimal decoding to string.
- `uint::construct_uint!` (existing 256-bit integer dependency for u256/i256) — not reused for the new type, but referenced as the existing-pattern template (`streamling-common/src/types/u256.rs:7`).
- Arbitrary-precision arithmetic library — NEEDS CLARIFICATION (Phase 0 research): `bigdecimal` (already pinned, mature, half-to-even rounding), `dashu-float` (faster, no_std), `malachite` (best perf, but LGPL — likely a license-fit blocker), or `num-bigint` + manual scale (most control, most code).

**Storage**: N/A — feature lives in-memory in Arrow `RecordBatch`es and on the wire via Arrow IPC, Postgres `NUMERIC`, ClickHouse `Decimal`/`String` (opt-in), Avro `decimal` logical type, JSON digit-strings.

**Testing**: `cargo test` (unit, per-crate); `crates/streamling-e2e` integration tests against k3s with Postgres + ClickHouse + Kafka (`AGENTS.md:80-100`); doc-tests on the new public APIs.

**Target Platform**: Linux server in containers (k3s); macOS dev workstations (cargo + nextest with linker workaround).

**Project Type**: Cargo workspace of Rust library crates wrapped by the `streamling` binary; this feature touches 5 existing crates.

**Performance Goals**: SC-003 is aspirational — no benchmark gate. Author-facing target: pipelines using only `Decimal128`/`Decimal256` show no perceptible change in throughput, latency, or memory after the feature lands. Pipelines using the new type pay an inherent cost proportional to declared precision; this is documented, not bounded.

**Constraints**:
- Cannot break existing `Decimal128`/`Decimal256` paths (FR-015).
- DataFusion 49.0.2 expression planner is the integration surface for native operator dispatch (FR-020); no fork.
- Arrow IPC and JSON / Avro must round-trip the type via standard Arrow extension-type metadata keys.
- Postgres source binary protocol must be used (existing string-binding workaround at `postgres/value_binding.rs:157-162` is the precedent for the new type).
- ClickHouse: native `Decimal` capped at 76; FR-019 opt-in is the only path beyond that.

**Scale/Scope**: Touches `streamling-common` (new type module + UDFs/UDAFs + format integrations), `streamling-core` (session registration, SQL preprocessor retirement, Postgres source mapping fix), `streamling-connectors` (Postgres + ClickHouse + Kafka type-mapping + value-binding), `streamling-flink-compat` (audit only — no expected change), `streamling-e2e` (new round-trip suites). Estimated ~3–4 KLOC of new code, mostly UDF/UDAF impls plus the coercion glue.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The repository's `.specify/memory/constitution.md` file is the unmodified template — every principle is a placeholder (`[PRINCIPLE_1_NAME]`, etc.). There are no concrete project principles to evaluate against.

**Pre-research gate**: PASS (vacuously — no principles to violate).
**Post-design gate** (re-evaluated after Phase 1, see below): PASS (vacuously).

If a real constitution is ratified later, a follow-up review of this plan against the new principles is recommended. Until then, this plan defers to the project's existing development conventions in `AGENTS.md` (just fix && lint, unit tests + e2e for pipeline-level behavior, no `.unwrap()` in production paths, `Drop`-based cleanup, conventional commit messages).

## Project Structure

### Documentation (this feature)

```text
specs/001-decimal-arbitrary-precision/
├── plan.md                    # This file (/speckit-plan output)
├── research.md                # Phase 0 output
├── data-model.md              # Phase 1 output
├── quickstart.md              # Phase 1 output
├── contracts/                 # Phase 1 output
│   ├── arrow-extension-type.md
│   ├── scalar-udf-signatures.md
│   ├── aggregate-udf-signatures.md
│   ├── connector-capability.md
│   └── yaml-schema.md
├── checklists/
│   └── requirements.md        # /speckit-specify output (already created)
├── spec.md                    # /speckit-specify output (already created)
└── tasks.md                   # /speckit-tasks output (NOT created by this command)
```

### Source Code (repository root)

```text
crates/
├── streamling-common/
│   ├── src/types/
│   │   ├── decimal_arb.rs                  # NEW: extension-type registration, in-memory value, builder, array
│   │   ├── u256.rs                         # existing (template)
│   │   ├── i256.rs                         # existing (template)
│   │   └── mod.rs                          # MODIFIED: re-export decimal_arb
│   ├── src/functions/
│   │   ├── decimal_arb_ops.rs              # NEW: ScalarUDF impls (add/sub/mul/div/mod/abs/neg/round, comparisons, casts)
│   │   ├── decimal_arb_aggregates.rs       # NEW: AggregateUDF impls (sum/min/max/avg)
│   │   ├── decimal_arb_coercion.rs         # NEW: type-coercion rules for native-operator dispatch (FR-020)
│   │   ├── u256_ops.rs                     # existing (template)
│   │   └── i256_ops.rs                     # existing (template)
│   └── src/formats/
│       ├── ipc.rs                          # MODIFIED: handle decimal_arb extension metadata
│       ├── json.rs                         # MODIFIED: emit/parse digit-string for decimal_arb
│       └── avro/
│           ├── schema.rs                   # MODIFIED: map declared precision >76 to decimal_arb (vs current Utf8 fallback)
│           └── arrow_array_reader.rs       # MODIFIED: build decimal_arb arrays from Avro decimal bytes
├── streamling-core/
│   ├── src/session.rs                      # MODIFIED: register decimal_arb UDFs/UDAFs alongside u256/i256
│   ├── src/utils/pg.rs                     # MODIFIED: fix line 255 mapping; >76 → decimal_arb (FR-018)
│   └── src/types/
│       └── bigint_sql_preprocessor.rs      # MODIFIED: stop rewriting CAST(... AS DECIMAL(>76, *)) to VARCHAR (retired by FR-015 auto-promotion)
├── streamling-connectors/
│   └── src/table_providers/
│       ├── postgres/
│       │   ├── type_mapping.rs             # MODIFIED: NUMERIC ↔ decimal_arb when precision >76
│       │   ├── value_binding.rs            # MODIFIED: bind decimal_arb values via NUMERIC text protocol
│       │   ├── query_builder.rs            # MODIFIED: emit ::numeric(p,s) cast for decimal_arb
│       │   └── projection.rs               # MODIFIED: project decimal_arb columns
│       ├── clickhouse.rs                   # MODIFIED: reject decimal_arb columns at config load unless coerce_to:string
│       └── hybrid.rs                       # MODIFIED: same as clickhouse.rs
├── streamling-config/
│   └── src/...                             # MODIFIED: parse coerce_to:string sink directive (FR-019)
└── streamling-e2e/
    └── tests/
        ├── decimal_arb_postgres_roundtrip.rs    # NEW
        ├── decimal_arb_kafka_json_roundtrip.rs  # NEW
        ├── decimal_arb_arithmetic.rs            # NEW
        ├── decimal_arb_aggregates.rs            # NEW
        ├── decimal_arb_sort_group.rs            # NEW
        ├── decimal_arb_clickhouse_reject.rs     # NEW: config-load rejection without opt-in
        └── decimal_arb_clickhouse_coerce.rs     # NEW: opt-in path

# Out of scope: streamling-flink-compat (audit only — no decimal-arb usage expected)
```

**Structure Decision**: Single Cargo workspace, follow the existing `streamling-common/src/types/{u256,i256}.rs` + `streamling-common/src/functions/{u256_ops,i256_ops}.rs` pattern for the new type. Connector touchpoints mirror what u256/i256 already wired through. No new crate is introduced — adding one would only fragment the existing extension-type pattern.

## Phase 0: Research

The unknowns surfaced in Technical Context drive Phase 0. Each is resolved in `research.md` with a Decision / Rationale / Alternatives entry. The questions:

1. **Which arbitrary-precision arithmetic library?** Evaluate `bigdecimal` (already pinned), `dashu-float`, `malachite`, raw `num-bigint` + scale. Criteria: API stability, performance for the (precision, scale) ranges authors realistically use (78–200 digits), dependency footprint, license (Apache-2/MIT-compatible — likely rules out `malachite`), correctness of half-to-even rounding, support for unbounded precision in division, exposure of byte representation for Arrow encoding.
2. **Arrow physical encoding.** `BinaryView` with length-prefixed BCD/two's-complement big-endian payload? `FixedSizeBinary(N)` with a per-column N driven by declared precision (matches `u256`'s pattern at 32 bytes)? `List<UInt64>` with little-endian limbs? Decisive criteria: Arrow IPC compatibility, zero-copy in/out of bigdecimal, fast bytewise sort correctness for **signed** values (the latent i256 sort bug is a hard "do not repeat"), hash stability across canonical equivalents (`123` and `0123` must hash equal).
3. **DataFusion expression-planner extension surface.** Identify the minimal set of hooks required to make `decimal_arb_col + decimal128_col` lower to the new type's `add` UDF without an explicit cast. Candidates: custom `TypeSignature::Coercible`, registering an `ExprPlanner`, intercepting `BinaryExpr` rewrite, or a `LogicalPlanRewriter` pass at session level. The u256/i256 pattern explicitly does **not** do this (users call `u256_add` by name) — so this is genuinely new ground.
4. **Aggregate UDF dispatch.** DataFusion's built-in `SUM`, `MIN`, `MAX`, `AVG`, `COUNT` resolve by argument type. Resolve whether we register named aliases or hook into the aggregate-resolution path so that `SUM(col)` automatically routes to a custom `AggregateUDFImpl` when `col` is `decimal_arb`.
5. **Sort correctness pattern.** Choose between (a) sign-flipped storage so bytewise sort is correct, (b) custom `Row` converter, (c) `PhysicalSortExpr` override. Each has cost in code paths that touch the new type.
6. **Postgres `NUMERIC` wire format.** Existing code at `postgres/value_binding.rs:157-162` already binds `Decimal256` as text. Confirm the same approach works for arbitrary-width values; if the binary protocol is faster, document the tradeoff but stay on text for v1 to keep parity.
7. **Avro `decimal` logical type.** Avro carries `decimal` as variable-length `bytes` or `fixed` with declared `precision` and `scale`. For declared `precision >76`, today's `streamling-common/src/formats/avro/schema.rs` falls back to `Utf8`. Replace that fallback with `decimal_arb` mapping. Confirm the byte-encoding is compatible with whatever Phase-0 storage choice (#2) is made.
8. **Default rounding mode plumbing.** Half-to-even (Assumptions in spec). Decide whether the rounding mode is a session-level setting or compiled into each operator. Session-level is more flexible; per-operator simpler. Pick the simpler one for v1 and revisit if a user need surfaces.
9. **ClickHouse client behavior.** Confirm that `String` is the only available coercion target for decimals >76 in the ClickHouse Rust client used by streamling. Document the FR-019 opt-in's wire-level behavior (column type emitted as `String`, server-side conversion).
10. **Plugin connector capability declaration.** The `streamling-plugin` system (FFI via `cdylib`+`abi_stable`) needs a way for plugins to advertise which (precision, scale) ranges they support. Decide between (a) a static "max precision" hint, (b) a per-row capability check, (c) explicit YAML opt-in. Static hint matches the configuration-load rule.

**Output**: `research.md` with one Decision/Rationale/Alternatives entry per question above.

## Phase 1: Design & Contracts

### `data-model.md`

Concrete entities (drawn from spec's Key Entities + the implementation):

- **`DecimalArbType`** (Arrow extension type)
  - `EXTENSION_NAME`: `"streamling.decimal_arb"`
  - Storage: chosen in Phase 0 (#2)
  - Field metadata: `ARROW:extension:name = streamling.decimal_arb`, `ARROW:extension:metadata = {"precision": <u32>, "scale": <u32>}` (JSON-encoded)
  - Invariants: `0 < precision <= MAX_PRECISION` (sanity guard, default 65535 — documented per Assumptions); `0 <= scale <= precision`
- **`DecimalArbValue`** (in-memory representation)
  - Wraps a `bigdecimal::BigDecimal` (assuming Phase 0 picks `bigdecimal`).
  - Carries no `(precision, scale)` of its own — those live on the column.
  - Validation on construction: `value.digits() <= precision`, `value.fractional_digit_count() <= scale`. Violations surface FR-013 errors.
- **`DecimalArbArray`** (Arrow array)
  - Backing buffer per Phase 0 (#2).
  - Builder API: `append_str(&str)`, `append_value(BigDecimal)`, `append_null()`.
  - Conversion to/from `Decimal128Array` and `Decimal256Array` for casts (FR-009).
- **`ConnectorCapabilityMatrix`** (config-load entity)
  - Each connector implements `fn supports_decimal_arb(precision: u32, scale: u32) -> CapabilityResult` returning `Native`, `OptInOnly`, or `Reject(reason)`.
  - Loaded once at pipeline start; FR-010/FR-011/FR-012 dispatch reads it.
- **`CoercionTable`** (DataFusion integration)
  - Maps `(BinaryOp, lhs_type, rhs_type)` → `(common_type, lhs_cast, rhs_cast)`.
  - Entries for `(*, decimal_arb(p1,s1), decimal128(p2,s2))`, `(*, decimal_arb, decimal256)`, `(*, decimal_arb, integer/float/string)`.
  - The "common_type" for two `decimal_arb`s with different (p, s) follows standard SQL-decimal-arithmetic widening rules.

State transitions: none (the type is value-typed, no lifecycle).

### `contracts/`

Five contract files, since this is a Rust-library + pipeline-engine project rather than a web service:

1. **`contracts/arrow-extension-type.md`**: extension-type name, field-metadata schema (JSON keys, value ranges), Arrow IPC compatibility statement, BinaryView/FixedSizeBinary/List<UInt64> physical layout (whichever Phase 0 picks), endianness, NULL representation, equality and hash canonicalization rules.
2. **`contracts/scalar-udf-signatures.md`**: ScalarUDF name + `Signature` + return type for every operation: arithmetic (`+`, `−`, `×`, `÷`, `%`, unary `-`, `abs`, `round`), comparison (`=`, `!=`, `<`, `≤`, `>`, `≥`), cast helpers (`to_decimal_arb`, `decimal_arb_to_string`, narrowing casts to `Decimal128`/`Decimal256`/`Float64`/`Int64`). Documents that these named functions are auxiliary; the binding contract is via type coercion (next entry).
3. **`contracts/aggregate-udf-signatures.md`**: AggregateUDF impls for `SUM`, `MIN`, `MAX`, `AVG`, `COUNT`. Contract specifies how DataFusion's built-in aggregate names resolve to these impls when the input column is `decimal_arb`. Documents the precision-widening rule on `SUM` and `AVG` (FR-007).
4. **`contracts/connector-capability.md`**: trait that each `TableProvider` / sink implementation implements (e.g., `trait DecimalArbCapability { fn supports(&self, precision: u32, scale: u32) -> CapabilityResult; }`). Per-connector concrete entries: Postgres (always Native if server version supports unbounded NUMERIC), ClickHouse (Native iff precision ≤76, OptInOnly iff `coerce_to:string`, Reject otherwise), Avro (Native iff declared bytes accommodate, Reject otherwise), Kafka+JSON (always Native), Plugin (delegates to plugin advertisement).
5. **`contracts/yaml-schema.md`**: YAML additions. `coerce_to: string` directive on a sink column (FR-019). No new top-level type keyword — auto-promotion is precision-driven (FR-015). Examples for each connector.

### `quickstart.md`

End-to-end example: a YAML pipeline reading a Postgres `NUMERIC(100, 18)` column, transforming with `SELECT a + b AS sum FROM source GROUP BY entity_id`, and writing to (a) Postgres `NUMERIC(100, 18)`, (b) Kafka with JSON encoding, (c) ClickHouse with the `coerce_to: string` opt-in. Each example shows YAML, the resulting Arrow schema, and the verified output. Runnable on the existing `just env-setup` k3s cluster.

### Agent context update

Update the plan reference between the `<!-- SPECKIT START -->` and `<!-- SPECKIT END -->` markers in `CLAUDE.md` (project-relative path) to point to this plan file: `specs/001-decimal-arbitrary-precision/plan.md`. If the markers are not present, append a short `## Active Speckit Plan` section that links to the plan.

## Constitution Check (Post-Design)

Re-evaluated after Phase 1. The design introduces:
- One new module per touched crate (`decimal_arb*`); follows existing extension-type pattern.
- One new dependency family is being *evaluated* (Phase 0 #1) but the strong default is `bigdecimal`, already pinned.
- One new public API surface (extension-type + UDFs/UDAFs); follows the convention used for u256/i256.
- One new config-time rejection path (FR-011/FR-019); centralizes the "fail at startup, not at row 1" behavior the engine already aspires to.

No principles to violate (template constitution); gate passes vacuously. The complexity of the change is concentrated in the DataFusion type-coercion glue (Phase 0 #3 / #4 / #5), which is unavoidable given FR-020 — there is no simpler path that satisfies "native SQL operators."

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Custom DataFusion type-coercion / expression-planner hook | FR-020 binds native operator surface (`a + b`, `SUM(a)`, `ORDER BY a`); without coercion, authors must write `decimal_arb_add(a, b)` which violates FR-020 and SC-006 | Function-call form (matches u256/i256 today) was explicitly rejected during clarification (Q4 → A); the user accepted the cost |
| Custom sort path for signed values | Bytewise-sort on two's-complement is wrong (latent i256 bug, see audit); FR-005 demands deterministic ordering | Re-using `FixedSizeBinary` default sort silently produces wrong order for negatives; not acceptable for a numeric type |
| Per-(column, connector) capability evaluation at config load | FR-010/FR-011/FR-012 require startup rejection; current ClickHouse silent String fallback violates this | Lazy evaluation at first row produces "fails on row 1" UX which the spec (Q2) explicitly rejected |
