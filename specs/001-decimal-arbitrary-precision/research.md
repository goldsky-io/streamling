# Phase 0 Research: Arbitrary-Precision Decimal Type

**Plan**: [plan.md](./plan.md) — **Spec**: [spec.md](./spec.md)

This document resolves the technical unknowns surfaced in the plan's Technical Context section. Each entry follows the **Decision / Rationale / Alternatives** format. Items marked `OPEN` need a code-level confirmation against DataFusion 49.0.2 / Arrow 55.2.0 source before implementation begins; the planning phase resolves the design direction, not the exact API surface.

---

## R1. Arbitrary-precision arithmetic library

**Decision**: Use `bigdecimal = 0.4.8` (already in `Cargo.toml`) as the in-memory value type for `DecimalArbValue`. Wrap it with a thin `streamling-common` newtype to centralize rounding-mode handling and validation against declared `(precision, scale)`.

**Rationale**:
- Already a workspace dependency (see `Cargo.toml:79`); zero new transitive deps.
- License: MIT/Apache-2.0 — compatible with the project.
- Half-to-even rounding is supported via `bigdecimal::RoundingMode::HalfEven`, which matches the spec's documented default (Assumptions, half-to-even).
- API stability: 0.4.x has been stable since 2023 and is widely used in the Rust ecosystem.
- Exposes `to_bigint_and_exponent()` and `from_bigint_and_exponent()` for cheap conversion to/from a (sign, magnitude bytes, scale) triple — what the Arrow encoding will need.
- Already proven in this codebase for Avro decimal decoding (`streamling-common/src/formats/avro/arrow_array_reader.rs:1030`), so there is a precedent and contributors are familiar with it.

**Alternatives considered**:
- **`dashu-float`**: faster on heavy arithmetic and `no_std` capable, but introduces a new dependency family. Reserved as a follow-up if benchmarks show `bigdecimal` is the bottleneck on representative workloads.
- **`malachite`**: best raw arithmetic performance, but LGPL-licensed — likely a license-fit problem for a permissively-licensed product. Rejected on license grounds.
- **`num-bigint` + manual scale management**: most control, but reimplements rounding, formatting, parsing, and edge-case handling that `bigdecimal` already provides. Strictly more code and more bugs for no gain.

---

## R2. Arrow physical encoding

**Decision**: Encode `DecimalArbArray` as an Arrow **`BinaryView`** (or `LargeBinary` if `BinaryView` proves to add IPC compatibility friction with downstream consumers; see OPEN below). Each value's bytes are a length-prefixed `(sign_byte, big_endian_two's_complement_magnitude_bytes)` payload, where `sign_byte = 0x00` for non-negative and `0xFF` for negative. Field metadata carries `precision` and `scale` as a JSON object in `ARROW:extension:metadata`.

**Rationale**:
- Variable-width payload is a hard requirement (FR-002 — arbitrary precision). `FixedSizeBinary(N)` would either cap precision (defeats the spec) or waste space at the lowest declared precision.
- `BinaryView` (Arrow ≥55) is preferred over `LargeBinary` because it gives O(1) inline storage for short payloads (most decimals fit in ≤16 bytes), avoiding a buffer indirection on the hot path. This matches how `BinaryView` is intended to be used.
- Big-endian two's-complement is the canonical Postgres `NUMERIC` over-the-wire format (text-protocol-decoded into a sign + digit groups, but the binary protocol uses BE 16-bit limbs). Mirroring that minimizes conversion cost at the Postgres connector boundary.
- Field metadata (`precision`, `scale`) lives on the `Field`, not in every value — matches how `Decimal128`/`Decimal256` are declared in Arrow today.
- Equality and hash are computed from the decoded `BigDecimal` value, not the raw bytes, to avoid `123` and `0123` (non-canonical encodings) hashing differently. **Builders must canonicalize before storing** — minimal-bytes representation only.

**Sort correctness**: Bytewise sort on the chosen encoding is **not** correct for negative values (sign byte `0xFF` sorts after `0x00`). Sort path must use a custom `Row` converter (or per-batch sort key transform) that maps the value to a sort-correct byte sequence — see R5.

**Alternatives considered**:
- **`FixedSizeBinary(N)` with N driven by declared precision**: locks the type to a per-column N, requiring a new type instance per `(precision, scale)`. Doesn't scale to truly arbitrary precision; matches u256/i256 pattern but doesn't match the spec.
- **`List<UInt64>` little-endian limbs**: structurally accurate but adds 8 bytes of List<> overhead per value and complicates IPC compatibility with non-Rust readers. Rejected.
- **Canonical decimal `Utf8`/`StringView`**: trivially Arrow-IPC-compatible and readable, but every operator pays string parse/format cost. Considered as a fallback for FR-019 (`coerce_to: string`) only, not as the primary encoding.

**RESOLVED (T006 spike, 2026-04-30)**: switch storage to **`LargeBinary`**, not `BinaryView`. `crates/streamling-core/src/session.rs:101` already configures DataFusion with `datafusion.optimizer.expand_views_at_output = true` to convert `BinaryView`/`Utf8View` to `LargeBinary`/`LargeUtf8` at output for ClickHouse-style sink compatibility. Using `BinaryView` for `decimal_arb` would add a per-batch view-expansion conversion on every sink path; using `LargeBinary` directly avoids it. `LargeBinary` was IPC-round-tripped end-to-end in `crates/streamling-common/tests/spike_binary_view_ipc.rs`, including extension-type metadata preservation. The `i64`-offset width also future-proofs against very large per-value payloads (decimal_arb at thousands of digits could exceed `BinaryArray`'s 2 GiB cumulative `i32`-offset cap).

---

## R3. DataFusion expression-planner extension surface (NATIVE OPERATORS)

**Decision**: Implement an `ExprPlanner` (DataFusion 49's planner-extension trait) registered on the `SessionContext` that intercepts `BinaryExpr` lowering. When either operand is the `decimal_arb` extension type, the planner rewrites the expression to a `ScalarUDF` invocation on the corresponding `decimal_arb_<op>` impl (defined in `decimal_arb_ops.rs`) with appropriate type coercion of the other operand. Comparisons (`=`, `<`, etc.) follow the same pattern.

**Rationale**:
- DataFusion 49 explicitly supports per-session `ExprPlanner` registration (introduced in 38.0.0, stabilized since); this is the documented hook for expression rewriting.
- Keeps the type-coercion logic in **one centralized module** (`decimal_arb_coercion.rs`) rather than scattered across every operator.
- Author writes `a + b` (FR-020); the planner does the work; UDFs run the arithmetic. No author-visible function names.
- u256/i256 do **not** use this pattern (function-call form) because integer arithmetic is rare enough in transforms that the friction is acceptable. Decimal arithmetic in financial/blockchain pipelines is *common*, so the cost of forcing function calls is not.

**Alternatives considered**:
- **Custom `TypeSignature::Coercible` on each BinaryOp UDF**: simpler, but DataFusion's binary-op resolution does not consult UDF signatures for native `+`/`-`/`*`/`/` — those are bound to Arrow primitive arithmetic kernels. Coercible signatures resolve nothing for native operators.
- **`OptimizerRule` (logical-plan rewrite)**: works after type-checking, when the plan already references built-in ops. Forces us to undo type-checking errors that should not have been raised. Earlier intervention via `ExprPlanner` is cleaner.
- **Fork DataFusion**: explicitly rejected by the constraint "no fork" in plan.md.

**RESOLVED (T005 spike, 2026-04-30)**: `ExprPlanner` is the right hook. The trait lives at `datafusion::logical_expr::planner::ExprPlanner`, with `RawBinaryExpr { op, left, right }` and `PlannerResult<T> { Planned(Expr), Original(T) }` (`datafusion-expr-49.0.2/src/planner.rs:120,263,316`). Registration is via the `FunctionRegistry` trait method `register_expr_planner(Arc<dyn ExprPlanner>)` (`datafusion-49.0.2/src/execution/context/mod.rs:1714`). The spike at `crates/streamling-common/tests/spike_expr_planner.rs` confirmed both **arithmetic** (`a + b`) and **comparison** (`a < b`) operators route through `plan_binary_op`, so a single ExprPlanner impl can cover the entire FR-003/FR-004 surface. **OptimizerRule fallback no longer required.**

---

## R4. Aggregate UDF dispatch (`SUM`, `MIN`, `MAX`, `AVG`, `COUNT`)

**Decision**: Register an `AggregateUDF` per aggregate name (`sum`, `min`, `max`, `avg`, `count`) with a `Signature::user_defined(Volatility::Immutable)` whose `coerce_types` implementation accepts the `decimal_arb` extension type. DataFusion's aggregate-resolution path consults UDFs ahead of built-ins **only** when the built-in cannot accept the input type; we exploit this by leaving the built-ins in place and supplying ours as the coercion fallback for `decimal_arb` inputs.

**Rationale**:
- Matches FR-007 ("invoked by their standard names").
- No special syntax required; `SUM(decimal_arb_col)` resolves to our impl, `SUM(decimal128_col)` continues to resolve to the built-in.
- Precision widening for `SUM` and `AVG` happens inside our impl: `SUM` returns `decimal_arb(p+log10(n_groups), s)` (capped at MAX_PRECISION); `AVG` returns `decimal_arb(p+1, s+1)` widened by one digit each side, matching standard SQL decimal-arithmetic conventions.
- `COUNT` returns `Int64`, identical to the built-in.

**Alternatives considered**:
- **Aliased function names** (`decimal_arb_sum`, etc.): violates FR-020 (author writes standard SQL). Rejected.
- **Hooking the aggregate-resolution path directly via a session extension**: more invasive than registering UDFs with overlapping signatures. Reserved as fallback if the coerce-types path doesn't surface our UDF before the built-in errors.

**RESOLVED (T007 spike, 2026-04-30)**: `SessionContext::register_udaf` with `name == "sum"` **overrides** the DataFusion built-in. Spike `crates/streamling-common/tests/spike_aggregate_dispatch.rs::udaf_registered_with_builtin_name_overrides_builtin` ran `SELECT SUM(x) FROM t` against an Int64 column where `x = [1,2,3,4,5]` (built-in would return `15`) with a sentinel UDAF named `sum` returning `999`; the result was `999`. US2's `decimal_arb_sum_udaf` (T044) can therefore use the standard SQL name `SUM` directly per FR-007 / FR-020. **`AggregateFunctionPlanner` wrapper fallback no longer required.**

---

## R5. Sort correctness for signed values

**Decision**: Implement a custom `Row` encoding for `decimal_arb` via DataFusion's `RowConverter`. The encoding is a fixed-width-then-variable-tail key: 1 byte (sign), 4 bytes (big-endian magnitude length), then magnitude bytes. For negatives, flip every bit of the entire payload (sign + length + magnitude) so the bytewise comparison reverses the natural order — matching how DataFusion's existing decimal128 row encoding handles signs.

**Rationale**:
- DataFusion's `RowConverter` is the standard hook for custom sort keys; signed-decimal types in DataFusion already use bit-flipping for negatives. Same pattern.
- Avoids the latent bug in `streamling.i256`, where bytewise sort over two's-complement places negatives after positives. (Audit found this; it should not be repeated.)
- Cost: one pass per batch on sort/merge paths; acceptable given the type's positioning as opt-in for high-precision use cases.
- Hash: a separate canonicalization step (R2) ensures equality and group-by consistency without depending on the sort encoding.

**Alternatives considered**:
- **Sign-flipped storage** (write the bit-flipped encoding directly to the Arrow buffer): saves the per-batch transform, but breaks the "raw bytes round-trip with Postgres NUMERIC" property. Rejected.
- **Custom `PhysicalSortExpr`**: works, but applies per-expression rather than per-type. Higher risk of someone adding a new sort path that forgets the override. Rejected.

---

## R6. Postgres `NUMERIC` wire format

**Decision**: Use the Postgres **text** protocol for `decimal_arb` ↔ `NUMERIC` round-trip in v1, mirroring the existing `Decimal256` text-binding workaround at `crates/streamling-connectors/src/table_providers/postgres/value_binding.rs:157-162`. The decoded text is parsed into `BigDecimal` directly.

**Rationale**:
- Text protocol is correct for unbounded precision (Postgres `NUMERIC` has no fixed binary format for arbitrary precision; the binary format uses 16-bit limbs but is more involved).
- Existing precedent in the codebase keeps the patch focused.
- Performance cost: parse/format is real but bounded by the row arrival rate, which is dominated by network for almost all pipelines we care about.

**Alternatives considered**:
- **Binary protocol** (`numeric_send` 16-bit-limb wire format): faster but adds a parser to maintain. Defer to a follow-up if profiling identifies it as a hotspot.

---

## R7. Avro `decimal` logical type

**Decision**: Replace the current `Utf8` fallback at `crates/streamling-common/src/formats/avro/schema.rs` for `precision >76` with a `decimal_arb` mapping. The Avro `decimal` payload (variable-length two's-complement big-endian magnitude bytes) is a near-exact match for the storage encoding chosen in R2, modulo our sign-byte prefix.

**Rationale**:
- Avro's encoding *is* big-endian two's-complement; converting to our format is a 1-byte prefix add (sign extracted from MSB of the Avro magnitude bytes) — near-zero cost.
- Removes a known data loss path (current `Utf8` fallback breaks any downstream consumer that expects numeric semantics).

**Alternatives considered**:
- **Keep `Utf8` fallback**: violates FR-018 (the existing buggy path must be retired).

---

## R8. Default rounding mode plumbing

**Decision**: Compile half-to-even (`bigdecimal::RoundingMode::HalfEven`) into each operator's implementation in v1. Do **not** introduce a session-level rounding-mode config.

**Rationale**:
- Spec assumption documents half-to-even as the default; no user has asked for alternatives yet.
- Per-operator simpler than session-level; no plumbing required through the planner.
- Easy to add later: introduce a `SessionConfig` key, thread it into the operator's invoke args, only when a user case surfaces.

**Alternatives considered**:
- **Session-level `RoundingMode` config**: speculative flexibility for hypothetical needs. Defer per the project's "don't design for hypothetical future requirements" convention (`AGENTS.md`).

---

## R9. ClickHouse client behavior

**Decision**: ClickHouse's native `Decimal(P, S)` caps `P` at 76 (`Decimal256`). For declared precision `>76`, the `decimal_arb` column can only be emitted as ClickHouse `String` via the FR-019 opt-in (`coerce_to: string` per-column on the sink). The existing silent String fallback at `clickhouse.rs:1972-1992` is **deleted**, not preserved as a default.

**Rationale**:
- Verified against ClickHouse's documented type system: no native arbitrary-precision decimal exists; `String` is the only lossless fallback.
- Aligns with FR-011/FR-019. Authors who explicitly want String emission set the directive; absent the directive, the pipeline is rejected at config load.

**Alternatives considered**:
- **Preserve silent fallback with a WARN log**: explicitly rejected during clarification (Q2 → B).

---

## R10. Plugin connector capability declaration

**Decision**: Extend the plugin FFI ABI (`abi_stable`) with one additional method on the connector trait: `fn supports_decimal_arb(&self, precision: u32, scale: u32) -> CapabilityResult`. The default implementation (for plugins that don't override) returns `Reject("plugin does not advertise decimal_arb support")`. Plugins that want to accept the type override the method.

**Rationale**:
- ABI-additive: existing plugins keep working without changes (default impl returns Reject).
- Explicit per-connector advertisement; no implicit "try and see what fails."
- Matches FR-010/FR-011's per-(column, connector) acceptance evaluation.

**Alternatives considered**:
- **Static "max precision" hint** (single integer per plugin): less expressive; doesn't capture plugins that support some scales but not others.
- **Per-row capability check**: runtime-only; violates the "fail at startup" principle (Q2 → B).

---

## Summary of OPEN items

**All three OPEN items resolved by the spike phase on 2026-04-30** (T005, T006, T007 in `tasks.md`). Spike test files live at `crates/streamling-common/tests/spike_*.rs`. Outcomes (one-line each):

1. **R2** — Storage type is `LargeBinary` (not `BinaryView`); the existing `expand_views_at_output` session flag would otherwise force a per-batch view expansion at every sink boundary. IPC round-trip verified.
2. **R3** — `ExprPlanner::plan_binary_op` covers both arithmetic and comparison; register via `FunctionRegistry::register_expr_planner`. No `OptimizerRule` fallback required.
3. **R4** — `SessionContext::register_udaf` with name `"sum"` overrides the DataFusion built-in. No `AggregateFunctionPlanner` wrapper required.

The plan's primary path stands on all three counts; no contracts or data-model entries need revision.
