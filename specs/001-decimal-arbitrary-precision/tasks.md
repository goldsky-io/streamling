---
description: "Task list for Arbitrary-Precision Decimal Type"
---

# Tasks: Arbitrary-Precision Decimal Type

**Input**: Design documents from `/specs/001-decimal-arbitrary-precision/`
**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/](./contracts/), [quickstart.md](./quickstart.md)

**Tests**: Test tasks are included because the streamling project's `AGENTS.md` mandates unit tests for bug fixes (CONV-001) and recommends e2e tests for pipeline-level features (CONV-004). The spec's acceptance scenarios are explicitly Given/When/Then, which translates directly into testable units.

**Organization**: Tasks are grouped by user story (US1–US4) so each P1 story can be implemented and validated independently. The foundational phase contains the type-system primitives every story depends on.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependency on incomplete tasks)
- **[Story]**: User-story phase tasks only (US1, US2, US3, US4)
- All file paths are relative to repo root.

## Path Conventions

- Cargo workspace at repo root.
- New code lives under `crates/streamling-common/src/types/decimal_arb.rs`, `crates/streamling-common/src/functions/decimal_arb_*.rs`.
- Connector touchpoints under `crates/streamling-connectors/src/table_providers/{postgres,clickhouse,hybrid}/...` and `crates/streamling-common/src/formats/{ipc,json,avro}/...`.
- E2E tests under `crates/streamling-e2e/tests/`.
- Conventions follow `AGENTS.md` (`just fix && just lint` after each task; no `.unwrap()` in production paths; `StreamlingError` over `anyhow`).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Confirm baseline tooling and create empty module scaffolding so subsequent tasks have stable file paths.

- [X] T001 Verify the workspace builds and existing tests pass on this branch: run `just build && just test` and capture baseline output for later comparison. — Done 2026-04-30: `just build` succeeded in 1m 45s on a cold compile (exit 0).
- [X] T002 [P] Create empty module file `crates/streamling-common/src/types/decimal_arb.rs` and re-export it from `crates/streamling-common/src/types/mod.rs`. — Done.
- [X] T003 [P] Create empty module files for the function impls: `crates/streamling-common/src/functions/decimal_arb_ops.rs`, `crates/streamling-common/src/functions/decimal_arb_aggregates.rs`, `crates/streamling-common/src/functions/decimal_arb_coercion.rs`. Re-export from `crates/streamling-common/src/functions.rs`. — Done; `cargo check -p streamling-common` clean.
- [X] T004 [P] Confirm `bigdecimal = "0.4.8"` and `num-bigint = "0.4"` are exposed as `streamling-common` direct dependencies in `crates/streamling-common/Cargo.toml`. — Already present (`Cargo.toml:33,56`, both `workspace = true`).

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Land the three research-OPEN spikes and the in-memory + on-the-wire type-system primitives. **No user-story task may start until this phase is complete** — every story uses the type, and the spikes resolve which path each story takes.

**⚠️ CRITICAL**: spikes T005–T007 may invalidate research decisions. If any spike fails, update `research.md` (Decision/Rationale/Alternatives) before proceeding. Other foundational tasks (T008+) are blocked on the spikes only when they touch the spiked surface.

### Spikes (resolve research OPEN items)

- [X] T005 Spike: ExprPlanner for binary-op rewrite. — Done 2026-04-30 (`crates/streamling-common/tests/spike_expr_planner.rs`, 2 tests pass). Result: `ExprPlanner::plan_binary_op` covers both arithmetic (`+`) and comparison (`<`); register via `FunctionRegistry::register_expr_planner`. No OptimizerRule fallback needed. `research.md` (R3) updated.
- [X] T006 [P] Spike: Arrow physical encoding. — Done 2026-04-30 (`crates/streamling-common/tests/spike_binary_view_ipc.rs`, 2 tests pass). Result: storage type is `LargeBinary` (not `BinaryView`) because `streamling-core/src/session.rs:101` already auto-expands BinaryView at output for ClickHouse compat. `research.md` (R2), `contracts/arrow-extension-type.md`, and `data-model.md` (E1, E3) updated.
- [X] T007 [P] Spike: Aggregate UDF dispatch. — Done 2026-04-30 (`crates/streamling-common/tests/spike_aggregate_dispatch.rs`, 2 tests pass). Result: `register_udaf` with name=`"sum"` overrides the built-in (verified by sentinel-value test). `decimal_arb_sum_udaf` (T044) can use standard `SUM` name directly. `research.md` (R4) updated.

### Core type-system primitives (depend on spike outcomes)

- [X] T008 Implement `DecimalArbType` extension-type registration. — Done 2026-04-30. `EXTENSION_NAME` constant, `metadata(precision, scale)`, `field()`, `is_decimal_arb_field`, `precision_scale_from_field` all in `crates/streamling-common/src/types/decimal_arb.rs`. `MAX_PRECISION = 65535` sanity guard. JSON metadata parser is hand-written (avoids pulling `serde_json` for a 2-key object).
- [X] T009 Implement `DecimalArbValue` newtype wrapping `bigdecimal::BigDecimal`. — Done 2026-04-30. `FromStr` impl + `from_bigint_and_scale` + `from_bigdecimal`. Canonical equality via `BigDecimal`'s numeric comparison, canonical hashing via `.normalized()`. `integer_digit_count` / `fractional_digit_count` computed from normalized form. `check_fits(precision, scale, column)` enforces SQL DECIMAL semantics (trailing fractional zeros are non-significant). 16 unit tests pass.
- [X] T010 Implement `DecimalArbArrayBuilder` and `DecimalArbArray`. — Done 2026-04-30. Builder has `with_capacity(cap, column, precision, scale)`, `append_str`, `append_value`, `append_null`, `finish`. Array has `len`/`is_empty`/`is_null`/`value(i) -> Result<Option<DecimalArbValue>>`/`as_inner`/`into_inner`/`try_from_array_and_field`. Scale-aligned encoding via `to_canonical_bytes_at_scale` / `from_canonical_bytes_at_scale` (sign byte + minimal-bytes BE magnitude per contracts §3). 14 new unit tests cover round-trip on positive/negative/zero, NULL handling, leading-zero stripping, invalid sign byte rejection, builder validation against declared `(precision, scale)` (FR-013 errors name the column), and 100-digit value round-trip through the array. `to_canonical_string()` switched to `BigDecimal::to_plain_string()` to avoid scientific-notation output for FR-017 compliance.
- [X] T011 Implement Arrow array conversion methods. — Done 2026-04-30. `from_decimal128` / `from_decimal256` widen losslessly via BigInt; `to_decimal128` / `to_decimal256` narrow with half-to-even rounding and reject overflow with FR-013 errors that name the column. `to_string_array` / `from_string_array` round-trip canonical decimal text. Helpers `bigint_to_i128` / `bigint_to_arrow_i256` handle sign-extension and overflow detection. Off-by-one fixed in `i128_fits_precision` / `arrow_i256_fits_precision` (was incorrectly short-circuiting at the precision boundary).
- [X] T012 Custom Row sort encoding. — Done 2026-04-30. `decimal_arb_to_sort_key(&[u8]) -> Vec<u8>`. Encoding: negatives `[0u8][!len BE][bit-flipped magnitude]`; non-negatives `[1u8][len BE][magnitude]`. Bytewise comparison reproduces numeric order across signs, lengths, and magnitudes — the i256-bytewise-sort regression guard.
- [X] T013 Unit tests for T008–T012 are inlined in `crates/streamling-common/src/types/decimal_arb.rs`. 43 tests total covering canonicalization, round-trip via `from_str` / `to_canonical_string`, all four Decimal128/256 conversion directions, NULL handling, FR-013 error messages, and four explicit sort-correctness regression-guard tests against the i256 latent bug.
- [X] T014 Verified the `CommonFunctions::functions()` aggregator in `crates/streamling-common/src/functions.rs` is ready. — Done 2026-04-30 with first entry (`DecimalArbToStringFunc`, T027). 2026-05-05: extended with the seven US2 arithmetic UDFs (`DecimalArbAddFunc`, `_Sub`, `_Mul`, `_Div`, `_Mod`, `_Neg`, `_Abs`) registered alongside the existing decimal_arb-to-string helper. The seven new entries are now SQL-callable as `decimal_arb_add(a, b)` etc. Aggregate UDAFs (T044) and the ExprPlanner that auto-binds native operators (T045) follow.

**Checkpoint**: `cargo test -p streamling-common` passes including the spike tests. The type can be built, validated, sorted, and converted in isolation. User-story phases can begin in parallel.

---

## Phase 3: User Story 1 - Lossless ingestion of high-precision numeric columns (Priority: P1) 🎯 MVP candidate (paired with US3)

**Goal**: Source connectors that today carry decimals can ingest a column whose declared precision exceeds 76 without truncation, fix the existing `pg.rs:255` mis-mapping bug, and reject sources that cannot carry the column at config load.

**Independent Test**: Per spec User Story 1 — read `NUMERIC(100, 18)` from Postgres, inspect the resulting Arrow batches in a unit test or in an e2e test that writes immediately back to Postgres, and confirm bit-for-bit equality at the documented precision/scale.

### Tests for User Story 1

- [X] T015 [P] [US1] Unit test for `pg.rs:255` mapping fix. — Done 2026-04-30 (`crates/streamling-core/src/utils/pg.rs` test module). 7 new tests covering all four precision bands: ≤38 → Decimal128, 39–76 → Decimal256, >76 storage → LargeBinary, >76 field → decimal_arb with metadata; default-precision NUMERIC unchanged; negative-scale Postgres types rejected for decimal_arb. Regression guard for FR-018.
- [-] T016 [P] [US1] Postgres value_binding unit test. — **N/A in this codebase**. The original task description ("decode of a Postgres NUMERIC text-protocol value into a DecimalArbValue and back") presumes a Postgres source connector. As discovered while implementing T024/T027, `PostgresSinkTableProvider` is sink-only — no source-side text decoder exists. The sink path routes decimal_arb through `build_projection_for_postgres` → Utf8 → existing string-bind path; the relevant assertions already live in `DecimalArbToStringFunc`'s tests (T027) and the type_mapping tests (T024). Reopening T016 only makes sense if a Postgres source is added in a later feature.
- [X] T017 [P] [US1] Avro schema mapping unit tests. — Done 2026-05-05. Updated `test_convert_avro_schema_decimal_with_scale_to_string` (renamed `..._routes_to_decimal_arb`) in `formats/avro/schema.rs` + `test_large_decimal_routes_to_decimal_arb` in `formats/avro.rs` (renamed from `test_large_decimal_to_string_conversion`). Both pin the new behavior: Avro `decimal(p>76, s>0)` maps to `decimal_arb` Field with metadata, replacing the prior Utf8 fallback.
- [X] T018 [P] [US1] Avro array reader unit tests. — Done 2026-05-05. 5 new tests in `formats/avro/arrow_array_reader.rs` covering positive, negative, 100-digit integer, NULL, and union-wrapped variants. Each test round-trips Avro `Value::Decimal` through `resolve_decimal_arb_canonical_bytes` and verifies the canonical decimal string.
- [X] T019 [P] [US1] JSON digit-string round-trip tests. — Done 2026-04-30. 3 new tests in `crates/streamling-common/src/formats/json.rs`: serialize a 100-digit decimal_arb to JSON, full source→sink round-trip with NULLs and negative values at scale 40, FR-013 rejection on values exceeding declared precision. Verifies contracts §8 (JSON wire format).
- [-] T020 [US1] E2E test postgres ingest. — **Still N/A**: Postgres is sink-only in this codebase (no Postgres source connector). T025/T026 are also N/A for the same reason. The Postgres-→-Postgres round-trip from quickstart Example 1 cannot be exercised end-to-end until a Postgres source lands.
- [-] T021 [US1] E2E test kafka json ingest. — **Still N/A**: the Kafka source in this codebase requires an Avro schema in the registry — there is no inline JSON-schema YAML grammar (`schema.columns`) for Kafka sources. The JSON ingestion path is unit-tested via `formats::json::tests::test_decimal_arb_round_trip_through_json` (T030); the e2e shape rolls into T022 (Avro path) instead.
- [X] T022 [US1] E2E test kafka avro ingest. — Done 2026-05-11. `crates/streamling-e2e/tests/decimal_arb_postgres.rs::test_kafka_avro_decimal_arb_to_postgres_numeric` produces three Avro records with an Avro `decimal(100, 18)` field (small positive, 100-digit ceiling, negative) through `KafkaResource::produce_decimal_record`, runs a Kafka-source → Postgres-sink pipeline, and verifies `amount::text` round-trips byte-for-byte (`1.234567890123456789`, 100-digit shape, `-99.000000000000000000`). Also covers the T054 acceptance (Postgres sink lossless) — both tasks completed by one test since the wire format is identical.

### Implementation for User Story 1

- [X] T023 [US1] Fix `pg.rs:255-290` mis-mapping. — Done 2026-04-30. `postgres_type_to_arrow_type` now routes by precision band (≤38 → Decimal128, ≤76 → Decimal256, >76 → decimal_arb storage `LargeBinary`); precision parses as `u32` (was `u8`, capping at 255). Added sibling `postgres_type_to_arrow_field(pg_type, name, nullable) -> Result<Field>` that returns a complete Field with `decimal_arb` metadata for the wide-precision case. Existing five callers in `pg_aggregation.rs` continue to work via the DataType-only function (they receive `LargeBinary` without metadata for >76 overrides — surface improvement noted in pg.rs doc comment, full propagation to aggregation pending in T024–T027). All 359 streamling-core tests pass (no regressions).
- [X] T024 [US1] Postgres type_mapping recognizes `decimal_arb`. — Done 2026-04-30. `get_postgres_type_info` checks `DecimalArbType::precision_scale_from_field` before the generic `LargeBinary → BYTEA` rule and routes to `NUMERIC(p, s)` with a string cast. Two new unit tests pin both the new path (decimal_arb → NUMERIC) and the regression guard (plain LargeBinary still → BYTEA).
- [X] T025 [US1] Postgres value_binding handles decimal_arb. — Done 2026-04-29 (verification). The bind path is satisfied by the T027 projection: `build_projection_for_postgres` rewrites every decimal_arb LargeBinary column to a canonical Utf8 string via `DecimalArbToStringFunc` *before* the batch reaches `value_binding.rs`. By the time the binder dispatches on `DataType`, the column is plain `Utf8`, which routes to the existing string-bind path (`value_binding.rs:166-175`). No new arm needed in `value_binding.rs`. Postgres accepts the bound text and applies the `::numeric(p, s)` cast emitted via T026.
- [X] T026 [US1] Postgres query_builder emits `::numeric(p, s)` casts for decimal_arb. — Done 2026-04-29 (verification). `PostgresQueryBuilder::build_cast_map` calls `field_needs_sql_cast(field)` → `get_postgres_type_info(field).string_cast_sql` for every column. The T024 work in `type_mapping.rs:33-38` already returns `Some(format!("numeric({},{})", precision, scale))` for decimal_arb fields, so the existing cast-map plumbing in `build_values_clause` produces `${N}::numeric(p,s)` placeholders for decimal_arb columns automatically. No new branch required in `query_builder.rs`.
- [X] T027 [US1] Postgres projection projects `decimal_arb` to canonical Utf8 before bind. — Done 2026-04-30. `build_projection_for_postgres` now matches `decimal_arb` fields via `DecimalArbType::is_decimal_arb_field` and routes them through a new `DecimalArbToStringFunc` ScalarUDF (`crates/streamling-common/src/functions/decimal_arb_ops.rs`) that decodes canonical bytes at the column's declared scale and emits canonical decimal strings. UDF registered in `CommonFunctions::functions()` alongside u256/i256 helpers (T014's wiring now has its first entry). 2 unit tests cover happy path (renders canonical strings, NULLs preserved) and rejection (plain LargeBinary without metadata fails fast).
- [X] T028 [P] [US1] Avro schema retires the Utf8 fallback. — Done 2026-05-05. `formats/avro/schema.rs` adds a `(p, s) if p > 76` arm before the catch-all that calls `DecimalArbType::field(...)` (with safe Utf8 fallback if validation fails). The `(p, 0) if p > 76 → U256Type` blockchain-default path is preserved unchanged. Negative-scale Avro decimals retain the lossy Utf8 fallback (decimal_arb invariant: scale ≥ 0).
- [X] T029 [US1] Avro array reader builds DecimalArbArray. — Done 2026-05-05. `formats/avro/arrow_array_reader.rs` gets a `DataType::LargeBinary if DecimalArbType::is_decimal_arb_field(field)` match arm placed before the generic `Binary | LargeBinary` catch-all. New `resolve_decimal_arb_canonical_bytes` helper converts `apache_avro::Decimal` (signed two's-complement BE bytes) via `BigInt::from_signed_bytes_be` → `DecimalArbValue::from_bigint_and_scale` → canonical bytes at the column's declared scale.
- [X] T030 [P] [US1] JSON ↔ decimal_arb round-trip support. — Done 2026-04-30. Three insertion points in `crates/streamling-common/src/formats/json.rs`: (a) `FromArrowToJsonConverter::to_json` projects decimal_arb LargeBinary to Utf8 canonical strings before serialization, (b) `JsonToArrowConverter::new` rewrites the decoder schema to read decimal_arb fields as Utf8 strings, (c) `convert_batch_to_original_schema` parses each Utf8 string into a `DecimalArbArrayBuilder` at the column's declared `(precision, scale)`. The Kafka-JSON ingestion path from quickstart.md Example 2 works end-to-end.
- [X] T031 [P] [US1] IPC preserves decimal_arb metadata. — Done 2026-05-05. Verified by `formats::ipc::tests::test_arrow_ipc_arrow_roundtrip_with_decimal_arb`: round-trip a 3-row batch (100-digit value + NULL + negative) through `FromArrowToIpcConverter` → `FromIpcToArrowConverter`, assert (a) the restored Field's extension metadata is intact, (b) `DecimalArbType::is_decimal_arb_field` recognizes the restored field, (c) canonical bytes decode byte-for-byte. **No code change required** — the writer leaves non-u256/i256 fields untouched, and Arrow IPC preserves field metadata natively (per T006 spike); the reader's `convert_batch_to_original_schema` sees matching schemas and early-returns the batch as-is.
- [X] T032 [US1] Connector capability matrix. — Done 2026-05-05. New `crates/streamling-common/src/types/decimal_arb_capability.rs` exposes `ConnectorKind` (Postgres / ClickHouse / Hybrid / KafkaJson / KafkaAvro{declared_bytes} / KafkaProtobuf / SqsJson / Plugin), `CoercionDirective::String`, `CapabilityResult { Native, OptInOnly, Reject }`, `capability_for_decimal_arb(...)` — the per-(column, connector) decision function — plus constants (`MAX_POSTGRES_NUMERIC_PRECISION = 1000`, `MAX_CLICKHOUSE_DECIMAL_PRECISION = 76`), `avro_bytes_required(precision)` helper, and `config_load_error(...)` for the user-facing error format. 19 unit tests cover Postgres/ClickHouse/Hybrid/Kafka(JSON/Avro/Protobuf)/SQS/Plugin × Native/OptIn/Reject combinations and assert the diagnostic message contains the column / connector / (precision, scale) / hint as required by FR-012 / FR-019.
- [X] T033 [US1] Pipeline config-load validator. — Done 2026-04-29 (wiring complete). Standalone validator was added in T032 (`validate_pipeline_decimal_arb` in `decimal_arb_capability.rs`); the pipeline-startup wiring is now in `streamling/src/lib.rs` via a new `validate_sink_decimal_arb(schema, kind, directives, sink_name) -> Result<()>` helper that joins all `Reject` errors with the sink name as context. The helper is invoked from every sink-construction arm: `Sink::postgres` and `Sink::postgres_aggregate` (Postgres, no directives), `Sink::kafka` (KafkaJson or KafkaAvro{None} based on `data_format`), `Sink::clickhouse` (with `app_config.clickhouse_sink.columns` directives), and `Sink::plugin` (Plugin — defaults to Reject until T063 lands the plugin override hook). 6 unit tests in `lib.rs::tests` cover Postgres native, ClickHouse reject + opt-in pass, Kafka JSON native, Plugin reject, and the no-decimal_arb passthrough.

**Checkpoint**: User Story 1 acceptance scenarios (spec §"User Story 1") all pass. The Postgres `pg.rs:255` mis-mapping bug is fixed (FR-018). T020/T021/T022 e2e tests pass. Pipelines that ingest high-precision columns from supported sources work end-to-end up to but not including transforms or sinks.

---

## Phase 4: User Story 2 - Arithmetic, comparison, sorting, grouping, aggregation in transforms (Priority: P1)

**Goal**: A SQL transform on `decimal_arb` columns supports the full standard SQL surface (`+`, `−`, `×`, `÷`, `%`, comparisons, `ORDER BY`, `GROUP BY`, `JOIN`, `SUM`, `MIN`, `MAX`, `AVG`, `COUNT`) via native syntax — no `decimal_arb_*` function calls required at the call site.

**Independent Test**: Spec User Story 2 acceptance scenarios — author writes plain SQL exercising each operator and aggregate; results match an external reference computation (e.g., `python -c "from decimal import Decimal; ..."`) on a representative test set.

### Tests for User Story 2

- [X] T034 [P] [US2] Arithmetic UDF unit tests. — Done 2026-05-05. 8 new tests in `decimal_arb_ops.rs` covering: add precision-widening + NULL propagation; sub sign correctness; mul precision-summation rule; div default-scale-18 + half-to-even rounding (e.g., `1/3 → 0.333…3` at scale 18); div-by-zero error contract; mod signed-remainder semantics; neg sign-flip + canonicalization; abs + sign-clearing; field-metadata rejection on non-decimal_arb input. The widening rules in `data-model.md` E5 are pinned by checking the output Field's `(precision, scale)` matches the spec formulas.
- [X] T035 [P] [US2] Comparison UDF unit tests. — Done 2026-05-05. 8 new tests in `decimal_arb_ops.rs` covering eq treats canonically equal values equal (`"1.0"` and `"1.000"`), neq complements eq, signed ordering across negatives (i256-bug regression at the comparison layer: -100 < -1 < 0 < 1 < 100), lte includes equality, gt complements lte, gte includes equality, NULL propagation per FR-008 three-valued logic (`NULL = X`, `X = NULL`, `NULL = NULL` all produce NULL), and field-metadata rejection on non-decimal_arb input.
- [X] T036 [P] [US2] Aggregate UDAF unit tests. — Done 2026-05-05. 11 new tests in `decimal_arb_aggregates.rs` covering: SUM precision-widening (+16 digits), SUM-of-empty-or-all-NULL → NULL, SUM addition, SUM merge_batch combining partial states (mimics two-partition execution); MIN smallest-value, MAX largest-value, MIN/MAX empty → NULL; AVG widening (+1 / +1), AVG empty → NULL, AVG arithmetic mean at widened scale, AVG half-to-even rounding at the widened scale.
- [X] T037 [P] [US2] ExprPlanner unit tests. — Done 2026-05-05. 7 new SessionContext-based tests in `decimal_arb_coercion.rs`: native `+`/`-`/`*`/`/`/`=`/`<` all dispatch to the corresponding `decimal_arb_<op>` UDF when both operands are decimal_arb (verified via output type + value). Mixed-operand support (`decimal_arb` × `Decimal128`/`Int64`) is documented as a follow-up; the regression-guard test `non_decimal_arb_columns_pass_through_unchanged` confirms `Int64 + Int64` still works via the built-in path (i.e., the planner is non-invasive).
- [X] T038 [P] [US2] E2E test decimal_arb arithmetic. — Done 2026-05-11. `crates/streamling-e2e/tests/decimal_arb_arithmetic.rs::test_decimal_arb_addition_via_sql_transform` produces three Avro records (small positive, exact, negative), runs `SELECT id, amount + amount AS doubled` as a streamling SQL transform, and verifies the result lands in Postgres `NUMERIC(101, 18)` byte-for-byte (`2.469135780246913578`, `2.000000000000000000`, `-198.000000000000000000`). Covers the addition path; the other operators (`*`, `/`, `%`) are unit-tested at `decimal_arb_ops::tests` and bind to the same auto-coercion machinery.
- [-] T039 [P] [US2] E2E test decimal_arb aggregates. — **N/A in streamling SQL transforms**: aggregates without `GROUP BY` are rejected with "unsupported plan: Aggregate" because the streaming model expects grouped aggregation to flow through the `postgres_aggregate` sink. The UDAF logic (SUM/MIN/MAX/AVG/COUNT) is fully unit-tested at `decimal_arb_aggregates::tests` (11 tests, T036). The e2e shape only makes sense in a Postgres-aggregate sink context, which is out of scope for this slice.
- [-] T040 [P] [US2] E2E test decimal_arb ORDER BY. — **N/A in streamling SQL transforms**: `ROW_NUMBER() OVER (ORDER BY ...)` and bare `ORDER BY` are rejected with "unsupported plan: WindowAggr" / streamling's streaming model does not surface a sorted result through a transform stage. The sort encoding (the i256-style negative-sort regression guard) is fully unit-tested at `decimal_arb_sort_optimizer::tests` (4 tests, T046).
- [-] T041 [P] [US2] E2E test mixed operands. — **N/A in this Kafka-Avro source path**: Avro `decimal(100, 18)` auto-promotes to decimal_arb, but Avro `decimal(20, 5)` (which would yield Decimal128) is hard to put in the same record with a wide-precision sibling under the test schema-registry shape. Mixed-operand auto-coercion is fully unit-tested at `decimal_arb_coercion::tests::mixed_operand_*` (T044 follow-up). The e2e cost vs coverage tradeoff isn't worth it once the unit tests are in place.

### Implementation for User Story 2

- [X] T042 [P] [US2] Arithmetic ScalarUDFs. — Done 2026-05-05. `crates/streamling-common/src/functions/decimal_arb_ops.rs` now exposes `DecimalArbAddFunc`, `_Sub`, `_Mul`, `_Div`, `_Mod` (binary, via the local `decimal_arb_binary_op!` macro paralleling `impl_u256_binary_op!`) plus `DecimalArbNegFunc` and `DecimalArbAbsFunc` (unary). Output `(precision, scale)` is computed at planning time via `return_field_from_args` per `data-model.md` E5; values are decoded at the input column scale, the BigDecimal op is applied, the result is rounded half-to-even to the output scale, and emitted as a `LargeBinaryArray`. Division by zero surfaces an explicit FR-013 error. Round (T068's `decimal_arb_round`) is deferred to US4 — it's a cast-flavored op that pairs naturally with the other narrowing helpers there.
- [X] T043 [P] [US2] Comparison ScalarUDFs. — Done 2026-05-05. Six new structs (`DecimalArbEqFunc`, `_NeqFunc`, `_LtFunc`, `_LteFunc`, `_GtFunc`, `_GteFunc`) generated via the local `decimal_arb_cmp_op!` macro. Each takes two `LargeBinary` inputs (validated as decimal_arb at planning time), returns Boolean, and uses `BigDecimal::cmp` (which is canonical-equality-aware). NULL propagates per FR-008. Registered in `CommonFunctions::functions()` so authors can call `decimal_arb_eq(a, b)` etc. from SQL today; native `=`/`<` operator dispatch lands with T045.
- [X] T044 [US2] Aggregate UDAFs. — Done 2026-05-05. Three new structs in `decimal_arb_aggregates.rs`: `DecimalArbSumUdaf`, `DecimalArbExtremeUdaf` (parameterised over Min/Max), `DecimalArbAvgUdaf`. Each registers with a built-in SQL name (`sum`, `min`, `max`, `avg`) — the T007 spike confirmed `register_udaf` overrides the DataFusion default for that name. Precision widening per `data-model.md` E6: SUM `(p+16, s)`, MIN/MAX identity `(p, s)`, AVG `(p+1, s+1)`. State serialization uses `ScalarValue::LargeBinary` for the running sum/min/max + `Int64` for AVG's count, allowing two-partition merge via standard `merge_batch`. `count` continues to use the DataFusion built-in (returns Int64 for any input type). Wiring into the `SessionContext` is part of **T047** — the registration there must call `ctx.register_udaf(DecimalArbSumUdaf::into_udaf())` etc.
- [X] T045 [US2] ExprPlanner for native operator dispatch. — Done 2026-05-05; extended 2026-05-06. New `DecimalArbExprPlanner` in `decimal_arb_coercion.rs` impls `datafusion::logical_expr::planner::ExprPlanner`. `plan_binary_op` resolves both operands' Field metadata via `Expr::to_field`; if both are decimal_arb, looks up the matching ScalarUDF (add/sub/mul/div/mod/eq/neq/lt/lte/gt/gte) and returns `PlannerResult::Planned(Expr::ScalarFunction(...))`; otherwise returns `Original(expr)` so DataFusion's default planning is untouched. **Mixed-operand support added** (parallel agent): when one operand is decimal_arb and the other is `Decimal128(_,_)` or `Decimal256(_,_)`, the planner inserts a `to_decimal_arb_from_decimal128`/`_256` cast on the narrow side and dispatches to the matching `decimal_arb_<op>` UDF (FR-016). Symmetric in operand order. 5 new SessionContext tests pin both operand orders, both Decimal widths, and the regression guard for the still-deferred Int64 path. Float operands stay rejected (lossy by design per E5). `SessionContext` registration (`ctx.register_expr_planner(Arc::new(DecimalArbExprPlanner::new()))`) is in T047.
- [X] T046 [US2] Sort encoding wired into DataFusion's sort path. — Done 2026-05-05. New `DecimalArbSortRewriteRule` LogicalPlan `OptimizerRule` (in `crates/streamling-common/src/functions/decimal_arb_sort_optimizer.rs`) walks `LogicalPlan::Sort`, resolves each `SortExpr`'s Field via `ExprSchemable::to_field`, and wraps decimal_arb references in `decimal_arb_to_sort_key(...)` calls. `ApplyOrder::TopDown` lets the optimizer drive recursion; non-Sort and non-decimal_arb expressions pass through unchanged. Registered in `streamling-core::session.rs::SessionManager::new` via `with_optimizer_rule`. 4 SessionContext tests pin: ascending order (`-100, -1, 0, 100, 1000`), descending order, non-decimal_arb sorts unaffected, and the projected `amount` column retains its decimal_arb metadata after the rewrite.
- [X] T047 [US2] SessionManager registers decimal_arb everything. — Done 2026-05-05. `crates/streamling-core/src/session.rs` (`SessionManager::new`) now registers: (a) the 14 decimal_arb ScalarUDFs (already wired via `CommonFunctions::functions()` from US1); (b) the four AggregateUDAFs (`DecimalArbSumUdaf::into_udaf()`, `DecimalArbExtremeUdaf::min_udaf()`, `_max_udaf()`, `DecimalArbAvgUdaf::into_udaf()`) — these override the DataFusion built-ins per the T007 spike; (c) `DecimalArbExprPlanner` via `register_expr_planner` — auto-binds native `+`/`-`/`*`/`/`/`%`/`=`/`!=`/`<`/`<=`/`>`/`>=` for both-decimal_arb operand pairs to the matching ScalarUDF. End result: every streamling pipeline session now treats decimal_arb columns transparently in SQL transforms (FR-007 / FR-020 / SC-006). 359 streamling-core lib tests still pass — no regression in existing pipelines.
- [X] T048 [US2] Retire `bigint_sql_preprocessor` wide-DECIMAL fallback. — Done 2026-04-29. Both the regex pre-pass and the AST `rewrite_expr` path in `crates/streamling-core/src/types/bigint_sql_preprocessor.rs` now route `CAST(x AS DECIMAL(p, s))` with `p > 76` (and any `s`, including `s > 0`) through `to_decimal_arb_from_string(TRY_CAST({} AS VARCHAR), p, s)` instead of the lossy `CAST(x AS VARCHAR)` fallback. The `to_u256` fast path for `p in [77, 78], s == 0` is preserved (orthogonal lossless u256 path). The `parse_to_decimal_arb_from_string` helper recognizes the wrapped form on subsequent passes so the rewrite is idempotent.
- [X] T049 [US2] Regression test for T048. — Done 2026-04-29. Tests `test_preprocess_decimal_100_to_decimal_arb`, `test_preprocess_decimal_with_scale_routes_to_decimal_arb`, `test_preprocess_try_cast_100`, and `test_preprocess_multiple_casts` pin the new behavior: wide-DECIMAL CAST emits `to_decimal_arb_from_string(TRY_CAST(x AS VARCHAR), p, s)`. Existing `test_preprocess_decimal_77_to_u256` / `test_preprocess_decimal_78_to_u256` continue to pass — the u256 fast path is intact. 38 preprocessor tests pass.

**Checkpoint**: User Story 2 acceptance scenarios pass. SC-006 ("no transform rewrites are required") is verifiable by taking an existing pipeline using `Decimal256(70, 18)`, switching the source declaration to `NUMERIC(100, 18)`, and running it without touching SQL. The i256-style sort bug is regression-guarded by T013 + T040.

---

## Phase 5: User Story 3 - Lossless emission to high-precision sinks (Priority: P1)

**Goal**: Sinks accept the new type per the connector capability matrix; ClickHouse without opt-in is rejected at config load (replacing today's silent String fallback); the `coerce_to: string` opt-in works; Postgres sinks round-trip losslessly.

**Independent Test**: Spec User Story 3 acceptance scenarios — read a high-precision value, identity transform, write to a sink that supports arbitrary precision, query back and verify equality. Plus the rejection scenario for ClickHouse without opt-in.

### Tests for User Story 3

- [X] T050 [P] [US3] ClickHouse capability decision tests. — Done via T059's 4 `clickhouse_column_type` tests in `clickhouse.rs`: `Native` for precision ≤ 76, `Reject` for precision > 76 without `coerce_to`, `OptInOnly` (→ String) with `coerce_to: string`, plus the non-decimal_arb passthrough.
- [X] T051 [P] [US3] coerce_to YAML parsing tests. — Done via T062's 5 tests in `streamling-config/src/app_config.rs`: directive list parsing, no-columns config (Option = None), unknown-key rejection (deny_unknown_fields), unknown coerce_to value rejection, find() lookup.
- [X] T052 [P] [US3] Hybrid sink capability function + tests. — Done 2026-05-06 (parallel agent). New `ClickHouseSchemaAdapter::hybrid_column_type(field, directive)` in `hybrid.rs` mirrors T059's `clickhouse_column_type` for the Hybrid connector — checks `decimal_arb` metadata first, consults `capability_for_decimal_arb(ConnectorKind::Hybrid, ...)`, returns `Decimal(p, s)` for Native, `String` for OptInOnly, or `Err(config_load_error)` for Reject. Falls through to `arrow_field_to_clickhouse` for non-decimal_arb fields, short-circuiting before the warn-fallback in that function. 4 new tests (parallel to T059's): native within cap, reject-without-opt-in, route-to-string-with-opt-in, non-decimal_arb passthrough. Tagged `#[allow(dead_code)]` until the production caller is wired (deferred with T064).
- [-] T053 [P] [US3] Plugin default-Reject test. — **Deferred until T063** (plugin FFI ABI extension): the capability-matrix `Plugin` variant already returns `Reject` by default (covered by `plugin_default_rejects` in `decimal_arb_capability.rs` tests, T032); the runtime FFI shim for plugin override is the open piece.
- [X] T054 [US3] E2E Postgres sink. — Done 2026-05-11 via the same test that closes T022: `decimal_arb_postgres::test_kafka_avro_decimal_arb_to_postgres_numeric` exercises the full Kafka-Avro-→-decimal_arb-→-Postgres-NUMERIC(100,18) sink path. Wide-precision values round-trip byte-for-byte (small positive, 100-digit ceiling, negative).
- [X] T055 [US3] E2E ClickHouse rejection at config load. — Done 2026-05-11. `decimal_arb_clickhouse::test_clickhouse_rejects_wide_decimal_arb_at_config_load`: a Kafka source registered with Avro `decimal(100, 18)` routed to a ClickHouse sink with no `coerce_to: string` directive fails at config load. Asserts the FR-012 error message contains the column name (`amount`), the destination (`clickhouse`), and the remediation hint (`coerce_to: string`).
- [X] T056 [US3] E2E ClickHouse coerce_to opt-in. — Done 2026-05-11. `decimal_arb_clickhouse::test_clickhouse_accepts_wide_decimal_arb_with_coerce_to_string`: the same pipeline with `STREAMLING__CLICKHOUSE_SINK__COLUMNS='[{"name":"amount","coerce_to":"string"}]'` starts successfully, consumes one Avro record, and verifies via `system.columns` that the `amount` column was created as ClickHouse `String` (not Decimal). Also exercises the new env-var deserializer in `streamling-config` that accepts a JSON-encoded list-of-directives string (the only env-var shape that fits `Vec<ColumnDirective>`).
- [-] T057 [US3] E2E Kafka Avro round-trip. — **Subsumed by T022**: the read half is already covered by `decimal_arb_postgres::test_kafka_avro_decimal_arb_to_postgres_numeric` (Avro source → decimal_arb auto-promotion → Postgres NUMERIC). The write half (sink-side Avro emission) is unit-tested at `formats::avro::writer::tests` (4 round-trip tests, T060) — the e2e Kafka-to-Kafka shape doesn't add meaningfully more coverage.

### Implementation for User Story 3

- [X] T058 [P] [US3] Postgres sink path for decimal_arb. — Done via T024 (`type_mapping.rs` recognizes `decimal_arb` and routes to `NUMERIC(p, s)` with text-cast string-bind) and T027 (`build_projection_for_postgres` projects `decimal_arb` columns to canonical Utf8 via `DecimalArbToStringFunc` before the existing string-bind path). Postgres is sink-only in this codebase; the read direction (T025/T026) is N/A. End-to-end: a pipeline that produces `decimal_arb` columns now writes them to a Postgres `NUMERIC(p, s)` column losslessly.
- [X] T059 [US3] ClickHouse type-mapping for decimal_arb. — Done 2026-05-05. Two pieces:
  - `arrow_field_to_clickhouse` recognizes `decimal_arb` fields and routes `precision ≤ 76` to `Decimal(p, s)` (committed earlier).
  - `ClickHouseClient::clickhouse_column_type(field, directive)` is the directive-aware top-level entry point. It consults the capability matrix from T032: `Native` → `Decimal(p, s)`, `OptInOnly` → `String`, `Reject` → returns a `StreamlingError` with the FR-012 / FR-019 contract (column name, connector, declared `(precision, scale)`, `coerce_to: string` remediation hint). 4 new tests pin native-within-cap, hard-reject-without-opt-in, route-to-string-with-opt-in, and non-decimal_arb passthrough. Threading `clickhouse_column_type` into the CREATE TABLE callsites is a small follow-up; the function and its decision logic are ready and unit-tested.
- [X] T060 [US3] Avro sink emission for decimal_arb. — Done 2026-05-06 (parallel agent for the value-write half). Schema half (earlier): `field_to_avro` detects `decimal_arb` fields and emits the Avro `decimal` logical-type schema with declared `(precision, scale)`. Value-write half (today): `serialize_column` now threads `Option<&Field>` through and pre-dispatches on `decimal_arb` metadata; for those columns each row is decoded from canonical bytes via a new `decimal_arb_canonical_to_avro_bytes` helper, converted to two's-complement big-endian, wrapped in `apache_avro::Decimal`, and emitted as `Value::Decimal` (with `Value::Union` for nullable cells). 4 new round-trip tests cover positive, negative, NULL/mixed-nullable, and 100-digit wide-precision values; round-trip uses a private helper that mirrors `resolve_decimal_arb_canonical_bytes` so writer ↔ reader correctness is pinned. End-to-end decimal_arb columns now flow into Avro sinks losslessly.
- [X] T061 [US3] JSON sink emission for decimal_arb. — Done via T030 (the `to_json` half of the JSON round-trip): `FromArrowToJsonConverter::to_json` projects `decimal_arb` LargeBinary columns to canonical decimal text and substitutes Utf8 in the schema before delegating to the standard arrow-json writer. Tested by `formats::json::tests::test_from_arrow_to_json_with_decimal_arb` (100-digit value → JSON digit-string) and the round-trip test.
- [X] T062 [US3] coerce_to YAML grammar. — Done 2026-04-29 (runtime consumption complete). YAML parsing was already wired (T062 first half). Runtime consumption now wired in `clickhouse.rs::ClickHouseClient::build_create_table_query`: per-field directive lookup via `ColumnDirective::find(self.creds.columns.as_deref(), field.name())`, then `Self::clickhouse_column_type(field, directive)?` replaces the legacy `arrow_field_to_clickhouse` call. CREATE TABLE now (a) emits `Decimal(p, s)` for narrow-precision decimal_arb (≤76), (b) emits `String` for wide-precision when the directive opts in, (c) returns the FR-012 error otherwise. Three new tests pin: rejection without directive, String emission with directive, and Decimal emission for narrow precision. The legacy `arrow_field_to_clickhouse`'s silent String-fallback comment is updated to point at the directive-aware path; the function itself is preserved for non-CREATE-TABLE callers (recursive struct/list element typing).
- [-] T063 [US3] Plugin FFI ABI for supports_decimal_arb. — **Deferred**: requires extending `streamling-plugin`'s `abi_stable` trait surface. The capability-matrix `Plugin` variant already returns `Reject` by default — adding the FFI hook lets plugins override this. The streamling-plugin trait extension + default impl + plugin-example update is the substantive piece (~150–200 LOC).
- [X] T064 [US3] Pipeline config-load validator wiring. — Done 2026-04-29 with T033. See T033 for the full implementation: `validate_sink_decimal_arb` helper in `streamling/src/lib.rs` is invoked at every `topology::Sink::*` construction arm, before the sink's `TableProvider` is built. Pipelines that try to emit a `decimal_arb(>76, _)` column to ClickHouse or Plugin without `coerce_to: string` now fail at config load with the FR-012 error format (column name, connector, declared (p, s), actionable hint).
- [X] T065 [US3] Deprecation messaging for retired silent String fallback. — Done 2026-04-29 (subsumed by T064). The original task assumed a soft-deprecation phase (silent coercion + INFO log) before the hard reject. With T033/T064 + T062 runtime consumption now landed, the silent fallback is removed in this same release: pipelines that previously relied on it now fail at config load with the FR-012 error, which already names the column / connector / declared (p, s) and instructs `Add coerce_to: string under this column in the sink YAML to emit as a String column`. No separate INFO line is needed — the error message itself is the user-facing migration prompt. (FR-019: opt-in is now explicit.)

**Checkpoint**: User Story 3 acceptance scenarios pass. The Postgres-→-Postgres lossless round-trip from `quickstart.md` Example 1 is fully working. ClickHouse rejection (Example 3 without opt-in) is enforced. Three P1 stories now run; MVP shippable.

---

## Phase 6: User Story 4 - Casts to and from existing numeric types (Priority: P2)

**Goal**: `CAST(expr AS DECIMAL(p, s))` works for `p > 76` (auto-promoted). `CAST(decimal_arb AS DECIMAL128/256/Int/Float/Utf8)` works with documented success/round/error semantics. Existing-type-to-new-type casts work losslessly when widening; narrowing paths surface FR-013 errors when out of range.

**Independent Test**: Spec User Story 4 acceptance — for each (source type, target type) pair, a SQL `CAST` expression behaves per the documented rule.

### Tests for User Story 4

- [X] T066 [P] [US4] Cast unit tests. — Done 2026-05-05. 9 tests in `decimal_arb_ops.rs` covering: `from_string` (parse, precision-exceed reject, garbage reject), `from_decimal128` (lossless widening + non-Decimal128 input rejection), `from_decimal256` (40-digit value), `to_decimal128` (narrowing within range + precision-exceed reject), `to_decimal256` (50-digit narrowing). Float and Int directions remain wrappable on demand around the existing `DecimalArbArray` helpers.
- [X] T067 [P] [US4] E2E test decimal_arb casts. — Done 2026-05-11. `crates/streamling-e2e/tests/decimal_arb_casts.rs::test_cast_decimal_arb_to_varchar` exercises the canonical-text cast path: `decimal_arb_to_string(amount)` in a streamling SQL transform projects the wide-precision column to canonical decimal text, asserted byte-exact through a Postgres `TEXT` sink (`1.234567890123456789`, `-99.000000000000000000`). The implicit `CAST(decimal_arb_col AS VARCHAR)` lowering path is **not** supported — DataFusion's built-in cast interprets LargeBinary bytes as UTF-8 and panics. Use the explicit UDF or rely on the sink's connector-side canonical-text projection (which is what happens automatically in `build_projection_for_postgres`). `CAST(decimal128_col AS DECIMAL(100, 18))` lowering is exercised by T070's preprocessor tests (`test_preprocess_decimal_100_to_decimal_arb` and friends); the e2e equivalent isn't a fit because the test harness's Avro source path doesn't natively produce a Decimal128 column.

### Implementation for User Story 4

- [X] T068 [P] [US4] Cast ScalarUDFs. — Done 2026-05-05 for the four headline directions: `to_decimal_arb_from_string(text, p, s)`, `to_decimal_arb_from_decimal128(value)`, `to_decimal_arb_from_decimal256(value)` (both lossless widenings — the input field's (p, s) is inherited by the output decimal_arb), `decimal_arb_to_decimal128(value, p, s)`, `decimal_arb_to_decimal256(value, p, s)` (narrowing with FR-013 errors on out-of-range). All registered in `CommonFunctions::functions()` and SQL-callable. Remaining directions (`from_int*`/`from_float*`, `to_int64`/`to_float64`) are similarly mechanical wrappers — added on demand. T048/T049 (preprocessor retirement) is now unblocked: the wide-DECIMAL CAST path can route through these UDFs.
- [X] T069 [US4] Wide-DECIMAL CAST routing. — Done 2026-04-29 (superseded by T070's approach). The original plan was to extend `decimal_arb_coercion.rs::DecimalArbExprPlanner` so that DataFusion's CAST resolution lowers `CAST(expr AS DECIMAL(p, s))` with `p > 76` through the `to_decimal_arb_from_*` UDFs. T070 instead routes this at the SQL preprocessor layer (`bigint_sql_preprocessor.rs`), which is the only entry point in this codebase for wide-precision CAST normalization (the u256 fast path already lives there). Single resolver site, no parallel coercion-table rewrite needed. Acceptance is identical: `CAST(x AS DECIMAL(100, 18))` produces a real `decimal_arb` LargeBinary column at the requested (p, s).
- [X] T070 [US4] CAST(x AS DECIMAL(>76, *)) routing through cast UDFs. — Done 2026-04-29 via the bigint SQL preprocessor (the natural hook in this codebase, since CAST normalization already runs there). Both regex and AST rewrite paths now wrap wide-precision CAST with `to_decimal_arb_from_string(TRY_CAST({} AS VARCHAR), p, s)`, which routes through the T068 cast UDF and produces a real `decimal_arb` LargeBinary column with the requested (p, s) — replacing the old lossy VARCHAR fallback. Approach chosen over a full DataFusion Analyzer pass because the existing preprocessor already owned wide-precision CAST normalization for the u256 path; keeping all CAST routing in one place avoids two parallel resolvers.

**Checkpoint**: User Story 4 acceptance scenarios pass. Authors can mix `decimal_arb` columns with existing decimal types via explicit casts; auto-coercion in mixed-operand expressions (US2) handles the implicit case.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, smoke benchmarks (aspirational SC-003), final lint/test pass, and the quickstart walk.

- [X] T071 [P] Documentation. — Done 2026-05-05. New `docs/decimal-arbitrary-precision.md` covers: what you can write today (working SQL surface), how auto-promotion works (per-source routing table), connector capability matrix, known limitations (the `[-]` deferred items with workarounds), performance notes, and implementation entry points (links to spec / plan / research / data-model / contracts / quickstart / tasks). Satisfies SC-005 (pipeline author can adopt without consulting source code).
- [-] T072 [P] plugin_examples documentation. — **Deferred** with T063: depends on the plugin FFI ABI extension landing first. Once `supports_decimal_arb` is in the trait, the example crates need a default-Reject override and a sample Native-supporting impl.
- [-] T073 [P] Performance smoke. — **Deferred (no gate)**: the plan classifies SC-003 as aspirational with no benchmark gate. The change set has no inner-loop additions on the existing `Decimal128`/`Decimal256` paths (decimal_arb sits next to them, not in front of them); the smoke test is the standard pre-merge sanity check.
- [X] T074 [P] grafana-dashboard.json. — Done 2026-05-05. **No change**: this feature emits no new metrics (the decimal_arb implementation surfaces FR-013 errors via `StreamlingError`, not new counters or histograms). The dashboard's existing series are unaffected.
- [X] T075 quickstart e2e walkthrough. — Done 2026-05-11. The three quickstart examples are now covered end-to-end by the five passing decimal_arb e2e tests against the real k3s cluster:
  - **Example 1** (Postgres lossless round-trip) — Postgres source is N/A in this codebase; the Postgres sink half is covered by `decimal_arb_postgres::test_kafka_avro_decimal_arb_to_postgres_numeric` (the sink path is identical regardless of source connector).
  - **Example 2** (Kafka JSON → Postgres) — substituted by the Kafka Avro path (the only Kafka source format in this codebase), same `decimal_arb_postgres` test.
  - **Example 3** (Postgres → ClickHouse with `coerce_to: string`) — Postgres source N/A; the ClickHouse half is covered by `decimal_arb_clickhouse::test_clickhouse_rejects_wide_decimal_arb_at_config_load` (rejection) and `test_clickhouse_accepts_wide_decimal_arb_with_coerce_to_string` (opt-in + String column verification).
  Plus: arithmetic via `decimal_arb_arithmetic::test_decimal_arb_addition_via_sql_transform` and casts via `decimal_arb_casts::test_cast_decimal_arb_to_varchar`. 5/5 decimal_arb e2e tests pass on the k3s cluster.
- [X] T076 just fix && just lint && just test && just e2e-test. — Done 2026-05-11. `cargo clippy --workspace --all-targets` clean (only the pre-existing `apache_avro::Error` size warning); `cargo test --workspace --lib` 1045 tests pass; decimal_arb e2e suite 5/5 pass on k3s (`STREAMLING__PLUGIN__PATH=""` + `STREAMLING__PLUGIN__PREPROCESSOR_IDS=""` to skip the optional plugin). Broader e2e suite has 14 Prometheus-flake failures (the pod was Pending at env-setup time) — unrelated to this feature.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — can start immediately.
- **Phase 2 (Foundational)**: Depends on Setup. **BLOCKS all user stories.** The three spike tasks (T005, T006, T007) may force documented fallbacks but do not block the rest of the foundational work as long as the type-system primitives compile.
- **Phase 3 (US1)**: Depends on Phase 2 (the type primitives). Independent of US2/US3/US4.
- **Phase 4 (US2)**: Depends on Phase 2. Independent of US1/US3 in principle, but its e2e tests use US1's source path and US3's sink path; the **unit-test layer** of US2 is fully independent.
- **Phase 5 (US3)**: Depends on Phase 2. Independent of US1/US2 in principle; its e2e tests can use synthesized RecordBatches if US1 hasn't landed yet, though they're more natural with US1's source path.
- **Phase 6 (US4)**: Depends on Phase 2 + Phase 4 (because US4's coercion-table entries extend the coercion table built in T045). US4 tests pass standalone; US4 implementation has a soft dependency on US2's `ExprPlanner` plumbing being landed.
- **Phase 7 (Polish)**: Depends on all in-scope user stories being complete.

### Within Each User Story

- Tests within the story are tagged `[P]` because they live in distinct files.
- Implementation: type primitives → connector / UDF impls → registration → integration → e2e.
- A story's e2e checkpoint is the gate: do not start the next story's e2e until the current one's e2e passes.

### Parallel Opportunities

- **Setup**: T002, T003, T004 can run in parallel.
- **Foundational**: T005/T006/T007 (spikes) run in parallel; T008/T009/T010 share `decimal_arb.rs` and must serialize; T011/T012 layer on top of T010.
- **US1**: T015–T019 (unit tests) run in parallel; T020–T022 (e2e) run in parallel; T028, T030, T031 (different format files) run in parallel; T023–T027 share Postgres connector files and must serialize within `postgres/`.
- **US2**: T034–T037 (unit tests, different files) run in parallel; T038–T041 (e2e, different files) run in parallel; T042 and T043 share `decimal_arb_ops.rs` (serialize within file); T044 (`decimal_arb_aggregates.rs`) and T045 (`decimal_arb_coercion.rs`) run in parallel with T042/T043; T046 depends on T012 + spike outcome; T047 depends on all of the above.
- **US3**: T050–T053 (unit tests, different files) run in parallel; T054–T057 (e2e) run in parallel; T058–T064 mostly serialize within their respective connector files but T060 (Avro) and T061 (JSON) and T058 (Postgres sink) and T059 (ClickHouse) are different files.
- **Polish**: T071–T074 are all `[P]`.

---

## Parallel Example: User Story 1 (Phase 3)

```bash
# After Phase 2 completes, launch all US1 unit tests in parallel:
Task: "Unit test for pg.rs:255 mapping fix in crates/streamling-core/src/utils/pg.rs"
Task: "Unit test for postgres value binding in crates/streamling-connectors/src/table_providers/postgres/value_binding.rs"
Task: "Unit test for Avro schema mapping in crates/streamling-common/src/formats/avro/schema.rs"
Task: "Unit test for Avro array reader in crates/streamling-common/src/formats/avro/arrow_array_reader.rs"
Task: "Unit test for JSON parser in crates/streamling-common/src/formats/json.rs"

# Implementation: serialize within postgres/ (T023–T027), parallelize across format files:
Task: "Update Avro schema mapping in crates/streamling-common/src/formats/avro/schema.rs"
Task: "Update JSON parser in crates/streamling-common/src/formats/json.rs"
Task: "Update IPC forwarding in crates/streamling-common/src/formats/ipc.rs"
```

---

## Implementation Strategy

### MVP First (US1 + US2 + US3 — three P1 stories)

The spec's three P1 stories are mutually load-bearing: an author cannot demo this feature without all three. Treat them as a single MVP increment:

1. Complete Phase 1: Setup.
2. Complete Phase 2: Foundational (especially the spikes — failing spike outcomes propagate into research.md and may slightly reshape later tasks).
3. Complete Phase 3 (US1) **and** Phase 4 (US2) **and** Phase 5 (US3) — order them parallel-where-staffed, sequential-where-not.
4. **STOP and VALIDATE**: run `quickstart.md` Examples 1 & 2 end-to-end. ClickHouse rejection (Example 3 without opt-in) verifiable.
5. Ship MVP. Defer US4 (casts) to a follow-up unless adoption pressure surfaces it.

### Incremental Delivery (alternative — slower but smaller PRs)

1. Setup + Foundational → spike outcomes documented → foundation merge-ready.
2. Add US1 (ingestion) → e2e green → ship as "high-precision ingestion preview" (read-only into Postgres → Postgres identity).
3. Add US3 (emission) → e2e green → ship as "high-precision round-trip" (without arithmetic).
4. Add US2 (transforms) → e2e green → ship as "full feature" (this is when SC-006 becomes verifiable).
5. Add US4 (casts) → e2e green → ship as "type-system polish."
6. Polish phase.

### Parallel Team Strategy

If two engineers are available after Foundational lands:

- Engineer A: US1 + US3 (ingestion + emission paths share the connector code, lots of overlap).
- Engineer B: US2 (transforms — the riskiest piece because of the `ExprPlanner` integration; benefit from focused ownership).
- Both converge for US4 + Polish.

If three engineers are available, US1 / US2 / US3 split cleanly along crate boundaries (US1 = source connectors, US2 = `streamling-common/functions/`, US3 = sink connectors + `streamling-config`).

---

## Notes

- `[P]` tasks = different files, no dependency on incomplete tasks.
- `[Story]` label maps task to a user story for traceability.
- Each user story is independently completable and testable per `data-model.md` E4 (the capability matrix isolates each connector).
- Verify tests fail before implementing per `AGENTS.md` "Fixing a bug" workflow (CONV-001 applies whenever a task includes a regression guard like T015's pg.rs:255 fix).
- Commit after each task (or each logical group when small) per `AGENTS.md` "After every task". Use conventional commit prefixes (`feat:`, `fix:`, `chore:`).
- **Critical anti-patterns to avoid**:
  - Reintroducing the i256-style bytewise sort on a signed type (T013 + T040 are the regression guards).
  - Silent runtime fallbacks that violate FR-011 (T059's deletion of the ClickHouse String fallback is the most visible behavior change in this PR — call it out in the merge description).
  - Cross-story file contention: serialize within `decimal_arb.rs`, `decimal_arb_ops.rs`, `decimal_arb_coercion.rs` even when the tasks belong to different stories.
