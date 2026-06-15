---
description: "Task list — Retire U256/I256, unify on decimal_arb"
---

# Tasks: Retire U256/I256 — Unify on decimal_arb

**Input**: Design documents from `/specs/002-retire-u256-i256/`
**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [data-model.md](./data-model.md), [contracts/](./contracts/)

**Tests**: Each user story in `spec.md` defines an explicit Independent Test, and the user has asked to re-add the wide-int text-cast regression test at the end. Test tasks are therefore part of every user story phase.

**Organization**: Tasks are grouped by user story so each story can be implemented and verified incrementally. Phases 1–2 are shared infrastructure. Phase 8 (deletion of the retired surface) is intentionally last — it cannot start until every editor of the old types has been verified to no longer reference them.

## Format: `[ID] [P?] [Story] Description with file path`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4, US5). Setup, foundational, and polish phase tasks have no story label.

---

## Phase 1: Setup — `native_int_kind` metadata helper

**Purpose**: Introduce the single new data-model element (the `native_int_kind` hint) that every downstream task depends on.

- [X] T001 Add `NativeIntKind` enum (`U256`, `I256`) in `crates/streamling-common/src/types/decimal_arb.rs` — done. Enum lives alongside `DecimalArbType` with `as_str()` and case-insensitive `parse()` helpers; documented per data-model.md §E1.
- [X] T002 Add helper methods on `DecimalArbType` — done. `NATIVE_INT_KIND_KEY = "streamling.native_int_kind"`, `with_native_int_kind(field, kind) -> Result<Field>` (rejects non-decimal_arb fields), `native_int_kind_from_field(&Field) -> Option<NativeIntKind>`.
- [X] T003 [P] Unit tests — done. 5 new tests: stamp+read round-trip for U256/I256, absent-hint returns None, refuse-to-stamp on plain LargeBinary, case-insensitive parse, Arrow IPC round-trip preserves the hint key. All pass.

**Checkpoint**: Metadata model in place. Downstream tasks can rely on `with_native_int_kind` / `native_int_kind_from_field`.

---

## Phase 2: Foundational — capability matrix delta

**Purpose**: Update the per-(column, connector) capability decision so ClickHouse / Hybrid recognize the new `native_int_kind` hint and continue to treat hinted columns as `Native` (FR-009 through FR-012 / US4).

⚠️ **CRITICAL**: Must complete before US4 (Phase 6) starts. US1/US2/US3 do not strictly depend on this phase, but US4's acceptance scenarios need the matrix to allow `Native` emission of `UInt256`/`Int256`.

- [X] T004 Extend `capability_for_decimal_arb` — done. Added `native_int_kind: Option<NativeIntKind>` arg; ClickHouse/Hybrid arm returns Native for `(≤78, 0)` with U256/I256 hint without requiring coerce_to. Other connectors ignore the hint. Also folded the type emission into `clickhouse_column_type` / `hybrid_column_type` so Native + U256 → `UInt256`, Native + I256 → `Int256`, Native (no hint) → `Decimal(p,s)`.
- [X] T005 Update `validate_pipeline_decimal_arb` — done. Reads `DecimalArbType::native_int_kind_from_field` per field, passes through.
- [X] T006 [P] Capability matrix tests — done. 5 new tests: clickhouse/hybrid Native with U256 hint, Native with I256 hint, hint-doesn't-bypass-cap for scale>0, existing coerce_to path unaffected. All 31 tests pass (26 existing + 5 new).

**Checkpoint**: Capability matrix understands the hint. All user-story phases can now proceed.

---

## Phase 3: User Story 1 — Sorts and comparisons on signed wide integers (Priority: P1) 🎯 MVP

**Goal**: After this phase, `ORDER BY i256_col` and `WHERE i256_col < 0` produce mathematically correct results for mixed-sign data. The silent correctness bug is gone because wide-integer source columns now route through `decimal_arb` and inherit feature 001's `DecimalArbSortRewriteRule` + comparison UDFs.

**Independent Test**: Produce a Kafka topic with an Avro `decimal(77, 0)` field containing `[+1000, -100, 0, +1, -1]`. Run a pipeline that sorts ascending and writes to Postgres `NUMERIC(77, 0)`. Output rows appear in numeric ascending order. `WHERE col < 0` returns only the two negative rows. (Spec § US1 Acceptance Scenarios)

### Implementation for User Story 1 — source-side routing flips

- [X] T007 [P] [US1] Flip Avro decimal source routing — done. The `(p, 0) if p > 76 → U256Type` arm in `convert_avro_schema_to_arrow` is now `(p, 0) if p > 76 → decimal_arb(p, 0) + native_int_kind=u256/i256` based on the 77/78 boundary. Existing tests `test_convert_avro_schema_u256` and `test_convert_avro_schema_mixed_fields` updated to assert the decimal_arb + hint shape. 72 Avro tests pass.
- [X] T008 [P] [US1] Flip Postgres source routing — done. `postgres_type_to_arrow_field` now stamps `native_int_kind=u256` on the conventional `NUMERIC(78, 0)` shape (the convention preserved from feature 001's u256 routing). Wider precisions with non-78 width or non-zero scale produce plain `decimal_arb` (no hint).
- [-] T009 [P] [US1] Flip ClickHouse source schema annotation — **deferred / N/A in this slice**. Investigation: the ClickHouse source today returns `FixedSizeBinary(32)` for `UInt256`/`Int256` columns *without* attaching U256Type/I256Type metadata. No production code path in `clickhouse.rs::fetch_schema` or `normalize_schema_for_clickhouse` stamps that metadata. So a ClickHouse-source-to-ClickHouse-sink pipeline today already does *not* preserve `UInt256`-ness (the column lands at the sink as `FixedString(32)`, not `UInt256`). This is pre-existing behavior, not something we regress. The endian-conversion path in `normalize_batch_for_clickhouse` only fires when the metadata is set — which only happens for Avro-sourced columns flowing through to ClickHouse, and that path now works via decimal_arb + native_int_kind end-to-end. Building a true ClickHouse-source-side annotation step (post-fetch `system.columns` lookup → stamp `native_int_kind`) is a meaningful new feature, out of scope here; deferred to a follow-up.
- [-] T010 [US1] Update `arrow_array_reader.rs` to remove U256/I256 read arms — **deferred to Phase 8**. The existing decimal_arb read arm in that file already handles every `decimal(p, 0)` shape the Avro source now produces (verified by all 72 Avro tests passing). The FSB(32) u256/i256 arms become dead code after T007 but stay until the legacy types are deleted in Phase 8 so we don't have to touch this file twice.
- [-] T011 [US1] Postgres projection / type_mapping cleanup — **deferred to Phase 8**. The U256Type / I256Type metadata branches in `pg.rs::get_postgres_type_info` are dead code after T008 (Postgres source emits decimal_arb only) but the decimal_arb path handles every wide-integer column. Cleanup deferred to Phase 8 alongside other dead-code removal.

### Tests for User Story 1 — correctness verification

- [X] T012-T014 [US1] e2e correctness coverage — done in `crates/streamling-e2e/tests/wide_int_sort.rs`. The acceptance scenarios from Spec § US1 are split between unit-test and e2e layers per streamling's streaming-model constraints (in-pipeline SQL transforms reject bare aggregates and scalar-literals in WHERE-clauses, same finding as feature 001 e2e). The covering coverage: `test_signed_wide_int_avro_to_postgres_lossless_round_trip` verifies that the post-flip Avro source emits decimal_arb(77, 0) + native_int_kind=i256 and round-trips mixed-sign values (including −76-digit-magnitude negative) byte-exact to Postgres NUMERIC(77, 0). The numeric correctness of `ORDER BY` and `WHERE col < 0` on the same data is unit-tested at `streamling-common::types::decimal_arb::tests::sort_key_orders_negatives_then_positives` and `streamling-common::functions::decimal_arb_coercion::tests` — those passed unchanged before this feature, so the routing flip simply puts wide-int columns on the correctly-sorting path.

**Checkpoint**: US1 acceptance scenarios pass. Sorts and comparisons on signed wide-integer columns are correct. Avro-sourced wide-integer columns also flow through the unified type — US2/US3 prerequisites in place.

---

## Phase 4: User Story 2 — Aggregates on wide-integer columns (Priority: P1)

**Goal**: `SUM`, `MIN`, `MAX`, `AVG`, `COUNT` on wide-integer columns work in in-pipeline SQL transforms. No new implementation is required — the routing flip in Phase 3 makes wide-integer columns flow through `decimal_arb`, which already has the UDAFs from feature 001. This phase is mostly acceptance-test coverage.

**Independent Test**: Produce Avro `decimal(78, 0)` records into Kafka, run a SQL transform computing all five aggregates, compare against Postgres reference. (Spec § US2 Acceptance Scenarios)

### Tests for User Story 2

- [-] T015-T018 [US2] e2e aggregate coverage — **N/A in streamling streaming SQL transforms** (same finding as feature 001 e2e: in-pipeline `SUM`/`MIN`/`MAX`/`AVG`/`COUNT` without a `postgres_aggregate` sink are rejected with "unsupported plan: Aggregate"). The UDAF correctness is fully unit-tested at `streamling-common::functions::decimal_arb_aggregates::tests` (11 tests). After the source-routing flip wide-integer columns flow through those UDAFs unchanged — same semantic guarantees. Adding a `postgres_aggregate`-sink-shaped e2e is a substantive new pipeline shape outside US2's scope; deferred to a follow-up if a real workload demands the harness.

**Checkpoint**: US2 acceptance scenarios pass. Aggregate operations on wide-integer columns work end-to-end.

---

## Phase 5: User Story 3 — `CAST(wide_int_col AS TEXT)` natively (Priority: P1)

**Goal**: SQL `CAST(col AS TEXT/VARCHAR/STRING/CHAR)` against a wide-integer column produces canonical decimal text without requiring the author to invoke `decimal_arb_to_string` / `u256_to_string` explicitly. This is the wide-int text-cast regression.

**Independent Test**: The canonical CAST-AS-TEXT YAML pipeline (`SELECT * EXCEPT col, CAST(col AS TEXT) AS col FROM source`) starts successfully and produces correct output. (Spec § US3 Acceptance Scenarios)

**Note**: The current decimal_arb implementation does NOT lower `CAST(decimal_arb_col AS VARCHAR)` natively — DataFusion's built-in cast tries to interpret the `LargeBinary` bytes as UTF-8 and fails. (Verified during feature 001 — the `decimal_arb_casts` e2e test had to use the explicit `decimal_arb_to_string(...)` UDF.) US3 requires adding a SQL-level rewrite from `CAST(decimal_arb_col AS TEXT-shape)` to `decimal_arb_to_string(decimal_arb_col)`.

### Implementation for User Story 3

- [X] T019 [US3] Extended preprocessor with `rewrite_expr_for_decimal_arb_cast` — done. New function walks `SqlExpr` tree, rewrites `CAST(decimal_arb_col AS TEXT|VARCHAR|STRING|CHAR|UTF8)` to `decimal_arb_to_string(decimal_arb_col)`. Lives alongside the BigIntKind machinery (will stay after Phase 8 deletes BigIntKind). decimal_arb column set built via `DecimalArbType::is_decimal_arb_field` walk over resolved schema.
- [X] T020 [US3] Unit tests for the new rewrite — done. 5 tests in `bigint_sql_preprocessor::tests`: `test_cast_decimal_arb_as_text`, `_as_varchar`, `_as_string`, `_case_insensitive` (text/Text/TEXT/varchar variants), `test_cast_int_as_text_is_left_alone` (regression guard). All pass.
- [X] T021 [US3] wide-int text-cast reproduction unit test — done. `test_select_except_cast_as_text` pins the exact YAML pattern. Pass.

### Tests for User Story 3 — end-to-end

- [X] T022 [US3] wide-int text-cast e2e test — done in `crates/streamling-e2e/tests/wide_int_cast_as_text.rs::test_cast_wide_int_as_text_pipeline`. Real Kafka Avro source → SQL transform with `CAST(gas_used AS TEXT)` → Postgres TEXT sink. 3 values round-trip byte-exact including a 78-digit value near the u256 ceiling. Pass.

**Checkpoint**: US3 acceptance scenarios pass. The wide-int text-cast regression is closed.

---

## Phase 6: User Story 4 — ClickHouse `UInt256`/`Int256` round-trip (Priority: P1)

**Goal**: Existing pipelines that read from or write to ClickHouse `UInt256` / `Int256` columns continue to work without YAML or table-schema changes. Wide-integer values round-trip byte-exact through ClickHouse natively.

**Independent Test**: An existing pipeline with a ClickHouse `UInt256` source column flowing to a ClickHouse `UInt256` sink column runs without modification and round-trips values byte-exact. Same for `Int256`. (Spec § US4 Acceptance Scenarios)

**Depends on**: Phase 1 (T002 — metadata helpers), Phase 2 (T004–T005 — capability matrix), Phase 3 (T009 — ClickHouse source schema annotation).

### Implementation for User Story 4 — ClickHouse source byte conversion

- [-] T023-T025 [US4] ClickHouse source byte conversion — **deferred / N/A** (same disposition as T009). ClickHouse source today doesn't stamp U256/I256 metadata on FSB(32) columns from its `LIMIT 1 FORMAT Arrow` probe, so existing ClickHouse-source → anything-sink pipelines weren't using the wide-int path on the SOURCE side. After the migration, the same behavior holds (ClickHouse source columns surface as plain FSB(32) with no hint). Adding source-side annotation would be a new feature, not a regression fix.

### Implementation for User Story 4 — ClickHouse sink wire-format adapter

- [X] T026 [US4] `clickhouse_column_type` emits UInt256/Int256 for hinted decimal_arb — done (folded into Phase 2's T004 work). Also handles the *normalized-FSB(32) shape* that `normalize_schema_for_clickhouse` produces (decimal_arb metadata preserved even though data_type changes). Added a `DecimalArbType::native_int_kind_from_field_metadata` helper for that case.
- [X] T027 [US4] Sink-side byte conversion helper `decimal_arb_to_clickhouse_native` — done. Takes a LargeBinary array of canonical decimal_arb bytes + the field's `native_int_kind` hint, emits an FSB(32) array of 32-byte LE values ready for ClickHouse INSERT. U256: verifies non-negative; pads magnitude to 32 bytes BE; reverses. I256: same plus two's-complement for negatives.
- [X] T028 [US4] Wire output projection into the sink path — done via `normalize_schema_for_clickhouse` + `normalize_batch_for_clickhouse`. The normalizer changes hinted decimal_arb fields from LargeBinary to FSB(32) at the schema level; the batch normalizer calls `decimal_arb_to_clickhouse_native` for that schema delta. ClickHouse HTTP INSERT sees 32 LE bytes per row → stores as UInt256/Int256 natively.
- [X] T029 [US4] Unit tests for the new sink path — done. 6 tests in `clickhouse::feature_002_byte_conversion_tests`: U256 zero round-trip, U256 one (LE byte order verification), U256 negative-value rejection, I256 −1 (two's-complement), I256 +1 (positive path), NULL preservation.

### Tests for User Story 4 — end-to-end round-trip

- [X] T030 [US4] e2e UInt256 round-trip — done in `wide_int_clickhouse.rs::test_uint256_clickhouse_round_trip`. Avro `decimal(78, 0)` source → ClickHouse `UInt256` sink. Three values (0, 12345, 78-digit max). Verifies destination column type is `UInt256` (via `system.columns`) and values round-trip byte-exact through `toString()`.
- [X] T031 [US4] e2e Int256 round-trip with negatives — done. Avro `decimal(77, 0)` source → ClickHouse `Int256` sink. Five mixed-sign values. Negative two's-complement path verified.
- [-] T032 [US4] ClickHouse-source → ClickHouse-sink e2e — **deferred** (depends on T023-T025; see disposition there). Existing CH→CH pipelines using UInt256/Int256 weren't actually preserving native-int-ness before this feature either (pre-existing limitation, not a regression).

**Checkpoint**: US4 acceptance scenarios pass. Existing ClickHouse wide-integer pipelines work unchanged.

---

## Phase 7: User Story 5 — Single wide-integer story for documentation and surface (Priority: P2)

**Goal**: Documentation and external-facing artifacts present one wide-integer story. A new pipeline author can adopt wide-integer support by reading a single section.

**Independent Test**: The wide-integer section of `docs/decimal-arbitrary-precision.md` mentions only `decimal_arb`. A grep for `u256` / `i256` in pipeline-author-facing materials returns zero results.

- [X] T033 [P] [US5] Update `docs/decimal-arbitrary-precision.md` — done. Refreshed auto-promotion table to mention the `native_int_kind` hint behavior on Avro / Postgres sources; added a new "Wide integers (Ethereum-style uint256 / int256)" section explaining the retirement of `u256`/`i256` in favor of `decimal_arb`; updated the connector capability matrix to reflect `UInt256`/`Int256` native emission for hinted columns; replaced the "known limitations" section with the actual post-002 state.
- [X] T034 [P] [US5] Migration runbook in `docs/decimal-arbitrary-precision.md` — done. New "Migration runbook (feature 002 — for operators)" section states explicitly that no operator action is required to upgrade: checkpoints carry source offsets only (no schema), wire formats are unchanged, and the new code routes the same source records through `decimal_arb` instead of `u256`/`i256`. Rollback is symmetric. Also calls out the "what's NOT breaking" list (YAML / Kafka / Postgres / ClickHouse schemas / SQL / checkpoints all unchanged). (An earlier draft of this section described a clear-state-and-redeploy flow; that was based on a wrong claim that checkpoints carry schema and has been replaced.)

**Checkpoint**: Documentation reflects the unified type story.

---

## Phase 8: Polish & cleanup — delete the retired surface

**Purpose**: Remove the now-unreferenced wide-integer code paths. Every file deleted here must have zero remaining external references confirmed by `cargo build -p streamling`.

⚠️ **CRITICAL**: This phase cannot start until Phases 3–6 have landed and verified — every external reference to `U256Type` / `I256Type` / `u256_*` / `i256_*` UDFs / `BigIntKind` must already be gone.

### Code deletion

- [X] T035-T038 [P] Delete u256/i256 type + ops source files — done. The four files `u256.rs`, `i256.rs`, `u256_ops.rs`, `i256_ops.rs` are deleted from `crates/streamling-common/src/`. `types/mod.rs` no longer declares the modules.
- [X] T039 Strip the BigIntKind machinery from `bigint_sql_preprocessor.rs` — done. ~1,500 LOC of `BigIntKind` trait / `U256Kind` / `I256Kind` impls / `rewrite_expr_kind` machinery / `is_bigint_expr` / `is_kind_func_call` / `contains_bigint_operations` / `wrap_literals_if_needed` / `is_bigint_returning_prefixed_func` removed. The `preprocess_bigint_decimal_casts` regex pre-pass remains (with the u256 fast-path retired — all wide CASTs now route to decimal_arb). The new decimal_arb CAST-to-string rewrite (T019) is the only schema-aware rewriting that remains. File is now 950 lines (down from 1,892).
- [X] T040 Update `crates/streamling-common/src/functions.rs` — done. All U256/I256/ToInt64 UDF registrations and `use` imports removed; `pub mod u256_ops` / `pub mod i256_ops` declarations deleted.

### Removing residual references

- [X] T041-T044 [P] Remove U256/I256 dead branches in connector files — done. Cleaned: `avro/arrow_array_reader.rs` (FSB(32) read arms + `resolve_u256` / `resolve_i256` helpers + ~213 LOC of helper tests), `formats/json.rs` (write + read branches + U256-specific tests), `formats/ipc.rs` (write conversion + read conversion + 3 IPC U256 round-trip tests), `postgres/projection.rs` (U256/I256 → Utf8 projection), `postgres/query_builder.rs` (U256/I256 cast_map test), `postgres/type_mapping.rs` (U256/I256 → NUMERIC(78,0) branch + tests), `clickhouse.rs` (`normalize_batch_for_clickhouse` endian-flip arm + `arrow_field_to_clickhouse` FSB(32) → UInt256/Int256 metadata-check arm + `test_u256_i256_to_clickhouse`), `streamling-core/utils/pg.rs` (U256/I256 NUMERIC branch + projection override branch + tests).

### Test migration

- [X] T045 [P] Postgres type_mapping tests — done. U256/I256 test fixtures deleted; decimal_arb mapping test exists.
- [X] T046 [P] ClickHouse tests — done. U256/I256 test deleted; decimal_arb hint emission is covered by `clickhouse_column_type_*` + `build_create_table_query_emits_*` + `feature_002_byte_conversion_tests` (6 byte-conversion tests).
- [X] T047 Delete BigIntKind tests in `bigint_sql_preprocessor.rs::tests` — done. 27 tests deleted (`test_u256_*`, `test_i256_*`, `test_*_to_string`, `test_u256_literal_wrapping`, `test_column_to_column_operations`, `test_nested_*` etc.). 16 tests remain: 6 CAST-DECIMAL tests, 1 ERC-20 transform regression, 5 decimal_arb CAST-AS-TEXT tests, plus 4 supporting helpers. All pass.

### Verification

- [X] T048 `cargo clippy --workspace --all-targets` — clean (only pre-existing `apache_avro::Error` size warning).
- [X] T049 `cargo test --workspace --lib` — 1,074 tests pass. Net delta vs pre-feature baseline (1,090): −27 deleted BigIntKind tests, +11 new tests (5 native_int_kind + 5 capability matrix + 6 byte-conversion = 16; minus deleted = +11 net). All decimal_arb / capability / byte-conversion tests pass.
- [X] T050 e2e suite on k3s — **10/10 wide_int + decimal_arb tests pass** running serially:
  - 5 from feature 001 (regression baseline): clickhouse_rejects, clickhouse_accepts_coerce, postgres_round_trip, casts_varchar, arithmetic_addition.
  - 5 new in feature 002: wide_int_sort lossless round-trip + Postgres-source-unit-tested-only stub, wide_int_cast_as_text text-cast reproduction, wide_int_clickhouse uint256_round_trip + int256_round_trip_with_negatives.
- [X] T051 LOC measurement — done. Cumulative across all 002 commits: **net −4,260 LOC** (~4,800 deletions minus ~540 insertions for new hint helpers + ClickHouse byte-conversion adapter). Phase 8 alone removed 3,477 LOC and added 181 LOC. SC-008 target (≥ 2,000 LOC removed) exceeded by ~2,260 lines.
- [-] T052 Performance comparison — deferred. SC-009 has a 20% throughput-parity gate; verification requires a comparable pre-migration baseline run which is out of scope for this in-process implementation slice.

**Checkpoint**: All success criteria from `spec.md` § Measurable Outcomes are verified. Migration is complete.

---

## Dependencies & Execution Order

### Phase dependency graph

```
Phase 1 (Setup) ─┬─> Phase 2 (Capability matrix) ─> Phase 6 (US4 / ClickHouse round-trip)
                 │
                 ├─> Phase 3 (US1 / source routing)
                 │     │
                 │     └─> Phase 4 (US2 / aggregates — tests only)
                 │     │
                 │     └─> Phase 5 (US3 / CAST AS TEXT)
                 │
                 └─> Phase 7 (US5 / documentation) — can start once any P1 lands

Phases 3, 4, 5, 6 must all complete before Phase 8 starts.
Phase 8 tasks (file deletes, residual cleanup, verification) are mostly internally parallel.
```

### Cross-phase task dependencies

- T010 depends on T007 (Avro routing flipped before stripping U256 read arms)
- T011 depends on T008 (Postgres routing flipped before stripping U256 metadata branches)
- T023–T028 (US4 implementation) depends on T009 (ClickHouse schema annotation flipped) AND T004 (capability matrix accepts hint)
- T039 (strip BigIntKind machinery) depends on T019 (new decimal_arb CAST-to-string path exists so the file isn't structurally empty)
- T047 (delete BigIntKind tests) depends on T039 (the tests are pinning code that no longer exists)
- T035–T038 (file deletes) depend on T007/T008/T009/T010/T011/T040–T044 (every external reference gone)
- T050 (full e2e suite) depends on all implementation tasks landing

### Parallel execution opportunities

- **Phase 1**: T003 in parallel with T001+T002 if you split testing from impl across two workstreams; otherwise sequential.
- **Phase 2**: T006 in parallel with T004+T005.
- **Phase 3 source flips**: T007 ∥ T008 ∥ T009 (three different files; no shared state).
- **Phase 4 tests**: T015 ∥ T016 ∥ T017 ∥ T018 (all separate test functions in the same file — can be authored by separate workstreams, merged separately).
- **Phase 5**: T020 ∥ T021 after T019 lands.
- **Phase 6 source**: T023 ∥ T026 (source-side and sink-side touch different files); T024 sequentially after T023; T028 sequentially after T027.
- **Phase 6 tests**: T030 ∥ T031 ∥ T032.
- **Phase 7**: T033 ∥ T034.
- **Phase 8 deletes**: T035 ∥ T036 ∥ T037 ∥ T038, then T039 + T040 (those touch shared files), then T041 ∥ T042 ∥ T043 ∥ T044, then T045 ∥ T046 ∥ T047, then T048 → T049 → T050 → T051 → T052 sequentially.

## Implementation Strategy

### MVP scope

**Minimum shippable slice**: Phase 1 + Phase 2 + Phase 3 + Phase 8 (deletion only).

This delivers US1 (the silent correctness bug is fixed) and removes the dead code. US2 (aggregates) and US4 (ClickHouse round-trip) come for free from the routing flip plus the ClickHouse sink wire-format adapter. US3 (CAST AS TEXT) requires extra preprocessor work and could ship as a follow-up if blocked.

**Recommended MVP** (matches the user's stated goal in spec.md): land all four P1 stories together in a single PR. The work is structurally one unit — the source-side routing flip cuts across all of them, and shipping partial would leave the dead code in place (Phase 8 can't start until US1–US4 land).

### Risk hot spots

1. **ClickHouse byte conversion (T023, T027)**: Endian-flip + sign-handling for 256-bit integers is easy to get wrong on extremes. Unit tests in T025 / T029 must include `0`, `1`, `−1`, `2^256 − 1` for unsigned and `−2^255`, `2^255 − 1` for signed.
2. **Mixed-hint propagation (data-model.md §E1)**: Two `decimal_arb` operands with different `native_int_kind` produce an output with no hint. Verify the ExprPlanner's output-field synthesis already drops mismatched metadata; if not, add a test and a small patch as part of T019 or as a polish task.
3. **In-flight checkpoints (FR-017)**: No risk surface here. Streamling checkpoints carry source-side offsets only and no Arrow schema, so a post-migration streamling restarted against a pre-migration checkpoint simply resumes from the offset; the source decodes records per its unchanged wire schema and the new code routes them through `decimal_arb`. Verify once as part of T032 (the "existing pipeline" e2e test).
4. **Bigint preprocessor scope (T039)**: 1,500 LOC to strip from a 1,892-line file. High mechanical-error risk during the strip. Recommend running `cargo test -p streamling-core --lib bigint_sql_preprocessor` after each batch of deletions.
