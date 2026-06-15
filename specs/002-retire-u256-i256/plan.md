# Implementation Plan: Retire U256/I256 — Unify on decimal_arb

**Branch**: `002-retire-u256-i256` | **Date**: 2026-05-11 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-retire-u256-i256/spec.md`

## Summary

Source-side connector routing flips from emitting `u256`/`i256` extension types to emitting `decimal_arb(78, 0)` with a small per-field hint that preserves the unsigned/signed distinction for sinks that have native fixed-width types (ClickHouse `UInt256`/`Int256`). Once every source path emits `decimal_arb` instead of `u256`/`i256`, the dedicated wide-integer files, their UDFs, and the bigint preprocessor's binary-op rewrite machinery become dead code and are removed.

The technical approach has three legs:

1. **Add a single metadata key to decimal_arb fields**: `streamling.native_int_kind` ∈ {`u256`, `i256`}, optional. Set by source connectors that read from a fixed-width native channel (ClickHouse `UInt256`/`Int256`, Avro `decimal(>=77, 0)`, Postgres `NUMERIC(78, 0)`). Consumed by sinks that have a matching native channel — first and foremost the ClickHouse sink, which switches its emission from `Decimal(78, 0)` / `String` to `UInt256` / `Int256` when the hint is present.

2. **Flip source-side routing** in three places: `formats/avro/schema.rs` (Avro decimal → decimal_arb), `utils/pg.rs` (Postgres NUMERIC → decimal_arb), and the ClickHouse source-side schema fetcher (UInt256/Int256 → decimal_arb). Each previously routed to `u256`/`i256`; now all three route to `decimal_arb` with the right hint.

3. **Delete the retired surface**: remove `u256.rs`, `i256.rs`, `u256_ops.rs`, `i256_ops.rs`; remove the `BigIntKind` trait + `U256Kind`/`I256Kind` impls + `rewrite_expr_kind` machinery from `bigint_sql_preprocessor.rs` (~1,500 LOC of the file); remove the corresponding registrations from `CommonFunctions::functions()`; update or retire any tests pinning the old surface.

The decimal_arb implementation from feature 001 already provides everything else: arithmetic, comparisons, aggregates, sort encoding, CAST routing, JSON/Avro/Postgres wire formats, the connector capability matrix. This feature is mostly a routing flip + a deletion.

## Technical Context

**Language/Version**: Rust 1.89 (workspace toolchain)
**Primary Dependencies**: DataFusion 49.0.2, Arrow 55.2.0, bigdecimal 0.4 (already in tree for decimal_arb)
**Storage**: Internal Arrow extension type `decimal_arb` (`LargeBinary` + metadata); external wire formats unchanged (Postgres NUMERIC, ClickHouse UInt256/Int256/Decimal/String, Kafka Avro decimal, JSON digit-strings)
**Testing**: `cargo test --workspace --lib` for unit tests; `cargo nextest run -p streamling-e2e` against the k3s test cluster for integration tests (`just env-setup`); the `decimal_arb_*` e2e tests from feature 001 are the regression baseline
**Target Platform**: Linux x86_64 / arm64 streamling binaries; same as today
**Project Type**: Streaming data pipeline (Rust workspace, multi-crate)
**Performance Goals**: ±20% throughput parity vs the current u256/i256 paths on a representative 100k-row Kafka→ClickHouse pipeline (SC-009). No hot-path benchmarks are required for merge — this feature explicitly assumes BigDecimal arithmetic is fast enough, with the option of adding a fixed-width fast path later if a real workload shows otherwise.
**Constraints**:
- Must not require changes to user-managed ClickHouse table schemas (FR-014, US4)
- Must not require changes to existing YAML pipeline definitions (FR-014)
- Must round-trip wide-integer values losslessly across all supported source/sink pairs (FR-015)
- Must allow pipelines restarted from pre-migration checkpoints to resume without operator action (FR-017) — trivially satisfied because checkpoints carry source offsets only, not schema
**Scale/Scope**: The codebase has ~2,132 LOC of u256/i256-dedicated code and ~1,892 LOC in the bigint SQL preprocessor (of which ~1,500 is exclusively wide-integer binary-op rewriting). Target: remove ≥ 2,000 LOC; add ~200–300 LOC of connector wire-format adapter code and migration helpers (SC-008).

## Constitution Check

The project constitution file (`.specify/memory/constitution.md`) is the un-customized template — no concrete principles have been ratified. **No gates apply**. This section is therefore a no-op; revisit if/when the constitution is filled in.

## Project Structure

### Documentation (this feature)

```text
specs/002-retire-u256-i256/
├── plan.md              # This file
├── research.md          # Phase 0 output — surveys decimal_arb metadata extension, ClickHouse Arrow IPC type mapping, and migration-safety patterns
├── data-model.md        # Phase 1 output — the decimal_arb-field metadata convention + the source/sink wire-format adapter contract
├── quickstart.md        # Phase 1 output — three migration scenarios (Kafka Avro pipeline, Postgres pipeline, ClickHouse round-trip)
├── contracts/           # Phase 1 output — connector adapter contracts (ClickHouse, Postgres, Kafka Avro)
├── checklists/
│   └── requirements.md  # spec-quality checklist (already created by /speckit-specify)
└── tasks.md             # Phase 2 output (created by /speckit-tasks)
```

### Source Code (repository root)

This is a multi-crate Rust workspace. The crates this feature touches:

```text
crates/
├── streamling-common/
│   └── src/
│       ├── types/
│       │   ├── decimal_arb.rs            # KEEP — add `native_int_kind` metadata helpers
│       │   ├── u256.rs                   # DELETE in final task
│       │   └── i256.rs                   # DELETE in final task
│       ├── functions/
│       │   ├── decimal_arb_ops.rs        # KEEP — unchanged
│       │   ├── decimal_arb_aggregates.rs # KEEP — unchanged
│       │   ├── decimal_arb_coercion.rs   # KEEP — unchanged
│       │   ├── decimal_arb_sort_optimizer.rs # KEEP — unchanged
│       │   ├── u256_ops.rs               # DELETE in final task
│       │   └── i256_ops.rs               # DELETE in final task
│       └── formats/
│           ├── avro/schema.rs            # EDIT — route decimal(p ≥ 77, 0) → decimal_arb(p, 0) with native_int_kind hint
│           ├── avro/arrow_array_reader.rs # EDIT — remove u256/i256 read arms; rely on existing decimal_arb arm
│           ├── avro/writer.rs            # EDIT — remove u256/i256 write arms; rely on existing decimal_arb arm
│           ├── json.rs                   # EDIT — remove u256/i256 projection; rely on existing decimal_arb path
│           └── ipc.rs                    # EDIT — remove u256/i256 references (passive, may be no-op)
├── streamling-core/
│   └── src/
│       ├── types/
│       │   └── bigint_sql_preprocessor.rs  # EDIT — strip BigIntKind + rewrite_expr_kind machinery; keep CAST→DECIMAL(>76) routing only
│       └── utils/
│           └── pg.rs                       # EDIT — Postgres NUMERIC(78,0) → decimal_arb(78,0); remove u256/i256 metadata checks
└── streamling-connectors/
    └── src/
        └── table_providers/
            ├── clickhouse.rs               # EDIT — UInt256/Int256 source → decimal_arb with native_int_kind hint; sink emission when hint present
            ├── postgres/
            │   ├── type_mapping.rs         # EDIT — drop u256/i256 NUMERIC(78,0) special case; rely on existing decimal_arb path
            │   ├── projection.rs           # EDIT — drop u256/i256 → Utf8 projection; rely on existing decimal_arb projection
            │   └── query_builder.rs        # EDIT — drop u256/i256 cast_map handling
            └── ... (other connectors unchanged)
```

**Structure Decision**: This is a refactor within the existing multi-crate workspace; no new top-level structure is introduced. Files are flagged as **KEEP**, **EDIT**, or **DELETE** above. The deletions only happen in the final phase, once every source-side routing has been verified to emit decimal_arb instead of u256/i256.

## Complexity Tracking

No constitution gates to violate. The feature has one inherent complexity worth calling out (not a violation, but noted for reviewer attention):

| Complexity | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| `native_int_kind` metadata on decimal_arb fields | Required to round-trip ClickHouse `UInt256`/`Int256` storage without forcing a schema migration to `Decimal(78, 0)` (FR-009 through FR-012, US4) | Always emitting `Decimal(78, 0)` to ClickHouse — would force every existing wide-integer table to be re-typed; breaks every production pipeline targeting ClickHouse on rollout day. Operationally unacceptable. |

## Phase 0 — Research

See [research.md](./research.md) for the consolidated findings on:

1. **`native_int_kind` metadata convention**: where to live in the Arrow extension metadata map, name choice, propagation rules through transforms.
2. **ClickHouse Arrow IPC representation of UInt256/Int256**: what `DataType` the ClickHouse HTTP `FORMAT Arrow` returns for these native types, and how to recognize them at the source-side schema-fetch boundary.
3. **Decimal-arb signedness vs. metadata signedness**: every decimal_arb value is signed in its canonical encoding (sign byte + BE magnitude). The `native_int_kind` metadata is a *hint about origin*, not a *constraint on values* — a `native_int_kind=u256` column whose value happens to be negative is a contract violation that the sink must reject (or fall back to a wider non-native type).
4. **Migration safety**: streamling pipeline checkpoints record source-side offsets only and carry no Arrow schema. On restart under the post-migration streamling, the pipeline resumes from the stored offset, the source decodes records per its unchanged wire schema, and the new code routes them through `decimal_arb`. No state migration is required in either direction (upgrade or rollback).

## Phase 1 — Design & Contracts

See:

- [data-model.md](./data-model.md) — the metadata model and the source/sink dataflow.
- [contracts/clickhouse-wide-int.md](./contracts/clickhouse-wide-int.md) — ClickHouse source/sink adapter contract.
- [contracts/avro-wide-int.md](./contracts/avro-wide-int.md) — Avro decimal logical type → decimal_arb routing.
- [contracts/postgres-wide-int.md](./contracts/postgres-wide-int.md) — Postgres NUMERIC source/sink routing.
- [contracts/migration-state.md](./contracts/migration-state.md) — checkpoint compatibility (offset-only checkpoints; no schema, no migration).
- [quickstart.md](./quickstart.md) — three example pipelines exercising the unified type.

## Re-evaluate Constitution Check (post-design)

No constitution gates apply (template-only constitution). Re-check pass.
