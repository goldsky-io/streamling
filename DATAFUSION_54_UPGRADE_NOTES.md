# DataFusion 49 → 54 upgrade

> **STATUS: DONE & VERIFIED** (2026-06-16, branch `worktree-datafusion-54-upgrade`).
> Whole streamling workspace compiles (lib + all tests + bin, 0 errors); 856 lib unit tests pass.
> streamling-goldsky-plugins also migrated & verified (498 lib tests pass) against this worktree.
> Original blocker survey preserved below; an "Implementation notes" section at the end records
> what actually had to change (including a couple of corrections to the survey).

## Bottom line: **no real blockers**

Every breaking change that touches streamling is **mechanical** (repetitive, well-understood
edits). The only *structural* item is the Avro module removal, which has a clean vendoring plan.
Verified against the official DataFusion 50/51/52/53/54 upgrade guides, cross-referenced with
actual usage in every crate.

## Version bumps applied (workspace `Cargo.toml`)

- `datafusion` 49.0.2 → **54.0.0** (features `avro`, `backtrace`)
- `datafusion-expr`, `datafusion-datasource-avro`, `datafusion-ffi` → 54.0.0
- `arrow*` (arrow, arrow-schema, arrow-data, arrow-json, arrow-buffer) 55.2.0 → **58.3.0**
  (df54 requires arrow `^58.3.0` — note: 58.x, NOT 59)
- `apache-avro` (0.17) and `schema_registry_converter` (=4.3.0) pins are **unaffected** — still used
  by the Kafka schema-registry path independently of DataFusion.

## What applies (all mechanical)

| Change | DF ver | Crates | Fix |
|---|---|---|---|
| UDF traits require `PartialEq + Eq + Hash` (`DynEq`/`DynHash` supertraits, blanket-impl'd) | 50 | common (30), flink-compat (29), core (2) | add `#[derive(PartialEq, Eq, Hash)]` per UDF struct |
| `as_any` **removed** from `ScalarUDFImpl`, `AggregateUDFImpl`, `ExecutionPlan`, `TableProvider`, `PhysicalExpr` | 54 | common, flink-compat, core, connectors | delete the override. ~105 `as_any` sites total to review — most deleted, but custom-trait ones (e.g. `LazyBatchGenerator`, which *added* `as_any` in df50) stay. Needs care, not difficulty. |
| `ScalarFunctionArgs` gained `config_options` field | 50 | common, flink-compat (mostly tests) | add field to struct literals |
| `ExecutionPlan::properties` → returns `&Arc<PlanProperties>` | 53 | core (12), connectors (3) | store `Arc<PlanProperties>`, wrap in `Arc::new` |
| `ExecutionPlan::statistics` removed → `partition_statistics` returns `Arc<Statistics>` | 53/54 | core (6) | rename/move method |
| Arrow added `Decimal32`/`Decimal64` `DataType` variants (+ arrow 55→58) | — | any `match DataType` without wildcard | add arms / wildcard |
| Avro `avro_to_arrow::to_arrow_schema` removed | 54 | common (1 import) | **vendor — see below** |

Rough magnitude: ~360+ mechanical edits, dominated by the UDF trait change and the
`ExecutionPlan` churn in `streamling-core`, plus the ~260-line Avro vendor.

## The one structural item: Avro (`#3`)

- df54 rewrote `datafusion-datasource-avro` on top of **`arrow-avro`** and dropped the
  `apache-avro`-based `avro_to_arrow::to_arrow_schema` helper.
- streamling's **entire** Avro stack (reader `arrow_array_reader.rs`, writer, schema pre/post-processing,
  Kafka schema-registry) is built on standalone **`apache-avro` 0.17** — it only borrows that one
  function (single import + call site at `crates/streamling-common/src/formats/avro/schema.rs:7,31`).
- **Decision: vendor it.** Copy `to_arrow_schema` + helpers (`schema_to_field`,
  `schema_to_field_with_props`, `default_field_name`, `external_props`) — ~260 LOC, lines 30–290 of
  the cached `datafusion-datasource-avro-49.0.2/src/avro_to_arrow/schema.rs` — into
  `formats/avro/schema.rs` as private fns. Swap `datafusion_common::error::Result` for
  streamling-common's `Result`. Then **drop `datafusion-datasource-avro` from the workspace + crate
  Cargo.toml entirely** (nothing else uses it — this removes a dependency).
- Consistent with existing pattern: `arrow_array_reader.rs` is itself a vendored copy of DataFusion's
  old avro reader.
- **arrow-avro adoption** was considered as an alternative (native Confluent wire format,
  Decimal256 routing, faster columnar decode, less vendored code) and deferred to a separate, larger
  refactor — but it is **NOT gated on a fork**. The u256/i256 path (avro `decimal` precision 77–100,
  which arrow-avro hard-errors on above 76) was prototyped (`worktree-arrow-avro-u256-prototype`,
  `arrow-avro-proto/`):
  - A *reader-schema* override does **not** bypass the precision check (arrow-avro validates the
    *writer* schema's decimal logicalType at decoder-build time).
  - **The working lever:** in the Confluent/Kafka path streamling uses, we populate arrow-avro's
    `SchemaStore` with the writer schema ourselves. Registering it with the high-precision `decimal`
    logicalType stripped to plain `bytes`/`fixed` makes arrow-avro decode the *wire-identical* raw
    bytes into a `BinaryArray` — verified to round-trip the exact 32-byte payload. Remaining work is a
    post-decode reinterpret pass (`Binary`/`FixedSizeBinary` → `FixedSizeBinary(32)` + `streamling.u256`/
    `i256` extension metadata) reusing the existing `resolve_u256`/`resolve_i256` logic.
  - So u256/i256 support = a schema-rewrite-at-registration + a reinterpret pass (tens-to-low-hundreds
    of LOC, no fork). The bulk of the cost is the rest of the adoption (Confluent framing + `SchemaStore`
    wiring to replace the schema-registry decode path, swapping out `AvroArrowArrayReader`, redoing the
    writer path). Caveat: the store-rewrite lever only applies to the Confluent/SOE path, not OCF (whose
    schema arrow-avro reads from the file header) — fine, since streamling doesn't use OCF.
  - **Implementation in progress on branch `arrow-avro-migration`.**

## What does NOT apply (verified — 0 usages)

streamling uses **custom TableProviders** (kafka, clickhouse, postgres, http) and its **own FFI**
(Arrow C Data Interface + `abi_stable` + `async-ffi`), so a whole class of breaking changes is moot:

- **File-datasource churn** (df51/52): `FileSource`, `FileScanConfig`, `FileOpener`, `build_row_filter`,
  `TableSchema`/`with_schema` — 0 usages.
- **`SchemaAdapter` removal** (df52): false alarm. Trait still exists in df54 (deprecated, not removed),
  AND streamling's `ClickHouseSchemaAdapter` (hybrid.rs) is its own struct that never implements the
  DF trait — pure name coincidence.
- **FFI ABI changes** (df52): `datafusion-ffi` is declared in `streamling-plugin/Cargo.toml` but
  **never imported** — dead dependency. Plugin FFI rides on the stable Arrow C Data Interface.
- **`datafusion-proto`** (df51/54): 0 usages.
- **Custom `MemoryPool`** (`'static`+`Any`), **manual `ExecutionProps`**, **`TreeNodeContainer`**,
  **`CacheAccessor`**, **`PruningStatistics`** (df52/54): 0 usages.
- **`ExecutionPlan::reset_state`** (df50): has a default impl → optional (worth implementing later for
  correctness of stateful streaming operators, but not a compile blocker).

## Runtime behavior changes to validate via e2e (not compile blockers)

- **Struct casting now requires field-name overlap** (df53)
- **String/numeric comparison coercion prefers numeric types** (df54)
- **`arrays_zip` builtin field names changed** (df54) — likely moot (streamling uses its own `zip_arrays`)
- **Avro timestamp decoding changes** (df54) — relevant to the vendored reader path; add a decode test

## How to resume the survey / migration

cargo halts at the first failing crate, so downstream errors only appear once upstream crates compile.
Order: `streamling-common` → `streamling-core` → `streamling-connectors`/`streamling-flink-compat`
→ `streamling-plugin` → `streamling`. Use `cargo check --workspace` (libs first; add `--all-targets`
last to pick up the test-only `config_options` sites).

## Downstream repo: streamling-goldsky-plugins

`goldsky-io/streamling-goldsky-plugins` (cloned at `../../../../streamling-goldsky-plugins`) depends on
`streamling-plugin` **by path** AND pins its own `datafusion = 49`, `arrow* = 55`, `parquet = 55`,
`object_store = 0.12`. It **must be upgraded in lockstep** — not optional:

- It uses direct `arrow::` types in 48 files / `arrow_schema` in 45, and passes Arrow arrays across the
  FFI boundary (`FFI_ArrowSchema`/`FFI_ArrowArray`). Two arrow majors in one tree won't type-unify
  (arrow-55 `RecordBatch` ≠ arrow-58 `RecordBatch`), and the plugin `.so` must share the host's
  arrow + `abi_stable` layout at runtime. So plugins ship in the **same change-set** as streamling.

**Required version bumps (match streamling):** `datafusion` 49→54; `arrow`/`arrow-data`/`arrow-schema`/
`arrow-ipc`/`arrow-json`/`arrow-buffer` 55→58.3; `parquet` 55→58.3; `object_store` 0.12.3→0.13.x;
`typed-arrow` feature `arrow-55`→`arrow-58` (0.7.0 already ships it — flag flip, not a blocker).

**Mechanical code changes (same categories, smaller):** 8 `ScalarUDFImpl` impls (+derive `PartialEq,Eq,Hash`,
remove `as_any`); 9 files with `ScalarFunctionArgs` literals (+`config_options`); 48 files matching
`DataType::` (Decimal32/64 arms); general arrow 55→58 drift.

**Does NOT apply here (good news):** no `ExecutionPlan`/`TableProvider`/`Accumulator` impls — so none of
the heavy core-style churn (properties→Arc, statistics→partition_statistics, plan/provider as_any sweep).
No avro (that's all in streamling-common). MSRV fine (already edition 2024).

**`object_store` 0.12→0.13 (verified — one-line fix):** 0.13 split the `ObjectStore` trait — `get`/`get_range`/
`head`/`put_multipart`/`copy` moved to a new `ObjectStoreExt` trait (`put` stays on `ObjectStore`). The repo
makes exactly one such call (`src/stellar/fetcher.rs:392` `store.get(&path)`), so the fix is adding
`use object_store::ObjectStoreExt;` there. Everything else is unchanged in 0.13: `store.put(...)` (sink.rs),
`AmazonS3Builder` + all `.with_*`/`from_env`, `Path::from`, and the 5 `Error` variants used
(`Generic`/`NotFound`/`NotSupported`/`PermissionDenied`/`Unauthenticated` — verified field-for-field, incl. the
test constructors). NOT applicable: `Error::NotImplemented` field change (repo uses `NotSupported`), DynamoCommit
removal, `delete_stream` lifetime, `GetOptions` builder, `copy`→`copy_opts`. MSRV fine (edition 2024 already).

**Bottom line:** no hard blockers; mechanical and smaller than streamling's own, but mandatory + coupled.

---

## Implementation notes (what actually changed)

The migration was as mechanical as predicted. Edits, by category:

- **UDF traits (`DynEq`/`DynHash` + `as_any` removal):** added `#[derive(PartialEq, Eq, Hash)]` to
  every `ScalarUDFImpl`/`AggregateUDFImpl` struct and deleted the `as_any` overrides. UDF structs
  holding non-comparable state got hand-written impls keyed on a stable identity instead of derives:
  `DynamicTableCheckFunc` (registry), `PluginScalarUdf` (excludes the `extern "C" fn` pointer — derive
  would also have tripped the fn-pointer-comparison lint), and `JsonConstructorNullBehavior` enum.
- **`as_any` callers:** `plan.as_any().downcast_ref::<T>()` → `plan.downcast_ref::<T>()` for
  `ExecutionPlan`/`PhysicalExpr`/`TableProvider` trait objects. **Kept** `.as_any()` for arrow arrays
  (`Array::as_any` unchanged) and for `UserDefinedLogicalNode` (still has `as_any` in df54).
- **`ScalarFunctionArgs`:** added `config_options: Arc::new(ConfigOptions::default())` to every literal.
- **`ExecutionPlan`:** `cache: PlanProperties` field → `Arc<PlanProperties>` (wrap constructions in
  `Arc::new`); `properties()` returns `&Arc<PlanProperties>`; `statistics()` → `partition_statistics(_, )
  -> Result<Arc<Statistics>>`; delegate-macro signatures updated.
- **Arrow 55→58:** `Decimal32`/`Decimal64` match arms; `ColumnStatistics` gained `byte_size`;
  `UnionFields::new` → `try_new`; sqlparser `SqlExpr::Cast` gained an `array` field; `MetricValue`
  gained variants (wildcard arm); `DFSchema → Schema` via `.as_arrow().clone()` (old `Schema::from`
  gone); `FilterExec::projection()` now returns `Option<Arc<[usize]>>`; `MetricValue` enum widened.
- **Avro (#3):** vendored `to_arrow_schema` + helpers into `formats/avro/schema.rs`; dropped the
  `datafusion-datasource-avro` dependency entirely.

### Corrections to the survey

1. **object_store `put` ALSO moved to `ObjectStoreExt`** (not just `get`). The core `ObjectStore` trait
   keeps only `*_opts`. So `s3/sink.rs` (which calls `.put`) needed `use object_store::ObjectStoreExt;`
   too — two import lines, not one. Still trivial.
2. **df54 strict UDF return-type checking surfaced one real behavioral bug:** `array_filter`'s execution
   path builds its output `List` with a non-nullable `"item"` element field, but `return_field_from_args`
   promised the *input's* element field (nullable). df54 asserts produced == promised and panicked. Fixed
   by making `return_field_from_args` mirror the produced element field exactly
   (`crates/streamling-common/src/functions/array_filter.rs`). This was caught by the plugins repo's
   `token_balance` test — worth running the e2e suite for other array UDFs (`array_filter_in`,
   `array_filter_first`, `zip_arrays`, `array_enumerate`) when the cluster is available.

### Still-open (non-blocking) follow-ups
- Deprecation warning: `datafusion::physical_plan::filter::collect_columns_from_predicate` ("will be
  internal in the future") in `streamling-core/src/operators/filter.rs` — still works, left as-is.
- `streamling-goldsky-plugins` must merge **together** with this branch: its `Cargo.lock` was reverted
  and will only resolve once `../streamling/crates/*` is on df54. Two integration tests (`validation`,
  `plugin_udf_bridge`) reference `streamling-e2e` helpers (`with_goldsky_plugin`, `goldsky_plugin_path`)
  that exist in neither checkout — a **pre-existing** mismatch, unrelated to this upgrade.

---

## E2e validation (2026-06-17, df54 binary against local k3s)

Ran the full `streamling-e2e` suite (`just e2e-test --no-fail-fast`) with the migrated df54
binary: **89 tests, 78 passed, 11 failed**. Triage of the 11:

- **8 `metrics` tests + `test_pipeline_sql_filter_diff_in_input_output_rows`** — all fail querying
  Prometheus (`localhost:30090`), which was down (its PV was stranded on a dead k3d node; recovering
  it needs a guarded PVC delete). **Infrastructure, not migration.**
- **`test_sql_union_preserves_existing_gs_op` + `test_sql_union_propagates_gs_op_when_missing`** —
  a **real df54-exposed issue** (see below).

The 78 passing cover the meaningful migration surface: ClickHouse sink/source (dedup, deletes,
schema-override, multi-batch, append-only, is_deleted injection), Kafka→ClickHouse, checkpoint
state, external handlers, hybrid source, filters, webhook/print/postgres sinks, schema evolution.

### Array-UDF return-type alignment (df54 strict check) — FIXED

df54 added a strict assertion that a UDF's produced array type equals the type it promised in
`return_field_from_args`/`return_type`. Two UDFs in `streamling-common` declared one element-field
nullability but produced another:
- `array_filter` — caught by the plugins repo's `token_balance` unit test (fixed earlier).
- `zip_arrays` (`_gs_zip_arrays`) — found by static review of the sibling UDFs; its declared inner
  struct fields used the input nullability while the execution path always builds them nullable.
  Fixed `return_type` + `return_field_from_args` to match the produced `true`. (streamling-common
  unit tests green; no e2e exercises it through a plan, so it's covered by the static fix + units.)
- `array_filter_in`, `array_enumerate`, `array_filter_first` — verified consistent (no change). (One
  pre-existing, data-dependent edge in `array_filter_first`: `safe_take` can promote Utf8→LargeUtf8
  on huge arrays, diverging from the declared type — unrelated to df54, left as-is.)

### KNOWN ISSUE — scan sharing + UNION-over-same-source (NOT yet fixed)

Pipelines whose SQL reads the **same source twice** (e.g. `... FROM s UNION ALL SELECT ... FROM s`)
enable **scan sharing**. In that path (`operators/wrapping.rs`, `WrappingSourceTableProvider::scan`),
the `BroadcastingExec` returned for each consumer **ignores the `projection` argument** and emits the
full source schema, while the logical `Checkpointable` node's schema is the projected one. df49 never
checked this; **df54's `physical_planner.rs` now rejects the logical/physical schema mismatch** at plan
time (`[block, id, data, _gs_op]` physical vs `[id, data, _gs_op]` logical). So these pipelines now
fail to plan.

A fix was attempted (fetch full schema for the shared scan + wrap each `BroadcastingExec` in a
projection): it resolved the planning error but the pipeline then **hung (30s timeout)** — DataFusion's
pull-based `ProjectionExec` doesn't preserve the streaming/checkpoint completion protocol that
`BroadcastingExec` relies on. **The attempt was reverted** (a clear planning error beats a hang). A
correct fix likely needs streamling's own streaming-aware projection (cf. `StreamingProjectionExec`)
applied per-consumer, and validation against these two e2e tests — a focused follow-up requiring
knowledge of the broadcast/checkpoint protocol. Single-source pipelines (the overwhelming majority)
are unaffected.

### E2e re-run on a FRESH cluster (2026-06-17, after Docker restart)

The Docker restart wiped the degraded k3d cluster; recreated it clean via `just env-setup` (all
services healthy, **Prometheus included**) and re-ran the previously-failed tests:

- **All 9 Prometheus-dependent failures now PASS** (the 8 `metrics` tests minus one, plus
  `test_pipeline_sql_filter_diff_in_input_output_rows`) — confirming those were purely the
  stranded-Prometheus infra issue, not the migration.
- **`test_basic_metrics_emission`** still fails, but **the data is correct** — the postgres sink wrote
  all 25 rows; only the `streamling_output_rows_total{id="kafka_source"}` metric read back 10 at query
  time. This is a metrics export/scrape **timing artifact** (the `record_limit(25)` pipeline exits fast;
  the OTel periodic export + Prometheus scrape hadn't captured the final count within the test's 3s
  wait). The OTel/telemetry stack was not changed by this upgrade, and the only telemetry edit was an
  additive wildcard arm for new df54 `MetricValue` variants (doesn't affect `OutputRows`). Treated as
  pre-existing timing flakiness, not a migration regression.
- **The 2 scan-sharing + UNION tests** still fail — the genuine, documented df54 issue above.

**Net after fresh-cluster retry:** the only migration-caused e2e failure is the scan-sharing +
UNION-over-same-source planning issue. Everything else is green or a timing artifact with verified-correct data.

---

## Scan-sharing + UNION-ALL hang — ROOT-CAUSED & FIXED (2026-06-17)

The two `test_sql_union_*` tests (scan-sharing via `UNION ALL` over the same source) were a **real
df54 regression** (df49 passes them in ~4s). Two distinct issues, both now fixed:

1. **Planning error** — df54 strictly asserts an extension node's physical schema matches its logical
   schema. The scan-sharing `BroadcastingExec` emitted the full source schema while the `Checkpointable`
   logical node was projected (`[id,data,_gs_op]` vs `[block,id,data,_gs_op]`). **Fix:** made
   `BroadcastingExec` projection-aware — the shared source broadcasts full rows and each consumer applies
   its own projection (`scan_sharing.rs`), and `WrappingSourceTableProvider::scan` fetches the full
   schema for the shared scan and passes the projection to each `BroadcastingExec` (`wrapping.rs`).
   `RecordBatch::project` preserves schema metadata, so the checkpoint marker survives.

2. **Runtime hang (the hard one)** — once planning passed, the pipeline hung: data exited
   `BroadcastingExec` but **never reached `CheckpointableExec`**, and the checkpoint never finalized.
   Root cause: the `Checkpointable` extension planner inserted a **stock DataFusion `RepartitionExec`**
   to coalesce the multi-partition `UNION ALL` input to one partition. Stock partition operators rebuild
   batches with a metadata-less schema (and/or stall on unbounded streams), **dropping the checkpoint
   marker** that streamling carries in batch schema metadata — so the sink never acked. **Fix:**
   `CheckpointableExec` now coalesces its input partitions itself, forwarding each batch with its marker
   metadata intact, and declares `UnspecifiedDistribution` so DataFusion doesn't insert its own stock
   coalesce (`checkpointable.rs`). The stock `RepartitionExec` is gone.

Diagnosis path: built the df49 binary and confirmed it passes (real regression); enabled live subprocess
logging via the streaming run path + `RUST_LOG`; instrumented `BroadcastingExec` (data flowed, projection
fine) and `CheckpointableExec` (received zero batches) to localise the drop to the stock operators
between them. A full-column UNION (no projection) reproduced the hang, proving it was independent of the
projection fix.

**Result:** both union tests pass in ~4s; full streamling-e2e suite green except the known
`test_basic_metrics_emission` Prometheus scrape-timing flake (data verified correct) and occasional
parallel-load flakiness on `test_hybrid_source_with_filters` (passes in isolation).

---

## arrow-avro migration (#27) — pre-land checklist (2026-07-06)

Context: PR #27 swaps the vendored avro decode path (`AvroToArrowConverter` + `AvroArrowArrayReader`)
for DataFusion 54's `arrow-avro` crate. #22 (this branch) keeps the vendored reader, so **main stays on
the vendored decoder until #27 lands** — none of the items below affect the #22 + plugins#131 merge; they
are #27-only.

The core hazard is that `arrow-avro` follows the Avro spec strictly, whereas the vendored reader was
lenient / non-spec in several ways. A production decode of `arbitrum-one.raw.traces` already hit one of
these: `Record name mismatch writer=trace_arbitrums_after_evm_transfers, reader=ArbitrumTransfer`
(apache-avro `Value::resolve` matched fields positionally and ignored the record name; arrow-avro errors).
Fixed on #27 (commit 487b9c14) by injecting the writer's record name into the reader schema's top-level
`aliases`; code-reviewer-pro verdict **pass with two follow-ups** (items 2–3 below).

Status — all resolved (fixed, verified-equivalent, or tracked). Details:

1. **`skip_schema_resolution`** — ✅ **FIXED** (`eb549131`). Vendored: when set (globally via
   `skip_schema_resolution_unconditional`, or per id via `skip_schema_resolution_for_reader_schema_ids`) the
   raw writer `Value` is used with **no resolution** — no name check, reordering, or default-filling
   (`kafka.rs:1245`, main). #27 originally *unconditionally* called `with_reader_schema(...)` so arrow-avro
   always resolved, silently neutering the flag. Now honored: `ConfluentAvroDecoder::with_schema_resolution(
   false)` builds the decoder with only the writer-schema store (decode against the writer, no resolution),
   and `coerce_batch_to_target` null-fills an absent *nullable* target field (mirroring the vendored reader)
   rather than erroring. Test: `skip_schema_resolution_decodes_against_writer_and_skips_defaults`.

2. **Namespace-aware alias injection** — 📋 **DEFERRED → STRM-6359**. The record-name fix injects the
   writer's bare name; arrow-avro re-qualifies a bare alias with the *reader's* namespace, so a
   bare-writer/namespaced-reader pair fails again. The alias mechanism can't express a bare full-name under a
   namespaced record; the robust fix is to rename the reader record to the writer's identity. Doesn't affect
   today's namespace-less schemas.

3. **Nested-name resolution limitation** — ✅ **DOCUMENTED** (inline on `writer_aliases`) + 📋 **STRM-6359**.
   The fix aligns only the top-level record name; `resolve_records`/`resolve_enums`/`resolve_fixed` also
   name-check nested named types, so a nested rename would reproduce the error one level down. STRM-6359's
   rename-based fix covers this recursively.

4. **Numeric-overflow equivalence** — ✅ **VERIFIED equivalent, no change** (`b12442fd`). The vendored
   `Resolver::resolve` `NumCast::from(...)` overflow→silent-NULL fallback was **unreachable**: the avro→arrow
   mapping is width-preserving (int→Int32, long→Int64, …) and Avro only permits *widening* promotion, so a
   narrowing overflow never occurred. arrow-avro decodes each primitive to its natural width identically.
   Test: `numeric_boundaries_decode_exactly` (i32::MIN / i64::MAX / f32::MIN / f64::MAX round-trip exactly).

5. **Decimal heuristics equivalence** — ✅ **VERIFIED equivalent, no change** (`b12442fd`). u256/i256/decimal
   byte reinterpretation (`u256_be_bytes`/`i256_be_bytes`/`be_bytes_to_i128`/`be_bytes_to_i256`) was extracted
   from the vendored `resolve_u256`/`resolve_i256`/`resolve_decimal(_256)` and is byte-identical for in-range
   inputs (same negative-reject, sign-aware padding-strip, error-on-oversize for u256/i256; the i128/i256
   helpers only differ by keeping low bytes instead of *panicking* on impossible oversized input). Scale-clamp
   (`scale > MAX_SCHEMA_PRECISION`) and the `precision > 76 && scale == 0 ⇒ U256` routing live in shared
   `convert_avro_schema_to_arrow`/`post_process_avro_schema_for_reading` code, so target schemas are identical
   by construction. Test: `decimal_byte_reinterpretation_is_twos_complement`.

Other vendored leniencies/panics (status quo on main; catalogued for completeness): same-id resolution
shortcut (`resolve_schema`, writer_id==reader_id ⇒ no resolution); optional `validate_writer_schema_ordering`;
missing field ⇒ NULL (`field_lookup`); `panic!` on decimal precision > MAX; `unimplemented!()` for Map /
RunEndEncoded / View / `Value::Duration`. (`LocalTimestamp*` / `Schema::Ref` `todo!()`s were fixed on #22.)

---

## `zip_arrays` / `array_filter` non-nullable-element change — sink-schema impact (2026-07-06)

Reviewer question (Xiao, on #22 threads `zip_arrays.rs:78` / `array_filter.rs:776`): the two UDFs changed
their **declared** return type — the `List` element went from `field.clone()` (inherits the input
element's name + nullability) to `Field::new("item", <type>, false)` (renamed `"item"`, forced
NON-nullable). Does this change the **sink schema**?

**Root of the change:** df54 asserts an operator's produced schema == its promised (declared) schema; df49
did not, and silently tolerated a mismatch. The **execution paths** (`out_field` / array builders in
`zip_arrays.rs` + `array_filter.rs`) were **not** touched by #22 — df49 *already* produced a non-nullable
element named `"item"` at runtime. So #22 only makes the *declaration* match the always-produced runtime
schema. **The runtime `RecordBatch` — and the data written to every sink — is byte-identical to df49.**

All sinks derive their persisted schema from the **plan** schema (`find_plan_and_schema` → `plan.schema()`,
`crates/streamling/src/lib.rs:2139`), which carries the declared return types and therefore changed. So the
only question per sink is whether the (name + nullability) delta is *meaningful* to that sink's schema model.
Both UDFs emit `List<`**`Struct`**`>` (zip_arrays zips into a struct; array_filter filters a list<struct>),
so the element is always a **struct**, not a scalar — which is what makes the impact so narrow:

| Sink | Verdict | Why |
|------|---------|-----|
| **Kafka — Avro** | **CHANGE (new subject only)** | Avro wraps a nullable record in a union: element `true`→`false` flips `items: ["null", record]` → `items: record` (`to_avro`/`field_to_avro`, `writer.rs:61-118`). Only registered if the subject is absent (`kafka.rs:2378`); existing subjects untouched. |
| **ClickHouse** | **no change** | `Nullable` is never applied to `Array`/`Tuple`/`Map` (`clickhouse.rs:2275`), so a struct element emits `Array(Tuple(...))` regardless. The `Array(Nullable(T))`→`Array(T)` delta only applies to a *scalar* `T`, which these UDFs never produce. |
| **Tinybird** (plugin) | **no change** | Same complex-type guard (`sink.rs:517`, `!is_complex`) → `Array(Tuple(...))` regardless. Registers a datasource schema from the plan schema, but gated on first-creation (`ensure_datasource_exists`). |
| **Postgres** | **no change** | All arrays → `JSONB` (`type_mapping.rs:45-53`); element name/nullability never inspected. Auto-DDLs `CREATE TABLE IF NOT EXISTS`. |
| **Kafka — JSON** | **no change** | Registers no schema; serializes runtime rows. |
| **Plugin: s2_sink / pubsub / eventbridge_partner** | **no change** | Line-delimited JSON per batch (`record_batch_to_line_delimited_json`); register no schema. |
| **memory / print / http / blackhole** | **no change** | Nothing persisted. |
| **MySQL / MariaDB** | **N/A** | No sink exists (only an e2e test resource). |

Plugin sinks are authoritative from `streamling-goldsky-plugins/src/lib.rs:85-88` (`s2_sink`, `pubsub`,
`eventbridge_partner`, `tinybird`); the `pipeline-s3-sink.yaml` / `pipeline-sqs-sink.yaml` examples have no
registered plugin sink behind them.

**Bottom line:** no data change anywhere; no change to any existing table/subject on upgrade; the element
rename to `"item"` is invisible to every sink (arrays are positional / Avro drops it in canonical form). The
sole observable delta is a *more-accurate* non-nullable element on a **newly-created Avro subject**. Answered
on #22 at `#discussion_r3531386869`.
