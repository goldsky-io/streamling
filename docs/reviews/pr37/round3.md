# PR #37 — third adversarial review

**Request changes.** The repaired head still permits silent numerical corruption. This round adds four independently demonstrated failure families and strengthens earlier findings with complete source → transform → sink executions.

Reviewed engine: `6e6072c7da604aa85536df7fe61ebbd025adb1bc`; original base: `ce1892bc50d96b8ed208791d25f12608fed91fcf`. Companion plugin repository: `11d5e0b0e66bab2f87376cb022ce389991186ac1`. Production code was left unchanged. No GitHub review comments or messages were posted.

The uninterrupted review resumed at **2026-09-15 16:24:15 UTC**. The earlier spend-cap interruption and inactive gap are excluded. Final verification time and executed commands are recorded in the evidence manifest; parallel agents' time is not added to the elapsed interval.

## Newly demonstrated failures

### 1. P1 — An existing plugin's integer `1` becomes `2^248` in ClickHouse

The current companion Ethereum source still emits `FixedSizeBinary(32)` with `ARROW:extension:name=streamling.u256`, containing **big-endian** integers. The PR removes the sink's conversion of that representation to ClickHouse's little-endian integer bytes. The replacement fallback accepts the old field and clones its bytes.

The **actual ClickHouse table provider and writer** successfully inserted five representative source values into a pre-existing UInt256 table. Every value read back incorrectly:

| Source value | Stored value |
|---|---|
| `1` | `452312848583266388373324160190187140051835877600158453279131187530910662656` (`2^248`) |
| `16` | `7237005577332262213973186563042994240829374041602535252466099000494570602496` |
| `256` | `1766847064778384329583297500742918515827483896875618958121606201292619776` |

The new engine JSON converter also changes the same existing plugin's integer 16 into a hex string ending in `10`. The unchanged full companion library type-checks against this engine with Rust 1.91.1 after a one-line scratch lockfile refresh. Its locally defined representation remains unchanged; compiling it does not migrate the encoding. The PR already acknowledges plugin work as a follow-up; these tests establish that accepting those sources during the transition causes corruption, not simply an unavailable feature.

Migrate that source contract with the engine, preserve a checked legacy conversion, or reject the incompatible schema before processing. Creating a new ClickHouse table instead also loses the numeric contract by selecting FixedString(32).

Changed source: [clickhouse.rs:2785–2799](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-connectors/src/table_providers/clickhouse.rs#L2785). Companion contract: [utils/u256.rs](https://github.com/goldsky-io/streamling-goldsky-plugins/blob/11d5e0b0e66bab2f87376cb022ce389991186ac1/src/utils/u256.rs#L32).

Tests: `deep_v3_plugin_clickhouse.rs`, `deep_v3_plugin_contract.rs`. The real source utility was separately executed, and the real sink used its exact schema/byte contract; an historical compiled plugin shared library was **not** loaded. Legacy I256 reversal was also demonstrated, but no current companion I256 producer exists, so it remains a diagnostic. New signed decimal conversion passed 22 boundary/null rows and four overflow rejection cases.

### 2. P1 — Nested decimals bypass the plugin rejection and serialize as different numbers

The startup validator rejects top-level decimal_arb for unsupported plugin sinks, but only recurses through containers for ClickHouse/Hybrid. Consequently a Struct, List, or List<Struct> containing the same unsupported decimal is accepted for a plugin sink.

The actual pinned companion JSON helper emits 16 and 18 as **`"0010"` and `"0012"`** in all three accepted shapes. These strings remain valid decimal text with different values. Its native Decimal128 controls preserve 16/18. This helper feeds S2, Tinybird, Pub/Sub and EventBridge; the local probe executed validation and serialization without sending records to those external services.

Apply the plugin capability decision recursively until each plugin has an exact decimal implementation. A direct top-level serializer call is excluded as a pipeline finding because the engine correctly rejects that schema.

Changed source: [decimal_arb_capability.rs:294](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/types/decimal_arb_capability.rs#L294), [container traversal:333](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/types/decimal_arb_capability.rs#L333). Test: `v3_plugin_rejection_must_apply_to_nested_decimal_leaves`; actual helper probe and logs are included in the evidence.

### 3. P1 — Ordinary aggregate outputs silently revert to byte comparisons

Over `a = [255,256]`, this complete bounded file-source pipeline emits **false**, expected true:

```sql
SELECT id,(SELECT MIN(a)<MAX(a) FROM t) AS result
FROM t WHERE id=1;
```

If b holds the identical numbers at scale 2, `MIN(a)=MIN(b)` also emits false. Neither query uses DISTINCT, CASE, or UNION. Native Decimal128 full-pipeline controls return true. Original-base controls preserve the positive MIN/MAX ordering, HAVING result and derived aggregate filter.

The wrappers implement a physical `return_type` but omit `return_field` with precision/scale metadata. Parent comparisons consequently resolve as LargeBinary operations. This also affects SUM/AVG comparisons, removes a valid HAVING row, and changes a comparison of running MIN/MAX outputs. The independent matrix found nine wrong results, thirteen exact controls and five excluded unsupported/native-failing cases.

Implement metadata-bearing aggregate result fields using each aggregate's output precision/scale rules, and verify the parent predicates after planning. This needs a separate fix from the DISTINCT accumulator issue.

Changed source: [decimal_arb_aggregates.rs:451–456](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_aggregates.rs#L451); equivalent SUM/AVG paths at 220 and 658. Tests: `deep_v3_relational.rs`, `v3_e2e_bounded_decimal_aggregate_parent_*`.

### 4. P1 — UNNEST turns decimal values into misleading strings

A real Kafka Avro source carrying `array(decimal(100,2))`, this SQL, and the print sink complete successfully:

```sql
SELECT id,UNNEST(xs) AS value,_gs_op FROM src;
```

The output values are wrong:

| Numeric input | Actual JSON string |
|---|---|
| `1.23` | `"007b"` |
| `-4.56` | `"ff01c8"` |
| `0.16` | `"0010"` |
| `0.18` | `"0012"` |
| `2.56` | `"000100"` |

The last three strings are still syntactically valid decimal text. Native precision-30 full-pipeline controls preserve all seven tested values. A byte-identical twelve-byte Confluent fixture also preserves 1.23/−4.56 on the original base and produces hex on this head.

The new nested LargeBinary representation depends on field metadata, but the existing UNNEST schema rebuild constructs fresh fields without it. Preserve the complete metadata while flattening. This is an integration regression caused by the new representation even though the UNNEST source file was not edited by the PR.

Changed introduction point: [avro/schema.rs:431–433](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/formats/avro/schema.rs#L431). Field-loss site: [unnest.rs:463](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-core/src/operators/unnest.rs#L463). Tests: `deep_v3_unnest_attribution.rs`, `v3_e2e_decimal_unnest_json`.

## Earlier findings strengthened in this round

The ten previous review groups remain documented in the second-round report. They were independently audited against the raw logs; the following now have complete binary evidence rather than only focused operator/component tests:

| Failure | Verified result through the complete binary |
|---|---|
| Nested identity script | `1000 → 3158064`, `-256 → 3290422`, through Kafka → JavaScript → Kafka Avro → Postgres JSONB; direct control exact |
| Mixed-scale UNION | Two numeric ones become `"0.01"` and `"1.00"` in actual ClickHouse; same-scale control exact |
| DISTINCT SUM/AVG | Public Boolean comparisons against correct 4/2 emit false; native controls true. The focused accumulator evidence establishes actual coefficient results 5/17 at scales 0/1. Text-cast output is separately raw control bytes, not claimed as numeric text 5/1.7 |
| Derived CASE comparison | `255 < 256` emits false after the inner projection; native and direct controls true |
| NULL extrema/BETWEEN | GREATEST with NULL selects 255 over 256; BETWEEN returns false instead of NULL; native controls correct |
| Mixed-scale CASE | Numeric 255 compares equal to 2.55 |
| NOT IN subquery | Keeps ids `[1,2,3]` instead of `[3]`; with RHS only 2.55, drops 255 and emits `[2,3]` instead of `[1,2,3]`; both native controls exact |
| Wide unquoted literal | Postgres stores `18446744073709552000` instead of `18446744073709551617`; quoted control exact. Original base explicitly rejects the unquoted expression |

Collection ordering and repeated volatile-operand evaluation retain their focused execution evidence. The full Kafka list-min experiment fails explicitly before output, so it is **not** claimed as an additional silently wrong full-pipeline result. The native/base positive collection controls executed were specifically array_min/max, list-element and struct comparisons, and ordered ARRAY_AGG; broader cross-scale collection claims are based on current/native differential tests.

## Additional new-input gap: Parquet metadata loss

A valid Parquet file containing decimal_arb(100,2) numeric 1 emits **"0064"** through the complete file → print pipeline; decimal_arb(100,0) emits **"0001"**. Both processes succeed. Parsing "0064" as decimal produces 64 instead of 1; "0001" still denotes 1 but has lost its declared decimal type and output format. The same result occurs with same-scale and mixed-scale files, including reversed file order. Five successful-output assertions fail the exact decimal output contract; they are not five independent numeric changes. Two equality-filter cases fail loudly instead; native controls preserve the values, and native conflicting scales explicitly reject at schema inference.

The file reader constructs `ParquetFormat::default()`, which discards extension metadata and uses BinaryView. Explicit format options in a component control preserve the exact number and reject conflicting metadata; available pipeline environment flags do not alter that default instance. See [file.rs:79](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-connectors/src/table_providers/file.rs#L79) and `deep_v3_parquet.rs`/`deep_v3_parquet_options.rs`.

This is a **new-type integration gap**, not a proven before/after regression of an existing supported Parquet decimal contract: the file reader defaults predate the PR and the extension type did not exist on the base. Ordinary binary controls correctly remain hex. The tests also check exact row IDs and reject unrelated setup errors.

## Expanded literal boundary evidence

A full bounded CSV → SQL → Postgres NUMERIC(77,0) matrix tests thirteen integer literals. All thirteen quoted literal controls preserve exact values. Five unquoted values change silently, while eight controls remain exact:

| Input | Stored value | Difference |
|---|---|---|
| 2^64 | 18446744073709552000 | +384 |
| 2^64 + 1 | 18446744073709552000 | +383 |
| 2^65 | 36893488147419103000 | −232 |
| 10^20 + 1 | 10^20 | −1 |
| 10^76 + 1 | 10^76 | −1 |

The complete source/SQL/sink process succeeds. Expected values and stored values are compared as exact integer text, never via floating point. This strengthens the earlier literal finding and directly demonstrates single-unit silent drift. Test: `deep_v3_literal_pipeline.rs`.

## Successful coverage and exclusions

- **12,096 exact arithmetic result comparisons** through the actual binary: 672 distinct independently generated signed/null/boundary fixtures, six arithmetic/recovery expressions, and three paths (direct Postgres, Avro roundtrip, JavaScript identity). Seven scale pairs include 0,2,18,78,100. Python Fraction provides the independent reference with single half-even rounding; no float tolerance is used.
- **234 exact hybrid-source values** matched across bounded ClickHouse and live Kafka phases. Six configurations cover String, Decimal(76,18), Decimal(76,38), UInt256 and Int256; signed extrema, nulls, unsigned boundaries and scale 100 fractions are included. The fixture is verified on ClickHouse before running the pipeline.
- **2,844 framed Confluent records across 204 configurations** passed mixed writer-schema-id, defaults, order, nullable-reader-field, root-union and high-scale checks. Support probes rejected by both versions are excluded from numeric counts.
- **1,200 mixed native/integer arithmetic result checks** and additional unsigned-max, null/scalar/empty-batch, negative-scale narrowing, modulo, precision-cap and rounding-carry controls passed.
- **256 ordinary aggregate-wrapper query/type combinations** yielded 187 exact type/value/nullability matches and 69 matching rejections, with no one-sided mismatch.
- A final actual-SessionManager scalar probe covered twenty function/scale pairs (ROUND, TRUNC, FLOOR, CEIL, ABS, POWER, SQRT and CEILING). All arbitrary-decimal forms rejected explicitly; seventeen native controls executed, two CEILING spellings were unregistered, and one native POWER overflowed. No additional silently accepted scalar result was found; these are unsupported-operation checks, not supported numeric roundtrips.
- Existing legacy-plugin Avro byte payloads survive equally on old/new heads; their missing numeric annotation is inherited. The new nested native-decimal Avro path passes a case that the base rejected.

Invalid constructor declarations above u32 range wrap into apparently valid precision/scale. Three tests reproduce this **P2 validation issue**; it is separate from valid-number corruption. A complete identity-script pipeline rejects a valid all-null arbitrary-decimal batch; native all-null and arbitrary mixed-null controls pass. The previously demonstrated ClickHouse native-key projection still panics in the focused deduplication test. A new complete Postgres test with explicit deduplicate:true and duplicate decimal keys passes, keeping that failure scoped to the incompatible projected array path. Initial harness errors (missing `_gs_op`, incomplete Postgres fixture schema, or partial unbounded script batches) are excluded and retained transparently in diagnostic logs.

## Final validation

The final replay used the unchanged reviewed binary and all new/retained review test targets, after independent assertion audits. Counts below describe tests, not independent bugs; individual matrix tests can check hundreds of values.

| Group | Passed | Failed | Ignored |
|---|---:|---:|---:|
| Review common tests | 25 | 12 | 6 |
| Review core tests | 22 | 64 | 12 |
| Review connector component tests | 6 | 7 | 5 |
| New full-binary e2e tests | 32 | 26 | 0 |
| Live ClickHouse provider tests, explicitly enabled | 2 | 2 | 0 |

The four default-ignored live cases in the connector row are executed separately in the last row. Their two failures are the current U256 contract regression and the qualified legacy I256 diagnostic. Some other ignored cases deliberately retain unsupported or inherited experiments and are not review findings.

All **270 existing e2e tests across 42 targets** passed locally. Initial print-output failures caused by inherited RUST_LOG=warn were rerun with info logging; these are harness/environment failures, not PR findings. Repository `just fix` and `just lint` pass, and existing PR CI was green before the test push. The prior library run at the identical reviewed head passed 1,267 active tests (three ignored); it was not relabeled as a fresh third-round library run.

The new full-binary replay includes all 12,096 arithmetic comparisons and 234 hybrid values described above. Both JSON oracle files and all twelve Parquet files independently regenerated byte-identically. Public SQL, complete pipeline, native/base, and actual-provider evidence are differentiated in the findings. Historical plugin shared-library loading remains unverified.

The pushed tests assert the required behavior and deliberately remain red where fixes are needed. Reproduction commands and per-target results are in the accompanying README and test-results.json. The separate evidence archive retains exact commands, source/binary hashes, raw logs, matrices, portable fixtures, original-base controls, excluded preliminary experiments, and the uninterrupted review timestamps.

## Fix direction

Scale cannot safely live only in optional metadata while unsupported expressions continue as generic binary. Preserve it across aggregate/UNNEST/container boundaries, normalize branches before merging schemas, cover comparisons introduced by later optimizer passes, and reject plans or connector schemas whose decimal identity has been lost. Migrate or reject existing plugin types before removing their converters. The passing arithmetic checks narrow the likely problem area; they do not establish that the PR is free of other silent failures.
