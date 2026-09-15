# PR #37 — deep second review

**Recommendation: request changes.** The repaired head still produces silently incorrect numbers, comparisons and row sets. Several are new regressions introduced by the repair itself. The failures below are executable reproductions, with native-decimal controls and an actual original-base checkout used to distinguish regressions from inherited limitations.

Reviewed head: `6e6072c7da604aa85536df7fe61ebbd025adb1bc`. Original base: `ce1892bc50d96b8ed208791d25f12608fed91fcf`. Earlier work began 2026-09-15 13:07:24 UTC. This historical timestamp is not a claim of uninterrupted review duration; the third-round report records the fresh continuous review interval. No production files or existing review tests were modified, and no PR comments were posted.

## Confirmed silent failures

### 1. P1 — A nested identity script changes the number

An actual embedded WASM identity transform, `row => ({nested: row.nested})`, changes nested decimal values when its output is serialized:

| Input | Output after identity script and Avro serialization |
|---:|---:|
| `1000` | **`3158064`** |
| `-256` | **`3290422`** |

This occurs in structs, lists, and lists of structs. Outbound IPC now converts nested decimal leaves to text, but inbound restoration only recognizes top-level decimal fields. Generic casts turn the nested strings into ASCII bytes while retaining decimal metadata; Avro then interprets those bytes as a different numeric magnitude. Restore nested leaves recursively before generic casting and validate canonical bytes before writing them.

Source: [ipc.rs:294–312](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/formats/ipc.rs#L294). Tests: `deep_v2_wasm.rs`.

### 2. P1 — A mixed-scale UNION stores `1` as `0.01`

The same source contains `a = 1` at scale 0 and `b = 1` at scale 2. This accepted SQL was tested through the actual ClickHouse sink input projection, with an explicit `coerce_to: string` directive:

```sql
SELECT id, a AS value, _gs_op FROM t
UNION ALL
SELECT id + 1, b AS value, _gs_op FROM t;
```

The stored values are **`"0.01"` and `"1.00"`**, although both inputs are numerically 1. The emitted Arrow data was inserted into a live local ClickHouse 24.8 instance and read back; a same-scale UNION control stores 1 and 1 correctly. Temporary test tables were removed. This second-round probe executed the production projection and then inserted the projected values separately; the third round adds the complete binary source → SQL → ClickHouse execution.

UNION drops conflicting scale metadata without normalizing its branches. The sink binds one conversion field at scale 2 and applies it to the scale 0 coefficient. Choose a common decimal type and rescale each branch before UNION, or reject the mismatch. The capability validator must also reject incomplete decimal extension metadata rather than skipping it.

Sources: [clickhouse.rs:480–495](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-connectors/src/table_providers/clickhouse.rs#L480), [decimal_arb_capability.rs:348](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/types/decimal_arb_capability.rs#L348). Test: `deep_v2_union_sink.rs`; live evidence: `union-clickhouse-live.log`.

### 3. P1 — Mixed DISTINCT aggregates count duplicates

For decimal input `[1, 1, 3, NULL]`, these accepted scalar-subquery queries produce coefficients representing **5 instead of 4** and **1.7 instead of 2** at the known result scales:

```sql
SELECT (SELECT SUM(DISTINCT a) FROM t HAVING COUNT(*) > 0) AS s
FROM t WHERE id = 1;

SELECT (SELECT AVG(DISTINCT a) FROM t HAVING COUNT(*) > 0) AS av
FROM t WHERE id = 1;
```

The focused test decodes the physical output at the expected scale; it does not assert that the text sink renders 5/1.7. The third round confirms the public comparisons against 4/2 are wrong in a complete pipeline. The decimal accumulators ignore `AccumulatorArgs::is_distinct`. DataFusion removes DISTINCT for some simple query shapes, masking the omission; combining it with an ordinary aggregate retains the flag. Matching native Decimal128 queries and isolated DISTINCT controls pass. These queries pass `SessionManager::create_supported_logical_plan` and execute successfully.

Sources: [decimal_arb_aggregates.rs:240](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_aggregates.rs#L240), [AVG constructor:683](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_aggregates.rs#L683). Test: `deep_v2_aggregate_continuity.rs`.

### 4. P1 — Derived tables and CTEs bypass the repaired comparisons

With same-scale `a = 255` and `c = 256`, a direct repaired CASE comparison works, but this query returns **zero rows instead of id 1**:

```sql
SELECT id
FROM (
  SELECT id, CASE WHEN id = 1 THEN a ELSE c END AS v, c FROM t
) AS q
WHERE v < c;
```

The inner CASE is stamped with decimal metadata, but the parent comparison was already left as native LargeBinary comparison. Filter pushdown retains `decimal_arb_with_meta(CASE...) < c` without a numeric comparator. Same-scale COALESCE and NULLIF fail through derived tables and chained CTEs too: **9 of 12 tested query shapes silently differ from native Decimal128**. The original-base checkout returns the correct result.

`NVL2(a,a,c) < c` exposes a related late-rewrite gap: it becomes a CASE during optimization, after the metadata-aware analyzer pass, and also reports 255<256 as false. Rewrite parents after projected metadata is available and cover expressions introduced by later simplification.

Sources: [decimal_arb_predicate_optimizer.rs:539–547](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L539), [registration in session.rs:194](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-core/src/session.rs#L194). Tests: `derived_*` and `nvl2_*` in `deep_v2_sql.rs`.

### 5. P1 — NULL arguments silently restore byte comparisons

`coerce` does not accept untyped NULL, so several rewrites abandon the entire expression. DataFusion then executes plausible but wrong byte comparisons:

| Expression | Expected | Actual |
|---|---:|---:|
| `greatest(255,256,NULL)` using decimal columns | 256 | **255** |
| `least(255,256,NULL)` | 255 | **256** |
| `255 BETWEEN NULL AND 256` | NULL | **false** |
| `-1 BETWEEN 0 AND NULL` | false | **NULL** |
| `CASE 256 WHEN NULL THEN false WHEN 256.00 THEN true ELSE false END` | true | **false** |

`NULLIF(a,NULL)` also loses metadata and makes a parent comparison wrong. The fixed IN-list handling passes its NULL matrix, but the other operators need equivalent null-aware handling. Actual original-base positive U256 extrema and BETWEEN controls pass.

Sources: [coerce:150–175](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L150), [BETWEEN fallback:309](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L309), [extreme_fold:255](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L255). Tests: `deep_v2_sql.rs`.

### 6. P1 — Mixed-scale branches can equate different numbers

Mixed-scale CASE/COALESCE/GREATEST/LEAST results are left as untyped bytes. Parent operators then compare coefficients without scales. Confirmed examples include:

- A CASE returning numeric 255 at scale 0 compares **equal to 2.55 at scale 2**.
- A GREATEST result of numeric 256 at scale 2 compares **equal to 25600 at scale 0**.
- `coalesce(a,b) < c` reports 255<256 as false.

The explicit `return None` for conflicting scales avoids stamping the wrong scale, but leaves an unsafe executable expression. Rescale result branches to a common decimal type or reject them; leaving raw bytes is not safe.

Source: [decimal_arb_predicate_optimizer.rs:124–127](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L124). Tests: mixed-scale and false-equality cases in `deep_v2_sql.rs`.

### 7. P1 — Numeric ordering/equality remains wrong inside collections and ordered aggregates

The literal-array MIN/MAX repair does not cover real decimal list columns or other composite operations:

- `array_min([255,256])` from a list column returns **256**; `array_max` returns **255**.
- `array_sort` returns **[256,255]**.
- List element and struct comparisons report **255<256 as false**.
- `array_has` can miss 256 when searched as 256.00, and can **find 2.55 in [255,256]**.
- Array DISTINCT and array/tuple equality compare scale-dependent bytes.
- An accepted scalar subquery using `ARRAY_AGG(a ORDER BY a ASC)` returns **[256,255]** for positive input 255,256. The sort rule only handles LogicalPlan::Sort, not aggregate ordering expressions.

The actual original-base checkout produces the correct positive ordering for these comparable operations, including ordered ARRAY_AGG. This is not merely the previously problematic ordering of signed fixed-width integers.

Sources: [literal-only array handling:504–515](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L504), [top-level decimal comparison guard:532](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L532), [sort-node-only rule](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_sort_optimizer.rs#L68). Tests: `deep_v2_sql.rs`, `deep_v2_aggregate_continuity.rs`.

### 8. P1 — NOT IN subqueries keep rows whose values are present

For `a = [255,256,-1,NULL]` at scale 0 and `b = [256,255,0,1]` at scale 2:

```sql
SELECT id FROM t WHERE a NOT IN (SELECT b FROM t);
```

Expected IDs are **[3]**; actual IDs are **[1,2,3]**. The query is accepted and executes, but DataFusion introduces a `LeftAnti Join` comparing raw LargeBinary coefficients after the rewrite stage. The same native Decimal128 query returns only id 3. Rewriting IN lists does not cover IN subqueries or subsequently introduced join comparisons.

Source: [InList-only handling:339](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L339). Tests: `not_in_filter_subquery_*` in `deep_v2_sql.rs`.

### 9. P1 — A wide integer literal changes before the decimal cast

```sql
SELECT CAST(18446744073709551617 AS DECIMAL(77,0)) FROM t;
```

Actual value: **18446744073709552000**, off by 383. The quoted literal remains exact. Lowering the cast through VARCHAR allows SQL planning to first parse the large literal as Float64; the new decimal parser then accepts its rounded scientific-notation text.

The actual original-base DECIMAL(77/78,0) route rejected that text through `to_u256`. This is a new transition from explicit failure to silently accepted approximation. It is distinct from the earlier DECIMAL(110) diagnostic, whose old VARCHAR fallback was already lossy. Preserve numeric literal tokens before Float64 planning when lowering wide decimal casts.

Source: [bigint_sql_preprocessor.rs:483–487](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-core/src/types/bigint_sql_preprocessor.rs#L483), [wide-cast replacement:533–541](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-core/src/types/bigint_sql_preprocessor.rs#L533). Current/base tests are included.

### 10. P2 — Rewrites evaluate volatile operands repeatedly

A deterministic volatile decimal UDF emits 257,258,259,... . A direct call returns 257 once. The repaired GREATEST returns **258**, NULLIF returns **259**, simple CASE chooses the **wrong branch**, and nullable null-safe equality returns **false instead of true**, because the rewrites clone and reevaluate operands.

Converting that same volatile result to native Decimal128 before each operation gives the correct result and exactly one call. Bind each operand once, or use dedicated decimal functions that preserve evaluation semantics.

Sources: [extreme_step:226–235](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L226), [null-safe equality:268](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L268), [simple CASE:414](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/functions/decimal_arb_predicate_optimizer.rs#L414). Tests: volatile cases and native controls in `deep_v2_sql.rs`.

## Additional confirmed issues, kept separate

- **Valid zero-width fixed lists can silently lose rows.** A single-column, three-row `FixedSizeList(decimal,0)` batch without a validity bitmap becomes zero rows through IPC. With another ordinary column present, row-length validation fails explicitly instead. The recursive bridge uses a constructor that infers length 0; preserve the explicit logical length. This is unusual valid Arrow input, not a claim about typical source schemas. [decimal_arb_text.rs:159](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/formats/decimal_arb_text.rs#L159).
- **An all-null decimal batch fails an identity script.** Flechette emits NullArray; the new top-level restoration rejects it instead of creating typed nulls. This is an explicit failure. [ipc.rs:237](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/formats/ipc.rs#L237).
- **A decimal primary key can panic in sink deduplication.** The existing Binary/LargeBinary branch downcasts both to BinaryArray, while the new decimal representation is LargeBinaryArray. The sink wrapper invokes this utility before writing. [dedup.rs:482](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-common/src/utils/dedup.rs#L482), [wrapping.rs:817](https://github.com/goldsky-io/streamling/blob/6e6072c7da604aa85536df7fe61ebbd025adb1bc/crates/streamling-core/src/operators/wrapping.rs#L817).
- **Three-argument GREATEST/LEAST and ordinary CASE/COALESCE arithmetic still have explicit planning failures.** The extreme fold only stamps the final CASE, leaving its intermediate CASE untyped; the new parent-expression rewrite handles comparisons but not arithmetic.

Nested-constructor/JSON_STRING/UNNEST hex-output gaps, ListView/Dictionary container gaps, and precision-only validation gaps are documented in the detailed notes. They were not promoted into newly introduced silent-number findings without adequate before/after or pipeline-reachability evidence. Projection IN, one window-rank experiment, and the final UNION DISTINCT/EXCEPT/INTERSECT diagnostics also failed native controls and were excluded.

## Validation and evidence

Across the SQL findings, a common requirement is to reject expressions once decimal scale information is unavailable; treating them as ordinary binary allows incorrect results to execute successfully. The fixes need coverage after later optimizer rewrites and through nested expressions, as well as direct scalar operations.

The numeric kernels received independent Python Fraction/half-even oracle checks: **1,908 divisions, 312 AVG batch/merge shapes, and 1,656 narrowing boundary cases all passed**. The Avro boundary oracle passed **1,164 cases**. Ordinary nested JSON shapes, scalar IPC text encodings and six plugin FFI shape/slice roundtrips also passed. These successes do not cover the SQL transformations and schema binding failures above.

The SQL differential matrices exercised **1,704 query shapes** against native Decimal128: **246 produced different values or Boolean/null outcomes**, 420 rejected, and 1,038 matched. These are repeated manifestations of the grouped findings, not 246 independent bugs. The original-base regression suite passed all five tests, including positive collection ordering and wide-literal rejection. The committed first-review suites now pass all 41 active tests (8 existing diagnostics ignored).

The existing library suites passed all 1,267 active tests (3 ignored). An initial sandboxed run could not bind mock-server sockets; the rerun with local socket access passed. Repository-required `just fix` and `just lint` passed without production changes. Exact commands, final test counts, all new test sources, oracle fixtures and raw logs are included in the evidence bundle and manifest. The detailed SQL and serialization notes preserve additional observations and exclusions.
