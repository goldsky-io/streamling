# decimal_arb coverage inventory

Audit of where `decimal_arb` is converted to/from across boundaries, which SQL
operations work, and whether DataFusion built-in / custom UDFs support it.
Two distinct axes are tracked: **code** (does the conversion/op exist in the
implementation?) and **e2e** (is there an end-to-end test exercising it?).
`decimal_arb` is physically an Arrow `LargeBinary` + extension metadata, so it is
**opaque** to anything that doesn't special-case it.

Legend: ✅ supported/tested · ⚠️ partial / unit-only / untested · ❌ absent

## 1. Conversion boundaries (source decode → arrow, arrow → sink encode)

| Boundary | top-level code | nested/complex code | e2e coverage |
| --- | --- | --- | --- |
| Kafka source — Avro decode | ✅ `avro/schema.rs`, `arrow_avro.rs` | ✅ (C1 fix; nested decimal→decimal_arb) | ✅ unit + `arrow_avro_equivalence` + most decimal e2e use this source |
| Kafka source — JSON decode | ✅ `json.rs` (top-level) | ❌ no nested recursion | ❌ no JSON-source decimal_arb test |
| Kafka **sink** — Avro encode | ✅ top-level | ❌ **F7** (nested → `Bytes` schema, encode fails) | ✅ top-level round-trip (`edge_boundary_sinks`); nested pinned (`edge_complex_decimal`, F7) |
| File source | ✅ via avro/json formats | inherits format | ❌ no decimal_arb file-source test |
| Postgres **sink** | ✅ → `NUMERIC(p,s)` (`pg.rs`, `value_binding`, `type_mapping`) | ❌ (no nested struct/jsonb path) | ✅ `decimal_arb_postgres`, `edge_*` |
| Postgres **source** | ✅ `pg.rs` auto-promotes `NUMERIC(78,0)`→decimal_arb | ❌ | ❌ untested |
| ClickHouse **sink** | ✅ native `UInt256`/`Int256`, `CanonicalString`, `Decimal` | ❌ nested (Array/Tuple of decimal_arb) not handled | ✅ `decimal_arb_clickhouse`, `edge_decimal_clickhouse` |
| ClickHouse **source** | ✅ native FSB(32)→decimal_arb + String→decimal_arb (H2) | ❌ nested | ⚠️ native ✅ (`hybrid_source`); **string path unit-only** |
| Hybrid source | ✅ schema adapter + `normalize_batch_from_clickhouse` | ❌ nested | ✅ `hybrid_source` |
| print sink | ✅ top-level (JSON) | ✅ **F6 fixed** (recursive metadata rewrite) | ✅ top-level (`edge_boundary_sinks`); nested (`edge_complex_decimal`) |
| Webhook / HTTP sink | ✅ top-level (JSON) | ✅ **F6 fixed** | ✅ top-level (`edge_boundary_sinks`) |
| External handler | ⚠️ JSON-based (same path as print/webhook) | ✅ **F6 fixed** (same JSON path) | ⚠️ same JSON path as webhook (not separately run) |
| MySQL sink | ❌ **no decimal_arb code at all** | ❌ | ❌ untested |
| SQS sink | ⚠️ JSON-based (top-level via json.rs) | ❌ | ❌ untested |
| memory / blackhole | n/a (discards) | n/a | n/a |

**Bottom line on boundaries:** well-covered (code + e2e) for **Kafka-Avro source, Postgres sink, ClickHouse sink/source (top-level), Hybrid**. **Code exists but no e2e:** Kafka-Avro sink, Postgres source, ClickHouse-source string path. **Top-level only via JSON, untested:** print, webhook, SQS, external handler. **No support:** MySQL sink. **Nested/complex `decimal_arb` (inside Struct/List/Map) is handled ONLY at the Avro boundary** (read + write); JSON, ClickHouse, and Postgres handle top-level columns only — a broad gap for the "complex types with decimal_arb" requirement.

## 2. SQL operations

| Operation | code | e2e | notes |
| --- | --- | --- | --- |
| `+ - * /  %` (both operands decimal_arb) | ✅ ExprPlanner→`decimal_arb_*` | ✅ `decimal_arb_arithmetic`, `edge_decimal_sql` | |
| decimal_arb × `Decimal128/256` column | ✅ planner inserts `to_decimal_arb_from_decimal*` cast | ⚠️ partial | only Decimal columns; |
| decimal_arb × **integer/float literal** | ❌ **F1** | tripwire | planner's `coerce_operand` maps only Decimal128/256, not Int64/literals |
| `= != < <= > >=` (both decimal_arb) | ✅ | ✅ | literal RHS → same F1 gap |
| unary `-` (neg) | ✅ | ⚠️ | |
| `abs` | ✅ `decimal_arb_abs` (named) | ⚠️ | DF builtin `abs()` does NOT route here |
| `CASE` / plain `COALESCE` | ⚠️ plans but **drops metadata (F2)** → sink fails | tripwire | use `coalesce_meta` to preserve metadata |
| `coalesce_meta(...)` | ✅ preserves first-arg metadata | ⚠️ | the working metadata-preserving path |
| casts to/from `decimal128/256/string/int` | ✅ | ✅ unit + `decimal_arb_casts` | |
| `ORDER BY` | ✅ sort optimizer → `decimal_arb_to_sort_key` | ✅ `wide_int_sort` | |
| `SUM / MIN / MAX / AVG / COUNT` | ✅ UDAF overrides | ⚠️ **unit-only** | streaming transforms reject bare aggregates; `postgres_aggregate` sink path untested for decimal_arb |
| `BETWEEN` / `IN` (column/self operands) | ✅ desugars to intercepted `>=`/`<=`/`=` | ✅ `edge_sql_runnable` | works without literals (numeric order, incl. negatives) |
| `BETWEEN` / `IN` (integer literal bound) | ❌ **F1** | tripwire `edge_sql_runnable` | literal coercion gap |
| `IS [NOT] NULL` | ✅ | ✅ `edge_sql_runnable` | |
| `GROUP BY` | ✅ groups by canonical bytes | ✅ unit (`session::tests::group_by_decimal_arb_groups_numerically_equal_values`) | verified: `5`/`5.0`/`05` collapse, `±0` same group |
| `DISTINCT` | ✅ dedupes by canonical bytes | ✅ unit (`session::tests::distinct_decimal_arb_dedupes_numerically_equal_values`) | numerically-equal values dedupe |
| `JOIN` on decimal_arb | n/a | n/a | streamling streaming transforms don't run JOINs |
| Window functions | n/a | n/a | not runnable in streaming transforms |
| `round/ceil/floor/trunc/sqrt/power/ln/log/exp/sign/...` | ❌ **no decimal_arb impl** | ❌ | see §3 |

## 3. DataFusion built-in UDFs on decimal_arb

**They do not work generically.** `decimal_arb` is `LargeBinary`+metadata; DataFusion built-ins operate on Arrow numeric types (`Int*`, `Float*`, `Decimal128/256`) and cannot interpret it. Each capability must be built specifically for decimal_arb.

- **Built for decimal_arb** (work): arithmetic operators, comparison operators, unary neg, `abs`, `SUM/MIN/MAX/AVG` (UDAF overrides), `COUNT` (type-agnostic), the casts, `to_string`, `to_sort_key`. Operators/aggregates are wired via the `ExprPlanner` + `register_udaf` overrides so normal SQL syntax reaches them.
- **NOT built** (do not work, would error or mishandle the bytes): every other numeric built-in — `round`, `ceil`, `floor`, `trunc`, `sqrt`, `cbrt`, `power`/`pow`, `ln`, `log`, `log10`, `log2`, `exp`, `sign`, `gcd`, `lcm`, `factorial`, `mod()` (as a function), `nanvl`, etc.; and all non-numeric built-ins are inapplicable. **None of these are tested** (they'd just fail). If any are needed for decimal_arb they require dedicated UDFs like `abs`/`neg` already do.

## 4. Custom UDFs in this repo

- **decimal_arb-specific** (`functions/decimal_arb_ops.rs`, `decimal_arb_aggregates.rs`, `decimal_arb_coercion.rs`, `decimal_arb_sort_optimizer.rs`): the set listed in §2/§3. Coverage is mostly **unit tests**; arithmetic/comparison/CASE are e2e via `edge_decimal_sql` (the broken cases as tripwires). `abs`/`neg`/aggregates are unit-only at e2e level.
- **`coalesce_meta`**: general metadata-preserving COALESCE — **does** carry `decimal_arb` metadata through. The only general-purpose custom UDF that interoperates usefully with decimal_arb, and the current workaround for the CASE/COALESCE metadata gap (F2).
- **All other custom UDFs** (`array_enumerate`, `array_filter*`, `zip_arrays`, `to_large_list`, `byte_to_hex`/`hex_to_byte`, `reverse_bytes32`, `conv_base`, `keccak256`, `xx_hash`, `generate_series`, `split_string_to_array`, `json_*`, `json_objects_to_clickhouse_tuples`, `uuid7`, `current_time/date/now`): operate on arrays / bytes / strings / json / time — `decimal_arb` is not a meaningful input, so no decimal_arb support is expected or needed. (`reverse_bytes32` was the retired-u256 helper.)

## Coverage closed in this pass

- Kafka **Avro sink** decimal_arb round-trip (top-level): tested ✅ (`edge_boundary_sinks`).
- **Webhook** and **Print** (JSON) decimal_arb (top-level): tested ✅.
- **Nested/complex decimal_arb** at the JSON output boundary (print/webhook/external
  handler) is now **supported** — **F6 fixed** via a recursive metadata-driven rewrite in
  `json.rs`. Still unsupported: the **Avro sink** (**F7**, nested fields emit as `Bytes`),
  and ClickHouse/Postgres (no nested column type). Nested *decode* avro→decimal_arb is
  correct (C1); nested decode from a JSON *source* remains unhandled (output-only fix).
- **Runnable SQL** over decimal_arb — `BETWEEN`, `IN`, `IS [NOT] NULL` (column/self
  operands): tested ✅; literal-bounded forms pin **F1**. (`JOIN`/window/bare
  aggregates aren't runnable in streaming transforms — out of scope by design.)

## Remaining gaps

**Real code findings to fix** (pinned as tripwire tests): **F1** (decimal_arb + integer
literal coercion), **F2** (CASE/COALESCE metadata loss; `coalesce_meta` is the
workaround), **F4** (sink retries non-retriable errors → hang; filed as STRM-6322),
**F5** (multiply overflow), **F7** (nested decimal_arb → Avro encode fails). **Fixed:**
**F3** (standard Decimal128/256 → Postgres bound the unscaled integer with the point
misplaced — now `unscaled_to_numeric_string`); **F6** (nested decimal_arb → JSON hex —
recursive metadata rewrite in `json.rs`).

**Coverage still thin** (lower priority): aggregates (SUM/MIN/MAX/AVG) are unit-only;
ClickHouse-source string→decimal_arb path is unit-only. (GROUP BY / DISTINCT are now
verified correct — numerically-equal values group/dedupe via canonical bytes.
Postgres source / MySQL sink / SQS sink / JSON-source-decimal / file source do **not**
exist or do not yield decimal_arb — not gaps.)

## Out of scope here: the `streamling-goldsky-plugins` repo

decimal_arb support and tests in *this* repo do **not** cover the additional
sources / sinks / transforms in `../streamling-goldsky-plugins` (e.g. the ethereum /
oasis / traces **sources**, the **pubsub sink**, and transforms like
`token_transfer_transform`, `instruction_transform`, `traces_grouping`). Those are
separate boundaries that must also convert to/from decimal_arb correctly (especially
the wide blockchain-integer fields) and need their own coverage. Flagged on the PR.
