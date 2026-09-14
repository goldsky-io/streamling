# PR 37 adversarial test handoff

These tests were written against PR head `3cdf2419a1846d5a7ef127d2eca64ebb3ddeedd3`
and base `ce1892bc50d96b8ed208791d25f12608fed91fcf`. They intentionally assert
correct numeric behavior and fail on the reviewed implementation. This commit
contains tests and this handoff only; production fixes are separate work.

## Run the reproductions

```sh
cargo test -p streamling-common --test review_math --test review_sql --test review_serialization --no-fail-fast -- --nocapture
cargo test -p streamling-core --test review_sql --test review_wasm --no-fail-fast -- --nocapture
cargo test -p streamling-connectors --lib pr37_adversarial_connector -- --nocapture
```

The core SQL tests use `SessionManager::create_supported_logical_plan` and actual
execution. The WASM tests use the embedded JavaScript runtime, not a mock.
Connector tests call the real conversion, schema, validation, and projection
helpers; they do not require a ClickHouse server.

## Findings covered

| Finding | Observed incorrect behavior | Tests |
|---|---|---|
| R1: premature division rounding | 19/80 exact-rational cases disagree; fractional digits become zero; a half-even boundary rounds to 0 instead of 1e-18; AVG also loses fractions | common `review_math.rs` division and AVG cases |
| R2: IPC/script numeric contract | Script returns `"100"`, Avro gets 12336; zero no longer matches JS string `"0"`; changing IPC scale turns 12.34 into 0.1234 | common `review_serialization.rs` IPC cases; core `review_wasm.rs` |
| R3: nested JSON decimal decoding | `"001000"` becomes 4096; `"1000"` becomes Avro 0; `"12.34"` fails as invalid hex | common `review_serialization.rs` nested JSON cases |
| R4: CASE/COALESCE comparisons | Matching negative rows are dropped; a CASE yielding 255 compares as not less than 256 | common/core `review_sql.rs` CASE/COALESCE and positive comparison cases |
| R5: uncovered numeric SQL dispatch | GREATEST(255,256)=255; array_min([255,256])=256; cross-scale 1 and 1.00 compare unequal in simple CASE, NULLIF, IS NOT DISTINCT FROM | common/core `review_sql.rs` |
| R6: NULL-containing IN list | A matching cross-scale member plus NULL yields NULL instead of true | common/core `review_sql.rs` IN cases |
| R7: composite VARCHAR casts | CASE/COALESCE values 1 and 2 become control-byte strings instead of decimal text | core `review_sql.rs` text-cast cases (SQL uses VARCHAR) |
| R8: nested ClickHouse values | Accepted nested 1.23 reaches a String leaf as canonical bytes 007B | connector nested wide-decimal case |
| R9: ClickHouse string ingestion | `"1.234"` silently becomes 1.23 at scale 2; excess integer precision is accepted | connector string fraction/precision cases |
| R10: multiplication precision cap | 1e-40000 * 1e-40000 silently becomes zero | common `review_math.rs` precision-cap case |

Use exact numeric assertions when fixing these. Returning an explicit precision
error is acceptable for the unrepresentable multiplication case. The IPC scale
mismatch may be rescaled or rejected. Unsupported nested ClickHouse decimals may
be rejected before writing or converted to exact decimal text. The connector
test executes the projection and inspects the resulting value, rather than
merely checking whether a new array was allocated.

The arithmetic oracle uses scaled BigInt quotient/remainder and one half-even
rounding; it does not use BigDecimal division as its reference. A separate grid
checks 2,000 operand pairs through canonical encoding and exact arithmetic.
Passing controls include top-level JSON/native IPC, Avro signed/null/wide-value
roundtrips, JS identity, native integer endianness, DISTINCT aggregates, and
per-batch UNION value preservation.

## Ignored diagnostics

The review found the following cases too, but did **not** classify them as
confirmed silent regressions introduced by this PR. They are retained with
`#[ignore = "reason"]`; inspect them separately with a test-name filter and
`-- --ignored --nocapture`.

- Pre-existing: unquoted wide numeric literals passing through Float64; Avro
  reader/writer scale mismatch; native ClickHouse UInt256/Int256 nullability.
- Secondary scope: aggregate output metadata, window ordering, mixed-scale CASE
  metadata, and downstream UNION planning. Normal streaming rejects aggregate
  and window plans; the CASE/UNION examples fail explicitly.
- Unproven UNION-to-Avro assertion: the planned Avro field is bytes, so comparing
  intermediate unscaled Decimal values 1 and 100 does not by itself establish
  numerical corruption after wire encoding/decoding.
- The cast probe prints diagnostic outcomes and has no correctness oracle.

The direct AVG arithmetic test remains active because it covers the same
division defect as R1, even though aggregate use in streaming is restricted.

## Review validation

Before this test commit, the existing library suites passed: common 386, core
555 (1 ignored), connectors 322 (1 ignored). `just fix` and `just lint` passed.
The focused handoff run contains 54 tests: 9 passed, 36 failed as expected, and
9 explicitly ignored diagnostics. The revised test helpers were rerun, and
`just fix` / `just lint` were rerun before committing.
The new failures are intentional reproductions and should become passing tests
as fixes land; do not invert assertions or mark confirmed regressions ignored
to make CI green.

A separate live probe against ClickHouse 24.8.14.39 verified that the nested
1.23 payload is successfully stored as text NUL + `{` (hex 007B). Its temporary
Memory tables were removed. A live native NULL-to-zero issue was also reproduced
but is pre-existing and ignored here. The full deployed Kafka/Postgres e2e
matrix and separate plugins repository were not covered by this review.
