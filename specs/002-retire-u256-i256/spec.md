# Feature Specification: Retire U256/I256 — Unify on decimal_arb

**Feature Branch**: `002-retire-u256-i256`
**Created**: 2026-05-11
**Status**: Draft
**Input**: User description: "Retire the U256 and I256 extension types in favor of the existing decimal_arb type."

## User Scenarios & Testing *(mandatory)*

The pipeline authors and platform operators are the relevant actors. Pipeline authors write SQL transforms; platform operators run those pipelines against Kafka, Postgres, and ClickHouse.

Today, blockchain pipelines route wide integers (Ethereum-style `uint256` / `int256`: gas, balances, token amounts) through two dedicated extension types whose surface in SQL is incomplete and partially broken. The `decimal_arb` type that landed in feature 001 is a strict numeric superset of both. This feature retires the dedicated wide-integer types so the pipeline author sees a single, consistent wide-numeric story — one that supports the full set of SQL operations correctly.

---

### User Story 1 — Sorts and comparisons on signed wide integers produce correct results (Priority: P1)

A pipeline author who joins, filters, or orders by a signed wide-integer column (e.g. a token balance delta that can be positive or negative) expects mathematically correct ordering: −100 sorts before 0 sorts before +100; `WHERE balance < 0` returns negative-balance rows.

**Why this priority**: This is a silent correctness bug today. The pipeline author has no warning that their `ORDER BY` or `WHERE` clause is returning the wrong rows when the column has mixed-sign values — the query "succeeds", just with wrong data. Any downstream analytics computed on top will inherit the error. Correctness bugs that produce wrong-but-plausible output without surfacing an error are the highest-risk category; this must be fixed first.

**Independent Test**: Produce a stream of records into a Kafka topic with an Avro `decimal(77, 0)` field containing both negative and positive values. Configure a pipeline that orders by that field ascending and writes to a Postgres `NUMERIC(77, 0)` sink. After the run, query the sink table ordered by row index. The output rows must appear in numeric ascending order: most-negative first, zero in the middle, most-positive last. Repeat with `WHERE col < 0` and confirm only the negative rows are returned.

**Acceptance Scenarios**:

1. **Given** a signed wide-integer column with values `[+1000, -100, 0, +1, -1]`, **When** the pipeline sorts ascending and writes them out, **Then** the output order is `[-100, -1, 0, +1, +1000]`.
2. **Given** the same input, **When** the SQL transform filters with `WHERE col < 0`, **Then** exactly the rows with values `-100` and `-1` appear in the output.
3. **Given** a stream with a value at the negative extreme (close to −2^255), **When** sorted ascending, **Then** that value appears in the first position.

---

### User Story 2 — Aggregate operations on wide-integer columns work in SQL transforms (Priority: P1)

A pipeline author who wants to compute totals (`SUM(gas_used)`), extremes (`MIN(balance)`, `MAX(balance)`), means (`AVG(price)`), or counts (`COUNT(*)`) over a wide-integer column expects these to work in an in-pipeline SQL transform — the same way they work for narrow numeric columns. Today, in-pipeline aggregates on wide-integer columns either fail outright or silently produce wrong-typed results, forcing the author to push aggregation down to a `postgres_aggregate` sink as a workaround.

**Why this priority**: Aggregation is a fundamental SQL operation that pipeline authors expect to work on any numeric column. The workaround (`postgres_aggregate` sink) only applies to Postgres destinations and forces additional Postgres-side infrastructure (a landing table, a trigger), turning a one-line SQL transform into a much heavier deployment. The workaround does not exist at all for ClickHouse or Kafka sinks. This blocks legitimate analytics use cases.

**Independent Test**: Produce a stream of records into a Kafka topic with an Avro `decimal(78, 0)` field. Configure a pipeline whose SQL transform computes `SUM(amount)`, `MIN(amount)`, `MAX(amount)`, `AVG(amount)`, and `COUNT(amount)` and writes the five results to a Postgres sink. Compare against the same aggregates computed in Postgres directly on the input data. Results must match exactly.

**Acceptance Scenarios**:

1. **Given** a column with 1000 values, **When** the SQL transform computes `SUM(col)`, **Then** the result equals the arithmetic sum of all 1000 input values with no precision loss.
2. **Given** a column with mixed-sign values, **When** the SQL transform computes `MIN(col)` and `MAX(col)`, **Then** `MIN` returns the most-negative value and `MAX` returns the most-positive value.
3. **Given** a column with 100 values, **When** the transform computes `AVG(col)`, **Then** the result equals the mean of the 100 input values (within rounding to a reasonable scale).
4. **Given** a `GROUP BY some_key` query that aggregates a wide-integer column, **When** the pipeline runs, **Then** each group's aggregate is computed correctly and the output rows match the per-group expected results.

---

### User Story 3 — `CAST(wide_int_col AS TEXT)` works without explicit UDF invocation (Priority: P1)

A pipeline author writing `SELECT * EXCEPT col, CAST(col AS TEXT) AS col FROM source` — the canonical "stringify one wide-integer column for downstream JSON or text consumption" transform — expects the cast to succeed and produce canonical decimal text. Today this fails at pipeline start with an "unsupported cast" error, forcing the author to remember and use a non-standard UDF name. This is the wide-int text-cast regression.

**Why this priority**: This is an active in-production regression with a public bug report and two open in-flight fix PRs. It blocks pipelines that previously worked. The workaround (use the `u256_to_string` / `i256_to_string` UDF instead of `CAST`) requires the author to know that the column is wide-integer-typed, which is implementation-leaky and obscures intent.

**Independent Test**: Take the canonical CAST-AS-TEXT YAML. Produce records with a wide-integer column. Run the pipeline. The pipeline starts successfully and the output column carries canonical decimal text (e.g. the integer `12345` becomes the string `"12345"`).

**Acceptance Scenarios**:

1. **Given** a pipeline using `SELECT * EXCEPT gas_used, CAST(gas_used AS TEXT) AS gas_used FROM traces` on a source where `gas_used` is wide-integer-typed, **When** the pipeline starts, **Then** it does not fail with an "unsupported cast" error.
2. **Given** an input value of `12345`, **When** projected via `CAST(col AS TEXT)`, **Then** the output is the string `"12345"`.
3. **Given** all four spellings (`TEXT`, `VARCHAR`, `STRING`, `CHAR`), each in upper, lower, and mixed case, **When** used in a `CAST` against a wide-integer column, **Then** all variants succeed and produce the same canonical string output.

---

### User Story 4 — Existing ClickHouse pipelines using `UInt256`/`Int256` continue to work unchanged (Priority: P1)

A platform operator running an existing pipeline today that reads from or writes to a ClickHouse table with `UInt256` or `Int256` columns expects to redeploy onto a newer streamling without changing the YAML, without altering the ClickHouse schema, and without losing storage compactness. ClickHouse's native `UInt256`/`Int256` are first-class fixed-width types and the existing tables are sized for them; the migration must not force re-typing those columns to `Decimal(78, 0)` or `String`.

**Why this priority**: Without this, the migration is breaking for every existing wide-integer pipeline targeting ClickHouse — which is the headline use case for u256/i256. A breaking ClickHouse migration would need lock-step coordination across pipelines and downstream consumers and is operationally unacceptable.

**Independent Test**: Take an existing pipeline whose source is a ClickHouse table with a `UInt256` column and whose sink is another ClickHouse table with a `UInt256` column. Run the pipeline against fresh streamling. No YAML changes. No `CREATE TABLE` changes. The output table has `UInt256` columns and the integer values round-trip exactly.

**Acceptance Scenarios**:

1. **Given** an existing ClickHouse source table with a `UInt256` column, **When** streamling reads from it, **Then** the values flow through the pipeline without precision loss.
2. **Given** a pipeline sink table created (or existing) with `UInt256` columns, **When** streamling writes wide-integer values to it, **Then** the destination column type is `UInt256` (not `Decimal(78, 0)`, not `String`) and the values match the source.
3. **Given** a pipeline whose source column is signed wide-integer (`Int256`), **When** routed through to a ClickHouse sink, **Then** the destination column type is `Int256` (not `UInt256`).

---

### User Story 5 — Pipeline authors get one wide-integer story to learn, not three (Priority: P2)

A pipeline author migrating an existing pipeline from a narrow decimal to a wider one — or onboarding to streamling for the first time — expects a single, consistent wide-numeric type with a documented surface. Today there are three (`u256`, `i256`, `decimal_arb`) with overlapping ranges, different operator coverage, different SQL idioms, and different connector behavior. The author has to learn which is which, when to use which, and which SQL is safe on which.

**Why this priority**: Quality-of-life and onboarding-cost improvement. Real but not urgent; nothing breaks if this lands later. Bundling it into the same change set as US1–US4 is high-leverage because once US1–US4 land, all wide-integer columns flow through one type anyway; the documentation and surface convergence is a natural byproduct rather than separate work.

**Independent Test**: The documentation page on wide-integer support mentions one type. A new pipeline author can write a working pipeline using a wide-integer column (Avro `decimal(78, 0)` → `NUMERIC(78, 0)`) without ever encountering the names `u256` or `i256` in error messages, YAML, or SQL.

**Acceptance Scenarios**:

1. **Given** a new pipeline author reading the wide-integer documentation, **When** they look up "how do I work with values larger than `Decimal256`", **Then** the documentation describes one type and one set of operations.
2. **Given** the codebase after this migration, **When** a developer greps for `u256` or `i256` outside of the connector wire-format adapters, **Then** zero results return.

---

### Edge Cases

- **Mixed-sign wide-integer sort at scale**: ordering 100k rows with a roughly even split of negative and positive values must produce a stable, monotonically-numeric output order. (Today: silently wrong for any negative-containing dataset on i256.)
- **Overflow in `SUM`**: summing 100 values each near 2^256 − 1 mathematically exceeds 2^256. The aggregate must surface the full result (wider precision, no overflow), not wrap.
- **`AVG` on a column where the arithmetic mean is fractional**: e.g. averaging the integers 1, 2, 3 yields 2.0; averaging 1, 2 yields 1.5. The output type must carry the fractional result correctly.
- **`AVG`/`SUM` on an empty input**: returns `NULL`, consistent with standard SQL.
- **`COUNT(col)` on a column with `NULL`s**: returns the count of non-null rows, consistent with standard SQL.
- **Existing checkpoint state from a prior streamling version**: streamling pipeline checkpoints record source-side offsets only — they do not carry the Arrow schema of in-flight data. On restart, the pipeline resumes consuming from the stored offset, the source decodes records per its unchanged wire schema, and the new code routes them through `decimal_arb` instead of `u256`/`i256`. No operator action is required; rollback (post-002 → pre-002) is symmetric.
- **A YAML pipeline that explicitly references the old type name**: today no YAML grammar requires the author to name `u256` or `i256` directly (the types are inferred from source schemas), so this should not arise. If it does — e.g. a `schema_override` map naming the old type — the pipeline must surface a clear "unknown type" error at config load.
- **Concurrent rollout**: a ClickHouse table being read by an old streamling version and an updated streamling version simultaneously. Both versions must see the same underlying ClickHouse data; ClickHouse-side schema is unchanged.
- **Round-trip through a wide-integer-incapable sink**: e.g. a Kafka Protobuf sink. The connector capability matrix must surface the same rejection (or `coerce_to: string` opt-in) it surfaces for any other wide `decimal_arb` column; no behavior difference from prior u256/i256 handling.

## Requirements *(mandatory)*

### Functional Requirements

**Correctness (US1)**

- **FR-001**: System MUST produce numerically-correct ascending order when sorting a column whose values are signed wide integers with mixed sign — values that are mathematically smaller MUST appear before values that are mathematically larger, regardless of two's-complement byte representation.
- **FR-002**: System MUST evaluate `<`, `<=`, `>`, `>=`, `=`, and `!=` on wide-integer columns in `WHERE` clauses and `JOIN` predicates to produce mathematically-correct comparisons, including for signed values with mixed-sign inputs.

**Aggregation (US2)**

- **FR-003**: System MUST support `SUM`, `MIN`, `MAX`, `AVG`, and `COUNT` aggregate functions on wide-integer columns in in-pipeline SQL transforms.
- **FR-004**: System MUST produce aggregate results that match what an equivalent Postgres query returns on the same input data.
- **FR-005**: System MUST support `GROUP BY` clauses that include or aggregate wide-integer columns.
- **FR-006**: `SUM` of inputs whose mathematical sum exceeds the input column's representable range MUST surface the full-precision result without overflow or wrap.

**Casts and string conversion (US3)**

- **FR-007**: System MUST allow `CAST(wide_int_col AS TEXT)`, `CAST(wide_int_col AS VARCHAR)`, `CAST(wide_int_col AS STRING)`, `CAST(wide_int_col AS CHAR)`, and `CAST(wide_int_col AS CHAR(N))` — in any letter case — and produce canonical decimal text output.
- **FR-008**: A pipeline author MUST NOT need to invoke any wide-integer-specific UDF (e.g. `u256_to_string`) to convert a wide-integer column to text; the standard SQL `CAST` syntax MUST suffice.

**Wire-format compatibility (US4)**

- **FR-009**: System MUST read ClickHouse `UInt256` source columns and represent their values in the pipeline without precision loss.
- **FR-010**: System MUST read ClickHouse `Int256` source columns and represent their values in the pipeline without precision loss, including for negative values.
- **FR-011**: System MUST write to ClickHouse `UInt256` sink columns when the originating data is unsigned wide-integer.
- **FR-012**: System MUST write to ClickHouse `Int256` sink columns when the originating data is signed wide-integer.
- **FR-013**: System MUST preserve the unsigned/signed distinction across the full source-to-sink path — a value that originated as an unsigned wide integer MUST NOT be silently re-typed as signed (or vice versa) when crossing a transform.
- **FR-014**: System MUST allow pipelines built against the prior wide-integer types to continue running unchanged — the same YAML, the same source schemas, the same sink table definitions.

**Round-trip integrity (cross-cutting)**

- **FR-015**: A wide-integer value MUST round-trip losslessly when ingested from one wire format (Kafka Avro `decimal`, Postgres `NUMERIC`, ClickHouse `UInt256`/`Int256`) and emitted to any other wire format that natively supports its range; pipelines whose sinks lack native support MUST behave per the existing connector capability matrix (e.g. opt-in `coerce_to: string` for ClickHouse `Decimal` precision overflow on a fractional column).

**Documentation and surface (US5)**

- **FR-016**: System documentation MUST present a single wide-integer story; references to `u256` and `i256` MUST be limited to a clearly-labeled "migration / wire-format adapter" section that explains where ClickHouse's native types still appear.

**Migration safety (cross-cutting)**

- **FR-017**: A pipeline restarted from a state checkpoint that pre-dates this migration MUST resume correctly without operator action. Since checkpoints carry source-side offsets only (no schema), and the source wire formats are unchanged, the post-migration code routes the decoded records through `decimal_arb` and the sink emits the same bytes as before. Rollback to the pre-migration streamling against a post-migration checkpoint MUST also resume correctly.
- **FR-018**: When a YAML pipeline explicitly references a removed type name in a configuration field (e.g. a schema override), the system MUST reject the pipeline at config load with an error naming the offending field and the replacement type.

### Key Entities

- **Wide integer (conceptual)**: an integer value whose magnitude exceeds the standard 128-bit `Decimal128` range. Today represented internally by three distinct types; after this feature, represented by a single type. The internal representation is not visible to pipeline authors — they see source/sink wire formats and SQL.
- **Wire-format channel**: the encoding a specific connector uses to carry a wide-integer column. Examples: Avro `decimal(p, s)` logical type, Postgres `NUMERIC(p, s)`, ClickHouse `UInt256` / `Int256` / `Decimal(p, s)` / `String`. The connector translates between its native channel and the single internal wide-integer representation.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Sorting a 1000-row column of signed wide integers containing at least 100 negative values and at least 100 positive values produces output in numeric ascending order, matching a Python reference implementation row-for-row.
- **SC-002**: A `WHERE col < 0` filter on a signed wide-integer column returns exactly the rows whose values are numerically less than zero — verified against a Postgres reference on the same input data.
- **SC-003**: `SUM`, `MIN`, `MAX`, `AVG`, and `COUNT` aggregates over a wide-integer column produce results identical to the same SQL run in Postgres on the same input data, with no precision loss and no overflow on `SUM` of full-width values.
- **SC-004**: A `GROUP BY` query aggregating a wide-integer column produces per-group results that match the equivalent Postgres aggregation.
- **SC-005**: `CAST(col AS TEXT)`, `CAST(col AS VARCHAR)`, `CAST(col AS STRING)`, and `CAST(col AS CHAR[(N)])` against a wide-integer column all produce string output equal to the canonical decimal representation of the value.
- **SC-006**: The canonical CAST-AS-TEXT YAML pipeline (a `SELECT * EXCEPT col, CAST(col AS TEXT) AS col FROM traces` shape against a source whose `col` carries values that previously routed to u256) runs end-to-end without error and produces correct output.
- **SC-007**: An existing pipeline whose source and sink both target ClickHouse `UInt256` / `Int256` columns runs against the migrated streamling without any YAML or ClickHouse schema changes, and round-trips wide-integer values byte-exact when the upstream source is hinted (Avro `decimal(78, 0)` / Postgres `NUMERIC(78, 0)`). **Caveat**: ClickHouse-source-to-ClickHouse-sink round-trip is not natively covered by this feature — source-side hint stamping for ClickHouse `UInt256` / `Int256` source columns is deferred to a follow-up (see `tasks.md` T009 and `contracts/clickhouse-wide-int.md` "Source side deferred"). This was pre-existing behavior, not a regression. Workaround documented in `docs/decimal-arbitrary-precision.md` "Known limitations".
- **SC-008**: After the migration is complete, the codebase contains zero references to the retired wide-integer type identifiers outside the connector-side wire-format adapter modules. The retired type's source files are removed from the tree.
- **SC-009**: A representative streamling pipeline (Kafka source → SQL transform → ClickHouse sink) processing wide-integer columns at 100,000 rows runs within ±20% of the pre-migration baseline throughput on the same hardware. **Status: unverified in this slice.** Benchmark task `T052` is deferred — verification requires a comparable pre-migration baseline run which was out of scope. The assumption (Assumptions §"BigDecimal-based arithmetic ... is performant enough") remains untested at the headline-pipeline level. The decimal_arb code path is the same as feature 001 (which has been in production); the change here is the routing flip, not the arithmetic kernel.
- **SC-010**: The pipeline author documentation describes a single wide-integer story, and a new author can construct a working wide-integer pipeline (Avro source → Postgres sink) by reading only that single section of documentation.

## Assumptions

- The `decimal_arb` type from feature 001 is the architectural baseline. Its existing arithmetic, comparison, aggregation, sort-key, and CAST surface (plus the connector capability matrix) is the proven template; this feature flips source-side routing to use it and removes the now-redundant infrastructure.
- ClickHouse versions in use across operator deployments support native `UInt256` and `Int256` column types (these have been GA in ClickHouse since 20.x; all currently-supported versions have them).
- No external user of streamling's library API directly imports the to-be-removed wide-integer type identifiers. The only consumers are this codebase and its tests.
- Pipeline-state checkpoints record source-side offsets only and carry no Arrow schema. On restart under the post-migration streamling, the pipeline resumes from the stored offset, the source decodes records per its unchanged wire schema, and the new code routes them through `decimal_arb`. Upgrade and rollback are both clean with no operator action.
- BigDecimal-based arithmetic (decimal_arb's underlying math) is performant enough for typical streamling pipeline throughput. The headline blockchain-data workloads are I/O- and serialization-bound, so per-row math is not the bottleneck. If a real production pipeline shows measurable slowdown after migration, a hot-path optimization (e.g. a fixed-width fast path for `decimal_arb(78, 0)` arithmetic) can be added later without re-introducing the dedicated wide-integer types.
- The bigint SQL preprocessor's binary-op rewrite machinery is exclusively used by the retired wide-integer paths; its `CAST(x AS DECIMAL(p > 76, s))` → `decimal_arb` routing (introduced in feature 001) is the part that stays.
- The connector capability matrix's existing behavior for `decimal_arb` columns is unchanged. The only addition is two ClickHouse-side wire-format cases (`UInt256` and `Int256` source/sink translation), both of which preserve existing column-storage compactness in user-managed ClickHouse tables.
