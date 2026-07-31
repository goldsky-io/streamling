# Feature Specification: Arbitrary-Precision Decimal Type

**Feature Branch**: `001-decimal-arbitrary-precision`
**Created**: 2026-04-29
**Status**: Draft
**Input**: User description: "Create a specification for a new custom datafusion type that can have higher precision than Decimal256 in datafusion. Say Decimal(100,18). Precision and Scale are arbitrary. New type needs to support same operations as Decimal256 (e.g., arithmetic, comparison, sorting, ...)."

## Clarifications

### Session 2026-04-29

- Q: How does v1 select the new arbitrary-precision type for a column? → A: Auto-promote based on declared precision: any column whose declared precision exceeds the existing fixed-width ceiling (whether the declaration comes from source metadata or YAML) automatically uses the new type; columns at or below the ceiling continue to use the existing fixed-width types unchanged.
- Q: When a sink's destination cannot natively hold the new type (e.g., ClickHouse `Decimal` capped at 76 digits), what happens? → A: The pipeline is rejected at configuration load with a clear, actionable error naming the column and destination. The pre-existing silent fallback (rewriting the column to `String` with a WARN log) is retired and only available as an explicit per-column opt-in (e.g., `coerce_to: string`).
- Q: Which connectors must support the new type for v1? → A: Every source and sink connector that today handles fixed-width decimals must accept the new type, evaluated per-connector at configuration load: a connector accepts the column if its underlying store or wire encoding can carry the declared precision/scale losslessly (e.g., Postgres `NUMERIC`, JSON digit-strings, Avro `decimal` whose declared fixed-width fits, plugin connectors that advertise the type). Any (column, connector) pair that cannot be carried losslessly is rejected at configuration load per FR-011 unless the FR-019 string-coercion opt-in is set. No connector is silently excluded; none silently downgrades.
- Q: Do native SQL operators (`+`, `−`, `×`, `÷`, `=`, `<`, `ORDER BY`, `GROUP BY`, `SUM`, `AVG`, `MIN`, `MAX`) work on the new type without explicit function calls? → A: Yes — native SQL operators and standard aggregate functions work on the new type directly, including in mixed-operand expressions with existing numeric types, with the same surface authors expect from the existing fixed-width decimal types. Function-call equivalents may also exist but are not required by the author. SC-006 ("no transform rewrites are required") is binding.
- Q: Is the "no measurable performance change for existing pipelines" criterion (SC-003) enforced by a benchmark or aspirational? → A: Aspirational. SC-003 stays in the spec as guidance for reviewers; no benchmark blocks merge in v1. Reviewers eyeball it on representative pipelines. A formal regression budget can be added in a later iteration if drift is observed.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Lossless ingestion of high-precision numeric columns (Priority: P1)

A pipeline author configures a source that reads from a relational system whose numeric columns carry more significant digits than the existing 76-digit decimal ceiling permits (for example, a Postgres `NUMERIC(100, 18)` column holding very large token balances or accumulated counters). Today, those values either fail to load, get rejected by the type system, or silently truncate/overflow when forced into the largest existing decimal. With the new type, the values pass through the pipeline byte-for-byte, with the precision and scale advertised on the source preserved end-to-end.

**Why this priority**: This is the entry point for every downstream use of the value. If the value cannot be ingested losslessly, no transform or sink can recover it. Until ingestion works, the feature delivers no value.

**Independent Test**: A pipeline that reads a column of `NUMERIC(100, 18)` from a source connector and writes it directly to a sink (or inspects the resulting Arrow batches) demonstrates the value is preserved at the documented precision/scale, and any value that fits within the declared precision/scale is accepted without error.

**Acceptance Scenarios**:

1. **Given** a source column declared as a high-precision decimal with precision greater than 76, **When** a row is read whose magnitude requires more than 76 digits, **Then** the value lands in the pipeline at the declared precision and scale with no loss.
2. **Given** the same column, **When** the value would have fit into the existing 76-digit decimal, **Then** it still loads correctly and reports the user-declared precision/scale.
3. **Given** a value that exceeds the column's declared precision, **When** the source attempts to load it, **Then** the pipeline raises a clear, actionable error identifying the offending value and column.

---

### User Story 2 - Arithmetic, comparison, and sorting in transforms (Priority: P1)

A pipeline author writes a SQL transform that adds, subtracts, multiplies, divides, compares, sorts, groups, and aggregates columns of the new type — alone or alongside other numeric types — and gets answers consistent with normal decimal arithmetic at the requested precision and scale.

**Why this priority**: Ingesting values is only useful if the transform layer can operate on them. Arithmetic, comparison, and sorting are the table-stakes operations a SQL author expects; missing any of them blocks real pipelines.

**Independent Test**: A SQL transform exercises each operation against the new type in isolation and against mixed operands, and the resulting output matches a reference computation done with an external arbitrary-precision tool.

**Acceptance Scenarios**:

1. **Given** two columns of the new type, **When** a transform computes `a + b`, `a - b`, `a * b`, `a / b`, and `a % b`, **Then** each result has a precision and scale consistent with standard decimal arithmetic rules and the values match the equivalent computation done outside the pipeline.
2. **Given** two columns of the new type, **When** a transform compares them with `=`, `!=`, `<`, `<=`, `>`, `>=`, **Then** the boolean results agree with numeric ordering of the underlying values.
3. **Given** a column of the new type, **When** a transform applies `ORDER BY`, **Then** rows come out in numeric order with NULLs placed consistently with the rest of the engine's NULL ordering.
4. **Given** a column of the new type, **When** a transform applies `SUM`, `MIN`, `MAX`, `AVG`, and `COUNT`, **Then** each aggregate returns a result at appropriate precision/scale and matches a reference computation.
5. **Given** a column of the new type used in `GROUP BY` or `JOIN` keys, **When** the transform runs, **Then** rows with numerically equal values group/join together regardless of how they were textually written.
6. **Given** a column of the new type and a column of an existing decimal or integer type, **When** they appear together in an arithmetic or comparison expression, **Then** the operation succeeds and the result is at least as precise as the wider operand, with no silent loss of significant digits.

---

### User Story 3 - Lossless emission to high-precision sinks (Priority: P1)

A pipeline author writes a sink that targets a destination capable of storing arbitrary-precision decimals (for example, Postgres `NUMERIC`). Values emerging from a transform of the new type are written to the destination at the declared precision and scale without truncation, rounding, or overflow.

**Why this priority**: Without the matching sink path, the new type is trapped inside the pipeline. The end-to-end ingest → transform → emit loop is what makes the feature usable.

**Independent Test**: A pipeline reads a high-precision value from a source, passes it through an identity transform, writes it to a sink that supports arbitrary precision, and a query against the destination returns the exact original value.

**Acceptance Scenarios**:

1. **Given** a value of the new type, **When** the pipeline writes it to a sink whose target column supports the same or greater precision/scale, **Then** the value lands with no loss.
2. **Given** a value that exceeds the destination column's declared precision/scale, **When** the sink attempts to write it, **Then** the pipeline raises a clear, actionable error identifying the offending value, column, and destination.
3. **Given** a sink targeting a destination that only supports a smaller decimal width, **When** the pipeline is configured to emit the new type to that destination, **Then** the configuration step surfaces the mismatch up front (rather than silently rounding at runtime).

---

### User Story 4 - Casts to and from existing numeric types (Priority: P2)

A pipeline author casts between the new type and the engine's existing numeric types (smaller decimals, integers, floats, strings) using the engine's standard cast mechanism, with predictable rules for when a cast succeeds, when it fails, and when it loses precision.

**Why this priority**: Real pipelines mix legacy and new columns. Without explicit cast semantics, authors cannot bridge the two safely. This priority sits below P1 because authors can work around it temporarily by keeping data in the new type, but it is required for production adoption.

**Independent Test**: For each supported source type and each supported target type, a cast expression behaves according to the documented rule (succeed, succeed-with-rounding, or fail).

**Acceptance Scenarios**:

1. **Given** a value in an existing smaller decimal type, **When** it is cast to the new type, **Then** the cast always succeeds and the numeric value is preserved exactly.
2. **Given** a value of the new type that fits within a smaller decimal's precision and scale, **When** it is cast to that smaller type, **Then** the cast succeeds and the value is preserved exactly.
3. **Given** a value of the new type that does not fit within a smaller target's precision and scale, **When** the cast is requested, **Then** the cast either fails with a clear error or rounds according to a documented rule (consistent with how the engine treats analogous narrowing decimal casts today).
4. **Given** a string in canonical decimal notation, **When** it is cast to the new type, **Then** the cast succeeds for any string within the declared precision/scale and fails with a clear error otherwise.
5. **Given** a value of the new type, **When** it is cast to a string, **Then** the result is a canonical decimal representation that round-trips back to the same value.

---

### Edge Cases

- **Zero, negative zero, and signed boundaries**: The type distinguishes positive and negative values correctly across all operations and treats `+0` and `-0` as numerically equal.
- **NULL handling**: NULL values in the new type behave the same as NULLs in existing decimal types for arithmetic, comparison, sorting, grouping, and aggregation.
- **Overflow on arithmetic**: When an arithmetic operation would produce a value exceeding the documented result precision, the engine surfaces a clear error rather than silently wrapping or truncating.
- **Division by zero**: Dividing by zero behaves the same as for existing decimal types (an error or NULL, consistent with current engine behavior).
- **Division and rounding**: Division (and other operations that may produce a result with more fractional digits than the result scale allows) follows a documented rounding rule, applied consistently across the pipeline.
- **Mixed precision/scale within a single column**: A column declares one precision and one scale; values that arrive at runtime with a different scale are normalized to the declared scale (with the same overflow rule above) before further processing.
- **Very large precision (thousands of digits)**: The pipeline does not silently degrade or crash on extremely large precision values, though performance characteristics for such values are documented rather than guaranteed.
- **Empty or all-NULL groups in aggregates**: `SUM`, `AVG`, etc., over empty or all-NULL groups follow the same rules as for existing decimal types.
- **Configuration-time validation**: A pipeline whose declared schemas at the source, transforms, and sink are mutually inconsistent (e.g., sink narrower than source) is rejected at configuration load, not on the first row.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST provide a numeric type whose precision (total significant digits) and scale (digits after the decimal point) are user-declared at column-definition time and are not bounded by the existing 76-digit decimal ceiling.
- **FR-002**: The system MUST allow precision and scale on this type to be arbitrary positive integers, with scale less than or equal to precision, subject to a documented practical upper bound (see Assumptions).
- **FR-003**: The system MUST support all of the following operations on the type via the standard SQL operator syntax (`+`, `-`, `*`, `/`, `%`, unary `-`) — not only via named function calls — with results that match standard decimal semantics: addition, subtraction, multiplication, division, modulo, unary negation, absolute value.
- **FR-004**: The system MUST support all of the following relational operations on the type via the standard SQL operator syntax (`=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`): equality, inequality, less-than, less-than-or-equal, greater-than, greater-than-or-equal.
- **FR-005**: The system MUST support deterministic ordering (ascending and descending) of the type, including in `ORDER BY`, window functions, and merge/sort operators internal to the engine, without requiring an explicit cast or function-call wrapper at the call site.
- **FR-006**: The system MUST support the type as a `GROUP BY` key and as a join key, with rows whose values are numerically equal grouping/joining together, without requiring an explicit cast or function-call wrapper at the call site.
- **FR-007**: The system MUST support the type as the input and output of the standard SQL aggregate functions (`SUM`, `MIN`, `MAX`, `AVG`, `COUNT`) invoked by their standard names, with `SUM` and `AVG` producing results at a precision/scale at least as wide as required to avoid silent overflow on the input.
- **FR-020**: The system MUST integrate the new type into the engine's expression-planning and type-coercion machinery so that an author writing transforms in standard SQL — using native operators, comparisons, sorting, grouping, and the standard aggregate names — gets the same syntactic experience they have today with the existing fixed-width decimal types. Named function-call equivalents (e.g., `<type>_add`) MAY exist for parity with related extension types, but the spec's acceptance scenarios are written against the native-syntax surface and that surface is binding.
- **FR-008**: The system MUST support NULL values in the type with the same semantics as NULLs in existing decimal types across all operations above.
- **FR-009**: The system MUST allow casts between this type and other supported numeric types (smaller decimals, integers, floats) and strings, with cast outcomes (success, rounding, error) following documented rules consistent with existing decimal cast semantics.
- **FR-010**: Every source connector that today handles fixed-width decimals MUST accept the new type. Acceptance is evaluated per-(column, connector) at configuration load: the connector accepts the column if its underlying store or wire encoding can carry the declared precision and scale losslessly. A column whose declared precision exceeds what the connector's encoding can carry MUST be rejected at configuration load per FR-012 (no silent downgrade, no per-row failure).
- **FR-011**: Every sink connector that today handles fixed-width decimals MUST accept the new type, under the same per-(column, connector) acceptance rule as FR-010. For sinks whose destination natively supports the declared precision, values flow through losslessly. For sinks whose destination cannot hold the declared precision, the pipeline MUST be rejected at configuration load per FR-012 unless the FR-019 string-coercion opt-in is set. Runtime silent fallback to a non-numeric encoding is not permitted.
- **FR-012**: The system MUST detect mismatches between the precision/scale declared on a column at the source, in transforms, and at the sink during configuration load and reject the pipeline with a clear error before any rows flow. This includes sinks whose destination type cannot hold the source-declared precision.
- **FR-013**: The system MUST surface arithmetic overflow, narrowing-cast overflow, and value-exceeds-declared-precision errors as actionable errors at runtime that name the offending column and value, for the cases that cannot be detected at configuration load (i.e., per-row violations of an otherwise-valid declared schema).
- **FR-019**: The system MAY offer an explicit per-column opt-in (e.g., a `coerce_to: string` directive on the sink) that allows a high-precision column to be emitted to a destination that cannot natively hold it, encoded as a string. When this opt-in is not set, FR-011 applies and the pipeline is rejected at configuration load. The opt-in MUST require an explicit declaration; it MUST NOT be inferred from the destination's capabilities.
- **FR-014**: The system MUST apply a documented, consistent rounding rule when an operation produces more fractional digits than the result scale permits.
- **FR-015**: The system MUST coexist with existing decimal types: any column whose declared precision is at or below the existing fixed-width decimal ceiling MUST continue to use the existing fixed-width types and behave exactly as it does today. The new type is selected automatically — without any YAML keyword or per-pipeline flag — whenever a column's declared precision exceeds that ceiling, regardless of whether the declaration comes from source metadata (e.g., a Postgres `NUMERIC(100, 18)` column) or YAML.
- **FR-018**: The system MUST treat the existing incorrect mapping of high-precision source columns to a too-narrow fixed-width type (for example, a Postgres `NUMERIC(100, 18)` column being mapped to a 38-digit fixed-width decimal) as a defect that the auto-promotion rule in FR-015 corrects. After this feature ships, no source-declared column with precision exceeding the fixed-width ceiling shall be silently mapped to a fixed-width type.
- **FR-016**: The system MUST allow the new type to participate in expressions alongside existing numeric types, with the result type at least as wide as required to preserve every significant digit of either operand.
- **FR-017**: The system MUST format values of the type as canonical decimal strings on output (logs, error messages, string casts) such that the formatted form parses back to the same value.

### Key Entities *(include if feature involves data)*

- **High-precision decimal column**: A column whose schema declares a precision and a scale exceeding the existing decimal ceiling. Carries the same metadata (name, nullability, declared precision/scale) as existing decimal columns and travels through every layer of the pipeline.
- **High-precision decimal value**: A single numeric value drawn from a high-precision decimal column, with an exact magnitude up to the declared precision and an exact fractional component up to the declared scale. Distinct from the column-level metadata: many values share one column declaration.
- **Decimal type catalog**: The conceptual set of decimal types the engine knows how to work with. Today it has fixed-width entries; the feature adds an entry for the new type. Authors discover and reference this set when writing schemas and casts.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A pipeline ingesting a high-precision decimal column from a source that natively supports arbitrary-precision numerics, applying an identity transform, and writing to a sink that supports arbitrary-precision numerics produces an output that is bit-for-bit equal to the input, on 100% of rows in a representative test set spanning small, large, negative, near-zero, and edge-of-precision values.
- **SC-002**: For every supported arithmetic, comparison, sorting, and aggregation operation on the new type, the pipeline produces results that match an external reference computation (such as a standard arbitrary-precision math library) on 100% of rows in a representative test set.
- **SC-003**: Pipelines that use only existing decimal types — and not the new type — should exhibit no perceptible change in throughput, latency, or memory use after the feature ships, within normal run-to-run variance. This is aspirational guidance for reviewers, not a benchmark-gated requirement; if observable regressions appear on representative workloads, a formal regression budget will be introduced in a follow-up.
- **SC-004**: A misconfigured pipeline whose source, transform, and sink schemas declare incompatible precision/scale for the same logical column is rejected at startup with an error message that names the column, the conflicting declarations, and the place to change them — rather than failing on the first row at runtime.
- **SC-005**: Documentation enumerates the supported operations, cast rules, rounding rule, error conditions, and connector coverage for the new type, and a pipeline author who has used the existing decimal types can adopt the new type without consulting source code.
- **SC-006**: An author can convert an existing pipeline that was silently truncating or rejecting high-precision values into one that handles them losslessly by changing only the declared column type — no transform rewrites are required for arithmetic, comparison, sorting, or aggregation steps that already worked on the old type.

## Assumptions

- The existing 76-digit decimal ceiling refers to the engine's current widest fixed-width decimal type. Any column whose declared precision is at or below that ceiling is expected to continue using the existing fixed-width type for performance reasons; the new type is selected when the declared precision exceeds it.
- "Arbitrary" precision and scale means user-declared without a fixed-width-driven ceiling, but in practice a generous upper bound (well above realistic schema declarations — for example, several thousand digits) is acceptable as a sanity guard. The exact bound is a tuning detail and is documented rather than a hard product requirement.
- The default rounding rule for results that exceed the declared scale is "half-to-even" (banker's rounding), consistent with the rule used by mainstream high-precision numeric implementations. This is documented and can be revisited later without changing the surface area of the feature.
- The initial connector coverage targets the set of source/sink connectors that today expose fixed-width decimals and whose underlying stores natively support arbitrary-precision numerics (e.g., a relational store with `NUMERIC` of unbounded precision). Connectors whose underlying stores cap precision (for example, fixed-width-only formats) are out of scope for this feature and rely on the existing narrower types.
- NULL ordering, error semantics, and cast semantics for the new type follow the engine's existing conventions for decimal types; the feature does not redefine engine-wide rules.
- The feature is purely additive at the type-system surface: existing schemas, transforms, sinks, and connectors that do not reference the new type continue to behave identically.
- Performance characteristics (throughput, memory, latency) for very large precision values may be materially worse than for fixed-width decimals; this is an inherent property of arbitrary precision arithmetic and is documented rather than treated as a regression.
- Pipeline authors are responsible for declaring matching precision/scale across source, transforms, and sink. The engine validates consistency at startup but does not infer or auto-widen across boundaries.
