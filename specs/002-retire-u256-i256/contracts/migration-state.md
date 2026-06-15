# Contract: Migration & State Compatibility

This contract specifies how a pipeline restarted under the post-migration streamling handles state checkpoints, configuration, and tests that pre-date the change.

## In-flight pipeline checkpoints

Streamling pipeline checkpoints record **source-side offsets only** — they do not carry the Arrow schema of in-flight data. A pipeline restarted under the post-migration streamling against a pre-migration checkpoint will:

1. Read the stored offset from the state backend.
2. Resume consuming from the source at that offset.
3. Decode each record per the source's wire schema (Avro `decimal(p, 0)` bytes, Postgres `NUMERIC` text, ClickHouse `UInt256` LE bytes — none of which changed).
4. Route the decoded record through the new code path (`decimal_arb` extension type with `native_int_kind` hint instead of `u256` / `i256`).
5. Emit to the sink in the same wire format as before.

No schema mismatch can arise from a checkpoint because no schema is stored in one. **No operator action is required to upgrade.**

This holds regardless of where the pipeline was in its processing cycle when stopped: mid-batch, between checkpoints, immediately after a flush — the resume path is identical.

### Rollback

Symmetric to the upgrade. A post-002 streamling stops, gets replaced with a pre-002 streamling, and the pre-002 streamling resumes from the same checkpoint. Since the checkpoint only carries offsets and the wire formats haven't changed, the downgrade is clean. No state migration in either direction.

## YAML pipeline configuration

No YAML grammar today exposes the `u256` / `i256` type identifiers. Source columns are typed via:

- Avro source: the registered Avro schema's `decimal(p, s)` logical type → automatic routing
- Postgres source: introspected from `information_schema` → automatic routing
- ClickHouse source: introspected from `system.columns` → automatic routing

So a YAML pipeline does not need to be edited to migrate from the old types to the new. Existing YAML files continue to work.

### One exception: `schema_override` in ClickHouse sinks

The ClickHouse sink's `schema_override` map (`HashMap<column_name, clickhouse_type>`) lets a pipeline author override the CREATE TABLE column type. If a user has manually configured a `schema_override` value like `"UInt256"` or `"Int256"`, it continues to work — these are ClickHouse type strings, not streamling-internal type identifiers.

If a user had configured a `schema_override` referencing a streamling-internal name (extremely unlikely), the pipeline must reject at config load with a clear "unknown ClickHouse type" error. The existing `clickhouse_column_type` rejection path handles this.

## Code references to the old types

After the routing flip, no code in streamling-common, streamling-core, or streamling-connectors will produce a field with `U256Type::metadata()` or `I256Type::metadata()`. The legacy type identifier symbols are then dead code and the final task in the implementation plan deletes them.

A few non-code references survive briefly as commit/PR history; nothing on the user-visible surface mentions the old types post-migration except the upgrade-notes section of the docs.

## Test compatibility

The existing test suite covers u256/i256 in several layers:

| Layer | Test surface | Migration |
|---|---|---|
| `streamling-common::types::u256::tests` | Type identity, metadata helpers | Delete with `u256.rs` |
| `streamling-common::types::i256::tests` | Type identity, metadata helpers | Delete with `i256.rs` |
| `streamling-common::functions::u256_ops::tests` | UDF correctness | Delete with `u256_ops.rs` |
| `streamling-common::functions::i256_ops::tests` | UDF correctness | Delete with `i256_ops.rs` |
| `streamling-core::types::bigint_sql_preprocessor::tests` | SQL string rewrites for binary ops | Audit and either delete (preprocessor path gone) or migrate to ExprPlanner-driven decimal_arb equivalents in `decimal_arb_coercion::tests` |
| `streamling-connectors::table_providers::postgres::*::tests` | Postgres NUMERIC(78,0) mapping | Migrate test fixtures to use `decimal_arb` field constructors |
| `streamling-connectors::table_providers::clickhouse::tests` | UInt256/Int256 type mapping | Migrate test fixtures; add `native_int_kind` hint coverage |
| `streamling-e2e::tests::decimal_arb_*` (feature 001) | End-to-end decimal_arb pipelines | Unchanged; existing tests are the regression baseline |

Specific tests that today pin behavior we're explicitly preserving (the `CAST AS TEXT` regression, ClickHouse `UInt256` round-trip) get re-added against the new code path as their final-task verification.

---

This contract is informational — it imposes no new API surface, only describes the migration behavior the implementation must produce given the changes specified in the other contracts.
