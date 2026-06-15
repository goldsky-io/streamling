# Contract: Avro Wide-Integer Routing

This contract specifies how the Kafka/Avro source decodes `decimal` logical-type fields whose precision exceeds the `Decimal256` ceiling (76 digits) and how the corresponding sink emits them.

## Source side (Avro → decimal_arb)

### Schema mapping

In `crates/streamling-common/src/formats/avro/schema.rs`, the existing `convert_avro_schema_to_arrow` walker has a `decimal` logical-type arm. Today:

```text
(p, s) where p > 76 and s > 0 → decimal_arb(p, s)
(p, 0) where p > 76          → U256Type (FixedSizeBinary(32))
(p, s) where p <= 76          → Decimal128 or Decimal256
```

After this feature:

```text
(p, s) where p > 76 and s > 0 → decimal_arb(p, s)  (unchanged, no hint)
(p, 0) where 77 <= p <= 78    → decimal_arb(p, 0)  with native_int_kind = u256
(p, 0) where p > 78           → decimal_arb(p, 0)  (no hint — historic U256
                                                    routing silently overflowed
                                                    at p > 78; new path is
                                                    lossless via Decimal(p, 0))
(p, s) where p <= 76          → Decimal128/256     (unchanged)
```

The `u256` hint preserves the pre-feature-002 routing (all `decimal(p > 76, 0)` mapped to `U256Type`). There is no native Avro convention for signed-vs-unsigned wide decimals, so the source path does not infer signedness; pipelines that need a signed `Int256` round-trip must use the ClickHouse sink's `schema_override` directive or wait for a future YAML opt-in. No new Avro schema syntax is introduced.

### Value decoding

The existing `formats/avro/arrow_array_reader.rs` decimal-decode path already produces canonical decimal_arb bytes from Avro `decimal` logical-type values (feature 001 work). The only change: the U256/I256-specific decode arms are deleted; every `decimal(p, 0)` with `p > 76` flows through the single decimal_arb decode arm, with the `native_int_kind` hint set on the field metadata at schema-build time.

## Sink side (decimal_arb → Avro)

Feature 001 already wired Avro sink emission for decimal_arb columns. The `native_int_kind` hint has no effect on Avro emission — Avro's `decimal` logical type is always mathematically signed, so any `decimal_arb` (with or without a hint) emits identically as `decimal(p, s)`.

The only change here is **deletion**: the u256/i256-specific Avro write arms in `formats/avro/writer.rs` are removed; all wide-integer Avro writes go through the existing decimal_arb path.

## Invariants

1. **Existing pipelines unchanged**: a YAML pipeline today that registers an Avro schema with `decimal(78, 0)` and produces records continues to work. The streamling-side Arrow type changes from FSB(32)+U256 to LargeBinary+decimal_arb+hint, but the on-wire Avro bytes and the user-observable behavior do not change.
2. **Symmetric encode/decode**: a value encoded by streamling as Avro `decimal(78, 0)` and re-decoded by the same code path is byte-identical to the original `decimal_arb` value.

## Out of scope

- Avro `bytes` (non-logical-type) fields — unaffected.
- Schema-registry compatibility checks — Avro `decimal(78, 0)` schema bytes are unchanged, so registry compatibility is preserved.
