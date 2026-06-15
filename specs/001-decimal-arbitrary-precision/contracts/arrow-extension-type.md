# Contract: Arrow Extension Type

**Spec**: [../spec.md](../spec.md) FR-001, FR-002, FR-008, FR-017 — **Plan**: [../plan.md](../plan.md) — **Data model**: [../data-model.md](../data-model.md) (E1, E3)

This contract defines the on-the-wire and in-memory shape of the `streamling.decimal_arb` Arrow extension type. Anything serializing or deserializing Arrow data (IPC, JSON, Avro, plugin FFI, downstream consumers) reads from this contract.

## 1. Extension identity

| Property | Value |
|---|---|
| Extension name | `streamling.decimal_arb` |
| Arrow metadata key (extension name) | `ARROW:extension:name` |
| Arrow metadata key (extension metadata) | `ARROW:extension:metadata` |
| Storage type | `DataType::LargeBinary` (resolved by T006 spike — `BinaryView` would be auto-expanded at output by the existing `expand_views_at_output` session config, so `LargeBinary` directly is simpler and cheaper) |

## 2. Field metadata schema

`ARROW:extension:metadata` is a JSON-encoded string with this exact shape:

```json
{ "precision": <u32>, "scale": <u32> }
```

Constraints:
- `precision`: `u32`, `1 <= precision <= 65535` (sanity guard `MAX_PRECISION = 65535`).
- `scale`: `u32`, `0 <= scale <= precision`.
- `precision > 76` is **expected**; producers MUST NOT use this extension for `precision <= 76` (those columns belong on `Decimal256`).

Producers that emit a `Field` with this extension MUST include both keys in metadata. Consumers MUST treat a missing key as a malformed schema (validation error at config load).

## 3. Per-value byte layout

Each non-null cell of the array stores a variable-length payload:

```
+----------+-------------------+
| sign (1) | magnitude (0..N)  |
+----------+-------------------+
```

| Field | Width | Encoding |
|---|---|---|
| `sign` | 1 byte | `0x00` for non-negative; `0xFF` for negative |
| `magnitude` | 0..⌈precision · log₂10 / 8⌉ bytes | big-endian two's-complement magnitude bytes; for negative values the bytes are the two's-complement representation of the absolute value, **not** the negative value (the sign byte carries sign separately) |

Canonicalization (REQUIRED — builders MUST canonicalize before storing):
- Magnitude bytes have no leading `0x00` byte (minimal encoding).
- For value zero, magnitude is empty and sign is `0x00`. There is no `-0`.
- The total payload for a non-zero value has `1 + N` bytes where `N >= 1`.

## 4. NULL representation

Standard Arrow validity bitmap. A null cell has the bit cleared and zero-length payload (or any payload — consumers MUST consult the validity bitmap, not the payload bytes).

## 5. Equality and hash

Two cells are equal iff their canonical `(sign, magnitude)` payloads are byte-equal AND both validity bits are set (or both cleared). Because canonicalization is required, byte-equality and value-equality coincide.

Hash is computed over the canonical payload bytes.

This contract is what makes `GROUP BY` and `JOIN` on the type correct (FR-006).

## 6. Sort

Bytewise sort over the canonical payload is **NOT** correct for negative values (sign byte `0xFF` sorts after `0x00`). DataFusion sort paths MUST use the custom row converter introduced in `streamling-common/src/types/decimal_arb.rs` rather than relying on default `BinaryView` sort. The custom encoding bit-flips the entire payload for negatives so bytewise comparison reproduces numeric order.

## 7. IPC compatibility

The type round-trips through Arrow IPC because:
- `BinaryView` is a standard Arrow type (≥55).
- Field metadata is preserved by the IPC format.

A consumer reading IPC data without knowledge of the extension type sees a `BinaryView` field with metadata it doesn't recognize — values are still readable as opaque bytes (canonical format documented above), so a downstream consumer can decode them with this contract alone.

## 8. JSON encoding (Kafka JSON, webhook, SQS)

When emitted to JSON:
- Non-null value → JSON **string** containing the canonical decimal representation (e.g., `"123.45"`, `"-0.0001"`, `"0"`).
- Null value → JSON `null`.

When parsed from JSON, the same canonical decimal grammar applies; values that parse but exceed `(precision, scale)` raise FR-013 errors at row time.

JSON consumers SHOULD treat the field as a **string** to avoid IEEE 754 precision loss in their JSON parser. This is a documentation point, not a wire-format guarantee.

## 9. Avro `decimal` logical type compatibility

Avro `decimal` (logical type over `bytes` or `fixed`) maps as follows:
- Avro `decimal(p, s)` with `p > 76` → `streamling.decimal_arb` with `precision = p`, `scale = s`.
- Avro magnitude bytes are big-endian two's-complement (already matches our magnitude encoding); the sign is extracted from the MSB of the Avro bytes and prepended as our sign byte.
- Avro `decimal(p, s)` with `p <= 76` continues to map to `Decimal128`/`Decimal256` (FR-015).

## 10. Postgres `NUMERIC` compatibility

The text protocol is the v1 path (research R6). On read, `NUMERIC` text decimal is parsed by `DecimalArbValue::from_str`. On write, `DecimalArbValue::to_string` produces canonical decimal text. The Arrow extension-type bytes are not directly transmitted to Postgres.

## 11. Versioning

This is v1 of `streamling.decimal_arb`. Future schema changes (e.g., a different magnitude encoding) MUST use a new extension name (e.g., `streamling.decimal_arb_v2`); this contract is frozen for the v1 name.
