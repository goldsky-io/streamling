//! Connector capability matrix for the `streamling.decimal_arb` extension type.
//!
//! At pipeline configuration load, the validator must decide for each
//! `(decimal_arb column, connector)` pair whether the connector can carry
//! the column. There are three outcomes:
//!
//! - `Native` — the underlying store / wire encoding handles the declared
//!   `(precision, scale)` losslessly.
//! - `OptInOnly(directive)` — the connector cannot natively hold the column
//!   but the user has explicitly opted in to a coercion (e.g.
//!   `coerce_to: string`). Carries the directive the connector will apply.
//! - `Reject(reason)` — the connector cannot carry the column. The pipeline
//!   is rejected at config load with an error that names the column,
//!   connector, declared `(precision, scale)`, and an actionable hint.
//!
//! This module provides the per-connector decision logic, plus
//! `validate_pipeline_decimal_arb`, which walks a connector's schema
//! (including nested leaves) and applies that logic to every decimal_arb
//! column it finds.

use crate::streamling_user_err;
use crate::types::decimal_arb::DecimalArbType;
use arrow_schema::Schema;
use std::fmt;

/// Identifies the connector kind being evaluated. Matches the YAML `type:`
/// value on a sink/source. Variants correspond to the connectors that
/// today handle decimals (Postgres, ClickHouse, Kafka with various
/// encodings, and webhook/SQS — JSON-only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectorKind {
    /// Postgres source/sink (`type: postgres`). Native arbitrary-precision
    /// `NUMERIC` (capped at the documented Postgres ~1000-digit limit).
    Postgres,
    /// ClickHouse source/sink (`type: clickhouse`). Native `Decimal(p, s)`
    /// is capped at 76 digits — wider columns require `coerce_to: string`.
    ClickHouse,
    /// ClickHouse-backed hybrid source/sink (`type: hybrid`). Same caps as
    /// ClickHouse.
    Hybrid,
    /// Kafka source/sink with JSON encoding. Carries digit-strings
    /// natively at any precision.
    KafkaJson,
    /// Kafka source/sink with Avro encoding. Native iff the Avro
    /// `decimal` field's declared byte width can hold the precision; see
    /// [`avro_bytes_required`].
    KafkaAvro {
        /// Declared `bytes` width of the Avro `decimal` field (`None` for
        /// `bytes` logical decimals which are unbounded).
        declared_bytes: Option<u32>,
    },
    /// Kafka source/sink with Protobuf encoding. No native decimal in
    /// proto3; requires `coerce_to: string`.
    KafkaProtobuf,
    /// SQS or webhook (JSON-encoded payload). Same as KafkaJson.
    SqsJson,
    /// Plugin-provided connector. The host hands the batch to the plugin
    /// unchanged — the column arrives as `LargeBinary` carrying the
    /// `decimal_arb` extension metadata, exactly as it travels between
    /// built-in operators — so there is nothing for the host to convert or
    /// refuse. Whether a given plugin understands the type is the plugin's
    /// contract, not a config-load decision.
    Plugin,
}

impl fmt::Display for ConnectorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorKind::Postgres => f.write_str("postgres"),
            ConnectorKind::ClickHouse => f.write_str("clickhouse"),
            ConnectorKind::Hybrid => f.write_str("hybrid"),
            ConnectorKind::KafkaJson => f.write_str("kafka (json encoding)"),
            ConnectorKind::KafkaAvro { .. } => f.write_str("kafka (avro encoding)"),
            ConnectorKind::KafkaProtobuf => f.write_str("kafka (protobuf encoding)"),
            ConnectorKind::SqsJson => f.write_str("sqs/webhook (json encoding)"),
            ConnectorKind::Plugin => f.write_str("plugin"),
        }
    }
}

/// Per-column user opt-in directive. Today only `string` exists; the enum
/// reserves the surface for future variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoercionDirective {
    /// `coerce_to: string` — emit the column as a string field on the
    /// destination, encoded as canonical decimal text.
    String,
}

/// Outcome of asking a connector whether it can carry a `decimal_arb`
/// column with given `(precision, scale)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityResult {
    /// Connector handles the column natively at the declared
    /// `(precision, scale)` with no loss.
    Native,
    /// Connector cannot natively hold the column, but a user-supplied
    /// `coerce_to` directive lets it transmit the value as a string.
    OptInOnly(CoercionDirective),
    /// Connector cannot carry this column and there is no opt-in path.
    /// The pipeline must be rejected at config load with the supplied
    /// human-readable reason.
    Reject(String),
}

/// Postgres NUMERIC's documented practical maximum precision.
/// Per the Postgres docs (recent versions), `NUMERIC` supports
/// "up to 131072 digits before the decimal point [and] up to 16383 digits
/// after" but the safe, widely-deployed practical cap is much lower.
/// We use 1000 as a conservative ceiling that all supported server
/// versions accept; pipelines declaring more should be rejected with a
/// clear hint pointing at this constant.
pub const MAX_POSTGRES_NUMERIC_PRECISION: u32 = 1000;

/// ClickHouse native `Decimal(p, s)` precision ceiling.
pub const MAX_CLICKHOUSE_DECIMAL_PRECISION: u32 = 76;

/// The smallest `fixed` size (in bytes) that can hold every Avro `decimal`
/// of the given precision.
///
/// Avro stores a decimal's unscaled value as a two's-complement big-endian
/// integer, so the sign costs one *bit*, not a byte: a `fixed(n)` holds
/// precision `p` iff `10^p <= 2^(8n - 1)` (the spec's
/// `floor(log10(2^(8n-1) - 1)) >= p`). Computed exactly — a `+1` byte for
/// the sign would reject `decimal(38)` in `fixed(16)`, the layout most
/// producers emit.
pub fn avro_bytes_required(precision: u32) -> u32 {
    use num_bigint::BigUint;
    use num_traits::Pow;
    // `bits(10^p)` = floor(p·log2 10) + 1; `10^p` is never a power of two,
    // so `10^p <= 2^k` exactly when `bits(10^p) <= k`. One more bit for the
    // sign, rounded up to whole bytes.
    let bits = BigUint::from(10_u32).pow(precision).bits();
    u32::try_from((bits + 1).div_ceil(8))
        .unwrap_or(u32::MAX)
        .max(1)
}

/// Decide whether a sink (`kind`) can carry a `decimal_arb` column with
/// declared `(precision, scale)`, given the user's `coerce_to_string`
/// opt-in (`true` if `coerce_to: string` is set on the sink column) and an
/// optional `native_int_kind` origin hint (set when the column
/// originated as a fixed-width native integer like ClickHouse
/// `UInt256` / `Int256`).
///
/// The same decision applies on the source side: a source advertises this
/// capability for the column it produces, and the validator rejects
/// mismatches there too.
pub fn capability_for_decimal_arb(
    kind: ConnectorKind,
    precision: u32,
    scale: u32,
    coerce_to_string: bool,
    native_int_kind: Option<crate::types::decimal_arb::NativeIntKind>,
) -> CapabilityResult {
    match kind {
        ConnectorKind::Postgres => {
            if precision <= MAX_POSTGRES_NUMERIC_PRECISION {
                CapabilityResult::Native
            } else if coerce_to_string {
                CapabilityResult::OptInOnly(CoercionDirective::String)
            } else {
                CapabilityResult::Reject(format!(
                    "Postgres NUMERIC supports up to {} digits; declared precision {} exceeds the cap. \
                     Reduce declared precision, or set `coerce_to: string` to emit as TEXT.",
                    MAX_POSTGRES_NUMERIC_PRECISION, precision,
                ))
            }
        }
        ConnectorKind::ClickHouse | ConnectorKind::Hybrid => {
            // An integer-shaped column (scale 0) carrying a native_int_kind
            // hint routes through ClickHouse's first-class UInt256 / Int256
            // types whatever its declared precision: the hint says the values
            // are 256-bit integers, and the sink range-checks every value
            // against that type on write — one that does not fit fails the
            // batch loudly instead of being truncated, which is what the
            // retired u256 path did for Avro `decimal(p > 78, 0)`. An explicit
            // `coerce_to: string` on the column wins over the hint at any
            // precision: the operator asked for a String, and it is the way
            // out for values that do not fit 256 bits.
            use crate::types::decimal_arb::NativeIntKind;
            if scale == 0
                && matches!(
                    native_int_kind,
                    Some(NativeIntKind::U256) | Some(NativeIntKind::I256)
                )
            {
                return if coerce_to_string {
                    CapabilityResult::OptInOnly(CoercionDirective::String)
                } else {
                    CapabilityResult::Native
                };
            }
            if precision <= MAX_CLICKHOUSE_DECIMAL_PRECISION {
                CapabilityResult::Native
            } else if coerce_to_string {
                CapabilityResult::OptInOnly(CoercionDirective::String)
            } else {
                CapabilityResult::Reject(format!(
                    "ClickHouse Decimal precision is capped at {} digits; declared precision {} exceeds the cap. \
                     Add `coerce_to: string` under this column in the sink YAML to emit as a String column; \
                     for an integer-shaped column (scale 0) whose values fit 256 bits, pin it to `UInt256` or \
                     `Int256` in the sink's `schema_override` instead; or reduce declared precision to ≤{} if \
                     the source data fits.",
                    MAX_CLICKHOUSE_DECIMAL_PRECISION, precision, MAX_CLICKHOUSE_DECIMAL_PRECISION,
                ))
            }
        }
        ConnectorKind::KafkaJson | ConnectorKind::SqsJson => CapabilityResult::Native,
        ConnectorKind::KafkaAvro { declared_bytes } => {
            let needed = avro_bytes_required(precision);
            match declared_bytes {
                None => CapabilityResult::Native, // unbounded `bytes` decimal
                Some(b) if b >= needed => CapabilityResult::Native,
                Some(b) if coerce_to_string => {
                    let _ = b;
                    CapabilityResult::OptInOnly(CoercionDirective::String)
                }
                Some(b) => CapabilityResult::Reject(format!(
                    "Avro decimal field declares {} byte(s); declared precision {} requires \
                     {} byte(s). Widen the Avro `bytes` declaration or set `coerce_to: string` \
                     to encode as an Avro string.",
                    b, precision, needed,
                )),
            }
        }
        ConnectorKind::KafkaProtobuf => {
            if coerce_to_string {
                CapabilityResult::OptInOnly(CoercionDirective::String)
            } else {
                CapabilityResult::Reject(format!(
                    "Protobuf has no native decimal type; declared precision {} cannot be carried as a numeric. \
                     Set `coerce_to: string` to encode as a string field.",
                    precision,
                ))
            }
        }
        // The host passes batches to a plugin verbatim; a column it could not
        // carry does not exist. Rejecting here stopped every wide-int → plugin
        // sink pipeline at startup with a hint pointing at a hook that does
        // not exist.
        ConnectorKind::Plugin => CapabilityResult::Native,
    }
}

/// [`capability_for_decimal_arb`] for a decimal_arb leaf nested inside a
/// column (Struct / List / Map / …) rather than a top-level column.
/// `coerce_to_string` is the directive on the top-level column the leaf
/// belongs to; a directive cannot name a nested leaf.
///
/// Where it differs from the top-level decision:
///
/// - **ClickHouse**: the column's `coerce_to: string` turns *every* leaf under
///   it into a `String`, narrow ones included — the directive is the one
///   switch a nested leaf has. (A top-level column keeps its native
///   `Decimal(p, s)` at precision ≤ 76 whatever the directive says, which
///   existing tables rely on.) A wide unhinted leaf is rejected with a hint
///   that does not point at `schema_override`, whose native-int pins match
///   top-level columns only.
/// - **Postgres**: a container column is written as JSONB text, where a
///   nested leaf is a decimal string of any precision, so `NUMERIC`'s
///   precision cap does not apply to it.
pub fn capability_for_nested_decimal_arb(
    kind: ConnectorKind,
    precision: u32,
    scale: u32,
    coerce_to_string: bool,
    native_int_kind: Option<crate::types::decimal_arb::NativeIntKind>,
) -> CapabilityResult {
    match kind {
        ConnectorKind::ClickHouse | ConnectorKind::Hybrid => {
            if coerce_to_string {
                return CapabilityResult::OptInOnly(CoercionDirective::String);
            }
            match capability_for_decimal_arb(kind, precision, scale, false, native_int_kind) {
                CapabilityResult::Reject(_) => CapabilityResult::Reject(format!(
                    "ClickHouse Decimal precision is capped at {} digits; declared precision {} \
                     exceeds the cap. Set `coerce_to: string` on the top-level column that holds \
                     this field in the ClickHouse sink's `columns` setting \
                     (`STREAMLING__CLICKHOUSE_SINK__COLUMNS` in the environment) to write its \
                     decimal leaves as Strings, or reduce declared precision to ≤{} if the source \
                     data fits.",
                    MAX_CLICKHOUSE_DECIMAL_PRECISION, precision, MAX_CLICKHOUSE_DECIMAL_PRECISION,
                )),
                other => other,
            }
        }
        ConnectorKind::Postgres => CapabilityResult::Native,
        _ => capability_for_decimal_arb(kind, precision, scale, coerce_to_string, native_int_kind),
    }
}

/// Build the user-facing config-load error string for a given Reject result.
/// Centralizes the error format so every connector emits a consistent shape:
/// column, connector, declared (p, s), reason, hint.
pub fn config_load_error(
    column: &str,
    kind: ConnectorKind,
    precision: u32,
    scale: u32,
    reason: &str,
) -> crate::error::StreamlingError {
    streamling_user_err!(
        "config error: column `{}` (declared decimal_arb({}, {})) cannot be carried by {}: {}",
        column,
        precision,
        scale,
        kind,
        reason,
    )
}

/// Minimal per-column directive view used by the pipeline-startup validator.
/// Connectors can either pass their own `ColumnDirective` slice or build
/// these from whatever directive shape they expose (Postgres / ClickHouse
/// configs both reduce to `(name, coerce_to_string?)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDirectiveView<'a> {
    pub name: &'a str,
    pub coerce_to_string: bool,
}

/// All `Reject` outcomes from a single pipeline-startup validation pass.
/// Carrying them together lets the validator surface every misconfiguration
/// at once instead of failing on the first bad column.
#[derive(Debug)]
pub struct DecimalArbConfigErrors(pub Vec<crate::error::StreamlingError>);

impl DecimalArbConfigErrors {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn into_inner(self) -> Vec<crate::error::StreamlingError> {
        self.0
    }
}

impl fmt::Display for DecimalArbConfigErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, err) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{}", err)?;
        }
        Ok(())
    }
}

/// Connectors whose decimal_arb conversion covers only top-level columns.
///
/// ClickHouse is not one of them: its sink rebuilds `Array` / `Tuple` / `Map`
/// columns with every nested leaf in the same native type a top-level column
/// gets (`UInt256` / `Int256`, `Decimal(p, s)`, or `String` under the column's
/// `coerce_to: string`). The ClickHouse-backed hybrid connector reads
/// top-level columns only.
fn converts_only_top_level(kind: ConnectorKind) -> bool {
    matches!(kind, ConnectorKind::Hybrid)
}

/// `(precision, scale, native_int_kind)` of a decimal_arb field — or of a
/// legacy `streamling.u256` / `streamling.i256` field, which every sink
/// upgrades to a hinted `decimal_arb(78, 0)` on the way in and must therefore
/// be able to carry as such.
fn decimal_arb_view(
    field: &arrow_schema::Field,
) -> Option<(u32, u32, Option<crate::types::decimal_arb::NativeIntKind>)> {
    if let Some((p, s)) = DecimalArbType::precision_scale_from_field(field) {
        return Some((p, s, DecimalArbType::native_int_kind_from_field(field)));
    }
    // A dictionary- / run-end-encoded leaf carrying the metadata on its own
    // field is checked as the plain leaf every sink unwraps it to.
    if crate::formats::decimal_arb_text::is_encoded_decimal_arb_field(field) {
        return crate::formats::decimal_arb_text::plain_layout_field(field)
            .and_then(|plain| decimal_arb_view(&plain));
    }
    crate::types::decimal_arb_legacy::legacy_wide_int_kind(field).map(|kind| {
        (
            crate::types::decimal_arb_legacy::LEGACY_WIDE_INT_PRECISION,
            0,
            Some(kind),
        )
    })
}

/// One decimal_arb leaf found while walking a column, with the dotted path
/// used in error messages.
struct DecimalArbLeaf {
    path: String,
    precision: u32,
    scale: u32,
    hint: Option<crate::types::decimal_arb::NativeIntKind>,
    /// The leaf sits inside a `Union` somewhere below the column. Nothing
    /// serialises decimal_arb through a union today (neither the text bridge
    /// nor the Avro writer descends into one), so such a column is rejected
    /// for every connector rather than written as raw bytes.
    under_union: bool,
}

/// Collect every decimal_arb leaf *below* `field` — the top-level field itself
/// is handled by the caller. Descends through every Arrow container that can
/// hold a field: Struct, the list family (view layouts included), Map, Union,
/// run-end encoding and dictionary encoding. The walk used to stop at
/// Struct / List / LargeList / FixedSizeList / Map, so a leaf under any other
/// layout was never checked and reached the sink unconverted.
fn collect_nested_decimal_arb(
    field: &arrow_schema::Field,
    path: &str,
    under_union: bool,
    out: &mut Vec<DecimalArbLeaf>,
) {
    // An encoded leaf carrying the metadata on its own field is the leaf
    // (`decimal_arb_view` reports it), not a layout holding one: descending
    // into its values would check the same leaf twice.
    if crate::formats::decimal_arb_text::is_encoded_decimal_arb_field(field) {
        return;
    }
    collect_nested_in_type(field.data_type(), path, under_union, out);
}

fn collect_nested_in_type(
    data_type: &arrow_schema::DataType,
    path: &str,
    under_union: bool,
    out: &mut Vec<DecimalArbLeaf>,
) {
    use arrow_schema::DataType;
    fn visit(
        child: &arrow_schema::Field,
        path: &str,
        under_union: bool,
        out: &mut Vec<DecimalArbLeaf>,
    ) {
        let child_path = format!("{}.{}", path, child.name());
        if let Some((precision, scale, hint)) = decimal_arb_view(child) {
            out.push(DecimalArbLeaf {
                path: child_path.clone(),
                precision,
                scale,
                hint,
                under_union,
            });
        }
        collect_nested_decimal_arb(child, &child_path, under_union, out);
    }
    match data_type {
        DataType::Struct(children) => children
            .iter()
            .for_each(|c| visit(c, path, under_union, out)),
        DataType::List(c)
        | DataType::LargeList(c)
        | DataType::FixedSizeList(c, _)
        | DataType::ListView(c)
        | DataType::LargeListView(c)
        | DataType::Map(c, _) => visit(c, path, under_union, out),
        DataType::RunEndEncoded(_, values) => visit(values, path, under_union, out),
        DataType::Union(fields, _) => fields.iter().for_each(|(_, f)| visit(f, path, true, out)),
        // A dictionary's value type is a bare `DataType`: a dictionary-encoded
        // leaf carries the extension metadata on the dictionary field itself
        // (picked up by `decimal_arb_view`), and the value type can still be
        // a container holding one.
        DataType::Dictionary(_, values) => collect_nested_in_type(values, path, under_union, out),
        _ => {}
    }
}

/// Walk an Arrow `Schema`'s decimal_arb fields and confirm the connector
/// (`kind`) can carry each one — Native, OptInOnly with the user's
/// `coerce_to: string` directive, or Reject (collected into the result).
///
/// Pipeline-startup wiring: every place that builds a sink (or source)
/// from YAML should call this with the connector's resolved `Schema` and
/// directive list, surfacing `DecimalArbConfigErrors` to abort startup.
/// Non-decimal_arb fields are ignored.
///
/// Leaves nested inside a Struct / List / Map (or any other container layout)
/// are decided by [`capability_for_nested_decimal_arb`] under the column's
/// directive: every connector that writes whole containers (JSON, Avro,
/// ClickHouse `Array` / `Tuple` / `Map`, Postgres JSONB, …) carries the leaf
/// when it would carry the column, and Postgres carries it at any precision
/// as JSON text. Hybrid converts top-level columns only, so a nested
/// leaf is rejected outright there rather than written as raw bytes, and a
/// leaf anywhere inside a `Union` is rejected for every connector because
/// nothing serialises decimal_arb through one.
pub fn validate_pipeline_decimal_arb(
    schema: &Schema,
    kind: ConnectorKind,
    directives: &[ColumnDirectiveView<'_>],
) -> Result<(), DecimalArbConfigErrors> {
    let mut errors: Vec<crate::error::StreamlingError> = Vec::new();
    for field in schema.fields() {
        let coerce_to_string = directives
            .iter()
            .find(|d| d.name == field.name())
            .map(|d| d.coerce_to_string)
            .unwrap_or(false);

        let mut leaves = Vec::new();
        if let Some((precision, scale, hint)) = decimal_arb_view(field) {
            leaves.push(DecimalArbLeaf {
                path: field.name().clone(),
                precision,
                scale,
                hint,
                under_union: false,
            });
        }
        let top_level = leaves.len();
        collect_nested_decimal_arb(field, field.name(), false, &mut leaves);

        for (i, leaf) in leaves.into_iter().enumerate() {
            let DecimalArbLeaf {
                path,
                precision,
                scale,
                hint,
                under_union,
            } = leaf;
            if under_union {
                errors.push(config_load_error(
                    &path,
                    kind,
                    precision,
                    scale,
                    "decimal_arb nested inside a union is not supported by any connector; the \
                     value would be written as raw bytes. Flatten the union in a transform.",
                ));
                continue;
            }
            if i >= top_level && converts_only_top_level(kind) {
                errors.push(config_load_error(
                    &path,
                    kind,
                    precision,
                    scale,
                    "decimal_arb nested inside a struct/list/map is not supported by this \
                     connector (only top-level columns are converted); flatten the column \
                     or send it to a JSON/Avro sink",
                ));
                continue;
            }
            let capability = if i >= top_level {
                capability_for_nested_decimal_arb(kind, precision, scale, coerce_to_string, hint)
            } else {
                capability_for_decimal_arb(kind, precision, scale, coerce_to_string, hint)
            };
            if let CapabilityResult::Reject(reason) = capability {
                errors.push(config_load_error(&path, kind, precision, scale, &reason));
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(DecimalArbConfigErrors(errors))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Postgres ----

    #[test]
    fn postgres_native_within_cap() {
        let r = capability_for_decimal_arb(ConnectorKind::Postgres, 100, 18, false, None);
        assert_eq!(r, CapabilityResult::Native);
    }

    #[test]
    fn postgres_at_documented_cap_is_native() {
        let r = capability_for_decimal_arb(
            ConnectorKind::Postgres,
            MAX_POSTGRES_NUMERIC_PRECISION,
            0,
            false,
            None,
        );
        assert_eq!(r, CapabilityResult::Native);
    }

    #[test]
    fn postgres_above_cap_rejects_without_opt_in() {
        let r = capability_for_decimal_arb(
            ConnectorKind::Postgres,
            MAX_POSTGRES_NUMERIC_PRECISION + 1,
            0,
            false,
            None,
        );
        match r {
            CapabilityResult::Reject(msg) => {
                assert!(msg.contains("Postgres"));
                assert!(msg.contains("1000"));
            }
            other => panic!("expected Reject, got {:?}", other),
        }
    }

    #[test]
    fn postgres_above_cap_with_opt_in_routes_to_string() {
        let r = capability_for_decimal_arb(
            ConnectorKind::Postgres,
            MAX_POSTGRES_NUMERIC_PRECISION + 1,
            0,
            true,
            None,
        );
        assert_eq!(r, CapabilityResult::OptInOnly(CoercionDirective::String));
    }

    // ---- ClickHouse ----

    #[test]
    fn clickhouse_native_at_or_below_76() {
        for p in [38, 50, 76] {
            assert_eq!(
                capability_for_decimal_arb(ConnectorKind::ClickHouse, p, 0, false, None),
                CapabilityResult::Native,
                "precision {} should be Native",
                p,
            );
        }
    }

    #[test]
    fn clickhouse_above_76_rejects_without_opt_in() {
        let r = capability_for_decimal_arb(ConnectorKind::ClickHouse, 100, 18, false, None);
        match r {
            CapabilityResult::Reject(msg) => {
                assert!(msg.contains("ClickHouse"));
                assert!(msg.contains("76"));
                assert!(msg.contains("coerce_to: string"));
            }
            other => panic!("expected Reject, got {:?}", other),
        }
    }

    #[test]
    fn clickhouse_above_76_with_opt_in_routes_to_string() {
        let r = capability_for_decimal_arb(ConnectorKind::ClickHouse, 100, 18, true, None);
        assert_eq!(r, CapabilityResult::OptInOnly(CoercionDirective::String));
    }

    #[test]
    fn hybrid_mirrors_clickhouse() {
        // Hybrid is ClickHouse-backed; same rules.
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::Hybrid, 76, 0, false, None),
            CapabilityResult::Native,
        );
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::Hybrid, 100, 18, true, None),
            CapabilityResult::OptInOnly(CoercionDirective::String),
        );
    }

    // ---- Kafka encodings ----

    #[test]
    fn kafka_json_native_at_any_precision() {
        for p in [1, 76, 1000, 65_535] {
            assert_eq!(
                capability_for_decimal_arb(ConnectorKind::KafkaJson, p, 0, false, None),
                CapabilityResult::Native,
            );
        }
    }

    #[test]
    fn kafka_avro_unbounded_bytes_is_native() {
        assert_eq!(
            capability_for_decimal_arb(
                ConnectorKind::KafkaAvro {
                    declared_bytes: None
                },
                1000,
                18,
                false,
                None,
            ),
            CapabilityResult::Native,
        );
    }

    #[test]
    fn kafka_avro_sufficient_bytes_is_native() {
        // Precision 38 needs exactly 16 bytes (Decimal128 in `fixed(16)`).
        let needed = avro_bytes_required(38);
        assert_eq!(needed, 16);
        assert_eq!(
            capability_for_decimal_arb(
                ConnectorKind::KafkaAvro {
                    declared_bytes: Some(needed)
                },
                38,
                10,
                false,
                None,
            ),
            CapabilityResult::Native,
        );
    }

    #[test]
    fn kafka_avro_insufficient_bytes_rejects() {
        let too_small = avro_bytes_required(38) - 1;
        let r = capability_for_decimal_arb(
            ConnectorKind::KafkaAvro {
                declared_bytes: Some(too_small),
            },
            38,
            10,
            false,
            None,
        );
        match r {
            CapabilityResult::Reject(msg) => {
                assert!(msg.contains("Avro decimal"));
                assert!(msg.contains("byte"));
            }
            other => panic!("expected Reject, got {:?}", other),
        }
    }

    #[test]
    fn kafka_avro_insufficient_bytes_with_opt_in_routes_to_string() {
        let too_small = avro_bytes_required(38) - 1;
        let r = capability_for_decimal_arb(
            ConnectorKind::KafkaAvro {
                declared_bytes: Some(too_small),
            },
            38,
            10,
            true,
            None,
        );
        assert_eq!(r, CapabilityResult::OptInOnly(CoercionDirective::String));
    }

    #[test]
    fn kafka_protobuf_rejects_without_opt_in() {
        let r = capability_for_decimal_arb(ConnectorKind::KafkaProtobuf, 38, 10, false, None);
        match r {
            CapabilityResult::Reject(msg) => {
                assert!(msg.contains("Protobuf"));
                assert!(msg.contains("coerce_to: string"));
            }
            other => panic!("expected Reject, got {:?}", other),
        }
    }

    #[test]
    fn kafka_protobuf_with_opt_in_routes_to_string() {
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::KafkaProtobuf, 38, 10, true, None),
            CapabilityResult::OptInOnly(CoercionDirective::String),
        );
    }

    // ---- Plugins / SQS ----

    #[test]
    fn plugin_passes_decimal_arb_through() {
        // The host does not convert or refuse anything on the way into a
        // plugin — the batch arrives as-is — so there is no capability gap
        // for it to report. Rejecting here stopped every wide-int → plugin
        // sink pipeline at startup, pointing at a hook that does not exist.
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::Plugin, 100, 18, false, None),
            CapabilityResult::Native,
        );
    }

    #[test]
    fn sqs_json_is_native() {
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::SqsJson, 100, 18, false, None),
            CapabilityResult::Native,
        );
    }

    // ---- Helpers ----

    #[test]
    fn avro_bytes_required_matches_documented_examples() {
        // Exact, per the Avro spec (`floor(log10(2^(8n-1) - 1)) >= p`):
        // the sign is one bit of the two's-complement magnitude, not a byte.
        assert_eq!(avro_bytes_required(1), 1); // 10 < 2^7
        assert_eq!(avro_bytes_required(2), 1); // 100 < 2^7 = 128
        assert_eq!(avro_bytes_required(3), 2); // 1000 > 127
        assert_eq!(avro_bytes_required(18), 8); // Decimal64 in fixed(8)
        assert_eq!(avro_bytes_required(38), 16); // Decimal128 in fixed(16)
        assert_eq!(avro_bytes_required(76), 32); // 10^76 < 2^255 ≈ 5.79e76
        assert_eq!(avro_bytes_required(77), 33); // 10^77 > 2^255
        assert_eq!(avro_bytes_required(100), 42);
        // Every result really holds 10^p - 1 and does not with one byte less.
        for p in 1..=200_u32 {
            let n = avro_bytes_required(p);
            let limit = |bytes: u32| num_bigint::BigUint::from(2_u32).pow(8 * bytes - 1);
            let max_value = num_bigint::BigUint::from(10_u32).pow(p) - 1_u32;
            assert!(max_value < limit(n), "precision {p} does not fit {n} bytes");
            if n > 1 {
                assert!(
                    max_value >= limit(n - 1),
                    "precision {p} fits {} bytes",
                    n - 1
                );
            }
        }
    }

    #[test]
    fn config_load_error_contains_diagnostic_fields() {
        let err = config_load_error(
            "pipeline.sinks.analytics.amount",
            ConnectorKind::ClickHouse,
            100,
            18,
            "ClickHouse Decimal precision is capped at 76 digits",
        );
        let msg = format!("{}", err);
        assert!(msg.contains("pipeline.sinks.analytics.amount"));
        assert!(msg.contains("decimal_arb(100, 18)"));
        assert!(msg.contains("clickhouse"));
        assert!(msg.contains("capped at 76"));
    }

    // ---- pipeline-startup validator ----

    use crate::types::decimal_arb::DecimalArbType;
    use arrow_schema::{DataType, Field, Schema};

    fn schema_with_amount_column(precision: u32, scale: u32) -> Schema {
        let amount = DecimalArbType::field("amount", precision, scale, true).unwrap();
        let id = Field::new("id", DataType::Int64, false);
        Schema::new(vec![id, amount])
    }

    #[test]
    fn validator_passes_for_native_only_pipeline() {
        // ClickHouse can natively carry decimal_arb(50, 5) (≤76).
        let schema = schema_with_amount_column(50, 5);
        let result = validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn validator_rejects_clickhouse_without_opt_in() {
        let schema = schema_with_amount_column(100, 18);
        let errs =
            validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]).unwrap_err();
        assert_eq!(errs.len(), 1);
        let msg = format!("{}", errs);
        assert!(msg.contains("amount"));
        assert!(msg.contains("clickhouse"));
        assert!(msg.contains("76"));
        assert!(msg.contains("coerce_to: string"));
    }

    #[test]
    fn validator_passes_clickhouse_with_opt_in() {
        let schema = schema_with_amount_column(100, 18);
        let directives = vec![ColumnDirectiveView {
            name: "amount",
            coerce_to_string: true,
        }];
        let result = validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &directives);
        assert!(result.is_ok());
    }

    #[test]
    fn validator_passes_for_postgres_at_any_supported_precision() {
        let schema = schema_with_amount_column(500, 100);
        let result = validate_pipeline_decimal_arb(&schema, ConnectorKind::Postgres, &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn validator_collects_all_errors_at_once() {
        // Three columns, two violate ClickHouse's cap, one is fine.
        let bad_a = DecimalArbType::field("balance", 100, 18, true).unwrap();
        let ok = DecimalArbType::field("rate", 50, 5, true).unwrap();
        let bad_b = DecimalArbType::field("supply", 200, 0, true).unwrap();
        let schema = Schema::new(vec![bad_a, ok, bad_b]);
        let errs =
            validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]).unwrap_err();
        assert_eq!(errs.len(), 2, "should surface BOTH offending columns");
        let msg = format!("{}", errs);
        assert!(msg.contains("balance"));
        assert!(msg.contains("supply"));
        assert!(
            !msg.contains("rate"),
            "the in-bounds column must not appear in the error list"
        );
    }

    #[test]
    fn validator_ignores_non_decimal_arb_fields() {
        // Pure Int64 / Decimal128 schema → no decimal_arb columns →
        // validator is a no-op even when targeting a strict connector.
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("price", DataType::Decimal128(20, 5), false),
        ]);
        let result = validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn validator_directive_lookup_is_per_column() {
        // Two wide-precision columns; only one has the opt-in. The other
        // must still surface a Reject.
        let amount = DecimalArbType::field("amount", 100, 18, true).unwrap();
        let supply = DecimalArbType::field("supply", 100, 0, true).unwrap();
        let schema = Schema::new(vec![amount, supply]);
        let directives = vec![ColumnDirectiveView {
            name: "amount",
            coerce_to_string: true,
        }];
        let errs = validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &directives)
            .unwrap_err();
        assert_eq!(errs.len(), 1);
        let msg = format!("{}", errs);
        assert!(msg.contains("supply"));
        assert!(!msg.contains("`amount`"));
    }

    // ------- native_int_kind hint affects ClickHouse / Hybrid matrix -------

    #[test]
    fn clickhouse_native_for_decimal_arb_78_0_with_u256_hint() {
        use crate::types::decimal_arb::NativeIntKind;
        let r = capability_for_decimal_arb(
            ConnectorKind::ClickHouse,
            78,
            0,
            false,
            Some(NativeIntKind::U256),
        );
        assert_eq!(r, CapabilityResult::Native);
    }

    #[test]
    fn clickhouse_native_for_decimal_arb_78_0_with_i256_hint() {
        use crate::types::decimal_arb::NativeIntKind;
        let r = capability_for_decimal_arb(
            ConnectorKind::ClickHouse,
            78,
            0,
            false,
            Some(NativeIntKind::I256),
        );
        assert_eq!(r, CapabilityResult::Native);
    }

    #[test]
    fn hybrid_native_for_decimal_arb_with_native_int_hint() {
        use crate::types::decimal_arb::NativeIntKind;
        let r = capability_for_decimal_arb(
            ConnectorKind::Hybrid,
            78,
            0,
            false,
            Some(NativeIntKind::U256),
        );
        assert_eq!(r, CapabilityResult::Native);
    }

    #[test]
    fn clickhouse_native_int_hint_does_not_bypass_precision_cap_for_fractional_scale() {
        use crate::types::decimal_arb::NativeIntKind;
        // (100, 18) is wide and fractional — the hint is set but should be
        // ignored (the matrix only honors the hint at scale 0).
        // Without coerce_to: string, this stays Reject.
        let r = capability_for_decimal_arb(
            ConnectorKind::ClickHouse,
            100,
            18,
            false,
            Some(NativeIntKind::U256),
        );
        match r {
            CapabilityResult::Reject(_) => {}
            other => panic!(
                "expected Reject (hint should not apply for scale>0); got {:?}",
                other
            ),
        }
    }

    #[test]
    fn clickhouse_native_for_hinted_integer_column_at_any_precision() {
        use crate::types::decimal_arb::NativeIntKind;
        // Avro `decimal(100, 0)` carries the u256 hint exactly like
        // `decimal(78, 0)`; the sink range-checks each value on write, so the
        // declared precision does not gate the native route. (Rejecting here
        // turned every `decimal(p > 78, 0)` → ClickHouse pipeline that loaded
        // on the retired u256 path into a config-load failure.)
        for p in [77, 78, 79, 100] {
            assert_eq!(
                capability_for_decimal_arb(
                    ConnectorKind::ClickHouse,
                    p,
                    0,
                    false,
                    Some(NativeIntKind::U256)
                ),
                CapabilityResult::Native,
                "precision {p}",
            );
        }
    }

    #[test]
    fn clickhouse_coerce_to_string_wins_over_native_int_hint() {
        use crate::types::decimal_arb::NativeIntKind;
        // At any precision — below the Decimal cap too, where an unhinted
        // column would be a native Decimal: the operator asked for a String.
        for precision in [76, 78, 100] {
            assert_eq!(
                capability_for_decimal_arb(
                    ConnectorKind::ClickHouse,
                    precision,
                    0,
                    true,
                    Some(NativeIntKind::U256)
                ),
                CapabilityResult::OptInOnly(CoercionDirective::String),
                "precision {precision}"
            );
        }
    }

    /// A dictionary- or run-end-encoded leaf carrying its metadata on the
    /// encoded field is one leaf, checked once under the column's own name.
    #[test]
    fn an_encoded_leaf_is_checked_once() {
        use std::sync::Arc;
        let wide = DecimalArbType::field("x", 100, 2, true).unwrap();
        let dict = Field::new(
            "x",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeBinary)),
            true,
        )
        .with_metadata(wide.metadata().clone());
        let ree = Field::new(
            "x",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::LargeBinary, true)),
            ),
            true,
        )
        .with_metadata(wide.metadata().clone());
        for field in [dict, ree] {
            let errs = validate_pipeline_decimal_arb(
                &Schema::new(vec![field]),
                ConnectorKind::ClickHouse,
                &[],
            )
            .unwrap_err();
            let msg = format!("{errs}");
            assert_eq!(errs.len(), 1, "{msg}");
            assert!(msg.contains("column `x`"), "{msg}");
            assert!(!msg.contains("x.values"), "{msg}");
        }
    }

    #[test]
    fn validator_sees_leaves_under_every_container_layout() {
        use arrow_schema::{Fields, UnionFields, UnionMode};
        use std::sync::Arc;
        // The walk stopped at Struct / List / LargeList / FixedSizeList / Map,
        // so a leaf under a list view, a run-end encoding, a dictionary or a
        // union was never checked — and the text bridge later passed its
        // bytes through untouched.
        let leaf = || Arc::new(DecimalArbType::field("amt", 100, 0, true).unwrap());
        let in_list_view = Field::new("lv", DataType::ListView(leaf()), true);
        let in_ree = Field::new(
            "ree",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                leaf(),
            ),
            true,
        );
        let in_dict = Field::new(
            "dict",
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Struct(Fields::from(vec![leaf()]))),
            ),
            true,
        );
        let schema = Schema::new(vec![in_list_view, in_ree, in_dict]);
        let errs =
            validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]).unwrap_err();
        let msg = format!("{}", errs);
        assert_eq!(errs.len(), 3, "{msg}");
        for path in ["lv.amt", "ree.amt", "dict.amt"] {
            assert!(msg.contains(path), "{path} missing from: {msg}");
        }

        // A leaf under a union is rejected even where the connector would
        // otherwise carry the column natively.
        let union = Field::new(
            "u",
            DataType::Union(
                UnionFields::try_new(
                    vec![0, 1],
                    vec![Arc::new(Field::new("s", DataType::Utf8, true)), leaf()],
                )
                .unwrap(),
                UnionMode::Dense,
            ),
            true,
        );
        let errs =
            validate_pipeline_decimal_arb(&Schema::new(vec![union]), ConnectorKind::KafkaJson, &[])
                .unwrap_err();
        let msg = format!("{}", errs);
        assert!(msg.contains("u.amt") && msg.contains("union"), "{msg}");
    }

    #[test]
    fn validator_sees_a_dictionary_encoded_leaf_with_metadata_on_its_field() {
        use std::sync::Arc;
        // The Arrow convention for a dictionary-encoded extension type puts
        // the extension metadata on the dictionary field itself; the leaf is
        // checked as the plain decimal_arb the sinks unwrap it to.
        let encoded = |name: &str| {
            Field::new(
                name,
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeBinary)),
                true,
            )
            .with_metadata(
                DecimalArbType::field(name, 100, 0, true)
                    .unwrap()
                    .metadata()
                    .clone(),
            )
        };
        let schema = Schema::new(vec![
            encoded("top"),
            Field::new("l", DataType::List(Arc::new(encoded("item"))), true),
        ]);
        let errs =
            validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]).unwrap_err();
        let msg = format!("{}", errs);
        assert_eq!(errs.len(), 2, "{msg}");
        for path in ["top", "l.item"] {
            assert!(msg.contains(path), "{path} missing from: {msg}");
        }
    }

    /// `traces: List<Struct<id Int64, value: leaf>>`, the shape plugins emit
    /// for per-transaction call traces.
    fn traces_schema(leaf: Field) -> Schema {
        use std::sync::Arc;
        let item = Field::new(
            "item",
            DataType::Struct(vec![Field::new("id", DataType::Int64, true), leaf].into()),
            true,
        );
        Schema::new(vec![Field::new(
            "traces",
            DataType::List(Arc::new(item)),
            true,
        )])
    }

    #[test]
    fn clickhouse_carries_nested_leaves_it_can_store_natively() {
        use crate::types::decimal_arb::NativeIntKind;
        // ClickHouse holds UInt256 / Int256 / Decimal(p ≤ 76, s) inside
        // Array / Tuple / Map, and the sink converts nested leaves, so these
        // no longer need to be rejected.
        for leaf in [
            DecimalArbType::with_native_int_kind(
                DecimalArbType::field("value", 78, 0, true).unwrap(),
                NativeIntKind::U256,
            )
            .unwrap(),
            DecimalArbType::with_native_int_kind(
                DecimalArbType::field("value", 78, 0, true).unwrap(),
                NativeIntKind::I256,
            )
            .unwrap(),
            DecimalArbType::field("value", 50, 5, true).unwrap(),
        ] {
            let schema = traces_schema(leaf);
            assert!(
                validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]).is_ok(),
                "{schema:?}"
            );
        }
    }

    #[test]
    fn clickhouse_nested_wide_leaf_needs_the_columns_coerce_to_string() {
        let schema = traces_schema(DecimalArbType::field("value", 100, 18, true).unwrap());
        let errs =
            validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]).unwrap_err();
        let msg = format!("{errs}");
        assert_eq!(errs.len(), 1, "{msg}");
        assert!(msg.contains("traces.item.value"), "{msg}");
        assert!(msg.contains("coerce_to: string"), "{msg}");

        // The directive sits on the top-level column and covers every leaf in it.
        let directives = [ColumnDirectiveView {
            name: "traces",
            coerce_to_string: true,
        }];
        assert!(
            validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &directives).is_ok()
        );
    }

    #[test]
    fn clickhouse_nested_reject_does_not_suggest_a_top_level_only_remedy() {
        // `schema_override` native-int pins match top-level columns only, so
        // the hint for a nested leaf must not point there.
        let schema = traces_schema(DecimalArbType::field("value", 100, 0, true).unwrap());
        let msg = format!(
            "{}",
            validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]).unwrap_err()
        );
        assert!(msg.contains("traces.item.value"), "{msg}");
        assert!(msg.contains("coerce_to: string"), "{msg}");
        assert!(!msg.contains("schema_override"), "{msg}");

        // The same column at the top level keeps the schema_override hint.
        let top = Schema::new(vec![DecimalArbType::field("value", 100, 0, true).unwrap()]);
        let msg = format!(
            "{}",
            validate_pipeline_decimal_arb(&top, ConnectorKind::ClickHouse, &[]).unwrap_err()
        );
        assert!(msg.contains("schema_override"), "{msg}");
    }

    #[test]
    fn clickhouse_nested_coerce_to_string_covers_every_leaf() {
        use crate::types::decimal_arb::NativeIntKind;
        // A nested leaf under a `coerce_to: string` column is a String
        // whatever its precision or hint; a top-level narrow column under the
        // same directive keeps its native Decimal.
        for hint in [None, Some(NativeIntKind::U256)] {
            assert_eq!(
                capability_for_nested_decimal_arb(ConnectorKind::ClickHouse, 50, 0, true, hint),
                CapabilityResult::OptInOnly(CoercionDirective::String)
            );
        }
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::ClickHouse, 50, 5, true, None),
            CapabilityResult::Native
        );
        // Without the directive a nested leaf gets the top-level decision.
        assert_eq!(
            capability_for_nested_decimal_arb(ConnectorKind::ClickHouse, 50, 5, false, None),
            CapabilityResult::Native
        );
    }

    #[test]
    fn postgres_nested_leaves_are_json_text_at_any_precision() {
        // Postgres writes container columns as JSONB, where a leaf is a
        // decimal string; NUMERIC's precision cap applies to top-level
        // columns only, and no directive exists to opt a nested leaf out.
        let schema = traces_schema(DecimalArbType::field("value", 1200, 0, true).unwrap());
        assert!(validate_pipeline_decimal_arb(&schema, ConnectorKind::Postgres, &[]).is_ok());
        let top = Schema::new(vec![DecimalArbType::field("value", 1200, 0, true).unwrap()]);
        assert!(validate_pipeline_decimal_arb(&top, ConnectorKind::Postgres, &[]).is_err());
    }

    #[test]
    fn hybrid_still_rejects_nested_leaves() {
        let schema = traces_schema(DecimalArbType::field("value", 50, 5, true).unwrap());
        let errs = validate_pipeline_decimal_arb(&schema, ConnectorKind::Hybrid, &[]).unwrap_err();
        let msg = format!("{errs}");
        assert!(msg.contains("traces.item.value"), "{msg}");
        assert!(msg.contains("only top-level columns"), "{msg}");
    }

    #[test]
    fn clickhouse_still_rejects_a_leaf_under_a_union() {
        use arrow_schema::{UnionFields, UnionMode};
        use std::sync::Arc;
        let leaf = Arc::new(DecimalArbType::field("amt", 50, 0, true).unwrap());
        let union = Field::new(
            "u",
            DataType::Union(
                UnionFields::try_new(vec![0], vec![leaf]).unwrap(),
                UnionMode::Dense,
            ),
            true,
        );
        let errs = validate_pipeline_decimal_arb(
            &Schema::new(vec![union]),
            ConnectorKind::ClickHouse,
            &[],
        )
        .unwrap_err();
        let msg = format!("{errs}");
        assert!(msg.contains("u.amt") && msg.contains("union"), "{msg}");
    }

    #[test]
    fn clickhouse_existing_coerce_to_path_unaffected_by_absent_hint() {
        // No hint, p > 76, coerce_to=string: still OptInOnly. Regression
        // guard for the pre-hint behavior.
        let r = capability_for_decimal_arb(ConnectorKind::ClickHouse, 100, 18, true, None);
        assert_eq!(r, CapabilityResult::OptInOnly(CoercionDirective::String));
    }
}
