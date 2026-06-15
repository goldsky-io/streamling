//! Connector capability matrix for the `streamling.decimal_arb` extension type.
//!
//! Implements the design from
//! `specs/001-decimal-arbitrary-precision/contracts/connector-capability.md`
//! (data-model.md E4 — `ConnectorCapabilityMatrix`).
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
//! This module provides the per-connector decision logic. The pipeline-
//! startup wiring (walking the configured pipeline DAG, extracting the
//! `(column, connector)` pairs, and consulting this module) is **T033** in
//! the spec's task list — that piece needs the pipeline-build infrastructure
//! and is left as a follow-up.

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
    /// Plugin-provided connector. The capability is whatever the plugin
    /// advertises; this module returns Reject by default for plugins that
    /// don't override (FR-019: opt-in must be explicit).
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

/// Per-column user opt-in directive. Today only `string` exists;
/// `contracts/yaml-schema.md` reserves the surface for future variants.
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

/// Compute the number of bytes required for an Avro `decimal` field at
/// the given precision, using the standard `ceil(precision * log2(10) / 8)`
/// formula plus one byte for the sign.
pub fn avro_bytes_required(precision: u32) -> u32 {
    let bits = (precision as f64) * std::f64::consts::LOG2_10;
    (bits.ceil() as u32).div_ceil(8) + 1
}

/// Decide whether a sink (`kind`) can carry a `decimal_arb` column with
/// declared `(precision, scale)`, given the user's `coerce_to_string`
/// opt-in (`true` if `coerce_to: string` is set on the sink column).
///
/// The same decision applies on the source side: a source advertises this
/// capability for the column it produces, and the validator rejects
/// mismatches there too.
pub fn capability_for_decimal_arb(
    kind: ConnectorKind,
    precision: u32,
    scale: u32,
    coerce_to_string: bool,
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
            if precision <= MAX_CLICKHOUSE_DECIMAL_PRECISION {
                CapabilityResult::Native
            } else if coerce_to_string {
                CapabilityResult::OptInOnly(CoercionDirective::String)
            } else {
                CapabilityResult::Reject(format!(
                    "ClickHouse Decimal precision is capped at {} digits; declared precision {} exceeds the cap. \
                     Add `coerce_to: string` under this column in the sink YAML to emit as a String column, \
                     or reduce declared precision to ≤{} if the source data fits.",
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
        ConnectorKind::Plugin => CapabilityResult::Reject(format!(
            "Plugin connector does not advertise streamling.decimal_arb support \
             (declared precision {}, scale {}). Implement `supports_decimal_arb` in \
             the plugin to override.",
            precision, scale,
        )),
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

/// Walk an Arrow `Schema`'s decimal_arb fields and confirm the connector
/// (`kind`) can carry each one — Native, OptInOnly with the user's
/// `coerce_to: string` directive, or Reject (collected into the result).
///
/// Pipeline-startup wiring: every place that builds a sink (or source)
/// from YAML should call this with the connector's resolved `Schema` and
/// directive list, surfacing `DecimalArbConfigErrors` to abort startup.
/// Non-decimal_arb fields are ignored.
pub fn validate_pipeline_decimal_arb(
    schema: &Schema,
    kind: ConnectorKind,
    directives: &[ColumnDirectiveView<'_>],
) -> Result<(), DecimalArbConfigErrors> {
    let mut errors: Vec<crate::error::StreamlingError> = Vec::new();
    for field in schema.fields() {
        let Some((precision, scale)) = DecimalArbType::precision_scale_from_field(field) else {
            continue;
        };
        let coerce_to_string = directives
            .iter()
            .find(|d| d.name == field.name())
            .map(|d| d.coerce_to_string)
            .unwrap_or(false);
        match capability_for_decimal_arb(kind, precision, scale, coerce_to_string) {
            CapabilityResult::Native | CapabilityResult::OptInOnly(_) => {}
            CapabilityResult::Reject(reason) => {
                errors.push(config_load_error(
                    field.name(),
                    kind,
                    precision,
                    scale,
                    &reason,
                ));
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
        let r = capability_for_decimal_arb(ConnectorKind::Postgres, 100, 18, false);
        assert_eq!(r, CapabilityResult::Native);
    }

    #[test]
    fn postgres_at_documented_cap_is_native() {
        let r = capability_for_decimal_arb(
            ConnectorKind::Postgres,
            MAX_POSTGRES_NUMERIC_PRECISION,
            0,
            false,
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
        );
        assert_eq!(r, CapabilityResult::OptInOnly(CoercionDirective::String));
    }

    // ---- ClickHouse ----

    #[test]
    fn clickhouse_native_at_or_below_76() {
        for p in [38, 50, 76] {
            assert_eq!(
                capability_for_decimal_arb(ConnectorKind::ClickHouse, p, 0, false),
                CapabilityResult::Native,
                "precision {} should be Native",
                p,
            );
        }
    }

    #[test]
    fn clickhouse_above_76_rejects_without_opt_in() {
        let r = capability_for_decimal_arb(ConnectorKind::ClickHouse, 100, 18, false);
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
        let r = capability_for_decimal_arb(ConnectorKind::ClickHouse, 100, 18, true);
        assert_eq!(r, CapabilityResult::OptInOnly(CoercionDirective::String));
    }

    #[test]
    fn hybrid_mirrors_clickhouse() {
        // Hybrid is ClickHouse-backed; same rules.
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::Hybrid, 76, 0, false),
            CapabilityResult::Native,
        );
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::Hybrid, 100, 18, true),
            CapabilityResult::OptInOnly(CoercionDirective::String),
        );
    }

    // ---- Kafka encodings ----

    #[test]
    fn kafka_json_native_at_any_precision() {
        for p in [1, 76, 1000, 65_535] {
            assert_eq!(
                capability_for_decimal_arb(ConnectorKind::KafkaJson, p, 0, false),
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
            ),
            CapabilityResult::Native,
        );
    }

    #[test]
    fn kafka_avro_sufficient_bytes_is_native() {
        // For precision 38, ~16 bytes are required (ceil(38 * 3.32 / 8) + 1).
        let needed = avro_bytes_required(38);
        assert_eq!(
            capability_for_decimal_arb(
                ConnectorKind::KafkaAvro {
                    declared_bytes: Some(needed)
                },
                38,
                10,
                false,
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
        );
        assert_eq!(r, CapabilityResult::OptInOnly(CoercionDirective::String));
    }

    #[test]
    fn kafka_protobuf_rejects_without_opt_in() {
        let r = capability_for_decimal_arb(ConnectorKind::KafkaProtobuf, 38, 10, false);
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
            capability_for_decimal_arb(ConnectorKind::KafkaProtobuf, 38, 10, true),
            CapabilityResult::OptInOnly(CoercionDirective::String),
        );
    }

    // ---- Plugins / SQS ----

    #[test]
    fn plugin_default_rejects() {
        let r = capability_for_decimal_arb(ConnectorKind::Plugin, 100, 18, false);
        match r {
            CapabilityResult::Reject(msg) => {
                assert!(msg.contains("Plugin"));
            }
            other => panic!("expected Reject, got {:?}", other),
        }
    }

    #[test]
    fn sqs_json_is_native() {
        assert_eq!(
            capability_for_decimal_arb(ConnectorKind::SqsJson, 100, 18, false),
            CapabilityResult::Native,
        );
    }

    // ---- Helpers ----

    #[test]
    fn avro_bytes_required_matches_documented_examples() {
        // 38-digit decimal: 38 * 3.32 / 8 = ~15.8, rounded up = 16, +1 sign = 17.
        // (Avro stores both sign and magnitude in the same two's-complement bytes,
        // so the +1 is conservative; we still want the conservative ceiling for the
        // capability check to be safe.)
        assert!(avro_bytes_required(38) >= 16);
        assert!(avro_bytes_required(76) >= 32);
        assert!(avro_bytes_required(100) >= 42);
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

    // ---- T033 / T064: pipeline-startup validator ----

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
}
