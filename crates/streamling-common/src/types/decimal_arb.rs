//! Arbitrary-precision decimal extension type (`streamling.decimal_arb`).
//!
//! Wire format and metadata schema: see
//! `specs/001-decimal-arbitrary-precision/contracts/arrow-extension-type.md`.
//! In-memory shape: see `data-model.md` (E1, E2).
//!
//! T008 (this module): extension-type registration helpers — `DecimalArbType`.
//! T009 (this module): in-memory value newtype — `DecimalArbValue`.
//! T010+ (later tasks): array, builder, conversions, sort encoding.

use crate::error::Result;
use crate::{streamling_err, streamling_user_err};
use arrow::array::{
    Array, Decimal128Array, Decimal128Builder, Decimal256Array, Decimal256Builder,
    LargeBinaryArray, LargeBinaryBuilder, StringArray, StringBuilder,
};
use arrow::datatypes::i256 as ArrowI256;
use arrow::datatypes::{DataType, Field};
use arrow_schema::extension::ExtensionType;
use arrow_schema::ArrowError;
use bigdecimal::BigDecimal;
use num_bigint::{BigInt, Sign};
use num_traits::Zero;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

// =====================================================================
// T008 — Extension-type registration
// =====================================================================

/// Sanity guard on `precision` per spec Assumptions / contracts arrow-extension-type §2.
/// This is documented as a "well above realistic schema declarations" bound, not a
/// hard product requirement; raise it if a real use case appears.
pub const MAX_PRECISION: u32 = 65_535;

/// Extension type identifier for arbitrary-precision decimals.
///
/// Field-level metadata is stored under the standard Arrow extension keys:
/// - `ARROW:extension:name = "streamling.decimal_arb"`
/// - `ARROW:extension:metadata = "{\"precision\": <u32>, \"scale\": <u32>}"`
///
/// The storage type is `DataType::LargeBinary` (T006 spike resolved this:
/// `BinaryView` would be auto-expanded at output by the existing
/// `expand_views_at_output` session config in `streamling-core`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DecimalArbType;

impl DecimalArbType {
    pub const EXTENSION_NAME: &'static str = "streamling.decimal_arb";
    pub const EXTENSION_NAME_KEY: &'static str = "ARROW:extension:name";
    pub const EXTENSION_METADATA_KEY: &'static str = "ARROW:extension:metadata";

    /// Field metadata key for the optional `native_int_kind` hint introduced
    /// by feature 002 (Retire U256/I256). Carries a value of `"u256"` or
    /// `"i256"` indicating which fixed-width native integer this decimal_arb
    /// column originated from, so sinks with matching native channels
    /// (ClickHouse `UInt256` / `Int256`) can preserve storage compactness.
    ///
    /// The hint is a property of the column's origin — *not* a constraint on
    /// runtime values. A `native_int_kind=u256` column whose value happens to
    /// be negative is legal in memory; it surfaces as an error only on a sink
    /// that has a matching native channel and cannot encode the negative.
    /// See `specs/002-retire-u256-i256/data-model.md` §E1 for the full
    /// semantics.
    pub const NATIVE_INT_KIND_KEY: &'static str = "streamling.native_int_kind";

    /// Storage type for the extension. Always `LargeBinary` in v1.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> DataType {
        DataType::LargeBinary
    }

    /// Build the per-`Field` metadata map for a `decimal_arb` column with the
    /// given declared `precision` and `scale`. Validates the invariants from
    /// `data-model.md` (E1) before producing the map.
    ///
    /// Delegates to the Arrow [`ExtensionType`] machinery so the
    /// `ARROW:extension:{name,metadata}` keys are produced canonically; the
    /// returned map is the same byte layout the type has always emitted.
    pub fn metadata(precision: u32, scale: u32) -> Result<HashMap<String, String>> {
        Ok(Self::field("decimal_arb", precision, scale, true)?
            .metadata()
            .clone())
    }

    /// Build a complete `Field` for a `decimal_arb` column.
    ///
    /// Construction goes through [`Field::try_with_extension_type`], which
    /// validates the `LargeBinary` storage invariant
    /// ([`DecimalArbExtension::supports_data_type`]) and stamps the standard
    /// Arrow extension keys.
    pub fn field(name: &str, precision: u32, scale: u32, nullable: bool) -> Result<Field> {
        let ext = DecimalArbExtension::new(precision, scale)?;
        let mut field = Field::new(name, Self::new(), nullable);
        field.try_with_extension_type(ext)?;
        Ok(field)
    }

    /// Returns `true` if the metadata map advertises the extension name.
    pub fn is_decimal_arb_metadata(metadata: &HashMap<String, String>) -> bool {
        metadata
            .get(Self::EXTENSION_NAME_KEY)
            .map(|v| v == Self::EXTENSION_NAME)
            .unwrap_or(false)
    }

    /// Returns `true` if the field carries the storage type AND the extension
    /// metadata. Either alone is insufficient — a plain `LargeBinary` column
    /// without the metadata is not `decimal_arb`.
    pub fn is_decimal_arb_field(field: &Field) -> bool {
        matches!(field.data_type(), DataType::LargeBinary)
            && Self::is_decimal_arb_metadata(field.metadata())
    }

    /// Extract `(precision, scale)` from a `decimal_arb` field's metadata.
    /// Returns `None` if the field is not `decimal_arb` or the metadata is
    /// missing/malformed.
    pub fn precision_scale_from_field(field: &Field) -> Option<(u32, u32)> {
        if !Self::is_decimal_arb_field(field) {
            return None;
        }
        field
            .try_extension_type::<DecimalArbExtension>()
            .ok()
            .map(|ext| (ext.params.precision, ext.params.scale))
    }

    /// Stamp the `native_int_kind` origin hint on a `decimal_arb` field.
    /// Returns the new `Field` with the hint added to its metadata.
    /// Rejects (with an internal error) if `field` is not a `decimal_arb`
    /// field — only decimal_arb columns may carry the hint per §E1.
    pub fn with_native_int_kind(field: Field, kind: NativeIntKind) -> Result<Field> {
        if !Self::is_decimal_arb_field(&field) {
            return Err(streamling_err!(
                "native_int_kind hint may only be applied to decimal_arb fields; got {:?}",
                field.data_type(),
            ));
        }
        let mut metadata = field.metadata().clone();
        metadata.insert(
            Self::NATIVE_INT_KIND_KEY.to_string(),
            kind.as_str().to_string(),
        );
        Ok(field.with_metadata(metadata))
    }

    /// Read the `native_int_kind` origin hint from a field's metadata.
    /// Returns `None` if the hint is absent (the common case for
    /// generic decimal_arb columns) or if the field is not decimal_arb.
    pub fn native_int_kind_from_field(field: &Field) -> Option<NativeIntKind> {
        if !Self::is_decimal_arb_field(field) {
            return None;
        }
        let raw = field.metadata().get(Self::NATIVE_INT_KIND_KEY)?;
        NativeIntKind::parse(raw)
    }

    /// Read the `native_int_kind` origin hint from a raw metadata map.
    /// Used by code paths that need to inspect the hint *after* a field's
    /// `DataType` has been transformed away from `LargeBinary` (e.g. the
    /// ClickHouse sink normalizes hinted decimal_arb columns to
    /// `FixedSizeBinary(32)` for wire-format compatibility, but keeps the
    /// metadata so the CREATE TABLE path can still consult the hint).
    pub fn native_int_kind_from_field_metadata(
        metadata: &HashMap<String, String>,
    ) -> Option<NativeIntKind> {
        if !Self::is_decimal_arb_metadata(metadata) {
            return None;
        }
        let raw = metadata.get(Self::NATIVE_INT_KIND_KEY)?;
        NativeIntKind::parse(raw)
    }
}

/// Resolved `(precision, scale)` carried in a `decimal_arb` field's
/// `ARROW:extension:metadata` payload, serialized as the JSON object
/// `{"precision":<u32>,"scale":<u32>}` (see
/// `specs/001-decimal-arbitrary-precision/contracts/arrow-extension-type.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecimalArbParams {
    pub precision: u32,
    pub scale: u32,
}

/// First-class Arrow 58 [`ExtensionType`] instance for `decimal_arb`.
///
/// [`DecimalArbType`] is a unit "namespace" of static helpers; this is the
/// concrete extension-type instance that owns the canonical (de)serialization
/// of the `(precision, scale)` metadata and enforces the `LargeBinary`
/// storage-type invariant. Field helpers route through the standard Arrow API
/// ([`Field::try_with_extension_type`] /
/// [`Field::try_extension_type`]), and new code may do the same:
///
/// ```ignore
/// let p_s = field.try_extension_type::<DecimalArbExtension>()
///     .map(|e| (e.precision(), e.scale()));
/// ```
///
/// The optional `native_int_kind` origin hint is stored under a *separate*
/// field-metadata key ([`DecimalArbType::NATIVE_INT_KIND_KEY`]), not inside
/// this extension's metadata, so it survives independently and is managed by
/// [`DecimalArbType::with_native_int_kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalArbExtension {
    params: DecimalArbParams,
}

impl DecimalArbExtension {
    /// Construct a validated extension instance for the given precision/scale.
    pub fn new(precision: u32, scale: u32) -> Result<Self> {
        validate_precision_scale(precision, scale)?;
        Ok(Self {
            params: DecimalArbParams { precision, scale },
        })
    }

    /// Declared precision.
    pub fn precision(&self) -> u32 {
        self.params.precision
    }

    /// Declared scale.
    pub fn scale(&self) -> u32 {
        self.params.scale
    }
}

impl ExtensionType for DecimalArbExtension {
    const NAME: &'static str = DecimalArbType::EXTENSION_NAME;
    type Metadata = DecimalArbParams;

    fn metadata(&self) -> &Self::Metadata {
        &self.params
    }

    fn serialize_metadata(&self) -> Option<String> {
        // Two u32s — infallible. Byte-for-byte the historical layout
        // (`{"precision":N,"scale":M}`) so on-wire/at-rest fields are unchanged.
        Some(format!(
            r#"{{"precision":{},"scale":{}}}"#,
            self.params.precision, self.params.scale
        ))
    }

    fn deserialize_metadata(metadata: Option<&str>) -> std::result::Result<Self::Metadata, ArrowError> {
        let raw = metadata.ok_or_else(|| {
            ArrowError::InvalidArgumentError("decimal_arb extension metadata missing".to_string())
        })?;
        let (precision, scale) = parse_precision_scale_json(raw)
            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
        Ok(DecimalArbParams { precision, scale })
    }

    fn supports_data_type(&self, data_type: &DataType) -> std::result::Result<(), ArrowError> {
        match data_type {
            DataType::LargeBinary => Ok(()),
            other => Err(ArrowError::InvalidArgumentError(format!(
                "decimal_arb storage type must be LargeBinary, got {other:?}"
            ))),
        }
    }

    fn try_new(
        data_type: &DataType,
        metadata: Self::Metadata,
    ) -> std::result::Result<Self, ArrowError> {
        validate_precision_scale(metadata.precision, metadata.scale)
            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))?;
        let ext = Self { params: metadata };
        ext.supports_data_type(data_type)?;
        Ok(ext)
    }
}

/// Origin hint for a `decimal_arb` column whose values were originally
/// carried as a fixed-width native integer in a wire format that supports
/// it (today: ClickHouse `UInt256` / `Int256`, Avro `decimal(p ≥ 77, 0)`,
/// Postgres `NUMERIC(78, 0)`). Sinks with matching native channels consult
/// this hint to preserve storage compactness; sinks without a matching
/// native channel ignore it.
///
/// Semantics: this is a *hint about origin*, not a *constraint on values*.
/// See `specs/002-retire-u256-i256/data-model.md` §E1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NativeIntKind {
    /// Originated as an unsigned 256-bit integer (Ethereum-style uint256,
    /// ClickHouse `UInt256`, Postgres `NUMERIC(78, 0)` by convention).
    U256,
    /// Originated as a signed 256-bit integer (Ethereum-style int256,
    /// ClickHouse `Int256`).
    I256,
}

impl NativeIntKind {
    /// String form used as the value of `streamling.native_int_kind` in
    /// Arrow field metadata.
    pub const fn as_str(&self) -> &'static str {
        match self {
            NativeIntKind::U256 => "u256",
            NativeIntKind::I256 => "i256",
        }
    }

    /// Parse the string form (case-insensitive). Returns `None` for any
    /// unrecognized value — callers treat that as "no hint" per the
    /// forward-compatibility rule.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "u256" => Some(NativeIntKind::U256),
            "i256" => Some(NativeIntKind::I256),
            _ => None,
        }
    }
}

impl Default for DecimalArbType {
    fn default() -> Self {
        Self
    }
}

fn validate_precision_scale(precision: u32, scale: u32) -> Result<()> {
    if precision == 0 {
        return Err(streamling_user_err!(
            "decimal_arb precision must be positive (got 0)"
        ));
    }
    if precision > MAX_PRECISION {
        return Err(streamling_user_err!(
            "decimal_arb precision {} exceeds maximum {}",
            precision,
            MAX_PRECISION
        ));
    }
    if scale > precision {
        return Err(streamling_user_err!(
            "decimal_arb scale {} cannot exceed precision {}",
            scale,
            precision
        ));
    }
    Ok(())
}

/// Minimal JSON parser for `{"precision":<n>,"scale":<m>}`. We avoid
/// pulling in `serde_json` here only to read two integers — the metadata
/// shape is fixed and tightly controlled by `metadata()` above. If the
/// shape ever broadens, switch to `serde_json`.
fn parse_precision_scale_json(raw: &str) -> Result<(u32, u32)> {
    let bytes = raw.trim();
    let inner = bytes
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            streamling_err!(
                "malformed decimal_arb metadata (expected JSON object): {}",
                raw
            )
        })?;

    let mut precision: Option<u32> = None;
    let mut scale: Option<u32> = None;
    for part in inner.split(',') {
        let (key, value) = part
            .split_once(':')
            .ok_or_else(|| streamling_err!("malformed decimal_arb metadata field: {}", part))?;
        let key = key.trim().trim_matches('"');
        let value = value.trim();
        let parsed: u32 = value
            .parse()
            .map_err(|e| streamling_err!("decimal_arb metadata field {} not a u32: {}", key, e))?;
        match key {
            "precision" => precision = Some(parsed),
            "scale" => scale = Some(parsed),
            other => {
                return Err(streamling_err!(
                    "unexpected key in decimal_arb metadata: {}",
                    other
                ));
            }
        }
    }

    let precision =
        precision.ok_or_else(|| streamling_err!("decimal_arb metadata missing 'precision' key"))?;
    let scale = scale.ok_or_else(|| streamling_err!("decimal_arb metadata missing 'scale' key"))?;
    validate_precision_scale(precision, scale)?;
    Ok((precision, scale))
}

// =====================================================================
// T009 — In-memory value
// =====================================================================

/// A single arbitrary-precision decimal value.
///
/// Wraps `bigdecimal::BigDecimal`. Always stored in a canonical form so
/// that two values that are numerically equal hash equal and compare equal
/// regardless of how they were textually written (`"123"`, `"0123"`, `"1.230"`,
/// `"1.23"` collapse to a single canonical representation).
///
/// `DecimalArbValue` carries no `(precision, scale)` of its own — those live
/// on the column-level `DecimalArbType`. Validation against column declarations
/// happens at boundaries (array append, sink emit, narrowing cast).
#[derive(Debug, Clone)]
pub struct DecimalArbValue(BigDecimal);

impl FromStr for DecimalArbValue {
    type Err = crate::error::StreamlingError;

    /// Parse a canonical decimal string (e.g. `"123.45"`, `"-0.0001"`, `"0"`).
    /// Strict — rejects malformed input.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let bd = BigDecimal::from_str(s)
            .map_err(|e| streamling_user_err!("failed to parse decimal_arb from '{}': {}", s, e))?;
        Ok(Self::canonicalize(bd))
    }
}

impl DecimalArbValue {
    /// Construct from a `(BigInt, scale)` pair, as produced by Avro / Postgres
    /// byte decoders. `scale` here follows the `bigdecimal` convention:
    /// positive scale means digits *after* the decimal point.
    pub fn from_bigint_and_scale(digits: BigInt, scale: i64) -> Self {
        Self::canonicalize(BigDecimal::from_bigint(digits, scale))
    }

    /// Construct from an existing `BigDecimal` (canonicalizing).
    pub fn from_bigdecimal(value: BigDecimal) -> Self {
        Self::canonicalize(value)
    }

    /// The underlying `BigDecimal` (canonical form).
    pub fn as_bigdecimal(&self) -> &BigDecimal {
        &self.0
    }

    /// Consume self and return the underlying `BigDecimal`.
    pub fn into_bigdecimal(self) -> BigDecimal {
        self.0
    }

    /// Number of digits required to represent the integer part of the value
    /// (sign and leading zeros excluded). Zero returns 0.
    pub fn integer_digit_count(&self) -> u64 {
        let normalized = self.0.normalized();
        if normalized.is_zero() {
            return 0;
        }
        let total = normalized.digits();
        let frac = normalized.fractional_digit_count();
        if frac >= 0 {
            total.saturating_sub(frac as u64)
        } else {
            // Negative fractional means the magnitude has trailing implicit
            // zeros (e.g. `100` stored as BigInt(1) × 10^2 has digits=1,
            // frac=-2, integer = 1 + 2 = 3).
            total.saturating_add((-frac) as u64)
        }
    }

    /// Number of *significant* digits after the decimal point. Trailing
    /// fractional zeros do not count, so `1.000` reports 0 and `1.23` reports 2.
    pub fn fractional_digit_count(&self) -> u64 {
        self.0.normalized().fractional_digit_count().max(0) as u64
    }

    /// Validate that this value fits a column's declared `(precision, scale)`
    /// in the SQL DECIMAL sense: integer digits ≤ `precision − scale` AND
    /// significant fractional digits ≤ `scale`. Trailing fractional zeros
    /// (e.g. `1.000` against `scale=1`) are non-significant and do not cause
    /// a failure. Caller supplies the column name for better error messages.
    pub fn check_fits(&self, precision: u32, scale: u32, column: &str) -> Result<()> {
        validate_precision_scale(precision, scale)?;
        let int_digits = self.integer_digit_count();
        let max_int_digits = (precision - scale) as u64;
        if int_digits > max_int_digits {
            return Err(streamling_user_err!(
                "value '{}' has {} integer digit(s), exceeds maximum {} for column '{}' \
                 (declared precision {} − scale {} = {} integer digits)",
                self.0,
                int_digits,
                max_int_digits,
                column,
                precision,
                scale,
                max_int_digits,
            ));
        }
        let frac = self.fractional_digit_count();
        if frac > scale as u64 {
            return Err(streamling_user_err!(
                "value '{}' has {} significant fractional digit(s), exceeds declared scale {} for column '{}'",
                self.0,
                frac,
                scale,
                column,
            ));
        }
        Ok(())
    }

    /// Canonical decimal string in non-exponent form (e.g. `"100"`, not
    /// `"1e+2"`). Round-trips through `from_str`. This matches the wire
    /// format expected by Postgres `NUMERIC`, ClickHouse `String`, and JSON
    /// digit-string consumers per `contracts/arrow-extension-type.md` §8.
    pub fn to_canonical_string(&self) -> String {
        self.0.to_plain_string()
    }

    /// Internal: bring the wrapped value into canonical form. We only
    /// canonicalize the zero / negative-zero case here; numerical equality
    /// of `1.0` vs `1.000` is handled by `BigDecimal`'s natural numeric
    /// semantics in `eq` / `cmp`, and by explicit normalization in `hash`.
    /// Leaving the original digit/scale form intact lets `digits()` and
    /// `fractional_digit_count()` reason about the original representation
    /// when callers need it.
    fn canonicalize(bd: BigDecimal) -> Self {
        if bd.is_zero() {
            return Self(BigDecimal::from(0i32));
        }
        Self(bd)
    }
}

impl PartialEq for DecimalArbValue {
    fn eq(&self, other: &Self) -> bool {
        // After canonicalization both sides share the same normalized form,
        // so BigDecimal's PartialEq (which checks numeric equality) is
        // sufficient — but we compare via BigDecimal::cmp to be explicit.
        self.0.eq(&other.0)
    }
}

impl Eq for DecimalArbValue {}

impl PartialOrd for DecimalArbValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DecimalArbValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Hash for DecimalArbValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Normalize specifically for hashing so that values which compare
        // equal (`1.0` and `1.000`) also hash equal. We do not store the
        // normalized form because callers may want to reason about the
        // original digit/scale shape (see `integer_digit_count`).
        let normalized = self.0.normalized();
        let (digits, scale) = normalized.as_bigint_and_exponent();
        let (sign, mag_bytes) = digits.to_bytes_be();
        match sign {
            Sign::Minus => 1u8.hash(state),
            Sign::NoSign | Sign::Plus => 0u8.hash(state),
        }
        mag_bytes.hash(state);
        scale.hash(state);
    }
}

impl std::fmt::Display for DecimalArbValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use the plain (non-exponent) form for consistency with FR-017 and
        // the wire format expected by Postgres / JSON.
        f.write_str(&self.0.to_plain_string())
    }
}

// =====================================================================
// Canonical byte encoding (required by T010 builder/array)
//
// Per `contracts/arrow-extension-type.md` §3:
//
//     [sign_byte][big-endian magnitude bytes]
//
//     sign_byte = 0x00 for non-negative, 0xFF for negative
//     magnitude = unsigned big-endian bytes of |value × 10^scale|, with
//                 leading 0x00 bytes stripped
//     value zero: sign_byte = 0x00, magnitude empty (1 total byte)
//
// The scale is NOT in the bytes — it lives on the column-level Field.
// Encoding therefore requires the *target* scale and rescales the value
// to it. Decoding requires the same scale (read from the Field metadata).
// =====================================================================

impl DecimalArbValue {
    /// Encode this value at the given column scale into canonical bytes.
    ///
    /// The value is first scale-aligned to `target_scale` (padding with
    /// trailing zeros if its own scale is smaller). If the value's
    /// significant fractional digits exceed `target_scale`, the caller
    /// must have already rejected via `check_fits` — we still call
    /// `with_scale_round` here as a defensive half-to-even round to keep
    /// the encoding total, but production callers should not depend on
    /// the rounding behavior to hide overflow.
    pub fn to_canonical_bytes_at_scale(&self, target_scale: u32) -> Vec<u8> {
        let scaled = self
            .0
            .with_scale_round(target_scale as i64, bigdecimal::RoundingMode::HalfEven);
        let (digits, _) = scaled.into_bigint_and_exponent();
        let (sign, magnitude) = digits.to_bytes_be();

        let sign_byte = match sign {
            Sign::Minus => 0xFFu8,
            Sign::NoSign | Sign::Plus => 0x00u8,
        };

        // Strip leading 0x00 bytes — canonical minimal representation.
        // For value zero `magnitude` is already empty (or a single 0x00,
        // which we strip).
        let start = magnitude
            .iter()
            .position(|&b| b != 0)
            .unwrap_or(magnitude.len());
        let stripped = &magnitude[start..];

        let mut out = Vec::with_capacity(1 + stripped.len());
        out.push(sign_byte);
        out.extend_from_slice(stripped);
        out
    }

    /// Decode a canonical byte payload at the given column scale.
    pub fn from_canonical_bytes_at_scale(bytes: &[u8], scale: u32) -> Result<Self> {
        if bytes.is_empty() {
            return Err(streamling_err!(
                "decimal_arb canonical bytes cannot be empty (need at least the sign byte)"
            ));
        }
        let sign_byte = bytes[0];
        let mag = &bytes[1..];
        let bigint = match (sign_byte, mag.is_empty()) {
            (0x00, true) => BigInt::from(0),
            (0x00, false) => BigInt::from_bytes_be(Sign::Plus, mag),
            (0xFF, true) => {
                return Err(streamling_err!(
                    "decimal_arb canonical bytes: negative zero is not a valid encoding"
                ));
            }
            (0xFF, false) => BigInt::from_bytes_be(Sign::Minus, mag),
            (b, _) => {
                return Err(streamling_err!(
                    "decimal_arb canonical bytes: invalid sign byte 0x{:02x}",
                    b
                ));
            }
        };
        Ok(Self::from_bigint_and_scale(bigint, scale as i64))
    }
}

// =====================================================================
// T010 — DecimalArbArrayBuilder + DecimalArbArray
// =====================================================================

/// Builder for a `DecimalArbArray`. Carries the column's declared
/// `(precision, scale)` and validates each appended value against them
/// per FR-013 (overflow surfaces as actionable error citing column name
/// and value).
pub struct DecimalArbArrayBuilder {
    column: String,
    precision: u32,
    scale: u32,
    builder: LargeBinaryBuilder,
}

/// Estimated average per-value byte size used to pre-size the underlying
/// `LargeBinaryBuilder`'s data buffer. Driven by precision: ~3.32 bits
/// per decimal digit + 1 byte for sign + small headroom.
fn estimated_value_bytes(precision: u32) -> usize {
    1 + ((precision as f64 * 3.322).ceil() as usize / 8 + 1)
}

impl DecimalArbArrayBuilder {
    /// Create a new builder for a column declared `(precision, scale)`.
    /// `column` is used for error messages.
    pub fn with_capacity(
        cap: usize,
        column: impl Into<String>,
        precision: u32,
        scale: u32,
    ) -> Result<Self> {
        validate_precision_scale(precision, scale)?;
        let avg = estimated_value_bytes(precision);
        Ok(Self {
            column: column.into(),
            precision,
            scale,
            builder: LargeBinaryBuilder::with_capacity(cap, cap.saturating_mul(avg)),
        })
    }

    /// Append a value (parses from canonical decimal text).
    pub fn append_str(&mut self, s: &str) -> Result<()> {
        let value = DecimalArbValue::from_str(s)?;
        self.append_value(&value)
    }

    /// Append a `DecimalArbValue`. Validates against declared
    /// `(precision, scale)` per FR-013 before encoding.
    pub fn append_value(&mut self, value: &DecimalArbValue) -> Result<()> {
        value.check_fits(self.precision, self.scale, &self.column)?;
        let bytes = value.to_canonical_bytes_at_scale(self.scale);
        self.builder.append_value(bytes);
        Ok(())
    }

    /// Append a NULL.
    pub fn append_null(&mut self) {
        self.builder.append_null();
    }

    /// Finalize the builder and return a `DecimalArbArray`.
    pub fn finish(mut self) -> DecimalArbArray {
        DecimalArbArray {
            inner: self.builder.finish(),
            precision: self.precision,
            scale: self.scale,
        }
    }
}

/// Arrow array of `decimal_arb` values. Wraps a `LargeBinaryArray` whose
/// payload is the canonical byte format from `arrow-extension-type.md` §3.
/// Carries the declared `(precision, scale)` so per-value decoding is
/// possible without consulting the source `Field`.
pub struct DecimalArbArray {
    inner: LargeBinaryArray,
    precision: u32,
    scale: u32,
}

impl DecimalArbArray {
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_null(&self, i: usize) -> bool {
        self.inner.is_null(i)
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    /// Return the value at index `i`, or `None` if NULL.
    pub fn value(&self, i: usize) -> Result<Option<DecimalArbValue>> {
        if self.inner.is_null(i) {
            return Ok(None);
        }
        let bytes = self.inner.value(i);
        DecimalArbValue::from_canonical_bytes_at_scale(bytes, self.scale).map(Some)
    }

    /// Borrow the underlying Arrow array (for IPC / passing to DataFusion).
    pub fn as_inner(&self) -> &LargeBinaryArray {
        &self.inner
    }

    /// Consume self and return the underlying Arrow array along with the
    /// declared `(precision, scale)`.
    pub fn into_inner(self) -> (LargeBinaryArray, u32, u32) {
        (self.inner, self.precision, self.scale)
    }

    /// Adopt an existing `LargeBinaryArray` as a `DecimalArbArray`,
    /// validating that the supplied `Field` carries `decimal_arb` metadata
    /// that matches.
    pub fn try_from_array_and_field(array: LargeBinaryArray, field: &Field) -> Result<Self> {
        let (precision, scale) = DecimalArbType::precision_scale_from_field(field).ok_or_else(
            || streamling_err!(
                "field '{}' is not a decimal_arb field (missing extension metadata or wrong storage type)",
                field.name(),
            ),
        )?;
        Ok(Self {
            inner: array,
            precision,
            scale,
        })
    }
}

// =====================================================================
// T011 — Arrow array conversions (FR-009 casts)
//
// All conversions go through `DecimalArbValue` so we inherit canonical
// equality, validation against `(precision, scale)`, and half-to-even
// rounding for narrowing casts.
// =====================================================================

impl DecimalArbArray {
    /// Cast a `Decimal128Array` (with known source `scale`) into a
    /// `DecimalArbArray` at the target `(precision, scale)`. Always
    /// lossless when widening — `decimal_arb` covers everything `Decimal128`
    /// can represent. NULLs are preserved.
    pub fn from_decimal128(
        source: &Decimal128Array,
        source_scale: i8,
        target_precision: u32,
        target_scale: u32,
        column: &str,
    ) -> Result<Self> {
        let mut builder = DecimalArbArrayBuilder::with_capacity(
            source.len(),
            column,
            target_precision,
            target_scale,
        )?;
        for i in 0..source.len() {
            if source.is_null(i) {
                builder.append_null();
                continue;
            }
            let value = DecimalArbValue::from_bigint_and_scale(
                BigInt::from(source.value(i)),
                source_scale as i64,
            );
            builder.append_value(&value)?;
        }
        Ok(builder.finish())
    }

    /// Cast a `Decimal256Array` (with known source `scale`) into a
    /// `DecimalArbArray`. Always lossless when widening. NULLs preserved.
    pub fn from_decimal256(
        source: &Decimal256Array,
        source_scale: i8,
        target_precision: u32,
        target_scale: u32,
        column: &str,
    ) -> Result<Self> {
        let mut builder = DecimalArbArrayBuilder::with_capacity(
            source.len(),
            column,
            target_precision,
            target_scale,
        )?;
        for i in 0..source.len() {
            if source.is_null(i) {
                builder.append_null();
                continue;
            }
            let bytes = source.value(i).to_be_bytes();
            let bigint = BigInt::from_signed_bytes_be(&bytes);
            let value = DecimalArbValue::from_bigint_and_scale(bigint, source_scale as i64);
            builder.append_value(&value)?;
        }
        Ok(builder.finish())
    }

    /// Narrow this array to `Decimal128(target_precision, target_scale)`.
    /// Each value is half-to-even rounded to `target_scale` and validated
    /// to fit `target_precision` (max 38). Out-of-range values surface
    /// FR-013 errors that name the column and value. NULLs preserved.
    pub fn to_decimal128(
        &self,
        target_precision: u8,
        target_scale: i8,
        column: &str,
    ) -> Result<Decimal128Array> {
        if target_precision == 0 || target_precision > 38 {
            return Err(streamling_user_err!(
                "Decimal128 precision must be in 1..=38 (got {}) for column '{}'",
                target_precision,
                column,
            ));
        }
        let mut builder = Decimal128Builder::with_capacity(self.len())
            .with_precision_and_scale(target_precision, target_scale)
            .map_err(|e| {
                streamling_err!(
                    "decimal128 builder rejected ({}, {}) for column '{}': {}",
                    target_precision,
                    target_scale,
                    column,
                    e,
                )
            })?;

        for i in 0..self.len() {
            match self.value(i)? {
                None => builder.append_null(),
                Some(v) => {
                    let scaled = v
                        .as_bigdecimal()
                        .with_scale_round(target_scale as i64, bigdecimal::RoundingMode::HalfEven);
                    let (bigint, _) = scaled.into_bigint_and_exponent();
                    let i128_val: i128 = bigint_to_i128(&bigint).ok_or_else(|| {
                        streamling_user_err!(
                            "value '{}' overflows Decimal128 for column '{}'",
                            v,
                            column,
                        )
                    })?;
                    // Validate against target precision: |i128_val| < 10^precision
                    if !i128_fits_precision(i128_val, target_precision) {
                        return Err(streamling_user_err!(
                            "value '{}' exceeds declared precision {} for Decimal128 column '{}'",
                            v,
                            target_precision,
                            column,
                        ));
                    }
                    builder.append_value(i128_val);
                }
            }
        }
        Ok(builder.finish())
    }

    /// Narrow this array to `Decimal256(target_precision, target_scale)`.
    /// Same semantics as `to_decimal128` but with 256-bit limits.
    pub fn to_decimal256(
        &self,
        target_precision: u8,
        target_scale: i8,
        column: &str,
    ) -> Result<Decimal256Array> {
        if target_precision == 0 || target_precision > 76 {
            return Err(streamling_user_err!(
                "Decimal256 precision must be in 1..=76 (got {}) for column '{}'",
                target_precision,
                column,
            ));
        }
        let mut builder = Decimal256Builder::with_capacity(self.len())
            .with_precision_and_scale(target_precision, target_scale)
            .map_err(|e| {
                streamling_err!(
                    "decimal256 builder rejected ({}, {}) for column '{}': {}",
                    target_precision,
                    target_scale,
                    column,
                    e,
                )
            })?;

        for i in 0..self.len() {
            match self.value(i)? {
                None => builder.append_null(),
                Some(v) => {
                    let scaled = v
                        .as_bigdecimal()
                        .with_scale_round(target_scale as i64, bigdecimal::RoundingMode::HalfEven);
                    let (bigint, _) = scaled.into_bigint_and_exponent();
                    let i256_val = bigint_to_arrow_i256(&bigint).ok_or_else(|| {
                        streamling_user_err!(
                            "value '{}' overflows Decimal256 for column '{}'",
                            v,
                            column,
                        )
                    })?;
                    if !arrow_i256_fits_precision(i256_val, target_precision) {
                        return Err(streamling_user_err!(
                            "value '{}' exceeds declared precision {} for Decimal256 column '{}'",
                            v,
                            target_precision,
                            column,
                        ));
                    }
                    builder.append_value(i256_val);
                }
            }
        }
        Ok(builder.finish())
    }

    /// Render every non-null value as its canonical decimal string. NULLs
    /// preserved.
    pub fn to_string_array(&self) -> Result<StringArray> {
        let mut builder = StringBuilder::with_capacity(self.len(), self.len() * 32);
        for i in 0..self.len() {
            match self.value(i)? {
                None => builder.append_null(),
                Some(v) => builder.append_value(v.to_canonical_string()),
            }
        }
        Ok(builder.finish())
    }

    /// Parse a `StringArray` into a `DecimalArbArray` at the target
    /// `(precision, scale)`. Strict: each non-null value must be a valid
    /// canonical decimal and fit the declared precision/scale. NULLs preserved.
    pub fn from_string_array(
        source: &StringArray,
        target_precision: u32,
        target_scale: u32,
        column: &str,
    ) -> Result<Self> {
        let mut builder = DecimalArbArrayBuilder::with_capacity(
            source.len(),
            column,
            target_precision,
            target_scale,
        )?;
        for i in 0..source.len() {
            if source.is_null(i) {
                builder.append_null();
                continue;
            }
            builder.append_str(source.value(i))?;
        }
        Ok(builder.finish())
    }
}

/// Convert a `BigInt` to `i128`, returning `None` on overflow.
fn bigint_to_i128(value: &BigInt) -> Option<i128> {
    let bytes = value.to_signed_bytes_be();
    if bytes.len() > 16 {
        return None;
    }
    let mut buf = if value.sign() == Sign::Minus {
        [0xFFu8; 16]
    } else {
        [0x00u8; 16]
    };
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(&bytes);
    Some(i128::from_be_bytes(buf))
}

/// Convert a `BigInt` to `arrow::buffer::i256`, returning `None` on overflow.
fn bigint_to_arrow_i256(value: &BigInt) -> Option<ArrowI256> {
    let bytes = value.to_signed_bytes_be();
    if bytes.len() > 32 {
        return None;
    }
    let mut buf = if value.sign() == Sign::Minus {
        [0xFFu8; 32]
    } else {
        [0x00u8; 32]
    };
    let start = 32 - bytes.len();
    buf[start..].copy_from_slice(&bytes);
    Some(ArrowI256::from_be_bytes(buf))
}

/// Returns true iff `|value| < 10^precision` (i.e. value fits a `Decimal128`
/// with the declared precision).
fn i128_fits_precision(value: i128, precision: u8) -> bool {
    let mut bound: i128 = 1;
    for _ in 0..precision {
        bound = bound.saturating_mul(10);
    }
    value > -bound && value < bound
}

/// Returns true iff |value| < 10^precision for an arrow `i256`.
fn arrow_i256_fits_precision(value: ArrowI256, precision: u8) -> bool {
    let mut bound = ArrowI256::from_i128(1);
    let ten = ArrowI256::from_i128(10);
    for _ in 0..precision {
        bound = bound.wrapping_mul(ten);
    }
    let neg_bound = bound.wrapping_neg();
    value > neg_bound && value < bound
}

// =====================================================================
// T012 — Custom row sort encoding for sort correctness on signed values
//
// Per research R5: bytewise compare on the canonical encoding is wrong for
// negatives (sign byte 0xFF sorts after 0x00). The function below converts
// a canonical-bytes payload into a sort key whose bytewise comparison
// reproduces numeric order across signs, magnitudes, and lengths.
// =====================================================================

/// Convert canonical `decimal_arb` bytes into a sort key.
///
/// Encoding:
/// - Negative: `[0u8] [!(magnitude_len as u32, BE)] [bit-flipped magnitude]`
/// - Non-negative: `[1u8] [magnitude_len as u32, BE] [magnitude]`
///
/// Properties:
/// - Negatives sort before positives (prefix `0` < `1`).
/// - Among positives: shorter magnitude sorts first (smaller value), within
///   same length, byte-by-byte compare matches numeric order.
/// - Among negatives: longer magnitude sorts first (more negative, smaller
///   value), via the flipped length; within same length, bit-flipped bytes
///   reverse the byte order so larger absolute values come first.
pub fn decimal_arb_to_sort_key(canonical_bytes: &[u8]) -> Vec<u8> {
    if canonical_bytes.is_empty() {
        // Defensive; in practice arrays use validity bits for NULL.
        return vec![1u8, 0, 0, 0, 0];
    }
    let sign_byte = canonical_bytes[0];
    let magnitude = &canonical_bytes[1..];
    let len = magnitude.len() as u32;

    let mut key = Vec::with_capacity(1 + 4 + magnitude.len());
    if sign_byte == 0xFF {
        key.push(0u8);
        key.extend_from_slice(&(!len).to_be_bytes());
        key.extend(magnitude.iter().map(|b| !b));
    } else {
        key.push(1u8);
        key.extend_from_slice(&len.to_be_bytes());
        key.extend_from_slice(magnitude);
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash<T: Hash>(t: &T) -> u64 {
        let mut h = DefaultHasher::new();
        t.hash(&mut h);
        h.finish()
    }

    // ------- DecimalArbType -------

    #[test]
    fn type_advertises_storage_as_large_binary() {
        assert_eq!(DecimalArbType::new(), DataType::LargeBinary);
    }

    #[test]
    fn metadata_round_trips_precision_and_scale() {
        let m = DecimalArbType::metadata(100, 18).unwrap();
        assert_eq!(
            m.get(DecimalArbType::EXTENSION_NAME_KEY)
                .map(|s| s.as_str()),
            Some(DecimalArbType::EXTENSION_NAME),
        );
        let raw = m.get(DecimalArbType::EXTENSION_METADATA_KEY).unwrap();
        let (p, s) = parse_precision_scale_json(raw).unwrap();
        assert_eq!((p, s), (100, 18));
    }

    #[test]
    fn field_helper_builds_a_recognizable_field() {
        let f = DecimalArbType::field("amount", 100, 18, true).unwrap();
        assert!(DecimalArbType::is_decimal_arb_field(&f));
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&f),
            Some((100, 18))
        );
    }

    #[test]
    fn plain_large_binary_field_is_not_decimal_arb() {
        let f = Field::new("blob", DataType::LargeBinary, true);
        assert!(!DecimalArbType::is_decimal_arb_field(&f));
        assert_eq!(DecimalArbType::precision_scale_from_field(&f), None);
    }

    #[test]
    fn field_round_trips_through_arrow_extension_api() {
        // The field built by the helper is recognized by the standard Arrow
        // extension-type API, and reading it back yields the same params.
        let f = DecimalArbType::field("amount", 100, 18, true).unwrap();
        assert!(f.has_valid_extension_type::<DecimalArbExtension>());
        let ext = f.try_extension_type::<DecimalArbExtension>().unwrap();
        assert_eq!((ext.precision(), ext.scale()), (100, 18));
        assert_eq!(f.extension_type_name(), Some(DecimalArbType::EXTENSION_NAME));
    }

    #[test]
    fn extension_rejects_non_large_binary_storage() {
        // supports_data_type / try_new enforce the LargeBinary invariant.
        let ext = DecimalArbExtension::new(10, 2).unwrap();
        assert!(ext.supports_data_type(&DataType::LargeBinary).is_ok());
        assert!(ext.supports_data_type(&DataType::Binary).is_err());
        let mut wrong = Field::new("x", DataType::Binary, true);
        assert!(wrong.try_with_extension_type(ext).is_err());
    }

    #[test]
    fn extension_metadata_serialization_is_byte_stable() {
        // Pins the historical `{"precision":N,"scale":M}` layout so existing
        // at-rest / on-wire fields keep deserializing.
        let ext = DecimalArbExtension::new(100, 18).unwrap();
        assert_eq!(
            ext.serialize_metadata().as_deref(),
            Some(r#"{"precision":100,"scale":18}"#)
        );
    }

    #[test]
    fn metadata_rejects_invalid_precision_scale() {
        assert!(DecimalArbType::metadata(0, 0).is_err());
        assert!(DecimalArbType::metadata(MAX_PRECISION + 1, 0).is_err());
        assert!(DecimalArbType::metadata(10, 11).is_err()); // scale > precision
    }

    #[test]
    fn metadata_parser_rejects_unknown_keys() {
        let bad = r#"{"precision":10,"scale":2,"extra":1}"#;
        assert!(parse_precision_scale_json(bad).is_err());
    }

    // ------- DecimalArbValue -------

    #[test]
    fn from_str_round_trips_canonical_form() {
        let v = DecimalArbValue::from_str("12345.678").unwrap();
        assert_eq!(v.to_canonical_string(), "12345.678");
    }

    #[test]
    fn from_str_canonicalizes_leading_zeros() {
        // Two textual forms; same canonical value; same hash; equal.
        let a = DecimalArbValue::from_str("0123").unwrap();
        let b = DecimalArbValue::from_str("123").unwrap();
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn from_str_canonicalizes_trailing_zeros() {
        let a = DecimalArbValue::from_str("1.0").unwrap();
        let b = DecimalArbValue::from_str("1.000").unwrap();
        assert_eq!(a, b, "trailing fractional zeros must canonicalize");
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn negative_zero_is_canonicalized_to_zero() {
        let neg_zero = DecimalArbValue::from_str("-0").unwrap();
        let zero = DecimalArbValue::from_str("0").unwrap();
        assert_eq!(neg_zero, zero);
        assert_eq!(hash(&neg_zero), hash(&zero));
    }

    #[test]
    fn from_str_rejects_garbage() {
        assert!(DecimalArbValue::from_str("not a number").is_err());
        assert!(DecimalArbValue::from_str("1.2.3").is_err());
    }

    #[test]
    fn ordering_works_for_negative_values() {
        // i256-style sort bug regression guard at the value level. The full
        // bytewise-sort guard lands with T012 (custom Row encoding).
        let neg = DecimalArbValue::from_str("-100").unwrap();
        let zero = DecimalArbValue::from_str("0").unwrap();
        let pos = DecimalArbValue::from_str("100").unwrap();
        let mut v = vec![pos.clone(), neg.clone(), zero.clone()];
        v.sort();
        assert_eq!(v, vec![neg, zero, pos]);
    }

    #[test]
    fn check_fits_validates_precision_and_scale() {
        // 100: 3 integer digits, 0 fractional.
        let v = DecimalArbValue::from_str("100").unwrap();
        assert_eq!(v.integer_digit_count(), 3);
        assert_eq!(v.fractional_digit_count(), 0);
        assert!(v.check_fits(3, 0, "x").is_ok());
        assert!(v.check_fits(2, 0, "x").is_err()); // precision − scale = 2 < 3 integer digits
        // 1.23: 1 integer digit, 2 fractional digits.
        let v = DecimalArbValue::from_str("1.23").unwrap();
        assert_eq!(v.integer_digit_count(), 1);
        assert_eq!(v.fractional_digit_count(), 2);
        assert!(v.check_fits(3, 2, "x").is_ok());
        assert!(v.check_fits(3, 1, "x").is_err()); // scale 1 < 2 sig fractional
    }

    #[test]
    fn check_fits_ignores_non_significant_trailing_zeros() {
        // 1.000 has 0 *significant* fractional digits, so it fits scale=0.
        let v = DecimalArbValue::from_str("1.000").unwrap();
        assert_eq!(v.fractional_digit_count(), 0);
        assert!(v.check_fits(1, 0, "x").is_ok());
    }

    #[test]
    fn very_large_precision_value_fits_when_declared() {
        // 100-digit integer; declared precision 100, scale 0.
        let mut s = String::with_capacity(101);
        s.push('1');
        for _ in 0..99 {
            s.push('0');
        }
        let v = DecimalArbValue::from_str(&s).unwrap();
        assert_eq!(v.integer_digit_count(), 100);
        assert_eq!(v.fractional_digit_count(), 0);
        assert!(v.check_fits(100, 0, "x").is_ok());
        assert!(v.check_fits(99, 0, "x").is_err());
    }

    #[test]
    fn from_bigint_and_scale_matches_from_str() {
        // 12345 with scale 3 = 12.345
        let from_components = DecimalArbValue::from_bigint_and_scale(BigInt::from(12_345), 3);
        let from_str = DecimalArbValue::from_str("12.345").unwrap();
        assert_eq!(from_components, from_str);
    }

    // ------- Canonical byte encoding -------

    #[test]
    fn encoding_round_trips_positive() {
        // 12345.678 at scale 3 = BigInt(12345678) → 0xBC614E (3 bytes)
        let v = DecimalArbValue::from_str("12345.678").unwrap();
        let bytes = v.to_canonical_bytes_at_scale(3);
        assert_eq!(bytes[0], 0x00, "non-negative sign byte");
        let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 3).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn encoding_round_trips_negative() {
        let v = DecimalArbValue::from_str("-12345.678").unwrap();
        let bytes = v.to_canonical_bytes_at_scale(3);
        assert_eq!(bytes[0], 0xFF, "negative sign byte");
        let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 3).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn encoding_round_trips_zero() {
        let v = DecimalArbValue::from_str("0").unwrap();
        let bytes = v.to_canonical_bytes_at_scale(0);
        assert_eq!(bytes, vec![0x00], "zero is single sign byte, no magnitude");
        let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 0).unwrap();
        assert_eq!(v, decoded);
    }

    #[test]
    fn encoding_pads_to_target_scale() {
        // value scale=1, target scale=5: bytes encode 1.00000 = BigInt(100000)
        let v = DecimalArbValue::from_str("1.0").unwrap();
        let bytes = v.to_canonical_bytes_at_scale(5);
        let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 5).unwrap();
        assert_eq!(v, decoded, "padding trailing zeros must round-trip");
    }

    #[test]
    fn encoding_strips_leading_magnitude_zeros() {
        let v = DecimalArbValue::from_str("1").unwrap();
        let bytes = v.to_canonical_bytes_at_scale(0);
        // 1 byte sign + 1 byte magnitude (0x01); no leading 0x00.
        assert_eq!(bytes, vec![0x00, 0x01]);
    }

    #[test]
    fn decoding_rejects_invalid_sign_byte() {
        let bad = vec![0x42_u8, 0x01];
        let err = DecimalArbValue::from_canonical_bytes_at_scale(&bad, 0).unwrap_err();
        assert!(format!("{}", err).contains("sign byte"));
    }

    #[test]
    fn decoding_rejects_negative_zero_encoding() {
        let bad = vec![0xFF_u8]; // sign=neg, no magnitude
        let err = DecimalArbValue::from_canonical_bytes_at_scale(&bad, 0).unwrap_err();
        assert!(format!("{}", err).to_lowercase().contains("negative zero"));
    }

    #[test]
    fn decoding_rejects_empty_bytes() {
        let err = DecimalArbValue::from_canonical_bytes_at_scale(&[], 0).unwrap_err();
        assert!(format!("{}", err).contains("empty"));
    }

    // ------- DecimalArbArrayBuilder + DecimalArbArray -------

    #[test]
    fn builder_round_trips_values_and_nulls() {
        let mut b = DecimalArbArrayBuilder::with_capacity(4, "amount", 100, 18).unwrap();
        b.append_str("12345.678").unwrap();
        b.append_null();
        b.append_str("-99.0").unwrap();
        b.append_str("0").unwrap();
        let arr = b.finish();

        assert_eq!(arr.len(), 4);
        assert_eq!(arr.precision(), 100);
        assert_eq!(arr.scale(), 18);

        assert_eq!(
            arr.value(0).unwrap(),
            Some(DecimalArbValue::from_str("12345.678").unwrap())
        );
        assert!(arr.is_null(1));
        assert_eq!(arr.value(1).unwrap(), None);
        assert_eq!(
            arr.value(2).unwrap(),
            Some(DecimalArbValue::from_str("-99.0").unwrap())
        );
        assert_eq!(
            arr.value(3).unwrap(),
            Some(DecimalArbValue::from_str("0").unwrap())
        );
    }

    #[test]
    fn builder_rejects_value_exceeding_precision() {
        // declare (3, 0); try to append "1234" (4 integer digits).
        let mut b = DecimalArbArrayBuilder::with_capacity(1, "x", 3, 0).unwrap();
        let err = b.append_str("1234").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("'x'"), "error must name the column: {}", msg);
        assert!(
            msg.contains("integer"),
            "error must mention digit count: {}",
            msg
        );
    }

    #[test]
    fn builder_rejects_value_with_too_many_significant_fractional_digits() {
        // declare (5, 1); try to append "1.234" (2 sig fractional > 1).
        let mut b = DecimalArbArrayBuilder::with_capacity(1, "y", 5, 1).unwrap();
        let err = b.append_str("1.234").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("'y'"));
        assert!(msg.contains("scale"));
    }

    #[test]
    fn array_adoption_from_field_validates_metadata() {
        // Build via builder, extract LargeBinaryArray, re-adopt via Field.
        let mut b = DecimalArbArrayBuilder::with_capacity(1, "amount", 50, 4).unwrap();
        b.append_str("3.1416").unwrap();
        let arr = b.finish();
        let (raw, p, s) = arr.into_inner();
        assert_eq!((p, s), (50, 4));

        let field = DecimalArbType::field("amount", 50, 4, true).unwrap();
        let adopted = DecimalArbArray::try_from_array_and_field(raw, &field).unwrap();
        assert_eq!(adopted.precision(), 50);
        assert_eq!(adopted.scale(), 4);
        assert_eq!(
            adopted.value(0).unwrap(),
            Some(DecimalArbValue::from_str("3.1416").unwrap()),
        );
    }

    #[test]
    fn array_adoption_rejects_field_without_metadata() {
        let mut b = DecimalArbArrayBuilder::with_capacity(1, "x", 10, 0).unwrap();
        b.append_str("1").unwrap();
        let (raw, _, _) = b.finish().into_inner();
        let plain = Field::new("x", DataType::LargeBinary, true);
        assert!(DecimalArbArray::try_from_array_and_field(raw, &plain).is_err());
    }

    #[test]
    fn very_large_precision_value_round_trips_through_array() {
        // 100-digit integer at (100, 0).
        let mut s = String::with_capacity(101);
        s.push('1');
        for _ in 0..99 {
            s.push('0');
        }
        let mut b = DecimalArbArrayBuilder::with_capacity(1, "big", 100, 0).unwrap();
        b.append_str(&s).unwrap();
        let arr = b.finish();
        let decoded = arr.value(0).unwrap().unwrap();
        assert_eq!(decoded.to_canonical_string(), s);
    }

    // ------- T011: Arrow array conversions -------

    fn build(column: &str, precision: u32, scale: u32, values: &[Option<&str>]) -> DecimalArbArray {
        let mut b =
            DecimalArbArrayBuilder::with_capacity(values.len(), column, precision, scale).unwrap();
        for v in values {
            match v {
                Some(s) => b.append_str(s).unwrap(),
                None => b.append_null(),
            }
        }
        b.finish()
    }

    #[test]
    fn from_decimal128_widens_losslessly() {
        let src = Decimal128Array::from(vec![Some(1234_i128), None, Some(-9876_i128)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let arr = DecimalArbArray::from_decimal128(&src, 2, 100, 18, "amount").unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(
            arr.value(0).unwrap(),
            Some(DecimalArbValue::from_str("12.34").unwrap())
        );
        assert!(arr.is_null(1));
        assert_eq!(
            arr.value(2).unwrap(),
            Some(DecimalArbValue::from_str("-98.76").unwrap())
        );
    }

    #[test]
    fn from_decimal256_widens_losslessly() {
        // Build a Decimal256 value at scale 5.
        let big = ArrowI256::from_i128(123_456_789_012_i128);
        let src = Decimal256Array::from(vec![Some(big), None])
            .with_precision_and_scale(40, 5)
            .unwrap();
        let arr = DecimalArbArray::from_decimal256(&src, 5, 100, 18, "x").unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(
            arr.value(0).unwrap(),
            Some(DecimalArbValue::from_str("1234567.89012").unwrap())
        );
        assert!(arr.is_null(1));
    }

    #[test]
    fn to_decimal128_narrows_with_validation() {
        let arr = build(
            "x",
            100,
            4,
            &[Some("1.2345"), Some("99999999999999.9999"), None],
        );
        // (precision 38, scale 4) — first value fits, NULL preserved.
        let truncated = arr.to_decimal128(38, 4, "x").unwrap();
        assert_eq!(truncated.len(), 3);
        assert_eq!(truncated.value(0), 12345_i128);
        assert!(truncated.is_null(2));
    }

    #[test]
    fn to_decimal128_rejects_overflow() {
        // 39-digit integer cannot fit Decimal128(38, 0).
        let mut s = String::with_capacity(40);
        s.push('1');
        for _ in 0..38 {
            s.push('0');
        }
        let arr = build("y", 100, 0, &[Some(&s)]);
        let err = arr.to_decimal128(38, 0, "y").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("'y'"));
        assert!(msg.contains("Decimal128"));
    }

    #[test]
    fn to_decimal128_rounds_excess_scale_half_to_even() {
        // 1.235 → at scale=2 with half-to-even, rounds to 1.24 (the round-half-to-even
        // rule rounds 5-with-odd-prev-digit up). Our decimal_arb stores 1.235 then
        // narrows to scale 2.
        let arr = build("x", 10, 4, &[Some("1.2345")]);
        let narrowed = arr.to_decimal128(10, 3, "x").unwrap();
        // 1.2345 rounded to 3 decimals = 1.234 (half-to-even with prev digit 4 stays).
        assert_eq!(narrowed.value(0), 1234_i128);
    }

    #[test]
    fn to_decimal256_handles_76_digit_values() {
        // 75-digit integer fits Decimal256(76, 0).
        let mut s = String::with_capacity(76);
        s.push('1');
        for _ in 0..74 {
            s.push('0');
        }
        let arr = build("x", 100, 0, &[Some(&s)]);
        let narrowed = arr.to_decimal256(76, 0, "x").unwrap();
        assert_eq!(narrowed.len(), 1);
        assert!(!narrowed.is_null(0));
    }

    #[test]
    fn to_decimal256_rejects_77_digit_values() {
        let mut s = String::with_capacity(77);
        s.push('1');
        for _ in 0..76 {
            s.push('0');
        }
        let arr = build("x", 100, 0, &[Some(&s)]);
        let err = arr.to_decimal256(76, 0, "x").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("'x'"));
        assert!(msg.contains("Decimal256"));
    }

    #[test]
    fn to_string_array_emits_canonical_decimal_text() {
        let arr = build(
            "x",
            100,
            4,
            &[Some("1.2345"), None, Some("-0.0001"), Some("0")],
        );
        let strings = arr.to_string_array().unwrap();
        assert_eq!(strings.value(0), "1.2345");
        assert!(strings.is_null(1));
        assert_eq!(strings.value(2), "-0.0001");
        assert_eq!(strings.value(3), "0");
    }

    #[test]
    fn from_string_array_round_trips() {
        let strings = StringArray::from(vec![Some("1.2345"), None, Some("-0.0001"), Some("0")]);
        let arr = DecimalArbArray::from_string_array(&strings, 100, 4, "x").unwrap();
        let back = arr.to_string_array().unwrap();
        assert_eq!(strings.len(), back.len());
        for i in 0..strings.len() {
            assert_eq!(strings.is_null(i), back.is_null(i));
            if !strings.is_null(i) {
                assert_eq!(strings.value(i), back.value(i));
            }
        }
    }

    #[test]
    fn from_string_array_rejects_garbage() {
        let strings = StringArray::from(vec![Some("not-a-number")]);
        assert!(DecimalArbArray::from_string_array(&strings, 10, 0, "x").is_err());
    }

    // ------- T012: sort key encoding (i256-bug regression guard) -------

    #[test]
    fn sort_key_orders_negatives_then_positives() {
        let bytes = |s: &str, scale: u32| {
            DecimalArbValue::from_str(s)
                .unwrap()
                .to_canonical_bytes_at_scale(scale)
        };
        let inputs: Vec<&str> = vec!["100", "-100", "0", "1", "-1", "256", "-256"];
        let mut keys: Vec<(Vec<u8>, &str)> = inputs
            .iter()
            .map(|s| (decimal_arb_to_sort_key(&bytes(s, 0)), *s))
            .collect();
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        let sorted: Vec<&str> = keys.iter().map(|(_, s)| *s).collect();
        // Numeric: -256, -100, -1, 0, 1, 100, 256.
        assert_eq!(sorted, vec!["-256", "-100", "-1", "0", "1", "100", "256"]);
    }

    #[test]
    fn sort_key_orders_long_negative_before_short_negative() {
        // Regression guard for the latent i256-style bug: longer magnitude
        // among negatives means smaller value, must sort first.
        let small_neg = DecimalArbValue::from_str("-1")
            .unwrap()
            .to_canonical_bytes_at_scale(0);
        let large_neg = DecimalArbValue::from_str("-1000000000000")
            .unwrap()
            .to_canonical_bytes_at_scale(0);
        let key_small = decimal_arb_to_sort_key(&small_neg);
        let key_large = decimal_arb_to_sort_key(&large_neg);
        assert!(
            key_large < key_small,
            "more-negative value must produce a smaller sort key"
        );
    }

    #[test]
    fn sort_key_orders_within_same_sign_correctly() {
        let bytes = |s: &str| {
            DecimalArbValue::from_str(s)
                .unwrap()
                .to_canonical_bytes_at_scale(0)
        };
        // Positive: 1 < 2 < 256 < 257
        let k1 = decimal_arb_to_sort_key(&bytes("1"));
        let k2 = decimal_arb_to_sort_key(&bytes("2"));
        let k256 = decimal_arb_to_sort_key(&bytes("256"));
        let k257 = decimal_arb_to_sort_key(&bytes("257"));
        assert!(k1 < k2);
        assert!(k2 < k256);
        assert!(k256 < k257);

        // Negative mirror: -1 > -2 > -256 > -257 (numeric order: -257 < -256 < -2 < -1)
        let kn1 = decimal_arb_to_sort_key(&bytes("-1"));
        let kn2 = decimal_arb_to_sort_key(&bytes("-2"));
        let kn256 = decimal_arb_to_sort_key(&bytes("-256"));
        let kn257 = decimal_arb_to_sort_key(&bytes("-257"));
        assert!(kn257 < kn256);
        assert!(kn256 < kn2);
        assert!(kn2 < kn1);
    }

    // ------- T002/T003: native_int_kind hint -------

    #[test]
    fn native_int_kind_round_trips_through_field_metadata() {
        let base = DecimalArbType::field("gas_used", 78, 0, false).unwrap();
        for kind in [NativeIntKind::U256, NativeIntKind::I256] {
            let stamped = DecimalArbType::with_native_int_kind(base.clone(), kind).unwrap();
            assert_eq!(
                DecimalArbType::native_int_kind_from_field(&stamped),
                Some(kind),
                "stamp+read round-trip for {:?}",
                kind,
            );
            // The hint must not break the existing helpers.
            assert!(DecimalArbType::is_decimal_arb_field(&stamped));
            assert_eq!(
                DecimalArbType::precision_scale_from_field(&stamped),
                Some((78, 0)),
            );
        }
    }

    #[test]
    fn native_int_kind_absent_when_not_stamped() {
        let base = DecimalArbType::field("amount", 100, 18, false).unwrap();
        assert_eq!(DecimalArbType::native_int_kind_from_field(&base), None);
    }

    #[test]
    fn native_int_kind_refused_on_non_decimal_arb_field() {
        let plain = Field::new("blob", DataType::LargeBinary, false);
        let err = DecimalArbType::with_native_int_kind(plain, NativeIntKind::U256).unwrap_err();
        assert!(
            err.to_string().contains("decimal_arb"),
            "error should name decimal_arb: {}",
            err
        );
    }

    #[test]
    fn native_int_kind_parse_is_case_insensitive() {
        assert_eq!(NativeIntKind::parse("u256"), Some(NativeIntKind::U256));
        assert_eq!(NativeIntKind::parse("U256"), Some(NativeIntKind::U256));
        assert_eq!(NativeIntKind::parse(" I256 "), Some(NativeIntKind::I256));
        assert_eq!(NativeIntKind::parse("decimal_arb"), None);
        assert_eq!(NativeIntKind::parse(""), None);
    }

    #[test]
    fn native_int_kind_survives_arrow_ipc_round_trip() {
        use arrow::array::LargeBinaryArray;
        use arrow::ipc::reader::StreamReader;
        use arrow::ipc::writer::StreamWriter;
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Build a one-column batch with a hinted decimal_arb field, write
        // through Arrow IPC, read back, and verify the hint key survives.
        let field = DecimalArbType::with_native_int_kind(
            DecimalArbType::field("amount", 78, 0, true).unwrap(),
            NativeIntKind::U256,
        )
        .unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let array = LargeBinaryArray::from(vec![None as Option<&[u8]>]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let reader = StreamReader::try_new(buf.as_slice(), None).unwrap();
        let out_schema = reader.schema();
        let out_field = out_schema.field(0);
        assert!(DecimalArbType::is_decimal_arb_field(out_field));
        assert_eq!(
            DecimalArbType::native_int_kind_from_field(out_field),
            Some(NativeIntKind::U256),
            "native_int_kind hint must survive Arrow IPC round-trip",
        );
    }
}
