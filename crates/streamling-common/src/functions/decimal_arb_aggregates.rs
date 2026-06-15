//! Aggregate UDFs for the `streamling.decimal_arb` extension type.
//!
//! See `contracts/aggregate-udf-signatures.md` (`data-model.md` E6) for the
//! signatures and widening rules. Each UDAF is registered with the
//! standard SQL aggregate name (`sum`, `min`, `max`, `avg`) — the T007
//! spike confirmed that `register_udaf` with a built-in name overrides
//! the DataFusion default, so authors get the spec's "no transform
//! rewrites" property (FR-007 / FR-020 / SC-006) directly.
//!
//! `count` reuses the DataFusion built-in unchanged — it's `Any`-typed and
//! returns `Int64` for any input.

use crate::types::decimal_arb::{DecimalArbType, DecimalArbValue, MAX_PRECISION};
use crate::{streamling_user_bail, streamling_user_err};
use arrow::array::{Array, ArrayRef, Int64Array, LargeBinaryArray};
use arrow_schema::{Field, FieldRef};
use bigdecimal::{BigDecimal, RoundingMode};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::functions_aggregate::{
    average::avg_udaf,
    min_max::{max_udaf, min_udaf},
    sum::sum_udaf,
};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, GroupsAccumulator, ReversedUDAF,
    SetMonotonicity, Signature, StatisticsArgs, Volatility,
};
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::sync::Arc;

/// Helper: read the input field from accumulator-style args and check whether
/// it carries the decimal_arb extension metadata.
fn input_is_decimal_arb(args: &AccumulatorArgs) -> Result<bool> {
    let field = args.exprs[0].return_field(args.schema)?;
    Ok(DecimalArbType::is_decimal_arb_field(&field))
}

/// Spec rule (E6): SUM widens precision by 16 digits and preserves scale.
/// 16 extra digits supports up to ~10^16 rows in the worst case before
/// hitting MAX_PRECISION; further widening gates on FR-013 overflow.
const SUM_PRECISION_HEADROOM: u32 = 16;

fn sum_output_precision_scale(p: u32, s: u32) -> (u32, u32) {
    let p_out = (p + SUM_PRECISION_HEADROOM).min(MAX_PRECISION);
    (p_out, s)
}

fn avg_output_precision_scale(p: u32, s: u32) -> (u32, u32) {
    // Postgres-style: AVG widens both by 1.
    let p_out = (p + 1).min(MAX_PRECISION);
    let s_out = (s + 1).min(p_out);
    (p_out, s_out)
}

/// Helper: build a Field for an aggregate's output / intermediate state.
fn decimal_arb_field(name: &str, precision: u32, scale: u32) -> Result<FieldRef> {
    let field = DecimalArbType::field(name, precision, scale, true)?;
    Ok(Arc::new(field))
}

/// Decode a `LargeBinary` row at the given scale into an optional value.
fn decode_value(
    array: &LargeBinaryArray,
    idx: usize,
    scale: u32,
) -> Result<Option<DecimalArbValue>> {
    if array.is_null(idx) {
        return Ok(None);
    }
    Ok(Some(DecimalArbValue::from_canonical_bytes_at_scale(
        array.value(idx),
        scale,
    )?))
}

/// Read `(precision, scale)` from an input/state Field.
fn require_decimal_arb(field: &Field, op_name: &str) -> Result<(u32, u32)> {
    DecimalArbType::precision_scale_from_field(field).ok_or_else(|| {
        datafusion::error::DataFusionError::from(streamling_user_err!(
            "{}: input field '{}' is not a streamling.decimal_arb column",
            op_name,
            field.name(),
        ))
    })
}

// =====================================================================
// SUM
// =====================================================================

/// `sum` UDAF wrapper: when the input is `decimal_arb`, the
/// `SumAccumulator` runs; for any other type (Int*, Float*,
/// Decimal128/256, …), the wrapped DataFusion built-in `sum`
/// is delegated to so existing pipelines that aggregate
/// non-decimal_arb columns continue to plan and run.
#[derive(Debug)]
pub struct DecimalArbSumUdaf {
    builtin: Arc<AggregateUDF>,
    signature: Signature,
}

impl Default for DecimalArbSumUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbSumUdaf {
    pub fn new() -> Self {
        Self {
            builtin: sum_udaf(),
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Build the AggregateUDF. Wrap with `register_udaf` under the name `"sum"`
    /// to override the built-in for `decimal_arb` inputs while preserving
    /// the built-in's behavior for every other input type.
    pub fn into_udaf() -> AggregateUDF {
        AggregateUDF::new_from_impl(Self::new())
    }
}

impl AggregateUDFImpl for DecimalArbSumUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "sum"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // We can't distinguish "decimal_arb LargeBinary" from "plain
        // LargeBinary" by DataType alone — field metadata is required.
        // Accept LargeBinary here; the accumulator path validates the
        // extension metadata and surfaces a clear error if absent.
        if matches!(arg_types.first(), Some(DataType::LargeBinary)) {
            Ok(vec![DataType::LargeBinary])
        } else {
            self.builtin.inner().coerce_types(arg_types)
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(arg_types.first(), Some(DataType::LargeBinary)) {
            Ok(DataType::LargeBinary)
        } else {
            self.builtin.inner().return_type(arg_types)
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if DecimalArbType::is_decimal_arb_field(args.input_fields[0].as_ref()) {
            let (p, s) = require_decimal_arb(args.input_fields[0].as_ref(), "decimal_arb sum")?;
            let (p_out, s_out) = sum_output_precision_scale(p, s);
            Ok(vec![decimal_arb_field(
                &format!("{}_state", args.name),
                p_out,
                s_out,
            )?])
        } else {
            self.builtin.inner().state_fields(args)
        }
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if input_is_decimal_arb(&args)? {
            let (p, s) = require_decimal_arb(
                args.exprs[0].return_field(args.schema)?.as_ref(),
                "decimal_arb sum",
            )?;
            let (p_out, s_out) = sum_output_precision_scale(p, s);
            Ok(Box::new(SumAccumulator {
                sum: None,
                input_scale: s,
                output_scale: s_out,
                output_precision: p_out,
            }))
        } else {
            self.builtin.inner().accumulator(args)
        }
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        // The decimal_arb path doesn't have a groups accumulator (use the
        // per-row Accumulator instead); for everything else, defer to the
        // built-in's optimized grouped path.
        match input_is_decimal_arb(&args) {
            Ok(true) => false,
            _ => self.builtin.inner().groups_accumulator_supported(args),
        }
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if input_is_decimal_arb(&args)? {
            // Never reached because groups_accumulator_supported returns
            // false for decimal_arb; but be explicit if invoked anyway.
            streamling_user_bail!(
                "decimal_arb sum does not provide a groups accumulator; \
                 use the per-row Accumulator path"
            )
        }
        self.builtin.inner().create_groups_accumulator(args)
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // The decimal_arb `SumAccumulator` has no `retract_batch`, so it
        // can't power window-frame sliding aggregation. Bail clearly for
        // the decimal_arb branch; for everything else delegate to the
        // built-in (which returns a retract-capable accumulator and
        // avoids O(window_size) recompute per step).
        if input_is_decimal_arb(&args)? {
            streamling_user_bail!("decimal_arb sum does not support sliding-window aggregation")
        }
        self.builtin.inner().create_sliding_accumulator(args)
    }
    // Pass-through delegations for optimizer-relevant signals — these
    // do not depend on field metadata, so always forward to the built-in.
    fn aliases(&self) -> &[String] {
        self.builtin.inner().aliases()
    }
    fn reverse_expr(&self) -> ReversedUDAF {
        self.builtin.inner().reverse_expr()
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.builtin.inner().order_sensitivity()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.builtin.inner().documentation()
    }
    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.builtin.inner().set_monotonicity(data_type)
    }
}

#[derive(Debug)]
struct SumAccumulator {
    sum: Option<BigDecimal>,
    input_scale: u32,
    output_scale: u32,
    output_precision: u32,
}

impl Accumulator for SumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "decimal_arb sum: expected LargeBinary input"
                ))
            })?;
        for i in 0..array.len() {
            if let Some(v) = decode_value(array, i, self.input_scale)? {
                let acc = self.sum.take().unwrap_or_else(|| BigDecimal::from(0i32));
                self.sum = Some(acc + v.into_bigdecimal());
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "decimal_arb sum: state must be LargeBinary"
                ))
            })?;
        for i in 0..array.len() {
            if let Some(v) = decode_value(array, i, self.output_scale)? {
                let acc = self.sum.take().unwrap_or_else(|| BigDecimal::from(0i32));
                self.sum = Some(acc + v.into_bigdecimal());
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.sum.as_ref() {
            None => Ok(ScalarValue::LargeBinary(None)),
            Some(sum) => {
                let v = DecimalArbValue::from_bigdecimal(sum.clone());
                v.check_fits(self.output_precision, self.output_scale, "sum")?;
                let bytes = v.to_canonical_bytes_at_scale(self.output_scale);
                Ok(ScalarValue::LargeBinary(Some(bytes)))
            }
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + 64
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        match self.sum.as_ref() {
            None => Ok(vec![ScalarValue::LargeBinary(None)]),
            Some(sum) => {
                let v = DecimalArbValue::from_bigdecimal(sum.clone());
                let bytes = v.to_canonical_bytes_at_scale(self.output_scale);
                Ok(vec![ScalarValue::LargeBinary(Some(bytes))])
            }
        }
    }
}

// =====================================================================
// MIN / MAX
// =====================================================================

#[derive(Debug, Clone, Copy)]
enum Extreme {
    Min,
    Max,
}

impl Extreme {
    fn name(self) -> &'static str {
        match self {
            Extreme::Min => "min",
            Extreme::Max => "max",
        }
    }
    fn keep(self, current: &BigDecimal, candidate: &BigDecimal) -> bool {
        match self {
            Extreme::Min => candidate < current,
            Extreme::Max => candidate > current,
        }
    }
}

/// `min` / `max` UDAF wrapper: decimal_arb inputs use `ExtremeAccumulator`;
/// any other input type delegates to the wrapped DataFusion built-in.
#[derive(Debug)]
pub struct DecimalArbExtremeUdaf {
    extreme: Extreme,
    builtin: Arc<AggregateUDF>,
    signature: Signature,
}

impl DecimalArbExtremeUdaf {
    fn new(extreme: Extreme) -> Self {
        let builtin = match extreme {
            Extreme::Min => min_udaf(),
            Extreme::Max => max_udaf(),
        };
        Self {
            extreme,
            builtin,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
    pub fn min_udaf() -> AggregateUDF {
        AggregateUDF::new_from_impl(Self::new(Extreme::Min))
    }
    pub fn max_udaf() -> AggregateUDF {
        AggregateUDF::new_from_impl(Self::new(Extreme::Max))
    }
}

impl AggregateUDFImpl for DecimalArbExtremeUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.extreme.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if matches!(arg_types.first(), Some(DataType::LargeBinary)) {
            Ok(vec![DataType::LargeBinary])
        } else {
            self.builtin.inner().coerce_types(arg_types)
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(arg_types.first(), Some(DataType::LargeBinary)) {
            Ok(DataType::LargeBinary)
        } else {
            self.builtin.inner().return_type(arg_types)
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if DecimalArbType::is_decimal_arb_field(args.input_fields[0].as_ref()) {
            let (p, s) = require_decimal_arb(args.input_fields[0].as_ref(), self.name())?;
            Ok(vec![decimal_arb_field(
                &format!("{}_state", args.name),
                p,
                s,
            )?])
        } else {
            self.builtin.inner().state_fields(args)
        }
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if input_is_decimal_arb(&args)? {
            let (p, s) = require_decimal_arb(
                args.exprs[0].return_field(args.schema)?.as_ref(),
                self.name(),
            )?;
            Ok(Box::new(ExtremeAccumulator {
                extreme: self.extreme,
                current: None,
                scale: s,
                precision: p,
            }))
        } else {
            self.builtin.inner().accumulator(args)
        }
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        match input_is_decimal_arb(&args) {
            Ok(true) => false,
            _ => self.builtin.inner().groups_accumulator_supported(args),
        }
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if input_is_decimal_arb(&args)? {
            streamling_user_bail!(
                "decimal_arb {} does not provide a groups accumulator",
                self.name()
            )
        }
        self.builtin.inner().create_groups_accumulator(args)
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if input_is_decimal_arb(&args)? {
            streamling_user_bail!(
                "decimal_arb {} does not support sliding-window aggregation",
                self.name()
            )
        }
        self.builtin.inner().create_sliding_accumulator(args)
    }
    fn aliases(&self) -> &[String] {
        self.builtin.inner().aliases()
    }
    fn reverse_expr(&self) -> ReversedUDAF {
        self.builtin.inner().reverse_expr()
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.builtin.inner().order_sensitivity()
    }
    fn is_descending(&self) -> Option<bool> {
        self.builtin.inner().is_descending()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.builtin.inner().documentation()
    }
    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.builtin.inner().set_monotonicity(data_type)
    }
    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        // For a decimal_arb input, the column's raw-byte statistics are
        // the canonical encoding (sign byte + BE magnitude), which is not
        // the same as decimal_arb numeric ordering — bytewise stats would
        // misclassify negatives. Bail out and let the regular accumulator
        // path compute the answer. For other types delegate to the
        // built-in, which can short-circuit MIN/MAX from precomputed
        // statistics on stats-aware sources.
        if matches!(statistics_args.return_type, DataType::LargeBinary) {
            return None;
        }
        self.builtin.inner().value_from_stats(statistics_args)
    }
}

#[derive(Debug)]
struct ExtremeAccumulator {
    extreme: Extreme,
    current: Option<BigDecimal>,
    scale: u32,
    precision: u32,
}

impl ExtremeAccumulator {
    fn observe(&mut self, candidate: BigDecimal) {
        match &self.current {
            None => self.current = Some(candidate),
            Some(cur) => {
                if self.extreme.keep(cur, &candidate) {
                    self.current = Some(candidate);
                }
            }
        }
    }
}

impl Accumulator for ExtremeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "decimal_arb {}: expected LargeBinary input",
                    self.extreme.name()
                ))
            })?;
        for i in 0..array.len() {
            if let Some(v) = decode_value(array, i, self.scale)? {
                self.observe(v.into_bigdecimal());
            }
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.current.as_ref() {
            None => Ok(ScalarValue::LargeBinary(None)),
            Some(v) => {
                let value = DecimalArbValue::from_bigdecimal(v.clone());
                value.check_fits(self.precision, self.scale, self.extreme.name())?;
                let bytes = value.to_canonical_bytes_at_scale(self.scale);
                Ok(ScalarValue::LargeBinary(Some(bytes)))
            }
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + 64
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        match self.current.as_ref() {
            None => Ok(vec![ScalarValue::LargeBinary(None)]),
            Some(v) => {
                let value = DecimalArbValue::from_bigdecimal(v.clone());
                let bytes = value.to_canonical_bytes_at_scale(self.scale);
                Ok(vec![ScalarValue::LargeBinary(Some(bytes))])
            }
        }
    }
}

// =====================================================================
// AVG
// =====================================================================

/// `avg` UDAF wrapper: decimal_arb inputs use `AvgAccumulator`; any other
/// input type delegates to the wrapped DataFusion built-in.
#[derive(Debug)]
pub struct DecimalArbAvgUdaf {
    builtin: Arc<AggregateUDF>,
    signature: Signature,
}

impl Default for DecimalArbAvgUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbAvgUdaf {
    pub fn new() -> Self {
        Self {
            builtin: avg_udaf(),
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
    pub fn into_udaf() -> AggregateUDF {
        AggregateUDF::new_from_impl(Self::new())
    }
}

impl AggregateUDFImpl for DecimalArbAvgUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "avg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if matches!(arg_types.first(), Some(DataType::LargeBinary)) {
            Ok(vec![DataType::LargeBinary])
        } else {
            self.builtin.inner().coerce_types(arg_types)
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(arg_types.first(), Some(DataType::LargeBinary)) {
            Ok(DataType::LargeBinary)
        } else {
            self.builtin.inner().return_type(arg_types)
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if DecimalArbType::is_decimal_arb_field(args.input_fields[0].as_ref()) {
            let (p, s) = require_decimal_arb(args.input_fields[0].as_ref(), "decimal_arb avg")?;
            // AVG state = (running sum, count). We use the SUM-style headroom on
            // the running sum and an Int64 row counter.
            let (sum_p, sum_s) = sum_output_precision_scale(p, s);
            Ok(vec![
                decimal_arb_field(&format!("{}_sum", args.name), sum_p, sum_s)?,
                Arc::new(Field::new(
                    format!("{}_count", args.name),
                    DataType::Int64,
                    true,
                )),
            ])
        } else {
            self.builtin.inner().state_fields(args)
        }
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if input_is_decimal_arb(&args)? {
            let (p, s) = require_decimal_arb(
                args.exprs[0].return_field(args.schema)?.as_ref(),
                "decimal_arb avg",
            )?;
            let (out_p, out_s) = avg_output_precision_scale(p, s);
            let (sum_p, sum_s) = sum_output_precision_scale(p, s);
            Ok(Box::new(AvgAccumulator {
                sum: BigDecimal::from(0i32),
                count: 0,
                input_scale: s,
                sum_scale: sum_s,
                sum_precision: sum_p,
                output_precision: out_p,
                output_scale: out_s,
            }))
        } else {
            self.builtin.inner().accumulator(args)
        }
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        match input_is_decimal_arb(&args) {
            Ok(true) => false,
            _ => self.builtin.inner().groups_accumulator_supported(args),
        }
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if input_is_decimal_arb(&args)? {
            streamling_user_bail!("decimal_arb avg does not provide a groups accumulator")
        }
        self.builtin.inner().create_groups_accumulator(args)
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Mirrors SUM/MIN/MAX: the decimal_arb `AvgAccumulator` has no
        // `retract_batch`, so it can't power window-frame sliding
        // aggregation — bail clearly instead of falling through to the
        // default non-retracting accumulator. For everything else delegate
        // to the built-in's retract-capable sliding accumulator.
        if input_is_decimal_arb(&args)? {
            streamling_user_bail!("decimal_arb avg does not support sliding-window aggregation")
        }
        self.builtin.inner().create_sliding_accumulator(args)
    }
    fn aliases(&self) -> &[String] {
        // Includes the `MEAN` alias the built-in registers.
        self.builtin.inner().aliases()
    }
    fn reverse_expr(&self) -> ReversedUDAF {
        self.builtin.inner().reverse_expr()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.builtin.inner().documentation()
    }
}

#[derive(Debug)]
struct AvgAccumulator {
    sum: BigDecimal,
    count: i64,
    input_scale: u32,
    sum_scale: u32,
    sum_precision: u32,
    output_precision: u32,
    output_scale: u32,
}

impl Accumulator for AvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "decimal_arb avg: expected LargeBinary input"
                ))
            })?;
        for i in 0..array.len() {
            if let Some(v) = decode_value(array, i, self.input_scale)? {
                self.sum += v.into_bigdecimal();
                self.count += 1;
            }
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            streamling_user_bail!("decimal_arb avg: expected (sum, count) state");
        }
        let sum_arr = states[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "decimal_arb avg: sum state must be LargeBinary"
                ))
            })?;
        let cnt_arr = states[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "decimal_arb avg: count state must be Int64"
                ))
            })?;
        for i in 0..sum_arr.len() {
            if let Some(v) = decode_value(sum_arr, i, self.sum_scale)? {
                self.sum += v.into_bigdecimal();
            }
            if !cnt_arr.is_null(i) {
                self.count += cnt_arr.value(i);
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            return Ok(ScalarValue::LargeBinary(None));
        }
        let avg = (&self.sum / BigDecimal::from(self.count))
            .with_scale_round(self.output_scale as i64, RoundingMode::HalfEven);
        let v = DecimalArbValue::from_bigdecimal(avg);
        v.check_fits(self.output_precision, self.output_scale, "avg")?;
        let bytes = v.to_canonical_bytes_at_scale(self.output_scale);
        Ok(ScalarValue::LargeBinary(Some(bytes)))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + 128
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let v = DecimalArbValue::from_bigdecimal(self.sum.clone());
        // Defensive: allow the state's running sum to fit the SUM-shape
        // precision; if it doesn't the partition will fail and the user can
        // raise declared precision.
        v.check_fits(self.sum_precision, self.sum_scale, "avg_sum")?;
        let bytes = v.to_canonical_bytes_at_scale(self.sum_scale);
        Ok(vec![
            ScalarValue::LargeBinary(Some(bytes)),
            ScalarValue::Int64(Some(self.count)),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decimal_arb::DecimalArbArrayBuilder;
    use std::str::FromStr;
    use std::sync::Arc;

    fn build_input(scale: u32, precision: u32, values: &[Option<&str>]) -> Arc<dyn Array> {
        let mut b =
            DecimalArbArrayBuilder::with_capacity(values.len(), "x", precision, scale).unwrap();
        for v in values {
            match v {
                Some(s) => b.append_str(s).unwrap(),
                None => b.append_null(),
            }
        }
        let (raw, _, _) = b.finish().into_inner();
        Arc::new(raw) as Arc<dyn Array>
    }

    fn make_sum_accumulator(p: u32, s: u32) -> Box<dyn Accumulator> {
        let (p_out, s_out) = sum_output_precision_scale(p, s);
        Box::new(SumAccumulator {
            sum: None,
            input_scale: s,
            output_scale: s_out,
            output_precision: p_out,
        })
    }

    fn make_extreme_accumulator(extreme: Extreme, p: u32, s: u32) -> Box<dyn Accumulator> {
        Box::new(ExtremeAccumulator {
            extreme,
            current: None,
            scale: s,
            precision: p,
        })
    }

    fn make_avg_accumulator(p: u32, s: u32) -> Box<dyn Accumulator> {
        let (out_p, out_s) = avg_output_precision_scale(p, s);
        let (sum_p, sum_s) = sum_output_precision_scale(p, s);
        Box::new(AvgAccumulator {
            sum: BigDecimal::from(0i32),
            count: 0,
            input_scale: s,
            sum_scale: sum_s,
            sum_precision: sum_p,
            output_precision: out_p,
            output_scale: out_s,
        })
    }

    fn unwrap_decimal_arb(v: ScalarValue, scale: u32) -> Option<DecimalArbValue> {
        match v {
            ScalarValue::LargeBinary(None) => None,
            ScalarValue::LargeBinary(Some(bytes)) => {
                Some(DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale).unwrap())
            }
            other => panic!("unexpected scalar: {:?}", other),
        }
    }

    // ---- SUM ----

    #[test]
    fn sum_widens_precision_by_16_and_keeps_scale() {
        // Field metadata check via the UDAF's own state/return shape would
        // require a SessionContext; assert the helper directly.
        assert_eq!(sum_output_precision_scale(100, 18), (116, 18));
        assert_eq!(
            sum_output_precision_scale(MAX_PRECISION, 5),
            (MAX_PRECISION, 5)
        );
    }

    #[test]
    fn sum_returns_null_for_empty_or_all_null_input() {
        let mut acc = make_sum_accumulator(10, 2);
        let v = acc.evaluate().unwrap();
        assert_eq!(v, ScalarValue::LargeBinary(None));

        let mut acc = make_sum_accumulator(10, 2);
        let arr = build_input(2, 10, &[None, None]);
        acc.update_batch(&[arr]).unwrap();
        let v = acc.evaluate().unwrap();
        assert_eq!(v, ScalarValue::LargeBinary(None));
    }

    #[test]
    fn sum_adds_input_rows() {
        let mut acc = make_sum_accumulator(10, 2);
        let arr = build_input(2, 10, &[Some("1.50"), Some("2.50"), None, Some("-1.00")]);
        acc.update_batch(&[arr]).unwrap();
        let (_, s_out) = sum_output_precision_scale(10, 2);
        let result = unwrap_decimal_arb(acc.evaluate().unwrap(), s_out).unwrap();
        assert_eq!(result, DecimalArbValue::from_str("3.00").unwrap());
    }

    #[test]
    fn sum_merge_combines_partial_states() {
        // Mimic two-partition execution: build per-partition sums via state(),
        // then merge into a fresh accumulator.
        let (_, s_out) = sum_output_precision_scale(10, 2);

        let mut acc1 = make_sum_accumulator(10, 2);
        let arr1 = build_input(2, 10, &[Some("1.00"), Some("2.00")]);
        acc1.update_batch(&[arr1]).unwrap();
        let state1 = acc1.state().unwrap();

        let mut acc2 = make_sum_accumulator(10, 2);
        let arr2 = build_input(2, 10, &[Some("3.00"), Some("4.00")]);
        acc2.update_batch(&[arr2]).unwrap();
        let state2 = acc2.state().unwrap();

        let mut combined = make_sum_accumulator(10, 2);
        // Pack the two state ScalarValues into a single LargeBinaryArray and
        // call merge_batch.
        let state_arr = LargeBinaryArray::from_iter_values([&state1[0], &state2[0]].iter().map(
            |sv| match sv {
                ScalarValue::LargeBinary(Some(b)) => b.as_slice(),
                _ => &[],
            },
        ));
        combined.merge_batch(&[Arc::new(state_arr)]).unwrap();
        let result = unwrap_decimal_arb(combined.evaluate().unwrap(), s_out).unwrap();
        assert_eq!(result, DecimalArbValue::from_str("10.00").unwrap());
    }

    // ---- MIN / MAX ----

    #[test]
    fn min_returns_smallest_value() {
        let mut acc = make_extreme_accumulator(Extreme::Min, 10, 0);
        let arr = build_input(
            0,
            10,
            &[Some("5"), Some("-100"), None, Some("3"), Some("-1000")],
        );
        acc.update_batch(&[arr]).unwrap();
        let v = unwrap_decimal_arb(acc.evaluate().unwrap(), 0).unwrap();
        assert_eq!(v, DecimalArbValue::from_str("-1000").unwrap());
    }

    #[test]
    fn max_returns_largest_value() {
        let mut acc = make_extreme_accumulator(Extreme::Max, 10, 2);
        let arr = build_input(2, 10, &[Some("5.00"), Some("-100.50"), Some("3.99")]);
        acc.update_batch(&[arr]).unwrap();
        let v = unwrap_decimal_arb(acc.evaluate().unwrap(), 2).unwrap();
        assert_eq!(v, DecimalArbValue::from_str("5").unwrap());
    }

    #[test]
    fn min_max_return_null_for_empty() {
        let mut acc = make_extreme_accumulator(Extreme::Min, 10, 0);
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::LargeBinary(None));
    }

    // ---- AVG ----

    #[test]
    fn avg_widens_both_precision_and_scale_by_one() {
        assert_eq!(avg_output_precision_scale(10, 2), (11, 3));
        assert_eq!(
            avg_output_precision_scale(MAX_PRECISION, 0),
            (MAX_PRECISION, 1)
        );
    }

    #[test]
    fn avg_returns_null_for_empty_or_all_null() {
        let mut acc = make_avg_accumulator(10, 2);
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::LargeBinary(None));

        let mut acc = make_avg_accumulator(10, 2);
        let arr = build_input(2, 10, &[None, None]);
        acc.update_batch(&[arr]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::LargeBinary(None));
    }

    #[test]
    fn avg_computes_arithmetic_mean_with_widened_scale() {
        let mut acc = make_avg_accumulator(10, 0);
        // (1 + 2 + 3 + 4 + 5) / 5 = 3
        let arr = build_input(
            0,
            10,
            &[Some("1"), Some("2"), Some("3"), Some("4"), Some("5")],
        );
        acc.update_batch(&[arr]).unwrap();
        let (_, s_out) = avg_output_precision_scale(10, 0);
        let v = unwrap_decimal_arb(acc.evaluate().unwrap(), s_out).unwrap();
        // s_out = 1, so result is "3.0"
        assert_eq!(v, DecimalArbValue::from_str("3").unwrap());
    }

    #[test]
    fn avg_rounds_half_to_even_at_widened_scale() {
        // (1 + 2) / 2 = 1.5 — at scale 1 (widened from 0), result is 1.5.
        let mut acc = make_avg_accumulator(10, 0);
        let arr = build_input(0, 10, &[Some("1"), Some("2")]);
        acc.update_batch(&[arr]).unwrap();
        let (_, s_out) = avg_output_precision_scale(10, 0);
        let v = unwrap_decimal_arb(acc.evaluate().unwrap(), s_out).unwrap();
        assert_eq!(v, DecimalArbValue::from_str("1.5").unwrap());
    }
}
