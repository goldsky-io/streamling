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

use crate::functions::decimal_arb_ops::exact_div_at_scale;
use crate::types::decimal_arb::{DecimalArbType, DecimalArbValue, MAX_PRECISION};
use crate::{streamling_user_bail, streamling_user_err};
use arrow::array::{Array, ArrayRef, Int64Array, LargeBinaryArray, ListArray};
use arrow_schema::{Field, FieldRef};
use bigdecimal::BigDecimal;
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
use std::collections::HashSet;
use std::sync::Arc;

/// Helper: read the input field from accumulator-style args and check whether
/// it carries the decimal_arb extension metadata.
fn input_is_decimal_arb(args: &AccumulatorArgs) -> Result<bool> {
    let field = args.exprs[0].return_field(args.schema)?;
    Ok(DecimalArbType::precision_scale_from_field(&field).is_some())
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

fn is_decimal_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Replicate the built-in `sum` numeric coercion for the non-decimal_arb path.
/// DF54's `sum` encodes coercion in its `Coercible` signature rather than
/// `coerce_types`, so delegating to `builtin.coerce_types` errors with
/// "does not implement coerce_types"; mirror the Postgres-style rules here.
/// Kept in parity with `datafusion-functions-aggregate` 54's `Sum` signature:
/// Null and Duration pass through, decimals are preserved, dictionary/REE wrap
/// integer/float values that coerce (decimal-valued dictionaries stay wrapped).
fn builtin_sum_coerce(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    fn coerced(dt: &DataType) -> Result<DataType> {
        match dt {
            // Typeless NULL flows through unchanged (DF54 short-circuits it).
            DataType::Null => Ok(dt.clone()),
            // Decimal-valued dictionaries/REE stay wrapped (DF54 doesn't decode
            // the Decimal class); integer/float wrappers coerce their value.
            DataType::Dictionary(_, v) if is_decimal_type(v) => Ok(dt.clone()),
            DataType::Dictionary(_, v) => coerced(v),
            DataType::RunEndEncoded(_, v) if is_decimal_type(v.data_type()) => Ok(dt.clone()),
            DataType::RunEndEncoded(_, v) => coerced(v.data_type()),
            DataType::Duration(_) => Ok(dt.clone()),
            d if is_decimal_type(d) => Ok(dt.clone()),
            d if d.is_signed_integer() => Ok(DataType::Int64),
            d if d.is_unsigned_integer() => Ok(DataType::UInt64),
            d if d.is_floating() => Ok(DataType::Float64),
            other => Err(datafusion::error::DataFusionError::from(
                streamling_user_err!("sum is not supported for input type {other:?}"),
            )),
        }
    }
    let Some(arg) = arg_types.first() else {
        return Err(datafusion::error::DataFusionError::from(
            streamling_user_err!("sum expects exactly one argument"),
        ));
    };
    Ok(vec![coerced(arg)?])
}

/// Replicate the built-in `avg` numeric coercion for the non-decimal_arb path
/// (same DF54 caveat as [`builtin_sum_coerce`]): integers/floats → Float64,
/// decimals and durations pass through unchanged, Null flows through, and
/// dictionary/REE wrappers behave as in `Average`'s DF54 signature.
fn builtin_avg_coerce(arg_types: &[DataType]) -> Result<Vec<DataType>> {
    fn coerced(dt: &DataType) -> Result<DataType> {
        match dt {
            DataType::Null => Ok(dt.clone()),
            DataType::Dictionary(_, v) if is_decimal_type(v) => Ok(dt.clone()),
            DataType::Dictionary(_, v) => coerced(v),
            DataType::RunEndEncoded(_, v) if is_decimal_type(v.data_type()) => Ok(dt.clone()),
            DataType::RunEndEncoded(_, v) => coerced(v.data_type()),
            DataType::Duration(_) => Ok(dt.clone()),
            d if is_decimal_type(d) => Ok(dt.clone()),
            d if d.is_integer() || d.is_floating() => Ok(DataType::Float64),
            other => Err(datafusion::error::DataFusionError::from(
                streamling_user_err!("avg is not supported for input type {other:?}"),
            )),
        }
    }
    let Some(arg) = arg_types.first() else {
        return Err(datafusion::error::DataFusionError::from(
            streamling_user_err!("avg expects exactly one argument"),
        ));
    };
    Ok(vec![coerced(arg)?])
}

// =====================================================================
// SUM
// =====================================================================

/// `sum` UDAF wrapper: when the input is `decimal_arb`, the
/// `SumAccumulator` runs; for any other type (Int*, Float*,
/// Decimal128/256, …), the wrapped DataFusion built-in `sum`
/// is delegated to so existing pipelines that aggregate
/// non-decimal_arb columns continue to plan and run.
#[derive(Debug, PartialEq, Eq, Hash)]
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
            builtin_sum_coerce(arg_types)
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(arg_types.first(), Some(DataType::LargeBinary)) {
            Ok(DataType::LargeBinary)
        } else {
            self.builtin.inner().return_type(arg_types)
        }
    }
    /// The output field must carry the decimal_arb `(precision, scale)`.
    ///
    /// Everything downstream — the binary-op planner, the predicate rewrite,
    /// the sort rule, `decimal_arb_to_string`, the JSON/Avro/IPC writers —
    /// recognises decimal_arb by field metadata. Without this override the
    /// default `return_field` built a bare `LargeBinary`, so `SUM(v)` rendered
    /// as hex in JSON, `ORDER BY SUM(v)` sorted bytewise, and
    /// `HAVING SUM(v) > 0` compared raw bytes.
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        // `precision_scale_from_field`, not `is_decimal_arb_field`: a UNION of
        // two scales carries the extension name but no `(p, s)` until the
        // optimizer unifies its inputs, and the schema is recomputed then.
        match arg_fields
            .first()
            .and_then(|f| DecimalArbType::precision_scale_from_field(f.as_ref()))
        {
            Some((p, s)) => {
                let (p_out, s_out) = sum_output_precision_scale(p, s);
                decimal_arb_field(self.name(), p_out, s_out)
            }
            // `LargeBinary` whose `(p, s)` is not known yet (see above): the
            // built-in has no SUM for it, so declare the storage type and let
            // the recomputed schema fill the metadata in.
            _ if matches!(
                arg_fields.first().map(|f| f.data_type()),
                Some(DataType::LargeBinary)
            ) =>
            {
                Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::LargeBinary,
                    true,
                )))
            }
            _ => self.builtin.inner().return_field(arg_fields),
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if DecimalArbType::precision_scale_from_field(args.input_fields[0].as_ref()).is_some() {
            if args.is_distinct {
                return Ok(vec![distinct_state_field(args.name)]);
            }
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
            // `SUM(DISTINCT v)` reaches this accumulator whenever DataFusion's
            // SingleDistinctToGroupBy rewrite does not apply (another aggregate
            // in the same SELECT, FILTER, ORDER BY …). Ignoring `is_distinct`
            // here returned the plain sum with no error.
            if args.is_distinct {
                return Ok(Box::new(DistinctSumAccumulator {
                    values: DistinctValues::default(),
                    input_scale: s,
                    output_scale: s_out,
                    output_precision: p_out,
                }));
            }
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

/// `array_agg` over decimal_arb: DataFusion's accumulator collects the raw
/// `LargeBinary` values as they are, which is exactly right — what it loses is
/// the element field's `(precision, scale)`, because its `return_type` builds
/// the list from the bare data type. Downstream that read as "a list of
/// bytes": the JSON writer printed hex and a later `UNNEST` compared bytes.
/// This wrapper re-declares the output list with the input's element metadata
/// and delegates everything else (state, ordering, DISTINCT, grouping) to the
/// built-in. Any `ORDER BY` inside the call is rewritten to a sort key by
/// `DecimalArbExprRewrite`, so the order the built-in applies is numeric.
pub struct DecimalArbArrayAggUdaf {
    builtin: Arc<AggregateUDF>,
}

impl std::fmt::Debug for DecimalArbArrayAggUdaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecimalArbArrayAggUdaf").finish()
    }
}

impl PartialEq for DecimalArbArrayAggUdaf {
    fn eq(&self, other: &Self) -> bool {
        // Stateless wrapper around one built-in: same name, same function.
        self.name() == other.name()
    }
}
impl Eq for DecimalArbArrayAggUdaf {}
impl std::hash::Hash for DecimalArbArrayAggUdaf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl Default for DecimalArbArrayAggUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbArrayAggUdaf {
    pub fn new() -> Self {
        Self {
            builtin: datafusion::functions_aggregate::array_agg::array_agg_udaf(),
        }
    }

    pub fn into_udaf() -> AggregateUDF {
        AggregateUDF::new_from_impl(Self::new())
    }
}

impl AggregateUDFImpl for DecimalArbArrayAggUdaf {
    fn name(&self) -> &str {
        "array_agg"
    }
    fn signature(&self) -> &Signature {
        self.builtin.inner().signature()
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.builtin.inner().coerce_types(arg_types)
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.builtin.inner().return_type(arg_types)
    }
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let field = self.builtin.inner().return_field(arg_fields)?;
        match arg_fields.first() {
            Some(input) if DecimalArbType::is_decimal_arb_field(input.as_ref()) => {
                let (p, s) = require_decimal_arb(input.as_ref(), "decimal_arb array_agg")?;
                let element = match field.data_type() {
                    DataType::List(e) | DataType::LargeList(e) => e,
                    other => {
                        return Err(datafusion::error::DataFusionError::from(
                            streamling_user_err!(
                                "array_agg over decimal_arb returned {:?}, expected a list",
                                other
                            ),
                        ));
                    }
                };
                let element = Arc::new(DecimalArbType::field(
                    element.name(),
                    p,
                    s,
                    element.is_nullable(),
                )?);
                let data_type = match field.data_type() {
                    DataType::LargeList(_) => DataType::LargeList(element),
                    _ => DataType::List(element),
                };
                Ok(Arc::new(
                    Field::new(field.name(), data_type, field.is_nullable())
                        .with_metadata(field.metadata().clone()),
                ))
            }
            _ => Ok(field),
        }
    }
    fn is_nullable(&self) -> bool {
        self.builtin.inner().is_nullable()
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.builtin.inner().state_fields(args)
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = args.return_field.data_type().clone();
        let inner = self.builtin.inner().accumulator(args)?;
        Ok(Box::new(RetypedAccumulator { inner, data_type }))
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.builtin.inner().groups_accumulator_supported(args)
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let data_type = args.return_field.data_type().clone();
        let inner = self.builtin.inner().create_groups_accumulator(args)?;
        Ok(Box::new(RetypedGroupsAccumulator { inner, data_type }))
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = args.return_field.data_type().clone();
        let inner = self.builtin.inner().create_sliding_accumulator(args)?;
        Ok(Box::new(RetypedAccumulator { inner, data_type }))
    }
    fn aliases(&self) -> &[String] {
        self.builtin.inner().aliases()
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.builtin.inner().order_sensitivity()
    }
    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        // The built-in re-creates itself with the ordering flag; wrap the
        // result again so the metadata override survives.
        let inner = Arc::clone(self.builtin.inner());
        Ok(inner
            .with_beneficial_ordering(beneficial_ordering)?
            .map(|inner| {
                Arc::new(DecimalArbArrayAggUdaf {
                    builtin: Arc::new(AggregateUDF::new_from_shared_impl(inner)),
                }) as Arc<dyn AggregateUDFImpl>
            }))
    }
    fn reverse_expr(&self) -> ReversedUDAF {
        match self.builtin.inner().reverse_expr() {
            ReversedUDAF::Reversed(_) => ReversedUDAF::Reversed(Arc::new(Self::into_udaf())),
            other => other,
        }
    }
    fn supports_null_handling_clause(&self) -> bool {
        self.builtin.inner().supports_null_handling_clause()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.builtin.inner().documentation()
    }
}

/// Same buffers, the declared type: the built-in `array_agg` accumulators
/// build their list from the bare element data type, and DataFusion checks
/// the produced batch against the declared schema with full type equality,
/// element metadata included.
fn retype_array(array: ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    if array.data_type() == data_type {
        return Ok(array);
    }
    let data = array
        .to_data()
        .into_builder()
        .data_type(data_type.clone())
        .build()?;
    Ok(arrow::array::make_array(data))
}

fn retype_scalar(value: ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
    if &value.data_type() == data_type {
        return Ok(value);
    }
    let array = retype_array(value.to_array()?, data_type)?;
    Ok(match value {
        ScalarValue::List(_) => ScalarValue::List(Arc::new(ListArray::from(array.to_data()))),
        ScalarValue::LargeList(_) => ScalarValue::LargeList(Arc::new(
            arrow::array::LargeListArray::from(array.to_data()),
        )),
        other => other,
    })
}

/// Delegating accumulator whose `evaluate` carries the declared output type.
#[derive(Debug)]
struct RetypedAccumulator {
    inner: Box<dyn Accumulator>,
    data_type: DataType,
}

impl Accumulator for RetypedAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        retype_scalar(self.inner.evaluate()?, &self.data_type)
    }
    fn size(&self) -> usize {
        self.inner.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(values)
    }
    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }
}

/// Grouped counterpart of [`RetypedAccumulator`].
struct RetypedGroupsAccumulator {
    inner: Box<dyn GroupsAccumulator>,
    data_type: DataType,
}

impl GroupsAccumulator for RetypedGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }
    fn evaluate(&mut self, emit_to: datafusion::logical_expr::EmitTo) -> Result<ArrayRef> {
        retype_array(self.inner.evaluate(emit_to)?, &self.data_type)
    }
    fn state(&mut self, emit_to: datafusion::logical_expr::EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&arrow::array::BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, opt_filter)
    }
    fn supports_convert_to_state(&self) -> bool {
        self.inner.supports_convert_to_state()
    }
    fn size(&self) -> usize {
        self.inner.size()
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

/// The state field for a `DISTINCT` decimal_arb SUM/AVG: one
/// `List<LargeBinary>` row holding every distinct canonical encoding seen so
/// far, so partitions can be merged. Mirrors DataFusion's own distinct-sum
/// state shape.
fn distinct_state_field(name: &str) -> FieldRef {
    Arc::new(Field::new_list(
        format!("{}_distinct", name),
        Field::new_list_field(DataType::LargeBinary, true),
        false,
    ))
}

/// Distinct non-null values of one decimal_arb column, keyed by canonical
/// bytes. One column has one scale, so one number has exactly one encoding
/// and byte identity is value identity.
#[derive(Debug, Default)]
struct DistinctValues {
    values: HashSet<Vec<u8>>,
}

impl DistinctValues {
    fn update(&mut self, array: &LargeBinaryArray) {
        for i in 0..array.len() {
            if !array.is_null(i) {
                self.values.insert(array.value(i).to_vec());
            }
        }
    }

    /// Fold another partition's serialised state into this one.
    fn merge(&mut self, state: &ArrayRef, op_name: &str) -> Result<()> {
        let lists = state.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            datafusion::error::DataFusionError::from(streamling_user_err!(
                "decimal_arb {}: distinct state must be List<LargeBinary>",
                op_name
            ))
        })?;
        for inner in lists.iter().flatten() {
            let array = inner
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::from(streamling_user_err!(
                        "decimal_arb {}: distinct state items must be LargeBinary",
                        op_name
                    ))
                })?;
            self.update(array);
        }
        Ok(())
    }

    fn state(&self) -> ScalarValue {
        let items: Vec<ScalarValue> = self
            .values
            .iter()
            .map(|bytes| ScalarValue::LargeBinary(Some(bytes.clone())))
            .collect();
        ScalarValue::List(ScalarValue::new_list_nullable(
            &items,
            &DataType::LargeBinary,
        ))
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    /// Exact sum of the distinct values, decoded at `scale`.
    fn sum(&self, scale: u32) -> Result<BigDecimal> {
        let mut acc = BigDecimal::from(0i32);
        for bytes in &self.values {
            acc += DecimalArbValue::from_canonical_bytes_at_scale(bytes, scale)?.into_bigdecimal();
        }
        Ok(acc)
    }

    fn size(&self) -> usize {
        self.values
            .iter()
            .map(|b| b.len() + std::mem::size_of::<Vec<u8>>())
            .sum()
    }
}

fn downcast_input<'a>(values: &'a [ArrayRef], op_name: &str) -> Result<&'a LargeBinaryArray> {
    values[0]
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::from(streamling_user_err!(
                "decimal_arb {}: expected LargeBinary input",
                op_name
            ))
        })
}

/// `SUM(DISTINCT v)` over decimal_arb.
#[derive(Debug)]
struct DistinctSumAccumulator {
    values: DistinctValues,
    input_scale: u32,
    output_scale: u32,
    output_precision: u32,
}

impl Accumulator for DistinctSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.values.update(downcast_input(values, "sum distinct")?);
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.values.merge(&states[0], "sum distinct")
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.len() == 0 {
            return Ok(ScalarValue::LargeBinary(None));
        }
        let v = DecimalArbValue::from_bigdecimal(self.values.sum(self.input_scale)?);
        v.check_fits(self.output_precision, self.output_scale, "sum")?;
        Ok(ScalarValue::LargeBinary(Some(
            v.to_canonical_bytes_at_scale(self.output_scale),
        )))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.values.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.values.state()])
    }
}

// =====================================================================
// MIN / MAX
// =====================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, PartialEq, Eq, Hash)]
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
    /// Keep the input's `(precision, scale)` on the output field — see
    /// `DecimalArbSumUdaf::return_field` for why the metadata matters. This is
    /// also what lets a nested `MAX(MAX(v))` (DataFusion's
    /// SingleDistinctToGroupBy rewrite produces those) recognise its input as
    /// decimal_arb instead of falling back to the built-in bytewise extreme.
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        match arg_fields
            .first()
            .and_then(|f| DecimalArbType::precision_scale_from_field(f.as_ref()))
        {
            Some((p, s)) => decimal_arb_field(self.name(), p, s),
            _ => self.builtin.inner().return_field(arg_fields),
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if DecimalArbType::precision_scale_from_field(args.input_fields[0].as_ref()).is_some() {
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
#[derive(Debug, PartialEq, Eq, Hash)]
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
            builtin_avg_coerce(arg_types)
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(arg_types.first(), Some(DataType::LargeBinary)) {
            Ok(DataType::LargeBinary)
        } else {
            self.builtin.inner().return_type(arg_types)
        }
    }
    /// See `DecimalArbSumUdaf::return_field`.
    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        // `precision_scale_from_field`, not `is_decimal_arb_field`: a UNION of
        // two scales carries the extension name but no `(p, s)` until the
        // optimizer unifies its inputs, and the schema is recomputed then.
        match arg_fields
            .first()
            .and_then(|f| DecimalArbType::precision_scale_from_field(f.as_ref()))
        {
            Some((p, s)) => {
                let (p_out, s_out) = avg_output_precision_scale(p, s);
                decimal_arb_field(self.name(), p_out, s_out)
            }
            _ if matches!(
                arg_fields.first().map(|f| f.data_type()),
                Some(DataType::LargeBinary)
            ) =>
            {
                Ok(Arc::new(Field::new(
                    self.name(),
                    DataType::LargeBinary,
                    true,
                )))
            }
            _ => self.builtin.inner().return_field(arg_fields),
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if DecimalArbType::precision_scale_from_field(args.input_fields[0].as_ref()).is_some() {
            if args.is_distinct {
                return Ok(vec![distinct_state_field(args.name)]);
            }
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
            // See `DecimalArbSumUdaf::accumulator` for why DISTINCT must be
            // honoured here.
            if args.is_distinct {
                return Ok(Box::new(DistinctAvgAccumulator {
                    values: DistinctValues::default(),
                    input_scale: s,
                    output_precision: out_p,
                    output_scale: out_s,
                }));
            }
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
        // Exact quotient with a single half-even rounding at the output scale.
        // `BigDecimal`'s `/` picks its own scale and truncates the fraction of a
        // wide sum before the rounding below could ever see it.
        let avg = exact_div_at_scale(&self.sum, &BigDecimal::from(self.count), self.output_scale);
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

/// `AVG(DISTINCT v)` over decimal_arb.
#[derive(Debug)]
struct DistinctAvgAccumulator {
    values: DistinctValues,
    input_scale: u32,
    output_precision: u32,
    output_scale: u32,
}

impl Accumulator for DistinctAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.values.update(downcast_input(values, "avg distinct")?);
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.values.merge(&states[0], "avg distinct")
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let count = self.values.len();
        if count == 0 {
            return Ok(ScalarValue::LargeBinary(None));
        }
        let sum = self.values.sum(self.input_scale)?;
        let avg = exact_div_at_scale(&sum, &BigDecimal::from(count as i64), self.output_scale);
        let v = DecimalArbValue::from_bigdecimal(avg);
        v.check_fits(self.output_precision, self.output_scale, "avg")?;
        Ok(ScalarValue::LargeBinary(Some(
            v.to_canonical_bytes_at_scale(self.output_scale),
        )))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.values.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.values.state()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decimal_arb::DecimalArbArrayBuilder;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn builtin_sum_coerce_matches_df54_numeric_rules() {
        let cases = [
            (DataType::Int8, DataType::Int64),
            (DataType::Int64, DataType::Int64),
            (DataType::UInt16, DataType::UInt64),
            (DataType::Float32, DataType::Float64),
            (DataType::Decimal128(20, 4), DataType::Decimal128(20, 4)),
            (DataType::Decimal256(60, 8), DataType::Decimal256(60, 8)),
            // Regression (H3): these were rejected before parity fix.
            (DataType::Null, DataType::Null),
            (
                DataType::Duration(arrow_schema::TimeUnit::Second),
                DataType::Duration(arrow_schema::TimeUnit::Second),
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                builtin_sum_coerce(std::slice::from_ref(&input)).unwrap(),
                vec![expected],
                "sum coercion for {input:?}"
            );
        }
        // Dictionary/REE of integer coerce their value; of decimal stay wrapped.
        let dict_i32 = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32));
        assert_eq!(
            builtin_sum_coerce(&[dict_i32]).unwrap(),
            vec![DataType::Int64]
        );
        let dict_dec = DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Decimal128(10, 2)),
        );
        assert_eq!(
            builtin_sum_coerce(std::slice::from_ref(&dict_dec)).unwrap(),
            vec![dict_dec]
        );
    }

    #[test]
    fn builtin_avg_coerce_matches_df54_numeric_rules() {
        assert_eq!(
            builtin_avg_coerce(&[DataType::Int32]).unwrap(),
            vec![DataType::Float64]
        );
        assert_eq!(
            builtin_avg_coerce(&[DataType::Float32]).unwrap(),
            vec![DataType::Float64]
        );
        assert_eq!(
            builtin_avg_coerce(&[DataType::Decimal128(20, 4)]).unwrap(),
            vec![DataType::Decimal128(20, 4)]
        );
        assert_eq!(
            builtin_avg_coerce(&[DataType::Null]).unwrap(),
            vec![DataType::Null]
        );
    }

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
