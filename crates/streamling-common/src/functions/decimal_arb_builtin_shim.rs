//! Permissive front for DataFusion builtins whose argument coercion refuses
//! `decimal_arb` mixed with other types.
//!
//! `SELECT greatest(amount, 0)` failed before any decimal_arb rule could run:
//! `Projection::try_new` type-checks every function call while the SQL is
//! still being planned, and `greatest`'s own coercion has no common type for
//! `LargeBinary` and `Int64`. The `DecimalArbExprRewrite` that knows how to
//! coerce the literal only runs afterwards, in the analyzer. The same
//! sequencing broke `coalesce(amount, 0)`, `abs(amount)`, `array_has(vals, 0)`
//! and friends in the SELECT list, while the identical predicate in `WHERE`
//! planned fine (filters are typed leniently).
//!
//! A shim is registered under the builtin's name. When the builtin accepts
//! the argument types it is used untouched. When it refuses and a
//! `LargeBinary` — decimal_arb's storage — is among them, the shim reports the
//! types as they are so planning proceeds, and the rewrite replaces or
//! re-coerces the call before execution. Should a mixed call still reach
//! execution, the shim fails loudly rather than letting the builtin read bytes.

use crate::streamling_user_err;
use crate::types::decimal_arb::DecimalArbType;
use arrow_schema::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// Is `dt` decimal_arb storage, or a list of it?
/// Is `field` decimal_arb, or a list whose element is?
fn involves_decimal_arb(field: &arrow_schema::Field) -> bool {
    if DecimalArbType::is_decimal_arb_field(field) {
        return true;
    }
    match field.data_type() {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            involves_decimal_arb(f)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::ReturnFieldArgs;

    #[test]
    fn bare_large_binary_fails_at_planning_not_on_the_first_batch() {
        let shim = DecimalArbBuiltinShim::new(datafusion::functions::core::coalesce());
        let bytea: FieldRef = Arc::new(arrow_schema::Field::new("b", DataType::LargeBinary, true));
        let int: FieldRef = Arc::new(arrow_schema::Field::new("i", DataType::Int64, true));
        let fields = vec![bytea, int.clone()];
        let err = shim
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None, None],
            })
            .expect_err("a plain bytea next to an Int64 is the builtin's error to report");
        assert!(err.to_string().contains("coalesce"), "{err}");
        // A decimal_arb argument keeps the provisional field for the rewrite.
        let decimal: FieldRef = Arc::new(DecimalArbType::field("d", 20, 2, true).unwrap());
        let fields = vec![decimal, int];
        let out = shim
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None, None],
            })
            .unwrap();
        assert!(DecimalArbType::is_decimal_arb_field(&out));
    }
}

fn involves_large_binary(dt: &DataType) -> bool {
    match dt {
        DataType::LargeBinary => true,
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            involves_large_binary(f.data_type())
        }
        _ => false,
    }
}

#[derive(Debug)]
pub struct DecimalArbBuiltinShim {
    inner: Arc<ScalarUDF>,
    signature: Signature,
}

impl PartialEq for DecimalArbBuiltinShim {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}
impl Eq for DecimalArbBuiltinShim {}
impl std::hash::Hash for DecimalArbBuiltinShim {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl DecimalArbBuiltinShim {
    pub fn new(inner: Arc<ScalarUDF>) -> Self {
        Self {
            inner,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Shim `inner` as a registrable UDF.
    pub fn wrap(inner: Arc<ScalarUDF>) -> ScalarUDF {
        ScalarUDF::from(Self::new(inner))
    }

    /// The builtin's own verdict on these argument types.
    fn inner_coerce(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let fields: Vec<FieldRef> = arg_types
            .iter()
            .map(|dt| Arc::new(arrow_schema::Field::new("arg", dt.clone(), true)))
            .collect();
        let coerced = datafusion::logical_expr::type_coercion::functions::fields_with_udf(
            &fields,
            self.inner.as_ref(),
        )?;
        Ok(coerced.iter().map(|f| f.data_type().clone()).collect())
    }
}

impl ScalarUDFImpl for DecimalArbBuiltinShim {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match self.inner_coerce(arg_types) {
            Ok(types) => Ok(types),
            // decimal_arb among the arguments: leave the types alone and let
            // the analyzer rewrite coerce them.
            Err(_) if arg_types.iter().any(involves_large_binary) => Ok(arg_types.to_vec()),
            Err(e) => Err(e),
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match self.inner.return_type(arg_types) {
            Ok(dt) => Ok(dt),
            Err(_) if arg_types.iter().any(involves_large_binary) => {
                // Provisional: a decimal_arb (or a list of them) comes out of
                // every shimmed builtin when one goes in. The rewrite replaces
                // the node before this type is relied on.
                Ok(arg_types
                    .iter()
                    .find(|t| involves_large_binary(t))
                    .cloned()
                    .unwrap_or(DataType::LargeBinary))
            }
            Err(e) => Err(e),
        }
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        if self.inner_coerce(&arg_types).is_ok() {
            return self.inner.return_field_from_args(args);
        }
        // Provisional field: a decimal_arb comes out when one goes in, at the
        // first decimal_arb argument's `(precision, scale)` (a list keeps its
        // element field). It exists so the SQL planner can type the enclosing
        // expression — `abs(amount) > 2` routes to `decimal_arb_gt` only if
        // the operand is visibly decimal_arb — and is replaced when the
        // analyzer rewrite substitutes the exact node.
        if let Some(field) = args
            .arg_fields
            .iter()
            .find(|f| DecimalArbType::is_decimal_arb_field(f))
        {
            let element = arrow_schema::Field::new("item", DataType::LargeBinary, true)
                .with_metadata(field.metadata().clone());
            // `make_array` builds a list of its arguments; everything else
            // shimmed returns a value of the argument's kind.
            let data_type = if self.name() == "make_array" {
                DataType::List(Arc::new(element))
            } else {
                DataType::LargeBinary
            };
            return Ok(Arc::new(
                arrow_schema::Field::new(self.name(), data_type, true)
                    .with_metadata(field.metadata().clone()),
            ));
        }
        if !args.arg_fields.iter().any(|f| involves_decimal_arb(f)) {
            // A LargeBinary without decimal_arb metadata (a plain bytea
            // column) is nothing the rewrite will touch, so the builtin's own
            // coercion error is the right answer — at planning time, not on
            // the first batch.
            self.inner_coerce(&arg_types)?;
        }
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            self.return_type(&arg_types)?,
            true,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        if self.inner_coerce(&arg_types).is_err() {
            return Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "{}: argument types {:?} were admitted for a decimal_arb rewrite that did not \
                 happen; refusing to evaluate the builtin on raw decimal_arb bytes",
                    self.name(),
                    arg_types,
                ),
            ));
        }
        self.inner.invoke_with_args(args)
    }
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // The builtin's simplifier may assume its own coercion ran; only
        // consult it once the argument types are ones it accepts.
        let types = args
            .iter()
            .map(|a| info.get_data_type(a))
            .collect::<Result<Vec<_>>>();
        match types {
            Ok(types) if self.inner_coerce(&types).is_ok() => {
                self.inner.inner().simplify(args, info)
            }
            _ => Ok(ExprSimplifyResult::Original(args)),
        }
    }
    fn short_circuits(&self) -> bool {
        self.inner.inner().short_circuits()
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}
