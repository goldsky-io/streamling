//! `watermark(column)` in a table's window: the largest value of `column` the
//! table has stored. Simplifying folds it to that value, the way it folds
//! `now()` to the current time, so each GC pass reads the current watermark.

use crate::{streamling_err, streamling_user_err};
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub const WATERMARK_NAME: &str = "watermark";

/// Column name to the largest non-null value stored, or a typed null before
/// one is. Never decreases.
pub(super) type Watermarks = Arc<Mutex<HashMap<String, ScalarValue>>>;

/// Registered only in a window's session, so `watermark()` is unknown
/// everywhere else.
#[derive(Debug)]
pub(super) struct WatermarkFunc {
    watermarks: Watermarks,
    signature: Signature,
}

// Same identity rule as `LookupEqFunc`: the watermarks are runtime state, so
// the UDF is identified by its signature.
impl PartialEq for WatermarkFunc {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}
impl Eq for WatermarkFunc {}
impl std::hash::Hash for WatermarkFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl WatermarkFunc {
    pub fn new(watermarks: Watermarks) -> Self {
        Self {
            watermarks,
            // Volatile: only `simplify` may fold it, never constant evaluation.
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for WatermarkFunc {
    fn name(&self) -> &str {
        WATERMARK_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            streamling_user_err!("{}() takes one column of the table", WATERMARK_NAME).into()
        })
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [Expr::Column(column)] = args.as_slice() else {
            return Err(
                streamling_user_err!("{}() takes one column of the table", WATERMARK_NAME).into(),
            );
        };
        let value = self
            .watermarks
            .lock()
            .get(&column.name)
            .cloned()
            .ok_or_else(|| {
                streamling_err!(
                    "{}: no watermark for column '{}'",
                    WATERMARK_NAME,
                    column.name
                )
            })?;
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(value, None)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(streamling_err!(
            "{}() must be folded to a literal before evaluation",
            WATERMARK_NAME
        )
        .into())
    }
}
