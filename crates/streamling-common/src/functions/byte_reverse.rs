use arrow::array::{Array, FixedSizeBinaryArray};
use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

use crate::streamling_user_err;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReverseBytes32Func {
    signature: Signature,
}

impl Default for ReverseBytes32Func {
    fn default() -> Self {
        Self::new()
    }
}

impl ReverseBytes32Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::FixedSizeBinary(32)], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ReverseBytes32Func {
    fn name(&self) -> &str {
        "reverse_bytes32"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::FixedSizeBinary(32))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Preserve original field metadata
        let input_field = args.arg_fields[0].clone();
        Ok(Arc::new(
            arrow_schema::Field::new(
                self.name(),
                DataType::FixedSizeBinary(32),
                input_field.is_nullable(),
            )
            .with_metadata(input_field.metadata().clone()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(streamling_user_err!("reverse_bytes32 expects one argument").into());
        }
        let arr = match &args.args[0] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| -> DataFusionError {
                streamling_user_err!("reverse_bytes32 expects FixedSizeBinary(32)").into()
            })?;
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(fsb.len());
        for i in 0..fsb.len() {
            if fsb.is_null(i) {
                out.push(None);
            } else {
                let mut v = fsb.value(i).to_vec();
                v.reverse();
                out.push(Some(v));
            }
        }
        let out_arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(out.into_iter(), 32)?;
        Ok(ColumnarValue::Array(Arc::new(out_arr)))
    }
}
