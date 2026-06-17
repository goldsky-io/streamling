use arrow::array::ArrayRef;
use datafusion::arrow::array::{Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::sync::Arc;

use crate::{streamling_user_bail, streamling_user_err};

/// coalesce_meta(arg1, arg2, ...) -> first non-null, preserving first arg Field metadata
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CoalesceMetaUdf {
    signature: Signature,
}

impl CoalesceMetaUdf {
    pub fn new() -> Self {
        // Variadic any to mirror COALESCE behavior
        Self {
            signature: Signature::one_of(vec![TypeSignature::VariadicAny], Volatility::Immutable),
        }
    }
}

impl Default for CoalesceMetaUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for CoalesceMetaUdf {
    fn name(&self) -> &str {
        "coalesce_meta"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            streamling_user_bail!("coalesce_meta requires at least one argument");
        }
        Ok(arg_types[0].clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Preserve first argument field (including metadata)
        if args.arg_fields.is_empty() {
            streamling_user_bail!("coalesce_meta requires at least one argument");
        }
        Ok(args.arg_fields[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            streamling_user_bail!("coalesce_meta requires at least one argument");
        }

        // Convert scalars to arrays where needed
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(args.args.len());
        let mut data_type: Option<DataType> = None;
        for (idx, arg) in args.args.iter().enumerate() {
            let array = match arg {
                ColumnarValue::Array(arr) => arr.clone(),
                ColumnarValue::Scalar(s) => s.to_array()?,
            };
            if idx == 0 {
                data_type = Some(array.data_type().clone());
            } else {
                // Ensure all args have same type
                let expected = data_type.as_ref().unwrap();
                if array.data_type() != expected {
                    streamling_user_bail!(
                        "coalesce_meta requires all arguments to have identical types: expected {:?}, got {:?}",
                        expected,
                        array.data_type()
                    );
                }
            }
            arrays.push(array);
        }

        let dt = data_type.expect("at least one arg guaranteed");
        match dt {
            DataType::FixedSizeBinary(32) => {
                // Coalesce element-wise for FixedSizeBinary(32)
                let len = arrays[0].len();
                let mut builder = FixedSizeBinaryBuilder::with_capacity(len, 32);
                for i in 0..len {
                    let mut wrote = false;
                    for arr in &arrays {
                        let a = arr
                            .as_any()
                            .downcast_ref::<FixedSizeBinaryArray>()
                            .ok_or_else(|| {
                                streamling_user_err!(
                                    "coalesce_meta: all arguments must be FixedSizeBinary(32)"
                                )
                            })?;
                        if a.is_valid(i) {
                            let val = a.value(i);
                            let _ = builder.append_value(val);
                            wrote = true;
                            break;
                        }
                    }
                    if !wrote {
                        builder.append_null();
                    }
                }
                let out: ArrayRef = Arc::new(builder.finish());
                Ok(ColumnarValue::Array(out))
            }
            _ => {
                // Fallback: use Arrow's compute coalesce if available in this version
                // Otherwise, error to avoid silent type mismatches
                Err(streamling_user_err!(
                    "coalesce_meta currently supports only FixedSizeBinary(32); got {:?}",
                    dt
                )
                .into())
            }
        }
    }
}
