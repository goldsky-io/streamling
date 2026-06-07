use datafusion::arrow::array::ArrayRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;

pub(super) fn prepare_row_evaluation(
    args: &ScalarFunctionArgs,
) -> DataFusionResult<(bool, Vec<ArrayRef>, usize)> {
    let all_scalar = args
        .args
        .iter()
        .all(|value| matches!(value, ColumnarValue::Scalar(_)));
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays.first().map(|array| array.len()).unwrap_or(1);
    Ok((all_scalar, arrays, row_count))
}

pub(super) fn finalize_row_evaluation(
    all_scalar: bool,
    array: ArrayRef,
) -> DataFusionResult<ColumnarValue> {
    if all_scalar {
        let scalar = ScalarValue::try_from_array(array.as_ref(), 0)?;
        Ok(ColumnarValue::Scalar(scalar))
    } else {
        Ok(ColumnarValue::Array(array))
    }
}

pub(super) fn string_scalar_to_owned(
    func: &str,
    arg: &str,
    value: &ScalarValue,
) -> DataFusionResult<Option<String>> {
    match value {
        ScalarValue::Utf8(opt) | ScalarValue::Utf8View(opt) | ScalarValue::LargeUtf8(opt) => {
            Ok(opt.clone())
        }
        ScalarValue::Dictionary(_, inner) => string_scalar_to_owned(func, arg, inner),
        ScalarValue::Null => Ok(None),
        other => Err(DataFusionError::Execution(format!(
            "{func} expects {arg} to be a string or NULL, received {other:?}"
        ))),
    }
}

pub(super) fn scalar_to_utf8_optional(
    _func: &str,
    _arg: &str,
    value: &ScalarValue,
) -> DataFusionResult<Option<String>> {
    match value {
        ScalarValue::Utf8(opt) => Ok(opt.clone()),
        ScalarValue::Utf8View(opt) => Ok(opt.clone()),
        ScalarValue::LargeUtf8(opt) => Ok(opt.clone()),
        ScalarValue::Null => Ok(None),
        other => {
            if other.is_null() {
                Ok(None)
            } else {
                Ok(Some(other.to_string()))
            }
        }
    }
}

pub(super) fn integer_scalar_to_i64(
    func: &str,
    arg: &str,
    value: &ScalarValue,
) -> DataFusionResult<Option<i64>> {
    let result = match value {
        ScalarValue::Null => None,
        ScalarValue::Int8(opt) => opt.map(|v| v as i64),
        ScalarValue::Int16(opt) => opt.map(|v| v as i64),
        ScalarValue::Int32(opt) => opt.map(|v| v as i64),
        ScalarValue::Int64(opt) => *opt,
        ScalarValue::UInt8(opt) => opt.map(|v| v as i64),
        ScalarValue::UInt16(opt) => opt.map(|v| v as i64),
        ScalarValue::UInt32(opt) => opt.map(|v| v as i64),
        ScalarValue::UInt64(opt) => match opt.as_ref().copied() {
            Some(v) if v <= i64::MAX as u64 => Some(v as i64),
            Some(v) => {
                return Err(DataFusionError::Execution(format!(
                    "{func} received {arg} that exceeds i64 range: {v}"
                )));
            }
            None => None,
        },
        ScalarValue::Dictionary(_, inner) => return integer_scalar_to_i64(func, arg, inner),
        other => {
            return Err(DataFusionError::Execution(format!(
                "{func} expects {arg} to be an integer, received {other:?}"
            )));
        }
    };

    Ok(result)
}
