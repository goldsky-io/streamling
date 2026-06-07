use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility, create_udf,
};
use datafusion::scalar::ScalarValue;

use super::shared::{
    JsonConstructorNullBehavior, build_json_array_row, build_json_object_row,
    canonicalize_json_scalar, finalize_row_evaluation, json_quote, prepare_row_evaluation,
};

pub(crate) fn json_quote_udf() -> ScalarUDF {
    let fun = Arc::new(
        |args: &[ColumnarValue]| -> DataFusionResult<ColumnarValue> {
            if args.len() != 1 {
                return Err(DataFusionError::Execution(format!(
                    "JSON_QUOTE expects exactly one argument, got {}",
                    args.len()
                )));
            }

            match &args[0] {
                ColumnarValue::Scalar(value) => {
                    Ok(ColumnarValue::Scalar(json_quote_scalar(value)?))
                }
                ColumnarValue::Array(array) => Ok(ColumnarValue::Array(json_quote_array(array)?)),
            }
        },
    );

    create_udf(
        "JSON_QUOTE",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        fun,
    )
}

pub(crate) fn json_array_udf(
    name: &'static str,
    aliases: [&'static str; 1],
    null_behavior: JsonConstructorNullBehavior,
) -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonArrayUdf::new(name, null_behavior)).with_aliases(aliases)
}

pub(crate) fn json_object_udf(
    name: &'static str,
    aliases: [&'static str; 1],
    null_behavior: JsonConstructorNullBehavior,
) -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonObjectUdf::new(name, null_behavior)).with_aliases(aliases)
}

pub(crate) fn json_unquote_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonUnquoteUdf::new())
}

pub(crate) fn json_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(JsonLiteralUdf::new())
}

fn json_quote_scalar(value: &ScalarValue) -> DataFusionResult<ScalarValue> {
    match value {
        ScalarValue::Utf8(opt) => Ok(ScalarValue::Utf8(opt.as_ref().map(|val| json_quote(val)))),
        ScalarValue::Null => Ok(ScalarValue::Utf8(None)),
        other => Err(DataFusionError::Execution(format!(
            "JSON_QUOTE supports Utf8 inputs, received scalar of type {other:?}"
        ))),
    }
}

fn json_quote_array(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("Expected Utf8 array".into()))?;
            let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 2);
            for maybe_value in arr.iter() {
                match maybe_value {
                    Some(value) => builder.append_value(json_quote(value)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(DataFusionError::Execution(format!(
            "JSON_QUOTE supports Utf8 arrays, received array of type {other:?}"
        ))),
    }
}

fn json_unquote_scalar(value: &ScalarValue) -> DataFusionResult<ScalarValue> {
    match value {
        ScalarValue::Null => Ok(ScalarValue::Utf8(None)),
        ScalarValue::Utf8(Some(v)) => Ok(ScalarValue::Utf8(Some(unquote_json_string(v)))),
        ScalarValue::Utf8(None) => Ok(ScalarValue::Utf8(None)),
        ScalarValue::Utf8View(Some(v)) => Ok(ScalarValue::Utf8(Some(unquote_json_string(v)))),
        ScalarValue::Utf8View(None) => Ok(ScalarValue::Utf8(None)),
        ScalarValue::LargeUtf8(Some(v)) => Ok(ScalarValue::Utf8(Some(unquote_json_string(v)))),
        ScalarValue::LargeUtf8(None) => Ok(ScalarValue::Utf8(None)),
        other => Err(DataFusionError::Execution(format!(
            "JSON_UNQUOTE expects Utf8 input, received scalar of type {other:?}"
        ))),
    }
}

#[derive(Debug)]
struct JsonArrayUdf {
    signature: Signature,
    name: &'static str,
    null_behavior: JsonConstructorNullBehavior,
}

impl JsonArrayUdf {
    fn new(name: &'static str, null_behavior: JsonConstructorNullBehavior) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
            name,
            null_behavior,
        }
    }
}

impl ScalarUDFImpl for JsonArrayUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "[]".to_string(),
            ))));
        }

        let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;
        let mut builder = StringBuilder::with_capacity(row_count, row_count);

        for row in 0..row_count {
            let mut scalars = Vec::with_capacity(arrays.len());
            for array in &arrays {
                scalars.push(ScalarValue::try_from_array(array.as_ref(), row)?);
            }
            let json = build_json_array_row(&scalars, self.null_behavior)?;
            builder.append_value(json);
        }

        let array: ArrayRef = Arc::new(builder.finish());
        finalize_row_evaluation(all_scalar, array)
    }
}

#[derive(Debug)]
struct JsonObjectUdf {
    signature: Signature,
    name: &'static str,
    null_behavior: JsonConstructorNullBehavior,
}

impl JsonObjectUdf {
    fn new(name: &'static str, null_behavior: JsonConstructorNullBehavior) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
            name,
            null_behavior,
        }
    }
}

impl ScalarUDFImpl for JsonObjectUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if !args.args.len().is_multiple_of(2) {
            return Err(DataFusionError::Execution(format!(
                "{} expects an even number of key/value arguments, got {}",
                self.name,
                args.args.len()
            )));
        }

        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "{}".to_string(),
            ))));
        }

        let (all_scalar, arrays, row_count) = prepare_row_evaluation(&args)?;
        let mut builder = StringBuilder::with_capacity(row_count, row_count);

        for row in 0..row_count {
            let mut keys = Vec::with_capacity(arrays.len() / 2);
            let mut values = Vec::with_capacity(arrays.len() / 2);

            for idx in (0..arrays.len()).step_by(2) {
                keys.push(ScalarValue::try_from_array(arrays[idx].as_ref(), row)?);
                values.push(ScalarValue::try_from_array(arrays[idx + 1].as_ref(), row)?);
            }

            let json = build_json_object_row(&keys, &values, self.null_behavior)?;
            builder.append_value(json);
        }

        let array: ArrayRef = Arc::new(builder.finish());
        finalize_row_evaluation(all_scalar, array)
    }
}

#[derive(Debug)]
struct JsonUnquoteUdf {
    signature: Signature,
}

impl JsonUnquoteUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::String(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonUnquoteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "JSON_UNQUOTE"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "JSON_UNQUOTE expects exactly 1 argument, got {}",
                args.args.len()
            )));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(value) => {
                let scalar = json_unquote_scalar(value)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
            ColumnarValue::Array(array) => {
                let string_array =
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "JSON_UNQUOTE expects Utf8 array for input".to_string(),
                            )
                        })?;

                let mut builder =
                    StringBuilder::with_capacity(string_array.len(), string_array.len());
                for row in 0..string_array.len() {
                    if string_array.is_null(row) {
                        builder.append_null();
                    } else {
                        let value = string_array.value(row);
                        builder.append_value(unquote_json_string(value));
                    }
                }
                let array: ArrayRef = Arc::new(builder.finish());
                finalize_row_evaluation(false, array)
            }
        }
    }
}

#[derive(Debug)]
struct JsonLiteralUdf {
    signature: Signature,
}

impl JsonLiteralUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::String(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonLiteralUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "JSON"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "JSON expects exactly 1 argument, got {}",
                args.args.len()
            )));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(value) => {
                let scalar = canonicalize_json_scalar(value, false)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
            ColumnarValue::Array(array) => {
                let string_array =
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution(
                                "JSON expects Utf8 array for input".to_string(),
                            )
                        })?;

                let mut builder =
                    StringBuilder::with_capacity(string_array.len(), string_array.len());
                for row in 0..string_array.len() {
                    if string_array.is_null(row) {
                        builder.append_null();
                        continue;
                    }

                    match canonicalize_json_scalar(
                        &ScalarValue::Utf8(Some(string_array.value(row).to_string())),
                        false,
                    )? {
                        ScalarValue::Utf8(Some(text)) => builder.append_value(text),
                        ScalarValue::Utf8(None) => builder.append_null(),
                        _ => unreachable!("canonicalize_json_scalar always returns Utf8"),
                    }
                }

                let array: ArrayRef = Arc::new(builder.finish());
                finalize_row_evaluation(false, array)
            }
        }
    }
}

fn unquote_json_string(value: &str) -> String {
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        serde_json::from_str::<String>(value).unwrap_or_else(|_| value.to_string())
    } else {
        value.to_string()
    }
}
