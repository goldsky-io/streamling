use crate::formats::FromArrowConverter;
use crate::formats::json::FromArrowToJsonConverter;
use crate::functions::util::{get_array_from_arg, validate_arg_count};
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::sync::Arc;

use crate::{streamling_err, streamling_user_bail, streamling_user_err};

#[derive(Debug)]
pub struct JsonStringFunc {
    signature: Signature,
}

impl Default for JsonStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonStringFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonStringFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "json_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(streamling_err!("return_field_from_args should be called instead").into())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<arrow_schema::FieldRef> {
        if args.arg_fields.len() != 1 {
            streamling_user_bail!("{} expects exactly 1 argument", self.name());
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        validate_arg_count(&args, 1, None, self.name())?;
        let array = get_array_from_arg(&args.args[0])?;

        // If Utf8/LargeUtf8, produce JSON-quoted strings
        if matches!(array.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            let mut builder = StringBuilder::new();
            let len = array.len();
            for i in 0..len {
                if array.is_null(i) {
                    builder.append_null();
                } else {
                    // Safe to use ScalarValue to extract string representation
                    let v = datafusion::common::ScalarValue::try_from_array(array.as_ref(), i)?;
                    let s = match v {
                        datafusion::common::ScalarValue::Utf8(Some(s)) => s,
                        datafusion::common::ScalarValue::LargeUtf8(Some(s)) => s,
                        _ => unreachable!("type checked above"),
                    };
                    // JSON-quote the string
                    let quoted = serde_json::Value::String(s).to_string();
                    builder.append_value(quoted);
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        // Batch convert entire column at once for efficiency
        let mut builder = StringBuilder::new();
        let field = Field::new("v", array.data_type().clone(), true);
        let schema = Arc::new(Schema::new(vec![field]));
        let converter = FromArrowToJsonConverter::new();

        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()])?;
        let rows = converter.convert_from_batch(&batch)?; // one JSON object per row

        for (i, row_bytes) in rows.iter().enumerate() {
            if i < array.len() && array.is_null(i) {
                builder.append_null();
                continue;
            }
            let json_obj = String::from_utf8(row_bytes.clone())
                .map_err(|e| streamling_user_err!("Invalid UTF8 in JSON: {}", e))?;
            let v: serde_json::Value = serde_json::from_str(&json_obj)
                .map_err(|e| streamling_user_err!("Failed to parse JSON: {}", e))?;
            let Some(value) = v.get("v") else {
                return Err(streamling_err!("Missing field 'v' in JSON object").into());
            };
            builder.append_value(value.to_string());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, ArrayRef, Int32Array, StringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::ScalarFunctionArgs;

    fn make_args_from_array(array: ArrayRef, field_name: &str) -> ScalarFunctionArgs {
        let arg_field = Arc::new(Field::new(field_name, array.data_type().clone(), true));
        let return_field = Arc::new(Field::new("gs_json_string", DataType::Utf8, true));
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array)],
            arg_fields: vec![arg_field],
            number_rows: 1,
            return_field,
        }
    }

    #[test]
    fn test_gs_json_string_int_scalar() {
        let func = JsonStringFunc::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(42)))],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Int64, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("r", DataType::Utf8, true)),
        };
        let out = func.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s.value(0), "42");
    }

    #[test]
    fn test_gs_json_string_utf8_array() {
        let func = JsonStringFunc::new();
        let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("y")]));
        let args = make_args_from_array(arr, "v");
        let out = func.invoke_with_args(args).unwrap();
        let s = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let s = s.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.len(), 3);
        // Strings appear quoted in JSON
        assert_eq!(s.value(0), "\"x\"");
        assert!(s.is_null(1));
        assert_eq!(s.value(2), "\"y\"");
    }

    #[test]
    fn test_gs_json_string_struct() {
        let func = JsonStringFunc::new();
        // Build a struct with fields a:int and b:utf8 for two rows
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(StringArray::from(vec![Some("foo"), Some("bar")]));
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let struct_arr = Arc::new(StructArray::try_new(fields.clone(), vec![a, b], None).unwrap());
        let args = make_args_from_array(struct_arr as ArrayRef, "v");
        let out = func.invoke_with_args(args).unwrap();
        let s = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let s = s.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.len(), 2);
        // Order of fields is deterministic as constructed
        assert_eq!(s.value(0), "{\"a\":1,\"b\":\"foo\"}");
        assert_eq!(s.value(1), "{\"a\":2,\"b\":\"bar\"}");
    }
}
