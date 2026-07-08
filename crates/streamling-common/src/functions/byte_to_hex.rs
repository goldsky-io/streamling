use crate::functions::util::unary_binary_to_string;
use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// Converts byte arrays to hexadecimal strings.
///
/// Takes a byte array and converts it to a hexadecimal string representation.
/// Returns null for null inputs.
///
/// # Arguments
/// * `bytes` - The byte array to convert
///
/// # Returns
/// A hexadecimal string representation of the bytes
///
/// # Examples
/// * `byte_to_hex([72, 101, 108, 108, 111])` returns `'48656c6c6f'` (hex for "Hello")
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ByteToHexFunc {
    signature: Signature,
}

impl Default for ByteToHexFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ByteToHexFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ByteToHexFunc {
    fn name(&self) -> &str {
        "_gs_byte_to_hex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::Utf8,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        unary_binary_to_string(&args, self.name(), |bytes| hex::encode(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BinaryArray, StringArray};

    #[test]
    fn test_byte_to_hex() {
        let func = ByteToHexFunc::new();

        let binary_values: Vec<Option<&[u8]>> = vec![
            Some(b"Hello"),
            Some(&[0xde, 0xad, 0xbe, 0xef]),
            Some(&[]),
            None,
        ];
        let binary_array = BinaryArray::from(binary_values);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(binary_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "bytes",
                DataType::Binary,
                false,
            ))],
            number_rows: 4,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();

            // "Hello" to hex
            assert_eq!(string_array.value(0), "48656c6c6f");

            // Bytes to hex
            assert_eq!(string_array.value(1), "deadbeef");

            // Empty bytes to empty string
            assert_eq!(string_array.value(2), "");

            // Null returns null
            assert!(string_array.is_null(3));
        } else {
            panic!("Expected array result");
        }
    }
}
