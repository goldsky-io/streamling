use crate::functions::util::unary_string_to_binary;
use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Converts hexadecimal strings to byte arrays.
///
/// Takes a hexadecimal string and converts it to a byte array.
/// Returns an empty byte array for null inputs or invalid hex strings.
///
/// # Arguments
/// * `hex_string` - The hexadecimal string to convert
///
/// # Returns
/// A byte array representing the hex string
///
/// # Examples
/// * `hex_to_byte('48656c6c6f')` returns bytes for "Hello"
/// * `hex_to_byte('deadbeef')` returns the corresponding bytes
#[derive(Debug)]
pub struct HexToByteFunc {
    signature: Signature,
}

impl Default for HexToByteFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl HexToByteFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HexToByteFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_gs_hex_to_byte"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::Binary,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        unary_string_to_binary(&args, self.name(), |hex_str| {
            // Remove 0x prefix if present
            let clean_hex = if hex_str.starts_with("0x") || hex_str.starts_with("0X") {
                &hex_str[2..]
            } else {
                hex_str
            };

            hex::decode(clean_hex).unwrap_or_default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BinaryArray, StringArray};

    #[test]
    fn test_hex_to_byte() {
        let func = HexToByteFunc::new();

        let string_array = StringArray::from(vec![
            Some("48656c6c6f"),
            Some("0xdeadbeef"),
            Some("invalid"),
            None,
        ]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(string_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "hex",
                DataType::Utf8,
                false,
            ))],
            number_rows: 4,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Binary, false)),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let binary_array = result_array.as_any().downcast_ref::<BinaryArray>().unwrap();

            // "Hello" in bytes
            assert_eq!(binary_array.value(0), b"Hello");

            // 0xdeadbeef decoded
            assert_eq!(binary_array.value(1), &[0xde, 0xad, 0xbe, 0xef]);

            // Invalid hex returns empty
            assert_eq!(binary_array.value(2).len(), 0);

            // Null returns empty
            assert_eq!(binary_array.value(3).len(), 0);
        } else {
            panic!("Expected array result");
        }
    }
}
