use crate::functions::util::unary_binary_to_string;
use arrow_schema::FieldRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// Encodes byte arrays to Base58 strings.
///
/// Takes a byte array and converts it to its Base58 string representation.
/// This is the inverse of `from_base58`. Returns null for null inputs.
///
/// # Arguments
/// * `bytes` - The byte array to encode
///
/// # Returns
/// A Base58 string representation of the bytes
///
/// # Examples
/// * `to_base58([72, 101, 108, 108, 111])` returns `'9Ajdvzr'` (Base58 for "Hello")
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToBase58Func {
    signature: Signature,
}

impl Default for ToBase58Func {
    fn default() -> Self {
        Self::new()
    }
}

impl ToBase58Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToBase58Func {
    fn name(&self) -> &str {
        "_gs_to_base58"
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
        unary_binary_to_string(&args, self.name(), |bytes| {
            bs58::encode(bytes).into_string()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BinaryArray, StringArray};

    fn invoke(values: Vec<Option<&[u8]>>) -> StringArray {
        let func = ToBase58Func::new();
        let len = values.len();
        let binary_array = BinaryArray::from(values);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(binary_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "bytes",
                DataType::Binary,
                false,
            ))],
            number_rows: len,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            result_array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone()
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_to_base58_basic() {
        let string_array = invoke(vec![
            Some(b"Hello"),
            Some(&[]),
            None,
            // Leading zero bytes encode as leading '1's
            Some(&[0, 0, 0xde, 0xad]),
        ]);

        // "Hello" to Base58
        assert_eq!(string_array.value(0), "9Ajdvzr");

        // Empty bytes to empty string
        assert_eq!(string_array.value(1), "");

        // Null returns null
        assert!(string_array.is_null(2));

        // Leading zero bytes become leading '1's
        assert_eq!(string_array.value(3), "11Hwr");
    }

    #[test]
    fn test_to_base58_known_vector() {
        // 32-byte value (e.g. a Solana account key) with a known Base58 encoding
        let bytes: [u8; 32] = [1; 32];
        let string_array = invoke(vec![Some(&bytes)]);
        assert_eq!(string_array.value(0), bs58::encode(&bytes).into_string(),);
        assert_eq!(
            string_array.value(0),
            "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi"
        );
    }

    #[test]
    fn test_to_base58_round_trip_with_from_base58() {
        let original: &[u8] = &[0, 1, 2, 3, 0xff, 0xde, 0xad, 0xbe, 0xef];
        let string_array = invoke(vec![Some(original)]);
        let encoded = string_array.value(0);

        // from_base58 decodes back to the original bytes
        let decoded = bs58::decode(encoded).into_vec().unwrap();
        assert_eq!(decoded.as_slice(), original);

        let input = StringArray::from(vec![Some(encoded)]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = crate::functions::from_base58::from_base58_impl(&args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let binary_array = result_array.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(binary_array.value(0), original);
        } else {
            panic!("Expected array result");
        }
    }
}
