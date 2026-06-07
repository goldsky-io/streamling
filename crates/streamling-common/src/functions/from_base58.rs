use crate::functions::util::unary_string_to_binary;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, Volatility, create_udf,
};
use std::sync::Arc;

/// Decodes Base58 strings to byte arrays.
///
/// Takes a Base58-encoded string and converts it to the corresponding byte array.
/// Returns an empty byte array for null inputs or invalid Base58 strings.
///
/// # Arguments
/// * `base58_string` - The Base58-encoded string to decode
///
/// # Returns
/// A byte array representing the decoded Base58 string
///
/// # Examples
/// * `from_base58('3yZe7d')` returns the corresponding byte array
/// * `from_base58('invalid!')` returns an empty byte array
/// * `from_base58(NULL)` returns an empty byte array
pub fn from_base58_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Convert ScalarFunctionArgs format to what unary_string_to_binary expects
    let args_wrapper = ScalarFunctionArgs {
        args: args.to_vec(),
        arg_fields: vec![Arc::new(arrow_schema::Field::new(
            "base58",
            DataType::Utf8,
            false,
        ))],
        number_rows: match &args[0] {
            ColumnarValue::Array(arr) => arr.len(),
            ColumnarValue::Scalar(_) => 1,
        },
        return_field: Arc::new(arrow_schema::Field::new("result", DataType::Binary, false)),
    };

    unary_string_to_binary(&args_wrapper, "from_base58", |base58_str| {
        bs58::decode(base58_str).into_vec().unwrap_or_default()
    })
}

/// Creates a ScalarUDF for from_base58 using create_udf
/// This is the simplified approach recommended for simple transformations
pub fn create_from_base58_udf() -> ScalarUDF {
    create_udf(
        "_gs_from_base58",
        vec![DataType::Utf8], // input types
        DataType::Binary,     // return type
        Volatility::Immutable,
        Arc::new(from_base58_impl),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BinaryArray, StringArray};

    #[test]
    fn test_base58_decode_basic() {
        // Test basic decoding using the core implementation directly
        let string_array = StringArray::from(vec![
            Some("3yZe7d"),          // "test" in base58
            Some("Cn8eVZg"),         // "hello" in base58
            Some("StV1DL6CwTryKyV"), // "hello world" in base58
            None,                    // null input
        ]);

        let args = vec![ColumnarValue::Array(Arc::new(string_array))];
        let result = from_base58_impl(&args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let binary_array = result_array.as_any().downcast_ref::<BinaryArray>().unwrap();

            // "test" decoded
            assert_eq!(binary_array.value(0), b"test");

            // "hello" decoded
            assert_eq!(binary_array.value(1), b"hello");

            // "hello world" decoded
            assert_eq!(binary_array.value(2), b"hello world");

            // Null returns empty
            assert_eq!(binary_array.value(3).len(), 0);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_base58_decode_solana_memo() {
        // Test with the actual Solana memo data from the conversation
        let memo_data = "P7S1xSLSi6hwpoDNi99khRHSNhWmm1kHixjWySCsb3REb9ydJFmDeVuNHgRJ6xiMfkgoUY97LDYbYMP12yTgk46oTuBFx8DXZe1QwCUPh3v7UcoYJ2MkZ3fHoseH1DXRpxLWcViPwUgC36Z1FLWHdRvdk3n1ce4ZA4c2rY7ErTiw9k8hv5HHGcThzuD";
        let expected_memo = "BONKbot MEV-Protect transaction priority tip for 31GmGasHRJuaaSFTp5mCXYuQzbQ5BXR9JLk27MVF5vvYTaRhUpurP4GA4Vb9uD9ySt5LfRkRThJ38acSJUnPnWYX";

        let string_array = StringArray::from(vec![Some(memo_data)]);
        let args = vec![ColumnarValue::Array(Arc::new(string_array))];
        let result = from_base58_impl(&args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let binary_array = result_array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let decoded_bytes = binary_array.value(0);
            let decoded_string = String::from_utf8(decoded_bytes.to_vec()).unwrap();
            assert_eq!(decoded_string, expected_memo);
        } else {
            panic!("Expected array result");
        }
    }
}
