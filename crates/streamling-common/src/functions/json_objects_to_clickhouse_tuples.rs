use crate::functions::util::{downcast_array, get_two_args};
use crate::streamling_user_err;
use arrow_schema::FieldRef;
use datafusion::arrow::array::{Array, ListArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

/// Converts a JSON array of objects into ClickHouse-compatible tuple format.
///
/// Takes a JSON string representing an array of objects and a list of property keys,
/// then extracts only those properties and formats them as ClickHouse tuples.
/// Strings are wrapped in single quotes, nulls and missing keys become 'null',
/// and other values are represented as-is.
///
/// # Arguments
/// * `json_string` - JSON array string like `[{"key": "value", ...}, ...]`
/// * `keys_to_extract` - Array of property names to extract from each object
///
/// # Returns
/// A string in ClickHouse tuple format like `[('value1', null, 123), ...]`
///
/// # Examples
/// * Input: `[{"pubkey": "abc", "signer": true}]`, keys: `["signer"]`
/// * Output: `[(true)]`
///
/// * Input: `[{"pubkey": "abc", "signer": true}]`, keys: `["pubkey"]`
/// * Output: `[('abc')]`
#[derive(Debug)]
pub struct JsonObjectsToClickhouseTuplesFunc {
    signature: Signature,
}

impl Default for JsonObjectsToClickhouseTuplesFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonObjectsToClickhouseTuplesFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Utf8,
                    DataType::List(Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Utf8,
                        false,
                    ))),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonObjectsToClickhouseTuplesFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_gs_json_objects_to_clickhouse_tuples"
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
        let (json_string_array, keys_list_array) = get_two_args::<StringArray, ListArray>(
            &args,
            self.name(),
            "json_string",
            "keys_array",
        )?;

        // Extract the keys once — they're a constant list, the same for every row.
        let keys: Vec<String> = if keys_list_array.is_empty() || keys_list_array.is_null(0) {
            vec![]
        } else {
            let keys_value = keys_list_array.value(0);
            let keys_str_array =
                downcast_array::<StringArray>(&keys_value, "string array for keys")?;
            (0..keys_str_array.len())
                .filter_map(|j| {
                    if keys_str_array.is_null(j) {
                        None
                    } else {
                        Some(keys_str_array.value(j).to_string())
                    }
                })
                .collect()
        };
        let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

        let result_values = (0..json_string_array.len())
            .map(|i| {
                if json_string_array.is_null(i) {
                    return Ok("[]".to_string());
                }
                convert_json_to_clickhouse_tuples(json_string_array.value(i), &keys_refs)
            })
            .collect::<Result<Vec<_>>>()?;

        let result_array = StringArray::from(result_values);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

fn convert_json_to_clickhouse_tuples(json_str: &str, keys_to_extract: &[&str]) -> Result<String> {
    let json_array: Vec<Value> = serde_json::from_str(json_str)
        .map_err(|e| streamling_user_err!("Failed to parse JSON: {}", e))?;

    if json_array.is_empty() {
        return Ok("[]".to_string());
    }

    let tuples = json_array
        .iter()
        .map(|obj| {
            let values = keys_to_extract
                .iter()
                .map(|key| match obj.get(*key) {
                    Some(Value::String(s)) => format!("'{}'", s),
                    Some(Value::Null) | None => "null".to_string(),
                    Some(other) => other.to_string(),
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("({})", values)
        })
        .collect::<Vec<_>>()
        .join(", ");

    Ok(format!("[{}]", tuples))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, ListBuilder, StringBuilder};
    use std::sync::Arc;

    #[test]
    fn test_extract_one_element_correctly() {
        let func = JsonObjectsToClickhouseTuplesFunc::new();

        let json_string = r#"[{"pubkey": "CtvdyHYt8cMuGVHFarV2RADfoCdnrbd8e9jAsB225uMW", "signer": true, "source": "transaction", "writable": true}, {"pubkey": "FLCrbfbwEhFARa8nK9rnZw8BVtKNAuHujh9EhWy5A4U4", "signer": false, "source": "transaction", "writable": true}, {"pubkey": "Vote111111111111111111111111111111111111111", "signer": false, "source": "transaction", "writable": false}]"#;
        let keys_to_extract = vec!["signer"];

        let result = eval_function(&func, json_string, keys_to_extract);
        assert_eq!(result, "[(true), (false), (false)]");
    }

    #[test]
    fn test_do_strings_correctly() {
        let func = JsonObjectsToClickhouseTuplesFunc::new();

        let json_string = r#"[{"pubkey": "CtvdyHYt8cMuGVHFarV2RADfoCdnrbd8e9jAsB225uMW", "signer": true, "source": "transaction", "writable": true}, {"pubkey": "FLCrbfbwEhFARa8nK9rnZw8BVtKNAuHujh9EhWy5A4U4", "signer": false, "source": "transaction", "writable": true}, {"pubkey": "Vote111111111111111111111111111111111111111", "signer": false, "source": "transaction", "writable": false}]"#;
        let keys_to_extract = vec!["pubkey"];

        let result = eval_function(&func, json_string, keys_to_extract);
        assert_eq!(
            result,
            "[('CtvdyHYt8cMuGVHFarV2RADfoCdnrbd8e9jAsB225uMW'), ('FLCrbfbwEhFARa8nK9rnZw8BVtKNAuHujh9EhWy5A4U4'), ('Vote111111111111111111111111111111111111111')]"
        );
    }

    #[test]
    fn test_do_nulls_correctly() {
        let func = JsonObjectsToClickhouseTuplesFunc::new();

        let json_string = r#"[{"signer": true, "source": "transaction", "writable": true}, {"pubkey": null, "signer": false, "source": "transaction", "writable": true}, {"pubkey": "Vote111111111111111111111111111111111111111", "signer": false, "source": "transaction", "writable": false}]"#;
        let keys_to_extract = vec!["pubkey"];

        let result = eval_function(&func, json_string, keys_to_extract);
        assert_eq!(
            result,
            "[(null), (null), ('Vote111111111111111111111111111111111111111')]"
        );
    }

    #[test]
    fn test_handle_null_instead_of_string() {
        let func = JsonObjectsToClickhouseTuplesFunc::new();

        let json_string = None; // null input
        let keys_to_extract = vec!["pubkey"];

        let result = eval_function_with_null(&func, json_string, keys_to_extract);
        assert_eq!(result, "[]");
    }

    #[test]
    fn test_work_when_called_sequentially() {
        let func = JsonObjectsToClickhouseTuplesFunc::new();

        // First call with null
        let json_string = None;
        let keys_to_extract = vec!["pubkey"];
        let result = eval_function_with_null(&func, json_string, keys_to_extract);
        assert_eq!(result, "[]");

        // Second call with actual data
        let json_string = r#"[{"signer": true, "source": "transaction", "writable": true}, {"pubkey": null, "signer": false, "source": "transaction", "writable": true}, {"pubkey": "Vote111111111111111111111111111111111111111", "signer": false, "source": "transaction", "writable": false}]"#;
        let keys_to_extract = vec!["pubkey"];
        let result = eval_function(&func, json_string, keys_to_extract);
        assert_eq!(
            result,
            "[(null), (null), ('Vote111111111111111111111111111111111111111')]"
        );
    }

    // Helper function to evaluate the UDF with a non-null JSON string
    fn eval_function(
        func: &JsonObjectsToClickhouseTuplesFunc,
        json_string: &str,
        keys: Vec<&str>,
    ) -> String {
        let json_array = StringArray::from(vec![json_string]);
        let keys_array = create_string_list(keys);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(json_array)),
                ColumnarValue::Array(keys_array),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("json", DataType::Utf8, false)),
                Arc::new(arrow_schema::Field::new(
                    "keys",
                    DataType::List(Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Utf8,
                        false,
                    ))),
                    false,
                )),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();
            string_array.value(0).to_string()
        } else {
            panic!("Expected array result");
        }
    }

    // Helper function to evaluate the UDF with a null JSON string
    fn eval_function_with_null(
        func: &JsonObjectsToClickhouseTuplesFunc,
        json_string: Option<&str>,
        keys: Vec<&str>,
    ) -> String {
        let json_array = StringArray::from(vec![json_string]);
        let keys_array = create_string_list(keys);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(json_array)),
                ColumnarValue::Array(keys_array),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("json", DataType::Utf8, false)),
                Arc::new(arrow_schema::Field::new(
                    "keys",
                    DataType::List(Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Utf8,
                        false,
                    ))),
                    false,
                )),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();
            string_array.value(0).to_string()
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_scalar_keys_with_multiple_json_rows() {
        let func = JsonObjectsToClickhouseTuplesFunc::new();

        // Multiple JSON rows, single (broadcast) keys array
        let json_array = StringArray::from(vec![
            r#"[{"slot": 1, "confirmation_count": 10}]"#,
            r#"[{"slot": 2, "confirmation_count": 20}]"#,
            r#"[{"slot": 3, "confirmation_count": 30}]"#,
        ]);
        // Keys array has only 1 row (scalar broadcast)
        let keys_array = create_string_list(vec!["confirmation_count", "slot"]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(json_array)),
                ColumnarValue::Array(keys_array),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("json", DataType::Utf8, false)),
                Arc::new(arrow_schema::Field::new(
                    "keys",
                    DataType::List(Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Utf8,
                        false,
                    ))),
                    false,
                )),
            ],
            number_rows: 3,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(string_array.len(), 3);
            assert_eq!(string_array.value(0), "[(10, 1)]");
            assert_eq!(string_array.value(1), "[(20, 2)]");
            assert_eq!(string_array.value(2), "[(30, 3)]");
        } else {
            panic!("Expected array result");
        }
    }

    // Helper to create a list array of strings
    fn create_string_list(values: Vec<&str>) -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new());
        let string_builder = builder.values();
        for value in values {
            string_builder.append_value(value);
        }
        builder.append(true);
        Arc::new(builder.finish())
    }
}
