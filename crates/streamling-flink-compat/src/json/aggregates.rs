use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Signature, Volatility};
use datafusion::scalar::ScalarValue;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl};
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::shared::{JsonConstructorNullBehavior, scalar_to_json_key, scalar_to_json_value};

pub(crate) fn json_arrayagg_udaf(
    name: &'static str,
    null_behavior: JsonConstructorNullBehavior,
    aliases: [&'static str; 1],
) -> AggregateUDF {
    let udf = AggregateUDF::new_from_impl(JsonArrayAggUdf::new(name, null_behavior));
    udf.with_aliases(aliases)
}

pub(crate) fn json_objectagg_udaf(
    name: &'static str,
    null_behavior: JsonConstructorNullBehavior,
    aliases: [&'static str; 1],
) -> AggregateUDF {
    let udf = AggregateUDF::new_from_impl(JsonObjectAggUdf::new(name, null_behavior));
    udf.with_aliases(aliases)
}

#[derive(Debug)]
struct JsonArrayAggUdf {
    signature: Signature,
    name: &'static str,
    null_behavior: JsonConstructorNullBehavior,
}

impl JsonArrayAggUdf {
    fn new(name: &'static str, null_behavior: JsonConstructorNullBehavior) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            name,
            null_behavior,
        }
    }
}

impl AggregateUDFImpl for JsonArrayAggUdf {
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

    fn state_fields(&self, args: StateFieldsArgs) -> DataFusionResult<Vec<FieldRef>> {
        let field = Field::new(format!("{}_state", args.name), DataType::Utf8, true);
        Ok(vec![Arc::new(field)])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return Err(DataFusionError::Execution(
                "JSON_ARRAYAGG does not support DISTINCT clauses".to_string(),
            ));
        }
        if !acc_args.order_bys.is_empty() {
            return Err(DataFusionError::Execution(
                "JSON_ARRAYAGG does not yet support ORDER BY clauses".to_string(),
            ));
        }
        if acc_args.ignore_nulls {
            return Err(DataFusionError::Execution(
                "JSON_ARRAYAGG does not support IGNORE NULLS".to_string(),
            ));
        }
        Ok(Box::new(JsonArrayAggAccumulator::new(matches!(
            self.null_behavior,
            JsonConstructorNullBehavior::NullOnNull
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }
}

#[derive(Debug)]
struct JsonArrayAggAccumulator {
    values: Vec<JsonValue>,
    include_nulls: bool,
}

impl JsonArrayAggAccumulator {
    fn new(include_nulls: bool) -> Self {
        Self {
            values: Vec::new(),
            include_nulls,
        }
    }
}

impl Accumulator for JsonArrayAggAccumulator {
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        if self.values.is_empty() {
            Ok(vec![ScalarValue::Utf8(None)])
        } else {
            Ok(vec![ScalarValue::Utf8(Some(
                JsonValue::Array(self.values.clone()).to_string(),
            ))])
        }
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let array = &values[0];
        for row in 0..array.len() {
            let scalar = ScalarValue::try_from_array(array.as_ref(), row)?;
            if scalar.is_null() {
                if self.include_nulls {
                    self.values.push(JsonValue::Null);
                }
                continue;
            }
            let json = scalar_to_json_value(&scalar)?;
            self.values.push(json);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        let array = states
            .first()
            .and_then(|arr| arr.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Execution("JSON_ARRAYAGG state must be Utf8".to_string())
            })?;

        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let state_value = array.value(row);
            let parsed = parse_json_array_state(state_value)?;
            self.values.extend(parsed);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        if self.values.is_empty() {
            Ok(ScalarValue::Utf8(None))
        } else {
            Ok(ScalarValue::Utf8(Some(
                JsonValue::Array(self.values.clone()).to_string(),
            )))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[derive(Debug)]
struct JsonObjectAggUdf {
    signature: Signature,
    name: &'static str,
    null_behavior: JsonConstructorNullBehavior,
}

impl JsonObjectAggUdf {
    fn new(name: &'static str, null_behavior: JsonConstructorNullBehavior) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            name,
            null_behavior,
        }
    }
}

impl AggregateUDFImpl for JsonObjectAggUdf {
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

    fn state_fields(&self, args: StateFieldsArgs) -> DataFusionResult<Vec<FieldRef>> {
        let field = Field::new(format!("{}_state", args.name), DataType::Utf8, true);
        Ok(vec![Arc::new(field)])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return Err(DataFusionError::Execution(
                "JSON_OBJECTAGG does not support DISTINCT clauses".to_string(),
            ));
        }
        if !acc_args.order_bys.is_empty() {
            return Err(DataFusionError::Execution(
                "JSON_OBJECTAGG does not yet support ORDER BY clauses".to_string(),
            ));
        }
        if acc_args.ignore_nulls {
            return Err(DataFusionError::Execution(
                "JSON_OBJECTAGG does not support IGNORE NULLS".to_string(),
            ));
        }
        Ok(Box::new(JsonObjectAggAccumulator::new(matches!(
            self.null_behavior,
            JsonConstructorNullBehavior::NullOnNull
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }
}

#[derive(Debug)]
struct JsonObjectAggAccumulator {
    entries: Vec<(String, JsonValue)>,
    include_nulls: bool,
    seen_keys: HashSet<String>,
}

impl JsonObjectAggAccumulator {
    fn new(include_nulls: bool) -> Self {
        Self {
            entries: Vec::new(),
            include_nulls,
            seen_keys: HashSet::new(),
        }
    }
}

impl Accumulator for JsonObjectAggAccumulator {
    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        if self.entries.is_empty() {
            Ok(vec![ScalarValue::Utf8(None)])
        } else {
            let mut map = JsonMap::new();
            for (key, value) in &self.entries {
                map.insert(key.clone(), value.clone());
            }
            Ok(vec![ScalarValue::Utf8(Some(
                JsonValue::Object(map).to_string(),
            ))])
        }
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        let keys = &values[0];
        let vals = &values[1];

        for row in 0..keys.len() {
            let key_scalar = ScalarValue::try_from_array(keys.as_ref(), row)?;
            if key_scalar.is_null() {
                return Err(DataFusionError::Execution(
                    "JSON_OBJECTAGG keys must not be NULL".to_string(),
                ));
            }
            let key = scalar_to_json_key(&key_scalar)?;

            let value_scalar = ScalarValue::try_from_array(vals.as_ref(), row)?;
            if value_scalar.is_null() && !self.include_nulls {
                continue;
            }

            if !self.seen_keys.insert(key.clone()) {
                return Err(DataFusionError::Execution(format!(
                    "JSON_OBJECTAGG encountered duplicate key '{key}'"
                )));
            }

            let json_value = if value_scalar.is_null() {
                JsonValue::Null
            } else {
                scalar_to_json_value(&value_scalar)?
            };
            self.entries.push((key, json_value));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        let array = states
            .first()
            .and_then(|arr| arr.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Execution("JSON_OBJECTAGG state must be Utf8".to_string())
            })?;

        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let state_value = array.value(row);
            let parsed = parse_json_object_state(state_value)?;
            for (key, value) in parsed {
                if !self.seen_keys.insert(key.clone()) {
                    return Err(DataFusionError::Execution(format!(
                        "JSON_OBJECTAGG encountered duplicate key '{key}'"
                    )));
                }
                self.entries.push((key, value));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        if self.entries.is_empty() {
            Ok(ScalarValue::Utf8(None))
        } else {
            let mut map = JsonMap::new();
            for (key, value) in &self.entries {
                map.insert(key.clone(), value.clone());
            }
            Ok(ScalarValue::Utf8(Some(JsonValue::Object(map).to_string())))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

fn parse_json_array_state(text: &str) -> DataFusionResult<Vec<JsonValue>> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let value = serde_json::from_str::<JsonValue>(text).map_err(|err| {
        DataFusionError::Execution(format!("Invalid JSON array state: `{text}` ({err})"))
    })?;
    match value {
        JsonValue::Array(values) => Ok(values),
        _ => Err(DataFusionError::Execution(
            "JSON_ARRAYAGG state expected array value".to_string(),
        )),
    }
}

fn parse_json_object_state(text: &str) -> DataFusionResult<Vec<(String, JsonValue)>> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let value = serde_json::from_str::<JsonValue>(text).map_err(|err| {
        DataFusionError::Execution(format!("Invalid JSON object state: `{text}` ({err})"))
    })?;
    match value {
        JsonValue::Object(map) => Ok(map.into_iter().collect()),
        _ => Err(DataFusionError::Execution(
            "JSON_OBJECTAGG state expected object value".to_string(),
        )),
    }
}
